from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import numpy as np

from services.chunking import Chunk

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    ONNX-backed embedding service.

    Replaces sentence-transformers + torch (~460 MB) with
    onnxruntime + optimum (~35 MB total).

    The model is exported to ONNX once on first load (or at Docker build
    time if pre-baked) and cached to HF_HOME on disk.  Subsequent cold
    starts load the cached ONNX file — no re-export, no spike.

    Output vectors are numerically identical to SentenceTransformer
    (same model weights, same mean-pooling + L2-normalisation).
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    ):
        self.model_name = model_name
        self._tokenizer = None
        self._model = None

    # ── Lazy load (triggered once at lifespan warm-up) ────────────────────────

    @property
    def model(self):
        if self._model is None:
            self._load()
        return self._model

    def _load(self) -> None:
        from optimum.onnxruntime import ORTModelForFeatureExtraction
        from transformers import AutoTokenizer

        logger.info("Loading ONNX tokenizer: %s", self.model_name)
        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)

        logger.info("Loading ONNX model: %s (export=True caches to HF_HOME)", self.model_name)
        self._model = ORTModelForFeatureExtraction.from_pretrained(
            self.model_name,
            export=True,   # converts HF → ONNX on first call, reuses cache after
        )
        logger.info("✔ ONNX model loaded")

    # ── Internal encode (tokenise → infer → mean-pool → L2-norm) ─────────────

    def _encode(self, texts: List[str]) -> np.ndarray:
        """
        Tokenise `texts`, run ONNX inference, mean-pool, L2-normalise.
        Returns float32 array of shape (len(texts), embedding_dim).
        """
        inputs = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=256,       # all-MiniLM-L12-v2 was trained at 256 tokens
            return_tensors="np",  # numpy tensors — no torch required
        )
        outputs = self.model(**inputs)

        # Mean pool over token dimension, weighted by attention mask
        token_emb   = outputs.last_hidden_state          # (B, T, D)
        mask        = inputs["attention_mask"]            # (B, T)
        mask_expand = mask[:, :, np.newaxis].astype(np.float32)  # (B, T, 1)
        summed      = (token_emb * mask_expand).sum(axis=1)       # (B, D)
        counts      = mask_expand.sum(axis=1).clip(min=1e-9)      # (B, 1)
        pooled      = summed / counts                              # (B, D)

        # L2 normalise
        norms = np.linalg.norm(pooled, axis=1, keepdims=True)
        return (pooled / np.maximum(norms, 1e-9)).astype(np.float32)

    # ── Public API ────────────────────────────────────────────────────────────

    def embed_chunks(
        self,
        chunks: List[Chunk],
        batch_size: int = 32,
        show_progress: bool = False,   # kept for API compatibility, unused
        **_,
    ) -> np.ndarray:
        """
        Embed a list of Chunk objects in batches.
        Attaches the vector to chunk.embedding in-place.
        Returns float32 array of shape (N, embedding_dim).
        """
        if not chunks:
            return np.empty((0,), dtype=np.float32)

        texts    = [c.text for c in chunks]
        all_vecs: List[np.ndarray] = []

        logger.info("Embedding %d chunks (batch_size=%d)", len(texts), batch_size)

        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            vecs  = self._encode(batch)
            all_vecs.append(vecs)

        vectors = np.vstack(all_vecs)   # (N, D)

        # Attach vectors back to chunks in-place
        for chunk, vec in zip(chunks, vectors):
            chunk.embedding = vec

        logger.info("✔ Embedding shape: %s", vectors.shape)
        return vectors

    def embed_texts(self, texts: List[str]) -> np.ndarray:
        """Embed arbitrary text strings. Returns float32 (N, D)."""
        if not texts:
            return np.empty((0,), dtype=np.float32)
        return self._encode(texts)

    def embed_query(self, query: str) -> np.ndarray:
        """Embed a single query string. Returns float32 (1, D)."""
        return self._encode([query])

    # ── FAISS helpers (unchanged from original) ───────────────────────────────

    def build_faiss_index(self, vectors: np.ndarray):
        try:
            import faiss
        except ImportError:
            raise ImportError("Install faiss-cpu: pip install faiss-cpu")

        dimension = vectors.shape[1]
        index     = faiss.IndexFlatL2(dimension)
        index.add(vectors)
        logger.info("FAISS index built: %d vectors, dim=%d", index.ntotal, dimension)
        return index

    def save_faiss_index(self, index, output_path: Path) -> Path:
        import faiss
        output_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(output_path))
        logger.info("FAISS index saved: %s", output_path)
        return output_path

    def load_faiss_index(self, index_path: Path):
        import faiss
        return faiss.read_index(str(index_path))
