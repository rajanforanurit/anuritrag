from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Optional

# Force PyTorch-only backend BEFORE imports
os.environ.setdefault("TRANSFORMERS_NO_TF", "1")
os.environ.setdefault("TRANSFORMERS_NO_FLAX", "1")

import numpy as np

from services.chunking import Chunk

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    SentenceTransformer wrapper.

    Supports:
    - Preloaded model (from FastAPI startup) ✅
    - Lazy loading fallback ✅
    """

    def __init__(
        self,
        model=None,
        model_name: Optional[str] = "sentence-transformers/all-MiniLM-L12-v2"
    ):
        self.model_name = model_name
        self._model = model  # may be preloaded or None

    @property
    def model(self):
        # If already provided (from main.py), use it
        if self._model is not None:
            return self._model

        # Fallback: lazy load (only if not preloaded)
        from sentence_transformers import SentenceTransformer
        logger.info("Lazy loading embedding model: %s", self.model_name)
        self._model = SentenceTransformer(self.model_name)

        return self._model

    # ── Core embedding ────────────────────────────────────────────────────

    def embed_chunks(
        self,
        chunks: List[Chunk],
        batch_size: int = 64,
        show_progress: bool = True,
    ) -> np.ndarray:

        if not chunks:
            return np.empty((0,), dtype=np.float32)

        texts = [c.text for c in chunks]

        logger.info("Embedding %d chunks", len(texts))

        vectors = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)

        # Attach vectors back to chunks
        for chunk, vec in zip(chunks, vectors):
            chunk.embedding = vec

        return vectors

    def embed_texts(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.empty((0,), dtype=np.float32)

        vectors = self.model.encode(
            texts,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        return vectors.astype(np.float32)

    def embed_query(self, query: str) -> np.ndarray:
        vector = self.model.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        return vector.astype(np.float32)

    # ── FAISS helpers ────────────────────────────────────────────────────

    def build_faiss_index(self, vectors: np.ndarray):
        try:
            import faiss
        except ImportError:
            raise ImportError("Install faiss-cpu: pip install faiss-cpu")

        dimension = vectors.shape[1]
        index = faiss.IndexFlatL2(dimension)
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
