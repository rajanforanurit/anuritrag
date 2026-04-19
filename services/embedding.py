"""
services/embedding.py — Generate embeddings using sentence-transformers (PyTorch only).

FIXES:
  1. Added embed_query() — was called by the chat retrieval side but never existed,
     causing AttributeError on every query.
  2. embed_chunks() now attaches vectors back onto each Chunk.embedding field so
     that metadata serialisation picks them up automatically without extra wiring.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

# Force PyTorch-only backend BEFORE any transformers/sentence-transformers import
os.environ.setdefault("TRANSFORMERS_NO_TF", "1")
os.environ.setdefault("TRANSFORMERS_NO_FLAX", "1")

import numpy as np

from services.chunking import Chunk

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    Wraps a SentenceTransformer model to encode Chunk objects and raw strings.
    Model is loaded lazily on first use. Always runs on PyTorch — no TF required.
    """

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L12-v2"):
        self.model_name = model_name
        self._model = None

    @property
    def model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            logger.info("Loading embedding model: %s", self.model_name)
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed_chunks(
        self,
        chunks: List[Chunk],
        batch_size: int = 64,
        show_progress: bool = True,
    ) -> np.ndarray:
        """
        Encode chunk texts and return an (N, D) float32 array.
        FIX: also attaches each vector back onto chunk.embedding so the caller
        doesn't have to zip manually.
        """
        if not chunks:
            return np.empty((0,), dtype=np.float32)

        texts = [c.text for c in chunks]
        logger.info("Embedding %d chunks with '%s'", len(texts), self.model_name)

        vectors = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)

        # Attach vectors to chunks so metadata serialiser picks them up
        for chunk, vec in zip(chunks, vectors):
            chunk.embedding = vec

        return vectors

    def embed_texts(self, texts: List[str]) -> np.ndarray:
        """Embed raw strings (no Chunk objects needed)."""
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
        return vector.astype(np.float32)  # shape (1, D)

    # ── FAISS helpers ──────────────────────────────────────────────────────

    def build_faiss_index(self, vectors: np.ndarray):
        """Build an in-memory FAISS flat L2 index from an embedding matrix."""
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
