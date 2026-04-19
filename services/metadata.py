"""
services/metadata.py — Serialize chunk metadata to JSON and aggregate pipeline summaries.

FIXES:
  1. chunks_to_jsonl_bytes() now serializes the embedding vector so chat sessions
     can skip re-embedding (was missing — embeddings were never stored in Azure).
  2. Added chunks_from_jsonl_bytes() which was completely absent from this file —
     without it the chat layer crashed with AttributeError when trying to load chunks.
     The deserializer supplies all required Chunk fields with safe fallbacks so
     old JSONL files (pre-fix) are still readable.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from services.chunking import Chunk
from utils.helpers import make_chunk_id, utc_now_iso

logger = logging.getLogger(__name__)


class MetadataService:

    # ── Serialization ──────────────────────────────────────────────────────

    def chunk_to_json_bytes(self, chunk: Chunk) -> bytes:
        """Serialize a single Chunk to UTF-8 JSON bytes."""
        return json.dumps(chunk.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")

    def chunks_to_jsonl_bytes(self, chunks: List[Chunk]) -> bytes:
        """
        Serialize all chunks as newline-delimited JSON.
        FIX: now includes embedding vectors so the chat layer can rebuild
        the FAISS index from Azure without re-running the embedding model.
        """
        lines = []
        for c in chunks:
            d = c.to_dict()
            # Include embedding if present
            if c.embedding is not None:
                emb = c.embedding
                d["embedding"] = (
                    emb.tolist() if isinstance(emb, np.ndarray) else list(emb)
                )
            else:
                d["embedding"] = None
            lines.append(json.dumps(d, ensure_ascii=False))
        return "\n".join(lines).encode("utf-8")

    def save_chunks_jsonl(self, chunks: List[Chunk], output_path: Path) -> Path:
        """Write chunks to a local JSONL file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(self.chunks_to_jsonl_bytes(chunks))
        logger.debug("Saved %d chunks → %s", len(chunks), output_path)
        return output_path

    # ── Deserialization ────────────────────────────────────────────────────

    def chunks_from_jsonl_bytes(self, raw: bytes) -> List[Chunk]:
        """
        FIX: This method was completely missing from the original file.
        The chat layer calls meta_svc.chunks_from_jsonl_bytes() which raised
        AttributeError, making it impossible to load any chunks from Azure.

        Reconstructs Chunk objects from JSONL bytes with safe fallbacks for
        all fields so pre-fix JSONL files (without embedding/page/etc.) still load.
        """
        chunks: List[Chunk] = []

        for line in raw.decode("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.warning("Skipping malformed JSONL line: %s", exc)
                continue

            chunk_index = data.get("chunk_index", 0)
            doc_id      = data.get("doc_id", "unknown")

            chunk = Chunk(
                doc_id=doc_id,
                chunk_id=data.get("chunk_id") or make_chunk_id(doc_id, chunk_index),
                chunk_index=chunk_index,
                text=data.get("text", ""),
                page=data.get("page", 0),
                source_file=data.get("source_file", ""),
                source_type=data.get("source_type", "local"),
                uploaded_at=data.get("uploaded_at") or utc_now_iso(),
                extra_metadata=data.get("metadata", {}),
            )

            raw_emb = data.get("embedding")
            if raw_emb is not None:
                chunk.embedding = np.array(raw_emb, dtype="float32")
            else:
                chunk.embedding = None

            chunks.append(chunk)

        return chunks

    # ── Run summary ────────────────────────────────────────────────────────

    def build_run_summary(
        self,
        doc_ids: List[str],
        chunk_counts: List[int],
        upload_results: List[dict],
        elapsed_seconds: float,
    ) -> dict:
        total_chunks  = sum(chunk_counts)
        success_count = sum(1 for r in upload_results if r.get("success"))
        failure_count = len(upload_results) - success_count

        return {
            "run_timestamp":       utc_now_iso(),
            "documents_processed": len(doc_ids),
            "total_chunks":        total_chunks,
            "uploads_succeeded":   success_count,
            "uploads_failed":      failure_count,
            "elapsed_seconds":     round(elapsed_seconds, 2),
            "per_document": [
                {"doc_id": d, "chunk_count": c}
                for d, c in zip(doc_ids, chunk_counts)
            ],
            "upload_errors": [r for r in upload_results if not r.get("success")],
        }

    def summary_to_dataframe(self, summary: dict) -> pd.DataFrame:
        return pd.DataFrame(summary.get("per_document", []))
