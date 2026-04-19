from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np
from langchain_text_splitters import RecursiveCharacterTextSplitter

from services.document_loader import RawDocument
from utils.helpers import make_chunk_id, utc_now_iso

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """A single text chunk with full provenance metadata."""

    doc_id: str
    chunk_id: str
    chunk_index: int
    text: str
    page: int
    source_file: str
    source_type: str        # "local" | "sharepoint" | …
    uploaded_at: str        # ISO-8601 UTC timestamp
    char_count: int = 0
    # FIX 1: use field(default_factory=dict) — mutable default fix
    extra_metadata: dict = field(default_factory=dict)
    # FIX 2: declare embedding field so pipeline can assign vectors cleanly
    embedding: Optional[np.ndarray] = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        self.char_count = len(self.text)

    def to_dict(self) -> dict:
        return {
            "doc_id":      self.doc_id,
            "chunk_id":    self.chunk_id,
            "chunk_index": self.chunk_index,
            "page":        self.page,
            "text":        self.text,
            "source_file": self.source_file,
            "source_type": self.source_type,
            "uploaded_at": self.uploaded_at,
            "char_count":  self.char_count,
            **self.extra_metadata,
        }


class Chunker:
    """
    Splits RawDocuments into fixed-size overlapping chunks.
    The splitter operates per page so page numbers remain accurate.
    """

    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 50):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ". ", " ", ""],
        )

    def chunk_document(self, doc: RawDocument) -> List[Chunk]:
        chunks: List[Chunk] = []
        global_index = 0
        timestamp = utc_now_iso()

        for page_entry in doc.pages:
            page_num  = page_entry["page"]
            page_text = page_entry["text"]

            if not page_text.strip():
                continue

            try:
                splits = self.splitter.split_text(page_text)
            except Exception:
                logger.exception(
                    "Chunking failed for doc '%s' page %d", doc.doc_id, page_num
                )
                continue

            for split_text in splits:
                stripped = split_text.strip()
                if not stripped:
                    continue

                chunks.append(
                    Chunk(
                        doc_id=doc.doc_id,
                        chunk_id=make_chunk_id(doc.doc_id, global_index),
                        chunk_index=global_index,
                        text=stripped,
                        page=page_num,
                        source_file=doc.file_path.name,
                        source_type=doc.source_type,
                        uploaded_at=timestamp,
                        extra_metadata=doc.extra_metadata,
                    )
                )
                global_index += 1

        logger.debug(
            "Chunked '%s' → %d chunks across %d pages",
            doc.doc_id, len(chunks), doc.total_pages,
        )
        return chunks
