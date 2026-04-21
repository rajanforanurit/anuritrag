from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import List, Optional
import numpy as np
from services.document_loader import RawDocument
from utils.helpers import make_chunk_id, utc_now_iso
logger = logging.getLogger(__name__)
@dataclass
class Chunk:
    doc_id: str
    chunk_id: str
    chunk_index: int
    text: str
    page: int
    source_file: str
    source_type: str   
    uploaded_at: str       
    char_count: int = 0
    extra_metadata: dict = field(default_factory=dict)
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
    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 2):
        self.chunk_size    = chunk_size
        self.chunk_overlap = chunk_overlap 

    def chunk_document(self, doc: RawDocument) -> List[Chunk]:
        chunks: List[Chunk] = []
        global_index = 0
        timestamp = utc_now_iso()

        for page_entry in doc.pages:
            page_num  = page_entry["page"]
            page_text = page_entry["text"]

            if not page_text.strip():
                continue
            lines = [
                ln.strip()
                for ln in page_text.replace("\r\n", "\n").split("\n")
                if ln.strip()
            ]
            buffer: List[str] = []
            for line in lines:
                projected_len = (
                    len("\n".join(buffer)) + 1 + len(line)
                    if buffer
                    else len(line)
                )
                if buffer and projected_len > self.chunk_size:
                    # Flush the current buffer
                    chunk_text = "\n".join(buffer).strip()
                    if len(chunk_text) > 30:
                        chunks.append(
                            Chunk(
                                doc_id=doc.doc_id,
                                chunk_id=make_chunk_id(doc.doc_id, global_index),
                                chunk_index=global_index,
                                text=chunk_text,
                                page=page_num,
                                source_file=doc.file_path.name,
                                source_type=doc.source_type,
                                uploaded_at=timestamp,
                                extra_metadata=doc.extra_metadata,
                            )
                        )
                        global_index += 1
                    buffer = buffer[-self.chunk_overlap:] if self.chunk_overlap > 0 else []

                buffer.append(line)
            if buffer:
                chunk_text = "\n".join(buffer).strip()
                if len(chunk_text) > 30:
                    chunks.append(
                        Chunk(
                            doc_id=doc.doc_id,
                            chunk_id=make_chunk_id(doc.doc_id, global_index),
                            chunk_index=global_index,
                            text=chunk_text,
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
