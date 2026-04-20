from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path
from typing import Callable, List, Optional

import numpy as np

from config import Config
from services.blob_storage import BlobStorageService
from services.chunking import Chunk, Chunker
from services.document_loader import DocumentLoader, RawDocument
from services.embedding import EmbeddingService
from services.metadata import MetadataService

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]


# ── Shared singleton services (module-level, initialised once) ─────────────────

_blob_svc:   Optional[BlobStorageService] = None
_embedder:   Optional[EmbeddingService]   = None
_meta_svc:   MetadataService              = MetadataService()
_loader:     DocumentLoader               = DocumentLoader()
_chunker:    Optional[Chunker]            = None


def get_blob_svc() -> BlobStorageService:
    global _blob_svc
    if _blob_svc is None:
        _blob_svc = BlobStorageService(
            Config.AZURE_CONNECTION_STRING,
            Config.AZURE_CONTAINER_NAME,
        )
    return _blob_svc

def get_embedder() -> EmbeddingService:
    global _embedder
    if _embedder is None:
        from main import embedding_model
        if embedding_model is None:
            raise RuntimeError("Model not loaded at startup")

        _embedder = EmbeddingService(model=embedding_model)

    return _embedder

def get_chunker() -> Chunker:
    global _chunker
    if _chunker is None:
        _chunker = Chunker(
            chunk_size=Config.CHUNK_SIZE,
            chunk_overlap=Config.CHUNK_OVERLAP,
        )
    return _chunker


# ── Public pipeline functions ──────────────────────────────────────────────────

def run_pipeline(
    source_path: Path,
    source_type: str = "local",
    source_id: Optional[str] = None,
    extra_metadata: Optional[dict] = None,
    log_fn: Optional[LogFn] = None,
    blob_svc: Optional[BlobStorageService] = None,
    embedder: Optional[EmbeddingService] = None,
) -> dict:

    def log(msg: str) -> None:
        logger.info(msg)
        if log_fn:
            log_fn(msg)

    start    = time.time()
    blob     = blob_svc or get_blob_svc()
    emb      = embedder  or get_embedder()
    chunker  = get_chunker()

    # 1 + 2 — Scan + extract
    log(f"▶ Scanning: {source_path}")
    try:
        docs = _loader.load_from_directory(
            source_path,
            source_type=source_type,
            extra_metadata=extra_metadata or {},
        )
    except ValueError as exc:
        return {"error": str(exc)}

    if not docs:
        return {"error": "No supported documents found in the provided path."}
    log(f"  ✔ {len(docs)} document(s) loaded")

    # 3 — Chunk
    all_chunks: List[Chunk] = []
    chunk_counts: List[int] = []
    doc_ids: List[str]      = []

    for doc in docs:
        chunks = chunker.chunk_document(doc)
        all_chunks.extend(chunks)
        chunk_counts.append(len(chunks))
        doc_ids.append(doc.doc_id)
        log(f"  ↳ {doc.doc_id} ({doc.file_path.name}) → {len(chunks)} chunks")
    log(f"  ✔ Total chunks: {len(all_chunks)}")

    # 4 — Embed (attaches vectors to chunk.embedding in place)
    log("▶ Generating embeddings…")
    vectors = emb.embed_chunks(all_chunks, show_progress=False)
    log(f"  ✔ Embedding shape: {vectors.shape}")

    # 5 — Optional FAISS backup
    faiss_path: Optional[Path] = None
    if Config.ENABLE_FAISS_BACKUP:
        try:
            index     = emb.build_faiss_index(vectors)
            faiss_path = Config.FAISS_INDEX_PATH
            emb.save_faiss_index(index, faiss_path)
            log(f"  ✔ FAISS index saved → {faiss_path}")
        except Exception as exc:
            log(f"  ⚠ FAISS skipped: {exc}")

    # 6 + 7 + 8 — Upload to Azure
    log("▶ Uploading to Azure Blob Storage…")
    upload_results: List[dict] = []

    # 6 — Raw files
    for doc in docs:
        blob_name = Config.BLOB_RAW_PREFIX + doc.file_path.name
        r = blob.upload_file(doc.file_path, blob_name)
        upload_results.append(r)
        log(f"  {'✔' if r['success'] else '✘'} {blob_name}")

    # 7 — Chunk JSONL (with embeddings)
    chunk_map: dict[str, List[Chunk]] = {}
    for chunk in all_chunks:
        chunk_map.setdefault(chunk.doc_id, []).append(chunk)

    for doc_id, chunks in chunk_map.items():
        jsonl     = _meta_svc.chunks_to_jsonl_bytes(chunks)
        blob_name = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"
        r = blob.upload_bytes(jsonl, blob_name, content_type="application/x-ndjson")
        upload_results.append(r)
        log(f"  {'✔' if r['success'] else '✘'} {blob_name} ({len(chunks)} chunks)")

    # 8 — Per-document meta JSON
    for doc in docs:
        meta = _build_doc_meta(doc, chunk_map.get(doc.doc_id, []))
        import json
        meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode()
        blob_name  = Config.BLOB_META_PREFIX + f"{doc.doc_id}_meta.json"
        r = blob.upload_bytes(meta_bytes, blob_name, content_type="application/json")
        upload_results.append(r)
        log(f"  {'✔' if r['success'] else '✘'} {blob_name}")

    # 9 — Optional FAISS upload
    if faiss_path and faiss_path.exists():
        r = blob.upload_file(faiss_path, Config.BLOB_FAISS_PREFIX + faiss_path.name)
        upload_results.append(r)
        log(f"  {'✔' if r['success'] else '✘'} faiss/{faiss_path.name}")

    elapsed = time.time() - start
    summary = _meta_svc.build_run_summary(doc_ids, chunk_counts, upload_results, elapsed)
    log(
        f"\n✔ Done — {summary['documents_processed']} docs | "
        f"{summary['total_chunks']} chunks | "
        f"{summary['uploads_succeeded']} uploads ok | {elapsed:.1f}s"
    )
    return summary


def run_pipeline_single_file(
    file_path: Path,
    source_type: str = "upload",
    extra_metadata: Optional[dict] = None,
    log_fn: Optional[LogFn] = None,
    blob_svc: Optional[BlobStorageService] = None,
    embedder: Optional[EmbeddingService] = None,
) -> dict:

    def log(msg: str) -> None:
        logger.info(msg)
        if log_fn:
            log_fn(msg)

    start   = time.time()
    blob    = blob_svc or get_blob_svc()
    emb     = embedder  or get_embedder()
    chunker = get_chunker()

    log(f"▶ Loading single file: {file_path.name}")
    doc = _loader.load_single_file(file_path, source_type=source_type)
    if doc is None:
        return {"error": f"Could not extract text from '{file_path.name}'. "
                         "File may be empty, corrupt, or an unsupported type."}

    if extra_metadata:
        doc.extra_metadata.update(extra_metadata)

    # Chunk
    chunks = chunker.chunk_document(doc)
    log(f"  ✔ {len(chunks)} chunks from {doc.file_path.name}")

    # Embed
    vectors = emb.embed_chunks(chunks, show_progress=False)
    log(f"  ✔ Embedding shape: {vectors.shape}")

    # Upload
    upload_results: List[dict] = []
    import json

    r = blob.upload_file(file_path, Config.BLOB_RAW_PREFIX + file_path.name)
    upload_results.append(r)
    log(f"  {'✔' if r['success'] else '✘'} raw/{file_path.name}")

    jsonl     = _meta_svc.chunks_to_jsonl_bytes(chunks)
    blob_name = Config.BLOB_CHUNKS_PREFIX + f"{doc.doc_id}_chunks.jsonl"
    r = blob.upload_bytes(jsonl, blob_name, content_type="application/x-ndjson")
    upload_results.append(r)
    log(f"  {'✔' if r['success'] else '✘'} {blob_name}")

    meta      = _build_doc_meta(doc, chunks)
    meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode()
    blob_name  = Config.BLOB_META_PREFIX + f"{doc.doc_id}_meta.json"
    r = blob.upload_bytes(meta_bytes, blob_name, content_type="application/json")
    upload_results.append(r)
    log(f"  {'✔' if r['success'] else '✘'} {blob_name}")

    elapsed = time.time() - start
    summary = _meta_svc.build_run_summary(
        [doc.doc_id], [len(chunks)], upload_results, elapsed
    )
    log(f"✔ Done — {elapsed:.1f}s")
    return summary


def rebuild_index_for_doc_id(
    doc_id: str,
    blob_svc: Optional[BlobStorageService] = None,
    embedder: Optional[EmbeddingService] = None,
    log_fn: Optional[LogFn] = None,
) -> dict:
    """
    Re-download chunks for doc_id from Azure, re-embed, re-upload.
    Used by POST /rebuild-index?doc_id=...
    """

    def log(msg: str) -> None:
        logger.info(msg)
        if log_fn:
            log_fn(msg)

    start = time.time()
    blob  = blob_svc or get_blob_svc()
    emb   = embedder  or get_embedder()

    blob_name = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"
    log(f"▶ Downloading chunks: {blob_name}")

    try:
        raw = blob.download_bytes(blob_name)
    except Exception as exc:
        return {"error": f"Could not download chunks for '{doc_id}': {exc}"}

    chunks = _meta_svc.chunks_from_jsonl_bytes(raw)
    if not chunks:
        return {"error": f"No chunks found for doc_id '{doc_id}'"}

    log(f"  ✔ {len(chunks)} chunks loaded")

    # Re-embed
    vectors = emb.embed_chunks(chunks, show_progress=False)
    log(f"  ✔ Re-embedded {len(chunks)} chunks")

    # Re-upload JSONL with fresh embeddings
    jsonl = _meta_svc.chunks_to_jsonl_bytes(chunks)
    r = blob.upload_bytes(jsonl, blob_name, content_type="application/x-ndjson")

    elapsed = time.time() - start
    return {
        "doc_id":         doc_id,
        "chunks_rebuilt": len(chunks),
        "upload_success": r["success"],
        "elapsed_seconds": round(elapsed, 2),
        "error":          r.get("error"),
    }


# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_doc_meta(doc: RawDocument, chunks: List[Chunk]) -> dict:
    """Build a rich metadata JSON for a processed document."""
    from utils.helpers import utc_now_iso
    return {
        "doc_id":          doc.doc_id,
        "source_file":     doc.file_path.name,
        "source_type":     doc.source_type,
        "total_pages":     doc.total_pages,
        "total_chunks":    len(chunks),
        "total_chars":     sum(c.char_count for c in chunks),
        "processed_at":    utc_now_iso(),
        "chunk_ids":       [c.chunk_id for c in chunks],
        "extra_metadata":  doc.extra_metadata,
        "blob_urls": {
            "raw":    f"{Config.BLOB_RAW_PREFIX}{doc.file_path.name}",
            "chunks": f"{Config.BLOB_CHUNKS_PREFIX}{doc.doc_id}_chunks.jsonl",
            "meta":   f"{Config.BLOB_META_PREFIX}{doc.doc_id}_meta.json",
        },
    }
