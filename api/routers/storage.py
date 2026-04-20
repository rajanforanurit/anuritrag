from __future__ import annotations
import json
import logging
import time
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.middleware.auth import require_api_key
from api.schemas import (
    BlobPrefixStats,
    ChunkDetail,
    ChunksResponse,
    DeleteDocumentResponse,
    DocumentListItem,
    DocumentListResponse,
    DocumentMetaResponse,
    ChunkSummary,
    RebuildIndexRequest,
    RebuildIndexResponse,
    StorageStatusResponse,
)
from config import Config
from services.metadata import MetadataService
from services.pipeline import get_blob_svc, get_embedder, rebuild_index_for_doc_id

logger = logging.getLogger(__name__)

router    = APIRouter(tags=["Storage & Documents"])
_meta_svc = MetadataService()


# ── GET /storage/status ────────────────────────────────────────────────────────

@router.get(
    "/storage/status",
    response_model=StorageStatusResponse,
    summary="Azure Blob Storage connection health check",
    description=(
        "Pings the configured Azure Blob container and returns connection status "
        "and per-prefix blob counts."
    ),
)
async def storage_status(_key: str = Depends(require_api_key)):
    blob = get_blob_svc()
    ping = blob.ping()

    prefix_stats: List[BlobPrefixStats] = []
    if ping["ok"]:
        for prefix in (
            Config.BLOB_RAW_PREFIX,
            Config.BLOB_CHUNKS_PREFIX,
            Config.BLOB_META_PREFIX,
            Config.BLOB_FAISS_PREFIX,
        ):
            details = blob.list_blob_details(prefix)
            prefix_stats.append(BlobPrefixStats(
                prefix=prefix,
                blob_count=len(details),
                total_bytes=sum(d["size_bytes"] or 0 for d in details),
            ))

    return StorageStatusResponse(
        ok=ping["ok"],
        account=ping.get("account"),
        container=ping.get("container"),
        connection_error=ping.get("error"),
        prefixes=prefix_stats,
    )


# ── GET /documents ─────────────────────────────────────────────────────────────

@router.get(
    "/documents",
    response_model=DocumentListResponse,
    summary="List all ingested documents",
    description="Returns a summary list of every document stored in Azure (reads meta/ prefix).",
)
async def list_documents(_key: str = Depends(require_api_key)):
    blob  = get_blob_svc()
    names = blob.list_blobs(prefix=Config.BLOB_META_PREFIX)
    meta_blobs = [n for n in names if n.endswith("_meta.json")]

    items: List[DocumentListItem] = []
    for blob_name in meta_blobs:
        try:
            raw  = blob.download_bytes(blob_name)
            meta = json.loads(raw)
            items.append(DocumentListItem(
                doc_id=meta.get("doc_id", ""),
                source_file=meta.get("source_file", ""),
                total_chunks=meta.get("total_chunks", 0),
                processed_at=meta.get("processed_at", ""),
                blob_url_meta=blob._url(blob_name),
            ))
        except Exception as exc:
            logger.warning("Could not parse meta blob '%s': %s", blob_name, exc)

    return DocumentListResponse(total=len(items), documents=items)


# ── GET /document/{doc_id} ─────────────────────────────────────────────────────

@router.get(
    "/document/{doc_id}",
    response_model=DocumentMetaResponse,
    summary="Retrieve metadata and chunk list for a document",
    description=(
        "Returns full document metadata including all chunk previews (first 200 chars). "
        "Use GET /chunks/{doc_id} for full chunk text."
    ),
)
async def get_document(doc_id: str, _key: str = Depends(require_api_key)):
    blob       = get_blob_svc()
    meta_blob  = Config.BLOB_META_PREFIX   + f"{doc_id}_meta.json"
    chunk_blob = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"

    if not blob.blob_exists(meta_blob):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Document '{doc_id}' not found. "
                   "Run an ingestion first or check the doc_id.",
        )

    try:
        meta = json.loads(blob.download_bytes(meta_blob))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read metadata: {exc}")

    chunk_summaries: List[ChunkSummary] = []
    if blob.blob_exists(chunk_blob):
        try:
            raw    = blob.download_bytes(chunk_blob)
            chunks = _meta_svc.chunks_from_jsonl_bytes(raw)
            chunk_summaries = [
                ChunkSummary(
                    chunk_id=c.chunk_id,
                    chunk_index=c.chunk_index,
                    page=c.page,
                    char_count=c.char_count,
                    text_preview=c.text[:200],
                )
                for c in chunks
            ]
        except Exception as exc:
            logger.warning("Could not load chunks for %s: %s", doc_id, exc)

    return DocumentMetaResponse(
        doc_id=meta.get("doc_id", doc_id),
        source_file=meta.get("source_file", ""),
        source_type=meta.get("source_type", ""),
        total_pages=meta.get("total_pages", 0),
        total_chunks=meta.get("total_chunks", 0),
        total_chars=meta.get("total_chars", 0),
        processed_at=meta.get("processed_at", ""),
        blob_urls=meta.get("blob_urls", {}),
        chunks=chunk_summaries,
        extra_metadata=meta.get("extra_metadata", {}),
    )


# ── DELETE /document/{doc_id} ──────────────────────────────────────────────────

@router.delete(
    "/document/{doc_id}",
    response_model=DeleteDocumentResponse,
    summary="Delete all blobs for a document",
    description=(
        "Removes the raw file, chunk JSONL, and meta JSON from Azure Blob Storage. "
        "This is irreversible."
    ),
)
async def delete_document(doc_id: str, _key: str = Depends(require_api_key)):
    blob           = get_blob_svc()
    meta_blob_name = Config.BLOB_META_PREFIX + f"{doc_id}_meta.json"
    deleted        = 0
    source_file: Optional[str] = None

    if blob.blob_exists(meta_blob_name):
        try:
            meta        = json.loads(blob.download_bytes(meta_blob_name))
            source_file = meta.get("source_file")
        except Exception:
            pass

    chunk_blob = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"
    if blob.delete_blob(chunk_blob):
        deleted += 1

    if blob.delete_blob(meta_blob_name):
        deleted += 1

    if source_file:
        raw_blob = Config.BLOB_RAW_PREFIX + source_file
        if blob.delete_blob(raw_blob):
            deleted += 1

    if deleted == 0:
        return DeleteDocumentResponse(
            doc_id=doc_id,
            blobs_deleted=0,
            status="not_found",
            message=f"No blobs found for doc_id '{doc_id}'.",
        )

    return DeleteDocumentResponse(
        doc_id=doc_id,
        blobs_deleted=deleted,
        status="success",
        message=f"Deleted {deleted} blob(s) for '{doc_id}'.",
    )


# ── GET /chunks/{doc_id} ───────────────────────────────────────────────────────

@router.get(
    "/chunks/{doc_id}",
    response_model=ChunksResponse,
    summary="Get full chunk text for a document",
    description="Downloads and returns all chunks including full text. Useful for debugging.",
)
async def get_chunks(
    doc_id: str,
    page: Optional[int] = Query(None, description="Filter by page number"),
    _key: str = Depends(require_api_key),
):
    blob       = get_blob_svc()
    chunk_blob = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"

    if not blob.blob_exists(chunk_blob):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No chunks found for doc_id '{doc_id}'.",
        )

    try:
        raw    = blob.download_bytes(chunk_blob)
        chunks = _meta_svc.chunks_from_jsonl_bytes(raw)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load chunks: {exc}")

    if page is not None:
        chunks = [c for c in chunks if c.page == page]

    return ChunksResponse(
        doc_id=doc_id,
        total=len(chunks),
        chunks=[
            ChunkDetail(
                chunk_id=c.chunk_id,
                chunk_index=c.chunk_index,
                page=c.page,
                char_count=c.char_count,
                source_file=c.source_file,
                uploaded_at=c.uploaded_at,
                text=c.text,
            )
            for c in chunks
        ],
    )


# ── POST /rebuild-index ────────────────────────────────────────────────────────

@router.post(
    "/rebuild-index",
    response_model=RebuildIndexResponse,
    summary="Rebuild embeddings for one or all documents",
    description=(
        "Re-downloads chunks from Azure, regenerates embeddings with the current model, "
        "and re-uploads the JSONL. Pass doc_id to rebuild a single document, "
        "or omit it to rebuild all documents (may take a long time)."
    ),
)
async def rebuild_index(
    body: RebuildIndexRequest,
    _key: str = Depends(require_api_key),
):
    blob  = get_blob_svc()
    start = time.time()

    if body.doc_id:
        result  = rebuild_index_for_doc_id(body.doc_id, blob_svc=blob)
        elapsed = time.time() - start

        if "error" in result and result["error"]:
            return RebuildIndexResponse(
                rebuilt=[],
                skipped=[],
                errors=[{"doc_id": body.doc_id, "error": result["error"]}],
                elapsed_seconds=round(elapsed, 2),
                status="error",
            )

        return RebuildIndexResponse(
            rebuilt=[body.doc_id],
            skipped=[],
            errors=[],
            elapsed_seconds=round(elapsed, 2),
            status="success",
        )

    names      = blob.list_blobs(prefix=Config.BLOB_META_PREFIX)
    meta_blobs = [n for n in names if n.endswith("_meta.json")]

    if not meta_blobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No documents found in storage. Run an ingestion first.",
        )

    rebuilt: List[str] = []
    skipped: List[str] = []
    errors:  List[dict] = []

    for meta_blob in meta_blobs:
        try:
            raw    = blob.download_bytes(meta_blob)
            meta   = json.loads(raw)
            doc_id = meta.get("doc_id", "")
            if not doc_id:
                continue

            chunk_blob = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"
            if not blob.blob_exists(chunk_blob):
                skipped.append(doc_id)
                continue

            result = rebuild_index_for_doc_id(doc_id, blob_svc=blob)
            if result.get("error"):
                errors.append({"doc_id": doc_id, "error": result["error"]})
            else:
                rebuilt.append(doc_id)
        except Exception as exc:
            errors.append({"blob": meta_blob, "error": str(exc)})

    elapsed = time.time() - start
    overall = "success" if not errors else ("partial" if rebuilt else "error")

    return RebuildIndexResponse(
        rebuilt=rebuilt,
        skipped=skipped,
        errors=errors,
        elapsed_seconds=round(elapsed, 2),
        status=overall,
    )
