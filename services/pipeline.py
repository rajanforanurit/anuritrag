from __future__ import annotations
import io
import json
import logging
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
from config import Config
from services.blob_storage import (
    BlobStorageService,
    blob_storage_service,
    upload_file_to_blob,
    upload_file_to_blob_for_client,
)
from services.chunking import Chunk, Chunker
from services.document_loader import DocumentLoader
from services.embedding import EmbeddingService
from utils.helpers import make_doc_id

logger = logging.getLogger(__name__)
_loader = DocumentLoader()

_chunker = Chunker(
    chunk_size=Config.CHUNK_SIZE,
    chunk_overlap=Config.CHUNK_OVERLAP,
)
_MINILM_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

_embedder = EmbeddingService(model_name=_MINILM_MODEL)
_VECTOR_CACHE: Dict[str, List[Dict[str, Any]]] = {}
BLOB_VECTORS_PREFIX = "meta/{client_id}/vectors/"   
def ensure_directory(path: str) -> None:
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def safe_write_json(path: str, data: Any) -> None:
    ensure_directory(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def build_client_paths(client_id: str) -> Dict[str, Any]:
    client_id = client_id.strip().lower()

    base_meta_dir = Config.BASE_DIR / "metadata"
    client_meta_dir = base_meta_dir / client_id
    client_temp_dir = Config.TMP_DIR / client_id

    client_meta_dir.mkdir(parents=True, exist_ok=True)
    client_temp_dir.mkdir(parents=True, exist_ok=True)

    return {
        "temp_dir":      str(client_temp_dir),
        "meta_dir":      str(client_meta_dir),
        "meta_dir_path": client_meta_dir,
    }

def upload_original_files(
    raw_documents: List,
    client_id: str,
) -> List[str]:
    """
    Upload the original source files to  raw/<clientId>/<filename>  in blob.
    These are kept for audit/reference only – retrieval NEVER reads them.
    Returns the list of blob paths that were successfully uploaded.
    """
    uploaded_blob_paths: List[str] = []

    for doc in raw_documents:
        try:
            source_file = doc.file_path
            if not source_file or not Path(source_file).exists():
                logger.warning("Skipped missing file: %s", source_file)
                continue

            source_file = Path(source_file)

            result = upload_file_to_blob_for_client(
                client_id=client_id,
                local_file_path=str(source_file),
                prefix="raw",
            )

            if result.get("success"):
                uploaded_blob_paths.append(result.get("blob_name"))
                logger.info("Raw upload: %s → %s", source_file.name, result.get("blob_name"))
            else:
                logger.error("Raw upload failed for %s: %s", source_file.name, result.get("error"))

        except Exception as exc:
            logger.exception("Blob raw upload exception: %s", exc)

    return uploaded_blob_paths


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 – Build vector JSON for a document's chunks
# ─────────────────────────────────────────────────────────────────────────────

def chunks_to_vector_records(chunks: List[Chunk]) -> List[Dict[str, Any]]:
    """
    Convert a list of Chunk objects (with .embedding already set) into
    a list of plain dicts ready for JSON serialisation.

    Each record:
    {
        "text":        str,
        "embedding":   list[float],   # normalised MiniLM vector
        "source_file": str,
        "chunk_index": int,
        "doc_id":      str,
        "chunk_id":    str,
        "page":        int,
        "char_count":  int,
        "uploaded_at": str
    }
    """
    records: List[Dict[str, Any]] = []
    for chunk in chunks:
        if chunk.embedding is None:
            logger.warning("Chunk %s has no embedding – skipping", chunk.chunk_id)
            continue

        # np.ndarray → plain Python list of floats (JSON serialisable)
        embedding_list: List[float] = chunk.embedding.tolist()

        records.append({
            "text":        chunk.text,
            "embedding":   embedding_list,
            "source_file": chunk.source_file,
            "chunk_index": chunk.chunk_index,
            "doc_id":      chunk.doc_id,
            "chunk_id":    chunk.chunk_id,
            "page":        chunk.page,
            "char_count":  chunk.char_count,
            "uploaded_at": chunk.uploaded_at,
        })

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 – Upload vector JSON files to blob  meta/<clientId>/vectors/<stem>.json
# ─────────────────────────────────────────────────────────────────────────────

def upload_vectors_for_document(
    client_id: str,
    source_file_name: str,
    vector_records: List[Dict[str, Any]],
    blob_svc: BlobStorageService = None,
) -> Optional[str]:
    """
    Serialise vector_records to JSON bytes and upload to:
        meta/<clientId>/vectors/<stem>.json

    Returns the blob name on success, None on failure.
    """
    svc = blob_svc or blob_storage_service

    # Use stem of source filename so we can match later (e.g. "data_dictionary.json")
    stem = Path(source_file_name).stem
    blob_name = f"meta/{client_id}/vectors/{stem}.json"

    json_bytes = json.dumps(vector_records, ensure_ascii=False).encode("utf-8")

    result = svc.upload_bytes(
        data=json_bytes,
        blob_name=blob_name,
        content_type="application/json",
        overwrite=True,
    )

    if result and result.get("success"):
        logger.info(
            "Vectors uploaded: %s  (%d chunks, %d bytes)",
            blob_name,
            len(vector_records),
            len(json_bytes),
        )
        return blob_name
    else:
        err = result.get("error") if result else "unknown"
        logger.error("Vector upload failed for %s: %s", blob_name, err)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 – Save ingestion-run metadata to blob
# ─────────────────────────────────────────────────────────────────────────────

def save_metadata(
    client_id: str,
    label: str,
    files_processed: int,
    chunks_created: int,
    blob_paths: List[str],
    vector_blob_paths: List[str],
    extra_metadata: Dict[str, Any],
    meta_dir: str,
) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    metadata_payload = {
        "client_id":        client_id,
        "label":            label,
        "files_processed":  files_processed,
        "chunks_created":   chunks_created,
        "blob_paths":       blob_paths,
        "vector_blob_paths": vector_blob_paths,
        "embedding_model":  _MINILM_MODEL,
        "extra_metadata":   extra_metadata or {},
        "created_at":       datetime.utcnow().isoformat(),
        "status":           "success",
    }

    metadata_path = os.path.join(meta_dir, f"ingestion_{timestamp}.json")
    safe_write_json(metadata_path, metadata_payload)

    try:
        blob_name = f"{Config.BLOB_META_PREFIX}{client_id}/ingestion_{timestamp}.json"
        upload_result = upload_file_to_blob(
            local_file_path=metadata_path,
            blob_name=blob_name,
        )
        if upload_result.get("success"):
            logger.info("Metadata uploaded to blob: %s", blob_name)
        else:
            logger.warning("Metadata blob upload failed: %s", upload_result.get("error"))
    except Exception as exc:
        logger.exception("Metadata upload exception: %s", exc)

    return metadata_path


# ─────────────────────────────────────────────────────────────────────────────
# QUERY-TIME helpers  –  vector loader + cosine similarity retrieval
# ─────────────────────────────────────────────────────────────────────────────

def load_vectors_for_client(
    client_id: str,
    blob_svc: BlobStorageService = None,
    force_reload: bool = False,
) -> List[Dict[str, Any]]:
    """
    Load all vector JSON files from:
        meta/<clientId>/vectors/

    Results are merged and cached in _VECTOR_CACHE.

    Args:
        client_id:    The client whose vectors to load.
        blob_svc:     Optional BlobStorageService override.
        force_reload: If True, bypass cache and reload from blob.

    Returns:
        List of vector record dicts (each has 'text', 'embedding', etc.).
    """
    global _VECTOR_CACHE

    client_id = client_id.strip().lower()

    if not force_reload and client_id in _VECTOR_CACHE:
        logger.debug("Vector cache HIT for client '%s' (%d chunks)", client_id, len(_VECTOR_CACHE[client_id]))
        return _VECTOR_CACHE[client_id]

    svc = blob_svc or blob_storage_service
    prefix = f"meta/{client_id}/vectors/"

    blob_names: List[str] = svc.list_blobs(prefix=prefix)
    json_blobs = [b for b in blob_names if b.endswith(".json")]

    if not json_blobs:
        logger.warning("No vector files found for client '%s' at prefix '%s'", client_id, prefix)
        return []

    all_records: List[Dict[str, Any]] = []

    for blob_name in json_blobs:
        try:
            raw_bytes = svc.download_bytes(blob_name)
            records = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(records, list):
                all_records.extend(records)
                logger.debug("Loaded %d chunks from %s", len(records), blob_name)
            else:
                logger.warning("Unexpected JSON structure in %s (expected list)", blob_name)
        except Exception as exc:
            logger.error("Failed to load vector blob '%s': %s", blob_name, exc)

    logger.info(
        "Loaded %d total chunks for client '%s' from %d vector files",
        len(all_records),
        client_id,
        len(json_blobs),
    )

    # Cache for this process lifetime
    _VECTOR_CACHE[client_id] = all_records
    return all_records


def invalidate_vector_cache(client_id: Optional[str] = None) -> None:
    """
    Evict the vector cache for a specific client (or all clients if None).
    Call this after a new ingestion run to force fresh vector loads.
    """
    global _VECTOR_CACHE
    if client_id is None:
        _VECTOR_CACHE.clear()
        logger.info("Vector cache cleared for all clients")
    else:
        _VECTOR_CACHE.pop(client_id.strip().lower(), None)
        logger.info("Vector cache cleared for client '%s'", client_id)


def cosine_similarity_search(
    query_embedding: np.ndarray,
    vector_records: List[Dict[str, Any]],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """
    Pure-numpy normalised cosine similarity search.

    query_embedding must already be L2-normalised (MiniLM output is normalised
    by default when normalize_embeddings=True in SentenceTransformer.encode()).

    Returns the top_k records sorted by descending similarity, each augmented
    with a 'score' key (float between 0 and 1).
    """
    if not vector_records:
        return []

    # Stack embeddings into a matrix  [N x D]
    try:
        matrix = np.array(
            [r["embedding"] for r in vector_records],
            dtype=np.float32,
        )
    except (KeyError, ValueError) as exc:
        logger.error("Failed to build embedding matrix: %s", exc)
        return []

    q = query_embedding.astype(np.float32).flatten()

    # Normalise query (guard against zero vector)
    q_norm = np.linalg.norm(q)
    if q_norm > 0:
        q = q / q_norm

    row_norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    row_norms = np.where(row_norms == 0, 1.0, row_norms)
    matrix = matrix / row_norms

    # Dot product = cosine similarity (both normalised)
    scores: np.ndarray = matrix @ q          # shape [N]

    # Take top-k (partial sort for efficiency on large N)
    top_k = min(top_k, len(vector_records))
    top_indices = np.argpartition(scores, -top_k)[-top_k:]
    top_indices = top_indices[np.argsort(-scores[top_indices])]   # sort desc

    results = []
    for idx in top_indices:
        record = dict(vector_records[idx])      # shallow copy
        record["score"] = float(scores[idx])
        results.append(record)

    return results


def retrieve_top_chunks(
    query: str,
    client_id: str,
    top_k: int = 5,
    blob_svc: BlobStorageService = None,
    force_reload: bool = False,
) -> List[Dict[str, Any]]:
    """
    End-to-end query helper:
      1. Embed the query with MiniLM (SAME model as ingestion).
      2. Load stored vectors for client (cached after first call).
      3. Run cosine similarity search.
      4. Return top_k chunk records with a 'score' key.

    This replaces the old flow of: download raw → extract text → chunk → embed.
    """
    # 1. Embed query with MiniLM
    query_embedding: np.ndarray = _embedder.embed_query(query)

    # 2. Load stored vectors (cached)
    vector_records = load_vectors_for_client(
        client_id=client_id,
        blob_svc=blob_svc,
        force_reload=force_reload,
    )

    if not vector_records:
        logger.warning("No vectors found for client '%s'; returning empty results", client_id)
        return []

    # 3. Cosine similarity search
    results = cosine_similarity_search(
        query_embedding=query_embedding,
        vector_records=vector_records,
        top_k=top_k,
    )

    logger.info(
        "Retrieved %d chunks for client='%s' query='%.60s...'",
        len(results),
        client_id,
        query,
    )

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline entry points
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    source_path,
    source_type: str = "local",
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Full ingestion pipeline.

    Steps:
        1. Load documents from source_path
        2. Upload original files to blob (raw/<clientId>/)  [audit copy]
        3. Chunk all documents
        4. Generate MiniLM embeddings (batch)
        5. Group chunks by source document
        6. Serialise vector records → JSON
        7. Upload vector JSON files to blob (meta/<clientId>/vectors/)
        8. Invalidate vector cache for this client
        9. Save run-level metadata
    """
    try:
        extra_metadata = extra_metadata or {}
        client_id = extra_metadata.get("client_id")

        if not client_id:
            raise ValueError("client_id is required inside extra_metadata")

        client_id = client_id.strip().lower()

        if not source_path:
            raise ValueError("source_path is required")

        print("\n========== PIPELINE START ==========")
        print(f"Client ID     : {client_id}")
        print(f"Source Path   : {source_path}")
        print(f"Source Type   : {source_type}")
        print(f"Embedding Model: {_MINILM_MODEL}")
        print("====================================\n")

        paths = build_client_paths(client_id)
        client_meta_dir = paths["meta_dir"]

        # ── 1. Load documents ─────────────────────────────────────────────
        print("Loading documents...")
        raw_documents = _loader.load_from_directory(
            root=Path(source_path),
            source_type=source_type,
            extra_metadata=extra_metadata,
        )

        if not raw_documents:
            raise Exception(f"No supported documents found in: {source_path}")

        files_processed = len(raw_documents)
        print(f"Loaded {files_processed} document(s)")

        # ── 2. Upload original files to blob (raw/ prefix) ────────────────
        print("Uploading original files to Azure Blob (raw/)...")
        blob_paths = upload_original_files(
            raw_documents=raw_documents,
            client_id=client_id,
        )
        print(f"Raw files uploaded: {len(blob_paths)}")

        # ── 3. Chunk all documents ────────────────────────────────────────
        print("Chunking documents...")
        # Map  doc_id → list[Chunk]  so we can group per-file later
        chunks_by_doc: Dict[str, List[Chunk]] = {}
        all_chunks: List[Chunk] = []

        for raw_doc in raw_documents:
            doc_chunks = _chunker.chunk_document(raw_doc)
            chunks_by_doc[raw_doc.doc_id] = doc_chunks
            all_chunks.extend(doc_chunks)

        if not all_chunks:
            raise Exception("Chunk creation failed – no chunks produced")

        chunks_created = len(all_chunks)
        print(f"Chunks created: {chunks_created}")

        # ── 4. Generate MiniLM embeddings (single batched call) ───────────
        print(f"Generating embeddings with {_MINILM_MODEL}...")
        _embedder.embed_chunks(all_chunks, batch_size=64, show_progress=True)
        print(f"Embeddings generated: {chunks_created} vectors")

        # ── 5+6+7. Per-document: build vector records → upload JSON ───────
        print("Uploading vector JSON files to Azure Blob (meta/<clientId>/vectors/)...")
        vector_blob_paths: List[str] = []

        for raw_doc in raw_documents:
            doc_chunks = chunks_by_doc.get(raw_doc.doc_id, [])
            if not doc_chunks:
                logger.warning("No chunks for doc_id=%s; skipping vector upload", raw_doc.doc_id)
                continue

            vector_records = chunks_to_vector_records(doc_chunks)

            if not vector_records:
                logger.warning("No embedded records for doc_id=%s", raw_doc.doc_id)
                continue

            blob_name = upload_vectors_for_document(
                client_id=client_id,
                source_file_name=raw_doc.file_path.name,
                vector_records=vector_records,
            )

            if blob_name:
                vector_blob_paths.append(blob_name)
                print(f"  Vectors uploaded: {blob_name}  ({len(vector_records)} chunks)")

        print(f"Vector files uploaded: {len(vector_blob_paths)} / {files_processed}")

        # ── 8. Invalidate in-process vector cache for this client ─────────
        invalidate_vector_cache(client_id)
        print("Vector cache invalidated")

        # ── 9. Save run-level metadata ────────────────────────────────────
        print("Saving ingestion metadata...")
        metadata_path = save_metadata(
            client_id=client_id,
            label=extra_metadata.get("ingest_label", f"{client_id}-ingestion"),
            files_processed=files_processed,
            chunks_created=chunks_created,
            blob_paths=blob_paths,
            vector_blob_paths=vector_blob_paths,
            extra_metadata=extra_metadata,
            meta_dir=client_meta_dir,
        )
        print(f"Metadata saved: {metadata_path}")
        print("\n========== PIPELINE SUCCESS ==========\n")

        return {
            "run_timestamp":        datetime.utcnow().isoformat(),
            "documents_processed":  files_processed,
            "total_chunks":         chunks_created,
            "uploads_succeeded":    len(blob_paths),
            "uploads_failed":       max(0, files_processed - len(blob_paths)),
            "vector_files_uploaded": len(vector_blob_paths),
            "embedding_model":      _MINILM_MODEL,
            "elapsed_seconds":      0.0,
            "per_document":         [],
            "upload_errors":        [],
            "status":               "success",
            "message":              "Pipeline completed successfully",
            "client_id":            client_id,
            "metadata_file":        metadata_path,
        }

    except Exception as exc:
        print("\n========== PIPELINE FAILED ==========")
        print(str(exc))
        print(traceback.format_exc())
        print("=====================================\n")

        return {
            "error":                str(exc),
            "run_timestamp":        datetime.utcnow().isoformat(),
            "documents_processed":  0,
            "total_chunks":         0,
            "uploads_succeeded":    0,
            "uploads_failed":       0,
            "vector_files_uploaded": 0,
            "elapsed_seconds":      0.0,
            "per_document":         [],
            "upload_errors":        [],
            "status":               "failed",
            "message":              "Pipeline execution failed",
        }


def run_pipeline_single_file(
    file_path,
    source_type: str = "upload",
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience wrapper: ingest a single file through run_pipeline().
    Creates a temporary directory, copies the file in, and cleans up after.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return {"error": f"File not found: {str(file_path)}"}

    temp_dir = Config.TMP_DIR / f"single_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file = temp_dir / file_path.name

    try:
        temp_file.write_bytes(file_path.read_bytes())

        return run_pipeline(
            source_path=temp_dir,
            source_type=source_type,
            extra_metadata=extra_metadata,
        )

    except Exception as exc:
        return {"error": str(exc)}

    finally:
        try:
            if temp_file.exists():
                temp_file.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()
        except Exception:
            pass

def get_blob_svc() -> BlobStorageService:
    """Return the shared BlobStorageService singleton."""
    return blob_storage_service


def get_embedder() -> EmbeddingService:
    """Return the shared EmbeddingService singleton (MiniLM)."""
    return _embedder

def rebuild_index_for_doc_id(
    doc_id: str,
    client_id: str,
    source_file_name: str,
    blob_svc: BlobStorageService = None,
) -> Dict[str, Any]:
    """
    Re-download the vector JSON for a specific document, strip old embeddings,
    re-embed with the current MiniLM model, and re-upload.

    Args:
        doc_id:           The document identifier.
        client_id:        The client this document belongs to.
        source_file_name: Original source filename (used to locate the vector blob).
        blob_svc:         Optional BlobStorageService override.

    Returns:
        dict with 'doc_id' and 'chunks_rebuilt' on success, or 'error' key.
    """
    svc = blob_svc or blob_storage_service
    client_id = client_id.strip().lower()

    stem = Path(source_file_name).stem
    blob_name = f"meta/{client_id}/vectors/{stem}.json"

    try:
        if not svc.blob_exists(blob_name):
            return {"error": f"No vector file found in blob: '{blob_name}'"}

        raw_bytes = svc.download_bytes(blob_name)
        records: List[Dict[str, Any]] = json.loads(raw_bytes.decode("utf-8"))

        if not records:
            return {"error": f"Vector file is empty for doc_id='{doc_id}'"}

        # Extract plain texts and rebuild Chunk objects for embedding
        texts = [r["text"] for r in records]
        temp_chunks = [
            Chunk(
                doc_id=r.get("doc_id", doc_id),
                chunk_id=r.get("chunk_id", ""),
                chunk_index=r.get("chunk_index", i),
                text=r["text"],
                page=r.get("page", 1),
                source_file=r.get("source_file", source_file_name),
                source_type=r.get("source_type", "local"),
                uploaded_at=r.get("uploaded_at", ""),
                char_count=r.get("char_count", len(r["text"])),
            )
            for i, r in enumerate(records)
        ]

        # Re-embed with current model
        _embedder.embed_chunks(temp_chunks, batch_size=64, show_progress=False)
        new_records = chunks_to_vector_records(temp_chunks)
        json_bytes = json.dumps(new_records, ensure_ascii=False).encode("utf-8")
        result = svc.upload_bytes(
            data=json_bytes,
            blob_name=blob_name,
            content_type="application/json",
            overwrite=True,
        )

        if not (result and result.get("success")):
            return {"error": f"Re-upload failed: {result.get('error') if result else 'unknown'}"}
        invalidate_vector_cache(client_id)

        logger.info("Rebuilt index for doc_id=%s (%d chunks)", doc_id, len(new_records))
        return {"doc_id": doc_id, "chunks_rebuilt": len(new_records)}

    except Exception as exc:
        logger.exception("rebuild_index_for_doc_id failed for %s", doc_id)
        return {"error": str(exc)}
