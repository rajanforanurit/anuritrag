from __future__ import annotations
import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from services.document_loader import DocumentLoader
from services.chunking import Chunker
from services.embedding import EmbeddingService
from services.blob_storage import (
    BlobStorageService,
    blob_storage_service,
    upload_file_to_blob,
    upload_file_to_blob_for_client,
)
from utils.helpers import make_doc_id
from config import Config

import logging
logger = logging.getLogger(__name__)

_loader   = DocumentLoader()
_chunker  = Chunker(
    chunk_size=Config.CHUNK_SIZE,
    chunk_overlap=Config.CHUNK_OVERLAP,
)
_embedder = EmbeddingService(model_name=Config.EMBEDDING_MODEL)


def ensure_directory(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def safe_write_json(path: str, data: Dict[str, Any]):
    ensure_directory(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def build_client_paths(client_id: str):
    client_id = client_id.strip().lower()

    base_faiss_dir = Config.BASE_DIR / "faiss"
    base_meta_dir  = Config.BASE_DIR / "metadata"

    client_faiss_dir = base_faiss_dir / client_id
    client_meta_dir  = base_meta_dir  / client_id
    client_temp_dir  = Config.TMP_DIR  / client_id

    client_faiss_dir.mkdir(parents=True, exist_ok=True)
    client_meta_dir.mkdir(parents=True,  exist_ok=True)
    client_temp_dir.mkdir(parents=True,  exist_ok=True)

    return {
        "temp_dir":       str(client_temp_dir),
        "faiss_dir":      str(client_faiss_dir),
        "meta_dir":       str(client_meta_dir),
        "faiss_dir_path": client_faiss_dir,
        "meta_dir_path":  client_meta_dir,
    }


def upload_original_files(
    raw_documents: List,
    client_id: str,
) -> List[str]:
    uploaded_blob_paths = []

    for doc in raw_documents:
        try:
            source_file = doc.file_path  # RawDocument.file_path is a Path

            if not source_file or not Path(source_file).exists():
                print(f"Skipped missing file: {source_file}")
                continue

            source_file = Path(source_file)

            result = upload_file_to_blob_for_client(
                client_id=client_id,
                local_file_path=str(source_file),
                prefix="raw",
            )

            if result.get("success"):
                uploaded_blob_paths.append(result.get("blob_name"))
                print(f"Uploaded: {source_file.name} -> {result.get('blob_name')}")
            else:
                print(f"Upload failed for {source_file.name}: {result.get('error')}")

        except Exception as exc:
            print(f"Blob upload failed for file: {str(exc)}")

    return uploaded_blob_paths


def save_metadata(
    client_id: str,
    label: str,
    files_processed: int,
    chunks_created: int,
    blob_paths: List[str],
    extra_metadata: Dict[str, Any],
    meta_dir: str,
):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    metadata_payload = {
        "client_id":       client_id,
        "label":           label,
        "files_processed": files_processed,
        "chunks_created":  chunks_created,
        "blob_paths":      blob_paths,
        "extra_metadata":  extra_metadata or {},
        "created_at":      datetime.utcnow().isoformat(),
        "status":          "success",
    }

    metadata_path = os.path.join(meta_dir, f"ingestion_{timestamp}.json")
    safe_write_json(metadata_path, metadata_payload)

    try:
        blob_name = (
            f"{Config.BLOB_META_PREFIX}"
            f"{client_id}/"
            f"ingestion_{timestamp}.json"
        )
        upload_result = upload_file_to_blob(
            local_file_path=metadata_path,
            blob_name=blob_name,
        )
        if upload_result.get("success"):
            print(f"Metadata uploaded to blob: {blob_name}")
        else:
            print(f"Metadata blob upload failed: {upload_result.get('error')}")
    except Exception as exc:
        print(f"Metadata upload error: {str(exc)}")

    return metadata_path


def run_pipeline(
    source_path,
    source_type: str = "local",
    extra_metadata: Optional[Dict[str, Any]] = None,
):
    try:
        extra_metadata = extra_metadata or {}
        client_id      = extra_metadata.get("client_id")

        if not client_id:
            raise ValueError("client_id is required inside extra_metadata")

        client_id = client_id.strip().lower()

        if not source_path:
            raise ValueError("source_path is required")

        print("\n========== PIPELINE START ==========")
        print(f"Client ID   : {client_id}")
        print(f"Source Path : {source_path}")
        print(f"Source Type : {source_type}")
        print("====================================\n")

        paths            = build_client_paths(client_id)
        client_faiss_dir = paths["faiss_dir"]
        client_meta_dir  = paths["meta_dir"]

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
        print(f"Loaded documents: {files_processed}")

        # ── 2. Upload original files to blob (client folder) ──────────────
        print("Uploading original files to Azure Blob...")
        blob_paths = upload_original_files(
            raw_documents=raw_documents,
            client_id=client_id,
        )
        print(f"Uploaded files to blob: {len(blob_paths)}")

        # ── 3. Chunk documents ────────────────────────────────────────────
        print("Creating chunks...")
        all_chunks = []
        for raw_doc in raw_documents:
            chunks = _chunker.chunk_document(raw_doc)
            all_chunks.extend(chunks)

        if not all_chunks:
            raise Exception("Chunk creation failed — no chunks produced")

        chunks_created = len(all_chunks)
        print(f"Chunks created: {chunks_created}")

        # ── 4. Embed + build FAISS index ──────────────────────────────────
        print("Generating embeddings and building FAISS index...")
        vectors = _embedder.embed_chunks(all_chunks)
        index   = _embedder.build_faiss_index(vectors)

        faiss_index_path = Path(client_faiss_dir) / "index.faiss"
        _embedder.save_faiss_index(index, faiss_index_path)
        print(f"FAISS index saved to: {faiss_index_path}")

        # ── 5. Save chunk metadata as JSON ────────────────────────────────
        chunks_json_path = Path(client_faiss_dir) / "chunks.json"
        with open(chunks_json_path, "w", encoding="utf-8") as f:
            json.dump(
                [c.to_dict() for c in all_chunks],
                f,
                indent=2,
                ensure_ascii=False,
            )
        print(f"Chunk metadata saved to: {chunks_json_path}")

        # ── 6. Save ingestion metadata ────────────────────────────────────
        print("Saving ingestion metadata...")
        metadata_path = save_metadata(
            client_id=client_id,
            label=extra_metadata.get("ingest_label", f"{client_id}-ingestion"),
            files_processed=files_processed,
            chunks_created=chunks_created,
            blob_paths=blob_paths,
            extra_metadata=extra_metadata,
            meta_dir=client_meta_dir,
        )
        print(f"Metadata saved: {metadata_path}")
        print("\n========== PIPELINE SUCCESS ==========\n")

        return {
            "run_timestamp":       datetime.utcnow().isoformat(),
            "documents_processed": files_processed,
            "total_chunks":        chunks_created,
            "uploads_succeeded":   len(blob_paths),
            "uploads_failed":      max(0, files_processed - len(blob_paths)),
            "elapsed_seconds":     0.0,
            "per_document":        [],
            "upload_errors":       [],
            "status":              "success",
            "message":             "Pipeline completed successfully",
            "client_id":           client_id,
            "faiss_path":          str(faiss_index_path),
            "metadata_file":       metadata_path,
        }

    except Exception as exc:
        print("\n========== PIPELINE FAILED ==========")
        print(str(exc))
        print(traceback.format_exc())
        print("=====================================\n")

        return {
            "error":               str(exc),
            "run_timestamp":       datetime.utcnow().isoformat(),
            "documents_processed": 0,
            "total_chunks":        0,
            "uploads_succeeded":   0,
            "uploads_failed":      0,
            "elapsed_seconds":     0.0,
            "per_document":        [],
            "upload_errors":       [],
            "status":              "failed",
            "message":             "Pipeline execution failed",
        }


def run_pipeline_single_file(
    file_path,
    source_type: str = "upload",
    extra_metadata: Optional[Dict[str, Any]] = None,
):
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


# ── Service accessors (used by storage.py router) ─────────────────────────────

def get_blob_svc() -> BlobStorageService:
    """Return the shared BlobStorageService singleton."""
    return blob_storage_service


def get_embedder() -> EmbeddingService:
    """Return the shared EmbeddingService singleton."""
    return _embedder


def rebuild_index_for_doc_id(
    doc_id: str,
    blob_svc: BlobStorageService = None,
) -> dict:
    """
    Re-download chunks for a doc_id from Azure Blob, regenerate embeddings,
    and re-upload the updated chunk JSONL.
    """
    from services.chunking import Chunk

    blob = blob_svc or blob_storage_service

    try:
        chunk_blob_name = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"

        if not blob.blob_exists(chunk_blob_name):
            return {"error": f"No chunks found in blob for doc_id '{doc_id}'"}

        # Download existing chunks JSONL
        raw_bytes = blob.download_bytes(chunk_blob_name)
        lines     = raw_bytes.decode("utf-8").strip().splitlines()

        chunks: list[Chunk] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            chunks.append(Chunk(
                doc_id       = data.get("doc_id", doc_id),
                chunk_id     = data.get("chunk_id", ""),
                chunk_index  = data.get("chunk_index", 0),
                text         = data.get("text", ""),
                page         = data.get("page", 1),
                source_file  = data.get("source_file", ""),
                source_type  = data.get("source_type", "local"),
                uploaded_at  = data.get("uploaded_at", ""),
                char_count   = data.get("char_count", 0),
                extra_metadata=data.get("extra_metadata", {}),
            ))

        if not chunks:
            return {"error": f"No chunks could be parsed for doc_id '{doc_id}'"}

        # Re-embed
        vectors = _embedder.embed_chunks(chunks)

        # Re-upload chunks JSONL (embeddings are attached to chunk objects)
        updated_jsonl = "\n".join(
            json.dumps(c.to_dict(), ensure_ascii=False) for c in chunks
        ).encode("utf-8")

        blob.upload_bytes(
            data         = updated_jsonl,
            blob_name    = chunk_blob_name,
            content_type = "application/jsonl",
            overwrite    = True,
        )

        logger.info("Rebuilt index for doc_id=%s (%d chunks)", doc_id, len(chunks))
        return {"doc_id": doc_id, "chunks_rebuilt": len(chunks)}

    except Exception as exc:
        logger.exception("rebuild_index_for_doc_id failed for %s", doc_id)
        return {"error": str(exc)}
