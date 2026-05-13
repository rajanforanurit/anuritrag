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
from services.azure_search import upload_chunks, delete_chunks_by_doc
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
    base_meta_dir   = Config.BASE_DIR / "metadata"
    client_meta_dir = base_meta_dir  / client_id
    client_temp_dir = Config.TMP_DIR  / client_id
    client_meta_dir.mkdir(parents=True, exist_ok=True)
    client_temp_dir.mkdir(parents=True, exist_ok=True)
    return {
        "temp_dir":      str(client_temp_dir),
        "meta_dir":      str(client_meta_dir),
        "meta_dir_path": client_meta_dir,
    }


def upload_original_files(raw_documents: List, client_id: str) -> List[str]:
    uploaded_blob_paths = []
    for doc in raw_documents:
        try:
            source_file = Path(doc.file_path)
            if not source_file.exists():
                print(f"Skipped missing file: {source_file}")
                continue
            result = upload_file_to_blob_for_client(
                client_id=client_id,
                local_file_path=str(source_file),
                prefix="raw",
            )
            if result.get("success"):
                uploaded_blob_paths.append(result.get("blob_name"))
                print(f"  Raw uploaded: {source_file.name}")
            else:
                print(f"  Raw upload failed for {source_file.name}: {result.get('error')}")
        except Exception as exc:
            print(f"  Blob upload error: {str(exc)}")
    return uploaded_blob_paths


def save_metadata(client_id, label, files_processed, chunks_created, blob_paths, extra_metadata, meta_dir):
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
        "search_index":    os.getenv("AZURE_SEARCH_INDEX", "rag-chunks"),
    }
    metadata_path = os.path.join(meta_dir, f"ingestion_{timestamp}.json")
    safe_write_json(metadata_path, metadata_payload)
    try:
        blob_name = f"{Config.BLOB_META_PREFIX}{client_id}/ingestion_{timestamp}.json"
        upload_result = upload_file_to_blob(local_file_path=metadata_path, blob_name=blob_name)
        if upload_result.get("success"):
            print(f"  Metadata blob: {blob_name}")
        else:
            print(f"  Metadata upload failed: {upload_result.get('error')}")
    except Exception as exc:
        print(f"  Metadata upload error: {str(exc)}")
    return metadata_path


def run_pipeline(source_path, source_type: str = "local", extra_metadata: Optional[Dict[str, Any]] = None):
    try:
        extra_metadata = extra_metadata or {}
        client_id = extra_metadata.get("client_id")
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

        paths = build_client_paths(client_id)
        client_meta_dir = paths["meta_dir"]

        # 1. Load
        print("Step 1/5 — Loading documents...")
        raw_documents = _loader.load_from_directory(
            root=Path(source_path),
            source_type=source_type,
            extra_metadata=extra_metadata,
        )
        if not raw_documents:
            raise Exception(f"No supported documents found in: {source_path}")
        files_processed = len(raw_documents)
        print(f"  Loaded {files_processed} document(s)")

        # 2. Upload raw files
        print("Step 2/5 — Uploading raw files to Azure Blob...")
        blob_paths = upload_original_files(raw_documents=raw_documents, client_id=client_id)
        print(f"  Uploaded {len(blob_paths)} raw file(s)")

        # 3. Chunk (delete old search docs first)
        print("Step 3/5 — Chunking documents...")
        all_chunks = []
        for raw_doc in raw_documents:
            try:
                delete_chunks_by_doc(doc_id=raw_doc.doc_id, client_id=client_id)
            except Exception as del_err:
                logger.warning("Could not delete old chunks for %s: %s", raw_doc.doc_id, del_err)
            chunks = _chunker.chunk_document(raw_doc)
            all_chunks.extend(chunks)
        if not all_chunks:
            raise Exception("Chunk creation failed — no chunks produced")
        chunks_created = len(all_chunks)
        print(f"  Created {chunks_created} chunk(s)")

        # 4. Embed
        print("Step 4/5 — Generating embeddings...")
        _embedder.embed_chunks(all_chunks)
        print(f"  Embedded {chunks_created} chunk(s)")

        # 5. Upload to Azure AI Search
        print("Step 5/5 — Uploading to Azure AI Search...")
        search_result = upload_chunks(all_chunks, client_id=client_id)
        print(f"  Search uploaded: {search_result['uploaded']}  failed: {search_result['failed']}")

        # Save metadata
        metadata_path = save_metadata(
            client_id=client_id,
            label=extra_metadata.get("ingest_label", f"{client_id}-ingestion"),
            files_processed=files_processed,
            chunks_created=chunks_created,
            blob_paths=blob_paths,
            extra_metadata=extra_metadata,
            meta_dir=client_meta_dir,
        )

        print("\n========== PIPELINE SUCCESS ==========\n")

        return {
            "run_timestamp":       datetime.utcnow().isoformat(),
            "documents_processed": files_processed,
            "total_chunks":        chunks_created,
            "search_uploaded":     search_result["uploaded"],
            "search_failed":       search_result["failed"],
            "uploads_succeeded":   len(blob_paths),
            "uploads_failed":      max(0, files_processed - len(blob_paths)),
            "elapsed_seconds":     0.0,
            "per_document":        [],
            "upload_errors":       [],
            "status":              "success",
            "message":             "Pipeline completed successfully",
            "client_id":           client_id,
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


def run_pipeline_single_file(file_path, source_type: str = "upload", extra_metadata: Optional[Dict[str, Any]] = None):
    file_path = Path(file_path)
    if not file_path.exists():
        return {"error": f"File not found: {str(file_path)}"}
    temp_dir = Config.TMP_DIR / f"single_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file = temp_dir / file_path.name
    try:
        temp_file.write_bytes(file_path.read_bytes())
        return run_pipeline(source_path=temp_dir, source_type=source_type, extra_metadata=extra_metadata)
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        try:
            if temp_file.exists(): temp_file.unlink()
            if temp_dir.exists(): temp_dir.rmdir()
        except Exception:
            pass


def get_blob_svc() -> BlobStorageService:
    return blob_storage_service


def get_embedder() -> EmbeddingService:
    return _embedder


def rebuild_index_for_doc_id(doc_id: str, blob_svc: BlobStorageService = None) -> dict:
    from services.chunking import Chunk
    blob = blob_svc or blob_storage_service
    try:
        chunk_blob_name = Config.BLOB_CHUNKS_PREFIX + f"{doc_id}_chunks.jsonl"
        if not blob.blob_exists(chunk_blob_name):
            return {"error": f"No chunks found in blob for doc_id '{doc_id}'"}
        raw_bytes = blob.download_bytes(chunk_blob_name)
        lines = raw_bytes.decode("utf-8").strip().splitlines()
        chunks: list[Chunk] = []
        client_id = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if not client_id:
                client_id = data.get("extra_metadata", {}).get("client_id", "")
            chunks.append(Chunk(
                doc_id        = data.get("doc_id", doc_id),
                chunk_id      = data.get("chunk_id", ""),
                chunk_index   = data.get("chunk_index", 0),
                text          = data.get("text", ""),
                page          = data.get("page", 1),
                source_file   = data.get("source_file", ""),
                source_type   = data.get("source_type", "local"),
                uploaded_at   = data.get("uploaded_at", ""),
                char_count    = data.get("char_count", 0),
                extra_metadata= data.get("extra_metadata", {}),
            ))
        if not chunks:
            return {"error": f"No chunks could be parsed for doc_id '{doc_id}'"}
        _embedder.embed_chunks(chunks)
        if client_id:
            delete_chunks_by_doc(doc_id=doc_id, client_id=client_id)
            result = upload_chunks(chunks, client_id=client_id)
        else:
            result = {"uploaded": 0, "failed": len(chunks)}
        logger.info("Rebuilt index for doc_id=%s (%d chunks)", doc_id, len(chunks))
        return {"doc_id": doc_id, "chunks_rebuilt": len(chunks), "search_uploaded": result.get("uploaded", 0)}
    except Exception as exc:
        logger.exception("rebuild_index_for_doc_id failed for %s", doc_id)
        return {"error": str(exc)}
