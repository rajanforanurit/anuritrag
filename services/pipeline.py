from __future__ import annotations
import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from services.document_loader import load_documents
from services.chunker import chunk_documents
from services.embeddings import generate_embeddings
from services.blob_storage import (
    upload_file_to_blob,
    upload_file_to_blob_for_client,
)
from utils.helpers import make_doc_id
from config import Config
def ensure_directory(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def safe_write_json(path: str, data: Dict[str, Any]):
    ensure_directory(os.path.dirname(path))

    with open(path, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=2,
            ensure_ascii=False,
        )


def build_client_paths(client_id: str):
    client_id = client_id.strip().lower()

    base_faiss_dir = Config.BASE_DIR / "faiss"
    base_meta_dir = Config.BASE_DIR / "metadata"

    client_faiss_dir = base_faiss_dir / client_id
    client_meta_dir = base_meta_dir / client_id
    client_temp_dir = Config.TMP_DIR / client_id

    client_faiss_dir.mkdir(parents=True, exist_ok=True)
    client_meta_dir.mkdir(parents=True, exist_ok=True)
    client_temp_dir.mkdir(parents=True, exist_ok=True)

    return {
        "temp_dir": str(client_temp_dir),
        "faiss_dir": str(client_faiss_dir),
        "meta_dir": str(client_meta_dir),
        "faiss_dir_path": client_faiss_dir,
        "meta_dir_path": client_meta_dir,
    }


def upload_original_files(
    documents: List,
    client_id: str,
) -> List[str]:
    uploaded_blob_paths = []

    for doc in documents:
        try:
            source_path = (
                doc.metadata.get("source")
                if hasattr(doc, "metadata")
                else None
            )

            if not source_path:
                continue

            source_file = Path(source_path)

            if not source_file.exists():
                print(f"Skipped missing file: {source_path}")
                continue

            result = upload_file_to_blob_for_client(
                client_id=client_id,
                local_file_path=str(source_file),
                prefix="raw",
            )

            if result.get("success"):
                uploaded_blob_paths.append(
                    result.get("blob_name")
                )
                print(
                    f"Uploaded: {source_file.name} -> {result.get('blob_name')}"
                )
            else:
                print(
                    f"Upload failed for {source_file.name}: {result.get('error')}"
                )

        except Exception as exc:
            print(
                f"Blob upload failed for file: {str(exc)}"
            )

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
    timestamp = datetime.utcnow().strftime(
        "%Y%m%d_%H%M%S"
    )

    metadata_payload = {
        "client_id": client_id,
        "label": label,
        "files_processed": files_processed,
        "chunks_created": chunks_created,
        "blob_paths": blob_paths,
        "extra_metadata": extra_metadata or {},
        "created_at": datetime.utcnow().isoformat(),
        "status": "success",
    }

    metadata_path = os.path.join(
        meta_dir,
        f"ingestion_{timestamp}.json",
    )

    safe_write_json(
        metadata_path,
        metadata_payload,
    )

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
            print(
                f"Metadata uploaded to blob: {blob_name}"
            )
        else:
            print(
                f"Metadata blob upload failed: {upload_result.get('error')}"
            )

    except Exception as exc:
        print(
            f"Metadata upload error: {str(exc)}"
        )

    return metadata_path


def run_pipeline(
    source_path,
    source_type: str = "local",
    extra_metadata: Optional[Dict[str, Any]] = None,
):
    try:
        extra_metadata = extra_metadata or {}

        client_id = extra_metadata.get("client_id")

        if not client_id:
            raise ValueError(
                "client_id is required inside extra_metadata"
            )

        client_id = client_id.strip().lower()

        if not source_path:
            raise ValueError(
                "source_path is required"
            )

        print("\n========== PIPELINE START ==========")
        print(f"Client ID   : {client_id}")
        print(f"Source Path : {source_path}")
        print(f"Source Type : {source_type}")
        print("====================================\n")

        paths = build_client_paths(client_id)

        client_faiss_dir = paths["faiss_dir"]
        client_meta_dir = paths["meta_dir"]

        print("Loading documents...")

        documents = load_documents(str(source_path))

        if not documents:
            raise Exception(
                f"No supported documents found in: {source_path}"
            )

        unique_files = list(
            set(
                [
                    doc.metadata.get("source", "")
                    for doc in documents
                    if hasattr(doc, "metadata")
                ]
            )
        )

        files_processed = len(unique_files)

        print(
            f"Loaded documents: {files_processed}"
        )

        print(
            "Uploading original files to Azure Blob..."
        )

        blob_paths = upload_original_files(
            documents=documents,
            client_id=client_id,
        )

        print(
            f"Uploaded files to blob: {len(blob_paths)}"
        )

        print("Creating chunks...")

        chunks = chunk_documents(documents)

        if not chunks:
            raise Exception(
                "Chunk creation failed"
            )

        chunks_created = len(chunks)

        print(
            f"Chunks created: {chunks_created}"
        )

        print(
            "Generating embeddings and FAISS..."
        )

        generate_embeddings(
            chunks=chunks,
            save_path=client_faiss_dir,
        )

        print(
            f"FAISS saved to: {client_faiss_dir}"
        )

        print("Saving metadata...")

        metadata_path = save_metadata(
            client_id=client_id,
            label=extra_metadata.get(
                "ingest_label",
                f"{client_id}-ingestion",
            ),
            files_processed=files_processed,
            chunks_created=chunks_created,
            blob_paths=blob_paths,
            extra_metadata=extra_metadata,
            meta_dir=client_meta_dir,
        )

        print(
            f"Metadata saved: {metadata_path}"
        )

        print(
            "\n========== PIPELINE SUCCESS ==========\n"
        )

        return {
            "run_timestamp": datetime.utcnow().isoformat(),
            "documents_processed": files_processed,
            "total_chunks": chunks_created,
            "uploads_succeeded": len(blob_paths),
            "uploads_failed": max(
                0,
                files_processed - len(blob_paths),
            ),
            "elapsed_seconds": 0.0,
            "per_document": [],
            "upload_errors": [],
            "status": "success",
            "message": "Pipeline completed successfully",
            "client_id": client_id,
            "faiss_path": client_faiss_dir,
            "metadata_file": metadata_path,
        }

    except Exception as exc:
        print(
            "\n========== PIPELINE FAILED =========="
        )
        print(str(exc))
        print(traceback.format_exc())
        print(
            "=====================================\n"
        )

        return {
            "error": str(exc),
            "run_timestamp": datetime.utcnow().isoformat(),
            "documents_processed": 0,
            "total_chunks": 0,
            "uploads_succeeded": 0,
            "uploads_failed": 0,
            "elapsed_seconds": 0.0,
            "per_document": [],
            "upload_errors": [],
            "status": "failed",
            "message": "Pipeline execution failed",
        }


def run_pipeline_single_file(
    file_path,
    source_type: str = "upload",
    extra_metadata: Optional[Dict[str, Any]] = None,
):
    file_path = Path(file_path)

    if not file_path.exists():
        return {
            "error": f"File not found: {str(file_path)}"
        }

    temp_dir = Config.TMP_DIR / f"single_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    temp_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = temp_dir / file_path.name

    try:
        temp_file.write_bytes(
            file_path.read_bytes()
        )

        return run_pipeline(
            source_path=temp_dir,
            source_type=source_type,
            extra_metadata=extra_metadata,
        )

    except Exception as exc:
        return {
            "error": str(exc)
        }

    finally:
        try:
            if temp_file.exists():
                temp_file.unlink()

            if temp_dir.exists():
                temp_dir.rmdir()

        except Exception:
            pass
