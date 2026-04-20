import os
import json
import traceback
from datetime import datetime
from typing import Dict, Any, List

from services.document_loader import load_documents
from services.chunker import chunk_documents
from services.embeddings import generate_embeddings
from services.blob_storage import upload_file_to_blob
from config import (
    TEMP_DIR,
    FAISS_DIR,
)


def ensure_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def safe_write_json(path: str, data: Dict[str, Any]):
    ensure_directory(os.path.dirname(path))

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def build_client_paths(client_id: str):
    """
    Create isolated storage paths for each client.

    Example:
        faiss/acme-corp/
        temp/acme-corp/
        metadata/acme-corp/
    """

    client_id = client_id.strip().lower()

    client_temp_dir = os.path.join(TEMP_DIR, client_id)
    client_faiss_dir = os.path.join(FAISS_DIR, client_id)
    client_meta_dir = os.path.join("metadata", client_id)

    ensure_directory(client_temp_dir)
    ensure_directory(client_faiss_dir)
    ensure_directory(client_meta_dir)

    return {
        "temp_dir": client_temp_dir,
        "faiss_dir": client_faiss_dir,
        "meta_dir": client_meta_dir,
    }


def upload_original_files(
    documents: List,
    client_id: str,
) -> List[str]:
    uploaded_blob_paths = []

    for doc in documents:
        try:
            source_path = doc.metadata.get("source")

            if not source_path:
                continue

            filename = os.path.basename(source_path)

            blob_name = f"raw/{client_id}/{filename}"

            upload_file_to_blob(
                local_file_path=source_path,
                blob_name=blob_name,
            )

            uploaded_blob_paths.append(blob_name)

        except Exception as e:
            print(f"Blob upload failed for file: {e}")

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
    """
    Save metadata per client.

    Example:
        metadata/acme-corp/ingestion_2026.json
    """

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    metadata_payload = {
        "client_id": client_id,
        "label": label,
        "files_processed": files_processed,
        "chunks_created": chunks_created,
        "blob_paths": blob_paths,
        "extra_metadata": extra_metadata or {},
        "created_at": datetime.utcnow().isoformat(),
    }

    metadata_path = os.path.join(
        meta_dir,
        f"ingestion_{timestamp}.json"
    )

    safe_write_json(metadata_path, metadata_payload)

    return metadata_path


def run_pipeline(
    client_id: str,
    directory_path: str,
    label: str = None,
    extra_metadata: Dict[str, Any] = None,
):

    try:
        if not client_id:
            raise ValueError("client_id is required")

        if not directory_path:
            raise ValueError("directory_path is required")

        client_id = client_id.strip().lower()

        print(f"\n========== PIPELINE START ==========")
        print(f"Client ID      : {client_id}")
        print(f"Directory Path : {directory_path}")
        print(f"Label          : {label}")
        print("====================================\n")

        paths = build_client_paths(client_id)

        client_temp_dir = paths["temp_dir"]
        client_faiss_dir = paths["faiss_dir"]
        client_meta_dir = paths["meta_dir"]

        # ------------------------------------------------------------------
        # Load documents
        # ------------------------------------------------------------------

        print("Loading documents...")

        documents = load_documents(directory_path)

        if not documents:
            raise Exception(
                f"No supported documents found in: {directory_path}"
            )

        files_processed = len(
            list(
                set(
                    [
                        doc.metadata.get("source", "")
                        for doc in documents
                    ]
                )
            )
        )

        print(f"Loaded documents: {files_processed}")

        # ------------------------------------------------------------------
        # Upload originals to Azure Blob
        # ------------------------------------------------------------------

        print("Uploading original files to Azure Blob...")

        blob_paths = upload_original_files(
            documents=documents,
            client_id=client_id,
        )

        print(f"Uploaded to blob: {len(blob_paths)}")

        # ------------------------------------------------------------------
        # Chunk documents
        # ------------------------------------------------------------------

        print("Creating chunks...")

        chunks = chunk_documents(documents)

        if not chunks:
            raise Exception("Chunk creation failed")

        chunks_created = len(chunks)

        print(f"Chunks created: {chunks_created}")

        # ------------------------------------------------------------------
        # Generate embeddings + FAISS
        # ------------------------------------------------------------------

        print("Generating embeddings + saving FAISS index...")

        generate_embeddings(
            chunks=chunks,
            save_path=client_faiss_dir,
        )

        print(f"FAISS saved to: {client_faiss_dir}")

        # ------------------------------------------------------------------
        # Save metadata
        # ------------------------------------------------------------------

        print("Saving metadata...")

        metadata_path = save_metadata(
            client_id=client_id,
            label=label or f"{client_id}-ingestion",
            files_processed=files_processed,
            chunks_created=chunks_created,
            blob_paths=blob_paths,
            extra_metadata=extra_metadata or {},
            meta_dir=client_meta_dir,
        )

        print(f"Metadata saved: {metadata_path}")

        print("\n========== PIPELINE SUCCESS ==========\n")

        return {
            "success": True,
            "message": "Client ingestion completed successfully",
            "client_id": client_id,
            "files_processed": files_processed,
            "chunks_created": chunks_created,
            "blob_paths": blob_paths,
            "metadata": {
                "faiss_path": client_faiss_dir,
                "metadata_file": metadata_path,
            },
            "error": None,
        }

    except Exception as e:
        print("\n========== PIPELINE FAILED ==========")
        print(str(e))
        print(traceback.format_exc())
        print("=====================================\n")

        return {
            "success": False,
            "message": "Pipeline execution failed",
            "client_id": client_id if client_id else None,
            "files_processed": 0,
            "chunks_created": 0,
            "blob_paths": [],
            "metadata": {},
            "error": str(e),
        }
