"""
api/routers/ingest.py — Ingestion endpoints for all source types.

Endpoints
─────────
POST /ingest/local-directory   Process all files from a local directory
POST /ingest/upload-file       Upload + process a single file (multipart)
POST /ingest/google-drive      Fetch + process all files from a Google Drive folder
POST /ingest/sharepoint        Fetch + process files from SharePoint (requires MSAL config)
POST /scan-directory           Preview directory contents without ingesting
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from api.middleware.auth import require_api_key
from api.schemas import (
    FileScanItem,
    IngestDirectoryRequest,
    IngestDirectoryResponse,
    IngestGoogleDriveRequest,
    IngestGoogleDriveResponse,
    IngestSharePointRequest,
    IngestSharePointResponse,
    PipelineSummary,
    ScanDirectoryRequest,
    ScanDirectoryResponse,
    UploadFileResponse,
)
from config import Config
from services.document_loader import DocumentLoader
from services.pipeline import run_pipeline, run_pipeline_single_file
from utils.helpers import make_doc_id

logger  = logging.getLogger(__name__)
router  = APIRouter(prefix="/ingest", tags=["Ingestion"])
_loader = DocumentLoader()


# ── POST /ingest/local-directory ───────────────────────────────────────────────

@router.post(
    "/local-directory",
    response_model=IngestDirectoryResponse,
    summary="Ingest all documents from a local directory",
    description=(
        "Recursively scans the given directory for all supported file types "
        "(PDF, PPTX, DOCX, TXT, XLSX, CSV, JSON, HTML, MD, RTF). "
        "Extracts text, splits into chunks, generates embeddings, "
        "and uploads everything to Azure Blob Storage (`vectordbforrag` container)."
    ),
)
async def ingest_local_directory(
    body: IngestDirectoryRequest,
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())
    logger.info("[%s] ingest/local-directory — path=%s", request_id, body.directory_path)

    path, err = Config.resolve_local_path(body.directory_path)
    if err:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=err)

    scan_rows = _loader.scan_directory(path)
    if not scan_rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No supported files found in '{body.directory_path}'. "
                f"Supported types: {sorted(Config.SUPPORTED_EXTENSIONS)}"
            ),
        )

    extra_meta = dict(body.extra_metadata or {})
    if body.label:
        extra_meta["ingest_label"] = body.label
    extra_meta["request_id"] = request_id

    summary = run_pipeline(
        source_path=path,
        source_type="local",
        extra_metadata=extra_meta,
    )

    if "error" in summary:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=summary["error"],
        )

    summary["status"] = "partial" if summary.get("uploads_failed", 0) else "success"
    return IngestDirectoryResponse(
        request_id=request_id,
        directory_path=str(path),
        files_found=len(scan_rows),
        summary=PipelineSummary(**summary),
    )


# ── POST /ingest/upload-file ───────────────────────────────────────────────────

@router.post(
    "/upload-file",
    response_model=UploadFileResponse,
    summary="Upload and ingest a single file",
    description=(
        "Accepts a multipart file upload (PDF, PPTX, DOCX, TXT, XLSX, CSV, etc.). "
        "Runs the full ingestion pipeline and stores results in Azure Blob Storage."
    ),
)
async def upload_and_ingest_file(
    file:  UploadFile = File(..., description="The document to ingest"),
    label: Optional[str] = Form(None, description="Human-readable label"),
    _key:  str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())
    logger.info("[%s] ingest/upload-file — filename=%s", request_id, file.filename)

    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in Config.SUPPORTED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=(
                f"Unsupported file type '{suffix}'. "
                f"Supported: {sorted(Config.SUPPORTED_EXTENSIONS)}"
            ),
        )

    content = await file.read()
    size_bytes = len(content)
    max_bytes  = Config.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    if size_bytes > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"File size {size_bytes / 1024 / 1024:.1f} MB exceeds "
                f"limit of {Config.MAX_UPLOAD_SIZE_MB} MB."
            ),
        )

    Config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = Config.TMP_DIR / f"{request_id}{suffix}"
    try:
        tmp_path.write_bytes(content)
        doc_id = make_doc_id(tmp_path.with_stem(Path(file.filename).stem))

        extra_meta: dict = {"request_id": request_id, "original_filename": file.filename}
        if label:
            extra_meta["ingest_label"] = label

        summary = run_pipeline_single_file(
            file_path=tmp_path,
            source_type="upload",
            extra_metadata=extra_meta,
        )
    finally:
        tmp_path.unlink(missing_ok=True)

    if "error" in summary:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=summary["error"],
        )

    summary["status"] = "partial" if summary.get("uploads_failed", 0) else "success"
    return UploadFileResponse(
        request_id=request_id,
        filename=file.filename or "",
        size_bytes=size_bytes,
        doc_id=doc_id,
        summary=PipelineSummary(**summary),
    )


# ── POST /ingest/google-drive ──────────────────────────────────────────────────

@router.post(
    "/google-drive",
    response_model=IngestGoogleDriveResponse,
    summary="Ingest all documents from a Google Drive folder",
    description=(
        "Fetches all supported files from a Google Drive folder (by folder ID), "
        "downloads them to a temporary directory, then runs the full ingestion pipeline. "
        "Requires `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_CREDENTIALS_FILE` in .env. "
        "The folder ID is the last part of the Drive folder URL: "
        "`https://drive.google.com/drive/folders/<FOLDER_ID>`"
    ),
)
async def ingest_google_drive(
    body: IngestGoogleDriveRequest,
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())
    logger.info("[%s] ingest/google-drive — folder_id=%s", request_id, body.folder_id)

    # Validate credentials are configured
    if not Config.GOOGLE_SERVICE_ACCOUNT_JSON and not Config.GOOGLE_CREDENTIALS_FILE:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Google Drive credentials are not configured. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON (service account JSON content) "
                "or GOOGLE_CREDENTIALS_FILE (path to credentials JSON) in .env."
            ),
        )

    from services.google_drive_loader import GoogleDriveLoader

    extra_meta = dict(body.extra_metadata or {})
    if body.label:
        extra_meta["ingest_label"] = body.label
    extra_meta["request_id"]    = request_id
    extra_meta["gdrive_folder"] = body.folder_id

    try:
        loader   = GoogleDriveLoader()
        tmp_dir  = Config.TMP_DIR / f"gdrive_{request_id}"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        logger.info("[%s] Downloading files from Google Drive folder: %s", request_id, body.folder_id)
        downloaded = loader.download_folder(
            folder_id=body.folder_id,
            dest_dir=tmp_dir,
            recursive=body.recursive,
        )

        if not downloaded:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"No supported files found in Google Drive folder '{body.folder_id}'. "
                    f"Supported types: {sorted(Config.SUPPORTED_EXTENSIONS)}"
                ),
            )

        logger.info("[%s] Downloaded %d file(s) — starting pipeline", request_id, len(downloaded))

        summary = run_pipeline(
            source_path=tmp_dir,
            source_type="google_drive",
            extra_metadata=extra_meta,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[%s] Google Drive ingestion failed", request_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Drive ingestion failed: {exc}",
        )
    finally:
        # Clean up temp downloads
        import shutil
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if "error" in summary:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=summary["error"],
        )

    summary["status"] = "partial" if summary.get("uploads_failed", 0) else "success"
    return IngestGoogleDriveResponse(
        request_id=request_id,
        folder_id=body.folder_id,
        files_found=len(downloaded),
        summary=PipelineSummary(**summary),
    )


# ── POST /ingest/sharepoint ────────────────────────────────────────────────────

@router.post(
    "/sharepoint",
    response_model=IngestSharePointResponse,
    summary="Ingest documents from a SharePoint folder",
    description=(
        "Fetches all supported files from a SharePoint document library folder, "
        "downloads them, then runs the full ingestion pipeline. "
        "Requires SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, "
        "SHAREPOINT_CLIENT_SECRET in .env (Azure App Registration with Sites.Read.All)."
    ),
)
async def ingest_sharepoint(
    body: IngestSharePointRequest,
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())
    logger.info(
        "[%s] ingest/sharepoint — site=%s folder=%s",
        request_id, body.site_url, body.folder_path,
    )

    # Check credentials
    missing: list[str] = []
    if not Config.SHAREPOINT_TENANT_ID:
        missing.append("SHAREPOINT_TENANT_ID")
    if not Config.SHAREPOINT_CLIENT_ID:
        missing.append("SHAREPOINT_CLIENT_ID")
    if not Config.SHAREPOINT_CLIENT_SECRET:
        missing.append("SHAREPOINT_CLIENT_SECRET")

    if missing:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"SharePoint credentials not configured. "
                f"Missing env vars: {', '.join(missing)}. "
                "Create an Azure App Registration with Sites.Read.All permission "
                "and set these in .env."
            ),
        )

    from services.sharepoint_loader import SharePointLoader

    extra_meta = dict(body.extra_metadata or {})
    if body.label:
        extra_meta["ingest_label"] = body.label
    extra_meta["request_id"]       = request_id
    extra_meta["sharepoint_site"]  = body.site_url
    extra_meta["sharepoint_folder"] = body.folder_path

    try:
        loader  = SharePointLoader()
        tmp_dir = Config.TMP_DIR / f"sharepoint_{request_id}"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        logger.info("[%s] Downloading from SharePoint: %s / %s", request_id, body.site_url, body.folder_path)
        downloaded = loader.download_folder(
            site_url=body.site_url,
            folder_path=body.folder_path,
            dest_dir=tmp_dir,
        )

        if not downloaded:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"No supported files found at SharePoint path '{body.folder_path}'. "
                    f"Supported types: {sorted(Config.SUPPORTED_EXTENSIONS)}"
                ),
            )

        logger.info("[%s] Downloaded %d file(s) — starting pipeline", request_id, len(downloaded))

        summary = run_pipeline(
            source_path=tmp_dir,
            source_type="sharepoint",
            extra_metadata=extra_meta,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[%s] SharePoint ingestion failed", request_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"SharePoint ingestion failed: {exc}",
        )
    finally:
        import shutil
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if "error" in summary:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=summary["error"],
        )

    summary["status"] = "partial" if summary.get("uploads_failed", 0) else "success"
    return IngestSharePointResponse(
        request_id=request_id,
        site_url=body.site_url,
        folder_path=body.folder_path,
        files_found=len(downloaded),
        summary=PipelineSummary(**summary),
    )


# ── POST /scan-directory ───────────────────────────────────────────────────────

@router.post(
    "/scan-directory",
    response_model=ScanDirectoryResponse,
    tags=["Utilities"],
    summary="Preview directory contents without ingesting",
    description=(
        "Scans a local directory and returns all supported files that would be "
        "processed by /ingest/local-directory. No files are modified."
    ),
)
async def scan_directory(
    body: ScanDirectoryRequest,
    _key: str = Depends(require_api_key),
):
    path, err = Config.resolve_local_path(body.directory_path)
    if err:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=err)

    rows = _loader.scan_directory(path)
    return ScanDirectoryResponse(
        directory_path=str(path),
        files_found=len(rows),
        files=[
            FileScanItem(
                filename=r["File"],
                file_type=r["Type"],
                size_kb=r["Size (KB)"],
                subfolder=r["Subfolder"],
            )
            for r in rows
        ],
        supported_extensions=sorted(Config.SUPPORTED_EXTENSIONS),
    )
