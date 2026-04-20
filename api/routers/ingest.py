from __future__ import annotations

import logging
import uuid
import threading
import shutil
from pathlib import Path
from typing import Optional

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    status,
)

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

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ingest", tags=["Ingestion"])
_loader = DocumentLoader()

_jobs: dict = {}
_jobs_lock = threading.Lock()


@router.post(
    "/local-directory",
    response_model=IngestDirectoryResponse,
)
async def ingest_local_directory(
    body: IngestDirectoryRequest,
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())

    logger.info(
        "[%s] local-directory | client_id=%s | path=%s",
        request_id,
        body.client_id,
        body.directory_path,
    )

    if not body.client_id or not body.client_id.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="client_id is required",
        )

    client_id = body.client_id.strip().lower()

    path, err = Config.resolve_local_path(body.directory_path)

    if err:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=err,
        )

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
    extra_meta["client_id"] = client_id
    extra_meta["request_id"] = request_id

    if body.label:
        extra_meta["ingest_label"] = body.label

    summary = run_pipeline(
        source_path=path,
        source_type="local",
        extra_metadata=extra_meta,
    )

    if "error" in summary:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=summary.get("error") or "Pipeline failed",
        )

    summary["status"] = (
        "partial"
        if summary.get("uploads_failed", 0)
        else "success"
    )

    return IngestDirectoryResponse(
        request_id=request_id,
        directory_path=str(path),
        files_found=len(scan_rows),
        summary=PipelineSummary(**summary),
    )


@router.post(
    "/upload-file",
    response_model=UploadFileResponse,
)
async def upload_and_ingest_file(
    client_id: str = Form(...),
    file: UploadFile = File(...),
    label: Optional[str] = Form(None),
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())

    logger.info(
        "[%s] upload-file | client_id=%s | filename=%s",
        request_id,
        client_id,
        file.filename,
    )

    if not client_id or not client_id.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="client_id is required",
        )

    client_id = client_id.strip().lower()

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
    max_bytes = Config.MAX_UPLOAD_SIZE_MB * 1024 * 1024

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

        doc_id = make_doc_id(
            tmp_path.with_stem(
                Path(file.filename).stem
            )
        )

        extra_meta = {
            "client_id": client_id,
            "request_id": request_id,
            "original_filename": file.filename,
        }

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
            detail=summary.get("error") or "Upload ingestion failed",
        )

    summary["status"] = (
        "partial"
        if summary.get("uploads_failed", 0)
        else "success"
    )

    return UploadFileResponse(
        request_id=request_id,
        filename=file.filename or "",
        size_bytes=size_bytes,
        doc_id=doc_id,
        summary=PipelineSummary(**summary),
    )


def _run_gdrive_job(
    request_id: str,
    client_id: str,
    folder_id: str,
    recursive: bool,
    extra_meta: dict,
):
    from services.google_drive_loader import GoogleDriveLoader

    tmp_dir = Config.TMP_DIR / f"gdrive_{client_id}_{request_id}"

    with _jobs_lock:
        _jobs[request_id]["status"] = "running"

    try:
        loader = GoogleDriveLoader()

        tmp_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        downloaded = loader.download_folder(
            folder_id=folder_id,
            dest_dir=tmp_dir,
            recursive=recursive,
        )

        if not downloaded:
            with _jobs_lock:
                _jobs[request_id] = {
                    "status": "error",
                    "detail": "No supported files found in Google Drive folder",
                }
            return

        summary = run_pipeline(
            source_path=tmp_dir,
            source_type="google_drive",
            extra_metadata=extra_meta,
        )

        if "error" in summary:
            with _jobs_lock:
                _jobs[request_id] = {
                    "status": "error",
                    "detail": summary.get("error"),
                }
            return

        summary["status"] = (
            "partial"
            if summary.get("uploads_failed", 0)
            else "success"
        )

        summary["files_found"] = len(downloaded)

        with _jobs_lock:
            _jobs[request_id] = {
                "status": "done",
                "result": summary,
            }

    except Exception as exc:
        logger.exception(
            "[%s] Google Drive ingestion failed",
            request_id,
        )

        with _jobs_lock:
            _jobs[request_id] = {
                "status": "error",
                "detail": str(exc),
            }

    finally:
        if tmp_dir.exists():
            shutil.rmtree(
                tmp_dir,
                ignore_errors=True,
            )


@router.post(
    "/google-drive",
    response_model=IngestGoogleDriveResponse,
    status_code=202,
)
async def ingest_google_drive(
    body: IngestGoogleDriveRequest,
    background_tasks: BackgroundTasks,
    _key: str = Depends(require_api_key),
):
    request_id = str(uuid.uuid4())

    logger.info(
        "[%s] google-drive | client_id=%s | folder_id=%s",
        request_id,
        body.client_id,
        body.folder_id,
    )

    if not body.client_id or not body.client_id.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="client_id is required",
        )

    client_id = body.client_id.strip().lower()

    if not Config.GOOGLE_SERVICE_ACCOUNT_JSON:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Google Drive credentials are not configured",
        )

    extra_meta = dict(body.extra_metadata or {})
    extra_meta["client_id"] = client_id
    extra_meta["request_id"] = request_id
    extra_meta["gdrive_folder"] = body.folder_id

    if body.label:
        extra_meta["ingest_label"] = body.label

    with _jobs_lock:
        _jobs[request_id] = {
            "status": "pending"
        }

    background_tasks.add_task(
        _run_gdrive_job,
        request_id=request_id,
        client_id=client_id,
        folder_id=body.folder_id,
        recursive=body.recursive,
        extra_meta=extra_meta,
    )

    return IngestGoogleDriveResponse(
        request_id=request_id,
        status="pending",
        message="Google Drive ingestion started",
        poll_url=f"/ingest/google-drive/status/{request_id}",
    )


@router.get(
    "/google-drive/status/{request_id}",
)
async def gdrive_job_status(
    request_id: str,
    _key: str = Depends(require_api_key),
):
    with _jobs_lock:
        job = _jobs.get(request_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No job found for request_id '{request_id}'",
        )

    return {
        "request_id": request_id,
        **job,
    }
