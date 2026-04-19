"""
services/google_drive_loader.py — Download files from Google Drive for ingestion.

Supports two auth methods:
  1. Service Account (recommended for servers): set GOOGLE_SERVICE_ACCOUNT_JSON
     in .env with the full JSON content of your service account key.
  2. OAuth credentials file: set GOOGLE_CREDENTIALS_FILE to the path of your
     credentials.json downloaded from Google Cloud Console.

Setup:
  1. Go to Google Cloud Console → APIs & Services → Enable "Google Drive API"
  2. Create a Service Account → Download JSON key
  3. Share the Drive folder with the service account email address
  4. Set GOOGLE_SERVICE_ACCOUNT_JSON in .env

Install:
  pip install google-api-python-client google-auth google-auth-httplib2

Usage in pipeline:
  loader = GoogleDriveLoader()
  files  = loader.download_folder(folder_id="1BxiMV...", dest_dir=Path("/tmp/gdrive"))
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from config import Config

logger = logging.getLogger(__name__)

# File MIME types we can download as binary files
_NATIVE_MIME_TO_EXT = {
    "application/pdf":                                      ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/plain":                                           ".txt",
    "text/csv":                                             ".csv",
    "text/html":                                            ".html",
    "text/markdown":                                        ".md",
    "application/json":                                     ".json",
    "application/rtf":                                      ".rtf",
    "text/rtf":                                             ".rtf",
}

# Google Workspace docs — must be exported to a downloadable format
_GOOGLE_DOC_EXPORT = {
    "application/vnd.google-apps.document":     ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"),
    "application/vnd.google-apps.spreadsheet":  ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
    "application/vnd.google-apps.presentation": ("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"),
}


class GoogleDriveLoader:
    """Downloads supported files from a Google Drive folder."""

    def __init__(self):
        self._service = None

    @property
    def service(self):
        if self._service is None:
            self._service = self._build_service()
        return self._service

    def _build_service(self):
        """Build the Google Drive API service using configured credentials."""
        try:
            from googleapiclient.discovery import build
            from google.oauth2 import service_account
            from google.oauth2.credentials import Credentials
        except ImportError:
            raise ImportError(
                "Google API client libraries not installed. "
                "Run: pip install google-api-python-client google-auth"
            )

        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

        if Config.GOOGLE_SERVICE_ACCOUNT_JSON:
            # Inline JSON string in env var
            try:
                info = json.loads(Config.GOOGLE_SERVICE_ACCOUNT_JSON)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {exc}"
                )
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=scopes
            )
        elif Config.GOOGLE_CREDENTIALS_FILE:
            creds = service_account.Credentials.from_service_account_file(
                Config.GOOGLE_CREDENTIALS_FILE, scopes=scopes
            )
        else:
            raise ValueError(
                "No Google credentials configured. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_CREDENTIALS_FILE in .env."
            )

        return build("drive", "v3", credentials=creds, cache_discovery=False)

    def download_folder(
        self,
        folder_id: str,
        dest_dir: Path,
        recursive: bool = True,
    ) -> List[Path]:
        """
        Download all supported files from a Drive folder to dest_dir.
        Returns list of local file paths that were successfully downloaded.
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        downloaded: List[Path] = []
        self._download_recursive(folder_id, dest_dir, downloaded, recursive)
        logger.info(
            "Google Drive download complete: %d files from folder %s",
            len(downloaded), folder_id,
        )
        return downloaded

    def _download_recursive(
        self,
        folder_id: str,
        dest_dir: Path,
        collected: List[Path],
        recursive: bool,
    ) -> None:
        """Recursively walk the folder tree and download supported files."""
        try:
            results = (
                self.service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    fields="files(id, name, mimeType)",
                    pageSize=1000,
                )
                .execute()
            )
        except Exception as exc:
            logger.error("Failed to list Drive folder %s: %s", folder_id, exc)
            return

        for item in results.get("files", []):
            file_id   = item["id"]
            file_name = item["name"]
            mime_type = item["mimeType"]

            if mime_type == "application/vnd.google-apps.folder":
                if recursive:
                    sub_dir = dest_dir / _safe_name(file_name)
                    sub_dir.mkdir(parents=True, exist_ok=True)
                    self._download_recursive(file_id, sub_dir, collected, recursive)
                continue

            # Google Workspace docs → export to Office format
            if mime_type in _GOOGLE_DOC_EXPORT:
                export_mime, ext = _GOOGLE_DOC_EXPORT[mime_type]
                local_path = dest_dir / (_safe_name(Path(file_name).stem) + ext)
                if self._export_file(file_id, export_mime, local_path):
                    if local_path.suffix.lower() in Config.SUPPORTED_EXTENSIONS:
                        collected.append(local_path)
                continue

            # Native binary files
            ext = _NATIVE_MIME_TO_EXT.get(mime_type) or Path(file_name).suffix.lower()
            if ext not in Config.SUPPORTED_EXTENSIONS:
                logger.debug("Skipping unsupported file type '%s': %s", mime_type, file_name)
                continue

            local_path = dest_dir / _safe_name(file_name)
            if self._download_file(file_id, local_path):
                collected.append(local_path)

    def _download_file(self, file_id: str, dest: Path) -> bool:
        """Download a binary file from Drive."""
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io

            request = self.service.files().get_media(fileId=file_id)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            dest.write_bytes(buf.getvalue())
            logger.debug("Downloaded: %s", dest.name)
            return True
        except Exception as exc:
            logger.error("Failed to download file %s: %s", file_id, exc)
            return False

    def _export_file(self, file_id: str, mime_type: str, dest: Path) -> bool:
        """Export a Google Workspace document."""
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io

            request = self.service.files().export_media(fileId=file_id, mimeType=mime_type)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            dest.write_bytes(buf.getvalue())
            logger.debug("Exported: %s", dest.name)
            return True
        except Exception as exc:
            logger.error("Failed to export file %s: %s", file_id, exc)
            return False


def _safe_name(name: str) -> str:
    """Remove characters that are invalid in filenames."""
    import re
    return re.sub(r'[\\/:*?"<>|]', "_", name)
