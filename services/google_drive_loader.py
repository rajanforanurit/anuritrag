from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List
from config import Config

logger = logging.getLogger(__name__)

_NATIVE_MIME_TO_EXT = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/plain": ".txt",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/markdown": ".md",
    "application/json": ".json",
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
}

_GOOGLE_DOC_EXPORT = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".docx",
    ),
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xlsx",
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".pptx",
    ),
}


# ─────────────────────────────────────────────────────────────
# Main Loader
# ─────────────────────────────────────────────────────────────

class GoogleDriveLoader:
    """Google Drive folder downloader using ONLY Service Account JSON."""

    def __init__(self):
        self._service = None

    @property
    def service(self):
        if self._service is None:
            self._service = self._build_service()
        return self._service

    # ─────────────────────────────────────────────────────────

    def _build_service(self):
        """Build Google Drive API service from service account JSON."""
        try:
            from googleapiclient.discovery import build
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "Missing dependencies. Install:\n"
                "pip install google-api-python-client google-auth"
            )

        if not Config.GOOGLE_SERVICE_ACCOUNT_JSON:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is missing in .env")

        try:
            info = json.loads(Config.GOOGLE_SERVICE_ACCOUNT_JSON)

            # 🔥 IMPORTANT FIX FOR RENDER
            if "private_key" in info:
                info["private_key"] = info["private_key"].replace("\\n", "\n")

        except Exception as exc:
            raise ValueError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {exc}")

        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

        try:
            creds = service_account.Credentials.from_service_account_info(
                info,
                scopes=scopes,
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to create Google credentials: {exc}")

        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # ─────────────────────────────────────────────────────────

    def download_folder(
        self,
        folder_id: str,
        dest_dir: Path,
        recursive: bool = True,
    ) -> List[Path]:

        dest_dir.mkdir(parents=True, exist_ok=True)
        downloaded: List[Path] = []

        self._download_recursive(folder_id, dest_dir, downloaded, recursive)

        logger.info(
            "Google Drive download completed: %d files from %s",
            len(downloaded),
            folder_id,
        )

        return downloaded

    # ─────────────────────────────────────────────────────────

    def _download_recursive(
        self,
        folder_id: str,
        dest_dir: Path,
        collected: List[Path],
        recursive: bool,
    ) -> None:

        try:
            results = (
                self.service.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields="files(id,name,mimeType)",
                    pageSize=1000,
                )
                .execute()
            )
        except Exception as exc:
            logger.error("Failed to list folder %s: %s", folder_id, exc)
            return

        for item in results.get("files", []):
            file_id = item["id"]
            name = item["name"]
            mime = item["mimeType"]

            # ── Folder ───────────────────────────────
            if mime == "application/vnd.google-apps.folder":
                if recursive:
                    sub_dir = dest_dir / _safe_name(name)
                    sub_dir.mkdir(parents=True, exist_ok=True)
                    self._download_recursive(file_id, sub_dir, collected, recursive)
                continue

            # ── Google Docs Export ───────────────────
            if mime in _GOOGLE_DOC_EXPORT:
                export_mime, ext = _GOOGLE_DOC_EXPORT[mime]
                local_path = dest_dir / (_safe_name(Path(name).stem) + ext)
                if self._export_file(file_id, export_mime, local_path):
                    collected.append(local_path)
                continue

            # ── Normal Files ─────────────────────────
            ext = _NATIVE_MIME_TO_EXT.get(mime) or Path(name).suffix.lower()

            if ext not in Config.SUPPORTED_EXTENSIONS:
                continue

            local_path = dest_dir / _safe_name(name)
            if self._download_file(file_id, local_path):
                collected.append(local_path)

    # ─────────────────────────────────────────────────────────

    def _download_file(self, file_id: str, dest: Path) -> bool:
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io

            request = self.service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            dest.write_bytes(fh.getvalue())
            return True

        except Exception as exc:
            logger.error("Download failed %s: %s", file_id, exc)
            return False

    # ─────────────────────────────────────────────────────────

    def _export_file(self, file_id: str, mime_type: str, dest: Path) -> bool:
        try:
            from googleapiclient.http import MediaIoBaseDownload
            import io

            request = self.service.files().export_media(
                fileId=file_id,
                mimeType=mime_type,
            )

            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            dest.write_bytes(fh.getvalue())
            return True

        except Exception as exc:
            logger.error("Export failed %s: %s", file_id, exc)
            return False


# ─────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────

def _safe_name(name: str) -> str:
    import re
    return re.sub(r'[\\/:*?"<>|]', "_", name)
