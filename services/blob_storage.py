"""
services/blob_storage.py — Azure Blob Storage service.

Covers: upload (file / bytes), download, list, delete, existence check, URL helper.
All public methods return structured dicts or raise — callers decide how to surface errors.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, List, Optional

logger = logging.getLogger(__name__)


class BlobStorageService:
    """Thin, dependency-free wrapper around azure-storage-blob."""

    def __init__(self, connection_string: str, container_name: str) -> None:
        if not connection_string:
            raise ValueError("AZURE_CONNECTION_STRING must not be empty.")
        self._conn_str  = connection_string
        self._container = container_name
        self._client    = None   # lazy — created on first access

    # ── Client lifecycle ───────────────────────────────────────────────────

    @property
    def client(self):
        """Lazy-initialised ContainerClient."""
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            svc          = BlobServiceClient.from_connection_string(self._conn_str)
            self._client = svc.get_container_client(self._container)
            self._ensure_container()
        return self._client

    def _ensure_container(self) -> None:
        try:
            self._client.create_container()
            logger.info("Container '%s' created.", self._container)
        except Exception as exc:
            if "ContainerAlreadyExists" not in str(exc):
                logger.debug("Container pre-check: %s", exc)

    # ── Upload ─────────────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: Path,
        blob_name: str,
        overwrite: bool = True,
    ) -> dict:
        """Upload a local file. Returns {'success', 'blob_name', 'blob_url'} or 'error'."""
        try:
            with open(local_path, "rb") as fh:
                self.client.upload_blob(name=blob_name, data=fh, overwrite=overwrite)
            logger.debug("Uploaded file → %s", blob_name)
            return {"success": True, "blob_name": blob_name, "blob_url": self._url(blob_name)}
        except Exception as exc:
            logger.error("upload_file failed '%s': %s", blob_name, exc)
            return {"success": False, "blob_name": blob_name, "error": str(exc)}

    def upload_bytes(
        self,
        data: bytes,
        blob_name: str,
        content_type: str = "application/octet-stream",
        overwrite: bool = True,
    ) -> dict:
        """Upload raw bytes. Returns {'success', 'blob_name', 'blob_url'} or 'error'."""
        try:
            from azure.storage.blob import ContentSettings
            self.client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=content_type),
            )
            logger.debug("Uploaded bytes → %s (%d B)", blob_name, len(data))
            return {"success": True, "blob_name": blob_name, "blob_url": self._url(blob_name)}
        except Exception as exc:
            logger.error("upload_bytes failed '%s': %s", blob_name, exc)
            return {"success": False, "blob_name": blob_name, "error": str(exc)}

    # ── Download ───────────────────────────────────────────────────────────

    def download_bytes(self, blob_name: str) -> bytes:
        """Download blob → bytes. Raises on failure."""
        blob_client = self.client.get_blob_client(blob_name)
        return blob_client.download_blob().readall()

    def download_to_file(self, blob_name: str, local_path: Path) -> None:
        """Stream blob to a local file. Raises on failure."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob_client = self.client.get_blob_client(blob_name)
        with open(local_path, "wb") as fh:
            fh.write(blob_client.download_blob().readall())

    # ── Listing ────────────────────────────────────────────────────────────

    def list_blobs(self, prefix: Optional[str] = None) -> List[str]:
        """Return blob names matching an optional prefix."""
        try:
            return [b.name for b in self.client.list_blobs(name_starts_with=prefix)]
        except Exception as exc:
            logger.error("list_blobs failed (prefix=%s): %s", prefix, exc)
            return []

    def list_blob_details(self, prefix: Optional[str] = None) -> List[dict]:
        """Return blob metadata dicts (name, size, last_modified, content_type)."""
        try:
            blobs = []
            for b in self.client.list_blobs(name_starts_with=prefix, include=["metadata"]):
                blobs.append({
                    "name":          b.name,
                    "size_bytes":    b.size,
                    "last_modified": b.last_modified.isoformat() if b.last_modified else None,
                    "content_type":  b.content_settings.content_type if b.content_settings else None,
                })
            return blobs
        except Exception as exc:
            logger.error("list_blob_details failed: %s", exc)
            return []

    # ── Existence / deletion ───────────────────────────────────────────────

    def blob_exists(self, blob_name: str) -> bool:
        try:
            self.client.get_blob_client(blob_name).get_blob_properties()
            return True
        except Exception:
            return False

    def delete_blob(self, blob_name: str) -> bool:
        """Delete a single blob. Returns True on success."""
        try:
            self.client.delete_blob(blob_name)
            logger.info("Deleted blob: %s", blob_name)
            return True
        except Exception as exc:
            logger.error("delete_blob failed '%s': %s", blob_name, exc)
            return False

    def delete_prefix(self, prefix: str) -> int:
        """Delete all blobs under a prefix. Returns count deleted."""
        names   = self.list_blobs(prefix)
        deleted = sum(1 for n in names if self.delete_blob(n))
        return deleted

    # ── Health check ───────────────────────────────────────────────────────

    def ping(self) -> dict:
        """
        Lightweight connectivity check.
        Returns {'ok': True/False, 'account': str, 'container': str, 'error'?: str}.
        """
        try:
            props = self.client.get_container_properties()
            return {
                "ok":           True,
                "account":      self.client.account_name,
                "container":    self._container,
                "lease_status": props.get("lease", {}).get("status", "unknown"),
            }
        except Exception as exc:
            return {"ok": False, "account": "", "container": self._container, "error": str(exc)}

    # ── Helpers ────────────────────────────────────────────────────────────

    def _url(self, blob_name: str) -> str:
        return (
            f"https://{self.client.account_name}.blob.core.windows.net"
            f"/{self._container}/{blob_name}"
        )
