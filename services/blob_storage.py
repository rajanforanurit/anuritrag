from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from config import Config

logger = logging.getLogger(__name__)


class BlobStorageService:
    def __init__(self, connection_string: str, container_name: str) -> None:
        if not connection_string:
            raise ValueError("AZURE_CONNECTION_STRING must not be empty.")

        self._conn_str = connection_string
        self._container = container_name
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from azure.storage.blob import BlobServiceClient

            service = BlobServiceClient.from_connection_string(self._conn_str)
            self._client = service.get_container_client(self._container)
            self._ensure_container()

        return self._client

    def _ensure_container(self) -> None:
        try:
            self._client.create_container()
            logger.info("Container created: %s", self._container)
        except Exception as exc:
            if "ContainerAlreadyExists" not in str(exc):
                logger.debug("Container check skipped: %s", str(exc))

    def build_client_blob_path(
        self,
        client_id: str,
        filename: str,
        prefix: str = "raw",
    ) -> str:
        if not client_id:
            raise ValueError("client_id is required")

        client_id = client_id.strip().lower()
        filename = Path(filename).name.strip()

        if not filename:
            raise ValueError("filename is required")

        return f"{prefix}/{client_id}/{filename}"

    def upload_file(
        self,
        local_path: Path,
        blob_name: str,
        overwrite: bool = True,
    ) -> dict:
        try:
            if not local_path.exists():
                return {
                    "success": False,
                    "blob_name": blob_name,
                    "error": f"Local file not found: {str(local_path)}",
                }

            with open(local_path, "rb") as file_handle:
                self.client.upload_blob(
                    name=blob_name,
                    data=file_handle,
                    overwrite=overwrite,
                )

            logger.info("Uploaded file to blob: %s", blob_name)

            return {
                "success": True,
                "blob_name": blob_name,
                "blob_url": self._url(blob_name),
            }

        except Exception as exc:
            logger.error(
                "upload_file failed | blob=%s | error=%s",
                blob_name,
                str(exc),
            )

            return {
                "success": False,
                "blob_name": blob_name,
                "error": str(exc),
            }

    def upload_file_for_client(
        self,
        client_id: str,
        local_path: Path,
        prefix: str = "raw",
        overwrite: bool = True,
    ) -> dict:
        try:
            blob_name = self.build_client_blob_path(
                client_id=client_id,
                filename=local_path.name,
                prefix=prefix,
            )

            return self.upload_file(
                local_path=local_path,
                blob_name=blob_name,
                overwrite=overwrite,
            )

        except Exception as exc:
            logger.error(
                "upload_file_for_client failed | client_id=%s | file=%s | error=%s",
                client_id,
                str(local_path),
                str(exc),
            )

            return {
                "success": False,
                "blob_name": "",
                "error": str(exc),
            }

    def upload_bytes(
        self,
        data: bytes,
        blob_name: str,
        content_type: str = "application/octet-stream",
        overwrite: bool = True,
    ) -> dict:
        try:
            from azure.storage.blob import ContentSettings

            self.client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=overwrite,
                content_settings=ContentSettings(
                    content_type=content_type
                ),
            )

            logger.info(
                "Uploaded bytes to blob: %s (%d bytes)",
                blob_name,
                len(data),
            )

            return {
                "success": True,
                "blob_name": blob_name,
                "blob_url": self._url(blob_name),
            }

        except Exception as exc:
            logger.error(
                "upload_bytes failed | blob=%s | error=%s",
                blob_name,
                str(exc),
            )

            return {
                "success": False,
                "blob_name": blob_name,
                "error": str(exc),
            }

    def download_bytes(self, blob_name: str) -> bytes:
        blob_client = self.client.get_blob_client(blob_name)
        return blob_client.download_blob().readall()

    def download_to_file(
        self,
        blob_name: str,
        local_path: Path,
    ) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)

        blob_client = self.client.get_blob_client(blob_name)

        with open(local_path, "wb") as file_handle:
            file_handle.write(
                blob_client.download_blob().readall()
            )

    def list_blobs(
        self,
        prefix: Optional[str] = None,
    ) -> List[str]:
        try:
            return [
                blob.name
                for blob in self.client.list_blobs(
                    name_starts_with=prefix
                )
            ]

        except Exception as exc:
            logger.error(
                "list_blobs failed | prefix=%s | error=%s",
                prefix,
                str(exc),
            )
            return []

    def list_blob_details(
        self,
        prefix: Optional[str] = None,
    ) -> List[dict]:
        try:
            results = []

            for blob in self.client.list_blobs(
                name_starts_with=prefix,
                include=["metadata"],
            ):
                results.append(
                    {
                        "name": blob.name,
                        "size_bytes": blob.size,
                        "last_modified": (
                            blob.last_modified.isoformat()
                            if blob.last_modified
                            else None
                        ),
                        "content_type": (
                            blob.content_settings.content_type
                            if blob.content_settings
                            else None
                        ),
                    }
                )

            return results

        except Exception as exc:
            logger.error(
                "list_blob_details failed | error=%s",
                str(exc),
            )
            return []

    def blob_exists(
        self,
        blob_name: str,
    ) -> bool:
        try:
            self.client.get_blob_client(
                blob_name
            ).get_blob_properties()
            return True
        except Exception:
            return False

    def delete_blob(
        self,
        blob_name: str,
    ) -> bool:
        try:
            self.client.delete_blob(blob_name)

            logger.info(
                "Deleted blob: %s",
                blob_name,
            )

            return True

        except Exception as exc:
            logger.error(
                "delete_blob failed | blob=%s | error=%s",
                blob_name,
                str(exc),
            )
            return False

    def delete_prefix(
        self,
        prefix: str,
    ) -> int:
        names = self.list_blobs(prefix)
        deleted = sum(
            1 for name in names
            if self.delete_blob(name)
        )
        return deleted

    def ping(self) -> dict:
        try:
            properties = self.client.get_container_properties()

            return {
                "ok": True,
                "account": self.client.account_name,
                "container": self._container,
                "lease_status": properties.get(
                    "lease",
                    {}
                ).get(
                    "status",
                    "unknown",
                ),
            }

        except Exception as exc:
            return {
                "ok": False,
                "account": "",
                "container": self._container,
                "error": str(exc),
            }

    def _url(
        self,
        blob_name: str,
    ) -> str:
        return (
            f"https://{self.client.account_name}.blob.core.windows.net"
            f"/{self._container}/{blob_name}"
        )

blob_storage_service = BlobStorageService(
    connection_string=Config.AZURE_CONNECTION_STRING,
    container_name=Config.AZURE_CONTAINER_NAME,
)


def upload_file_to_blob(
    local_file_path,
    blob_name,
):
    local_path = Path(local_file_path)

    return blob_storage_service.upload_file(
        local_path=local_path,
        blob_name=blob_name,
    )


def upload_file_to_blob_for_client(
    client_id,
    local_file_path,
    prefix="raw",
):
    local_path = Path(local_file_path)

    return blob_storage_service.upload_file_for_client(
        client_id=client_id,
        local_path=local_path,
        prefix=prefix,
    )
