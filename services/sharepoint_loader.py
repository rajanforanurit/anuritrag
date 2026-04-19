"""
services/sharepoint_loader.py — Download files from SharePoint for ingestion.

Auth: Azure App Registration with Sites.Read.All (application permission).

Setup:
  1. Azure Portal → App Registrations → New Registration
  2. API Permissions → Add → SharePoint → Sites.Read.All (Application)
  3. Grant Admin Consent
  4. Certificates & Secrets → New client secret
  5. Set in .env:
       SHAREPOINT_TENANT_ID=<Directory (tenant) ID>
       SHAREPOINT_CLIENT_ID=<Application (client) ID>
       SHAREPOINT_CLIENT_SECRET=<secret value>

Install:
  pip install msal requests

Usage:
  loader = SharePointLoader()
  files  = loader.download_folder(
      site_url="https://company.sharepoint.com/sites/HR",
      folder_path="Shared Documents/Policies",
      dest_dir=Path("/tmp/sp"),
  )
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote, urlparse

from config import Config

logger = logging.getLogger(__name__)


class SharePointLoader:
    """Downloads supported files from a SharePoint document library folder."""

    def __init__(self):
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        """Acquire an access token via MSAL client credentials flow."""
        if self._token:
            return self._token
        try:
            import msal
        except ImportError:
            raise ImportError(
                "msal not installed. Run: pip install msal"
            )

        authority = f"https://login.microsoftonline.com/{Config.SHAREPOINT_TENANT_ID}"
        app = msal.ConfidentialClientApplication(
            Config.SHAREPOINT_CLIENT_ID,
            authority=authority,
            client_credential=Config.SHAREPOINT_CLIENT_SECRET,
        )
        result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" not in result:
            raise RuntimeError(
                f"Failed to acquire SharePoint token: {result.get('error_description', result)}"
            )
        self._token = result["access_token"]
        return self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _site_id(self, site_url: str) -> str:
        """Resolve a SharePoint site URL to its Graph API site ID."""
        import requests
        parsed   = urlparse(site_url)
        hostname = parsed.netloc
        site_path = parsed.path.rstrip("/")
        url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()["id"]

    def _drive_id(self, site_id: str) -> str:
        """Get the default document library (drive) ID for a site."""
        import requests
        url  = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        drives = resp.json().get("value", [])
        # Use the Documents drive (first one, or match by name)
        for drive in drives:
            if drive.get("name", "").lower() in ("documents", "shared documents"):
                return drive["id"]
        if drives:
            return drives[0]["id"]
        raise RuntimeError(f"No drives found for site {site_id}")

    def download_folder(
        self,
        site_url: str,
        folder_path: str,
        dest_dir: Path,
    ) -> List[Path]:
        """
        Download all supported files from a SharePoint folder to dest_dir.
        Returns list of local paths that were downloaded.
        """
        import requests

        dest_dir.mkdir(parents=True, exist_ok=True)
        downloaded: List[Path] = []

        site_id  = self._site_id(site_url)
        drive_id = self._drive_id(site_id)

        # Normalise folder path  e.g. "Shared Documents/Reports" → "Reports"
        # Graph API uses path relative to the drive root
        folder_path = folder_path.strip("/")
        if folder_path.lower().startswith("shared documents/"):
            folder_path = folder_path[len("shared documents/"):]
        if folder_path.lower() == "shared documents":
            folder_path = ""

        self._download_recursive(drive_id, folder_path, dest_dir, downloaded)
        logger.info(
            "SharePoint download complete: %d files from '%s'",
            len(downloaded), folder_path or "(root)",
        )
        return downloaded

    def _download_recursive(
        self,
        drive_id: str,
        folder_path: str,
        dest_dir: Path,
        collected: List[Path],
    ) -> None:
        import requests

        if folder_path:
            url = (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
                f"/root:/{quote(folder_path)}:/children"
            )
        else:
            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"

        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            data  = resp.json()
            items = data.get("value", [])

            for item in items:
                name = item["name"]
                if "folder" in item:
                    sub_path = f"{folder_path}/{name}" if folder_path else name
                    sub_dir  = dest_dir / _safe_name(name)
                    sub_dir.mkdir(parents=True, exist_ok=True)
                    self._download_recursive(drive_id, sub_path, sub_dir, collected)
                elif "file" in item:
                    ext = Path(name).suffix.lower()
                    if ext not in Config.SUPPORTED_EXTENSIONS:
                        logger.debug("Skipping unsupported file: %s", name)
                        continue
                    download_url = item.get("@microsoft.graph.downloadUrl")
                    if not download_url:
                        logger.warning("No download URL for: %s", name)
                        continue
                    local_path = dest_dir / _safe_name(name)
                    if self._download_file(download_url, local_path):
                        collected.append(local_path)

            url = data.get("@odata.nextLink")  # pagination

    def _download_file(self, download_url: str, dest: Path) -> bool:
        import requests
        try:
            resp = requests.get(download_url, timeout=60, stream=True)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.debug("Downloaded: %s", dest.name)
            return True
        except Exception as exc:
            logger.error("Failed to download '%s': %s", dest.name, exc)
            return False


def _safe_name(name: str) -> str:
    """Strip characters invalid in local filenames."""
    return re.sub(r'[\\/:*?"<>|]', "_", name)
