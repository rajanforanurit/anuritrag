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
        if self._token:
            return self._token

        import msal

        authority = f"https://login.microsoftonline.com/{Config.SHAREPOINT_TENANT_ID}"

        app = msal.ConfidentialClientApplication(
            Config.SHAREPOINT_CLIENT_ID,
            authority=authority,
            client_credential=Config.SHAREPOINT_CLIENT_SECRET,
        )

        result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )

        # 🔥 Debug logging
        logger.error(f"MSAL response: {result}")

        if "access_token" not in result:
            raise RuntimeError(
                f"Token error: {result.get('error_description', result)}"
            )

        self._token = result["access_token"]
        return self._token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._get_token()}"}

    # ─────────────────────────────────────────────────────────────
    # 🔍 FIXED: Resolve site_id using search (NO 400 error)
    # ─────────────────────────────────────────────────────────────
    def _site_id(self, site_url: str) -> str:
        import requests

        parsed = urlparse(site_url)
        site_name = parsed.path.strip("/").split("/")[-1]

        url = f"https://graph.microsoft.com/v1.0/sites?search={site_name}"

        resp = requests.get(url, headers=self._headers(), timeout=30)

        if not resp.ok:
            logger.error(f"Site search error: {resp.text}")
            raise Exception(f"Site search failed: {resp.text}")

        sites = resp.json().get("value", [])

        for site in sites:
            if site_name.lower() in site.get("name", "").lower():
                logger.info(f"Matched site: {site.get('webUrl')}")
                return site["id"]

        raise Exception(f"Site '{site_name}' not found")

    # ─────────────────────────────────────────────────────────────
    # 📁 Get Drive ID
    # ─────────────────────────────────────────────────────────────
    def _drive_id(self, site_id: str) -> str:
        import requests

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        resp = requests.get(url, headers=self._headers(), timeout=30)

        if not resp.ok:
            logger.error(f"Drive fetch error: {resp.text}")
            raise Exception(resp.text)

        drives = resp.json().get("value", [])

        for drive in drives:
            if drive.get("name", "").lower() in ("documents", "shared documents"):
                return drive["id"]

        if drives:
            return drives[0]["id"]

        raise RuntimeError(f"No drives found for site {site_id}")

    # ─────────────────────────────────────────────────────────────
    # 📥 Download folder
    # ─────────────────────────────────────────────────────────────
    def download_folder(
        self,
        site_url: str,
        folder_path: str,
        dest_dir: Path,
    ) -> List[Path]:

        dest_dir.mkdir(parents=True, exist_ok=True)
        downloaded: List[Path] = []

        site_id = self._site_id(site_url)
        drive_id = self._drive_id(site_id)

        # Normalize folder path
        folder_path = folder_path.strip("/")

        if folder_path.lower().startswith("shared documents/"):
            folder_path = folder_path[len("shared documents/"):]

        if folder_path.lower() == "shared documents":
            folder_path = ""

        self._download_recursive(drive_id, folder_path, dest_dir, downloaded)

        logger.info(f"Downloaded {len(downloaded)} files from SharePoint")

        return downloaded

    # ─────────────────────────────────────────────────────────────
    # 🔁 Recursive download
    # ─────────────────────────────────────────────────────────────
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

            if not resp.ok:
                logger.error(f"Folder fetch error: {resp.text}")
                raise Exception(resp.text)

            data = resp.json()
            items = data.get("value", [])

            for item in items:
                name = item["name"]

                # 📁 Folder
                if "folder" in item:
                    sub_path = f"{folder_path}/{name}" if folder_path else name
                    sub_dir = dest_dir / _safe_name(name)
                    sub_dir.mkdir(parents=True, exist_ok=True)

                    self._download_recursive(
                        drive_id, sub_path, sub_dir, collected
                    )

                # 📄 File
                elif "file" in item:
                    ext = Path(name).suffix.lower()

                    if ext not in Config.SUPPORTED_EXTENSIONS:
                        continue

                    download_url = item.get("@microsoft.graph.downloadUrl")

                    if not download_url:
                        logger.warning(f"No download URL for: {name}")
                        continue

                    local_path = dest_dir / _safe_name(name)

                    if self._download_file(download_url, local_path):
                        collected.append(local_path)

            url = data.get("@odata.nextLink")

    # ─────────────────────────────────────────────────────────────
    # ⬇️ File download
    # ─────────────────────────────────────────────────────────────
    def _download_file(self, download_url: str, dest: Path) -> bool:
        import requests

        try:
            resp = requests.get(download_url, timeout=60, stream=True)

            if not resp.ok:
                logger.error(f"Download failed: {resp.text}")
                return False

            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded: {dest.name}")
            return True

        except Exception as exc:
            logger.error(f"Download error: {exc}")
            return False

def _safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)
