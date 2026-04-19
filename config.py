"""
config.py — Centralised configuration loaded from environment / .env file.

All settings are class-level attributes — import anywhere without instantiation.
Call Config.validate() on startup to catch missing required values early.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Project layout ──────────────────────────────────────────────────────
    BASE_DIR: Path = Path(__file__).parent
    TMP_DIR:  Path = BASE_DIR / "tmp"

    # ── Azure Blob Storage ──────────────────────────────────────────────────
    AZURE_CONNECTION_STRING: str = os.getenv("AZURE_CONNECTION_STRING", "")
    AZURE_CONTAINER_NAME:    str = os.getenv("AZURE_CONTAINER_NAME", "vectordbforrag")

    # Blob key prefixes (folder structure inside container)
    BLOB_RAW_PREFIX:    str = "raw/"
    BLOB_CHUNKS_PREFIX: str = "chunks/"
    BLOB_FAISS_PREFIX:  str = "faiss/"
    BLOB_META_PREFIX:   str = "meta/"

    # ── Embedding model ─────────────────────────────────────────────────────
    EMBEDDING_MODEL: str = os.getenv(
        "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L12-v2"
    )

    # ── Chunking ────────────────────────────────────────────────────────────
    CHUNK_SIZE:    int = int(os.getenv("CHUNK_SIZE",    "500"))
    CHUNK_OVERLAP: int = int(os.getenv("CHUNK_OVERLAP", "50"))

    # ── FAISS (optional local index backup) ─────────────────────────────────
    ENABLE_FAISS_BACKUP: bool = (
        os.getenv("ENABLE_FAISS_BACKUP", "false").lower() == "true"
    )
    FAISS_INDEX_PATH: Path = Path(
        os.getenv("FAISS_INDEX_PATH", str(BASE_DIR / "faiss_index.index"))
    )

    # ── API Security (simple API key — no JWT in ingestion pipeline) ────────
    # JWT will be added when the admin panel is built as a separate project.
    API_KEY: str = os.getenv("API_KEY", "")

    # ── File handling ───────────────────────────────────────────────────────
    SUPPORTED_EXTENSIONS: set = {
        ".pdf", ".pptx", ".txt", ".docx",
        ".xlsx", ".xls", ".csv", ".json",
        ".html", ".xml", ".md", ".rtf",
    }
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50"))

    # ── Google Drive ────────────────────────────────────────────────────────
    # Service account JSON key (path or inline JSON string)
    GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    # Or use OAuth credentials file path
    GOOGLE_CREDENTIALS_FILE: str = os.getenv("GOOGLE_CREDENTIALS_FILE", "")

    # ── SharePoint ──────────────────────────────────────────────────────────
    SHAREPOINT_TENANT_ID:     str = os.getenv("SHAREPOINT_TENANT_ID",     "")
    SHAREPOINT_CLIENT_ID:     str = os.getenv("SHAREPOINT_CLIENT_ID",     "")
    SHAREPOINT_CLIENT_SECRET: str = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    SHAREPOINT_SITE_URL:      str = os.getenv("SHAREPOINT_SITE_URL",      "")

    # ── CORS ────────────────────────────────────────────────────────────────
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

    # ── LLM keys (optional — stored so chat layer can verify config) ────────
    OPENAI_API_KEY:        str = os.getenv("OPENAI_API_KEY",        "")
    AZURE_OPENAI_API_KEY:  str = os.getenv("AZURE_OPENAI_API_KEY",  "")
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")

    @classmethod
    def validate(cls) -> list[str]:
        """Return list of human-readable warnings for missing/bad config."""
        errors: list[str] = []
        if not cls.AZURE_CONNECTION_STRING:
            errors.append("AZURE_CONNECTION_STRING is not set — Azure storage unavailable")
        if not cls.AZURE_CONTAINER_NAME:
            errors.append("AZURE_CONTAINER_NAME is not set")
        if not cls.API_KEY:
            errors.append(
                "API_KEY is not set — all API endpoints are unprotected! "
                "Set a strong random value in .env."
            )
        return errors

    @classmethod
    def resolve_local_path(cls, raw: str) -> tuple[Path, str | None]:
        """
        Parse and validate a local directory path string.
        Returns (path, error_message). error_message is None on success.
        """
        raw = raw.strip()
        if not raw:
            return Path("."), "No path provided"
        p = Path(raw)
        if not p.exists():
            return p, f"Path does not exist: {raw}"
        if not p.is_dir():
            return p, f"Path is not a directory: {raw}"
        return p, None
