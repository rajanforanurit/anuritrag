from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
class Config:
    BASE_DIR: Path = Path(__file__).parent
    TMP_DIR:  Path = BASE_DIR / "tmp"
    AZURE_CONNECTION_STRING: str = os.getenv("AZURE_CONNECTION_STRING", "")
    AZURE_CONTAINER_NAME:    str = os.getenv("AZURE_CONTAINER_NAME", "vectordbforrag")
    BLOB_RAW_PREFIX:    str = "raw/"
    BLOB_CHUNKS_PREFIX: str = "chunks/"
    BLOB_FAISS_PREFIX:  str = "faiss/"
    BLOB_META_PREFIX:   str = "meta/"
    AZURE_SEARCH_ENDPOINT: str = os.getenv("AZURE_SEARCH_ENDPOINT", "")
    AZURE_SEARCH_KEY:      str = os.getenv("AZURE_SEARCH_KEY", "")
    AZURE_SEARCH_INDEX:    str = os.getenv("AZURE_SEARCH_INDEX", "rag-chunks")
    EMBEDDING_DIM:         int = int(os.getenv("EMBEDDING_DIM", "384"))
    EMBEDDING_MODEL: str = os.getenv(
        "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L12-v2"
    )
    CHUNK_SIZE:    int = int(os.getenv("CHUNK_SIZE", "500"))
    CHUNK_OVERLAP: int = int(os.getenv("CHUNK_OVERLAP", "2"))
    API_KEY: str = os.getenv("API_KEY", "")
    SUPPORTED_EXTENSIONS: set = {
        ".pdf", ".pptx", ".txt", ".docx",
        ".xlsx", ".xls", ".csv", ".json",
        ".html", ".xml", ".md", ".rtf",
    }
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50"))
    GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    SHAREPOINT_TENANT_ID:     str = os.getenv("SHAREPOINT_TENANT_ID", "")
    SHAREPOINT_CLIENT_ID:     str = os.getenv("SHAREPOINT_CLIENT_ID", "")
    SHAREPOINT_CLIENT_SECRET: str = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
    SHAREPOINT_SITE_URL:      str = os.getenv("SHAREPOINT_SITE_URL", "")

    # ── CORS ────────────────────────────────────────────────────────────────
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "*").split(",")

    # ── LLM keys ────────────────────────────────────────────────────────────
    OPENAI_API_KEY:        str = os.getenv("OPENAI_API_KEY", "")
    AZURE_OPENAI_API_KEY:  str = os.getenv("AZURE_OPENAI_API_KEY", "")
    AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")

    @classmethod
    def resolve_local_path(cls, path_str: str):
        p = Path(path_str)
        if not p.exists():
            return None, f"Path does not exist: {path_str}"
        if not p.is_dir():
            return None, f"Path is not a directory: {path_str}"
        return p, None

    @classmethod
    def validate(cls) -> list[str]:
        errors: list[str] = []
        if not cls.AZURE_CONNECTION_STRING:
            errors.append("AZURE_CONNECTION_STRING is not set")
        if not cls.API_KEY:
            errors.append("API_KEY is not set")
        if not cls.AZURE_SEARCH_ENDPOINT:
            errors.append("AZURE_SEARCH_ENDPOINT is not set")
        if not cls.AZURE_SEARCH_KEY:
            errors.append("AZURE_SEARCH_KEY is not set")
        return errors
