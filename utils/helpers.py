"""
utils/helpers.py — Shared utility functions used across services.
"""

import re
import uuid
import hashlib
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def make_doc_id(file_path: Path) -> str:
    """
    Derive a stable, filesystem-safe document ID from a file path.
    Uses the stem (filename without extension), lowercased and slugified.
    """
    stem = file_path.stem
    slug = re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")
    return slug or "doc_" + uuid.uuid4().hex[:8]


def make_chunk_id(doc_id: str, index: int) -> str:
    return f"{doc_id}_chunk_{index:04d}"


def sha256_file(file_path: Path) -> str:
    """Compute SHA-256 hex digest of a file for deduplication / integrity checks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def safe_str(value: object, default: str = "") -> str:
    """Coerce a value to string, returning *default* on None / empty."""
    if value is None:
        return default
    return str(value).strip() or default
