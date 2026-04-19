"""
api/middleware/auth.py — Simple API key authentication dependency for FastAPI.

Usage in any router:
    from api.middleware.auth import require_api_key

    @router.post("/endpoint")
    async def handler(_key: str = Depends(require_api_key)):
        ...

Clients send the key as:
    Authorization: Bearer <API_KEY>

Note: JWT authentication will be added when the admin panel is built
as a separate project. This pipeline uses simple API key auth only.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import Config

logger = logging.getLogger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


async def require_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> str:
    """
    FastAPI dependency — validates the Bearer API key.
    Returns the key string on success, raises 401 on failure.
    """
    if not Config.API_KEY:
        # Dev mode: no key configured — allow through with a loud warning
        logger.warning(
            "API_KEY is not configured — request allowed without auth. "
            "Set API_KEY in .env for production."
        )
        return "no-key-configured"

    token: Optional[str] = None
    if credentials and credentials.credentials:
        token = credentials.credentials

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Provide it as: Authorization: Bearer <API_KEY>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if token != Config.API_KEY:
        logger.warning("Invalid API key attempt from request")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return token
