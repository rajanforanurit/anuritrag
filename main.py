from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import Config
from api.routers import ingest, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("═══ RAG Ingestion Pipeline starting ═══")

    errors = Config.validate()
    for err in errors:
        logger.warning("⚠  Config: %s", err)

    # Pre-warm embedding model (avoids timeout on first request)
    try:
        from services.pipeline import get_embedder
        logger.info("Pre-loading embedding model: %s", Config.EMBEDDING_MODEL)
        get_embedder()
        logger.info("✔ Embedding model ready")
    except Exception as exc:
        logger.error("Embedding model failed to load: %s", exc)

    Config.TMP_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("═══ Pipeline ready ═══")
    yield
    logger.info("═══ RAG Ingestion Pipeline shutting down ═══")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="RAG Ingestion Pipeline API",
    description=(
        "Document ingestion pipeline: fetches files from local folders, "
        "Google Drive, or SharePoint — extracts text, chunks, embeds, "
        "and stores everything in Azure Blob Storage.\n\n"
        "**Auth**: All endpoints require `Authorization: Bearer <API_KEY>` header.\n\n"
        "**Supported file types**: PDF, PPTX, DOCX, TXT, XLSX, CSV, JSON, HTML, MD, RTF\n\n"
        "**Storage**: Azure Blob Storage — chunks + embeddings (JSONL), raw files, metadata JSON\n\n"
        "**Container**: `vectordbforrag`"
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ── CORS ───────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Global exception handler ───────────────────────────────────────────────────

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error.", "error_type": type(exc).__name__},
    )

# ── Routers ────────────────────────────────────────────────────────────────────

app.include_router(ingest.router)
app.include_router(storage.router)

# ── Health / root ──────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"], summary="API root")
async def root():
    return {
        "service": "RAG Ingestion Pipeline API",
        "version": "2.0.0",
        "docs":    "/docs",
        "health":  "/health",
        "sources": ["local-directory", "upload-file", "google-drive", "sharepoint"],
    }


@app.get("/health", tags=["Health"], summary="Liveness probe")
async def health():
    """Used by Render / Docker health checks. Returns 200 while alive."""
    return {"status": "ok"}
