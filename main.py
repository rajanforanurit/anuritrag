from __future__ import annotations
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sentence_transformers import SentenceTransformer
from config import Config
from api.routers import ingest, storage
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

embedding_model: SentenceTransformer | None = None
@asynccontextmanager
async def lifespan(app: FastAPI):
    global embedding_model

    logger.info("═══ RAG Ingestion Pipeline starting ═══")

    errors = Config.validate()
    for err in errors:
        logger.warning("⚠  Config: %s", err)
    try:
        model_name = Config.EMBEDDING_MODEL
        logger.info("Loading embedding model: %s", model_name)
        embedding_model = SentenceTransformer(model_name)
        logger.info("✔ Embedding model loaded")
    except Exception as exc:
        logger.error("Embedding model failed to load: %s", exc)

    # Create Azure AI Search index if not exists
    try:
        from services.azure_search import create_index
        create_index()
        logger.info("✔ Azure AI Search index ready")
    except Exception as exc:
        logger.warning("Azure AI Search index setup failed (will retry on first ingest): %s", exc)

    Config.TMP_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("═══ Pipeline ready ═══")
    yield
    logger.info("═══ RAG Ingestion Pipeline shutting down ═══")


app = FastAPI(
    title="RAG Ingestion Pipeline API",
    description=(
        "Document ingestion pipeline — extracts, chunks, embeds, "
        "and stores documents in Azure AI Search + Azure Blob Storage.\n\n"
        "**Auth**: All endpoints require `Authorization: Bearer <API_KEY>` header.\n\n"
        "**Vector store**: Azure AI Search (hybrid BM25 + vector + semantic reranking)\n\n"
        "**Raw file storage**: Azure Blob Storage (`vectordbforrag` container)"
    ),
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=Config.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error.", "error_type": type(exc).__name__},
    )


app.include_router(ingest.router)
app.include_router(storage.router)
@app.get("/", tags=["Health"])
async def root():
    return {
        "service": "RAG Ingestion Pipeline API",
        "version": "3.0.0",
        "vector_store": "Azure AI Search",
        "docs": "/docs",
        "health": "/health",
        "search_health": "/search/health",
    }
@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}

@app.get("/search/health", tags=["Health"])
async def search_health():
    from services.azure_search import ping
    return ping()
