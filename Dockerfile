# ── RAG Ingestion Pipeline — Dockerfile ───────────────────────────────────────
# Target: Render (free/starter tier), single uvicorn worker, CPU-only PyTorch.
# Build time: ~5-8 min (model pre-cache adds ~2 min but saves cold-start latency).

FROM python:3.11-slim

# Environment flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRANSFORMERS_NO_TF=1 \
    TRANSFORMERS_NO_FLAX=1 \
    TRANSFORMERS_CACHE=/app/.cache/huggingface \
    HF_HOME=/app/.cache/huggingface \
    CUDA_VISIBLE_DEVICES="" \
    PORT=8000

WORKDIR /app

# System deps: libgomp (faiss), curl (health check), libreoffice-headless (DOC/PPT conversion)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        curl \
        libreoffice-headless \
    && rm -rf /var/lib/apt/lists/*

# Install PyTorch CPU-only FIRST (large wheel — separate layer for cache efficiency)
RUN pip install --no-cache-dir \
    torch==2.2.0+cpu \
    --index-url https://download.pytorch.org/whl/cpu

# Install all other Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary runtime directories
RUN mkdir -p /app/tmp /app/.cache/huggingface

# Pre-download the embedding model at build time so the first request is fast.
# If you want faster builds at the cost of a slower first request, comment this out.
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
import os; \
model_name = os.getenv('EMBEDDING_MODEL', 'sentence-transformers/all-MiniLM-L12-v2'); \
print(f'Pre-caching model: {model_name}'); \
SentenceTransformer(model_name); \
print('Model cached successfully.')"

# Expose the API port
EXPOSE 8000

# Health check — Render and Docker both use this
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start the API server
# Workers=1 on free/starter tier — scale up with a larger plan
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
