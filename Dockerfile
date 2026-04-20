FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRANSFORMERS_NO_TF=1 \
    TRANSFORMERS_NO_FLAX=1 \
    TRANSFORMERS_CACHE=/app/.cache/huggingface \
    HF_HOME=/app/.cache/huggingface \
    CUDA_VISIBLE_DEVICES="" \
    PORT=8000

WORKDIR /app

# System deps — libgomp1 is still needed by faiss-cpu (OpenMP)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        curl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application code ──────────────────────────────────────────────────────
COPY . .

RUN mkdir -p /app/tmp /app/.cache/huggingface
RUN python - <<'EOF'
import os
os.environ["TRANSFORMERS_NO_TF"]   = "1"
os.environ["TRANSFORMERS_NO_FLAX"] = "1"

from optimum.onnxruntime import ORTModelForFeatureExtraction
from transformers import AutoTokenizer

model_name = os.getenv(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L12-v2",
)
print(f"Pre-baking ONNX model: {model_name}")
AutoTokenizer.from_pretrained(model_name)
ORTModelForFeatureExtraction.from_pretrained(model_name, export=True)
print("ONNX export complete — cached to HF_HOME")
EOF

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
