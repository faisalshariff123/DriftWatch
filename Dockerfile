FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bake the embedding model into the image. Without this, the first request
# after every cold start downloads ~90MB from HuggingFace and hangs for a
# minute - on Cloud Run that reads as a dead frontend.
ENV HF_HOME=/opt/hf
RUN python -c "from sentence_transformers import SentenceTransformer; \
               SentenceTransformer('all-MiniLM-L6-v2')"

COPY app ./app
COPY dashboard.html .

ENV PORT=8080
# Single worker: the in-memory rate limiter and the centroid cache are
# per-process, so extra workers would each keep their own copy.
CMD exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --workers 1
