FROM python:3.12.13-slim

ARG CACHE_BUST=2026-06-02-v2359
RUN echo "Cache bust: $CACHE_BUST"

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Eliminar todos los .pyc cacheados para forzar recompilación
RUN find /app -name "*.pyc" -delete && find /app -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

RUN mkdir -p data outputs

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Argentina/Buenos_Aires

EXPOSE 8080
CMD ["python", "start_server.py"]
