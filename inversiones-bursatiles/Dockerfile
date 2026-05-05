FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data outputs

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Argentina/Buenos_Aires

EXPOSE 8080

CMD mkdir -p data && \
    curl -sf -H "Authorization: token ${GH_TOKEN}" \
    "https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/main/inversiones-bursatiles/data/merval_cierres.csv" \
    -o data/merval_cierres.csv && \
    curl -sf -H "Authorization: token ${GH_TOKEN}" \
    "https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/main/inversiones-bursatiles/data/bovespa_cierres.csv" \
    -o data/bovespa_cierres.csv && \
    curl -sf -H "Authorization: token ${GH_TOKEN}" \
    "https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/main/inversiones-bursatiles/data/sp500_cierres.csv" \
    -o data/sp500_cierres.csv && \
    curl -sf -H "Authorization: token ${GH_TOKEN}" \
    "https://raw.githubusercontent.com/${GH_USER}/${GH_REPO}/main/inversiones-bursatiles/data/download_status.json" \
    -o data/download_status.json ; \
    python main.py
