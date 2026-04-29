FROM python:3.12-slim

# Dependencias del sistema (mínimas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependencias Python primero (capa cacheable)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Crear directorios de trabajo
RUN mkdir -p data outputs

# Variables de entorno con defaults (se sobreescriben en Railway)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Argentina/Buenos_Aires

EXPOSE 8080

CMD ["python", "main.py"]
