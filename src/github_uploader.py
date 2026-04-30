"""
src/github_uploader.py
Sube el dashboard HTML a GitHub via API REST.
No requiere git instalado ni .gitignore.
"""

import os
import base64
import logging
import requests

logger = logging.getLogger(__name__)

GH_TOKEN = os.getenv("GH_TOKEN", "")
GH_USER  = os.getenv("GH_USER", "Brunogatti79")
GH_REPO  = os.getenv("GH_REPO", "inversiones-bursatiles")


def upload_dashboard(file_path: str) -> str | None:
    """
    Sube el archivo HTML a GitHub via API.
    Retorna la URL de GitHub Pages si tuvo éxito, None si falló.
    """
    if not GH_TOKEN:
        logger.warning("GH_TOKEN no configurado")
        return None

    filename = os.path.basename(file_path)
    github_path = f"outputs/{filename}"
    api_url = f"https://api.github.com/repos/{GH_USER}/{GH_REPO}/contents/{github_path}"

    headers = {
        "Authorization": f"token {GH_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Leer el archivo HTML
    try:
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        logger.error(f"Error leyendo archivo: {e}")
        return None

    # Verificar si el archivo ya existe (para obtener el SHA)
    sha = None
    try:
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
    except Exception:
        pass

    # Subir el archivo
    payload = {
        "message": f"dashboard update",
        "content": content,
    }
    if sha:
        payload["sha"] = sha

    try:
        r = requests.put(api_url, headers=headers, json=payload, timeout=30)
        if r.status_code in (200, 201):
            pages_url = f"https://{GH_USER}.github.io/{GH_REPO}/outputs/{filename}"
            logger.info(f"Dashboard subido a GitHub Pages: {pages_url}")
            return pages_url
        else:
            logger.error(f"Error subiendo a GitHub: {r.status_code} {r.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"Error en upload: {e}")
        return None
