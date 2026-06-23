"""
src/github_persistence.py

API única de persistencia contra GitHub Contents API. Reemplaza los 8 puntos
duplicados que existían (tracker.py x2, monitor.py, backtester.py,
macro_auto.py, opportunities_log.py, trailing_stop.py, bot.py) — cada uno con
su propia copia casi idéntica del patrón GET-sha/PUT.

Por qué existe: el filesystem de Railway es efímero y se resetea en cada
redeploy (que ocurre en cada push, incluso los automáticos del pipeline). Todo
archivo en data/ que deba sobrevivir entre runs tiene que pasar por acá.

Uso:
    from src.github_persistence import save_json, load_json, append_by_date

    save_json("data/foo.json", {"a": 1})            # guarda local + pushea
    data = load_json("data/foo.json", default={})   # lee local (no pega a GitHub)
    append_by_date("data/signals_history.json", "2026-06-22", records, max_days=60)
"""

import os
import json
import time
import base64
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

REPO = "Brunogatti79/inversiones-bursatiles"
MAX_RETRIES = 3
BACKOFF_BASE = 1.5  # segundos: 1.5, 2.25, 3.375...


def _headers():
    token = os.environ.get("GH_TOKEN", "")
    return token, {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}


def _get_sha(path, headers):
    import requests
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json().get("sha", "")
        if r.status_code == 403 and "rate limit" in r.text.lower():
            logger.warning(f"[github_persistence] rate limit en GET {path}")
        return None
    except Exception as e:
        logger.warning(f"[github_persistence] GET sha falló para {path}: {e}")
        return None


def push_file(path: str, message: str = None) -> bool:
    """Pushea el archivo local `path` a GitHub. Reintenta con backoff exponencial
    ante 409 (sha desactualizado) o rate limit. Devuelve True/False."""
    import requests

    token, headers = _headers()
    if not token:
        logger.warning(f"[github_persistence] GH_TOKEN no disponible — {path} solo quedó local")
        return False
    if not os.path.exists(path):
        logger.warning(f"[github_persistence] {path} no existe localmente, nada para pushear")
        return False

    message = message or f"auto: {os.path.basename(path)} {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"

    for attempt in range(1, MAX_RETRIES + 1):
        sha = _get_sha(path, headers)
        payload = {"message": message, "content": b64_content}
        if sha:
            payload["sha"] = sha
        try:
            r = requests.put(url, headers=headers, json=payload, timeout=20)
            if r.status_code in (200, 201):
                logger.info(f"[github_persistence] {path} pusheado (intento {attempt})")
                return True
            if r.status_code == 409 and attempt < MAX_RETRIES:
                # sha desactualizado (alguien pusheó en paralelo) — reintentar con backoff
                time.sleep(BACKOFF_BASE ** attempt)
                continue
            logger.warning(f"[github_persistence] push {path} falló: {r.status_code} {r.text[:200]}")
            return False
        except Exception as e:
            logger.warning(f"[github_persistence] error pusheando {path} (intento {attempt}): {e}")
            time.sleep(BACKOFF_BASE ** attempt)

    return False


def pull_file(path: str) -> bool:
    """Descarga `path` fresco de GitHub al filesystem local. Para usar al arrancar Railway."""
    import requests

    token, headers = _headers()
    if not token:
        return False
    try:
        url = f"https://api.github.com/repos/{REPO}/contents/{path}"
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return False
        content = base64.b64decode(r.json()["content"]).decode("utf-8")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"[github_persistence] {path} sincronizado desde GitHub")
        return True
    except Exception as e:
        logger.warning(f"[github_persistence] No se pudo sincronizar {path}: {e}")
        return False


def save_json(path: str, data, message: str = None, push: bool = True) -> bool:
    """Guarda `data` como JSON en `path` y lo pushea a GitHub (salvo push=False)."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        logger.warning(f"[github_persistence] No se pudo escribir {path}: {e}")
        return False
    return push_file(path, message) if push else True


def load_json(path: str, default=None):
    """Lee JSON local. No pega a GitHub (eso es responsabilidad de pull_file/sync al boot)."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[github_persistence] No se pudo leer {path}: {e}")
        return default


def append_by_date(path: str, date_key: str, records, max_days: int = 60, message: str = None) -> bool:
    """
    Patrón común a signals_history.json / macro_score_history.json /
    opportunities_log.json: dict {fecha: [...]/valor}, overwrite del día si ya
    corrió, purga de días viejos, guarda + pushea.
    """
    log = load_json(path, default={})
    log[date_key] = records
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    log = {d: v for d, v in log.items() if d >= cutoff}
    msg = message or f"auto: {os.path.basename(path)} {date_key}"
    return save_json(path, log, message=msg)


def sync_all_at_startup(filenames: list[str]):
    """Llamar una vez al arrancar Railway (start_server.py) para repoblar
    data/ con todo lo que sobrevive entre redeploys.

    Paralelizado (incidente 23/06/2026): con 11 archivos y timeout de 10s cada
    uno, la versión secuencial podía bloquear hasta ~110s antes de levantar
    el servidor HTTP — alargando la ventana en la que la instancia vieja y la
    nueva de Railway corren en simultáneo en cada redeploy, lo que agravaba
    el conflicto de "terminated by other getUpdates request" del bot de
    Telegram (dos instancias polleando a la vez)."""
    import concurrent.futures
    paths = [f"data/{f}" if not f.startswith("data/") else f for f in filenames]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(paths) or 1) as ex:
        list(ex.map(pull_file, paths))
