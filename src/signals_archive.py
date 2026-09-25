"""
src/signals_archive.py

Archivo PERMANENTE (append-only) de señales -- plan de acción semana 1,
25/09/2026 (sesión con Claude).

Por qué existe: signals_history.json es una ventana rolling de 61 días (la
fecha más vieja se poda cada día). Es la fuente operativa del backtester y
está bien que siga así, pero como base de investigación destruye la única
cosa que se acumula con el tiempo: muestra independiente. Este módulo guarda
cada día hábil para siempre, en un archivo por mes:

    data/signals_archive_YYYY-MM.jsonl   -> una línea por (fecha, ticker)
    data/signals_archive_runs.jsonl      -> una línea por fecha: code_sha +
                                            model_version + pesos vigentes

Reglas (invariantes, cubiertas por tests/test_signals_archive.py):
  1. Un día CERRADO nunca se modifica. Solo se reemplaza el día en curso
     (el pipeline corre 4 veces por día y cada corrida pisa la anterior,
     igual que history[today] en tracker.py).
  2. Si llega una fecha ANTERIOR a la última ya archivada, se rechaza.
  3. Fines de semana no se archivan (mismo criterio que el backtester).
  4. La base del merge es SIEMPRE el contenido remoto en GitHub, nunca el
     disco local de Railway (efímero). Si no se puede leer el remoto, NO se
     pushea -- el día en curso se reescribe en la próxima corrida. Es la
     lección del incidente 27-28/07/2026 (historial colapsado de 19 días a 1
     por confiar en un archivo local vacío tras un redeploy).
  5. Los retornos NO se escriben acá. Se calculan al leer, desde los CSVs de
     cierres. Así el archivo es inmutable de verdad.

Trazabilidad: MODEL_VERSION quedó en "4.16" desde el 27/07 aunque después
entraron el fix de Simpson, la recalibración del predictor y otros cambios
de reglas -- un número manual se desactualiza. Por eso cada fila lleva
code_sha (RAILWAY_GIT_COMMIT_SHA): identifica exactamente el código que
generó la señal, incluyendo reglas de compra y lógica de confianza.

Nunca debe romper el pipeline: toda la entrada pública está envuelta en
try/except en tracker.py.
"""

import json
import os
import logging
from datetime import datetime, date, timezone

logger = logging.getLogger(__name__)

REPO = "Brunogatti79/inversiones-bursatiles"
DATA_DIR = "data"
RUNS_PATH = f"{DATA_DIR}/signals_archive_runs.jsonl"
WEIGHTS_PATH = f"{DATA_DIR}/optimized_weights.json"


def month_path(fecha: str) -> str:
    return f"{DATA_DIR}/signals_archive_{fecha[:7]}.jsonl"


def es_fin_de_semana(fecha: str) -> bool:
    return date.fromisoformat(fecha).weekday() >= 5


# ── Lógica pura (testeable sin red) ─────────────────────────────────────────

def parse_jsonl(text: str) -> list[dict]:
    out = []
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def dump_jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=False) + "\n" for r in rows)


def merge_day(base: list[dict], fecha: str, nuevas: list[dict]) -> list[dict] | None:
    """Devuelve las filas resultantes de reemplazar `fecha` en `base`, o None
    si la operación violaría la inmutabilidad (fecha anterior a la última
    archivada). Las filas de otras fechas se conservan tal cual, en orden."""
    fechas = {r.get("fecha") for r in base}
    ultima = max(fechas) if fechas else None
    if ultima is not None and fecha < ultima:
        return None
    conservadas = [r for r in base if r.get("fecha") != fecha]
    resultado = conservadas + nuevas
    # Chequeo defensivo: ninguna fila de otra fecha puede perderse.
    if len([r for r in resultado if r.get("fecha") != fecha]) != len(conservadas):
        return None
    return resultado


def union_cerradas(remoto: list[dict], local: list[dict], fecha_en_curso: str) -> list[dict]:
    """Red de seguridad ante un remoto levemente desactualizado (cache de
    raw.githubusercontent): si el disco local de esta instancia tiene fechas
    cerradas que el remoto todavía no muestra, se suman. Nunca borra nada del
    remoto."""
    fechas_remoto = {r.get("fecha") for r in remoto}
    extra = [r for r in local
             if r.get("fecha") not in fechas_remoto and r.get("fecha") != fecha_en_curso]
    if extra:
        logger.warning(f"[signals_archive] {len({r['fecha'] for r in extra})} fecha(s) "
                       f"presentes en local y no en remoto -- se conservan")
    combinado = remoto + extra
    combinado.sort(key=lambda r: r.get("fecha", ""))  # sort estable: orden intra-fecha intacto
    return combinado


def build_rows(fecha: str, history_rows: list[dict], code_sha: str | None,
               origen: str = "pipeline") -> list[dict]:
    ahora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []
    for r in history_rows:
        fila = {"fecha": fecha}
        fila.update(r)
        fila["code_sha"] = code_sha
        fila["origen"] = origen
        fila["archivado_en"] = ahora
        out.append(fila)
    return out


# ── I/O contra GitHub ───────────────────────────────────────────────────────

class RemoteUnavailable(Exception):
    pass


def fetch_remote_text(path: str) -> str:
    """Lee `path` de GitHub. Devuelve "" si el archivo no existe (404 real).
    Lanza RemoteUnavailable ante cualquier otra falla -- nunca devuelve
    vacío por un error de red, que es exactamente el punto ciego que causó
    el incidente de julio.

    Primero intenta resolver el último commit del archivo y leerlo por SHA
    (inmutable, sin la cache de ~5 min de raw/main). Si eso falla, cae a
    raw/main."""
    import requests
    token = os.environ.get("GH_TOKEN")
    headers = {"Authorization": f"token {token}"} if token else {}

    url_main = f"https://raw.githubusercontent.com/{REPO}/main/{path}"
    try:
        r = requests.get(f"https://api.github.com/repos/{REPO}/commits",
                         params={"path": path, "per_page": 1}, headers=headers, timeout=15)
        if r.status_code == 200:
            commits = r.json()
            if not commits:
                return ""  # el archivo nunca existió
            sha = commits[0]["sha"]
            rr = requests.get(f"https://raw.githubusercontent.com/{REPO}/{sha}/{path}", timeout=30)
            if rr.status_code == 200:
                return rr.text
    except Exception as e:
        logger.warning(f"[signals_archive] lectura por SHA falló para {path}: {e}")

    try:
        rr = requests.get(url_main, timeout=30)
    except Exception as e:
        raise RemoteUnavailable(str(e))
    if rr.status_code == 404:
        return ""
    if rr.status_code != 200:
        raise RemoteUnavailable(f"HTTP {rr.status_code}")
    return rr.text


def _read_local(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return parse_jsonl(f.read())
    except Exception:
        return []


def _write_and_push(path: str, rows: list[dict], message: str) -> bool:
    from src.github_persistence import push_file
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(dump_jsonl(rows))
    return push_file(path, message)


def _weights_snapshot():
    try:
        with open(WEIGHTS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── Entrada pública ─────────────────────────────────────────────────────────

def archive_day(fecha: str, history_rows: list[dict], fetch=fetch_remote_text) -> bool:
    """Archiva (o reemplaza, si es el día en curso) las filas de `fecha`.
    Devuelve True si pusheó. `fetch` es inyectable para tests."""
    if es_fin_de_semana(fecha):
        logger.info(f"[signals_archive] {fecha} es fin de semana -- no se archiva")
        return False
    if not history_rows:
        return False
    if not os.environ.get("GH_TOKEN"):
        # Sin token no hay push posible (push_file también lo exige). Salir
        # antes de tocar la red mantiene los tests de tracker offline.
        logger.warning("[signals_archive] GH_TOKEN no disponible -- no se archiva")
        return False

    code_sha = os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if not code_sha:
        logger.warning("[signals_archive] RAILWAY_GIT_COMMIT_SHA no disponible -- code_sha=None")

    path = month_path(fecha)
    try:
        remoto = parse_jsonl(fetch(path))
        runs_remoto = parse_jsonl(fetch(RUNS_PATH))
    except RemoteUnavailable as e:
        logger.error(f"[signals_archive] No se pudo leer el remoto ({e}) -- NO se pushea "
                     f"este ciclo para no arriesgar pisar historia. Se reintenta en la próxima corrida.")
        return False

    base = union_cerradas(remoto, _read_local(path), fecha)
    nuevas = build_rows(fecha, history_rows, code_sha)
    resultado = merge_day(base, fecha, nuevas)
    if resultado is None:
        logger.error(f"[signals_archive] {fecha} es anterior a la última fecha archivada -- "
                     f"rechazado (los días cerrados son inmutables)")
        return False

    run = {
        "fecha": fecha,
        "code_sha": code_sha,
        "model_version": (history_rows[0] or {}).get("model_version"),
        "n_filas": len(nuevas),
        "pesos_optimizados": _weights_snapshot(),
        "origen": "pipeline",
        "archivado_en": nuevas[0]["archivado_en"],
    }
    runs = merge_day(union_cerradas(runs_remoto, _read_local(RUNS_PATH), fecha), fecha, [run])

    ok = _write_and_push(path, resultado, f"auto: signals_archive {fecha}")
    if ok and runs is not None:
        _write_and_push(RUNS_PATH, runs, f"auto: signals_archive_runs {fecha}")
    logger.info(f"[signals_archive] {fecha}: {len(nuevas)} filas, "
                f"{len({r['fecha'] for r in resultado})} fechas en {path}, push={'OK' if ok else 'FALLÓ'}")
    return ok


def load_archive(desde: str | None = None, hasta: str | None = None, data_dir: str = DATA_DIR) -> list[dict]:
    """Para consumidores (backtester, diagnósticos): lee todos los meses locales."""
    import glob
    rows = []
    for p in sorted(glob.glob(f"{data_dir}/signals_archive_20*.jsonl")):
        rows.extend(_read_local(p))
    if desde:
        rows = [r for r in rows if r.get("fecha", "") >= desde]
    if hasta:
        rows = [r for r in rows if r.get("fecha", "") <= hasta]
    return rows
