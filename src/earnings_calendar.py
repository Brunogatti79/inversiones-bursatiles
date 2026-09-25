"""
src/earnings_calendar.py

Calendario de resultados trimestrales + shadow de blackout pre-earnings.

MOTIVO (auditoría con Claude, 25/09/2026)
----------------------------------------
El "alpha negativo de BOVESPA" (pregunta abierta v22 §11.10) resultó ser
casi entero un solo evento: HAPV3 cayó -33.1% el 13/08/2026 (probable
publicación de resultados 2T) y tenía 5 de sus 6 señales COMPRA en las dos
semanas previas. Alpha 21d de COMPRA BOVESPA (27/07-26/08): -1.12pp con
HAPV3, +0.80pp sin ella. El diagnóstico no es falla sistemática de
stock-picking sino exposición a riesgo de evento sin control.

MODO SHADOW -- NO SE APLICA A NINGUNA SEÑAL NI A KELLY
------------------------------------------------------
Este módulo solo GRABA, por señal y al momento de emisión (point-in-time),
a cuántas ruedas está el próximo resultado. No modifica signal, signal_v2,
scores ni portfolio_optimizer. Activarlo como filtro real requiere el
go/no-go explícito de Bruno después de evaluar la evidencia.

UMBRAL PRE-REGISTRADO
---------------------
PRE_BLACKOUT_BDAYS = 10 ruedas hábiles. Se fija ANTES de ver resultados
para no elegir el umbral que mejor queda (data snooping). Justificación:
las señales COMPRA de HAPV3 estuvieron entre 3 y 12 ruedas antes del
evento. Se persiste además earnings_days_to crudo, así la evaluación puede
reportar otros umbrales como análisis SECUNDARIO -- pero la decisión se
toma sobre el de 10.

FUENTE Y HONESTIDAD SOBRE COBERTURA
-----------------------------------
yfinance 0.2.54 Ticker.get_earnings_dates() pega al endpoint JSON
/v1/finance/visualization de Yahoo (fechas pasadas y futuras). Cobertura
para .SA/.BA NO VERIFICADA al escribir esto (el sandbox de desarrollo no
llega a Yahoo) -- por eso:
  * Si el ticker local no devuelve nada, se prueba con su ADR (misma
    empresa, misma fecha de publicación). Mapeo explícito en ADR_FALLBACK.
  * Si no hay fecha futura conocida, earnings_blackout_shadow = None, NUNCA
    False. "No sé" no es "no hay resultados cerca" (mismo criterio que el
    fix del ISM en macro_auto: dato ausente queda ausente, no se inventa).
  * Cada refresh loguea cobertura por mercado.

DÓNDE CORRE CADA PARTE (fix 25/09/2026, primer run real)
--------------------------------------------------------
El primer run en Railway trajo 0/84 tickers, AAPL incluido: Yahoo bloquea
a Railway (ya documentado en downloader._download_direct -- por eso los
precios se bajan desde GitHub Actions). Mismo patrón que los CSVs:
  * FETCH: scripts/download_data.py (GitHub Actions) llama
    refresh_earnings_calendar(push=False) y el workflow commitea el JSON.
  * LECTURA: pipeline.py (Railway) hace sync_calendar_from_github() y solo
    lee. Railway NUNCA escribe este archivo -- un solo escritor.

PERSISTENCIA
------------
data/earnings_calendar.json: lo escribe Actions (commit del workflow), se
sincroniza al arrancar Railway (start_server.py) y se re-lee fresco en cada
run del pipeline. Las fechas se MERGEAN (unión) entre refreshes:
así el histórico de fechas crece aunque Yahoo deje de devolver las viejas,
y queda disponible para el backtest retroactivo
(scripts/diagnostico_earnings_blackout.py).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, date

import numpy as np

logger = logging.getLogger(__name__)

CACHE_PATH = "data/earnings_calendar.json"
REFRESH_DAYS = 7            # Yahoo rate-limitea: refresh semanal, no por run
PRE_BLACKOUT_BDAYS = 10     # umbral pre-registrado (ver docstring)
YF_LIMIT = 24               # ~4 próximos + ~20 trimestres pasados
SLEEP_BETWEEN = 0.4         # segundos entre tickers
MIN_COVERAGE_RETRY = 0.5    # si el último refresh cubrió menos, reintenta sin esperar 7 días

_fetch_errors: list[str] = []   # muestra de errores del último refresh (para el log)

# Fallback a ADR cuando el ticker local no devuelve fechas. Misma empresa ->
# misma fecha de publicación. Solo se incluyen equivalencias 1:1 de emisor
# (TXAR.BA NO mapea a TX: Ternium S.A. es otra entidad que Ternium Argentina).
ADR_FALLBACK = {
    # MERVAL
    "GGAL.BA": "GGAL", "BMA.BA": "BMA", "SUPV.BA": "SUPV", "BBAR.BA": "BBAR",
    "PAMP.BA": "PAM", "CEPU.BA": "CEPU", "TGSU2.BA": "TGS", "EDN.BA": "EDN",
    "CRES.BA": "CRESY", "IRSA.BA": "IRS", "TECO2.BA": "TEO", "LOMA.BA": "LOMA",
    "YPFD.BA": "YPF",
    # BOVESPA
    "PETR4.SA": "PBR", "VALE3.SA": "VALE", "ITUB4.SA": "ITUB", "BBDC4.SA": "BBD",
    "ABEV3.SA": "ABEV", "SUZB3.SA": "SUZ", "CSNA3.SA": "SID", "BBAS3.SA": "BDORY",
    "SANB11.SA": "BSBR",
}


# ── Fetch ────────────────────────────────────────────────────────────────────

def _to_iso(d) -> str | None:
    try:
        if hasattr(d, "date"):
            d = d.date()
        if isinstance(d, date):
            return d.isoformat()
        return str(d)[:10]
    except Exception:
        return None


def fetch_earnings_dates_yf(symbol: str) -> list[str]:
    """Fechas de resultados (pasadas y futuras) para `symbol` vía yfinance.
    Devuelve lista ISO ordenada, vacía si falla o no hay datos. Nunca lanza."""
    dates: set[str] = set()
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        try:
            df = tk.get_earnings_dates(limit=YF_LIMIT)
            if df is not None and len(df):
                dates.update(filter(None, (_to_iso(x) for x in df.index)))
        except Exception as e:
            _fetch_errors.append(f"{symbol} get_earnings_dates: {type(e).__name__}: {e}"[:200])
        try:
            cal = tk.calendar
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date") or []
                if not isinstance(ed, (list, tuple)):
                    ed = [ed]
                dates.update(filter(None, (_to_iso(x) for x in ed)))
        except Exception as e:
            _fetch_errors.append(f"{symbol} calendar: {type(e).__name__}: {e}"[:200])
    except Exception as e:
        _fetch_errors.append(f"{symbol} yfinance: {type(e).__name__}: {e}"[:200])
    return sorted(dates)


def _fetch_with_fallback(ticker: str, fetcher) -> tuple[list[str], str | None]:
    dates = fetcher(ticker)
    if dates:
        return dates, "yf"
    adr = ADR_FALLBACK.get(ticker)
    if adr:
        dates = fetcher(adr)
        if dates:
            return dates, f"yf_adr:{adr}"
    return [], None


# ── Cache + refresh ──────────────────────────────────────────────────────────

def load_calendar(path: str | None = None) -> dict:
    from src.github_persistence import load_json
    path = path or CACHE_PATH   # resuelto en tiempo de llamada (tests monkeypatchean CACHE_PATH)
    cal = load_json(path, default=None)
    if not isinstance(cal, dict) or "tickers" not in cal:
        return {"generated": None, "tickers": {}}
    return cal


def _needs_refresh(cal: dict, tickers: list[str], now: datetime) -> bool:
    gen = cal.get("generated")
    if not gen:
        return True
    try:
        age = (now - datetime.fromisoformat(gen)).days
    except Exception:
        return True
    if age >= REFRESH_DAYS:
        return True
    # FIX 25/09: un refresh fallido (0/84 en el primer run) no puede contar
    # como "fresco" por 7 días -- se reintenta en la próxima corrida.
    total = cal.get("last_refresh_total") or 0
    if total and (cal.get("last_refresh_ok") or 0) / total < MIN_COVERAGE_RETRY:
        return True
    # tickers nuevos en el universo que nunca se intentaron
    return any(t not in cal.get("tickers", {}) for t in tickers)


def refresh_earnings_calendar(tickers: list[str], *, force: bool = False,
                              now: datetime | None = None, fetcher=None,
                              path: str | None = None, push: bool = True,
                              sleep: float = SLEEP_BETWEEN) -> dict:
    """Actualiza el calendario si está vencido (>= REFRESH_DAYS) o hay tickers
    nuevos. Mergea fechas (unión) con lo ya guardado -- nunca pierde fechas
    viejas. Si el fetch de un ticker falla, conserva lo que tenía."""
    from src.github_persistence import save_json
    now = now or datetime.now()
    path = path or CACHE_PATH
    fetcher = fetcher or fetch_earnings_dates_yf
    cal = load_calendar(path)
    if not force and not _needs_refresh(cal, tickers, now):
        return cal

    entries = cal.setdefault("tickers", {})
    _fetch_errors.clear()
    ok = 0
    for i, t in enumerate(sorted(set(tickers))):
        prev = entries.get(t, {})
        try:
            dates, source = _fetch_with_fallback(t, fetcher)
        except Exception as e:
            _fetch_errors.append(f"{t}: {type(e).__name__}: {e}"[:200])
            dates, source = [], None
        merged = sorted(set(prev.get("dates", [])) | set(dates))
        entries[t] = {
            "dates": merged,
            "source": source or prev.get("source"),
            "last_fetch_ok": now.isoformat(timespec="seconds") if dates else prev.get("last_fetch_ok"),
            "last_attempt": now.isoformat(timespec="seconds"),
        }
        ok += bool(dates)
        if sleep and i:
            time.sleep(sleep)

    cal["generated"] = now.isoformat(timespec="seconds")
    cal["pre_blackout_bdays"] = PRE_BLACKOUT_BDAYS
    cal["last_refresh_ok"] = ok
    cal["last_refresh_total"] = len(set(tickers))
    cal["last_error_samples"] = _fetch_errors[:5]
    save_json(path, cal, message=f"auto: earnings_calendar.json {now:%Y-%m-%d}", push=push)
    total = len(set(tickers))
    msg = f"[earnings] Refresh: {ok}/{total} tickers con fechas"
    if total and ok / total < MIN_COVERAGE_RETRY:
        logger.warning(msg + f" -- cobertura baja, se reintenta en la próxima corrida. "
                             f"Errores ({len(_fetch_errors)}): {_fetch_errors[:3]}")
    else:
        logger.info(msg)
    return cal


def sync_calendar_from_github(path: str | None = None) -> dict:
    """Railway: trae la versión fresca que commiteó Actions (sin redeploy,
    mismo patrón que downloader.reload_price_csvs_fresh) y la lee. Si el
    pull falla, usa la copia local del sync de arranque."""
    path = path or CACHE_PATH
    try:
        from src.github_persistence import pull_file
        pull_file(path)
    except Exception as e:
        logger.warning(f"[earnings] pull_file falló, uso copia local: {e}")
    return load_calendar(path)


# ── Campos shadow por señal ──────────────────────────────────────────────────

def compute_earnings_fields(ticker: str, as_of: date | str, cal: dict) -> dict:
    """Campos point-in-time para una señal emitida en `as_of`.

    earnings_days_to: ruedas hábiles (lun-vie) desde as_of hasta el próximo
      resultado; 0 si es hoy. None si no hay fecha futura conocida.
    earnings_blackout_shadow: True/False si se conoce la próxima fecha,
      None si no (desconocido NO es False).
    """
    as_of = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of
    entry = (cal or {}).get("tickers", {}).get(ticker) or {}
    dates = sorted(entry.get("dates", []))
    out = {
        "earnings_next_date": None,
        "earnings_days_to": None,
        "earnings_blackout_shadow": None,
        "earnings_last_date": None,
        "earnings_source": entry.get("source"),
    }
    iso = as_of.isoformat()
    past = [d for d in dates if d < iso]
    future = [d for d in dates if d >= iso]
    if past:
        out["earnings_last_date"] = past[-1]
    if future:
        nxt = future[0]
        days = int(np.busday_count(iso, nxt))
        out["earnings_next_date"] = nxt
        out["earnings_days_to"] = days
        out["earnings_blackout_shadow"] = days <= PRE_BLACKOUT_BDAYS
    return out


def inject_earnings_shadow(signals: list[dict], cal: dict,
                           as_of: date | str | None = None) -> dict:
    """Agrega los campos earnings_* a cada señal (in-place). NO toca
    signal/signal_v2/scores. Devuelve cobertura por mercado para el log."""
    as_of = as_of or date.today()
    cov: dict[str, dict] = {}
    for s in signals:
        s.update(compute_earnings_fields(s.get("ticker", ""), as_of, cal))
        c = cov.setdefault(s.get("mercado", "?"), {"n": 0, "conocido": 0, "blackout": 0})
        c["n"] += 1
        if s["earnings_blackout_shadow"] is not None:
            c["conocido"] += 1
            c["blackout"] += bool(s["earnings_blackout_shadow"])
    return cov
