#!/usr/bin/env python3
"""
scripts/backfill_precios_10y.py — Dataset de investigación: 10 años de precios
==============================================================================

SHADOW / INVESTIGACIÓN. No toca los CSV de producción (data/*_cierres.csv),
ni señales, ni Kelly. Escribe en data/research/ (fuera de watchPatterns de
Railway, no dispara redeploy).

Por qué: con 13 meses de historia hay ~10 bloques independientes de 21 ruedas
por mercado — muy poco para validar nada. Con 10 años hay ~120. Esto se corre
UNA vez (y eventualmente se refresca cada tanto); el pipeline diario no lo usa.

Qué baja (Yahoo Finance, ticker por ticker con delays, mismo patrón que
scripts/download_data.py para no chocar con el rate limit):
  data/research/merval_10y.csv          cierre ajustado (auto_adjust=True)
  data/research/bovespa_10y.csv         idem
  data/research/sp500_10y.csv           idem
  data/research/{mercado}_10y_volumen.csv   volumen diario
  data/research/fx_10y.csv              CCL implícito (GGAL, YPF), BRL=X, ARS=X
  data/research/backfill_meta.json      cobertura y control de calidad por ticker

Columnas = ticker de Yahoo (no nombre), más "INDICE". Formato igual a los CSV
de producción: sep ';', decimal ',', utf-8-sig.

El CCL se calcula con cierres SIN ajustar (auto_adjust=False) de la acción
local y el ADR: con precios ajustados por dividendos el cociente se deforma.
  CCL_GGAL = GGAL.BA * 10 / GGAL      (1 ADR = 10 acciones)
  CCL_YPF  = YPFD.BA / YPF            (1 ADR = 1 acción)
  CCL      = mediana de ambos cuando los dos existen

Uso (recomendado): GitHub → Actions → "Backfill research 10y" → Run workflow.
  Corre con yfinance actualizado (el de Railway está fijado en 0.2.54 y
  Yahoo ya no le responde) y commitea data/research/ con git.
Uso alternativo:
  Telegram: /backfill_precios [aplicar]   (depende del yfinance de Railway)
  CLI:      python scripts/backfill_precios_10y.py [--aplicar] [--no-push]

Advertencias que viajan en backfill_meta.json:
  - Sesgo de supervivencia: solo tickers actuales.
  - Yahoo tiene huecos y splits mal ajustados en .BA; el motor neutraliza
    saltos diarios > 45% (clean_panel en diagnostico_ic.py), acá solo se
    cuentan y se reportan.
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    import yfinance as yf
except Exception:  # pragma: no cover - en tests se inyecta un fake
    yf = None

logger = logging.getLogger(__name__)

RESEARCH_DIR = "data/research"
PERIOD = "10y"
DELAY_MIN, DELAY_MAX = 1.5, 3.0
RETRIES = 3
MIN_ROWS = 60
SALTO_SOSPECHOSO = 0.45
N_PRUEBA = 2

FX_TICKERS = {
    "GGAL": "ADR GGAL (USD)",
    "GGAL.BA": "GGAL local (ARS)",
    "YPF": "ADR YPF (USD)",
    "YPFD.BA": "YPF local (ARS)",
    "BRL=X": "BRL por USD",
    "ARS=X": "ARS por USD oficial",
}


def _universo() -> dict:
    """{mercado: (tickers_dict, indice)} desde la fuente oficial del repo."""
    from src import downloader as dl
    return {
        "merval": (dict(dl.MERVAL_TICKERS), dl.MERVAL_INDEX),
        "bovespa": (dict(dl.BOVESPA_TICKERS), dl.BOVESPA_INDEX),
        "sp500": (dict(dl.SP500_TICKERS), dl.SP500_INDEX),
    }


def _bajar(ticker: str, auto_adjust: bool = True) -> pd.DataFrame | None:
    """Historia diaria de un ticker con reintentos. Devuelve DataFrame con
    columnas Close / Volume e índice naive (sin tz), o None."""
    if yf is None:
        raise RuntimeError("yfinance no disponible")
    for intento in range(1, RETRIES + 1):
        try:
            hist = yf.Ticker(ticker).history(period=PERIOD, interval="1d",
                                             auto_adjust=auto_adjust)
            if hist is not None and not hist.empty and len(hist) >= MIN_ROWS:
                hist = hist.copy()
                idx = pd.to_datetime(hist.index)
                if getattr(idx, "tz", None) is not None:
                    idx = idx.tz_localize(None)
                hist.index = idx.normalize()
                hist = hist[~hist.index.duplicated(keep="last")].sort_index()
                cols = [c for c in ("Close", "Volume") if c in hist.columns]
                return hist[cols]
            logger.warning(f"[backfill] {ticker}: vacío o corto (intento {intento})")
        except Exception as e:
            logger.warning(f"[backfill] {ticker}: error {e} (intento {intento})")
        time.sleep(random.uniform(3, 6) * intento)
    return None


def _calidad(s: pd.Series) -> dict:
    s = s.dropna()
    if s.empty:
        return {"filas": 0}
    r = s.pct_change().dropna()
    return {
        "desde": str(s.index[0].date()),
        "hasta": str(s.index[-1].date()),
        "filas": int(len(s)),
        "saltos_sospechosos": int((r.abs() > SALTO_SOSPECHOSO).sum()),
        "precios_no_positivos": int((s <= 0).sum()),
    }


def _guardar_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.index.name = "Fecha"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig",
              date_format="%Y-%m-%d")


def _bajar_mercado(mercado: str, tickers: dict, indice: str, prueba: bool):
    lista = list(tickers.keys())
    if prueba:
        lista = lista[:N_PRUEBA]
    lista = lista + [indice]
    closes, vols, meta, fallidos = {}, {}, {}, []
    for i, t in enumerate(lista):
        h = _bajar(t)
        col = "INDICE" if t == indice else t
        if h is None:
            fallidos.append(t)
            meta[col] = {"filas": 0, "error": "sin datos"}
        else:
            closes[col] = h["Close"]
            if "Volume" in h.columns:
                vols[col] = h["Volume"]
            meta[col] = _calidad(h["Close"])
            logger.info(f"[backfill] {mercado} ✓ {t} ({meta[col]['filas']} filas)")
        if i < len(lista) - 1:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    if not closes:  # todo falló: sin esto pd.DataFrame({}) trae RangeIndex y rompe .dayofweek
        return pd.DataFrame(), pd.DataFrame(), meta, fallidos
    close_df = pd.DataFrame(closes).sort_index()
    vol_df = pd.DataFrame(vols).sort_index()
    close_df = close_df[close_df.index.dayofweek < 5].dropna(how="all")
    vol_df = vol_df.reindex(close_df.index)
    return close_df, vol_df, meta, fallidos


def _bajar_fx() -> tuple[pd.DataFrame, dict]:
    series, meta = {}, {}
    for t in FX_TICKERS:
        h = _bajar(t, auto_adjust=False)  # sin ajustar: el CCL necesita precios crudos
        if h is None:
            meta[t] = {"filas": 0, "error": "sin datos"}
        else:
            series[t] = h["Close"]
            meta[t] = _calidad(h["Close"])
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    if not series:
        return pd.DataFrame(), meta
    fx = pd.DataFrame(series).sort_index()
    fx = fx[fx.index.dayofweek < 5]
    if {"GGAL", "GGAL.BA"} <= set(fx.columns):
        fx["CCL_GGAL"] = fx["GGAL.BA"] * 10 / fx["GGAL"]
    if {"YPF", "YPFD.BA"} <= set(fx.columns):
        fx["CCL_YPF"] = fx["YPFD.BA"] / fx["YPF"]
    ccl_cols = [c for c in ("CCL_GGAL", "CCL_YPF") if c in fx.columns]
    if ccl_cols:
        fx["CCL"] = fx[ccl_cols].median(axis=1, skipna=True)
        meta["CCL"] = _calidad(fx["CCL"])
        # diferencia entre las dos fuentes: si es grande, alguna está rota
        if len(ccl_cols) == 2:
            dif = (fx["CCL_GGAL"] / fx["CCL_YPF"] - 1).abs().dropna()
            meta["CCL"]["dif_mediana_ggal_vs_ypf_pct"] = round(float(dif.median() * 100), 2) if len(dif) else None
            meta["CCL"]["dias_dif_mayor_10pct"] = int((dif > 0.10).sum())
    return fx, meta


def main(aplicar: bool = False, push: bool = True) -> dict:
    prueba = not aplicar
    t0 = time.time()
    resumen = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "modo": "prueba" if prueba else "completo",
        "period": PERIOD,
        "yfinance_version": getattr(yf, "__version__", "desconocida"),
        "advertencias": [
            "Sesgo de supervivencia: solo tickers actuales del universo.",
            "Cierres ajustados por splits y dividendos (auto_adjust=True); "
            "FX y CCL con cierres sin ajustar.",
            "Saltos diarios > 45% se reportan acá y se neutralizan en el motor.",
        ],
        "mercados": {},
        "fx": {},
        "archivos": [],
    }
    destino = RESEARCH_DIR if not prueba else os.path.join(RESEARCH_DIR, "_prueba")

    for mercado, (tickers, indice) in _universo().items():
        close_df, vol_df, meta, fallidos = _bajar_mercado(mercado, tickers, indice, prueba)
        if close_df.empty:
            resumen["mercados"][mercado] = {"error": "sin datos", "fallidos": fallidos}
            continue
        p_close = os.path.join(destino, f"{mercado}_10y.csv")
        p_vol = os.path.join(destino, f"{mercado}_10y_volumen.csv")
        _guardar_csv(close_df, p_close)
        _guardar_csv(vol_df, p_vol)
        resumen["archivos"] += [p_close, p_vol]
        resumen["mercados"][mercado] = {
            "tickers_ok": int(close_df.shape[1]),
            "fallidos": fallidos,
            "desde": str(close_df.index[0].date()),
            "hasta": str(close_df.index[-1].date()),
            "filas": int(len(close_df)),
            "por_ticker": meta,
        }

    total_ok = sum(d.get("tickers_ok", 0) for d in resumen["mercados"].values())
    if total_ok == 0:
        # Yahoo no responde desde este entorno: no seguir bajando FX ni pushear nada
        resumen["error_global"] = (
            f"0 series descargadas en los 3 mercados: Yahoo no responde desde este entorno "
            f"(yfinance {resumen['yfinance_version']}). No se escribió ni pusheó nada."
        )
        resumen["duracion_seg"] = round(time.time() - t0, 1)
        resumen["pusheados"] = []
        resumen["telegram_lines"] = _telegram_lines(resumen)
        return resumen

    fx, meta_fx = _bajar_fx()
    if not fx.empty:
        p_fx = os.path.join(destino, "fx_10y.csv")
        _guardar_csv(fx, p_fx)
        resumen["archivos"].append(p_fx)
    resumen["fx"] = meta_fx

    p_meta = os.path.join(destino, "backfill_meta.json")
    os.makedirs(destino, exist_ok=True)
    resumen["duracion_seg"] = round(time.time() - t0, 1)
    with open(p_meta, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)
    resumen["archivos"].append(p_meta)

    pusheados = []
    if aplicar and push:
        try:
            from src.github_persistence import push_file
            for p in resumen["archivos"]:
                ok = push_file(p, message=f"research: backfill {os.path.basename(p)}")
                pusheados.append((p, bool(ok)))
                time.sleep(1.0)
        except Exception as e:
            logger.warning(f"[backfill] push falló: {e}")
    resumen["pusheados"] = pusheados
    resumen["telegram_lines"] = _telegram_lines(resumen)
    return resumen


def _telegram_lines(r: dict) -> list:
    L = [f"<b>📦 Backfill precios {r['period']} ({r['modo']})</b> — {r.get('duracion_seg', '?')} s · yfinance {r.get('yfinance_version')}"]
    if r.get("error_global"):
        L.append(f"❌ {r['error_global']}")
        return L
    for mk, d in r["mercados"].items():
        if "error" in d:
            L.append(f"<b>{mk.upper()}</b>: ❌ {d['error']}")
            continue
        saltos = sum(v.get("saltos_sospechosos", 0) for v in d["por_ticker"].values())
        cortos = [t for t, v in d["por_ticker"].items() if 0 < v.get("filas", 0) < 1000]
        L.append(
            f"<b>{mk.upper()}</b>: {d['tickers_ok']} series · {d['desde']} → {d['hasta']} "
            f"({d['filas']} ruedas) · saltos &gt;45%: {saltos}"
            + (f"\n  ❌ fallidos: {', '.join(d['fallidos'])}" if d["fallidos"] else "")
            + (f"\n  ⚠️ &lt;4 años: {', '.join(cortos)}" if cortos else "")
        )
    ccl = r["fx"].get("CCL")
    if ccl:
        L.append(f"<b>CCL</b>: {ccl.get('desde')} → {ccl.get('hasta')} · dif. GGAL vs YPF mediana "
                 f"{ccl.get('dif_mediana_ggal_vs_ypf_pct')}% · días &gt;10%: {ccl.get('dias_dif_mayor_10pct')}")
    else:
        L.append("<b>CCL</b>: ❌ no se pudo calcular")
    if r["modo"] == "completo":
        ok = sum(1 for _, v in r.get("pusheados", []) if v)
        L.append(f"GitHub: {ok}/{len(r.get('pusheados', []))} archivos pusheados a data/research/")
        L.append("Siguiente: /diagnostico_ic 10y")
    else:
        L.append("Prueba OK → correr <code>/backfill_precios aplicar</code> (~5-8 min)")
    return L


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out = main(aplicar="--aplicar" in sys.argv, push="--no-push" not in sys.argv)
    print("\n".join(out["telegram_lines"]))
    if out.get("error_global"):
        sys.exit(1)  # que el workflow de GitHub Actions quede en rojo
