"""
src/macro_loader.py
Lee el modelo_macro_micro_señales.xlsx y extrae:
  - Score Macro por mercado (promedio ponderado de las 9 variables)
  - Score Técnico, Sectorial y Score Final por ticker
  - Señal por ticker

El xlsx debe estar en data/modelo_macro_micro_señales.xlsx
Bruno lo actualiza semanalmente con las variables macro reales.
"""

import logging
import os
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

XLSX_PATH = os.getenv("MACRO_XLSX_PATH", "data/modelo_macro_micro_señales.xlsx")

# Mapeo de nombre de hoja → clave de mercado usada en el pipeline
SHEET_MARKET_MAP = {
    "MERVAL Señales":  "MERVAL",
    "BOVESPA Señales": "BOVESPA",
    "S&P500 Señales":  "SP500",
}

# Fallback si no se puede leer el xlsx
MACRO_SCORES_FALLBACK = {
    "MERVAL":  41.9,
    "BOVESPA": 52.5,
    "SP500":   44.1,
}


def _clean_numeric(val):
    """Convierte valores con coma decimal o espacios a float."""
    if pd.isna(val):
        return None
    s = str(val).replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_signal(raw):
    """Normaliza la señal del xlsx al formato del pipeline."""
    if pd.isna(raw):
        return "🟡 NEUTRAL/ESPERAR"
    s = str(raw).upper()
    if "COMPRA FUERTE" in s or "⭐" in s:
        return "⭐ COMPRA FUERTE"
    if "COMPRA" in s or "🟢" in s:
        return "🟢 COMPRA"
    if "VENTA PARCIAL" in s or "🟠" in s:
        return "🟠 VENTA PARCIAL"
    if "VENTA" in s or "🔴" in s:
        return "🔴 VENTA"
    return "🟡 NEUTRAL/ESPERAR"


def load_xlsx_signals(xlsx_path: str = None) -> dict:
    """
    Lee el xlsx y retorna un dict con:
    {
      "macro_scores":  {"MERVAL": 41.9, "BOVESPA": 52.5, "SP500": 44.1},
      "ticker_scores": {
          "GGAL.BA": {
              "score_macro": 41.9, "score_tecnico": 64.3,
              "score_sector": 37.8, "score_final": 50.1,
              "signal": "🟡 NEUTRAL/ESPERAR",
              "rsi": 30.8, "momentum_21d": 1.46, "ma_cross": True,
              "ret_dia": 0.0, "ret_sem": -7.19, "ret_mes": 1.46,
          },
          ...
      }
    }
    """
    path = xlsx_path or XLSX_PATH

    if not os.path.exists(path):
        logger.warning(f"[macro_loader] xlsx no encontrado: {path} — usando fallback hardcoded")
        return {"macro_scores": MACRO_SCORES_FALLBACK, "ticker_scores": {}}

    try:
        # ── Hoja Macro Variables → score macro por mercado ────────────────────
        df_macro = pd.read_excel(path, sheet_name="Macro Variables", engine="openpyxl")
        macro_scores = {}
        for pais, market_key in [("Argentina", "MERVAL"), ("Brasil", "BOVESPA"), ("EE.UU.", "SP500")]:
            rows = df_macro[df_macro["País"] == pais]
            if not rows.empty:
                scores = rows["Score (0-100)"].apply(_clean_numeric).dropna()
                if len(scores) > 0:
                    macro_scores[market_key] = round(float(scores.mean()), 1)
                    logger.info(f"[macro_loader] Score macro {market_key}: {macro_scores[market_key]}")
        # Rellenar faltantes con fallback
        for k, v in MACRO_SCORES_FALLBACK.items():
            macro_scores.setdefault(k, v)

        # ── Hojas de señales → scores por ticker ─────────────────────────────
        ticker_scores = {}
        for sheet_name, market_key in SHEET_MARKET_MAP.items():
            try:
                df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
            except Exception as e:
                logger.warning(f"[macro_loader] No se pudo leer hoja '{sheet_name}': {e}")
                continue

            for _, row in df.iterrows():
                ticker = str(row.get("Ticker", "")).strip()
                if not ticker or ticker == "nan":
                    continue

                ma_raw = row.get("MA20>MA50", "No")
                ma_cross = str(ma_raw).strip().lower() in ("sí", "si", "yes", "true", "1")

                ticker_scores[ticker] = {
                    "mercado":       market_key,
                    "empresa":       str(row.get("Empresa", ticker)),
                    "sector":        str(row.get("Sector", "GENERAL")),
                    "score_macro":   _clean_numeric(row.get("Score Macro"))   or macro_scores.get(market_key, 44.0),
                    "score_tecnico": _clean_numeric(row.get("Score Técnico")) or 50.0,
                    "score_sector":  _clean_numeric(row.get("Score Sectorial")) or 42.0,
                    "score_final":   _clean_numeric(row.get("SCORE FINAL"))   or 50.0,
                    "signal":        _parse_signal(row.get("SEÑAL")),
                    "rsi":           _clean_numeric(row.get("RSI(14)"))        or 50.0,
                    "momentum_21d":  _clean_numeric(row.get("Momentum 21d (%)")) or 0.0,
                    "ma_cross":      ma_cross,
                    "ret_dia":       _clean_numeric(row.get("Var 1d (%)"))     or 0.0,
                    "ret_sem":       _clean_numeric(row.get("Var 5d (%)"))     or 0.0,
                    "ret_mes":       _clean_numeric(row.get("Var 1m (%)"))     or 0.0,
                    "precio_actual": _clean_numeric(row.get("Precio actual"))  or 0.0,
                }

        logger.info(f"[macro_loader] Cargados {len(ticker_scores)} tickers del xlsx")
        return {"macro_scores": macro_scores, "ticker_scores": ticker_scores}

    except Exception as e:
        logger.error(f"[macro_loader] Error leyendo xlsx: {e} — usando fallback")
        return {"macro_scores": MACRO_SCORES_FALLBACK, "ticker_scores": {}}


def get_macro_scores(xlsx_path: str = None) -> dict:
    """Retorna solo los scores macro por mercado."""
    return load_xlsx_signals(xlsx_path)["macro_scores"]


def get_ticker_scores(xlsx_path: str = None) -> dict:
    """Retorna los scores completos por ticker."""
    return load_xlsx_signals(xlsx_path)["ticker_scores"]
