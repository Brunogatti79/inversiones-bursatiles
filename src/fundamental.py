"""
src/fundamental.py
Lee ratios_consolidado_quant.csv y calcula Score Fundamental (0-100)
por ticker para incorporar al modelo de scoring.

El CSV debe estar en data/ratios_consolidado_quant.csv
Bruno lo actualiza con los estados contables de las empresas.

Score Fundamental (0-100):
  Valuación   30%: P/E vs sector, EV/EBITDA vs sector, Upside vs Graham
  Rentabilidad 30%: ROE, Margen EBITDA, Margen Operativo
  Solidez     20%: Deuda/Equity, Current Ratio
  Crecimiento 20%: Crec. Ingresos YoY, Crec. Ganancias YoY
"""

import logging
import os
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

CSV_PATH = os.getenv("RATIOS_CSV_PATH", "data/ratios_consolidado_quant.csv")

# Score cuantitativo base del CSV — rango real: -192 a 9.5
# Lo normalizamos a 0-100
SCORE_CUANT_MIN = -200.0
SCORE_CUANT_MAX = 100.0


def _clean_numeric(val):
    """Convierte valores con coma decimal o espacios a float."""
    if pd.isna(val):
        return None
    s = str(val).replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _normalize_score(raw_score, min_val=SCORE_CUANT_MIN, max_val=SCORE_CUANT_MAX):
    """Normaliza un score a rango 0-100."""
    if raw_score is None:
        return 50.0
    clamped = max(min_val, min(max_val, raw_score))
    return round((clamped - min_val) / (max_val - min_val) * 100, 1)


def _score_fundamental_from_ratios(row, sector_medians: dict) -> float:
    """
    Calcula score fundamental 0-100 desde los ratios individuales.
    Usa medianas sectoriales para comparar valuación relativa.
    """
    score = 50.0  # base neutral
    sector = str(row.get("Sector", "")).upper()

    # ── VALUACIÓN (30%) ───────────────────────────────────────────────────────
    # P/E: menor que mediana sectorial es mejor
    pe = _clean_numeric(row.get("P/E (trailing)"))
    pe_med = sector_medians.get(sector, {}).get("pe", 20.0)
    if pe and pe > 0 and pe_med:
        if pe < pe_med * 0.7:    score += 10   # muy barato
        elif pe < pe_med:        score += 6    # barato
        elif pe > pe_med * 1.5:  score -= 6   # caro

    # EV/EBITDA: menor es mejor
    ev = _clean_numeric(row.get("EV/EBITDA"))
    ev_med = sector_medians.get(sector, {}).get("ev_ebitda", 12.0)
    if ev and ev > 0 and ev_med:
        if ev < ev_med * 0.7:    score += 10
        elif ev < ev_med:        score += 5
        elif ev > ev_med * 1.5:  score -= 5

    # Upside vs Graham: mayor upside = más subvaluado
    upside = _clean_numeric(row.get("Upside vs Graham (%)"))
    if upside is not None:
        if upside > 50:    score += 10
        elif upside > 20:  score += 6
        elif upside > 0:   score += 3
        elif upside < -30: score -= 8

    # ── RENTABILIDAD (30%) ────────────────────────────────────────────────────
    roe = _clean_numeric(row.get("ROE (%)"))
    if roe is not None:
        if roe > 25:    score += 8
        elif roe > 15:  score += 5
        elif roe > 8:   score += 2
        elif roe < 0:   score -= 8

    margen_ebitda = _clean_numeric(row.get("Margen EBITDA (%)"))
    if margen_ebitda is not None:
        if margen_ebitda > 30:   score += 8
        elif margen_ebitda > 15: score += 4
        elif margen_ebitda < 0:  score -= 6

    margen_op = _clean_numeric(row.get("Margen Operativo (%)"))
    if margen_op is not None:
        if margen_op > 20:   score += 4
        elif margen_op > 10: score += 2
        elif margen_op < 0:  score -= 4

    # ── SOLIDEZ (20%) ─────────────────────────────────────────────────────────
    deuda_eq = _clean_numeric(row.get("Deuda/Equity"))
    if deuda_eq is not None and deuda_eq >= 0:
        if deuda_eq < 0.3:   score += 8
        elif deuda_eq < 1.0: score += 4
        elif deuda_eq > 3.0: score -= 6
        elif deuda_eq > 5.0: score -= 10

    current = _clean_numeric(row.get("Current Ratio"))
    if current is not None:
        if current > 2.0:   score += 4
        elif current > 1.2: score += 2
        elif current < 0.8: score -= 6

    # ── CRECIMIENTO (20%) ─────────────────────────────────────────────────────
    rev_growth = _clean_numeric(row.get("Crec. Ingresos YoY (%)"))
    if rev_growth is not None:
        if rev_growth > 20:   score += 6
        elif rev_growth > 10: score += 4
        elif rev_growth > 0:  score += 2
        elif rev_growth < -10: score -= 6

    earn_growth = _clean_numeric(row.get("Crec. Ganancias YoY (%)"))
    if earn_growth is not None:
        if earn_growth > 30:   score += 6
        elif earn_growth > 15: score += 4
        elif earn_growth > 0:  score += 2
        elif earn_growth < -20: score -= 6

    return round(max(0.0, min(100.0, score)), 1)


def _compute_sector_medians(df: pd.DataFrame) -> dict:
    """Calcula medianas de P/E y EV/EBITDA por sector para comparaciones relativas."""
    medians = {}
    for sector, group in df.groupby("Sector"):
        key = str(sector).upper()
        pe_vals = group["P/E (trailing)"].apply(_clean_numeric).dropna()
        ev_vals = group["EV/EBITDA"].apply(_clean_numeric).dropna()
        medians[key] = {
            "pe":       float(pe_vals.median()) if len(pe_vals) > 0 else 20.0,
            "ev_ebitda": float(ev_vals.median()) if len(ev_vals) > 0 else 12.0,
        }
    return medians


def load_fundamental_scores(csv_path: str = None) -> dict:
    """
    Lee el CSV de ratios y retorna dict {ticker: score_fundamental (0-100)}.
    Si el CSV no existe, retorna dict vacío (el pipeline usa 50.0 por defecto).
    """
    path = csv_path or CSV_PATH

    if not os.path.exists(path):
        logger.warning(f"[fundamental] CSV no encontrado: {path} — scores fundamentales desactivados")
        return {}

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8")
        logger.info(f"[fundamental] CSV cargado: {len(df)} filas, {len(df.columns)} columnas")

        sector_medians = _compute_sector_medians(df)
        scores = {}

        for _, row in df.iterrows():
            ticker = str(row.get("Ticker", "")).strip()
            if not ticker or ticker == "nan":
                continue

            # Usar Score Cuantitativo del CSV como base normalizada
            score_cuant_raw = _clean_numeric(row.get("Score Cuantitativo"))
            if score_cuant_raw is not None:
                # Normalizar el score cuantitativo a 0-100
                score_base = _normalize_score(score_cuant_raw)
            else:
                # Calcular desde ratios individuales
                score_base = _score_fundamental_from_ratios(row, sector_medians)

            scores[ticker] = round(score_base, 1)

        logger.info(f"[fundamental] Scores fundamentales calculados para {len(scores)} tickers")
        return scores

    except Exception as e:
        logger.error(f"[fundamental] Error leyendo CSV: {e}")
        return {}


def get_fundamental_score(ticker: str, scores: dict = None) -> float:
    """Retorna el score fundamental de un ticker. 50.0 si no está disponible."""
    if scores is None:
        scores = load_fundamental_scores()
    return scores.get(ticker, 50.0)
