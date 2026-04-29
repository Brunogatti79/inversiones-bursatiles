"""
src/analyzer.py
Motor de análisis: calcula rendimientos, señales del modelo,
rankings y detecta cambios de señal respecto al día anterior.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import os

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constantes del modelo
# ─────────────────────────────────────────────

SIGNAL_LABELS = {
    5: "⭐ COMPRA FUERTE",
    4: "🟢 COMPRA",
    3: "🟡 NEUTRAL/ESPERAR",
    2: "🟠 VENTA PARCIAL",
    1: "🔴 VENTA",
}

SIGNAL_EMOJI = {
    "⭐ COMPRA FUERTE": "⭐",
    "🟢 COMPRA":        "🟢",
    "🟡 NEUTRAL/ESPERAR": "🟡",
    "🟠 VENTA PARCIAL": "🟠",
    "🔴 VENTA":         "🔴",
}

# Pesos del modelo macro-micro-técnico
W_MACRO   = 0.40
W_TECNICO = 0.40
W_SECTOR  = 0.20

# Score macro por mercado (actualizar semanalmente desde modelo xlsx)
# En producción se lee del archivo modelo_macro_micro_señales.xlsx si está disponible
MACRO_SCORES = {
    "MERVAL":  41.9,
    "BOVESPA": 52.5,
    "SP500":   44.1,
}

SECTOR_SCORES_DEFAULT = {
    "FINANCIERO":  40.0,
    "ENERGÍA":     42.0,
    "UTILITIES":   44.0,
    "MATERIALES":  43.0,
    "CONSUMO":     42.0,
    "TELECOM":     45.0,
    "INMOBILIARIO": 42.0,
    "INDUSTRIAL":  43.0,
    "SALUD":       46.0,
    "TECNOLOGÍA":  42.0,
}

SECTOR_MAP = {
    # MERVAL
    "GGAL.BA": "FINANCIERO", "BMA.BA": "FINANCIERO", "SUPV.BA": "FINANCIERO",
    "VALO.BA": "FINANCIERO", "BYMA.BA": "FINANCIERO",
    "PAMP.BA": "ENERGÍA", "CEPU.BA": "ENERGÍA", "TGSU2.BA": "ENERGÍA",
    "TRAN.BA": "UTILITIES", "EDN.BA": "UTILITIES",
    "TXAR.BA": "MATERIALES", "ALUA.BA": "MATERIALES", "LOMA.BA": "MATERIALES",
    "COME.BA": "CONSUMO", "MOLI.BA": "CONSUMO", "MIRG.BA": "CONSUMO",
    "TECO2.BA": "TELECOM",
    "CRES.BA": "INMOBILIARIO", "IRSA.BA": "INMOBILIARIO",
    "HARG.BA": "MATERIALES",
    # BOVESPA
    "PETR4.SA": "ENERGÍA", "RAIZ4.SA": "ENERGÍA",
    "VALE3.SA": "MATERIALES", "CSNA3.SA": "MATERIALES", "SUZB3.SA": "MATERIALES",
    "ITUB4.SA": "FINANCIERO", "BBDC4.SA": "FINANCIERO",
    "BBAS3.SA": "FINANCIERO", "BPAC11.SA": "FINANCIERO",
    "WEGE3.SA": "INDUSTRIAL", "RENT3.SA": "INDUSTRIAL",
    "ABEV3.SA": "CONSUMO", "MGLU3.SA": "CONSUMO", "LREN3.SA": "CONSUMO",
    "HAPV3.SA": "SALUD", "RDOR3.SA": "SALUD",
    "EQTL3.SA": "UTILITIES", "EGIE3.SA": "UTILITIES",
    "CYRE3.SA": "INMOBILIARIO",
    # SP500
    "AAPL": "TECNOLOGÍA", "MSFT": "TECNOLOGÍA", "NVDA": "TECNOLOGÍA",
    "GOOGL": "TECNOLOGÍA", "META": "TECNOLOGÍA", "TSLA": "TECNOLOGÍA",
    "AMZN": "CONSUMO", "WMT": "CONSUMO", "KO": "CONSUMO",
    "MCD": "CONSUMO", "PG": "CONSUMO",
    "JPM": "FINANCIERO", "BAC": "FINANCIERO", "GS": "FINANCIERO", "V": "FINANCIERO",
    "XOM": "ENERGÍA", "CVX": "ENERGÍA",
    "JNJ": "SALUD", "UNH": "SALUD", "LLY": "SALUD",
    "CAT": "INDUSTRIAL", "BA": "INDUSTRIAL", "GE": "INDUSTRIAL",
}


# ─────────────────────────────────────────────
# Cálculo de indicadores técnicos
# ─────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> float:
    """RSI simple."""
    delta = series.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if len(rsi) >= period else 50.0


def _momentum(series: pd.Series, period: int = 21) -> float:
    """Momentum = variación % últimos N días."""
    if len(series) < period + 1:
        return 0.0
    return float((series.iloc[-1] / series.iloc[-period] - 1) * 100)


def _ma_cross(series: pd.Series) -> bool:
    """True si MA20 > MA50."""
    if len(series) < 50:
        return False
    ma20 = series.rolling(20).mean().iloc[-1]
    ma50 = series.rolling(50).mean().iloc[-1]
    return bool(ma20 > ma50)


def _score_tecnico(rsi: float, momentum: float, ma_cross: bool) -> float:
    """Calcula score técnico 0-100."""
    # RSI: 30-70 neutral, <30 sobreventa (oportunidad compra) >70 sobrecompra
    if rsi < 30:
        rsi_score = 75
    elif rsi < 50:
        rsi_score = 60
    elif rsi < 65:
        rsi_score = 55
    elif rsi < 75:
        rsi_score = 45
    else:
        rsi_score = 30

    # Momentum: positivo suma, negativo resta
    if momentum > 15:
        mom_score = 70
    elif momentum > 5:
        mom_score = 60
    elif momentum > 0:
        mom_score = 52
    elif momentum > -5:
        mom_score = 45
    elif momentum > -15:
        mom_score = 35
    else:
        mom_score = 25

    # MA cross
    cross_score = 65 if ma_cross else 40

    return round(rsi_score * 0.35 + mom_score * 0.35 + cross_score * 0.30, 1)


def _score_to_signal(score: float) -> str:
    if score >= 70:
        return "⭐ COMPRA FUERTE"
    elif score >= 58:
        return "🟢 COMPRA"
    elif score >= 45:
        return "🟡 NEUTRAL/ESPERAR"
    elif score >= 35:
        return "🟠 VENTA PARCIAL"
    else:
        return "🔴 VENTA"


# ─────────────────────────────────────────────
# Análisis por mercado
# ─────────────────────────────────────────────

def analyze_market(df: pd.DataFrame, market: str, ticker_names: dict) -> list[dict]:
    """
    Analiza todas las acciones de un mercado.
    Retorna lista de dicts con todas las métricas por acción.
    """
    end = df.index[-1]
    start_12m = end - pd.DateOffset(months=12)
    start_1m  = end - pd.DateOffset(days=30)
    start_1w  = end - pd.DateOffset(days=7)

    df_12m = df[df.index >= start_12m]
    macro_score = MACRO_SCORES.get(market, 44.0)

    # Detectar columna de índice
    index_col = [c for c in df.columns if "ÍNDICE" in c or c.startswith("^")]
    index_col = index_col[0] if index_col else None

    results = []
    for ticker, name in ticker_names.items():
        col = name  # el df ya tiene nombres en lugar de tickers
        if col not in df.columns:
            # fallback: buscar por ticker
            col = ticker if ticker in df.columns else None
        if col is None:
            continue

        serie = df_12m[col].dropna()
        if len(serie) < 10:
            continue

        precio_actual = float(serie.iloc[-1])
        precio_12m    = float(serie.iloc[0])
        ret_anual     = (precio_actual / precio_12m - 1) * 100

        # 1 mes
        s1m = df[df.index >= start_1m][col].dropna()
        ret_mes = float((precio_actual / s1m.iloc[0] - 1) * 100) if len(s1m) >= 2 else 0.0

        # 1 semana
        s1w = df[df.index >= start_1w][col].dropna()
        ret_sem = float((precio_actual / s1w.iloc[0] - 1) * 100) if len(s1w) >= 2 else 0.0

        # Técnicos
        rsi     = _rsi(serie)
        mom     = _momentum(serie)
        ma_cr   = _ma_cross(serie)
        s_tec   = _score_tecnico(rsi, mom, ma_cr)
        sector  = SECTOR_MAP.get(ticker, "GENERAL")
        s_sect  = SECTOR_SCORES_DEFAULT.get(sector, 42.0)

        score_final = round(macro_score * W_MACRO + s_tec * W_TECNICO + s_sect * W_SECTOR, 1)
        signal      = _score_to_signal(score_final)

        # Max / min 12m
        max_val  = float(serie.max())
        min_val  = float(serie.min())
        max_date = serie.idxmax().strftime("%d/%m/%Y")
        min_date = serie.idxmin().strftime("%d/%m/%Y")

        results.append({
            "ticker":         ticker,
            "empresa":        name,
            "sector":         sector,
            "mercado":        market,
            "precio_actual":  round(precio_actual, 2),
            "ret_anual":      round(ret_anual, 2),
            "ret_mes":        round(ret_mes, 2),
            "ret_sem":        round(ret_sem, 2),
            "max_12m":        round(max_val, 2),
            "max_12m_date":   max_date,
            "min_12m":        round(min_val, 2),
            "min_12m_date":   min_date,
            "rsi":            round(rsi, 1),
            "momentum_21d":   round(mom, 2),
            "ma_cross":       ma_cr,
            "score_macro":    macro_score,
            "score_tecnico":  s_tec,
            "score_sector":   s_sect,
            "score_final":    score_final,
            "signal":         signal,
            "fecha":          end.strftime("%Y-%m-%d"),
        })

    results.sort(key=lambda x: x["score_final"], reverse=True)
    return results


def detect_signal_changes(current: list[dict], prev_path: str = "data/signals_prev.json") -> list[dict]:
    """
    Compara señales actuales con las del día anterior.
    Retorna lista de cambios: {ticker, empresa, mercado, prev_signal, new_signal}.
    """
    changes = []

    if not os.path.exists(prev_path):
        logger.info("No hay señales previas para comparar.")
        return changes

    try:
        with open(prev_path) as f:
            prev_data = json.load(f)
        prev_map = {item["ticker"]: item["signal"] for item in prev_data}
    except Exception as e:
        logger.warning(f"No se pudo leer señales previas: {e}")
        return changes

    for item in current:
        prev_sig = prev_map.get(item["ticker"])
        if prev_sig and prev_sig != item["signal"]:
            changes.append({
                "ticker":      item["ticker"],
                "empresa":     item["empresa"],
                "mercado":     item["mercado"],
                "prev_signal": prev_sig,
                "new_signal":  item["signal"],
            })
            logger.info(f"Cambio de señal: {item['ticker']} {prev_sig} → {item['signal']}")

    return changes


def save_signals(signals: list[dict], path: str = "data/signals_prev.json"):
    """Persiste señales actuales para comparación futura."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)
    logger.info(f"Señales guardadas en {path}")


def get_index_stats(df: pd.DataFrame, index_col: str) -> dict:
    """Estadísticas del índice para los últimos 12 meses."""
    end = df.index[-1]
    start = end - pd.DateOffset(months=12)
    serie = df[df.index >= start][index_col].dropna()

    if len(serie) < 2:
    continue

if len(serie) < 2:
    return {}
ret = (serie.iloc[-1] / serie.iloc[0] - 1) * 100
    vol = serie.pct_change().dropna().std() * np.sqrt(252) * 100
    max_val = serie.max()
    min_val = serie.min()

    return {
        "actual":         round(float(serie.iloc[-1]), 0),
        "inicio":         round(float(serie.iloc[0]), 0),
        "ret_anual":      round(float(ret), 2),
        "volatilidad":    round(float(vol), 2),
        "max_12m":        round(float(max_val), 0),
        "max_12m_date":   serie.idxmax().strftime("%d/%m/%Y"),
        "min_12m":        round(float(min_val), 0),
        "min_12m_date":   serie.idxmin().strftime("%d/%m/%Y"),
        "fecha":          end.strftime("%d/%m/%Y"),
        "monthly_labels": _monthly_labels(serie),
        "monthly_values": _monthly_values(serie),
    }


def _monthly_labels(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [d.strftime("%b-%y") for d in m.index]


def _monthly_values(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [round(float(v), 2) for v in m.values]
