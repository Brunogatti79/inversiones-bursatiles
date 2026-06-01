"""
src/analyzer.py
 
Motor de análisis: calcula rendimientos, señales del modelo,
rankings y detecta cambios de señal respecto al día anterior.
 
FASE 2 + V2 + 10 MEJORAS (mayo 2026):
- Modelo ponderado: optimizado por mercado
- V2: Asset Quality + Entry Score + R/R + Ranking Accionable
- Mejora 1: Score sectorial dinámico (sensibilidad a macro)
- Mejora 2: Pendiente MA50 + confirmación de volumen
- Mejora 3: R/R con resistencias reales
- Mejora 4: Decay del score macro por antigüedad
- Mejora 5: Horizonte temporal (corto/mediano/swing/evitar)
- Mejora 6: Filtro de liquidez
- Mejora 8: Normalización adaptativa (percentiles)
- Mejora 9: Consenso V1/V2
- Multi-timeframe: penalización escalonada por tendencia semanal
- Cross-market: correlación rolling entre mercados
"""
 
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import os
 
logger = logging.getLogger(__name__)

# ── Pesos optimizados (cargados lazy una vez por proceso) ───────────────────
_OPTIMIZED_WEIGHTS: dict = {}

def _load_optimized_weights_once():
    global _OPTIMIZED_WEIGHTS
    if _OPTIMIZED_WEIGHTS:
        return
    try:
        from src.weight_optimizer import load_optimized_weights
        _OPTIMIZED_WEIGHTS = load_optimized_weights()
    except Exception:
        _OPTIMIZED_WEIGHTS = {}
 
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
 
# Ponderaciones modelo Fase 2 — optimizadas por mercado
W_DEFAULT = {"macro": 0.35, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.20}
W_POR_MERCADO = {
    "MERVAL":  {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
    "BOVESPA": {"macro": 0.25, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.30},
    "SP500":   {"macro": 0.20, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.40},
}
 
# Fallback hardcoded (usado si xlsx no está disponible)
MACRO_SCORES_FALLBACK = {
    "MERVAL": 41.9,
    "BOVESPA": 52.5,
    "SP500": 44.1,
}
 
SECTOR_SCORES_DEFAULT = {
    "FINANCIERO": 40.0,
    "ENERGÍA": 42.0,
    "UTILITIES": 44.0,
    "MATERIALES": 43.0,
    "CONSUMO": 42.0,
    "TELECOM": 45.0,
    "INMOBILIARIO": 42.0,
    "INDUSTRIAL": 43.0,
    "SALUD": 46.0,
    "TECNOLOGÍA": 42.0,
}
 
# Mejora 1: Matriz de sensibilidad sectorial a variables macro
SECTOR_MACRO_SENSITIVITY = {
    "FINANCIERO":    (+0.8, -0.3, +0.5, -0.2),
    "ENERGÍA":       (-0.2, +0.3, +0.7, +0.5),
    "UTILITIES":     (-0.6, -0.2, +0.3, -0.1),
    "MATERIALES":    (-0.3, +0.2, +0.8, +0.4),
    "CONSUMO":       (-0.4, -0.6, +0.5, -0.3),
    "TELECOM":       (-0.3, -0.2, +0.4, -0.1),
    "INMOBILIARIO":  (-0.9, -0.4, +0.6, -0.5),
    "INDUSTRIAL":    (-0.3, +0.1, +0.9, +0.3),
    "SALUD":         (-0.1, -0.1, +0.3, +0.1),
    "TECNOLOGÍA":    (-0.5, -0.2, +0.6, +0.2),
    "GENERAL":       (-0.3, -0.2, +0.5, +0.1),
}
 
# Mejora 6: Umbrales de liquidez mínima (volumen promedio 20d)
LIQUIDITY_PENALTY_THRESHOLD = {
    "MERVAL": 5_000_000,
    "BOVESPA": 10_000_000,
    "SP500": 50_000_000,
}
LIQUIDITY_PENALTY_FACTOR = 0.85
 
# Ponderaciones AQ/ES por mercado
MARKET_WEIGHTS = {
    "MERVAL":  {"aq_weight": 0.60, "es_weight": 0.40},
    "BOVESPA": {"aq_weight": 0.55, "es_weight": 0.45},
    "SP500":   {"aq_weight": 0.50, "es_weight": 0.50},
}
 
AQ_WEIGHTS = {"macro": 0.45, "fundamental": 0.35, "sectorial": 0.20}
ES_WEIGHTS = {"tecnico": 0.55, "dist_max": 0.25, "dist_support": 0.20}
 
SECTOR_MAP = {
    # MERVAL
    "GGAL.BA": "FINANCIERO", "BMA.BA": "FINANCIERO", "SUPV.BA": "FINANCIERO",
    "VALO.BA": "FINANCIERO", "BYMA.BA": "FINANCIERO", "BBAR.BA": "FINANCIERO",
    "PAMP.BA": "ENERGÍA", "CEPU.BA": "ENERGÍA", "TGSU2.BA": "ENERGÍA",
    "YPFD.BA": "ENERGÍA",
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
    "WEGE3.SA": "INDUSTRIAL", "RENT3.SA": "INDUSTRIAL", "EMBR3.SA": "INDUSTRIAL",
    "ABEV3.SA": "CONSUMO", "MGLU3.SA": "CONSUMO", "LREN3.SA": "CONSUMO",
    "HAPV3.SA": "SALUD", "RDOR3.SA": "SALUD",
    "EQTL3.SA": "UTILITIES", "EGIE3.SA": "UTILITIES", "CMIG4.SA": "UTILITIES",
    "CYRE3.SA": "INMOBILIARIO",
    "B3SA3.SA": "FINANCIERO", "ITSA4.SA": "FINANCIERO",
    "SANB11.SA": "FINANCIERO",
    "JBSS3.SA": "CONSUMO", "VIVT3.SA": "TELECOM",
    # SP500
    "AAPL": "TECNOLOGÍA", "MSFT": "TECNOLOGÍA", "NVDA": "TECNOLOGÍA",
    "GOOGL": "TECNOLOGÍA", "META": "TECNOLOGÍA", "AMD": "TECNOLOGÍA",
    "AMZN": "CONSUMO", "WMT": "CONSUMO", "KO": "CONSUMO",
    "MCD": "CONSUMO", "PG": "CONSUMO", "HD": "CONSUMO",
    "TSLA": "CONSUMO", "NFLX": "CONSUMO", "DIS": "CONSUMO",
    "JPM": "FINANCIERO", "BAC": "FINANCIERO", "GS": "FINANCIERO",
    "V": "FINANCIERO", "MA": "FINANCIERO",
    "XOM": "ENERGÍA", "CVX": "ENERGÍA",
    "JNJ": "SALUD", "UNH": "SALUD", "LLY": "SALUD",
    "CAT": "INDUSTRIAL", "BA": "INDUSTRIAL", "GE": "INDUSTRIAL",
    "PLTR": "TECNOLOGÍA", "CRM": "TECNOLOGÍA", "ORCL": "TECNOLOGÍA",
    "BRK-B": "FINANCIERO",
    "MELI": "TECNOLOGÍA", "GLOB": "TECNOLOGÍA",
    "COPX": "MATERIALES", "IBB": "SALUD", "EWZ": "FINANCIERO",
    "RIO": "MATERIALES", "PBR": "ENERGÍA", "QCOM": "TECNOLOGÍA",
}
 
 
# ─────────────────────────────────────────────
# Indicadores técnicos
# ─────────────────────────────────────────────
 
def _rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if len(rsi) >= period else 50.0
 
 
def _momentum(series: pd.Series, period: int = 21) -> float:
    if len(series) < period + 1:
        return 0.0
    return float((series.iloc[-1] / series.iloc[-period] - 1) * 100)
 
 
def _ma_cross(series: pd.Series) -> bool:
    if len(series) < 50:
        return False
    ma20 = series.rolling(20).mean().iloc[-1]
    ma50 = series.rolling(50).mean().iloc[-1]
    return bool(ma20 > ma50)
 
 
def _ma50_slope(series, period=50, lookback=10):
    if len(series) < period + lookback:
        return 0.0
    ma50 = series.rolling(period).mean()
    ma50_now = float(ma50.iloc[-1])
    ma50_prev = float(ma50.iloc[-lookback])
    if ma50_prev <= 0:
        return 0.0
    return round(((ma50_now / ma50_prev) - 1) * 100, 2)
 
 
def _volume_confirmation(volume_series, price_series, lookback=5):
    if volume_series is None or len(volume_series) < lookback + 5:
        return 50.0
    vol_recent = float(volume_series.tail(lookback).mean())
    vol_prev = float(volume_series.iloc[-(lookback*2):-lookback].mean())
    price_change = float(price_series.iloc[-1] / price_series.iloc[-lookback] - 1)
    if vol_prev <= 0:
        return 50.0
    vol_ratio = vol_recent / vol_prev
    if price_change > 0 and vol_ratio > 1.1:
        return min(100.0, 60.0 + vol_ratio * 20)
    elif price_change > 0 and vol_ratio < 0.8:
        return 35.0
    elif price_change < 0 and vol_ratio > 1.3:
        return 20.0
    else:
        return 50.0
 
 
def _atr(high_series, low_series, close_series, period=14):
    if high_series is None or len(high_series) < period + 1:
        return 0.0
    tr1 = high_series - low_series
    tr2 = abs(high_series - close_series.shift(1))
    tr3 = abs(low_series - close_series.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else 0.0
 
 
def _rsi_divergence(series, rsi_period=14, lookback=20):
    if len(series) < rsi_period + lookback + 5:
        return None, 0
    delta = series.diff().dropna()
    gain = delta.clip(lower=0).rolling(rsi_period).mean()
    loss = (-delta.clip(upper=0)).rolling(rsi_period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    rsi_series = rsi_series.dropna()
    if len(rsi_series) < lookback:
        return None, 0
    half = lookback // 2
    price_recent = series.iloc[-half:]
    price_prev = series.iloc[-(lookback):-half]
    rsi_recent = rsi_series.iloc[-half:]
    rsi_prev = rsi_series.iloc[-(lookback):-half]
    if len(price_prev) < 3 or len(rsi_prev) < 3:
        return None, 0
    price_low_prev = float(price_prev.min())
    price_low_recent = float(price_recent.min())
    rsi_low_prev = float(rsi_prev.min())
    rsi_low_recent = float(rsi_recent.min())
    price_high_prev = float(price_prev.max())
    price_high_recent = float(price_recent.max())
    rsi_high_prev = float(rsi_prev.max())
    rsi_high_recent = float(rsi_recent.max())
    if price_low_recent < price_low_prev and rsi_low_recent > rsi_low_prev:
        return "bullish", 10
    if price_high_recent > price_high_prev and rsi_high_recent < rsi_high_prev:
        return "bearish", -10
    return None, 0
 
 
def _volatility_squeeze(series, bb_period=20, lookback=20):
    if len(series) < bb_period + lookback:
        return False, 0
    ma = series.rolling(bb_period).mean()
    std = series.rolling(bb_period).std()
    bb_width = (std * 2) / ma
    bb_width = bb_width.dropna()
    if len(bb_width) < lookback:
        return False, 0
    current_width = float(bb_width.iloc[-1])
    min_width_20d = float(bb_width.iloc[-lookback:].min())
    if current_width <= min_width_20d * 1.10:
        return True, 15
    return False, 0
 
 
def _score_tecnico(rsi: float, momentum: float, ma_cross: bool,
                   ma50_slope: float = 0.0, vol_confirm: float = 50.0) -> float:
    if rsi < 30: rsi_score = 75
    elif rsi < 50: rsi_score = 60
    elif rsi < 65: rsi_score = 55
    elif rsi < 75: rsi_score = 45
    else: rsi_score = 30
 
    if momentum > 15: mom_score = 70
    elif momentum > 5: mom_score = 60
    elif momentum > 0: mom_score = 52
    elif momentum > -5: mom_score = 45
    elif momentum > -15: mom_score = 35
    else: mom_score = 25
 
    cross_score = 65 if ma_cross else 40
 
    if ma50_slope > 2: slope_score = 75
    elif ma50_slope > 0.5: slope_score = 60
    elif ma50_slope > -0.5: slope_score = 50
    elif ma50_slope > -2: slope_score = 35
    else: slope_score = 20
 
    vol_score = vol_confirm
 
    return round(
        rsi_score * 0.25 +
        mom_score * 0.25 +
        cross_score * 0.20 +
        slope_score * 0.15 +
        vol_score * 0.15,
        1
    )
 
 
def _score_to_signal(score: float) -> str:
    if score >= 70:   return "⭐ COMPRA FUERTE"
    elif score >= 58: return "🟢 COMPRA"
    elif score >= 45: return "🟡 NEUTRAL/ESPERAR"
    elif score >= 35: return "🟠 VENTA PARCIAL"
    else:             return "🔴 VENTA"
 
 
# ─────────────────────────────────────────────
# Funciones V2
# ─────────────────────────────────────────────
 
def _calcular_rr(precio, max_52s, min_52s):
    if precio <= 0 or max_52s <= 0 or min_52s <= 0:
        return 0.0, 0.0
    upside = (max_52s - precio) / precio
    downside = (precio - min_52s) / precio
    if downside <= 0.001:
        rr = min(upside / 0.001, 5.0)
    else:
        rr = upside / downside
    rr_norm = min(100.0, max(0.0, (rr / 3.0) * 100.0))
    return round(rr, 2), round(rr_norm, 1)
 
 
def _normalizar_dist_max(dist_max_pct):
    return round(min(100.0, max(0.0, (abs(dist_max_pct) / 40.0) * 100.0)), 1)
 
 
def _asset_quality(score_macro, score_fundamental, score_sectorial):
    return round(
        AQ_WEIGHTS["macro"] * score_macro +
        AQ_WEIGHTS["fundamental"] * score_fundamental +
        AQ_WEIGHTS["sectorial"] * score_sectorial, 1)
 
 
def _weekly_trend(serie_diaria: pd.Series) -> dict:
    """Calcula tendencia semanal como filtro confirmatorio."""
    if len(serie_diaria) < 100:
        return {"weekly_trend": "SIN DATOS", "weekly_rsi": 50.0,
                "weekly_ma_cross": False, "weekly_confirms": True}
    weekly = serie_diaria.resample('W').last().dropna()
    if len(weekly) < 26:
        return {"weekly_trend": "SIN DATOS", "weekly_rsi": 50.0,
                "weekly_ma_cross": False, "weekly_confirms": True}
    ma13w = weekly.rolling(13).mean()
    ma26w = weekly.rolling(26).mean()
    cross = bool(ma13w.iloc[-1] > ma26w.iloc[-1])
    rsi_w = _rsi(weekly)
    if cross and rsi_w > 40:
        trend = "ALCISTA"
    elif not cross and rsi_w < 60:
        trend = "BAJISTA"
    else:
        trend = "LATERAL"
    return {
        "weekly_trend": trend,
        "weekly_rsi": round(rsi_w, 1),
        "weekly_ma_cross": cross,
        "weekly_confirms": trend != "BAJISTA"
    }


def _monthly_trend(serie_diaria: pd.Series) -> dict:
    """Tendencia mensual (MA6M/MA12M) como confirmación de largo plazo."""
    if len(serie_diaria) < 200:
        return {"monthly_trend": "SIN DATOS", "monthly_rsi": 50.0,
                "monthly_ma_cross": False, "monthly_mom_6m": 0.0, "monthly_confirms": True}
    monthly = serie_diaria.resample("ME").last().dropna()
    if len(monthly) < 12:
        return {"monthly_trend": "SIN DATOS", "monthly_rsi": 50.0,
                "monthly_ma_cross": False, "monthly_mom_6m": 0.0, "monthly_confirms": True}
    ma6m  = monthly.rolling(6).mean()
    ma12m = monthly.rolling(12).mean()
    cross = bool(ma6m.iloc[-1] > ma12m.iloc[-1])
    rsi_m = _rsi(monthly)
    if isinstance(rsi_m, float) and np.isnan(rsi_m):
        rsi_m = 50.0
    mom_6m = float((monthly.iloc[-1] / monthly.iloc[-7] - 1) * 100) if len(monthly) >= 7 else 0.0
    if cross and mom_6m > -5:
        trend = "ALCISTA"
    elif not cross and mom_6m < 5:
        trend = "BAJISTA"
    else:
        trend = "LATERAL"
    return {
        "monthly_trend":    trend,
        "monthly_rsi":      round(rsi_m, 1),
        "monthly_ma_cross": cross,
        "monthly_mom_6m":   round(mom_6m, 1),
        "monthly_confirms": trend != "BAJISTA",
    }


def _timeframe_alignment(daily_signal: str, weekly: dict, monthly: dict) -> dict:
    """
    Alineación de los 3 timeframes (diario, semanal, mensual).
    Retorna bonus/penalización para ajustar el score_final.
    """
    is_buy  = any(x in daily_signal for x in ["COMPRA", "FUERTE"])
    is_sell = any(x in daily_signal for x in ["VENTA"])

    wk_ok = weekly.get("weekly_trend")  != "SIN DATOS"
    mo_ok = monthly.get("monthly_trend") != "SIN DATOS"

    if is_buy:
        conf = sum([weekly.get("weekly_trend")  == "ALCISTA" if wk_ok else False,
                    monthly.get("monthly_trend") == "ALCISTA" if mo_ok else False])
        conf_opp = sum([weekly.get("weekly_trend")  == "BAJISTA" if wk_ok else False,
                        monthly.get("monthly_trend") == "BAJISTA" if mo_ok else False])
    elif is_sell:
        conf = sum([weekly.get("weekly_trend")  == "BAJISTA" if wk_ok else False,
                    monthly.get("monthly_trend") == "BAJISTA" if mo_ok else False])
        conf_opp = sum([weekly.get("weekly_trend")  == "ALCISTA" if wk_ok else False,
                        monthly.get("monthly_trend") == "ALCISTA" if mo_ok else False])
    else:
        return {"alignment_score": 50, "alignment_label": "NEUTRAL",
                "alignment_bonus": 0, "alignment_penalty": 0}

    if conf == 2 and conf_opp == 0:
        score, label, bonus, penalty = 95, "TRIPLE CONFIRMACIÓN", 5, 0
    elif conf == 1 and conf_opp == 0:
        score, label, bonus, penalty = 70, "DOBLE CONFIRMACIÓN", 2, 0
    elif conf == 0 and conf_opp == 0:
        score, label, bonus, penalty = 50, "SIN DATOS", 0, 0
    elif conf == 1 and conf_opp == 1:
        score, label, bonus, penalty = 45, "SEÑALES MIXTAS", 0, 3
    elif conf == 0 and conf_opp == 1:
        score, label, bonus, penalty = 30, "CONFLICTO PARCIAL", 0, 5
    else:
        score, label, bonus, penalty = 15, "CONFLICTO TOTAL", 0, 8

    return {"alignment_score": score, "alignment_label": label,
            "alignment_bonus": bonus, "alignment_penalty": penalty}


def _entry_score(score_tecnico, dist_max_norm, dist_support_norm):
    return round(
        ES_WEIGHTS["tecnico"] * score_tecnico +
        ES_WEIGHTS["dist_max"] * dist_max_norm +
        ES_WEIGHTS["dist_support"] * dist_support_norm, 1)
 
 
def _dist_support_norm(precio, soportes):
    if not soportes or precio <= 0:
        return 50.0
    soporte = soportes[0]
    if soporte <= 0:
        return 50.0
    dist_pct = ((precio - soporte) / precio) * 100
    if dist_pct <= 2:
        return 100.0
    elif dist_pct <= 5:
        return round(100 - (dist_pct - 2) * 13.3, 1)
    elif dist_pct <= 10:
        return round(60 - (dist_pct - 5) * 6, 1)
    else:
        return max(10.0, round(30 - (dist_pct - 10) * 2, 1))
 
 
def _score_final_v2(asset_quality, entry_score, market="SP500"):
    w = MARKET_WEIGHTS.get(market, {"aq_weight": 0.50, "es_weight": 0.50})
    return round(w["aq_weight"] * asset_quality + w["es_weight"] * entry_score, 1)
 
 
def _signal_v2(score):
    if score >= 70: return "⭐ COMPRA FUERTE"
    elif score >= 60: return "🟢 COMPRA"
    elif score >= 45: return "🟡 NEUTRAL/ESPERAR"
    elif score >= 35: return "🟠 VENTA PARCIAL"
    else: return "🔴 VENTA"
 
 
def _ranking_accionable(score_v2, asset_quality, rr_norm, volatility_score):
    return round(
        0.50 * score_v2 +
        0.30 * asset_quality +
        0.20 * volatility_score, 1)
 
 
def _volatility_score(series, period=60):
    if len(series) < period:
        return 50.0
    returns = series.pct_change().dropna().tail(period)
    vol = float(returns.std() * np.sqrt(252) * 100)
    if vol <= 10:
        return 90.0
    elif vol <= 20:
        return round(90 - (vol - 10) * 2, 1)
    elif vol <= 40:
        return round(70 - (vol - 20) * 1.5, 1)
    elif vol <= 60:
        return round(40 - (vol - 40) * 1, 1)
    else:
        return 20.0
 
 
def _adx(high_series, low_series, close_series, period=14):
    if high_series is None or len(high_series) < period * 2:
        return 0.0, 50.0
    plus_dm = high_series.diff()
    minus_dm = -low_series.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr1 = high_series - low_series
    tr2 = abs(high_series - close_series.shift(1))
    tr3 = abs(low_series - close_series.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_s = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_s)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_s)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    adx_val = float(dx.rolling(period).mean().iloc[-1])
    if pd.isna(adx_val):
        return 0.0, 50.0
    if adx_val >= 40:
        score = 85.0
    elif adx_val >= 25:
        score = 70.0
    elif adx_val >= 20:
        score = 50.0
    else:
        score = 30.0
    return round(adx_val, 1), round(score, 1)
 
 
# ─────────────────────────────────────────────
# Funciones de mejoras adicionales
# ─────────────────────────────────────────────
 
def _dynamic_sector_score(sector, macro_score, market):
    base = SECTOR_SCORES_DEFAULT.get(sector, 42.0)
    sens = SECTOR_MACRO_SENSITIVITY.get(sector, (0, 0, 0, 0))
    macro_delta = (macro_score - 50) / 50
    adjustment = macro_delta * sum(sens) / len(sens) * 10
    return round(max(20.0, min(70.0, base + adjustment)), 1)
 
 
def _find_levels(serie, window=15):
    if len(serie) < window * 2:
        return [], []
    highs = serie.rolling(window, center=True).max()
    lows = serie.rolling(window, center=True).min()
    precio = float(serie.iloc[-1])
    res = sorted(set([round(float(r), 2) for r in serie[serie == highs].dropna().values if r > precio]))[:3]
    sup = sorted(set([round(float(s), 2) for s in serie[serie == lows].dropna().values if s < precio]), reverse=True)[:3]
    return sup, res
 
 
def _macro_decay_weight(macro_timestamp=None, base_weight=0.50):
    if macro_timestamp is None:
        days_old = 7
    else:
        try:
            if isinstance(macro_timestamp, str):
                macro_dt = datetime.fromisoformat(macro_timestamp)
            else:
                macro_dt = macro_timestamp
            days_old = (datetime.now() - macro_dt).days
        except Exception:
            days_old = 7
    if days_old <= 7:
        decay = 1.0
    elif days_old <= 15:
        decay = 0.85
    elif days_old <= 30:
        decay = 0.65
    else:
        decay = 0.45
    return round(base_weight * decay, 3)
 
 
def _horizonte(asset_quality, entry_score):
    if asset_quality >= 50 and entry_score >= 50:
        return "Corto plazo (1-4 sem)"
    elif asset_quality >= 50 and entry_score < 50:
        return "Mediano plazo (1-3 meses)"
    elif asset_quality < 50 and entry_score >= 50:
        return "Trade corto (swing)"
    else:
        return "Evitar / solo monitorear"
 
 
def _consenso(signal_v1, signal_v2, score_v1, score_v2):
    level_map = {
        "⭐ COMPRA FUERTE": 5, "🟢 COMPRA": 4,
        "🟡 NEUTRAL/ESPERAR": 3,
        "🟠 VENTA PARCIAL": 2, "🔴 VENTA": 1,
    }
    l1 = level_map.get(signal_v1, 3)
    l2 = level_map.get(signal_v2, 3)
    diff = l1 - l2
    if abs(diff) <= 1:
        return "Consenso"
    elif diff >= 2:
        return "V1↑/V2↓ buen activo, mal timing"
    elif diff <= -2:
        return "V1↓/V2↑ activo débil, buen entry"
    return "Consenso"
 
 
# ─────────────────────────────────────────────
# Análisis por mercado — FASE 2 + V2 + MEJORAS
# ─────────────────────────────────────────────
 
def analyze_market(df: pd.DataFrame, market: str, ticker_names: dict,
                   xlsx_signals: dict = None, fund_scores: dict = None) -> list[dict]:
    if df is None or df.empty or len(df) < 5:
        logger.warning(f"[{market}] DataFrame vacío, saltando análisis")
        return []
 
    end = df.index[-1]
    start_12m = end - pd.DateOffset(months=12)
    start_1m  = end - pd.DateOffset(days=30)
    start_1w  = end - pd.DateOffset(days=7)
    df_12m = df[df.index >= start_12m]
 
    # Score macro del xlsx si está disponible, sino fallback
    if xlsx_signals and xlsx_signals.get("macro_scores"):
        macro_score = xlsx_signals["macro_scores"].get(market, MACRO_SCORES_FALLBACK.get(market, 44.0))
        logger.info(f"[{market}] Score macro desde xlsx: {macro_score}")
    else:
        macro_score = MACRO_SCORES_FALLBACK.get(market, 44.0)
        logger.info(f"[{market}] Score macro fallback: {macro_score}")
 
    # Seleccionar pesos optimizados para este mercado
    # Prioridad: archivo optimizados > hardcoded por mercado > default
    _load_optimized_weights_once()
    if _OPTIMIZED_WEIGHTS.get(market):
        W = _OPTIMIZED_WEIGHTS[market]
        logger.debug(f"[{market}] Usando pesos optimizados: {W}")
    else:
        W = W_POR_MERCADO.get(market, W_DEFAULT)
 
    results = []
 
    for ticker, name in ticker_names.items():
        col = name if name in df.columns else (ticker if ticker in df.columns else None)
        if col is None:
            continue
 
        serie = df_12m[col].dropna()
        if len(serie) < 10:
            continue
 
        precio_actual = float(serie.iloc[-1])
        precio_12m = float(serie.iloc[0])
        ret_anual = (precio_actual / precio_12m - 1) * 100
 
        s1m = df[df.index >= start_1m][col].dropna()
        ret_mes = float((precio_actual / s1m.iloc[0] - 1) * 100) if len(s1m) >= 2 else 0.0
 
        s1w = df[df.index >= start_1w][col].dropna()
        ret_sem = float((precio_actual / s1w.iloc[0] - 1) * 100) if len(s1w) >= 2 else 0.0
 
        # Indicadores técnicos base
        rsi = _rsi(serie)
        mom = _momentum(serie)
        ma_cr = _ma_cross(serie)
        # Multi-timeframe: tendencia semanal + mensual
        wt = _weekly_trend(serie)
        mt = _monthly_trend(serie)
 
        # Mejora 2: indicadores adicionales
        ma50_sl = _ma50_slope(serie)
 
        # Intentar obtener volumen
        vol_series = None
        vol_col_candidates = [
            col.replace('Close', 'Volume') if 'Close' in str(col) else None,
            f"{ticker}_Volume",
            "Volume",
        ]
        for vc in vol_col_candidates:
            if vc and vc in df_12m.columns:
                vol_series = df_12m[vc].dropna()
                break
 
        vol_conf = _volume_confirmation(vol_series, serie) if vol_series is not None and len(vol_series) > 10 else 50.0
 
        # Score técnico con 5 componentes (Mejora 2)
        s_tec = _score_tecnico(rsi, mom, ma_cr, ma50_sl, vol_conf)
        # Multi-timeframe: penalización se aplica al score final (más abajo)
 
        sector = SECTOR_MAP.get(ticker, "GENERAL")
 
        # Mejora 1: score sectorial dinámico
        s_sect = _dynamic_sector_score(sector, macro_score, market)
 
        # Score fundamental desde CSV
        s_fund = 50.0
        upside_graham = None
        score_cuant = None
        if fund_scores:
            entry = fund_scores.get(ticker, 50.0)
            if isinstance(entry, dict):
                s_fund = entry.get("score", 50.0)
                upside_graham = entry.get("upside_graham")
                score_cuant = entry.get("score_cuant")
            else:
                s_fund = entry
 
        # Si el xlsx tiene señales precalculadas para este ticker, usarlas como base
        xlsx_ticker = None
        if xlsx_signals and xlsx_signals.get("ticker_scores"):
            xlsx_ticker = xlsx_signals["ticker_scores"].get(ticker)
 
        if xlsx_ticker:
            s_tec_xlsx = xlsx_ticker.get("score_tecnico", s_tec)
            s_sect_xlsx = xlsx_ticker.get("score_sector", s_sect)
            macro_xlsx = xlsx_ticker.get("score_macro", macro_score)
            score_final = round(
                macro_xlsx  * W["macro"] +
                s_tec       * W["tecnico"] +
                s_sect_xlsx * W["sector"] +
                s_fund      * W["fundamental"],
                1
            )
            # Penalización multi-timeframe escalonada al score final
            if wt["weekly_trend"] == "BAJISTA" and wt["weekly_rsi"] < 40:
                score_final = round(score_final * 0.80, 1)
            elif wt["weekly_trend"] == "BAJISTA":
                score_final = round(score_final * 0.90, 1)
            # Penalización mensual adicional
            if mt["monthly_trend"] == "BAJISTA":
                score_final = round(score_final * 0.93, 1)
            signal = _score_to_signal(score_final)
            # Alignment score
            align = _timeframe_alignment(signal, wt, mt)
            score_final = round(min(100, max(0,
                score_final + align["alignment_bonus"] - align["alignment_penalty"]
            )), 1)
            signal = _score_to_signal(score_final)
            rsi_final = xlsx_ticker.get("rsi", rsi)
            mom_final = mom
            ma_cr_final = xlsx_ticker.get("ma_cross", ma_cr)
            logger.debug(f"[{market}] {ticker}: macro={macro_xlsx} tec={s_tec:.1f} sect={s_sect_xlsx} fund={s_fund} → {score_final}")
        else:
            score_final = round(
                macro_score * W["macro"] +
                s_tec       * W["tecnico"] +
                s_sect      * W["sector"] +
                s_fund      * W["fundamental"],
                1
            )
            # Penalización multi-timeframe escalonada al score final
            if wt["weekly_trend"] == "BAJISTA" and wt["weekly_rsi"] < 40:
                score_final = round(score_final * 0.80, 1)
            elif wt["weekly_trend"] == "BAJISTA":
                score_final = round(score_final * 0.90, 1)
            # Penalización mensual adicional
            if mt["monthly_trend"] == "BAJISTA":
                score_final = round(score_final * 0.93, 1)
            signal = _score_to_signal(score_final)
            # Alignment score
            align = _timeframe_alignment(signal, wt, mt)
            score_final = round(min(100, max(0,
                score_final + align["alignment_bonus"] - align["alignment_penalty"]
            )), 1)
            signal = _score_to_signal(score_final)
            rsi_final = rsi
            mom_final = mom
            ma_cr_final = ma_cr
 
        max_val  = float(serie.max())
        min_val  = float(serie.min())
        max_date = serie.idxmax().strftime("%d/%m/%Y")
        min_date = serie.idxmin().strftime("%d/%m/%Y")
 
        # ── V2: cálculos nuevos ──
        dist_max_pct = ((precio_actual - max_val) / max_val) * 100 if max_val > 0 else 0
 
        # Mejora 3: R/R con resistencias reales si es posible
        # Fix R/R máximos: si la resistencia más cercana está muy cerca (<3%)
        # usar el siguiente nivel para evitar R/R colapsado
        try:
            soportes, resistencias = _find_levels(serie)
            rr_stop = soportes[1] if len(soportes) > 1 else (soportes[0] if soportes else min_val)
            if resistencias:
                # Buscar primer nivel con distancia significativa (≥ 3%)
                rr_target = next(
                    (r for r in resistencias if (r - precio_actual) / max(precio_actual, 1) >= 0.03),
                    resistencias[-1]  # fallback: resistencia más lejana
                )
                # Si aún así es demasiado cercana, proyectar con ATR
                if (rr_target - precio_actual) / max(precio_actual, 1) < 0.02:
                    rr_target = precio_actual * 1.06  # target mínimo 6%
            else:
                rr_target = max_val
        except Exception:
            rr_target = max_val
            rr_stop = min_val
            soportes, resistencias = [], []
 
        rr_ratio, rr_norm = _calcular_rr(precio_actual, rr_target, rr_stop)
        dist_max_norm = _normalizar_dist_max(dist_max_pct)
 
        # Distance to support (Fase 2)
        dist_sup_norm = _dist_support_norm(precio_actual, soportes)
 
        aq = _asset_quality(macro_score, s_fund, s_sect)
        es = _entry_score(s_tec, dist_max_norm, dist_sup_norm)
 
        # Indicadores líderes: RSI divergence + Volatility squeeze
        div_type, div_boost = _rsi_divergence(serie)
        is_squeeze, squeeze_boost = _volatility_squeeze(serie)
        # Aplicar boosts al Entry Score (capped 0-100)
        es = round(max(0, min(100, es + div_boost + squeeze_boost)), 1)
 
        # Volatility score y ADX (Fase 2)
        vol_score = _volatility_score(serie)
        adx_val, adx_score = 0.0, 50.0
        try:
            high_col = col.replace('Close', 'High') if 'Close' in str(col) else None
            low_col = col.replace('Close', 'Low') if 'Close' in str(col) else None
            if high_col and low_col and high_col in df_12m.columns and low_col in df_12m.columns:
                adx_val, adx_score = _adx(df_12m[high_col], df_12m[low_col], serie)
        except Exception:
            pass
 
        sf_v2 = _score_final_v2(aq, es, market)
        sig_v2 = _signal_v2(sf_v2)
        rank_acc = _ranking_accionable(sf_v2, aq, rr_norm, vol_score)
 
        # ATR para stops dinámicos
        atr_val = 0.0
        try:
            high_col = col.replace('Close', 'High') if 'Close' in str(col) else None
            low_col = col.replace('Close', 'Low') if 'Close' in str(col) else None
            if high_col and low_col and high_col in df_12m.columns and low_col in df_12m.columns:
                atr_val = _atr(df_12m[high_col], df_12m[low_col], serie)
        except Exception:
            atr_val = 0.0
        atr_stop = round(precio_actual - (atr_val * 2), 2) if atr_val > 0 else 0.0
        atr_target = round(precio_actual + (atr_val * 3), 2) if atr_val > 0 else 0.0
 
        # Mejora 5: horizonte temporal
        horizonte = _horizonte(aq, es)
 
        # Mejora 6: penalización por liquidez
        avg_vol_20d = 0
        liquidity_ok = True
        try:
            if vol_series is not None and len(vol_series) >= 20:
                avg_vol_20d = float(vol_series.tail(20).mean())
                threshold = LIQUIDITY_PENALTY_THRESHOLD.get(market, 0)
                if threshold > 0 and avg_vol_20d < threshold:
                    rank_acc = round(rank_acc * LIQUIDITY_PENALTY_FACTOR, 1)
                    sf_v2 = round(sf_v2 * LIQUIDITY_PENALTY_FACTOR, 1)
                    sig_v2 = _signal_v2(sf_v2)
                    liquidity_ok = False
        except Exception:
            pass
 
        # Mejora 9: consenso V1/V2
        consenso = _consenso(signal, sig_v2, score_final, sf_v2)
 
        results.append({
            "ticker": ticker,
            "empresa": name,
            "sector": sector,
            "mercado": market,
            "precio_actual": round(precio_actual, 2),
            "ret_anual": round(ret_anual, 2),
            "ret_mes": round(ret_mes, 2),
            "ret_sem": round(ret_sem, 2),
            "max_12m": round(max_val, 2),
            "max_12m_date": max_date,
            "min_12m": round(min_val, 2),
            "min_12m_date": min_date,
            "rsi": round(rsi_final, 1),
            "momentum_21d": round(mom_final, 2),
            "ma_cross": ma_cr_final,
            "score_macro": macro_score,
            "score_tecnico": round(s_tec, 1),
            "score_sector": round(s_sect, 1),
            "score_sectorial": round(s_sect, 1),
            "score_fundamental": round(s_fund, 1),
            "score_final": score_final,
            "signal": signal,
            "fecha": end.strftime("%Y-%m-%d"),
            # ── Fundamental detalle ──
            "upside_graham": upside_graham,
            "score_cuant": score_cuant,
            "momentum": round(mom_final, 2),
            # ── V2 ──
            "dist_max_pct": round(dist_max_pct, 1),
            "rr_ratio": rr_ratio,
            "rr_norm": rr_norm,
            "dist_max_norm": dist_max_norm,
            "asset_quality": aq,
            "entry_score": es,
            "score_final_v2": sf_v2,
            "signal_v2": sig_v2,
            "ranking_accionable": rank_acc,
            # ── Mejoras adicionales ──
            "ma50_slope": round(ma50_sl, 2),
            "vol_confirmation": round(vol_conf, 1),
            "horizonte": horizonte,
            "liquidity_ok": liquidity_ok,
            "avg_vol_20d": round(avg_vol_20d, 0),
            "consenso": consenso,
            # ── ATR stops dinámicos ──
            "atr": round(atr_val, 2),
            "atr_stop": atr_stop,
            "atr_target": atr_target,
            # ── Indicadores líderes ──
            "rsi_divergence": div_type,
            "rsi_div_boost": div_boost,
            "volatility_squeeze": is_squeeze,
            "squeeze_boost": squeeze_boost,
            # ── Fase 2: riesgo y tendencia ──
            "volatility_score": round(vol_score, 1),
            "adx": adx_val,
            "adx_score": adx_score,
            "dist_support_norm": round(dist_sup_norm, 1),
            # ── Multi-timeframe ──
            "weekly_trend":    wt["weekly_trend"],
            "weekly_rsi":      wt["weekly_rsi"],
            "weekly_ma_cross": wt["weekly_ma_cross"],
            "weekly_confirms": wt["weekly_confirms"],
            "monthly_trend":    mt["monthly_trend"],
            "monthly_rsi":      mt["monthly_rsi"],
            "monthly_ma_cross": mt["monthly_ma_cross"],
            "monthly_confirms": mt["monthly_confirms"],
            "alignment_score":  align.get("alignment_score", 50),
            "alignment_label":  align.get("alignment_label", ""),
            # ── Factor decomposition (Fase 6) ─────────────────────────────────
            # Contribución de cada factor al score final (en puntos)
            "factor_contrib": {
                "macro":       round(macro_score * W["macro"], 1),
                "tecnico":     round(s_tec       * W["tecnico"], 1),
                "fundamental": round(s_fund      * W["fundamental"], 1),
                "sector":      round(s_sect      * W["sector"], 1),
            },
            "factor_dominante": max(
                [("macro", macro_score * W["macro"]),
                 ("tecnico", s_tec * W["tecnico"]),
                 ("fundamental", s_fund * W["fundamental"]),
                 ("sector", s_sect * W["sector"])],
                key=lambda x: x[1]
            )[0],
        })
 
    results.sort(key=lambda x: x.get("ranking_accionable", x["score_final"]), reverse=True)
    return results
 
 
def detect_signal_changes(current: list[dict], prev_path: str = "data/signals_prev.json") -> list[dict]:
    changes = []
    if not os.path.exists(prev_path):
        logger.info("No hay señales previas para comparar.")
        return changes
    try:
        with open(prev_path) as f:
            prev_data = json.load(f)
        prev_map = {item["ticker"]: item for item in prev_data}
    except Exception as e:
        logger.warning(f"No se pudo leer señales previas: {e}")
        return changes
 
    for item in current:
        prev_item = prev_map.get(item["ticker"])
        if prev_item:
            prev_sig = prev_item.get("signal_v2") or prev_item.get("signal", "")
            curr_sig = item.get("signal_v2") or item.get("signal", "")
            if prev_sig and prev_sig != curr_sig:
                changes.append({
                    "ticker": item["ticker"],
                    "empresa": item["empresa"],
                    "mercado": item["mercado"],
                    "prev_signal": prev_sig,
                    "new_signal": curr_sig,
                    "prev_ranking": prev_item.get("ranking_accionable", 0),
                    "new_ranking": item.get("ranking_accionable", 0),
                })
                logger.info(f"Cambio de señal: {item['ticker']} {prev_sig} → {curr_sig}")
    return changes
 
 
def save_signals(signals: list[dict], path: str = "data/signals_prev.json"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)
    logger.info(f"Señales guardadas en {path}")
 
 
def get_index_stats(df: pd.DataFrame, index_col: str) -> dict:
    end = df.index[-1]
    start = end - pd.DateOffset(months=12)
    serie = df[df.index >= start][index_col].dropna()
    if len(serie) < 2:
        return {}
 
    ret = (serie.iloc[-1] / serie.iloc[0] - 1) * 100
    vol = serie.pct_change().dropna().std() * np.sqrt(252) * 100
    max_val = serie.max()
    min_val = serie.min()
 
    try:
        serie_full = df[index_col].dropna()
        if len(serie_full) >= 2:
            ret_dia = round(float((serie_full.iloc[-1] / serie_full.iloc[-2] - 1) * 100), 2)
        else:
            ret_dia = None
    except Exception:
        ret_dia = None
 
    return {
        "actual": round(float(serie.iloc[-1]), 0),
        "inicio": round(float(serie.iloc[0]), 0),
        "ret_anual": round(float(ret), 2),
        "volatilidad": round(float(vol), 2),
        "max_12m": round(float(max_val), 0),
        "max_12m_date": serie.idxmax().strftime("%d/%m/%Y"),
        "min_12m": round(float(min_val), 0),
        "min_12m_date": serie.idxmin().strftime("%d/%m/%Y"),
        "fecha": end.strftime("%d/%m/%Y"),
        "ret_dia": ret_dia,
        "monthly_labels": _monthly_labels(serie),
        "monthly_values": _monthly_values(serie),
    }
 
 
def _monthly_labels(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [d.strftime("%b-%y") for d in m.index]
 
 
def _monthly_values(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [round(float(v), 2) for v in m.values]
 
 
def get_cross_market_correlation(merval_df, bovespa_df, sp500_df,
                                  merval_col, bovespa_col, sp500_col,
                                  window=21) -> dict:
    """Calcula correlación rolling entre los 3 mercados."""
    try:
        mv = merval_df[merval_col].pct_change().dropna().tail(window * 2)
        bv = bovespa_df[bovespa_col].pct_change().dropna().tail(window * 2)
        sp = sp500_df[sp500_col].pct_change().dropna().tail(window * 2)
        combined = pd.DataFrame({"mv": mv, "bv": bv, "sp": sp}).dropna()
        if len(combined) < window:
            return {"regime": "SIN DATOS", "avg_correlation": 0}
        corr_mv_bv = combined["mv"].rolling(window).corr(combined["bv"]).iloc[-1]
        corr_mv_sp = combined["mv"].rolling(window).corr(combined["sp"]).iloc[-1]
        corr_bv_sp = combined["bv"].rolling(window).corr(combined["sp"]).iloc[-1]
        avg = (corr_mv_bv + corr_mv_sp + corr_bv_sp) / 3
        regime = ("CONVERGENTE" if avg > 0.5 else
                  "DIVERGENTE" if avg < 0.1 else "MIXTO")
        return {
            "regime": regime,
            "avg_correlation": round(float(avg), 2),
            "merval_bovespa": round(float(corr_mv_bv), 2),
            "merval_sp500": round(float(corr_mv_sp), 2),
            "bovespa_sp500": round(float(corr_bv_sp), 2),
        }
    except Exception as e:
        logger.warning(f"[cross_market] Error: {e}")
        return {"regime": "ERROR", "avg_correlation": 0}
 
