"""
src/analyzer.py
Motor de análisis: calcula rendimientos, señales del modelo,
rankings y detecta cambios de señal respecto al día anterior.
 
CAMBIO FASE 2:
  - Integra macro_loader.py: Score Macro dinámico desde xlsx (no hardcoded)
  - Integra fundamental.py: Score Fundamental desde CSV de ratios
  - Nuevo modelo ponderado: 35% Macro + 35% Técnico + 10% Sectorial + 20% Fundamental
  - Si xlsx/csv no están disponibles → fallback a valores hardcoded (sin romper pipeline)
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
 
# Ponderaciones nuevo modelo Fase 2
W_MACRO       = 0.35   # era 0.40
W_TECNICO     = 0.35   # era 0.40
W_SECTOR      = 0.10   # era 0.20
W_FUNDAMENTAL = 0.20   # NUEVO
 
# Fallback hardcoded (usado si xlsx no está disponible)
MACRO_SCORES_FALLBACK = {
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
 
 
def _score_tecnico(rsi: float, momentum: float, ma_cross: bool) -> float:
    if rsi < 30:      rsi_score = 75
    elif rsi < 50:    rsi_score = 60
    elif rsi < 65:    rsi_score = 55
    elif rsi < 75:    rsi_score = 45
    else:             rsi_score = 30
 
    if momentum > 15:      mom_score = 70
    elif momentum > 5:     mom_score = 60
    elif momentum > 0:     mom_score = 52
    elif momentum > -5:    mom_score = 45
    elif momentum > -15:   mom_score = 35
    else:                  mom_score = 25
 
    cross_score = 65 if ma_cross else 40
    return round(rsi_score * 0.35 + mom_score * 0.35 + cross_score * 0.30, 1)
 
 
def _score_to_signal(score: float) -> str:
    if score >= 70:   return "⭐ COMPRA FUERTE"
    elif score >= 58: return "🟢 COMPRA"
    elif score >= 45: return "🟡 NEUTRAL/ESPERAR"
    elif score >= 35: return "🟠 VENTA PARCIAL"
    else:             return "🔴 VENTA"
# ─────────────────────────────────────────────
# Mejoras V2: Asset Quality, Entry Score, R/R
# ─────────────────────────────────────────────

def _calcular_rr(precio, max_52s, min_52s):
    """Ratio Riesgo/Retorno normalizado a 0-100."""
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
    """0% (en el max) = score 0, -40%+ = score 100."""
    return round(min(100.0, max(0.0, (abs(dist_max_pct) / 40.0) * 100.0)), 1)

def _asset_quality(score_macro, score_fundamental, score_sectorial):
    """50% Macro + 30% Fundamental + 20% Sectorial."""
    return round(0.50 * score_macro + 0.30 * score_fundamental + 0.20 * score_sectorial, 1)

def _entry_score(score_tecnico, rr_norm, dist_max_norm):
    """60% Técnico + 25% R/R + 15% Dist Max."""
    return round(0.60 * score_tecnico + 0.25 * rr_norm + 0.15 * dist_max_norm, 1)

def _score_final_v2(asset_quality, entry_score):
    """50% Asset Quality + 50% Entry Score."""
    return round(0.50 * asset_quality + 0.50 * entry_score, 1)

def _signal_v2(score):
    if score >= 70: return "⭐ COMPRA FUERTE"
    elif score >= 60: return "🟢 COMPRA"
    elif score >= 45: return "🟡 NEUTRAL/ESPERAR"
    elif score >= 35: return "🟠 VENTA PARCIAL"
    else: return "🔴 VENTA"

def _ranking_accionable(score_v2, rr_norm):
    """60% Score V2 + 40% R/R Norm."""
    return round(0.60 * score_v2 + 0.40 * rr_norm, 1)
 
 
# ─────────────────────────────────────────────
# Análisis por mercado — FASE 2 con datos reales
# ─────────────────────────────────────────────
 
def analyze_market(df: pd.DataFrame, market: str, ticker_names: dict,
                   xlsx_signals: dict = None, fund_scores: dict = None) -> list[dict]:
    """
    Analiza el mercado integrando:
    - xlsx_signals: señales del modelo del xlsx de Bruno (prioridad)
    - fund_scores: scores fundamentales del CSV de ratios
    - df: precios de Yahoo Finance para cálculos técnicos en tiempo real
    """
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
 
    results = []
    for ticker, name in ticker_names.items():
        col = name if name in df.columns else (ticker if ticker in df.columns else None)
        if col is None:
            continue
 
        serie = df_12m[col].dropna()
        if len(serie) < 10:
            continue
 
        precio_actual = float(serie.iloc[-1])
        precio_12m    = float(serie.iloc[0])
        ret_anual     = (precio_actual / precio_12m - 1) * 100
 
        s1m = df[df.index >= start_1m][col].dropna()
        ret_mes = float((precio_actual / s1m.iloc[0] - 1) * 100) if len(s1m) >= 2 else 0.0
 
        s1w = df[df.index >= start_1w][col].dropna()
        ret_sem = float((precio_actual / s1w.iloc[0] - 1) * 100) if len(s1w) >= 2 else 0.0
 
        # Indicadores técnicos desde precios reales
        rsi   = _rsi(serie)
        mom   = _momentum(serie)
        ma_cr = _ma_cross(serie)
        s_tec = _score_tecnico(rsi, mom, ma_cr)
 
        sector  = SECTOR_MAP.get(ticker, "GENERAL")
        s_sect  = SECTOR_SCORES_DEFAULT.get(sector, 42.0)
 
        # Score fundamental desde CSV
        s_fund = 50.0
        if fund_scores:
            s_fund = fund_scores.get(ticker, 50.0)
 
        # Si el xlsx tiene señales precalculadas para este ticker, usarlas como base
        xlsx_ticker = None
        if xlsx_signals and xlsx_signals.get("ticker_scores"):
            xlsx_ticker = xlsx_signals["ticker_scores"].get(ticker)
 
        if xlsx_ticker:
            # Usar scores del xlsx (calculados por Bruno con modelo completo)
            # pero recalcular técnico en tiempo real desde precios
            s_tec_xlsx = xlsx_ticker.get("score_tecnico", s_tec)
            s_sect_xlsx = xlsx_ticker.get("score_sector", s_sect)
            macro_xlsx  = xlsx_ticker.get("score_macro", macro_score)
 
            # Score final: combina xlsx con fundamental del CSV
            score_final = round(
                macro_xlsx  * W_MACRO +
                s_tec       * W_TECNICO +      # técnico recalculado en tiempo real
                s_sect_xlsx * W_SECTOR +
                s_fund      * W_FUNDAMENTAL,
                1
            )
            signal = _score_to_signal(score_final)
 
            # RSI y momentum del xlsx son más actuales si el xlsx es reciente
            rsi_final = xlsx_ticker.get("rsi", rsi)
            mom_final = xlsx_ticker.get("momentum_21d", mom)
            ma_cr_final = xlsx_ticker.get("ma_cross", ma_cr)
 
            logger.debug(f"[{market}] {ticker}: macro={macro_xlsx} tec={s_tec:.1f} sect={s_sect_xlsx} fund={s_fund} → {score_final}")
        else:
            # Sin datos del xlsx → modelo puramente técnico + fallback
            score_final = round(
                macro_score * W_MACRO +
                s_tec       * W_TECNICO +
                s_sect      * W_SECTOR +
                s_fund      * W_FUNDAMENTAL,
                1
            )
            signal = _score_to_signal(score_final)
            rsi_final = rsi
            mom_final = mom
            ma_cr_final = ma_cr
 
        max_val  = float(serie.max())
        min_val  = float(serie.min())
        max_date = serie.idxmax().strftime("%d/%m/%Y")
        min_date = serie.idxmin().strftime("%d/%m/%Y")
 
        results.append({
# ── V2: cálculos nuevos ──
        dist_max_pct = ((precio_actual - max_val) / max_val) * 100 if max_val > 0 else 0
        rr_ratio, rr_norm = _calcular_rr(precio_actual, max_val, min_val)
        dist_max_norm = _normalizar_dist_max(dist_max_pct)
        aq = _asset_quality(macro_score, s_fund, s_sect)
        es = _entry_score(s_tec, rr_norm, dist_max_norm)
        sf_v2 = _score_final_v2(aq, es)
        sig_v2 = _signal_v2(sf_v2)
        rank_acc = _ranking_accionable(sf_v2, rr_norm)

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
            "score_sector": s_sect,
            "score_fundamental": round(s_fund, 1),
            "score_final": score_final,
            "signal": signal,
            "fecha": end.strftime("%Y-%m-%d"),
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
        })
 
    results.sort(key=lambda x: x["score_final"], reverse=True)
    return results
 
 
def detect_signal_changes(current: list[dict], prev_path: str = "data/signals_prev.json") -> list[dict]:
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
        "actual":         round(float(serie.iloc[-1]), 0),
        "inicio":         round(float(serie.iloc[0]), 0),
        "ret_anual":      round(float(ret), 2),
        "volatilidad":    round(float(vol), 2),
        "max_12m":        round(float(max_val), 0),
        "max_12m_date":   serie.idxmax().strftime("%d/%m/%Y"),
        "min_12m":        round(float(min_val), 0),
        "min_12m_date":   serie.idxmin().strftime("%d/%m/%Y"),
        "fecha":          end.strftime("%d/%m/%Y"),
        "ret_dia":        ret_dia,
        "monthly_labels": _monthly_labels(serie),
        "monthly_values": _monthly_values(serie),
    }
 
 
def _monthly_labels(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [d.strftime("%b-%y") for d in m.index]
 
 
def _monthly_values(serie: pd.Series) -> list:
    m = serie.resample("ME").last()
    return [round(float(v), 2) for v in m.values]
 
