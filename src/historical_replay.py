"""
src/historical_replay.py — Backtest histórico con 12 meses de precios

PROBLEMA QUE RESUELVE:
  El weight optimizer y el Kelly necesitan historial estadístico sólido.
  Con solo 60 días de signals_history.json, las métricas son inestables.
  Este módulo genera ~52 snapshots semanales retroactivos sobre los 12 meses
  de precios disponibles, dando ~3.500 observaciones para calibración.

DISEÑO:
  Para cada semana del año pasado (snapshot semanal):
    1. Slice de precio hasta esa fecha (sin lookahead)
    2. Recalcula componentes técnicos en ese punto temporal:
       RSI, momentum, MA cross, MA50 slope, dist_max
    3. Evalúa contra retorno real a 5d / 10d / 21d (datos posteriores)
  Score macro: usa valor actual (sin cambios históricos disponibles)
  Score fundamental: usa valor actual del CSV

OUTPUT:
  data/historical_replay.json — 3.500 observaciones por señal/mercado/sector
  Formato compatible con weight_optimizer._evaluate_weights()

USO desde pipeline.py (1x/semana):
    from src.historical_replay import run_historical_replay
    run_historical_replay(price_data, ticker_cols, macro_scores, fund_scores)
"""

import json
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

REPLAY_PATH  = "data/historical_replay.json"
REPLAY_FREQ  = 7           # días entre snapshots (semanal)
MIN_TRAIN    = 60          # días mínimos de historia para calcular indicadores
HORIZONS     = [5, 10, 21] # días de evaluación futura

# ── Entrypoint principal ────────────────────────────────────────────────

def run_historical_replay(
    price_data:   dict,
    ticker_cols:  dict,
    macro_scores: dict,
    fund_scores:  dict,
) -> dict:
    """
    Genera el historial retroactivo de señales para calibración del optimizer.

    Args:
        price_data:   {"merval": df, "bovespa": df, "sp500": df}
        ticker_cols:  {ticker: col_name}
        macro_scores: {"MERVAL": float, "BOVESPA": float, "SP500": float}
        fund_scores:  {ticker: float}  ← score fundamental 0-100
    """
    # Solo corre 1x/semana para no desperdiciar tiempo de pipeline
    if os.path.exists(REPLAY_PATH):
        age_days = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(REPLAY_PATH))).days
        if age_days < 6:
            logger.info(f"[historical_replay] Archivo reciente ({age_days}d), saltando")
            return _load_replay()

    logger.info("[historical_replay] Iniciando backtest histórico (12 meses)...")

    col_to_ticker = {v: k for k, v in ticker_cols.items()}
    all_observations = []
    total_tickers = 0

    for market_key, df in price_data.items():
        if df is None or df.empty:
            continue

        market_label = {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}.get(market_key, market_key.upper())
        macro_score  = macro_scores.get(market_label, 50.0)

        for col in df.columns:
            ticker = col_to_ticker.get(col, col)
            if any(x in col.upper() for x in ["MERVAL", "BOVESPA", "S&P", "INDEX", "^"]):
                continue  # saltar índices

            serie = df[col].dropna()
            if len(serie) < MIN_TRAIN + max(HORIZONS) + 10:
                continue

            fund_score = float(fund_scores.get(ticker, 50.0) or 50.0)
            observations = _replay_ticker(ticker, serie, market_label, macro_score, fund_score)
            all_observations.extend(observations)
            total_tickers += 1

    if not all_observations:
        logger.warning("[historical_replay] Sin observaciones generadas")
        return {}

    result = {
        "generated":    datetime.now().isoformat(),
        "total_obs":    len(all_observations),
        "total_tickers": total_tickers,
        "observations": all_observations,
    }

    os.makedirs("data", exist_ok=True)
    with open(REPLAY_PATH, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=None)  # compact para ahorrar espacio

    logger.info(
        f"[historical_replay] ✅ {len(all_observations)} observaciones | "
        f"{total_tickers} tickers | {len(all_observations)//total_tickers if total_tickers else 0} obs/ticker avg"
    )
    return result


def _replay_ticker(
    ticker: str,
    serie: pd.Series,
    market: str,
    macro_score: float,
    fund_score: float,
) -> list[dict]:
    """
    Genera observaciones retroactivas para un ticker.
    Cada observación = estado del modelo en fecha t + retorno real a t+N.
    """
    observations = []
    dates = serie.index.tolist()
    n     = len(dates)

    # Iterar semanalmente hacia atrás, dejando margen para la evaluación futura
    eval_end = n - max(HORIZONS) - 2
    step     = REPLAY_FREQ

    for end_idx in range(MIN_TRAIN, eval_end, step):
        # Slice SIN lookahead: solo datos hasta end_idx
        slice_series = serie.iloc[:end_idx]
        if len(slice_series) < MIN_TRAIN:
            continue

        try:
            # Calcular componentes técnicos en este punto temporal
            s_tec    = _calc_tech_score(slice_series)
            dist_max = _calc_dist_max(slice_series)
            precio   = float(slice_series.iloc[-1])

            if precio <= 0 or s_tec is None:
                continue

            # Retornos reales futuros (esto es evaluación, no lookahead del modelo)
            future_rets = {}
            for h in HORIZONS:
                if end_idx + h < n:
                    future_price = float(serie.iloc[end_idx + h])
                    future_rets[f"ret_{h}d"] = round((future_price / precio - 1) * 100, 2)
                else:
                    future_rets[f"ret_{h}d"] = None

            if future_rets.get("ret_21d") is None:
                continue

            observations.append({
                "ticker":    ticker,
                "mercado":   market,
                "date_idx":  end_idx,
                "precio":    round(precio, 2),
                "s_macro":   macro_score,
                "s_tec":     round(s_tec, 1),
                "s_fund":    fund_score,
                "dist_max":  round(dist_max, 1),
                **future_rets,
            })

        except Exception:
            continue

    return observations


# ── Cálculo de componentes técnicos históricos ─────────────────────────

def _calc_tech_score(serie: pd.Series) -> float | None:
    """
    Calcula el score técnico V1 en un punto temporal dado.
    Replica la lógica de _score_tecnico() de analyzer.py.
    """
    if len(serie) < 50:
        return None

    try:
        # RSI(14)
        delta  = serie.diff().dropna()
        gain   = delta.clip(lower=0).rolling(14).mean()
        loss   = (-delta.clip(upper=0)).rolling(14).mean()
        rs     = gain / loss.replace(0, np.nan)
        rsi    = float((100 - 100 / (1 + rs)).iloc[-1])
        if np.isnan(rsi):
            rsi = 50.0

        # RSI score (inverso — contrarian)
        rsi_score = (75 if rsi < 30 else 60 if rsi < 50 else 55 if rsi < 65 else 45 if rsi < 75 else 30)

        # Momentum 21d
        if len(serie) >= 22:
            mom = (float(serie.iloc[-1]) / float(serie.iloc[-22]) - 1) * 100
        else:
            mom = 0.0
        mom_score = (70 if mom > 15 else 60 if mom > 5 else 52 if mom > 0 else 45 if mom > -5 else 35 if mom > -15 else 25)

        # MA cross (MA20 vs MA50)
        ma20  = float(serie.rolling(20).mean().iloc[-1])
        ma50  = float(serie.rolling(50).mean().iloc[-1])
        cross = 65 if ma20 > ma50 else 40

        # MA50 slope
        ma50_series = serie.rolling(50).mean().dropna()
        if len(ma50_series) >= 10:
            slope_pct = (float(ma50_series.iloc[-1]) - float(ma50_series.iloc[-10])) / float(ma50_series.iloc[-10]) * 100
        else:
            slope_pct = 0.0
        slope_score = (75 if slope_pct > 2 else 60 if slope_pct > 0.5 else 50 if slope_pct > -0.5 else 35 if slope_pct > -2 else 20)

        s_tec = (rsi_score * 0.25 + mom_score * 0.25 + cross * 0.20 + slope_score * 0.15 + 50 * 0.15)
        return round(s_tec, 1)

    except Exception:
        return None


def _calc_dist_max(serie: pd.Series) -> float:
    """Distancia normalizada al máximo del período."""
    if len(serie) < 2:
        return 0.0
    max_val   = float(serie.max())
    precio    = float(serie.iloc[-1])
    if max_val <= 0:
        return 0.0
    dist_pct  = (precio - max_val) / max_val * 100  # negativo
    return round(min(100, abs(dist_pct) / 40 * 100), 1)


# ── Loader para weight_optimizer ───────────────────────────────────────

def _load_replay() -> dict:
    """Carga el replay existente."""
    if not os.path.exists(REPLAY_PATH):
        return {}
    try:
        with open(REPLAY_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def load_replay_observations() -> list[dict]:
    """
    Retorna las observaciones del replay para uso en weight_optimizer.
    Formato compatible con _evaluate_weights().
    """
    data = _load_replay()
    return data.get("observations", [])


def get_replay_summary() -> dict:
    """Resumen del replay para logs y dashboard."""
    data = _load_replay()
    if not data:
        return {"available": False}
    return {
        "available":      True,
        "total_obs":      data.get("total_obs", 0),
        "total_tickers":  data.get("total_tickers", 0),
        "generated":      data.get("generated", "")[:10],
    }
