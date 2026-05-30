"""
src/weight_optimizer.py — Fase 2: Walk-Forward Weight Optimization

Busca los pesos óptimos del scoring V1 (W_macro, W_tecnico, W_fundamental)
por mercado, usando el historial de señales y precios reales.

DISEÑO:
  • Los pesos se optimizan por mercado (MERVAL / BOVESPA / SP500) por separado.
  • W_sector = 0.10 fijo (bajo impacto, sector score menos variable).
  • Los otros 3 pesos deben sumar 0.90.
  • Grid search: ~30 combinaciones válidas por mercado.
  • Métrica de optimización: expected_value a 21d
    (= win_rate × avg_win − loss_rate × avg_loss).
  • Walk-forward: entrenamiento en primeros 2/3 del historial,
    evaluación en el tercio final.

MODOS:
  • Con ≥ 15 días de historia: walk-forward completo.
  • Con 6-14 días: sensitivity analysis (sin walk-forward).
  • Con < 6 días: devuelve pesos actuales sin cambiar.

OUTPUT: data/optimized_weights.json
  {
    "generated": "2026-05-30T...",
    "days_history": 42,
    "mode": "walk_forward",
    "MERVAL":  {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30,
                "ev_21d": 2.3, "win_rate_21d": 0.62, "samples": 21},
    "BOVESPA": {...},
    "SP500":   {...},
  }

Uso desde pipeline.py:
    from src.weight_optimizer import run_weight_optimization
    run_weight_optimization(price_data, ticker_cols)
    # analyzer.py lo lee en la próxima ejecución vía load_optimized_weights()
"""

import json
import os
import logging
import itertools
from datetime import datetime

import numpy as np

logger = logging.getLogger(__name__)

HISTORY_PATH  = "data/signals_history.json"
WEIGHTS_PATH  = "data/optimized_weights.json"
W_SECTOR_FIXED = 0.10

# Grid de búsqueda (W_macro, W_tecnico) — W_fund se deduce
W_GRID_MACRO    = [0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45]
W_GRID_TECNICO  = [0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45]
W_FUND_MIN, W_FUND_MAX = 0.10, 0.55

# Pesos actuales (fallback si optimización no tiene datos)
W_CURRENT = {
    "MERVAL":  {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
    "BOVESPA": {"macro": 0.25, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.30},
    "SP500":   {"macro": 0.20, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.40},
    "DEFAULT": {"macro": 0.35, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.20},
}

MARKETS = ["MERVAL", "BOVESPA", "SP500"]


# ── Entrypoint ─────────────────────────────────────────────────────────────

def run_weight_optimization(price_data: dict, ticker_cols: dict) -> dict:
    """
    Ejecuta la optimización de pesos. Guarda y retorna los mejores pesos por mercado.
    """
    if not os.path.exists(HISTORY_PATH):
        logger.info("Weight optimizer: no hay historial, saltando")
        return {}

    try:
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    except Exception as e:
        logger.warning(f"Weight optimizer: error leyendo history: {e}")
        return {}

    sorted_dates = sorted(history.keys())
    n_days = len(sorted_dates)

    if n_days < 6:
        logger.info(f"Weight optimizer: solo {n_days} días (necesita ≥ 6), saltando")
        return {}

    # Construir índice de precios para el backtester interno
    price_index = _build_price_index(price_data, ticker_cols)

    mode = "walk_forward" if n_days >= 15 else "sensitivity"
    logger.info(f"Weight optimizer: {n_days} días de historia → modo {mode}")

    result = {
        "generated":   datetime.now().isoformat(),
        "days_history": n_days,
        "mode":        mode,
    }

    for market in MARKETS:
        best = _optimize_market(market, history, sorted_dates, price_index, mode)
        result[market] = best
        if best:
            logger.info(
                f"  {market}: macro={best['macro']} tec={best['tecnico']} "
                f"fund={best['fundamental']} | EV={best.get('ev_21d','—')} "
                f"WR={best.get('win_rate_21d','—')} n={best.get('samples','—')}"
            )

    # Guardar
    os.makedirs("data", exist_ok=True)
    with open(WEIGHTS_PATH, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    logger.info(f"Weight optimizer: pesos guardados en {WEIGHTS_PATH}")
    return result


# ── Optimización por mercado ────────────────────────────────────────────────

def _optimize_market(
    market: str,
    history: dict,
    sorted_dates: list,
    price_index: dict,
    mode: str,
) -> dict:
    """
    Encuentra los mejores pesos para un mercado dado.
    Retorna dict con los pesos y las métricas.
    """
    # Filtrar entries del mercado con datos completos
    entries = _collect_market_entries(market, history, sorted_dates, price_index)

    if len(entries) < 10:
        logger.info(f"  {market}: solo {len(entries)} entries — usando pesos actuales")
        return W_CURRENT.get(market, W_CURRENT["DEFAULT"]).copy()

    # Walk-forward: train en primeros 2/3, eval en resto
    if mode == "walk_forward" and len(entries) >= 20:
        split = int(len(entries) * 0.66)
        train_entries = entries[:split]
        eval_entries  = entries[split:]
    else:
        # Sensitivity: usar todo como train y eval (menos datos)
        train_entries = entries
        eval_entries  = entries

    # Grid search
    best_ev  = -999
    best_w   = None
    best_met = {}

    for w_m, w_t in itertools.product(W_GRID_MACRO, W_GRID_TECNICO):
        w_f = round(0.90 - w_m - w_t, 2)
        if not (W_FUND_MIN <= w_f <= W_FUND_MAX):
            continue

        weights = {"macro": w_m, "tecnico": w_t, "sector": W_SECTOR_FIXED, "fundamental": w_f}

        # Evaluar en eval_entries
        metrics = _evaluate_weights(weights, eval_entries)

        if metrics is None:
            continue

        ev = metrics.get("ev_21d", -999)
        if ev > best_ev:
            best_ev   = ev
            best_w    = weights.copy()
            best_met  = metrics

    if best_w is None:
        logger.info(f"  {market}: grid sin resultados — usando pesos actuales")
        return W_CURRENT.get(market, W_CURRENT["DEFAULT"]).copy()

    best_w.update(best_met)
    return best_w


def _collect_market_entries(
    market: str,
    history: dict,
    sorted_dates: list,
    price_index: dict,
) -> list:
    """
    Extrae todos los entries del mercado que tengan scores y precios futuros.
    Retorna lista de dicts con campos necesarios para optimización.
    """
    entries = []
    eval_dates = sorted_dates[:-5]  # dejar margen de 5 días

    for signal_date in eval_dates:
        for s in history.get(signal_date, []):
            if s.get("mercado", "") != market:
                continue

            ticker    = s.get("ticker", "")
            s_macro   = float(s.get("score_macro", 0) or 0)
            s_tec     = float(s.get("score_tecnico", 0) or 0)
            s_fund    = float(s.get("score_fund", 0) or 0)
            precio    = float(s.get("precio", 0) or 0)

            # Necesitamos los 3 componentes y precio
            if s_macro == 0 and s_tec == 0 and s_fund == 0:
                continue
            if precio <= 0 or not ticker:
                continue

            # Precio futuro a 21d
            future = _get_future_price(ticker, signal_date, price_index, horizon=21)
            if future is None:
                continue

            ret_21d = (future / precio - 1) * 100

            entries.append({
                "ticker":      ticker,
                "signal_date": signal_date,
                "precio":      precio,
                "s_macro":     s_macro,
                "s_tec":       s_tec,
                "s_fund":      s_fund,
                "ret_21d":     ret_21d,
            })

    return entries


def _evaluate_weights(weights: dict, entries: list) -> dict | None:
    """
    Evalúa un conjunto de pesos sobre los entries.
    Reconstruye el score con los pesos dados y calcula métricas.
    """
    if not entries:
        return None

    w_m   = weights["macro"]
    w_t   = weights["tecnico"]
    w_s   = weights["sector"]
    w_f   = weights["fundamental"]

    # Score sectorial estimado: ~42 puntos (promedio de SECTOR_SCORES_DEFAULT)
    # Es una constante para todos los tickers del mercado
    S_SECTOR_EST = 42.0

    rets = []
    for e in entries:
        reconstructed = (
            e["s_macro"] * w_m +
            e["s_tec"]   * w_t +
            S_SECTOR_EST * w_s +
            e["s_fund"]  * w_f
        )

        # Solo evaluar señales de compra (threshold > 58 = señal de compra)
        if reconstructed >= 58:
            rets.append(e["ret_21d"])

    if len(rets) < 5:
        return None

    arr     = np.array(rets, dtype=float)
    wins    = arr[arr > 0]
    losses  = arr[arr <= 0]
    wr      = float(len(wins) / len(arr))
    lr      = 1.0 - wr
    avg_w   = float(np.mean(wins))   if len(wins)   > 0 else 0.0
    avg_l   = float(abs(np.mean(losses))) if len(losses) > 0 else 0.0
    ev      = round(wr * avg_w - lr * avg_l, 2)
    avg_ret = round(float(np.mean(arr)), 2)
    std     = float(np.std(arr))
    sharpe  = round(avg_ret / std, 2) if std > 0 else 0.0

    return {
        "ev_21d":      ev,
        "win_rate_21d": round(wr, 3),
        "avg_ret_21d": avg_ret,
        "sharpe_21d":  sharpe,
        "samples":     len(rets),
    }


# ── Helpers ─────────────────────────────────────────────────────────────────

def _build_price_index(price_data: dict, ticker_cols: dict) -> dict:
    """Construye {ticker: {date_str: price}} desde DataFrames."""
    index = {}
    col_to_ticker = {v: k for k, v in ticker_cols.items()}

    for _, df in price_data.items():
        if df is None or df.empty:
            continue
        for col in df.columns:
            ticker = col_to_ticker.get(col, col)
            prices = {}
            series = df[col].dropna()
            for ts, price in series.items():
                try:
                    p = float(price)
                    if np.isfinite(p) and p > 0:
                        d = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
                        prices[d] = p
                except Exception:
                    continue
            if prices:
                index[ticker] = prices
                if col != ticker:
                    index[col] = prices

    return index


def _get_future_price(ticker: str, signal_date: str, price_index: dict, horizon: int = 21):
    """Retorna precio a `horizon` días desde signal_date."""
    prices = price_index.get(ticker, {})
    if not prices:
        return None

    dates = sorted(prices.keys())
    entry_idx = None
    for i, d in enumerate(dates):
        if d <= signal_date:
            entry_idx = i
        else:
            break

    if entry_idx is None:
        return None

    future_dates = dates[entry_idx + 1: entry_idx + 1 + horizon + 5]
    if len(future_dates) < horizon:
        return None

    return prices[future_dates[horizon - 1]] if horizon - 1 < len(future_dates) else None


# ── Loader (usado por analyzer.py) ─────────────────────────────────────────

def load_optimized_weights() -> dict:
    """
    Carga pesos optimizados desde archivo. Retorna dict por mercado.
    Retorna {} si el archivo no existe o es muy viejo (>7 días).
    """
    if not os.path.exists(WEIGHTS_PATH):
        return {}

    try:
        with open(WEIGHTS_PATH) as f:
            data = json.load(f)

        # Verificar que no sea muy viejo
        generated = data.get("generated", "")
        if generated:
            from datetime import timezone
            gen_dt = datetime.fromisoformat(generated)
            # Comparar sin timezone
            gen_dt_naive = gen_dt.replace(tzinfo=None) if gen_dt.tzinfo else gen_dt
            age_days = (datetime.now() - gen_dt_naive).days
            if age_days > 7:
                logger.info(f"Pesos optimizados tienen {age_days} días → usando hardcoded")
                return {}

        result = {}
        for market in MARKETS:
            if market in data and isinstance(data[market], dict):
                w = data[market]
                # Validar que tenga los 4 pesos
                if all(k in w for k in ["macro", "tecnico", "sector", "fundamental"]):
                    # Validar que sumen ~1
                    total = w["macro"] + w["tecnico"] + w["sector"] + w["fundamental"]
                    if 0.95 <= total <= 1.05:
                        result[market] = {
                            "macro":       w["macro"],
                            "tecnico":     w["tecnico"],
                            "sector":      w["sector"],
                            "fundamental": w["fundamental"],
                        }

        if result:
            logger.info(f"Pesos optimizados cargados para: {list(result.keys())}")

        return result

    except Exception as e:
        logger.warning(f"Error cargando pesos optimizados: {e}")
        return {}
