"""
src/optimizer.py — Fase 2: Walk-Forward Weight Optimization

Encuentra los pesos óptimos W_macro / W_tecnico / W_sector / W_fundamental
por mercado usando datos históricos reales.

ESTRATEGIA:
  1. Historical Replay (disponible hoy): usa los 12 meses de precios para
     generar ~50 snapshots semanales × 68 tickers = ~3.400 observaciones.
     Calcula component scores en cada snapshot y evalúa contra retorno real +21d.

  2. Walk-Forward (cuando history ≥ 30 días): divide el historial live en
     ventana de calibración (70%) y validación (30%). Optimiza en calib,
     evalúa out-of-sample en validación.

  3. Sensitivity analysis: varía cada peso ±0.05 para identificar cuáles
     impactan más en el EV — filtra dónde enfocar la búsqueda.

ESPACIO DE BÚSQUEDA:
  W_macro:       0.10 – 0.45  (step 0.05)
  W_tecnico:     0.15 – 0.45  (step 0.05)
  W_sector:      0.05 – 0.15  (step 0.05)
  W_fundamental: remainder    (mínimo 0.10)
  Restricción: Σ = 1.00

OBJETIVO:
  Maximizar expected_value de señales COMPRA a 21d.
  EV = win_rate × avg_win - loss_rate × avg_loss

  Bonus si los pesos mejoran también la diferenciación COMPRA vs VENTA
  (señales VENTA con retornos negativos → sistema discrimina bien).

OUTPUT:
  data/optimized_weights.json — pesos óptimos por mercado
  data/optimization_report.json — métricas completas

USO desde pipeline.py:
  from src.optimizer import run_optimization, load_optimized_weights
  optimized = load_optimized_weights()
  if optimized:
      import src.analyzer as _ana
      for mkt, w in optimized.items():
          if mkt in _ana.W_POR_MERCADO:
              _ana.W_POR_MERCADO[mkt].update(w)
"""

import json
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from itertools import product
from typing import Optional

logger = logging.getLogger(__name__)

WEIGHTS_PATH = "data/optimized_weights.json"
REPORT_PATH  = "data/optimization_report.json"
HISTORY_PATH = "data/signals_history.json"

# Cada cuántos días volver a optimizar (evita recalcular en cada run)
OPTIMIZE_EVERY_DAYS = 7

# Restricciones del espacio de búsqueda
SEARCH_SPACE = {
    "macro":       np.arange(0.10, 0.50, 0.05),
    "tecnico":     np.arange(0.15, 0.50, 0.05),
    "sector":      np.arange(0.05, 0.20, 0.05),
    # fundamental = 1 - macro - tecnico - sector (min 0.10)
}
FUND_MIN = 0.10
FUND_MAX = 0.60

# Pesos actuales (referencia para comparación)
W_CURRENT = {
    "MERVAL":  {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
    "BOVESPA": {"macro": 0.25, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.30},
    "SP500":   {"macro": 0.20, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.40},
}


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoints públicos
# ─────────────────────────────────────────────────────────────────────────────

def run_optimization(
    price_data: dict,
    ticker_cols: dict,
    xlsx_signals: dict,
    fund_scores: dict,
    force: bool = False,
) -> dict:
    """
    Ejecuta la optimización de pesos.

    Args:
        price_data:  {"merval": df, "bovespa": df, "sp500": df}
        ticker_cols: {ticker: col_name}
        xlsx_signals: dict con macro_scores y sector_scores
        fund_scores:  {ticker: score_fundamental}
        force:        forzar ejecución aunque no sea el día programado

    Returns:
        dict con pesos optimizados por mercado (vacío si no corresponde correr)
    """
    if not force and not _should_optimize():
        logger.info("[optimizer] No es momento de optimizar — usando pesos previos")
        return load_optimized_weights()

    logger.info("[optimizer] ══ Iniciando optimización de pesos ══")

    macro_scores = xlsx_signals.get("macro_scores", {})
    col_to_ticker = {v: k for k, v in ticker_cols.items()}

    # Construir dataset histórico (replay de 12 meses)
    dataset = _build_historical_dataset(
        price_data, col_to_ticker, macro_scores, fund_scores
    )

    if not dataset:
        logger.warning("[optimizer] Dataset histórico vacío — saltando")
        return load_optimized_weights()

    logger.info(f"[optimizer] Dataset: {len(dataset)} observaciones históricas")

    # Combinar con live history si hay suficiente
    live_data = _load_live_history(price_data, col_to_ticker)
    if live_data:
        dataset = dataset + live_data
        logger.info(f"[optimizer] +{len(live_data)} observaciones live → total {len(dataset)}")

    results_by_market = {}
    report_by_market  = {}

    markets = ["MERVAL", "BOVESPA", "SP500"]
    for market in markets:
        market_data = [d for d in dataset if d["mercado"] == market]
        if len(market_data) < 20:
            logger.info(f"[optimizer] {market}: insuficientes datos ({len(market_data)}), saltando")
            continue

        logger.info(f"[optimizer] {market}: optimizando sobre {len(market_data)} obs...")

        # 1. Sensibilidad
        sensitivity = _sensitivity_analysis(market_data, W_CURRENT[market], market)

        # 2. Grid search
        best_weights, best_metrics, all_results = _grid_search(market_data, market)

        # 3. Walk-forward si hay suficiente historia live
        wf_results = {}
        if len(live_data) >= 30 and any(d["mercado"] == market for d in live_data):
            wf_results = _walk_forward(
                [d for d in live_data if d["mercado"] == market], market
            )

        # Comparar con pesos actuales
        current_metrics = _evaluate_weights(market_data, W_CURRENT[market], market)

        improvement = {
            k: round(best_metrics.get(k, 0) - current_metrics.get(k, 0), 3)
            for k in ["ev_21d", "win_rate_21d", "sharpe_21d"]
        }

        results_by_market[market] = best_weights
        report_by_market[market] = {
            "current_weights":   W_CURRENT[market],
            "optimized_weights": best_weights,
            "improvement":       improvement,
            "current_metrics":   current_metrics,
            "optimized_metrics": best_metrics,
            "sensitivity":       sensitivity,
            "walk_forward":      wf_results,
            "n_observations":    len(market_data),
            "n_combinations_evaluated": len(all_results),
            "top_5_combinations": sorted(
                all_results, key=lambda x: x["ev_21d"], reverse=True
            )[:5],
        }

        logger.info(
            f"[optimizer] {market}: "
            f"macro={best_weights['macro']:.2f} tec={best_weights['tecnico']:.2f} "
            f"sect={best_weights['sector']:.2f} fund={best_weights['fundamental']:.2f} | "
            f"EV: {current_metrics.get('ev_21d', 0):+.2f}% → {best_metrics.get('ev_21d', 0):+.2f}% "
            f"({improvement.get('ev_21d', 0):+.3f})"
        )

    # Guardar resultados
    _save_weights(results_by_market)
    _save_report(report_by_market)

    return results_by_market


def load_optimized_weights() -> dict:
    """Carga pesos optimizados del archivo. Retorna {} si no existe."""
    if not os.path.exists(WEIGHTS_PATH):
        return {}
    try:
        with open(WEIGHTS_PATH) as f:
            data = json.load(f)
        weights = data.get("weights", {})
        logger.info(f"[optimizer] Pesos optimizados cargados: {list(weights.keys())}")
        return weights
    except Exception as e:
        logger.warning(f"[optimizer] Error cargando pesos: {e}")
        return {}


def apply_optimized_weights(weights: dict):
    """
    Aplica pesos optimizados al módulo analyzer (muta W_POR_MERCADO).
    Llamar desde pipeline ANTES de analyze_market.
    """
    if not weights:
        return
    try:
        import src.analyzer as _ana
        for market, w in weights.items():
            if market in _ana.W_POR_MERCADO:
                old = _ana.W_POR_MERCADO[market].copy()
                _ana.W_POR_MERCADO[market].update(w)
                logger.info(f"[optimizer] Pesos {market}: {old} → {w}")
    except Exception as e:
        logger.warning(f"[optimizer] Error aplicando pesos: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del dataset histórico (replay 12 meses)
# ─────────────────────────────────────────────────────────────────────────────

def _build_historical_dataset(
    price_data: dict,
    col_to_ticker: dict,
    macro_scores: dict,
    fund_scores: dict,
) -> list:
    """
    Genera observaciones históricas: component scores + retorno real 21d.
    Snapshots semanales (cada 5 días hábiles) sobre los últimos 12 meses.
    """
    from src.analyzer import (
        _rsi, _momentum, _ma_cross, _ma50_slope, _score_tecnico,
        _dynamic_sector_score, SECTOR_MAP, W_POR_MERCADO,
    )

    market_map = {
        "merval": ("MERVAL", macro_scores.get("MERVAL", 41.9)),
        "bovespa": ("BOVESPA", macro_scores.get("BOVESPA", 52.5)),
        "sp500":   ("SP500",   macro_scores.get("SP500",   44.1)),
    }

    records = []

    for market_key, df in price_data.items():
        if df is None or df.empty or market_key not in market_map:
            continue

        market_name, macro_score = market_map[market_key]

        for col in df.columns:
            ticker = col_to_ticker.get(col, col)

            # Saltar índices (columnas que no son acciones)
            if any(kw in str(col).upper() for kw in ["MERVAL", "BOVESPA", "S&P", "^"]):
                continue

            serie = df[col].dropna()
            if len(serie) < 60:  # mínimo para calcular indicadores confiables
                continue

            # Snapshots semanales (cada 5 días)
            snapshot_indices = range(60, len(serie) - 22, 5)  # dejar 22 días para retorno futuro

            sector = SECTOR_MAP.get(ticker, "GENERAL")
            s_fund = float(fund_scores.get(ticker, 50.0) or 50.0)
            s_sect = _dynamic_sector_score(sector, macro_score, market_name)

            for idx in snapshot_indices:
                try:
                    serie_to_date = serie.iloc[:idx]
                    precio = float(serie_to_date.iloc[-1])
                    if precio <= 0:
                        continue

                    # Componentes técnicos
                    rsi = _rsi(serie_to_date)
                    mom = _momentum(serie_to_date)
                    mac = _ma_cross(serie_to_date)
                    slo = _ma50_slope(serie_to_date)
                    s_tec = _score_tecnico(rsi, mom, mac, slo, 50.0)  # vol_conf=50 neutro

                    # Retorno real a 21d (ground truth)
                    future_price = float(serie.iloc[idx + 21])
                    ret_21d = (future_price / precio - 1) * 100

                    records.append({
                        "ticker":     ticker,
                        "mercado":    market_name,
                        "sector":     sector,
                        "score_macro":  macro_score,
                        "score_tecnico": round(s_tec, 2),
                        "score_sector":  round(s_sect, 2),
                        "score_fund":    round(s_fund, 2),
                        "ret_21d":    round(ret_21d, 3),
                    })

                except (IndexError, ValueError, ZeroDivisionError):
                    continue

    return records


def _load_live_history(price_data: dict, col_to_ticker: dict) -> list:
    """
    Carga observaciones del historial live (signals_history.json).
    Enriquece con retorno real 21d desde los DataFrames.
    """
    if not os.path.exists(HISTORY_PATH):
        return []

    try:
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    except Exception:
        return []

    sorted_dates = sorted(history.keys())
    if len(sorted_dates) < 6:
        return []

    # Índice de precios
    price_index = {}
    for market_key, df in price_data.items():
        if df is None or df.empty:
            continue
        for col in df.columns:
            ticker = col_to_ticker.get(col, col)
            prices = {}
            for ts, p in df[col].dropna().items():
                try:
                    pf = float(p)
                    if np.isfinite(pf) and pf > 0:
                        prices[ts.strftime("%Y-%m-%d")] = pf
                except Exception:
                    continue
            if prices:
                price_index[ticker] = prices
                if col != ticker:
                    price_index[col] = prices

    records = []
    # Excluir últimos 22 días (no hay retorno futuro aún)
    eval_dates = sorted_dates[:-22] if len(sorted_dates) > 22 else []

    for date_str in eval_dates:
        for entry in history.get(date_str, []):
            try:
                ticker   = entry.get("ticker", "")
                mercado  = entry.get("mercado", "")
                s_macro  = float(entry.get("score_macro", 0) or 0)
                s_tec    = float(entry.get("score_tecnico", 0) or 0)
                s_fund   = float(entry.get("score_fund", 0) or 0)
                sector   = entry.get("sector", "GENERAL")
                precio   = float(entry.get("precio", 0) or 0)

                if not mercado or s_macro == 0 or precio <= 0:
                    continue

                # Sector score aproximado
                s_sect = 42.0  # usamos default si no está en history

                # Retorno real 21d
                ticker_prices = price_index.get(ticker, {})
                sorted_p_dates = sorted(ticker_prices.keys())
                try:
                    entry_idx = sorted_p_dates.index(date_str)
                    if entry_idx + 21 < len(sorted_p_dates):
                        future_date = sorted_p_dates[entry_idx + 21]
                        future_price = ticker_prices[future_date]
                        ret_21d = (future_price / precio - 1) * 100
                    else:
                        continue
                except (ValueError, KeyError):
                    continue

                records.append({
                    "ticker":       ticker,
                    "mercado":      mercado,
                    "sector":       sector,
                    "score_macro":  s_macro,
                    "score_tecnico": s_tec,
                    "score_sector":  s_sect,
                    "score_fund":    s_fund,
                    "ret_21d":      round(ret_21d, 3),
                    "source":       "live",
                })

            except Exception:
                continue

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Evaluación y grid search
# ─────────────────────────────────────────────────────────────────────────────

def _score_from_weights(record: dict, weights: dict) -> float:
    """Calcula score final para un registro con pesos dados."""
    return (
        record["score_macro"]    * weights["macro"]
        + record["score_tecnico"] * weights["tecnico"]
        + record["score_sector"]  * weights["sector"]
        + record["score_fund"]    * weights["fundamental"]
    )


def _score_to_signal_label(score: float) -> str:
    if score >= 70: return "COMPRA_FUERTE"
    if score >= 58: return "COMPRA"
    if score >= 45: return "NEUTRAL"
    if score >= 35: return "VENTA_PARCIAL"
    return "VENTA"


def _evaluate_weights(records: list, weights: dict, market: str) -> dict:
    """
    Evalúa una combinación de pesos sobre el dataset.
    Retorna métricas de calidad de señal.
    """
    by_signal: dict[str, list] = {}

    for rec in records:
        score  = _score_from_weights(rec, weights)
        signal = _score_to_signal_label(score)
        by_signal.setdefault(signal, []).append(rec["ret_21d"])

    # Métricas por señal
    signal_metrics = {}
    for sig, rets in by_signal.items():
        arr  = np.array(rets)
        wins = arr[arr > 0]
        loss = arr[arr <= 0]
        wr   = len(wins) / len(arr) if len(arr) > 0 else 0
        lr   = 1 - wr
        aw   = float(np.mean(wins))  if len(wins)  > 0 else 0
        al   = float(abs(np.mean(loss))) if len(loss) > 0 else 0
        ev   = wr * aw - lr * al
        std  = float(np.std(arr))
        signal_metrics[sig] = {
            "n":       len(rets),
            "ev":      round(ev, 3),
            "win_rate": round(wr, 3),
            "avg_ret": round(float(np.mean(arr)), 3),
            "sharpe":  round(float(np.mean(arr)) / std, 3) if std > 0 else 0,
        }

    # Métrica global de discriminación:
    # Queremos: COMPRA_FUERTE > COMPRA > NEUTRAL > VENTA_PARCIAL > VENTA
    compra_ev   = np.mean([signal_metrics.get(s, {}).get("ev", 0)
                           for s in ["COMPRA_FUERTE", "COMPRA"]])
    venta_ev    = np.mean([signal_metrics.get(s, {}).get("ev", 0)
                           for s in ["VENTA", "VENTA_PARCIAL"]])
    neutral_ev  = signal_metrics.get("NEUTRAL", {}).get("ev", 0)

    # Objetivo principal: maximizar EV de compras
    # Bonus: penalizar si ventas tienen EV positivo (el modelo no discrimina)
    discrimination = float(compra_ev) - float(neutral_ev)
    venta_penalty  = max(0, float(venta_ev))  # si ventas ganan, es un problema

    obj = compra_ev - venta_penalty * 0.5

    # Win rate de compras
    compra_wr = np.mean([signal_metrics.get(s, {}).get("win_rate", 0)
                         for s in ["COMPRA_FUERTE", "COMPRA"]])

    # Sharpe de compras
    compra_sharpe = np.mean([signal_metrics.get(s, {}).get("sharpe", 0)
                              for s in ["COMPRA_FUERTE", "COMPRA"]])

    return {
        "ev_21d":         round(float(compra_ev), 3),
        "win_rate_21d":   round(float(compra_wr), 3),
        "sharpe_21d":     round(float(compra_sharpe), 3),
        "discrimination": round(float(discrimination), 3),
        "objective":      round(float(obj), 3),
        "by_signal":      signal_metrics,
    }


def _grid_search(records: list, market: str) -> tuple[dict, dict, list]:
    """
    Busca la combinación de pesos que maximiza el objetivo.
    Retorna (best_weights, best_metrics, all_results).
    """
    best_weights = W_CURRENT.get(market, W_CURRENT["SP500"]).copy()
    best_metrics = _evaluate_weights(records, best_weights, market)
    best_obj     = best_metrics["objective"]
    all_results  = []

    for macro, tecnico, sector in product(
        SEARCH_SPACE["macro"],
        SEARCH_SPACE["tecnico"],
        SEARCH_SPACE["sector"],
    ):
        fundamental = round(1.0 - macro - tecnico - sector, 4)

        # Restricciones
        if fundamental < FUND_MIN or fundamental > FUND_MAX:
            continue
        if macro + tecnico + sector + fundamental > 1.001:
            continue

        weights = {
            "macro":       round(float(macro), 4),
            "tecnico":     round(float(tecnico), 4),
            "sector":      round(float(sector), 4),
            "fundamental": round(float(fundamental), 4),
        }

        metrics = _evaluate_weights(records, weights, market)
        all_results.append({**weights, **metrics})

        if metrics["objective"] > best_obj:
            best_obj     = metrics["objective"]
            best_weights = weights.copy()
            best_metrics = metrics

    logger.info(f"[optimizer] {market}: evaluadas {len(all_results)} combinaciones")
    return best_weights, best_metrics, all_results


# ─────────────────────────────────────────────────────────────────────────────
# Sensitivity analysis
# ─────────────────────────────────────────────────────────────────────────────

def _sensitivity_analysis(records: list, current_weights: dict, market: str) -> dict:
    """
    Varía cada peso ±0.05 y mide el impacto en el objetivo.
    Retorna {componente: sensitivity_score} donde mayor = más impacto.
    """
    base_metrics = _evaluate_weights(records, current_weights, market)
    base_obj     = base_metrics["objective"]
    delta        = 0.05
    sensitivity  = {}

    for component in ["macro", "tecnico", "sector", "fundamental"]:
        impacts = []

        for sign in [+1, -1]:
            tweaked = current_weights.copy()
            new_val = round(tweaked[component] + sign * delta, 4)

            if new_val < 0.05 or new_val > 0.65:
                continue

            # Renormalizar los otros pesos proporcionalmente
            adjustment = tweaked[component] - new_val
            others = [k for k in tweaked if k != component]
            other_sum = sum(tweaked[k] for k in others)

            if other_sum <= 0:
                continue

            tweaked[component] = new_val
            for k in others:
                tweaked[k] = round(tweaked[k] + adjustment * (tweaked[k] / other_sum), 4)

            # Asegurarse que suman a 1
            total = sum(tweaked.values())
            if abs(total - 1.0) > 0.01:
                continue

            m = _evaluate_weights(records, tweaked, market)
            impacts.append(abs(m["objective"] - base_obj))

        sensitivity[component] = round(float(np.mean(impacts)) if impacts else 0, 4)

    # Normalizar a 0-1 (el de mayor impacto = 1.0)
    max_s = max(sensitivity.values()) if sensitivity else 1
    if max_s > 0:
        sensitivity = {k: round(v / max_s, 3) for k, v in sensitivity.items()}

    return sensitivity


# ─────────────────────────────────────────────────────────────────────────────
# Walk-forward (con live history)
# ─────────────────────────────────────────────────────────────────────────────

def _walk_forward(records: list, market: str, n_splits: int = 3) -> dict:
    """
    Divide el historial en ventanas calib/validación y reporta OOS metrics.
    Solo corre si hay ≥ 30 registros.
    """
    if len(records) < 30:
        return {"note": f"Insuficientes datos ({len(records)}) para walk-forward"}

    split_size = len(records) // (n_splits + 1)
    oos_results = []

    for i in range(n_splits):
        calib_end   = split_size * (i + 1)
        val_start   = calib_end
        val_end     = min(calib_end + split_size, len(records))

        calib  = records[:calib_end]
        valid  = records[val_start:val_end]

        if len(calib) < 10 or len(valid) < 5:
            continue

        best_w, best_m, _ = _grid_search(calib, market)
        oos_m = _evaluate_weights(valid, best_w, market)

        oos_results.append({
            "split":           i + 1,
            "calib_n":         len(calib),
            "valid_n":         len(valid),
            "calib_ev":        best_m.get("ev_21d"),
            "oos_ev":          oos_m.get("ev_21d"),
            "degradation":     round(oos_m.get("ev_21d", 0) - best_m.get("ev_21d", 0), 3),
        })

    if not oos_results:
        return {"note": "Sin splits válidos"}

    avg_deg = round(float(np.mean([r["degradation"] for r in oos_results])), 3)
    return {
        "splits":          oos_results,
        "avg_degradation": avg_deg,
        "robust":          avg_deg > -0.5,  # < 0.5% degradación OOS → robusto
    }


# ─────────────────────────────────────────────────────────────────────────────
# Persistencia
# ─────────────────────────────────────────────────────────────────────────────

def _save_weights(weights_by_market: dict):
    os.makedirs("data", exist_ok=True)
    output = {
        "generated": datetime.now().isoformat(),
        "weights":   weights_by_market,
    }
    with open(WEIGHTS_PATH, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"[optimizer] Pesos guardados en {WEIGHTS_PATH}")


def _save_report(report: dict):
    os.makedirs("data", exist_ok=True)
    output = {
        "generated": datetime.now().isoformat(),
        "markets":   report,
    }
    with open(REPORT_PATH, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"[optimizer] Reporte guardado en {REPORT_PATH}")


def _should_optimize() -> bool:
    """Retorna True si han pasado OPTIMIZE_EVERY_DAYS desde la última optimización."""
    if not os.path.exists(WEIGHTS_PATH):
        return True
    try:
        with open(WEIGHTS_PATH) as f:
            data = json.load(f)
        last = datetime.fromisoformat(data.get("generated", "2000-01-01"))
        days_since = (datetime.now() - last).days
        return days_since >= OPTIMIZE_EVERY_DAYS
    except Exception:
        return True
