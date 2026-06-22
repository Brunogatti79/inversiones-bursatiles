"""
src/backtester.py — Fase 0: Framework de Backtesting

Evalúa la efectividad histórica del modelo sobre señales reales guardadas
en signals_history.json, cruzando contra precios reales de los CSVs.

Métricas calculadas:
  • win_rate / avg_ret / avg_win / avg_loss / expected_value / sharpe / max_drawdown
    → por horizonte (5d, 10d, 21d)
  • Exits stop/target: % target hit, % stop hit, % time exit, avg days to exit
  • Accuracy del predictor: dirección correcta 21d, MAE
  • Breakdown por señal, mercado, sector

Diseño:
  • Modo "forward": usa history guardada (hasta 60 días). Disponible inmediatamente.
  • Los stops/targets vienen de atr_stop / atr_target ya calculados en analyzer.py.
  • Requiere que tracker.update_history() guarde los campos extendidos (ver tracker.py).

Uso desde pipeline.py:
    from src.backtester import run_backtest
    backtest_results = run_backtest(price_data, ticker_cols)
"""

import json
import os
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

HISTORY_PATH  = "data/signals_history.json"
RESULTS_PATH  = "data/backtest_results.json"

# Horizontes a evaluar (días hábiles aproximados)
HORIZONS = [5, 10, 21]


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint principal
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(price_data: dict, ticker_cols: dict = None) -> dict:
    """
    Ejecuta backtesting sobre el historial de señales.

    Args:
        price_data:  {"merval": DataFrame, "bovespa": DataFrame, "sp500": DataFrame}
        ticker_cols: {ticker: col_name} — mapeo inverso para buscar precios.
                     Si None, se construye desde price_data.

    Returns:
        dict con métricas completas. También guarda data/backtest_results.json.
    """
    if not os.path.exists(HISTORY_PATH):
        logger.info("Backtester: no hay signals_history.json aún, saltando")
        return {}

    try:
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    except Exception as e:
        logger.warning(f"Backtester: error leyendo history: {e}")
        return {}

    sorted_dates = sorted(history.keys())
    if len(sorted_dates) < 6:
        logger.info(f"Backtester: solo {len(sorted_dates)} días de historia (necesita ≥6), saltando")
        return {}

    # Construir índice de precios: {ticker: {date_str: price}}
    price_index = _build_price_index(price_data, ticker_cols or {})

    # Calcular trades
    trades = _build_trades(history, sorted_dates, price_index)

    if not trades:
        logger.info("Backtester: no hay trades calculables aún (esperando precios futuros)")
        return {}

    # Agregar métricas
    results = {
        "generated":        datetime.now().isoformat(),
        "total_trades":     len(trades),
        "days_history":     len(sorted_dates),
        "by_signal":        _aggregate_by(trades, "signal"),
        "by_market":        _aggregate_by(trades, "mercado"),
        "by_sector":        _aggregate_by(trades, "sector"),
        "predictor":        _predictor_accuracy(trades),
        "stop_target":      _stop_target_global(trades),
        "top_performers":   _top_bottom(trades, top=True),
        "worst_performers": _top_bottom(trades, top=False),
        "signal_summary":   _signal_summary_table(trades),
    }

    # Guardar
    os.makedirs("data", exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    _push_backtest_to_github()
    _log_summary(results)
    return results


def _push_backtest_to_github():
    """Pushea backtest_results.json a GitHub (filesystem de Railway es efímero)."""
    from src.github_persistence import push_file
    push_file(RESULTS_PATH, f"auto: backtest_results {datetime.now().strftime('%Y-%m-%d %H:%M')}")


# ─────────────────────────────────────────────────────────────────────────────
# Construcción del índice de precios
# ─────────────────────────────────────────────────────────────────────────────

def _build_price_index(price_data: dict, ticker_cols: dict) -> dict:
    """
    Retorna {ticker: {date_str: price}} combinando todos los DataFrames.

    ticker_cols: {ticker: col_name_en_dataframe}  — provisto por pipeline.
    Si no está disponible, intenta mapear directamente por nombre de columna.
    """
    index = {}  # ticker → {date: price}

    # Índice inverso: col_name → ticker
    col_to_ticker = {v: k for k, v in ticker_cols.items()}

    for market_key, df in price_data.items():
        if df is None or df.empty:
            continue

        for col in df.columns:
            # Determinar ticker para esta columna
            ticker = col_to_ticker.get(col, col)

            prices_by_date = {}
            series = df[col].dropna()
            for ts, price in series.items():
                try:
                    price_f = float(price)
                    if np.isfinite(price_f) and price_f > 0:
                        date_str = ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
                        prices_by_date[date_str] = price_f
                except Exception:
                    continue

            if prices_by_date:
                index[ticker] = prices_by_date
                # También indexar por col_name por si acaso
                if col != ticker:
                    index[col] = prices_by_date

    logger.info(f"Backtester: índice de precios construido — {len(index)} series")
    return index


# ─────────────────────────────────────────────────────────────────────────────
# Construcción de trades
# ─────────────────────────────────────────────────────────────────────────────

def _build_trades(history: dict, sorted_dates: list, price_index: dict) -> list:
    """
    Para cada señal en cada fecha, calcula los outcomes.
    Excluye los últimos 5 días (no hay suficiente futuro aún).
    """
    trades = []
    evaluation_dates = sorted_dates[:-5] if len(sorted_dates) > 5 else []

    for signal_date in evaluation_dates:
        entries = history.get(signal_date, [])

        for entry in entries:
            ticker        = entry.get("ticker", "")
            signal        = entry.get("signal_v2") or entry.get("signal", "")
            precio_entry  = float(entry.get("precio", 0) or 0)
            atr_stop      = float(entry.get("atr_stop", 0) or 0)
            atr_target    = float(entry.get("atr_target", 0) or 0)
            sector        = entry.get("sector", "GENERAL")
            mercado       = entry.get("mercado", "")
            pred_21d      = entry.get("pred_21d")
            pred_signal   = entry.get("pred_signal", "")
            score_v1      = float(entry.get("score_v1", 0) or 0)
            score_v2      = float(entry.get("score_v2", 0) or 0)

            if not signal or precio_entry <= 0 or not ticker:
                continue

            # Obtener precios futuros
            future_prices = _get_future_prices(ticker, signal_date, price_index, max_horizon=25)
            if not future_prices:
                continue

            trade = {
                "ticker":       ticker,
                "mercado":      mercado,
                "sector":       sector,
                "signal":       signal,
                "signal_date":  signal_date,
                "precio_entry": precio_entry,
                "atr_stop":     atr_stop,
                "atr_target":   atr_target,
                "score_v1":     score_v1,
                "score_v2":     score_v2,
                "pred_21d":     pred_21d,
                "pred_signal":  pred_signal,
            }

            # Retornos hold-to-horizon
            for h in HORIZONS:
                if len(future_prices) >= h:
                    exit_price = future_prices[h - 1]
                    ret = (exit_price / precio_entry - 1) * 100
                    trade[f"ret_{h}d"]        = round(ret, 2)
                    trade[f"exit_price_{h}d"] = round(exit_price, 2)
                else:
                    trade[f"ret_{h}d"]        = None
                    trade[f"exit_price_{h}d"] = None

            # Exit con stop/target
            st = _calc_stop_target_exit(
                future_prices, precio_entry, atr_stop, atr_target, max_days=21
            )
            trade.update(st)

            trades.append(trade)

    logger.info(f"Backtester: {len(trades)} trades calculados desde {len(evaluation_dates)} fechas")
    return trades


def _get_future_prices(ticker: str, signal_date: str, price_index: dict, max_horizon: int = 25) -> list:
    """
    Retorna lista de precios futuros (signal_date+1 en adelante).
    """
    ticker_prices = price_index.get(ticker, {})
    if not ticker_prices:
        return []

    sorted_dates = sorted(ticker_prices.keys())

    # Posición de signal_date (o la más cercana anterior)
    entry_idx = None
    for i, d in enumerate(sorted_dates):
        if d <= signal_date:
            entry_idx = i
        elif entry_idx is not None:
            break

    if entry_idx is None:
        return []

    future_dates = sorted_dates[entry_idx + 1: entry_idx + 1 + max_horizon]
    return [ticker_prices[d] for d in future_dates]


def _calc_stop_target_exit(future_prices: list, entry_price: float,
                            atr_stop: float, atr_target: float, max_days: int = 21) -> dict:
    """
    Simula exit con stop/target. Retorna tipo de exit, retorno y día.
    """
    if not future_prices or entry_price <= 0:
        return {"st_exit_type": "no_data", "st_ret": None, "st_exit_day": None, "st_exit_price": None}

    has_stop   = (atr_stop   > 0 and atr_stop   < entry_price)
    has_target = (atr_target > 0 and atr_target > entry_price)

    for day_idx, price in enumerate(future_prices[:max_days], 1):
        # Stop tiene prioridad (risk management primero)
        if has_stop and price <= atr_stop:
            return {
                "st_exit_type":  "stop",
                "st_ret":        round((price / entry_price - 1) * 100, 2),
                "st_exit_day":   day_idx,
                "st_exit_price": round(price, 2),
            }
        if has_target and price >= atr_target:
            return {
                "st_exit_type":  "target",
                "st_ret":        round((price / entry_price - 1) * 100, 2),
                "st_exit_day":   day_idx,
                "st_exit_price": round(price, 2),
            }

    # Time exit
    if len(future_prices) >= max_days:
        ep = future_prices[max_days - 1]
        return {
            "st_exit_type":  "time",
            "st_ret":        round((ep / entry_price - 1) * 100, 2),
            "st_exit_day":   max_days,
            "st_exit_price": round(ep, 2),
        }

    return {"st_exit_type": "pending", "st_ret": None, "st_exit_day": None, "st_exit_price": None}


# ─────────────────────────────────────────────────────────────────────────────
# Agregaciones y métricas
# ─────────────────────────────────────────────────────────────────────────────

def _aggregate_by(trades: list, group_key: str) -> dict:
    """Agrupa por group_key y calcula métricas por horizonte + stop/target."""
    groups: dict[str, list] = {}
    for t in trades:
        key = t.get(group_key, "UNKNOWN") or "UNKNOWN"
        groups.setdefault(key, []).append(t)

    result = {}
    for key, gtrades in groups.items():
        entry = {"count": len(gtrades)}

        for h in HORIZONS:
            rets = [t[f"ret_{h}d"] for t in gtrades if t.get(f"ret_{h}d") is not None]
            entry[f"h{h}d"] = _metrics_from_rets(rets)

        # Stop/target stats del grupo
        st_trades = [t for t in gtrades if t.get("st_exit_type") not in (None, "no_data", "pending")]
        if st_trades:
            st_rets      = [t["st_ret"] for t in st_trades if t.get("st_ret") is not None]
            hits_target  = sum(1 for t in st_trades if t["st_exit_type"] == "target")
            hits_stop    = sum(1 for t in st_trades if t["st_exit_type"] == "stop")
            hits_time    = sum(1 for t in st_trades if t["st_exit_type"] == "time")
            n            = len(st_trades)
            entry["stop_target"] = {
                "samples":    n,
                "avg_ret":    round(float(np.mean(st_rets)), 2) if st_rets else None,
                "win_rate":   round(len([r for r in st_rets if r > 0]) / len(st_rets), 3) if st_rets else None,
                "target_hits": hits_target,
                "stop_hits":   hits_stop,
                "time_exits":  hits_time,
                "pct_target":  round(hits_target / n * 100, 1),
                "pct_stop":    round(hits_stop   / n * 100, 1),
                "pct_time":    round(hits_time   / n * 100, 1),
            }

        result[key] = entry

    return result


def _metrics_from_rets(rets: list) -> dict | None:
    """Calcula métricas estándar desde lista de retornos. Retorna None si vacío."""
    if not rets:
        return None

    arr     = np.array(rets, dtype=float)
    wins    = arr[arr > 0]
    losses  = arr[arr <= 0]
    wr      = float(len(wins) / len(arr))
    lr      = 1 - wr
    avg_ret = float(np.mean(arr))
    avg_win = float(np.mean(wins)) if len(wins) > 0 else 0.0
    avg_los = float(abs(np.mean(losses))) if len(losses) > 0 else 0.0
    ev      = wr * avg_win - lr * avg_los
    std     = float(np.std(arr))
    sharpe  = avg_ret / std if std > 0 else 0.0

    # Max drawdown sobre la curva de equity acumulada
    cum  = np.cumprod(1 + arr / 100)
    peak = np.maximum.accumulate(cum)
    dd   = (cum - peak) / peak * 100
    mdd  = float(np.min(dd))

    return {
        "samples":         len(rets),
        "win_rate":        round(wr, 3),
        "avg_ret":         round(avg_ret, 2),
        "avg_win":         round(avg_win, 2),
        "avg_loss":        round(avg_los, 2),
        "expected_value":  round(ev, 2),
        "sharpe":          round(sharpe, 2),
        "max_drawdown":    round(mdd, 2),
    }


def _predictor_accuracy(trades: list) -> dict:
    """Mide accuracy del predictor: dirección correcta 21d y MAE."""
    valid = [t for t in trades
             if t.get("pred_21d") is not None and t.get("ret_21d") is not None]

    if not valid:
        return {"samples": 0, "note": "Sin datos de predictor aún"}

    dir_ok  = [np.sign(t["pred_21d"]) == np.sign(t["ret_21d"]) for t in valid]
    dir_acc = round(float(np.mean(dir_ok)), 3)
    mae     = round(float(np.mean([abs(t["pred_21d"] - t["ret_21d"]) for t in valid])), 2)

    # Breakdown por señal predictiva
    by_ps: dict[str, list] = {}
    for t in valid:
        ps = t.get("pred_signal") or "UNKNOWN"
        by_ps.setdefault(ps, []).append(t["ret_21d"])

    pred_breakdown = {}
    for ps, rets in by_ps.items():
        pred_breakdown[ps] = {
            "samples":      len(rets),
            "avg_real_ret": round(float(np.mean(rets)), 2),
            "win_rate":     round(len([r for r in rets if r > 0]) / len(rets), 3),
        }

    return {
        "samples":               len(valid),
        "directional_accuracy":  dir_acc,
        "mae":                   mae,
        "by_pred_signal":        pred_breakdown,
    }


def _stop_target_global(trades: list) -> dict:
    """Estadísticas globales de exits stop/target."""
    valid = [t for t in trades
             if t.get("st_exit_type") not in (None, "no_data", "pending")]

    if not valid:
        return {"samples": 0}

    by_type: dict[str, list] = {}
    for t in valid:
        et = t.get("st_exit_type", "unknown")
        by_type.setdefault(et, []).append(t.get("st_ret") or 0)

    result = {
        "samples":      len(valid),
        "by_exit_type": {},
    }
    for et, rets in by_type.items():
        result["by_exit_type"][et] = {
            "count":   len(rets),
            "pct":     round(len(rets) / len(valid) * 100, 1),
            "avg_ret": round(float(np.mean(rets)), 2),
        }

    exit_days = [t["st_exit_day"] for t in valid if t.get("st_exit_day")]
    if exit_days:
        result["avg_days_to_exit"] = round(float(np.mean(exit_days)), 1)

    return result


def _top_bottom(trades: list, top: bool = True) -> list:
    """Top/bottom 5 por ret_21d."""
    valid = [t for t in trades if t.get("ret_21d") is not None]
    if not valid:
        return []
    sorted_t = sorted(valid, key=lambda x: x["ret_21d"], reverse=top)[:5]
    return [
        {
            "ticker":      t["ticker"],
            "mercado":     t["mercado"],
            "signal_date": t["signal_date"],
            "signal":      t["signal"],
            "ret_21d":     t["ret_21d"],
            "precio_entry": t["precio_entry"],
            "st_exit_type": t.get("st_exit_type"),
        }
        for t in sorted_t
    ]


def _signal_summary_table(trades: list) -> list:
    """
    Tabla resumen ejecutiva: una fila por tipo de señal con métricas clave.
    Ordenada por expected_value_21d desc.
    """
    by_signal = _aggregate_by(trades, "signal")
    rows = []
    for signal, data in by_signal.items():
        h21 = data.get("h21d") or {}
        st  = data.get("stop_target") or {}
        rows.append({
            "signal":         signal,
            "n":              data["count"],
            "win_rate_21d":   h21.get("win_rate"),
            "avg_ret_21d":    h21.get("avg_ret"),
            "expected_value": h21.get("expected_value"),
            "sharpe_21d":     h21.get("sharpe"),
            "max_drawdown":   h21.get("max_drawdown"),
            "pct_target_hit": st.get("pct_target"),
            "pct_stop_hit":   st.get("pct_stop"),
        })
    rows.sort(key=lambda x: x.get("expected_value") or -99, reverse=True)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _log_summary(results: dict):
    logger.info("══════════ BACKTEST RESULTS ══════════")
    logger.info(f"Trades: {results['total_trades']} | Días historia: {results['days_history']}")

    for row in results.get("signal_summary", []):
        wr  = f"{row['win_rate_21d']:.0%}"  if row.get("win_rate_21d")  is not None else "—"
        ret = f"{row['avg_ret_21d']:+.1f}%" if row.get("avg_ret_21d")   is not None else "—"
        ev  = f"{row['expected_value']:+.1f}%" if row.get("expected_value") is not None else "—"
        sh  = f"{row['sharpe_21d']:.2f}"    if row.get("sharpe_21d")    is not None else "—"
        logger.info(f"  {row['signal']:<30} n={row['n']:>3} | WR={wr} AvgRet={ret} EV={ev} Sharpe={sh}")

    pred = results.get("predictor", {})
    if pred.get("directional_accuracy"):
        logger.info(
            f"  Predictor 21d: accuracy={pred['directional_accuracy']:.0%} | MAE={pred['mae']:.1f}% | n={pred['samples']}"
        )

    st = results.get("stop_target", {})
    if st.get("samples", 0) > 0:
        by_et = st.get("by_exit_type", {})
        logger.info(
            f"  Exits — target: {by_et.get('target', {}).get('pct', 0):.0f}% | "
            f"stop: {by_et.get('stop', {}).get('pct', 0):.0f}% | "
            f"time: {by_et.get('time', {}).get('pct', 0):.0f}% | "
            f"avg_days: {st.get('avg_days_to_exit', '—')}"
        )
    logger.info("══════════════════════════════════════")
