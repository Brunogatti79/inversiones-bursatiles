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

def _confidence_calibration_table(trades: list) -> dict:
    """
    Calibración de confianza (roadmap externo #9, jul-2026): parte los
    trades en 5 quintiles por confidence_score numérico y calcula WR/EV
    por quintil, con un chequeo explícito de monotonía.

    Diferencia con confidence_quantiles (top/bottom 20%): eso responde
    "¿los extremos se distinguen entre sí?" -- esto responde una pregunta
    más específica y más dura: "¿la relación es CONSISTENTE en todo el
    rango, o solo en los extremos?". Un score puede distinguir bien el
    20% mejor del 20% peor y aun así no ser monótono en el medio (ej.
    quintil 3 con peor WR que el quintil 2) -- eso indicaría que el score
    numérico no está bien calibrado en su rango medio, aunque separe bien
    los extremos. Esta es la pregunta que hace falta para decir "el
    confidence score está aprobado" en un sentido más riguroso.
    """
    valid = [t for t in trades
             if t.get("confidence_score") is not None and _best_ret(t)[1] is not None]
    n = len(valid)
    if n < 15:
        return {"samples": n, "note": "Necesita ≥15 trades con confidence_score para 5 quintiles confiables"}

    sorted_t = sorted(valid, key=lambda t: t["confidence_score"])
    quintiles = np.array_split(sorted_t, 5)  # de menor a mayor confianza

    buckets = []
    for i, q in enumerate(quintiles):
        q = list(q)
        rets_horizontes = [_best_ret(t) for t in q]
        rets = [r for _, r in rets_horizontes if r is not None]
        horizontes_usados = sorted({h for h, r in rets_horizontes if r is not None}, reverse=True)
        m = _metrics_from_rets(rets) or {}
        vals = [t["confidence_score"] for t in q]
        buckets.append({
            "quintil":               i + 1,
            "rango_confianza":       [round(min(vals), 1), round(max(vals), 1)] if vals else None,
            "count":                 len(q),
            "win_rate":              m.get("win_rate"),
            "expected_value":        m.get("expected_value"),
            "horizontes_mezclados":  horizontes_usados if len(horizontes_usados) > 1 else None,
        })

    wrs = [b["win_rate"] for b in buckets if b.get("win_rate") is not None]
    monotona = all(wrs[i] <= wrs[i + 1] for i in range(len(wrs) - 1)) if len(wrs) >= 2 else None

    return {
        "samples":   n,
        "quintiles": buckets,
        "monotona":  monotona,
        "nota": ("true = a mayor confianza, mayor win rate en TODO el rango (deseable) | "
                 "false = hay al menos un quintil fuera de orden, revisar calibración | "
                 "null = muy pocos quintiles con datos todavía para evaluar"),
    }


def _isotonic_calibration_curve(trades: list, min_samples: int = 30) -> dict:
    """
    Calibración probabilística (roadmap externo "Institucional PRO",
    jul-2026): en vez de asumir que confidence_score es monótono --ya
    confirmado que NO lo es, ver _confidence_calibration_table(), quintil
    4 rompe la tendencia-- ajusta una curva NO-DECRECIENTE de P(ganar) en
    función de confidence_score vía regresión isotónica (PAVA:
    Pool-Adjacent-Violators Algorithm). Esto "absorbe" la no-monotonía en
    vez de fingir que no existe: donde el dato crudo dice que confianza 60
    gana menos que confianza 55, la curva isotónica los pool-ea a un valor
    intermedio común, en vez de reportar una probabilidad que baja al
    subir la confianza (lo cual no tendría sentido para un usuario).

    Por qué ahora y no "esperar más historia": el problema encontrado es
    de FORMA de la relación, no de tamaño de muestra -- con 536 trades ya
    hay señal suficiente para un primer ajuste razonable, y la calibración
    isotónica es precisamente la herramienta estándar para este caso (no
    asume una forma paramétrica, solo que la relación debería ser
    no-decreciente, que es la única premisa que confidence_score necesita
    para ser útil).

    Devuelve una curva serializable en JSON (no el modelo sklearn en sí)
    -- ver calibrated_win_probability() para usarla después.
    """
    try:
        from sklearn.isotonic import IsotonicRegression
    except ImportError:
        return {"status": "sklearn_no_disponible"}

    valid = [t for t in trades if t.get("confidence_score") is not None and _best_ret(t)[1] is not None]
    n = len(valid)
    if n < min_samples:
        return {
            "status": "insuficiente_historia",
            "samples": n,
            "nota": f"Necesita ≥{min_samples} trades con confidence_score + retorno para un ajuste isotónico razonable",
        }

    X = np.array([float(t["confidence_score"]) for t in valid])
    y = np.array([1.0 if _best_ret(t)[1] > 0 else 0.0 for t in valid])  # binario: ganó/perdió

    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    iso.fit(X, y)

    # Curva en 10 puntos representativos del rango observado (serializable)
    puntos_x = np.linspace(float(X.min()), float(X.max()), 10)
    puntos_y = iso.predict(puntos_x)
    curva = [
        {"confidence_score": round(float(px), 1), "p_ganar_calibrada": round(float(py), 3)}
        for px, py in zip(puntos_x, puntos_y)
    ]

    # Correlación del score crudo contra el resultado binario -- una forma
    # simple de cuantificar "cuánta información real tiene el score",
    # independiente de si es monótono o no.
    correlacion = float(np.corrcoef(X, y)[0, 1]) if len(set(X.tolist())) > 1 else None

    return {
        "status": "ok",
        "samples": n,
        "rango_confidence_score": [round(float(X.min()), 1), round(float(X.max()), 1)],
        "curva": curva,
        "correlacion_score_crudo_vs_resultado": round(correlacion, 3) if correlacion is not None else None,
        "metodo": "isotonic_regression_pava",
    }


def calibrated_win_probability(confidence_score: float, curva: list) -> float | None:
    """
    Interpola sobre una curva ya calculada por _isotonic_calibration_curve()
    (el campo "curva" de su resultado) para estimar P(ganar) para un
    confidence_score arbitrario -- pensado para usarse después, desde el
    dashboard o desde cualquier otro lugar que solo tenga el JSON guardado
    en backtest_results.json, sin necesitar reentrenar nada.
    """
    if not curva:
        return None
    xs = [p["confidence_score"] for p in curva]
    ys = [p["p_ganar_calibrada"] for p in curva]
    if confidence_score <= xs[0]:
        return ys[0]
    if confidence_score >= xs[-1]:
        return ys[-1]
    return float(np.interp(confidence_score, xs, ys))


def _cell_summary(cell: dict) -> dict | None:
    """Resume una celda de un cruce (ej. confidence_x_signal) al horizonte
    con más muestra real disponible. Devuelve None si la celda no existe."""
    if not cell:
        return None
    h5  = cell.get("h5d") or {}
    h10 = cell.get("h10d") or {}
    n5, n10 = h5.get("samples") or 0, h10.get("samples") or 0
    best = h5 if n5 >= n10 else h10
    return {"n": best.get("samples") or 0, "ev": best.get("expected_value")}


def _detect_pattern_discoveries(cross: dict, path: str = "data/pattern_discovery_log.json",
                                 min_samples: int = 15) -> list[dict]:
    """
    Instrucción permanente (pedido de Bruno, 27/07/2026): "a partir de los
    resultados, que el modelo vaya aprendiendo -- veamos qué posibilidades
    se pueden ir poniendo a la luz en el dashboard para ir mejorando".
    Este es el mecanismo real detrás de eso, no solo una intención en un
    documento: compara el cruce confidence_x_signal de ESTA corrida contra
    el snapshot de la corrida anterior (persistido acá mismo) y detecta 2
    eventos dignos de revisión humana, sin esperar a que alguien vaya a
    buscarlos a mano (como se hizo hoy con Alta+Compra, a pulmón):

      - "nueva_evidencia": una combinación que antes NO tenía muestra
        suficiente (<min_samples en su mejor horizonte) ahora sí la tiene
        -- candidata a exponerse en el dashboard (ej. un badge, como
        _estado_regla_compra en generator.py) si además su resultado es
        bueno.
      - "cambio_de_signo": una combinación que ya tenía muestra suficiente
        cambió de signo en su expected_value -- un patrón que rendía bien
        podría haber dejado de hacerlo, o viceversa. Alerta para revisar,
        no para actuar solo.

    Devuelve SOLO los eventos NUEVOS de esta corrida (no el historial
    completo) -- generator.py los usa para el banner "🔎 Patrones nuevos"
    en Panorama. El log completo (con snapshot + histórico acotado a 200
    eventos) queda persistido en `path` vía github_persistence, igual que
    el resto de los archivos que necesitan sobrevivir a un redeploy.
    """
    from src.github_persistence import load_json, save_json

    log = load_json(path, default={"eventos": [], "_ultimo_snapshot": {}})
    prev_snapshot = log.get("_ultimo_snapshot", {})

    nuevos_eventos = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    for conf_label, signals in (cross or {}).items():
        for sig, cell in signals.items():
            key = f"{conf_label} + {sig}"
            cur = _cell_summary(cell)
            if cur is None or cur["n"] < min_samples:
                continue  # todavía sin evidencia suficiente esta corrida -- nada que reportar

            prev = prev_snapshot.get(key)
            if prev is None or prev.get("n", 0) < min_samples:
                nuevos_eventos.append({
                    "fecha": now, "tipo": "nueva_evidencia", "combinacion": key,
                    "n": cur["n"], "ev": cur["ev"],
                })
            elif prev.get("ev") is not None and cur["ev"] is not None:
                if (prev["ev"] > 0) != (cur["ev"] > 0):
                    nuevos_eventos.append({
                        "fecha": now, "tipo": "cambio_de_signo", "combinacion": key,
                        "ev_antes": prev["ev"], "ev_ahora": cur["ev"], "n": cur["n"],
                    })

    nuevo_snapshot = {}
    for conf_label, signals in (cross or {}).items():
        for sig, cell in signals.items():
            cur = _cell_summary(cell)
            if cur is not None:
                nuevo_snapshot[f"{conf_label} + {sig}"] = cur

    log["_ultimo_snapshot"] = nuevo_snapshot
    if nuevos_eventos:
        log.setdefault("eventos", []).extend(nuevos_eventos)
        log["eventos"] = log["eventos"][-200:]  # cap -- no crecer sin límite

    save_json(path, log, message=f"auto: pattern_discovery {now}")
    return nuevos_eventos


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
        # ── Prioridad 1 (roadmap externo, 25/06/2026) — cierra el loop de
        # aprendizaje: ¿el consenso V1/V2 predice algo? ¿el confidence score
        # ordena bien? ¿el ranking_accionable separa a los mejores del resto?
        # Sin esto se estaba optimizando estructura del sistema sin medir si
        # las señales que produce esa estructura son mejores que ruido.
        "by_consenso":            _aggregate_by(trades, "consenso"),
        "consenso_vs_no":         _aggregate_by(trades, "consenso_binario"),
        "by_confidence_label":    _aggregate_by(trades, "confidence_label"),
        "confidence_quantiles":   _confidence_quantile_breakdown(trades),
        "confidence_calibration": _confidence_calibration_table(trades),
        "confidence_calibration_curve": _isotonic_calibration_curve(trades),
        "attribution_by_factor": _aggregate_by(trades, "factor_dominante"),
        "ranking_top_vs_rest":    _ranking_quantile_breakdown(trades),
        # Pedido de Bruno (27/07/2026): cruce real confidence_label × signal
        # -- antes solo existían por separado (by_confidence_label, by_signal),
        # que no alcanzan para saber si "Alta + COMPRA" rinde distinto que
        # "Alta" en general o "COMPRA" en general. Sin esto, cualquier regla
        # de decisión que combine ambos campos se basaba en intuición.
        "confidence_x_signal":    _aggregate_cross(trades, "confidence_label", "signal"),
    }

    results["pattern_discoveries_nuevas"] = _detect_pattern_discoveries(
        results["confidence_x_signal"]
    )

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
            # ── Prioridad 1 (roadmap externo, 25/06/2026) ────────────────
            # Campos que no existían como dimensión de backtest hasta hoy:
            # consenso V1/V2 y confidence_score por señal. Entradas viejas
            # de signals_history.json (previas a este fix) no los tienen --
            # se manejan como ausentes, no como "sin consenso"/confianza 0,
            # para no contaminar el breakdown con falsos negativos.
            consenso          = entry.get("consenso", "") or ""
            confidence_score  = entry.get("confidence_score")
            confidence_label  = entry.get("confidence_label", "") or ""
            ranking           = entry.get("ranking", 0)
            # ── Attribution engine (roadmap externo "Institucional PRO", jul-2026) ──
            # factor_dominante ya se calculaba en analyzer.py y se persistía en
            # tracker.py desde el 23/07 (roadmap externo #6), pero nunca había
            # llegado al backtester -- sin esto no se puede responder "¿el modelo
            # gana/pierde según qué factor domina la señal?". Mismo criterio que
            # consenso/confidence_score: entradas viejas sin el campo se manejan
            # como ausentes ("UNKNOWN"), no como una categoría falsa.
            factor_dominante  = entry.get("factor_dominante", "") or ""

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
                "ranking":          ranking,
                "consenso":         consenso or "UNKNOWN",
                "consenso_binario": ("Consenso" if consenso == "Consenso"
                                     else "Sin consenso" if consenso
                                     else None),
                "confidence_score": confidence_score,
                "confidence_label": confidence_label or "UNKNOWN",
                "factor_dominante": factor_dominante or "UNKNOWN",
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


def _aggregate_cross(trades: list, key1: str, key2: str, min_cell_samples: int = 5) -> dict:
    """
    Cruce de 2 claves (ej. confidence_label × signal) -- pedido real de
    Bruno (27/07/2026): los desgloses marginales (by_confidence_label,
    by_signal) por separado no alcanzan para saber si "Alta + COMPRA" rinde
    distinto que "Alta" en general o que "COMPRA" en general. Sin este
    cruce, cualquier regla de decisión que combine ambos campos (como la
    que Bruno quiere fijar) se basa en una intuición, no en el dato real.

    Celdas con menos de min_cell_samples trades quedan marcadas
    explícitamente con "muestra_insuficiente": True en vez de mostrar un
    win_rate que sería puro ruido -- evita que una celda de 2 trades con
    100% de acierto se lea como una señal fuerte.
    """
    groups: dict[tuple, list] = {}
    for t in trades:
        k1 = t.get(key1, "UNKNOWN") or "UNKNOWN"
        k2 = t.get(key2, "UNKNOWN") or "UNKNOWN"
        groups.setdefault((k1, k2), []).append(t)

    result = {}
    for (k1, k2), gtrades in groups.items():
        entry = {"count": len(gtrades), "muestra_insuficiente": len(gtrades) < min_cell_samples}

        for h in HORIZONS:
            rets = [t[f"ret_{h}d"] for t in gtrades if t.get(f"ret_{h}d") is not None]
            entry[f"h{h}d"] = _metrics_from_rets(rets)

        st_trades = [t for t in gtrades if t.get("st_exit_type") not in (None, "no_data", "pending")]
        if st_trades:
            st_rets     = [t["st_ret"] for t in st_trades if t.get("st_ret") is not None]
            hits_target = sum(1 for t in st_trades if t["st_exit_type"] == "target")
            hits_stop   = sum(1 for t in st_trades if t["st_exit_type"] == "stop")
            n           = len(st_trades)
            entry["stop_target"] = {
                "samples":   n,
                "avg_ret":   round(float(np.mean(st_rets)), 2) if st_rets else None,
                "win_rate":  round(len([r for r in st_rets if r > 0]) / len(st_rets), 3) if st_rets else None,
                "pct_target": round(hits_target / n * 100, 1),
                "pct_stop":   round(hits_stop   / n * 100, 1),
            }

        result.setdefault(k1, {})[k2] = entry

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


def _best_ret(trade: dict) -> tuple:
    """
    Helper compartido (fix 23/07/2026, roadmap externo #9): elige el
    horizonte más largo disponible por trade (21d > 10d > 5d) -- mismo
    criterio que ya se usa en el panel del dashboard y en
    log_model_run()/model_version.py. Antes, _quantile_split() dependía
    exclusivamente de "ret_21d", así que confidence_quantiles y
    ranking_top_vs_rest devolvían 0 muestras durante todo el período en
    que la historia real todavía no llega a 21 días -- que es
    precisamente cuando más útil sería tener un primer chequeo de
    calibración, aunque sea preliminar a 5 días.
    """
    for h in (21, 10, 5):
        r = trade.get(f"ret_{h}d")
        if r is not None:
            return h, r
    return None, None


def _quantile_split(valid: list, score_key: str, top_pct: float = 0.20) -> dict | None:
    """
    Helper compartido: ordena `valid` por `score_key` y separa
    top_pct / bottom_pct / medio. Reutilizado por confidence_quantiles y
    ranking_top_vs_rest -- misma lógica, distinta columna de score.

    FIX 23/07/2026: usa el retorno del horizonte más largo disponible por
    trade (ver _best_ret()) en vez de depender solo de ret_21d -- con poca
    historia esto puede mezclar horizontes distintos dentro del mismo
    bucket (algunos trades a 5d, otros a 21d si ya los tienen). Se
    documenta explícitamente en el output (`horizontes_mezclados`) para
    que no se lea como una comparación 100% homogénea todavía -- se
    homogeniza sola a 21d en cuanto haya suficiente historia real.
    """
    n = len(valid)
    if n < 10:
        return {"samples": n, "note": f"Necesita ≥10 trades con {score_key} para un split confiable"}

    sorted_t = sorted(valid, key=lambda t: t[score_key])
    cut = max(1, int(n * top_pct))

    bottom = sorted_t[:cut]
    top    = sorted_t[-cut:]
    middle = sorted_t[cut:-cut] if n > 2 * cut else []

    def _bucket(group):
        rets_horizontes = [_best_ret(t) for t in group]
        rets = [r for _, r in rets_horizontes if r is not None]
        horizontes_usados = sorted({h for h, r in rets_horizontes if r is not None}, reverse=True)
        m = _metrics_from_rets(rets) or {"samples": 0}
        m["count"] = len(group)
        m["horizontes_mezclados"] = horizontes_usados if len(horizontes_usados) > 1 else None
        if group:
            vals = [t[score_key] for t in group]
            m[f"{score_key}_range"] = [round(min(vals), 1), round(max(vals), 1)]
        return m

    return {
        "samples":      n,
        "top_20pct":    _bucket(top),
        "bottom_20pct": _bucket(bottom),
        "middle_60pct": _bucket(middle),
    }


def _confidence_quantile_breakdown(trades: list) -> dict:
    """
    Test #3 de la devolución externa (25/06/2026): separa por PERCENTIL de
    confidence_score numérico (no por el label de negocio, que ya cubre
    by_confidence_label) -- top 20% vs bottom 20% vs el resto. Sin esto,
    "el confidence score funciona" es una afirmación sin verificar: el
    score puede estar perfectamente calculado y aun así no correlacionar
    con nada real.
    """
    valid = [t for t in trades
             if t.get("confidence_score") is not None and _best_ret(t)[1] is not None]
    return _quantile_split(valid, "confidence_score") or {"samples": 0}


def _ranking_quantile_breakdown(trades: list) -> dict:
    """
    Test #2 de la devolución externa: ¿el ranking_accionable (lo que
    efectivamente ordena la tabla que ve Bruno) separa a los mejores del
    resto, o da lo mismo mirar cualquier fila? Mismo split top/bottom 20%
    que confidence_quantiles, sobre la columna 'ranking'.
    """
    valid = [t for t in trades
             if t.get("ranking") not in (None, 0) and _best_ret(t)[1] is not None]
    return _quantile_split(valid, "ranking") or {"samples": 0}


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

    # ── Prioridad 1 (roadmap externo, 25/06/2026) ────────────────────────
    cvn = results.get("consenso_vs_no", {})
    if cvn:
        c = (cvn.get("Consenso") or {}).get("h21d") or {}
        s = (cvn.get("Sin consenso") or {}).get("h21d") or {}
        if c or s:
            logger.info(
                f"  Consenso V1/V2 — con consenso: n={(cvn.get('Consenso') or {}).get('count', 0)} "
                f"WR={_fmt_pct(c.get('win_rate'))} EV={_fmt_pct(c.get('expected_value'))} | "
                f"sin consenso: n={(cvn.get('Sin consenso') or {}).get('count', 0)} "
                f"WR={_fmt_pct(s.get('win_rate'))} EV={_fmt_pct(s.get('expected_value'))}"
            )

    cq = results.get("confidence_quantiles", {})
    if cq.get("samples", 0) >= 10:
        t20, b20 = cq.get("top_20pct", {}), cq.get("bottom_20pct", {})
        logger.info(
            f"  Confidence quantiles — top20%: n={t20.get('count', 0)} EV={_fmt_pct(t20.get('expected_value'))} | "
            f"bottom20%: n={b20.get('count', 0)} EV={_fmt_pct(b20.get('expected_value'))}"
        )

    rk = results.get("ranking_top_vs_rest", {})
    if rk.get("samples", 0) >= 10:
        t20, b20 = rk.get("top_20pct", {}), rk.get("bottom_20pct", {})
        logger.info(
            f"  Ranking top vs resto — top20%: n={t20.get('count', 0)} EV={_fmt_pct(t20.get('expected_value'))} | "
            f"bottom20%: n={b20.get('count', 0)} EV={_fmt_pct(b20.get('expected_value'))}"
        )

    logger.info("══════════════════════════════════════")


def _fmt_pct(v) -> str:
    return f"{v:+.1f}%" if isinstance(v, (int, float)) else "—"
