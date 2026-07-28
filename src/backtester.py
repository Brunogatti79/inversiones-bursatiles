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


def _walk_forward_validate_patterns(history: dict, sorted_dates: list, price_index: dict,
                                     split_ratio: float = 0.7, min_samples: int = 15,
                                     min_validation_samples: int = 5) -> dict:
    """
    Walk-forward sobre los patrones de confidence_x_signal (auditoría externa
    28/07/2026, punto 5 del roadmap: "separar ventana de descubrimiento de
    ventana de validación" -- distinto del walk-forward que ya tiene
    weight_optimizer.py, que optimiza los PESOS del score V1, no valida si
    un patrón de confianza x señal es real o ruido).

    Por qué hace falta además de lo que ya existe: confidence_x_signal
    (más arriba en este archivo) y _detect_pattern_discoveries() usan y
    comparan TODA la historia disponible de una sola vez -- "Alta + COMPRA
    rinde +0.95% con n=41" es una afirmación sobre la MISMA muestra donde
    se la descubrió. Es exactamente el riesgo que señaló la auditoría
    externa como el más grave del proyecto: no hay forma de saber, con ese
    número solo, si es un edge real o sobreajuste a esos 41 casos
    puntuales. Esta función corta la historia en 2 ventanas CRONOLÓGICAS
    (nunca al azar -- mezclar fechas destruye la naturaleza temporal del
    test y volvería a mezclar pasado con futuro): la primera porción
    (`split_ratio`, 70% por defecto) es "descubrimiento" -- ahí se mide qué
    combinaciones tienen buen EV. La porción final es "validación" -- se
    recalcula la MISMA combinación usando solo esos trades, nunca vistos
    al momento de descubrirla, y se compara el signo del EV.

    Estados posibles por combinación:
      - "confirmado": mismo signo de EV en descubrimiento y en validación
        fuera de muestra -- la evidencia más fuerte que el sistema puede
        dar hoy de que un patrón es real.
      - "no_confirmado": el EV cambió de signo fuera de muestra -- señal de
        alerta, no de descarte automático (la ventana de validación suele
        ser chica).
      - "sin_datos_validacion": la ventana de validación no tiene muestra
        suficiente para esa combinación todavía.

    Con la historia real todavía chica (incidente signals_history.json,
    28/07/2026 -- ver notas de sesión), esta función va a devolver
    mayormente "sin_datos_validacion". Es el resultado correcto y honesto:
    no hay overfitting posible en un resultado que dice "no hay suficiente
    historia para confirmar nada todavía". El valor de tenerla lista ahora
    es que empieza a producir evidencia real sin tocar código de nuevo en
    cuanto la historia crezca.
    """
    n_dates = len(sorted_dates)
    split_idx = int(n_dates * split_ratio)
    dates_discovery  = sorted_dates[:split_idx]
    dates_validation = sorted_dates[split_idx:]

    if len(dates_discovery) < 2 or len(dates_validation) < 1:
        return {
            "status": "sin_datos_suficientes",
            "n_dias_total": n_dates,
            "n_dias_descubrimiento": len(dates_discovery),
            "n_dias_validacion": len(dates_validation),
            "combinaciones": {},
        }

    trades_discovery  = _build_trades(history, dates_discovery, price_index)
    trades_validation = _build_trades(history, dates_validation, price_index)

    cross_discovery  = _aggregate_cross(trades_discovery,  "confidence_label", "signal")
    cross_validation = _aggregate_cross(trades_validation, "confidence_label", "signal")

    combinaciones = {}
    for conf_label, signals in cross_discovery.items():
        for sig, cell_disc in signals.items():
            disc_summary = _cell_summary(cell_disc)
            if disc_summary is None or disc_summary["n"] < min_samples:
                continue  # ni siquiera hay patrón que valga la pena validar

            key = f"{conf_label} + {sig}"
            cell_val = (cross_validation.get(conf_label) or {}).get(sig)
            val_summary = _cell_summary(cell_val) if cell_val else None

            if (val_summary is None or val_summary["n"] < min_validation_samples
                    or val_summary["ev"] is None or disc_summary["ev"] is None):
                combinaciones[key] = {
                    "descubrimiento": disc_summary,
                    "validacion": val_summary,
                    "estado": "sin_datos_validacion",
                }
                continue

            mismo_signo = (disc_summary["ev"] > 0) == (val_summary["ev"] > 0)
            combinaciones[key] = {
                "descubrimiento": disc_summary,
                "validacion": val_summary,
                "estado": "confirmado" if mismo_signo else "no_confirmado",
            }

    return {
        "status": "ok",
        "n_dias_total": n_dates,
        "n_dias_descubrimiento": len(dates_discovery),
        "n_dias_validacion": len(dates_validation),
        "split_ratio": split_ratio,
        "combinaciones": combinaciones,
    }


def _rank_discovered_rules(cross: dict, walk_forward: dict = None,
                            recent_events: list = None, min_samples: int = 15) -> list[dict]:
    """
    Meta-backtester (auditoría externa 28/07/2026, punto 6 del roadmap):
    "construiría una capa que rankee automáticamente: Regla, N, EV, Sharpe,
    Drawdown, Estabilidad, Cambio reciente -- y genere un ranking".

    Por qué hace falta esto además de confidence_x_signal y
    _detect_pattern_discoveries: hoy cada combinación se mira por separado
    (una celda del cruce, o un evento aislado del log de descubrimiento).
    A medida que el sistema empiece a producir más combinaciones con
    evidencia (por mercado, por régimen, etc. -- roadmap del punto 2 en
    adelante), no hay forma de comparar cuál es la MEJOR regla disponible
    hoy sin juntarlas todas en una sola tabla ordenada.

    Deliberadamente NO inventa un score único 0-100 que mezcle todo en un
    número opaco (mismo criterio que _compute_historical_edge_scores):
    devuelve una tabla con todas las columnas visibles, ordenada por
    prioridad explícita y auditable:
      1. Estado de walk-forward (confirmado primero, no_confirmado último
         -- la evidencia fuera de muestra pesa más que cualquier otra
         columna, es la que distingue un edge real de sobreajuste)
      2. Expected Value descendente
      3. Tamaño de muestra descendente (como desempate)

    `walk_forward` es el dict que devuelve _walk_forward_validate_patterns
    (opcional -- si no se pasa, todas las reglas quedan "no_evaluado", no
    se inventa un estado). `recent_events` es la lista que devuelve
    _detect_pattern_discoveries EN ESTA CORRIDA (no relee el log de disco
    de nuevo -- ya se calculó una vez en run_backtest(), reusarlo evita
    una lectura redundante de github_persistence).
    """
    wf_combos = (walk_forward or {}).get("combinaciones", {})
    recent_keys = {ev.get("combinacion") for ev in (recent_events or [])}

    _ESTADO_RANK = {"confirmado": 0, "sin_datos_validacion": 1, "no_evaluado": 2, "no_confirmado": 3}

    rules = []
    for conf_label, signals in (cross or {}).items():
        for sig, cell in (signals or {}).items():
            resumen = _cell_summary(cell)
            if resumen is None or resumen["n"] < min_samples:
                continue
            key = f"{conf_label} + {sig}"

            # Traer sharpe/drawdown/profit_factor del MISMO horizonte que
            # _cell_summary usó para n/ev -- mezclar métricas de horizontes
            # distintos en una misma fila sería engañoso.
            h5, h10 = (cell or {}).get("h5d") or {}, (cell or {}).get("h10d") or {}
            n5, n10 = h5.get("samples") or 0, h10.get("samples") or 0
            best, horizonte = (h5, "5d") if n5 >= n10 else (h10, "10d")

            wf_entry = wf_combos.get(key)
            estado_wf = wf_entry.get("estado") if wf_entry else "no_evaluado"

            rules.append({
                "regla":               key,
                "n":                   resumen["n"],
                "horizonte":           horizonte,
                "ev":                  resumen["ev"],
                "win_rate":            best.get("win_rate"),
                "sharpe":              best.get("sharpe"),
                "max_drawdown":        best.get("max_drawdown"),
                "profit_factor":       best.get("profit_factor"),
                "significativo_95":    bool(best.get("significativo_95", False)),
                "walk_forward_estado": estado_wf,
                "cambio_reciente":     key in recent_keys,
            })

    rules.sort(key=lambda r: (
        _ESTADO_RANK.get(r["walk_forward_estado"], 2),
        -(r["ev"] if r["ev"] is not None else -999.0),
        -r["n"],
    ))
    return rules


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
        # Auditoría externa (28/07/2026), punto 2: cruce de 3 vías -- "Alta +
        # COMPRA" puede estar promediando un mercado malo con uno bueno.
        # La mayoría de las celdas van a salir con muestra insuficiente con
        # ~900 trades repartidos en 3 mercados -- eso es esperado, no un bug.
        "confidence_x_signal_x_mercado": _aggregate_cross3(
            trades, "confidence_label", "signal", "mercado"
        ),
        # Auditoría externa (28/07/2026), punto 3: ¿el predictor aporta valor
        # marginal real, o el edge viene de otro lado del modelo? Compara EV
        # real cuando el predictor confirma la señal vs. cuando la contradice.
        "predictor_contribution": _predictor_contribution_analysis(trades),
    }

    results["pattern_discoveries_nuevas"] = _detect_pattern_discoveries(
        results["confidence_x_signal"]
    )

    # Auditoría externa (28/07/2026), punto 4: Historical Edge Score --
    # puntaje informativo por combinación, basado en el cruce confidence x
    # signal ya calculado arriba. Ver docstring de la función: es SOLO
    # diagnóstico, no toca ranking_accionable ni ninguna señal en vivo.
    results["historical_edge_score"] = _compute_historical_edge_scores(
        results["confidence_x_signal"]
    )

    # Auditoría externa (28/07/2026), punto 5 del roadmap: walk-forward real
    # sobre los patrones (distinto del walk-forward que ya tiene
    # weight_optimizer.py, que valida PESOS no PATRONES). Envuelto aparte
    # -- no debe tumbar el resto de results si algo falla con historia real
    # todavía chica o irregular.
    try:
        results["walk_forward_validation"] = _walk_forward_validate_patterns(
            history, sorted_dates, price_index
        )
    except Exception as e_wf:
        logger.warning(f"Backtester: walk-forward validation no crítico — continuando: {e_wf}")
        results["walk_forward_validation"] = {"status": "error", "combinaciones": {}}

    # Auditoría externa (28/07/2026), punto 6 del roadmap: meta-backtester --
    # ranking único de todas las reglas descubiertas (N, EV, Sharpe,
    # Drawdown, estado de walk-forward, si tuvo evento reciente), en vez de
    # tener que ir a mirar cada combinación por separado.
    try:
        results["ranked_rules"] = _rank_discovered_rules(
            results["confidence_x_signal"],
            walk_forward=results.get("walk_forward_validation"),
            recent_events=results.get("pattern_discoveries_nuevas"),
        )
    except Exception as e_rr:
        logger.warning(f"Backtester: ranked_rules no crítico — continuando: {e_rr}")
        results["ranked_rules"] = []

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


def _aggregate_cross3(trades: list, key1: str, key2: str, key3: str,
                       min_cell_samples: int = 5) -> dict:
    """
    Cruce de 3 claves (confidence_label × signal × mercado) -- pedido
    externo (auditoría 28/07/2026): "Alta + COMPRA" ya se sabe que rinde
    bien en general, pero eso puede estar promediando un MERVAL malo con
    un SP500 excelente. Sin este cruce no se puede saber si conviene
    filtrar también por mercado antes de operar.

    Con solo ~900 trades totales repartidos en 3 mercados x 5 niveles de
    confianza x ~5 señales, la mayoría de las celdas de este cruce van a
    tener muestra insuficiente por diseño -- eso es correcto y esperable,
    no un bug: es la razón por la que este cruce se agrega como campo
    adicional (para ir acumulando evidencia con "muestra_insuficiente"
    explícito) y NO reemplaza a confidence_x_signal, que sigue siendo la
    vista con muestra más confiable hoy.
    """
    groups: dict[tuple, list] = {}
    for t in trades:
        k1 = t.get(key1, "UNKNOWN") or "UNKNOWN"
        k2 = t.get(key2, "UNKNOWN") or "UNKNOWN"
        k3 = t.get(key3, "UNKNOWN") or "UNKNOWN"
        groups.setdefault((k1, k2, k3), []).append(t)

    result = {}
    for (k1, k2, k3), gtrades in groups.items():
        entry = {"count": len(gtrades), "muestra_insuficiente": len(gtrades) < min_cell_samples}

        for h in HORIZONS:
            rets = [t[f"ret_{h}d"] for t in gtrades if t.get(f"ret_{h}d") is not None]
            entry[f"h{h}d"] = _metrics_from_rets(rets)

        result.setdefault(k1, {}).setdefault(k2, {})[k3] = entry

    return result


def _metrics_from_rets(rets: list) -> dict | None:
    """Calcula métricas estándar desde lista de retornos. Retorna None si vacío.

    Incluye significancia estadística (auditoría externa 28/07/2026): un EV
    positivo sobre pocas muestras puede ser ruido. p_value/IC95% responden
    "¿es distinguible de cero?" -- profit_factor responde "¿cuánto gana por
    cada unidad que pierde?", pregunta distinta de win_rate/EV. Ninguno de
    estos campos reemplaza a los anteriores; se agregan al lado.
    """
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

    # Profit factor: ganancia bruta / pérdida bruta. No es lo mismo que EV --
    # un sistema puede tener EV positivo con profit_factor bajo si gana
    # seguido y poco, y pierde poco seguido pero fuerte una vez.
    gross_profit = float(np.sum(wins)) if len(wins) > 0 else 0.0
    gross_loss   = float(abs(np.sum(losses))) if len(losses) > 0 else 0.0
    if gross_loss > 0:
        profit_factor = round(gross_profit / gross_loss, 2)
    elif gross_profit > 0:
        profit_factor = None  # sin pérdidas en la muestra -- infinito no es informativo
    else:
        profit_factor = 0.0

    # Significancia estadística: t-test de una muestra contra media=0.
    # n<2 no permite calcular varianza -- se devuelve sin p_value/IC en vez
    # de un valor engañoso.
    p_value = None
    ic95    = None
    significativo_95 = False
    n = len(arr)
    if n >= 2 and std > 0:
        try:
            from scipy import stats as _stats
            t_stat, p_value = _stats.ttest_1samp(arr, popmean=0.0)
            p_value = round(float(p_value), 4)
            sem     = std / np.sqrt(n)
            t_crit  = _stats.t.ppf(0.975, df=n - 1)
            margin  = float(t_crit * sem)
            ic95    = [round(avg_ret - margin, 2), round(avg_ret + margin, 2)]
            significativo_95 = bool(p_value < 0.05)
        except Exception as e:
            logger.warning(f"Backtester: no se pudo calcular significancia (n={n}): {e}")

    return {
        "samples":          len(rets),
        "win_rate":         round(wr, 3),
        "avg_ret":          round(avg_ret, 2),
        "avg_win":          round(avg_win, 2),
        "avg_loss":         round(avg_los, 2),
        "expected_value":   round(ev, 2),
        "sharpe":           round(sharpe, 2),
        "max_drawdown":     round(mdd, 2),
        "profit_factor":    profit_factor,
        "p_value":          p_value,
        "ic95":             ic95,
        "significativo_95": significativo_95,
    }


def _predictor_contribution_analysis(trades: list) -> dict:
    """
    Ablation práctica del predictor (auditoría externa 28/07/2026): la duda
    de fondo no es "¿el predictor acierta la dirección?" (eso ya lo mide
    _predictor_accuracy) sino "¿importa para el resultado real que el
    predictor esté de acuerdo con la señal?". Se separan los trades en 3
    grupos según si pred_21d confirma, contradice, o no hay dato del
    predictor para esa señal, y se compara el EV/win_rate real de cada
    grupo. Si "confirma" y "contradice" no se distinguen entre sí, es
    evidencia de que el predictor no aporta valor marginal sobre lo que ya
    aporta el resto del modelo (V1/V2/confianza) -- la sospecha concreta
    que motivó este análisis.

    No es un recomputo del confidence_score sin el término del predictor
    (eso requeriría reconstruir el score completo desde componentes que hoy
    no se persisten trade por trade) -- es la pregunta más directa que sí
    se puede verificar con los datos que existen: ¿el resultado real
    difiere según lo que dijo el predictor?
    """
    groups: dict[str, list] = {"confirma": [], "contradice": [], "sin_dato": []}
    for t in trades:
        signal  = t.get("signal", "") or ""
        pred    = t.get("pred_21d")
        is_buy  = "COMPRA" in signal
        is_sell = "VENTA"  in signal
        if pred is None or (not is_buy and not is_sell):
            groups["sin_dato"].append(t)
            continue
        confirma = (is_buy and pred > 0) or (is_sell and pred < 0)
        groups["confirma" if confirma else "contradice"].append(t)

    result = {}
    for label, gtrades in groups.items():
        entry = {"count": len(gtrades)}
        for h in HORIZONS:
            rets = [t[f"ret_{h}d"] for t in gtrades if t.get(f"ret_{h}d") is not None]
            entry[f"h{h}d"] = _metrics_from_rets(rets)
        result[label] = entry

    ev_confirma   = (result.get("confirma", {}).get("h21d") or {}).get("expected_value")
    ev_contradice = (result.get("contradice", {}).get("h21d") or {}).get("expected_value")
    delta = (round(ev_confirma - ev_contradice, 2)
             if ev_confirma is not None and ev_contradice is not None else None)

    result["_resumen"] = {
        "ev_21d_confirma":   ev_confirma,
        "ev_21d_contradice": ev_contradice,
        "delta_ev_21d":      delta,
        "nota": ("Si delta_ev_21d es chico o negativo, el predictor no está "
                 "aportando valor marginal detectable sobre el resto del "
                 "modelo con la muestra actual -- no concluir sin revisar "
                 "antes el tamaño de muestra (count) de cada grupo."),
    }
    return result


def _compute_historical_edge_scores(cross: dict, min_samples: int = 15) -> dict:
    """
    Historical Edge Score (propuesta externa, auditoría 28/07/2026):
    puntaje INFORMATIVO 0-100 por combinación confidence_label × signal,
    que resume qué tan bien respaldada está esa combinación por el
    historial real (muestra, EV, significancia).

    IMPORTANTE -- alcance deliberadamente acotado: esto es SOLO un campo
    de diagnóstico en backtest_results.json / dashboard. NO se conecta al
    ranking_accionable ni a ninguna decisión en vivo todavía. Conectarlo
    (usarlo para pesar o filtrar señales reales) requiere validación
    adicional fuera de muestra (walk-forward -- pendiente, roadmap punto 3
    de la auditoría externa) y una decisión explícita de Bruno antes de
    tocar señales reales. Mezclar "descubrir un patrón" con "operar ese
    patrón" sin separar la ventana de datos es exactamente el riesgo de
    sobreajuste que la auditoría externa señaló como el problema
    estratégico más importante del sistema hoy.

    Fórmula (heurística simple, sujeta a revisión):
      - Sin muestra suficiente (n < min_samples en el mejor horizonte) → score None
      - Base 50 (neutral)
      - ± hasta 30pt según expected_value del mejor horizonte (6pt por cada 1% de EV)
      - +15pt si significativo_95 es True
      - +5pt si n >= 50
      - Resultado clampeado a [0, 100]
    """
    result = {}
    for conf_label, signals in (cross or {}).items():
        for sig, cell in (signals or {}).items():
            h5  = (cell or {}).get("h5d")  or {}
            h10 = (cell or {}).get("h10d") or {}
            best = h5 if (h5.get("samples") or 0) >= (h10.get("samples") or 0) else h10
            n  = best.get("samples") or 0
            ev = best.get("expected_value")
            key = f"{conf_label} + {sig}"

            if n < min_samples or ev is None:
                result[key] = {"score": None, "n": n, "nota": "muestra insuficiente"}
                continue

            score = 50 + max(-30, min(30, ev * 6))
            if best.get("significativo_95"):
                score += 15
            if n >= 50:
                score += 5
            score = round(max(0, min(100, score)), 1)

            result[key] = {
                "score":            score,
                "n":                n,
                "ev":               ev,
                "significativo_95": bool(best.get("significativo_95", False)),
            }
    return result


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
