"""
src/kill_switch_log.py — Historial del Kill Switch + Validación Retroactiva
(Prioridad 2, roadmap externo — devolución 25/06/2026)

PROBLEMA QUE RESUELVE:
  confidence_score.compute_global_confidence() decide en cada run si activar
  el kill switch (umbral score<35, o >=MIN_CRITICAL_FOR_KILL críticas
  estructurales), pero monitor.persist_global_confidence() solo guarda el
  ÚLTIMO estado en data/system_confidence.json -- se pisa en cada corrida.
  No hay manera de responder "¿el kill switch se activó alguna vez? ¿le
  acertó?" sin un historial.

  Los umbrales (35, 5) siguen siendo arbitrarios -- la devolución del
  25/06/2026 lo señala explícitamente como Prioridad 2. No se pueden
  calibrar con datos reales hasta que exista ese historial. Este módulo es
  el historial, más una evaluación retroactiva apoyada en precios (mismo
  principio que historical_replay.py / predictor_validation.py: usar los
  CSVs de precio ya disponibles en vez de esperar semanas a que el kill
  switch se active "en vivo" lo suficiente como para sacar conclusiones).

DOS RESPONSABILIDADES SEPARADAS A PROPÓSITO:
  1. log_kill_switch_event()        -- corre TODAS las corridas (4x/día).
     Append-only, nunca muta entradas pasadas. Pura bitácora.
     -> data/kill_switch_history.json

  2. evaluate_kill_switch_history() -- corre ~1x/semana (mismo gate por
     contenido que predictor_validation.py / historical_replay.py: si
     'generated' tiene menos de STALENESS_DAYS, no recalcula). Lee el
     historial + los CSVs de precio y mide: cuando el kill switch estuvo
     activo un día, ¿el índice de cada mercado cayó en los FORWARD_HORIZON
     días siguientes (el freno de capital "ayudó") o subió (costo de
     oportunidad)? Se contrasta contra el mismo cálculo en días SIN kill
     switch activo, como baseline -- sin ese contraste, "ayudó X% de las
     veces" no dice nada (un mercado en tendencia alcista casi siempre
     "ayuda menos" sin que eso implique que el kill switch esté mal
     calibrado).
     -> data/kill_switch_validation.json

LIMITACIÓN CONOCIDA (documentarla, no esconderla):
  El kill switch es un circuit breaker GLOBAL (afecta los 3 mercados a la
  vez, frena kelly_half en TODAS las señales), pero el efecto real de
  frenar capital es por señal/posición individual. Este módulo usa el
  forward return del ÍNDICE de cada mercado como proxy -- es razonable (si
  el índice cae, la mayoría de las señales de COMPRA frenadas
  probablemente iban a perder valor también) pero no es lo mismo que medir
  el P&L real evitado, que requeriría simular qué señales de COMPRA
  específicas estaban activas ese día y cuánto valían. Eso queda para
  cuando haya más corridas reales con kill switch activo acumuladas (a
  fecha de este módulo: 1 sola corrida real, ver changelog 4.3 en
  model_version.py -- ese caso fue además un falso positivo ya corregido,
  por lo que ni siquiera cuenta como una muestra útil todavía).

USO desde pipeline.py:
    from src.kill_switch_log import log_kill_switch_event, evaluate_kill_switch_history

    log_kill_switch_event(global_conf, quality_resumen, validacion_nivel,
                           sla_status, n_signals=len(all_signals))
    ...
    evaluate_kill_switch_history(price_data={"merval": merval_df, ...},
                                  index_cols=index_cols)
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

HISTORY_PATH    = "data/kill_switch_history.json"
VALIDATION_PATH = "data/kill_switch_validation.json"

MAX_DAYS        = 120   # ventana de retención del log -- más larga que los
                         # 60d de signals_history.json a propósito: el kill
                         # switch activo es (se espera) un evento raro, y
                         # cortar a 60d podría perder los pocos casos reales
                         # que sí ocurran antes de poder analizarlos.
FORWARD_HORIZON = 5      # ruedas hacia adelante para medir el efecto (mismo
                         # horizonte "corto plazo" que predictor.py/pred_5d)
STALENESS_DAYS  = 6      # mismo criterio que predictor_validation.py


# ── 1. Logging (corre cada run, append-only) ────────────────────────────

def log_kill_switch_event(
    global_conf: dict,
    quality_resumen: dict = None,
    validacion_nivel: str = "OK",
    sla_status: str = "OK",
    n_signals: int = 0,
) -> bool:
    """
    Agrega una entrada para la corrida actual a data/kill_switch_history.json,
    agrupado por fecha (puede haber hasta 4 entradas por día, una por
    ventana de ejecución). Nunca muta entradas pasadas -- la interpretación
    retroactiva vive en evaluate_kill_switch_history(), no acá.

    No lanza excepciones hacia pipeline.py: esto es observabilidad, nunca
    debe poder tumbar una corrida.
    """
    global_conf     = global_conf or {}
    quality_resumen = quality_resumen or {}

    try:
        from src.github_persistence import load_json, save_json

        log   = load_json(HISTORY_PATH, default={})
        today = datetime.now().strftime("%Y-%m-%d")

        criticas = quality_resumen.get("criticas", 0)
        entry = {
            "timestamp":              datetime.now().isoformat(),
            "global_score":           global_conf.get("global_score"),
            "label":                  global_conf.get("label"),
            "kill_switch_active":     bool(global_conf.get("kill_switch_active", False)),
            "kill_switch_reasons":    global_conf.get("kill_switch_reasons", []),
            "criticas":               criticas,
            "criticas_estructurales": quality_resumen.get("criticas_estructurales", criticas),
            "advertencias":           quality_resumen.get("advertencias", 0),
            "validacion_nivel":       validacion_nivel,
            "sla_status":             sla_status,
            "n_signals":              n_signals,
        }

        log.setdefault(today, []).append(entry)

        cutoff = (datetime.now() - timedelta(days=MAX_DAYS)).strftime("%Y-%m-%d")
        log = {d: v for d, v in log.items() if d >= cutoff}

        ok = save_json(
            HISTORY_PATH, log,
            message=f"auto: kill_switch_history {today} (active={entry['kill_switch_active']})",
        )
        if entry["kill_switch_active"]:
            logger.warning(f"[kill_switch_log] Run registrado con kill switch ACTIVO ({today})")
        return ok

    except Exception as e:
        logger.warning(f"[kill_switch_log] No se pudo registrar evento: {e}")
        return False


# ── 2. Evaluación retroactiva (corre ~1x/semana) ────────────────────────

def evaluate_kill_switch_history(price_data: dict, index_cols: dict) -> dict:
    """
    Para cada fecha del historial, mide el forward return a FORWARD_HORIZON
    ruedas del índice de cada mercado, separando días con kill switch activo
    (helped si el índice cayó) de días sin kill switch activo (baseline para
    contraste).

    Args:
        price_data:  {"merval": df, "bovespa": df, "sp500": df} -- mismos
                     DataFrames con índice de fechas que usa el resto del
                     pipeline (downloader.py).
        index_cols:  {"merval": "INDICE MERVAL", ...} -- nombre de columna
                     del índice por mercado (ya resuelto en pipeline.py via
                     _idx_col(), se reusa tal cual).
    """
    from src.github_persistence import load_json, save_json, push_file

    existing = load_json(VALIDATION_PATH, default={})
    generated_str = existing.get("generated")
    if generated_str:
        try:
            age_days = (datetime.now() - datetime.fromisoformat(generated_str)).days
            if age_days < STALENESS_DAYS:
                logger.info(f"[kill_switch_log] Validación reciente ({age_days}d), saltando")
                return existing
        except Exception:
            pass  # 'generated' mal formado -> regenerar

    log = load_json(HISTORY_PATH, default={})
    if not log:
        logger.info("[kill_switch_log] Sin historial todavía, nada que evaluar")
        return existing

    # Un día cuenta como "activo" si CUALQUIER corrida de ese día tuvo el
    # kill switch activo -- es un circuit breaker de sistema, basta que se
    # haya frenado capital una sola vez en el día para que el día cuente.
    active_by_date = {
        d: any(bool(e.get("kill_switch_active")) for e in entries)
        for d, entries in log.items()
    }

    records = []
    for market, df in (price_data or {}).items():
        col = (index_cols or {}).get(market, "")
        if not col or df is None or df.empty or col not in df.columns:
            continue
        serie = df[col].dropna().sort_index()
        if len(serie) < FORWARD_HORIZON + 1:
            continue

        for date_str, active in active_by_date.items():
            fwd_ret = _forward_return(serie, date_str, FORWARD_HORIZON)
            if fwd_ret is None:
                continue  # fecha no encontrada en la serie, o sin suficiente futuro todavía
            records.append({
                "date":            date_str,
                "market":          market,
                "kill_switch_active": active,
                "forward_return_5d":  fwd_ret,
            })

    if not records:
        logger.info("[kill_switch_log] Sin registros evaluables (falta historia de precios futura)")
        return existing

    result = {
        "generated":      datetime.now().isoformat(),
        "n_records":      len(records),
        "forward_horizon_dias": FORWARD_HORIZON,
        "global":         _aggregate(records),
        "by_market":      {m: _aggregate([r for r in records if r["market"] == m])
                            for m in sorted({r["market"] for r in records})},
    }

    try:
        import os
        os.makedirs("data", exist_ok=True)
        import json
        with open(VALIDATION_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        push_file(
            VALIDATION_PATH,
            f"auto: kill_switch_validation {datetime.now().strftime('%Y-%m-%d %H:%M')} "
            f"({len(records)} registros)",
        )
    except Exception as e:
        logger.warning(f"[kill_switch_log] No se pudo persistir validación: {e}")

    g = result["global"]
    logger.info(
        f"[kill_switch_log] ✅ {len(records)} registros | "
        f"activo: helped={g['active']['pct_decline']}% (n={g['active']['n']}) | "
        f"baseline: helped={g['inactive']['pct_decline']}% (n={g['inactive']['n']})"
    )
    return result


def _forward_return(serie, date_str: str, horizon: int):
    """Retorno % del índice a `horizon` ruedas desde `date_str`, o None si la
    fecha no está en la serie o todavía no hay `horizon` ruedas posteriores
    (caso normal para eventos muy recientes -- se reevalúa la semana
    próxima cuando ya haya precio suficiente)."""
    import pandas as pd
    try:
        ts = pd.Timestamp(date_str)
    except Exception:
        return None

    idx = serie.index
    if ts not in idx:
        # Fecha de log sin rueda exacta (fin de semana / feriado en algún
        # mercado puntual) -- se descarta en vez de aproximar, para no
        # introducir ruido en algo que ya es una muestra chica.
        return None

    pos = idx.get_loc(ts)
    if isinstance(pos, slice):  # índice con duplicados, no debería pasar
        return None
    if pos + horizon >= len(serie):
        return None  # todavía no pasaron suficientes ruedas

    base   = serie.iloc[pos]
    future = serie.iloc[pos + horizon]
    if base == 0:
        return None
    return round(float((future / base - 1) * 100), 2)


def _aggregate(records: list[dict]) -> dict:
    """Separa por kill_switch_active y calcula promedio + % de caídas (la
    interpretación de 'ayudó') para cada grupo."""
    def _stats(subset):
        n = len(subset)
        if n == 0:
            return {"n": 0, "avg_return_5d": None, "pct_decline": None}
        rets = [r["forward_return_5d"] for r in subset]
        avg = round(sum(rets) / n, 2)
        pct_decline = round(100 * sum(1 for r in rets if r < 0) / n, 1)
        return {"n": n, "avg_return_5d": avg, "pct_decline": pct_decline}

    active   = [r for r in records if r["kill_switch_active"]]
    inactive = [r for r in records if not r["kill_switch_active"]]
    return {"active": _stats(active), "inactive": _stats(inactive)}


# ── Resumen para logs/dashboard/Telegram (consistente con get_validation_summary) ─

def get_kill_switch_validation_summary() -> dict:
    from src.github_persistence import load_json
    data = load_json(VALIDATION_PATH, default={})
    if not data:
        return {"available": False}
    return {
        "available": True,
        "generated": data.get("generated", "")[:10],
        "n_records": data.get("n_records", 0),
        "global":    data.get("global", {}),
    }
