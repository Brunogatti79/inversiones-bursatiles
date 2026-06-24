"""
src/predictor_validation.py — Predictor vs Baselines (Prioridad 3, roadmap externo)

PROBLEMA QUE RESUELVE:
  El predictor (predictor.py) es un ensemble de 4 modelos (Holt-Winters +
  Gradient Boosting + Random Forest + Linear baseline) pero nunca se midió
  contra algo simple. Sin esa comparación no se sabe si agrega valor real
  o si un modelo trivial (no predecir nada, o asumir que la tendencia
  continúa) explica igual o mejor los retornos futuros.

POR QUÉ ES ACCIONABLE HOY (a diferencia de otras validaciones que necesitan
semanas de signals_history.json):
  predictor.py usa exclusivamente features derivados de PRECIO (r3/r5/r10/r21,
  vol10/vol21, dist_MA20/50, rsi_proxy, dist_high) -- nada de macro ni de
  señales en vivo. Eso significa que se puede backtestear retroactivamente
  sobre los 12 meses de precios ya disponibles en los CSVs, sin esperar a
  acumular historia real. Mismo principio que historical_replay.py, pero acá
  se llama al predictor REAL en vez de reimplementar una versión simplificada
  del score técnico.

DISEÑO:
  Para cada ticker, ~10-12 snapshots espaciados a lo largo del año
  (más espaciados que los 52 semanales de historical_replay.py -- llamar al
  ensemble completo, con GBR haciendo cross-validation interna, es mucho más
  caro por evaluación que el score técnico simplificado de historical_replay):
    1. Slice de precio hasta esa fecha (sin lookahead)
    2. predictor.predict_ticker() sobre ese slice -> pred_5d/10d/21d reales
    3. 3 baselines sobre el mismo slice (zero / momentum / promedio histórico)
    4. Retorno real futuro a 5d/10d/21d (datos posteriores, no usado en (2)/(3))

AISLAMIENTO DE CACHE:
  predictor._CACHE está keyeado solo por ticker (no por fecha) -- llamar a
  predict_ticker() para el mismo ticker en distintos puntos históricos dentro
  de la misma corrida pisaría resultados entre snapshots si se usa el cache
  de producción. Este módulo aísla _CACHE/_CACHE_DATE/CACHE_PATH durante toda
  la validación y los restaura al terminar -- nunca toca ni infla
  data/pred_cache.json de producción.

OUTPUT: data/predictor_validation.json
  {
    "generated": "...", "n_snapshots": 640,
    "global": {"predictor": {...}, "zero": {...}, "momentum": {...}, "historical_avg": {...}},
    "by_market": {...}, "by_horizon": {...},
  }

PERSISTENCIA: mismo patrón que historical_replay.py (fix 24/06/2026) desde
el día 1 -- push_file() al generar, pull_file() en el sync de arranque,
staleness contra el campo 'generated' del contenido (no mtime local).

USO desde pipeline.py (1x/semana, igual que historical_replay):
    from src.predictor_validation import run_predictor_validation
    run_predictor_validation(price_data, ticker_cols)
"""

import json
import os
import logging
from datetime import datetime, date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

VALIDATION_PATH = "data/predictor_validation.json"
MIN_TRAIN       = 90          # días mínimos de historia antes del primer snapshot
SNAPSHOT_FREQ   = 21          # ~1 snapshot por mes (más espaciado que historical_replay
                               # a propósito: el ensemble completo es mucho más caro por
                               # evaluación que el score técnico simplificado)
HORIZONS        = [5, 10, 21]
MAX_SNAPSHOTS_PER_TICKER = 12  # tope duro -- nunca corre indefinidamente aunque
                                # haya años de historia


# ── Entrypoint principal ────────────────────────────────────────────────

def run_predictor_validation(price_data: dict, ticker_cols: dict) -> dict:
    """
    Backtest retroactivo del predictor real contra 3 baselines.

    Args:
        price_data:  {"merval": df, "bovespa": df, "sp500": df}
        ticker_cols: {ticker: col_name}
    """
    existing = _load_validation()
    generated_str = existing.get("generated")
    if generated_str:
        try:
            age_days = (datetime.now() - datetime.fromisoformat(generated_str)).days
            if age_days < 6:
                logger.info(f"[predictor_validation] Datos recientes ({age_days}d), saltando")
                return existing
        except Exception:
            pass  # 'generated' mal formado -> regenerar

    logger.info("[predictor_validation] Iniciando validación retroactiva del predictor...")

    col_to_ticker = {v: k for k, v in ticker_cols.items()}
    market_label = {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}

    records = []
    with _isolated_predictor_cache():
        for market_key, df in price_data.items():
            if df is None or df.empty:
                continue
            market = market_label.get(market_key, market_key.upper())

            for col in df.columns:
                ticker = col_to_ticker.get(col, col)
                if any(x in col.upper() for x in ["MERVAL", "BOVESPA", "S&P", "INDEX", "^"]):
                    continue  # saltar índices

                serie = df[col].dropna()
                records.extend(_validate_ticker(ticker, market, serie))

    if not records:
        logger.warning("[predictor_validation] Sin registros generados")
        return {}

    result = {
        "generated":   datetime.now().isoformat(),
        "n_snapshots": len(records),
        "global":      _aggregate(records),
        "by_market":   {m: _aggregate([r for r in records if r["mercado"] == m])
                         for m in sorted({r["mercado"] for r in records})},
        "by_horizon":  {h: _aggregate(records, only_horizon=h) for h in HORIZONS},
    }

    _save_validation(result)
    logger.info(
        f"[predictor_validation] ✅ {len(records)} registros | "
        f"predictor dir_acc={result['global']['predictor']['directional_accuracy']} vs "
        f"momentum={result['global']['momentum']['directional_accuracy']} "
        f"zero={result['global']['zero']['directional_accuracy']}"
    )
    return result


def _validate_ticker(ticker: str, market: str, serie: pd.Series) -> list[dict]:
    """Genera los registros de comparación predictor-vs-baselines para un ticker."""
    n = len(serie)
    eval_end = n - max(HORIZONS) - 2
    if eval_end <= MIN_TRAIN:
        return []

    cutoffs = list(range(MIN_TRAIN, eval_end, SNAPSHOT_FREQ))[:MAX_SNAPSHOTS_PER_TICKER]
    records = []

    for snap_i, end_idx in enumerate(cutoffs):
        slice_serie = serie.iloc[:end_idx]
        precio = float(slice_serie.iloc[-1])
        if precio <= 0 or len(slice_serie) < 30:
            continue

        try:
            cache_key = f"__VALID__{ticker}__{snap_i}"
            pred = _predict_ticker_module().predict_ticker(cache_key, slice_serie, include_submodels=True)
        except Exception as e:
            logger.warning(f"[predictor_validation] {ticker} snapshot {snap_i}: predictor falló ({e})")
            continue

        submodels = pred.get("submodels", {})

        baselines = {
            "zero":           _zero_baseline(),
            "momentum":       _momentum_baseline(slice_serie),
            "historical_avg": _historical_avg_baseline(slice_serie),
        }

        actuals = {}
        for h in HORIZONS:
            if end_idx + h < n:
                future_price = float(serie.iloc[end_idx + h])
                actuals[h] = round((future_price / precio - 1) * 100, 2)
            else:
                actuals[h] = None

        if all(v is None for v in actuals.values()):
            continue

        records.append({
            "ticker": ticker, "mercado": market, "snapshot": snap_i,
            "predictor": {5: pred.get("pred_5d"), 10: pred.get("pred_10d"), 21: pred.get("pred_21d")},
            "submodels": submodels,
            "baselines": baselines,
            "actual":    actuals,
        })

    return records


def _predict_ticker_module():
    """Import diferido (evita import circular si algún día predictor.py
    importa algo de este módulo) y deja un único punto para monkeypatchear
    en tests."""
    import src.predictor as predictor
    return predictor


# ── Baselines ────────────────────────────────────────────────────────────

def _zero_baseline() -> dict:
    """Random walk / null model: predice 0% para todos los horizontes."""
    return {h: 0.0 for h in HORIZONS}


def _momentum_baseline(serie: pd.Series) -> dict:
    """Asume que el retorno de los últimos H días se repite hacia adelante."""
    out = {}
    for h in HORIZONS:
        if len(serie) > h:
            ret = (float(serie.iloc[-1]) / float(serie.iloc[-1 - h]) - 1) * 100
            out[h] = round(ret, 2)
        else:
            out[h] = None
    return out


def _historical_avg_baseline(serie: pd.Series) -> dict:
    """Promedio histórico del retorno a H días, calculado SOLO con datos
    hasta el punto de corte (sin lookahead) -- ventana expansiva, no toda
    la serie completa."""
    out = {}
    for h in HORIZONS:
        rets_h = serie.pct_change(h, fill_method=None).dropna()
        out[h] = round(float(rets_h.mean()) * 100, 2) if len(rets_h) > 0 else None
    return out


# ── Agregación de métricas ──────────────────────────────────────────────

# ── Agregación de métricas ──────────────────────────────────────────────

BASELINE_METHODS  = ["momentum", "zero", "historical_avg"]
SUBMODEL_METHODS  = ["holt_winters", "gradient_boosting", "random_forest", "linear_baseline"]


def _aggregate(records: list, only_horizon: int = None) -> dict:
    """
    Calcula directional_accuracy / MAE / correlación para el predictor
    (ensemble final), cada baseline, y cada sub-modelo individual (Prioridad
    3 ampliada: "¿hay un modelo arrastrando al resto?"), sobre los registros
    dados (opcionalmente filtrados a un solo horizonte).

    Nota sobre random_forest: mientras ENABLE_RF_PREDICTOR=false (estado
    actual de producción), siempre predice 0.0 -- sale en esta agregación
    con métricas pobres por diseño, no porque el modelo en sí sea malo.
    Comparar de verdad requiere reactivarlo primero.
    """
    horizons = [only_horizon] if only_horizon else HORIZONS
    out = {}

    def _collect(value_lookup):
        preds, actuals = [], []
        for r in records:
            for h in horizons:
                actual = r["actual"].get(h)
                if actual is None:
                    continue
                pred_val = value_lookup(r, h)
                if pred_val is None:
                    continue
                preds.append(float(pred_val))
                actuals.append(float(actual))
        return _metrics(preds, actuals)

    out["predictor"] = _collect(lambda r, h: r["predictor"].get(h))

    for method in BASELINE_METHODS:
        out[method] = _collect(lambda r, h, m=method: r["baselines"].get(m, {}).get(h))

    for model in SUBMODEL_METHODS:
        out[model] = _collect(lambda r, h, m=model: r.get("submodels", {}).get(m, {}).get(h))

    return out


def _metrics(preds: list, actuals: list) -> dict:
    if len(preds) < 5:
        return {"n": len(preds), "directional_accuracy": None, "mae": None, "correlation": None}

    p = np.array(preds, dtype=float)
    a = np.array(actuals, dtype=float)

    # Direccional: 0% se cuenta como "no acertó ni erró" en zero_baseline
    # (predicción exactamente 0 nunca coincide en signo con un retorno real
    # distinto de 0) -- es la métrica correcta para juzgar si vale la pena
    # predecir dirección en vez de no decir nada.
    same_sign = np.sign(p) == np.sign(a)
    dir_acc = round(float(same_sign.mean()), 3)
    mae = round(float(np.mean(np.abs(p - a))), 3)

    corr = None
    if len(p) >= 5 and np.std(p) > 1e-9 and np.std(a) > 1e-9:
        corr = round(float(np.corrcoef(p, a)[0, 1]), 3)

    return {"n": len(preds), "directional_accuracy": dir_acc, "mae": mae, "correlation": corr}


# ── Aislamiento de cache (no tocar data/pred_cache.json de producción) ──

class _isolated_predictor_cache:
    """Context manager: aísla _CACHE/_CACHE_DATE/CACHE_PATH de predictor.py
    durante la validación retroactiva, y restaura el estado original al
    salir -- sin esto, las ~600+ llamadas a predict_ticker() de este módulo
    (cada una con una cache_key sintética distinta) inflarían
    data/pred_cache.json sin límite y nunca se limpiarían.

    CACHE_PATH temporal único por entrada al context manager (PID + tiempo
    de alta resolución), no una ruta fija -- encontrado durante el
    desarrollo de este módulo: con una ruta fija, dos corridas de
    run_predictor_validation() el mismo día calendario dentro del mismo
    proceso largo (ej. Railway sin reiniciar) podrían leer el cache
    'aislado' de la corrida anterior en vez de arrancar limpio, porque
    predictor._load_cache() solo chequea que la fecha adentro del archivo
    sea 'hoy' -- no le importa de qué corrida vino. En producción esto es
    improbable (el gate semanal evita llamar a esto dos veces el mismo
    día), pero la garantía de aislamiento debería sostenerse sin depender
    de esa coincidencia."""

    def __enter__(self):
        import tempfile
        predictor = _predict_ticker_module()
        self._saved_cache = predictor._CACHE
        self._saved_date  = predictor._CACHE_DATE
        self._saved_path  = predictor.CACHE_PATH
        self._tmp_path    = tempfile.mktemp(prefix="_predictor_validation_cache_", suffix=".json")
        predictor._CACHE = {}
        predictor._CACHE_DATE = date.today().isoformat()
        predictor.CACHE_PATH = self._tmp_path
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        predictor = _predict_ticker_module()
        predictor._CACHE = self._saved_cache
        predictor._CACHE_DATE = self._saved_date
        predictor.CACHE_PATH = self._saved_path
        try:
            if os.path.exists(self._tmp_path):
                os.remove(self._tmp_path)
        except Exception:
            pass
        return False


# ── Persistencia (mismo patrón que historical_replay.py, fix 24/06/2026) ─

def _load_validation() -> dict:
    from src.github_persistence import load_json
    return load_json(VALIDATION_PATH, default={})


def _save_validation(result: dict):
    os.makedirs("data", exist_ok=True)
    with open(VALIDATION_PATH, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=None)

    from src.github_persistence import push_file
    push_file(
        VALIDATION_PATH,
        f"auto: predictor_validation {datetime.now().strftime('%Y-%m-%d %H:%M')} "
        f"({result.get('n_snapshots', 0)} snapshots)",
    )


def get_validation_summary() -> dict:
    """Resumen para logs/dashboard/Telegram."""
    data = _load_validation()
    if not data:
        return {"available": False}
    return {
        "available":   True,
        "generated":   data.get("generated", "")[:10],
        "n_snapshots": data.get("n_snapshots", 0),
        "global":      data.get("global", {}),
    }
