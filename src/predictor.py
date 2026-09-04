"""
src/predictor.py
Predictor ARIMA Ensemble para Inversiones Bursátiles.
Modelo ligero: Holt-Winters (ExponentialSmoothing) + Gradient Boosting.
Horizontes: 5d, 10d, 21d por ticker.
Cache diario para evitar recalcular en runs sucesivos del mismo día.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, date

logger = logging.getLogger(__name__)

# ── Reliability weights (27/07/2026, roadmap externo P3) ───────────────────────
# El ensemble ponderaba cada submodelo SOLO por la confianza que ese modelo
# reporta de sí mismo (ancho de intervalo de Holt-Winters, CV score de GBR,
# etc.) — nunca por qué tan bien predijo en la realidad. predictor_validation.py
# ya mide esto (2016 observaciones a la fecha de este cambio): holt_winters
# accuracy 52.0%/correlación 0.156, gradient_boosting 47.6%/0.012, linear_baseline
# 50.1%/-0.096 (correlación NEGATIVA — peor que ruido). Decisión explícita de
# Bruno (27/07/2026): linear_baseline pasa a peso 0 (se excluye del ensemble,
# ensemble() ya ignora automáticamente pairs con confianza <=0); holt_winters y
# gradient_boosting quedan ponderados por su correlación real, normalizada
# contra el mejor de los dos (así el mejor modelo no pierde confianza global,
# y el más débil aporta en proporción a lo poco que realmente aporta).
# random_forest no se toca acá — ya en confianza 0 mientras ENABLE_RF_PREDICTOR
# esté en false (decisión aparte de Bruno).
VALIDATION_PATH = "data/predictor_validation.json"
_RELIABILITY_CACHE: dict = None


def _load_reliability_weights() -> dict:
    """
    Devuelve un multiplicador (0-1) por submodelo para aplicar sobre su
    confianza auto-reportada, derivado de la correlación GLOBAL real en
    data/predictor_validation.json. Fallback a 1.0 en todos (sin cambio de
    comportamiento respecto a antes de este fix) si el archivo no existe
    todavía, no tiene la forma esperada, o falla la lectura — para no romper
    una corrida si predictor_validation.py todavía no corrió ni una vez.
    """
    default = {"holt_winters": 1.0, "gradient_boosting": 1.0, "linear_baseline": 1.0}
    try:
        with open(VALIDATION_PATH) as f:
            data = json.load(f)
    except Exception:
        return default

    if not isinstance(data, dict):
        return default
    glob = data.get("global") or {}
    corr_hw  = (glob.get("holt_winters") or {}).get("correlation")
    corr_gbr = (glob.get("gradient_boosting") or {}).get("correlation")
    if corr_hw is None or corr_gbr is None:
        return default

    corr_hw  = max(0.0, float(corr_hw))
    corr_gbr = max(0.0, float(corr_gbr))
    best = max(corr_hw, corr_gbr) or 1.0

    return {
        "holt_winters":      round(corr_hw / best, 3),
        "gradient_boosting": round(corr_gbr / best, 3),
        "linear_baseline":   0.0,  # decisión explícita 27/07/2026: correlación negativa, se excluye
    }


def _get_reliability_weights() -> dict:
    """Cachea _load_reliability_weights() a nivel de módulo — se llama una vez
    por ticker (78 veces por corrida); no tiene sentido releer el archivo cada
    vez dentro del mismo proceso de pipeline."""
    global _RELIABILITY_CACHE
    if _RELIABILITY_CACHE is None:
        _RELIABILITY_CACHE = _load_reliability_weights()
        logger.info(f"Predictor reliability weights (real, no auto-reportada): {_RELIABILITY_CACHE}")
    return _RELIABILITY_CACHE


# ── Cache ──────────────────────────────────────────────────────────────────────
CACHE_PATH = "data/pred_cache.json"
_CACHE: dict = {}
_CACHE_DATE: str = ""

# FIX 04/09/2026 (hallazgo real: predictor MERVAL con 35.1% de acierto
# direccional a 21d -- peor que el azar -- auditoría en sesión con Bruno,
# confirmado que se mantuvo predictor.py como "SUBA" ~4 semanas seguidas
# mientras el índice caía -4% a -12% cada vez). Causa raíz encontrada en
# _build_features(): sp500_trend_score/macro_score/cross_market_regime SÍ
# estaban ahí como columnas, pero con np.full(len(s), valor_de_HOY) --
# la misma constante repetida en cada fila del set de entrenamiento
# (que cubre meses de historia). Un feature sin varianza no le enseña
# nada a un árbol de decisión: el GBR terminaba prediciendo casi
# exclusivamente en base a momentum/mean-reversion (RSI, distancia a
# medias), que en una caída sostenida de semanas insiste en "sobrevendido,
# viene el rebote" sin ninguna noción de que el régimen macro seguía mal.
#
# _load_historical_context_map() arma un mapa {fecha: {...}} con los
# valores REALES de cada día, leídos de signals_history.json --
# score_macro, vol_regime_mercado y cross_market_regime SÍ se persisten
# por señal desde hace semanas, solo que nunca se habían usado para esto.
# Cacheado en memoria (mismo proceso) porque se llama una vez por ticker
# en run_predictions() -- no tiene sentido releer/reparsear el JSON
# completo ~67 veces por corrida.
_CONTEXT_HISTORY_CACHE: dict | None = None
_SIGNALS_HISTORY_PATH = "data/signals_history.json"


def _load_historical_context_map(history_path: str = _SIGNALS_HISTORY_PATH) -> dict:
    """
    Devuelve {fecha_str: {"MERVAL": {...}, "BOVESPA": {...}, "SP500": {...},
    "cross_market_regime": str|None}}, leído de signals_history.json.

    Por mercado se guarda score_macro y vol_regime_mercado (el primero que
    aparezca ese día -- son valores de mercado, no de ticker, deberían
    coincidir entre todos los tickers del mismo mercado el mismo día).
    Para SP500 además se guarda market_trend_score, usado como proxy de
    "sp500_trend_score" (el indicador líder global que usa cross_market.py)
    -- no hay un campo separado con ese nombre exacto persistido
    históricamente, pero el market_trend_score de un ticker SP500 ES ese
    mismo número (cross_market.py usa SP500 como mercado líder).
    cross_market_regime es global (no varía entre mercados el mismo día),
    se guarda una sola vez por fecha.

    Nunca levanta excepción hacia el caller -- si signals_history.json no
    existe o está corrupto, devuelve {} y _build_features cae al
    comportamiento viejo (broadcast del contexto de hoy).
    """
    global _CONTEXT_HISTORY_CACHE
    if _CONTEXT_HISTORY_CACHE is not None:
        return _CONTEXT_HISTORY_CACHE

    result: dict = {}
    try:
        if os.path.exists(history_path):
            with open(history_path) as f:
                raw = json.load(f)
            for date_str, entries in raw.items():
                day = {"MERVAL": {}, "BOVESPA": {}, "SP500": {}, "cross_market_regime": None}
                for e in entries:
                    mkt = e.get("mercado")
                    if mkt not in ("MERVAL", "BOVESPA", "SP500"):
                        continue
                    if "score_macro" not in day[mkt] and e.get("score_macro") is not None:
                        day[mkt]["score_macro"] = e["score_macro"]
                    if "vol_regime_mercado" not in day[mkt] and e.get("vol_regime_mercado"):
                        day[mkt]["vol_regime_mercado"] = e["vol_regime_mercado"]
                    if mkt == "SP500" and "market_trend_score" not in day["SP500"] \
                       and e.get("market_trend_score") is not None:
                        day["SP500"]["market_trend_score"] = e["market_trend_score"]
                    if day["cross_market_regime"] is None and e.get("cross_market_regime"):
                        day["cross_market_regime"] = e["cross_market_regime"]
                result[date_str] = day
    except Exception as e:
        logger.warning(f"[predictor] No se pudo cargar historial de contexto: {e}")
        result = {}

    _CONTEXT_HISTORY_CACHE = result
    return result


def _context_series_for(history_map: dict, market: str, dates) -> dict | None:
    """
    Alinea el historial real de contexto a las fechas exactas de la serie
    de precios de un ticker (`dates`), con forward-fill (si una fecha no
    tiene dato propio, usa el último valor conocido ANTERIOR -- nunca el
    de una fecha futura, para no filtrar información hacia atrás en el
    tiempo) y un default neutro (50 / NORMAL / NEUTRAL) para el tramo
    inicial sin ningún dato previo disponible.

    Devuelve None si `history_map` está vacío (deja que el caller decida
    el fallback) -- nunca levanta excepción.
    """
    if not history_map:
        return None
    try:
        dates_idx = pd.DatetimeIndex(pd.to_datetime(dates))
        hist_dates = sorted(history_map.keys())
        hist_idx = pd.DatetimeIndex(pd.to_datetime(hist_dates))

        macro_raw  = [history_map[d].get(market, {}).get("score_macro") for d in hist_dates]
        vol_raw    = [history_map[d].get(market, {}).get("vol_regime_mercado") for d in hist_dates]
        sp_raw     = [history_map[d].get("SP500", {}).get("market_trend_score") for d in hist_dates]
        regime_raw = [history_map[d].get("cross_market_regime") for d in hist_dates]

        def _align_numeric(raw_vals, default):
            s = pd.Series(raw_vals, index=hist_idx).sort_index()
            s = s[~s.index.duplicated(keep="last")]
            union_idx = s.index.union(dates_idx)
            aligned = s.reindex(union_idx).ffill().reindex(dates_idx)
            return aligned.fillna(default).astype(float).values

        def _align_categorical(raw_vals, mapping, default_code):
            s = pd.Series(raw_vals, index=hist_idx).sort_index()
            s = s[~s.index.duplicated(keep="last")]
            union_idx = s.index.union(dates_idx)
            aligned = s.reindex(union_idx).ffill().reindex(dates_idx)
            return aligned.map(mapping).fillna(default_code).astype(float).values

        return {
            "macro_local": _align_numeric(macro_raw, 50.0) / 100.0,
            "sp500_trend": _align_numeric(sp_raw, 50.0) / 100.0,
            "vol_regime_v": _align_categorical(
                vol_raw, {"LOW": 0.25, "NORMAL": 0.50, "HIGH": 0.75}, 0.50),
            "regime_v": _align_categorical(
                regime_raw, {"RISK_ON": 0.75, "NEUTRAL": 0.50, "RISK_OFF": 0.25}, 0.50),
        }
    except Exception as e:
        logger.debug(f"[predictor] Contexto histórico no disponible, fallback a broadcast: {e}")
        return None


def _load_cache():
    global _CACHE, _CACHE_DATE
    today = date.today().isoformat()
    if _CACHE_DATE == today and _CACHE:
        return
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH) as f:
                data = json.load(f)
            if data.get("date") == today:
                _CACHE = data.get("predictions", {})
                _CACHE_DATE = today
                logger.info(f"Cache de predicciones cargado ({len(_CACHE)} tickers)")
                return
    except Exception:
        pass
    _CACHE = {}
    _CACHE_DATE = today
    global _CONTEXT_HISTORY_CACHE
    _CONTEXT_HISTORY_CACHE = None

def _save_cache():
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "w") as f:
            json.dump({"date": _CACHE_DATE, "predictions": _CACHE}, f)
    except Exception as e:
        logger.warning(f"No se pudo guardar cache de predicciones: {e}")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(v, default=None):
    try:
        r = float(v)
        return r if np.isfinite(r) else default
    except Exception:
        return default


def _truncate_at_last_split(serie: pd.Series, threshold: float = 0.60) -> pd.Series:
    """
    FIX 04/09/2026 (mismo incidente YA documentado en backtester.py
    _detect_split_horizon, 10/08/2026: YPF y Mirgor hicieron split 1:10 el
    03/08/2026 -- ver ese docstring para el contexto completo). backtester.py
    se protege truncando el TRADE en el salto (mira hacia ADELANTE desde
    precio_entry), pero predictor.py nunca tuvo la misma protección porque
    entrena sus modelos sobre la serie histórica completa hacia ATRÁS, un
    código de ingesta de precios totalmente separado del backtester.

    Caso real que confirmó que hacía falta: MIRG.BA dio pred_21d=-1307.42%
    el 2026-08-04 -- exactamente un día después del split real de esa
    misma acción (precio de $16.350 a $1.640, ratio ~10:1). Sin ajustar,
    Holt-Winters/GBR/RF/Ridge fitean esa caída de -90% de un día para el
    otro como si fuera un movimiento de mercado real.

    Recorre la serie de atrás para adelante buscando el salto más
    reciente (cambio relativo >= threshold, mismo 0.60 default que
    backtester.py para quedar consistentes) entre dos precios
    consecutivos, y devuelve solo el tramo POSTERIOR a ese salto -- el
    tramo pre-split queda descartado, no se intenta reconstruir el ratio
    para "corregir" los precios viejos (mismo argumento que backtester.py:
    adivinar mal el ratio sería peor que no usar esos datos). Si no hay
    ningún salto, devuelve la serie completa sin tocar.

    Efecto colateral esperado y aceptado: un ticker recién splitteado
    puede quedar sin predicción (por debajo del mínimo de observaciones)
    hasta acumular suficiente historia post-split -- mismo trade-off que
    ya aceptaste en backtester.py ("se prioriza no contaminar sobre no
    perder esas observaciones").
    """
    if len(serie) < 2:
        return serie
    vals = serie.values
    split_pos = None  # índice del primer valor YA post-split
    for i in range(len(vals) - 1, 0, -1):
        prev, curr = vals[i - 1], vals[i]
        if prev is None or prev <= 0 or curr is None or curr <= 0:
            continue
        cambio = abs(float(curr) / float(prev) - 1)
        if cambio >= threshold:
            split_pos = i
            break
    if split_pos is None:
        return serie
    return serie.iloc[split_pos:]


def _clamp_pred(value: float, horizon: int) -> tuple[float, bool]:
    """
    Clamp de sanidad defensivo (04/09/2026) -- segunda capa de protección,
    además de _truncate_at_last_split(). Ese fix cubre la causa RAÍZ del
    caso real encontrado (MIRG.BA, pred_21d=-1307% el 04/08, por un split
    no ajustado), pero esto actúa como red de seguridad genérica ante
    cualquier OTRO glitch numérico no relacionado a splits (outlier en el
    CSV, fit inestable de Holt-Winters, etc.) -- un pred_21d por debajo de
    -100% es matemáticamente imposible (el precio no puede caer más del
    100%), así que un valor así SIEMPRE es un bug, nunca una predicción
    válida real, sin importar la causa.

    Límites deliberadamente generosos y escalados por horizonte -- no son
    un límite de "qué tan lejos puede moverse el precio de verdad" (eso lo
    filtran otras partes del sistema), son un piso/techo de sanidad
    matemática nada más. Devuelve (valor_clampeado, se_clampeo).
    """
    upper = {5: 60.0, 10: 80.0, 21: 120.0}.get(horizon, 100.0)
    clamped = max(-95.0, min(upper, value))
    return clamped, (clamped != value)


def _pred_signal(ret_21d: float) -> str:
    if ret_21d is None:
        return "➡️ LATERAL"
    if ret_21d > 5:
        return "📈 SUBA"
    if ret_21d < -10:
        return "⚠️ BAJA FUERTE"
    if ret_21d < -2:
        return "📉 BAJA"
    return "➡️ LATERAL"


def _holt_winters(serie: np.ndarray, horizon: int) -> tuple[float, float]:
    """
    Exponential Smoothing simple. Retorna (forecast_pct_change, confidence 0-1).
    Usa statsmodels si disponible, fallback a EWM simple.
    """
    n = len(serie)
    if n < 20:
        return 0.0, 0.3

    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        model = ExponentialSmoothing(
            serie, trend="add", seasonal=None, initialization_method="estimated"
        ).fit(optimized=True, use_brute=False)
        fcast = model.forecast(horizon)
        pred_val = float(fcast[-1])
        last_val = float(serie[-1])
        pct = (pred_val / last_val - 1) * 100 if last_val != 0 else 0.0
        # Confianza: inversa al error relativo del fit
        fitted = model.fittedvalues
        residuals = np.abs(serie[-30:] - fitted[-30:]) / (np.abs(serie[-30:]) + 1e-8)
        conf = max(0.3, min(0.9, 1 - float(residuals.mean())))
        return _safe_float(pct, 0.0), conf
    except Exception:
        pass

    # Fallback EWM
    ewm = pd.Series(serie).ewm(span=min(21, n // 2)).mean()
    slope = float(ewm.iloc[-1] - ewm.iloc[-min(5, n // 3)]) / max(1, min(5, n // 3))
    pred_pct = slope / float(serie[-1]) * 100 * horizon if serie[-1] != 0 else 0.0
    return _safe_float(pred_pct, 0.0), 0.45


def _build_features(serie: np.ndarray, context: dict = None,
                     dates=None, market: str = None, history: dict = None):
    """
    Feature engineering compartido (10 features técnicas + 4 de contexto).
    Factorizado de _gradient_boosting para reusar en _random_forest y
    _linear_baseline (mejora 2.4) sin duplicar la lógica.
    Retorna (X, s) o (None, None) si la serie es muy corta.

    `dates`/`market`/`history` (agregados 04/09/2026, ver comentario junto
    a _load_historical_context_map): cuando los 3 están presentes, las 4
    columnas de contexto (sp500_trend, macro_local, vol_regime, regime) se
    alinean a la fecha real de CADA fila histórica en vez de repetir el
    valor de hoy en todas -- ver docstring de _context_series_for. Si
    falta alguno de los 3, o el historial no tiene datos, cae al
    comportamiento viejo (broadcast de `context`) -- backward compatible,
    ningún caller existente se rompe por no pasar estos parámetros.
    """
    s = pd.Series(serie)
    if len(s) < 25:
        return None, None

    r3  = s.pct_change(3, fill_method=None).fillna(0)
    r5  = s.pct_change(5, fill_method=None).fillna(0)
    r10 = s.pct_change(10, fill_method=None).fillna(0)
    r21 = s.pct_change(21, fill_method=None).fillna(0)

    vol10 = s.pct_change(fill_method=None).rolling(10).std().fillna(0)
    vol21 = s.pct_change(fill_method=None).rolling(21).std().fillna(0)

    ma20  = s.rolling(20).mean().fillna(s)
    ma50  = s.rolling(50).mean().fillna(s)
    dist_ma20 = ((s - ma20) / ma20.replace(0, 1)).fillna(0)
    dist_ma50 = ((s - ma50) / ma50.replace(0, 1)).fillna(0)

    delta     = s.diff().fillna(0)
    gain      = delta.clip(lower=0).rolling(14).mean().fillna(0)
    loss      = (-delta.clip(upper=0)).rolling(14).mean().fillna(0)
    rs        = gain / (loss.replace(0, 1e-9))
    rsi_proxy = (100 - 100 / (1 + rs)).fillna(50)

    high_52w   = s.rolling(min(252, len(s))).max().fillna(s)
    dist_high  = ((s - high_52w) / high_52w.replace(0, 1)).fillna(0)

    ctx = context or {}
    hist_ctx = None
    if dates is not None and market is not None and history:
        hist_ctx = _context_series_for(history, market, dates)

    if hist_ctx is not None and len(hist_ctx["macro_local"]) == len(s):
        sp500_trend  = hist_ctx["sp500_trend"]
        macro_local  = hist_ctx["macro_local"]
        vol_regime_v = hist_ctx["vol_regime_v"]
        regime_v     = hist_ctx["regime_v"]
    else:
        # Fallback: comportamiento viejo, mismo valor (de hoy) repetido en
        # todas las filas -- se mantiene por compatibilidad para callers
        # que no pasan dates/market/history, o para cuando el historial
        # todavía no tiene datos suficientes.
        sp500_trend  = np.full(len(s), float(ctx.get("sp500_trend_score", 50)) / 100)
        # FIX 04/09/2026: si vino macro_scores_by_market (pipeline.py) y
        # tenemos `market`, usar el macro score del mercado correcto en
        # vez del "macro_score" plano, que siempre era el de SP500.
        _macro_by_mkt = ctx.get("macro_scores_by_market") or {}
        _macro_fallback = _macro_by_mkt.get(market) if market else None
        if _macro_fallback is None:
            _macro_fallback = ctx.get("macro_score", 50)
        macro_local  = np.full(len(s), float(_macro_fallback) / 100)
        vol_regime_f = {"LOW": 0.25, "NORMAL": 0.50, "HIGH": 0.75}.get(
                          ctx.get("vol_regime", "NORMAL"), 0.50)
        vol_regime_v = np.full(len(s), vol_regime_f)
        regime_enc   = {"RISK_ON": 0.75, "NEUTRAL": 0.50, "RISK_OFF": 0.25}.get(
                          ctx.get("cross_market_regime", "NEUTRAL"), 0.50)
        regime_v     = np.full(len(s), regime_enc)

    X = np.column_stack([
        r3, r5, r10, r21,
        vol10, vol21,
        dist_ma20, dist_ma50,
        rsi_proxy / 100,
        dist_high,
        sp500_trend, macro_local, vol_regime_v, regime_v,
    ])
    return X, s


def _gradient_boosting(serie: np.ndarray, horizon: int, context: dict = None,
                        dates=None, market: str = None, history: dict = None) -> tuple[float, float]:
    """
    Gradient Boosting Regressor con features de rolling.
    Retorna (forecast_pct_change, confidence 0-1).
    """
    n = len(serie)
    if n < 40:
        return 0.0, 0.3

    try:
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import cross_val_score

        X, s = _build_features(serie, context, dates=dates, market=market, history=history)
        if X is None:
            return 0.0, 0.3

        y_raw = s.pct_change(horizon, fill_method=None).shift(-horizon).fillna(0)

        # Ventana mínima de entrenamiento: 60 muestras
        min_train = 60
        if n < min_train + horizon + 10:
            return 0.0, 0.35

        X_train = X[:-horizon]
        y_train = y_raw.values[:-horizon]
        # Quitar NaN
        mask = np.isfinite(X_train).all(axis=1) & np.isfinite(y_train)
        X_train, y_train = X_train[mask], y_train[mask]

        if len(X_train) < 30:
            return 0.0, 0.35

        gbr = GradientBoostingRegressor(
            n_estimators=80, max_depth=3, learning_rate=0.05,
            subsample=0.8, random_state=42
        )
        gbr.fit(X_train, y_train)

        # Predicción con última fila de features
        x_last = X[-1:].copy()
        pred_pct = float(gbr.predict(x_last)[0]) * 100

        # Confianza via cross-val (rápida, 3 folds)
        try:
            scores = cross_val_score(gbr, X_train, y_train, cv=3,
                                     scoring="neg_mean_absolute_error")
            mae = -float(scores.mean())
            # Normalizar: MAE < 0.01 → conf ~0.85, MAE > 0.05 → conf ~0.45
            conf = max(0.4, min(0.88, 0.85 - mae * 8))
        except Exception:
            conf = 0.50

        return _safe_float(pred_pct, 0.0), conf

    except ImportError:
        logger.warning("sklearn no disponible, usando fallback para predicciones")
        return 0.0, 0.30
    except Exception as e:
        logger.debug(f"GBR error: {e}")
        return 0.0, 0.30


def _random_forest(serie: np.ndarray, horizon: int, context: dict = None,
                    dates=None, market: str = None, history: dict = None) -> tuple[float, float]:
    """
    Mejora 2.4: Random Forest Regressor — mismo feature set que el GBR pero
    vía bagging en vez de boosting. Aporta diversidad real al ensemble: GBR
    y RF cometen errores correlacionados con menos frecuencia que dos
    variantes del mismo método, lo que reduce el riesgo de que el ensemble
    entero se equivoque junto en el mismo escenario.

    Nota de performance (incidente 23/06/2026): usa oob_score (nativo de
    RandomForest, gratis durante el fit) en vez de cross_val_score — evita
    3 fits adicionales por llamada. Con ~67 tickers × 3 horizontes, eso son
    ~600 fits de RF menos por corrida del pipeline.

    Retorna (forecast_pct_change, confidence 0-1).
    """
    n = len(serie)
    if n < 40:
        return 0.0, 0.3

    try:
        from sklearn.ensemble import RandomForestRegressor

        X, s = _build_features(serie, context, dates=dates, market=market, history=history)
        if X is None:
            return 0.0, 0.3

        y_raw = s.pct_change(horizon, fill_method=None).shift(-horizon).fillna(0)

        min_train = 60
        if n < min_train + horizon + 10:
            return 0.0, 0.35

        X_train = X[:-horizon]
        y_train = y_raw.values[:-horizon]
        mask = np.isfinite(X_train).all(axis=1) & np.isfinite(y_train)
        X_train, y_train = X_train[mask], y_train[mask]

        if len(X_train) < 30:
            return 0.0, 0.35

        rf = RandomForestRegressor(
            n_estimators=50, max_depth=4, min_samples_leaf=5,
            random_state=42, n_jobs=1, oob_score=True, bootstrap=True,
        )
        rf.fit(X_train, y_train)

        x_last = X[-1:].copy()
        pred_pct = float(rf.predict(x_last)[0]) * 100

        try:
            # oob_score_ es R² sobre las muestras out-of-bag — gratis, no
            # requiere fits adicionales (a diferencia de cross_val_score)
            r2 = max(0.0, min(1.0, rf.oob_score_))
            conf = max(0.4, min(0.85, 0.45 + r2 * 0.40))
        except Exception:
            conf = 0.48

        return _safe_float(pred_pct, 0.0), conf

    except ImportError:
        return 0.0, 0.30
    except Exception as e:
        logger.debug(f"RF error: {e}")
        return 0.0, 0.30


def _linear_baseline(serie: np.ndarray, horizon: int, context: dict = None,
                      dates=None, market: str = None, history: dict = None) -> tuple[float, float]:
    """
    Mejora 2.4: baseline lineal (Ridge regularizado) sobre el mismo feature
    set. Sirve como control de cordura del ensemble: si GBR/RF predicen un
    movimiento fuerte que el modelo lineal no detecta en absoluto, suele ser
    señal de overfitting a ruido de corto plazo más que de una relación real
    — por eso pesa menos en la confianza, pero participa en el voto de
    dirección. Confianza deliberadamente conservadora (no captura no
    linealidades), nunca por encima de los modelos no lineales.
    Retorna (forecast_pct_change, confidence 0-1).
    """
    n = len(serie)
    if n < 30:
        return 0.0, 0.25

    try:
        from sklearn.linear_model import Ridge

        X, s = _build_features(serie, context, dates=dates, market=market, history=history)
        if X is None:
            return 0.0, 0.25

        y_raw = s.pct_change(horizon, fill_method=None).shift(-horizon).fillna(0)
        if n < 40 + horizon:
            return 0.0, 0.25

        X_train = X[:-horizon]
        y_train = y_raw.values[:-horizon]
        mask = np.isfinite(X_train).all(axis=1) & np.isfinite(y_train)
        X_train, y_train = X_train[mask], y_train[mask]

        if len(X_train) < 25:
            return 0.0, 0.25

        model = Ridge(alpha=1.0, random_state=42)
        model.fit(X_train, y_train)

        x_last = X[-1:].copy()
        pred_pct = float(model.predict(x_last)[0]) * 100

        # R² in-sample como proxy de confianza (capeado bajo — es un modelo
        # simple a propósito, no debería dominar el ensemble)
        try:
            r2 = max(0.0, min(1.0, model.score(X_train, y_train)))
            conf = max(0.25, min(0.55, 0.30 + r2 * 0.25))
        except Exception:
            conf = 0.30

        return _safe_float(pred_pct, 0.0), conf

    except ImportError:
        return 0.0, 0.25
    except Exception as e:
        logger.debug(f"Linear baseline error: {e}")
        return 0.0, 0.25


# ── Predicción por ticker ──────────────────────────────────────────────────────

def predict_ticker(ticker: str, serie: pd.Series, context: dict = None,
                    include_submodels: bool = False,
                    market: str = None, history: dict = None) -> dict:
    """
    Genera predicciones ensemble para un ticker dado su serie de precios.
    Retorna dict con pred_5d, pred_10d, pred_21d, pred_target,
    pred_confidence, pred_signal, pred_method, pred_direction_agree.

    `market`/`history` (agregados 04/09/2026): ver comentario junto a
    _load_historical_context_map -- permiten que el GBR/RF/Ridge usen el
    régimen/macro REAL de cada fecha histórica en vez de repetir el de
    hoy. Opcionales (default None) por compatibilidad -- sin ellos, cae
    al comportamiento viejo (broadcast de `context`).

    include_submodels=True (default False, sin efecto en producción):
    agrega result["submodels"] con el (valor, confianza) crudo de cada uno
    de los 4 modelos por horizonte -- pensado para
    predictor_validation.py, para poder medir si alguno de los 4 está
    arrastrando al resto en vez de aportar. No se usa en el pipeline
    normal (run_predictions no lo pasa).

    OJO con el cache si algún día se llama con include_submodels=True
    usando el MISMO ticker (cache key) que ya se usó sin ese flag en la
    misma ventana de cache diario: el resultado cacheado se devuelve tal
    cual estaba, sin el campo "submodels" -- esto no pasa hoy porque el
    único caller con include_submodels=True (predictor_validation.py)
    siempre usa cache keys sintéticas y únicas por snapshot.
    """
    _load_cache()
    if ticker in _CACHE:
        return _CACHE[ticker]

    result = {
        "pred_5d":  None, "pred_10d": None, "pred_21d": None,
        "pred_target": None, "pred_confidence": None,
        "pred_signal": "", "pred_method": "ensemble",
        "pred_direction_agree": False,
    }

    try:
        serie = serie.dropna()
        # FIX 04/09/2026: descartar el tramo pre-split si lo hay -- ver
        # docstring de _truncate_at_last_split. Tiene que ir ANTES del
        # chequeo de longitud mínima: un ticker recién splitteado con
        # poca historia post-split debe caer al "sin datos aún" (result
        # default), no fitear sobre una discontinuidad de -90%.
        serie = _truncate_at_last_split(serie)
        if len(serie) < 30:
            return result

        dates = serie.index
        arr = serie.values.astype(float)
        price_last = float(arr[-1])

        # ── Modelo 1: Holt-Winters
        hw5,  conf_hw5  = _holt_winters(arr, 5)
        hw21, conf_hw21 = _holt_winters(arr, 21)

        # ── Modelo 2: Gradient Boosting
        gb5,  conf_gb5  = _gradient_boosting(arr, 5,  context=context, dates=dates, market=market, history=history)
        gb21, conf_gb21 = _gradient_boosting(arr, 21, context=context, dates=dates, market=market, history=history)

        # ── Modelo 3: Random Forest (mejora 2.4 — diversidad real vs GBR)
        # DESACTIVADO TEMPORALMENTE (incidente 23/06/2026): es el modelo más
        # pesado en CPU/memoria de los 4 — candidato principal a estar
        # provocando que Railway mate el contenedor a mitad de la corrida
        # (sin excepción de Python, sin notificación — consistente con OOM).
        # Reactivar con env var ENABLE_RF_PREDICTOR=true una vez confirmado
        # que el contenedor soporta la carga (ver Metrics de memoria en Railway).
        if os.getenv("ENABLE_RF_PREDICTOR", "false").lower() == "true":
            rf5,  conf_rf5  = _random_forest(arr, 5,  context=context, dates=dates, market=market, history=history)
            rf21, conf_rf21 = _random_forest(arr, 21, context=context, dates=dates, market=market, history=history)
        else:
            rf5,  conf_rf5  = 0.0, 0.0
            rf21, conf_rf21 = 0.0, 0.0

        # ── Modelo 4: Baseline lineal (mejora 2.4 — control de cordura/overfitting)
        ln5,  conf_ln5  = _linear_baseline(arr, 5,  context=context, dates=dates, market=market, history=history)
        ln21, conf_ln21 = _linear_baseline(arr, 21, context=context, dates=dates, market=market, history=history)

        # Mejora 27/07/2026: ponderar cada submodelo por su precisión REAL
        # validada (correlación global en predictor_validation.json), no solo
        # por la confianza que cada uno reporta de sí mismo — ver docstring de
        # _load_reliability_weights(). linear_baseline queda en 0 (excluido);
        # ensemble() ya ignora automáticamente pairs con confianza <=0.
        _rel = _get_reliability_weights()
        conf_hw5  = round(conf_hw5  * _rel["holt_winters"], 4)
        conf_hw21 = round(conf_hw21 * _rel["holt_winters"], 4)
        conf_gb5  = round(conf_gb5  * _rel["gradient_boosting"], 4)
        conf_gb21 = round(conf_gb21 * _rel["gradient_boosting"], 4)
        conf_ln5  = round(conf_ln5  * _rel["linear_baseline"], 4)
        conf_ln21 = round(conf_ln21 * _rel["linear_baseline"], 4)

        # ── Ensemble ponderado por confianza (generalizado a N modelos)
        def ensemble(*pairs):
            """pairs: lista de (valor, confianza). Ignora automáticamente los
            modelos con confianza 0 (ej. RF desactivado). Promedio ponderado
            por confianza + bonus si la mayoría coincide en dirección."""
            pairs = [(v, c) for v, c in pairs if c > 0] or [(0.0, 0.3)]
            total_c = sum(c for _, c in pairs)
            if total_c < 1e-6:
                vals = [v for v, _ in pairs]
                return sum(vals) / len(vals), max(c for _, c in pairs)
            val = sum(v * c for v, c in pairs) / total_c
            conf = total_c / len(pairs)
            n_pos = sum(1 for v, _ in pairs if v >= 0)
            n_neg = len(pairs) - n_pos
            # Bonus si la mayoría (no necesariamente todos) coincide en dirección
            if max(n_pos, n_neg) >= len(pairs) - 1 and len(pairs) > 1:
                conf = min(0.95, conf * 1.10)
            return val, conf

        p5,  c5  = ensemble((hw5, conf_hw5), (gb5, conf_gb5), (rf5, conf_rf5), (ln5, conf_ln5))
        p21, c21 = ensemble((hw21, conf_hw21), (gb21, conf_gb21), (rf21, conf_rf21), (ln21, conf_ln21))

        # pred_10d: predicción real a 10d (no interpolación)
        gb10, conf_gb10 = _gradient_boosting(arr, 10, context=context, dates=dates, market=market, history=history)
        hw10, conf_hw10 = _holt_winters(arr, 10)
        if os.getenv("ENABLE_RF_PREDICTOR", "false").lower() == "true":
            rf10, conf_rf10 = _random_forest(arr, 10, context=context, dates=dates, market=market, history=history)
        else:
            rf10, conf_rf10 = 0.0, 0.0
        ln10, conf_ln10 = _linear_baseline(arr, 10, context=context, dates=dates, market=market, history=history)
        conf_hw10 = round(conf_hw10 * _rel["holt_winters"], 4)
        conf_gb10 = round(conf_gb10 * _rel["gradient_boosting"], 4)
        conf_ln10 = round(conf_ln10 * _rel["linear_baseline"], 4)
        p10, c10 = ensemble((hw10, conf_hw10), (gb10, conf_gb10), (rf10, conf_rf10), (ln10, conf_ln10))

        # FIX 04/09/2026: clamp de sanidad defensivo -- ver docstring de
        # _clamp_pred. Se loguea si alguno se clampeó, para poder ir
        # auditando si aparecen más casos además del de MIRG.BA (que ya
        # debería quedar cubierto por _truncate_at_last_split, esto es
        # la red de seguridad de más atrás).
        p5,  clamped5  = _clamp_pred(p5, 5)
        p10, clamped10 = _clamp_pred(p10, 10)
        p21, clamped21 = _clamp_pred(p21, 21)
        if clamped5 or clamped10 or clamped21:
            logger.warning(
                f"[predictor] {ticker}: predicción fuera de rango plausible, clampeada "
                f"(5d={p5} 10d={p10} 21d={p21}) -- revisar posible split no detectado u otro outlier"
            )

        # Target precio a 21d
        target = round(price_last * (1 + p21 / 100), 2)

        # Confianza global
        conf_global = round((c5 + c21) / 2, 3)

        # Señal predictiva
        sig = _pred_signal(p21)

        result.update({
            "pred_5d":  round(p5, 2),
            "pred_10d": round(p10, 2),
            "pred_21d": round(p21, 2),
            "pred_target":     target,
            "pred_confidence": conf_global,
            "pred_signal":     sig,
            "pred_method":     "ensemble",
            "reliability_weights": _rel,
        })

        if include_submodels:
            result["submodels"] = {
                "holt_winters":      {5: round(hw5, 2),  10: round(hw10, 2),  21: round(hw21, 2)},
                "gradient_boosting": {5: round(gb5, 2),  10: round(gb10, 2),  21: round(gb21, 2)},
                "random_forest":     {5: round(rf5, 2),  10: round(rf10, 2),  21: round(rf21, 2)},
                "linear_baseline":   {5: round(ln5, 2),  10: round(ln10, 2),  21: round(ln21, 2)},
                "rf_enabled":        os.getenv("ENABLE_RF_PREDICTOR", "false").lower() == "true",
            }

    except Exception as e:
        logger.warning(f"Predicción fallida para {ticker}: {e}")

    _CACHE[ticker] = result
    _save_cache()
    return result


# ── Función principal: predecir todos los tickers ─────────────────────────────

def run_predictions(signals: list[dict], price_data: dict, ticker_cols: dict = None, context: dict = None) -> list[dict]:
    """
    Enriquece cada señal en `signals` con campos pred_* usando price_data.
    price_data: {'merval': df, 'bovespa': df, 'sp500': df}
    Retorna la lista de señales actualizada.
    """
    _load_cache()
    # FIX 04/09/2026: se carga UNA vez por corrida (no por ticker) -- ver
    # docstring de _load_historical_context_map. `context` (el snapshot de
    # hoy) se sigue pasando igual, queda como fallback para fechas sin
    # datos en el historial.
    history_map = _load_historical_context_map()
    enriched = 0
    skipped  = 0

    for s in signals:
        ticker = s.get("ticker", "")
        market = s.get("mercado", "")
        df_key = "merval" if market == "MERVAL" else "bovespa" if market == "BOVESPA" else "sp500"
        df     = price_data.get(df_key)

        if df is None or df.empty:
            skipped += 1
            continue

        # Buscar columna del ticker — 3 estrategias
        col = None
        ticker_base = ticker.replace(".BA", "").replace(".SA", "")
        # Estrategia 1: ticker base como substring (TRAN → TRANSENER)
        for c in df.columns:
            if ticker_base.upper() in c.upper():
                col = c
                break
        # Estrategia 2: ticker sin número final (PETR4→PETR, VALE3→VALE)
        if col is None and ticker_base and ticker_base[-1].isdigit():
            ticker_nonum = ticker_base.rstrip("0123456789")
            if len(ticker_nonum) >= 3:
                for c in df.columns:
                    if ticker_nonum.upper() in c.upper():
                        col = c
                        break
        # Estrategia 3: mapeo directo ticker→empresa usando dicts del downloader
        if col is None:
            try:
                from src.downloader import MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
                all_tickers = {**MERVAL_TICKERS, **BOVESPA_TICKERS, **SP500_TICKERS}
                empresa = all_tickers.get(ticker, "")
                if empresa:
                    empresa_up = empresa.upper()
                    # Buscar columna cuyo nombre coincide mejor con la empresa
                    best, best_score = None, 0
                    for c in df.columns:
                        c_up = c.upper()
                        # Score: palabras del nombre empresa que aparecen en columna
                        words = [w for w in empresa_up.split() if len(w) >= 3]
                        score = sum(1 for w in words if w in c_up)
                        if score > best_score:
                            best_score = score
                            best = c
                    if best_score > 0:
                        col = best
            except ImportError:
                pass

        if col is None:
            skipped += 1
            continue

        serie = df[col].dropna()
        if len(serie) < 30:
            skipped += 1
            continue

        pred = predict_ticker(ticker, serie, context=context, market=market, history=history_map)

        # Determinar si la predicción coincide con la señal del modelo
        signal_up = "COMPRA" in s.get("signal", "")
        pred_up   = (pred.get("pred_21d") or 0) > 2
        pred["pred_direction_agree"] = (signal_up == pred_up)

        s.update(pred)
        enriched += 1

    logger.info(f"Predicciones: {enriched} calculadas, {skipped} sin datos")
    return signals
