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

# ── Cache ──────────────────────────────────────────────────────────────────────
CACHE_PATH = "data/pred_cache.json"
_CACHE: dict = {}
_CACHE_DATE: str = ""

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


def _gradient_boosting(serie: np.ndarray, horizon: int) -> tuple[float, float]:
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

        s = pd.Series(serie)
        # Features: retornos rolling 5/10/21, volatilidad, RSI proxy
        r5  = s.pct_change(5).fillna(0)
        r10 = s.pct_change(10).fillna(0)
        r21 = s.pct_change(21).fillna(0)
        vol = s.pct_change().rolling(10).std().fillna(0)
        ma5  = s.rolling(5).mean().fillna(s)
        ma20 = s.rolling(20).mean().fillna(s)
        dist = ((s - ma20) / (ma20.replace(0, 1))).fillna(0)

        X = np.column_stack([r5, r10, r21, vol, dist])
        y_raw = s.pct_change(horizon).shift(-horizon).fillna(0)

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


# ── Predicción por ticker ──────────────────────────────────────────────────────

def predict_ticker(ticker: str, serie: pd.Series) -> dict:
    """
    Genera predicciones ensemble para un ticker dado su serie de precios.
    Retorna dict con pred_5d, pred_10d, pred_21d, pred_target,
    pred_confidence, pred_signal, pred_method, pred_direction_agree.
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
        if len(serie) < 30:
            return result

        arr = serie.values.astype(float)
        price_last = float(arr[-1])

        # ── Modelo 1: Holt-Winters
        hw5,  conf_hw5  = _holt_winters(arr, 5)
        hw21, conf_hw21 = _holt_winters(arr, 21)

        # ── Modelo 2: Gradient Boosting
        gb5,  conf_gb5  = _gradient_boosting(arr, 5)
        gb21, conf_gb21 = _gradient_boosting(arr, 21)

        # ── Ensemble ponderado por confianza
        def ensemble(hw, c_hw, gb, c_gb):
            total = c_hw + c_gb
            if total < 1e-6:
                return (hw + gb) / 2, max(c_hw, c_gb)
            val = (hw * c_hw + gb * c_gb) / total
            conf = (c_hw + c_gb) / 2
            # Bonus si coinciden en dirección
            if (hw >= 0) == (gb >= 0):
                conf = min(0.95, conf * 1.10)
            return val, conf

        p5,  c5  = ensemble(hw5,  conf_hw5,  gb5,  conf_gb5)
        p21, c21 = ensemble(hw21, conf_hw21, gb21, conf_gb21)

        # pred_10d: interpolación simple
        p10 = (p5 + p21) / 2

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
        })

    except Exception as e:
        logger.warning(f"Predicción fallida para {ticker}: {e}")

    _CACHE[ticker] = result
    _save_cache()
    return result


# ── Función principal: predecir todos los tickers ─────────────────────────────

def run_predictions(signals: list[dict], price_data: dict) -> list[dict]:
    """
    Enriquece cada señal en `signals` con campos pred_* usando price_data.
    price_data: {'merval': df, 'bovespa': df, 'sp500': df}
    Retorna la lista de señales actualizada.
    """
    _load_cache()
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

        # Buscar columna del ticker
        col = None
        ticker_base = ticker.replace(".BA", "").replace(".SA", "")
        for c in df.columns:
            if ticker_base.upper() in c.upper():
                col = c
                break

        if col is None:
            skipped += 1
            continue

        serie = df[col].dropna()
        if len(serie) < 30:
            skipped += 1
            continue

        pred = predict_ticker(ticker, serie)

        # Determinar si la predicción coincide con la señal del modelo
        signal_up = "COMPRA" in s.get("signal", "")
        pred_up   = (pred.get("pred_21d") or 0) > 2
        pred["pred_direction_agree"] = (signal_up == pred_up)

        s.update(pred)
        enriched += 1

    logger.info(f"Predicciones: {enriched} calculadas, {skipped} sin datos")
    return signals
