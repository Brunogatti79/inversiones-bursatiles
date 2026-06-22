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


def _build_features(serie: np.ndarray, context: dict = None):
    """
    Feature engineering compartido (10 features técnicas + 4 de contexto).
    Factorizado de _gradient_boosting para reusar en _random_forest y
    _linear_baseline (mejora 2.4) sin duplicar la lógica.
    Retorna (X, s) o (None, None) si la serie es muy corta.
    """
    s = pd.Series(serie)
    if len(s) < 25:
        return None, None

    r3  = s.pct_change(3).fillna(0)
    r5  = s.pct_change(5).fillna(0)
    r10 = s.pct_change(10).fillna(0)
    r21 = s.pct_change(21).fillna(0)

    vol10 = s.pct_change().rolling(10).std().fillna(0)
    vol21 = s.pct_change().rolling(21).std().fillna(0)

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
    sp500_trend  = np.full(len(s), float(ctx.get("sp500_trend_score", 50)) / 100)
    macro_local  = np.full(len(s), float(ctx.get("macro_score", 50)) / 100)
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


def _gradient_boosting(serie: np.ndarray, horizon: int, context: dict = None) -> tuple[float, float]:
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

        X, s = _build_features(serie, context)
        if X is None:
            return 0.0, 0.3

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


def _random_forest(serie: np.ndarray, horizon: int, context: dict = None) -> tuple[float, float]:
    """
    Mejora 2.4: Random Forest Regressor — mismo feature set que el GBR pero
    vía bagging en vez de boosting. Aporta diversidad real al ensemble: GBR
    y RF cometen errores correlacionados con menos frecuencia que dos
    variantes del mismo método, lo que reduce el riesgo de que el ensemble
    entero se equivoque junto en el mismo escenario.
    Retorna (forecast_pct_change, confidence 0-1).
    """
    n = len(serie)
    if n < 40:
        return 0.0, 0.3

    try:
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.model_selection import cross_val_score

        X, s = _build_features(serie, context)
        if X is None:
            return 0.0, 0.3

        y_raw = s.pct_change(horizon).shift(-horizon).fillna(0)

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
            n_estimators=100, max_depth=4, min_samples_leaf=5,
            random_state=42, n_jobs=1,
        )
        rf.fit(X_train, y_train)

        x_last = X[-1:].copy()
        pred_pct = float(rf.predict(x_last)[0]) * 100

        try:
            scores = cross_val_score(rf, X_train, y_train, cv=3,
                                     scoring="neg_mean_absolute_error")
            mae = -float(scores.mean())
            conf = max(0.4, min(0.85, 0.82 - mae * 8))
        except Exception:
            conf = 0.48

        return _safe_float(pred_pct, 0.0), conf

    except ImportError:
        return 0.0, 0.30
    except Exception as e:
        logger.debug(f"RF error: {e}")
        return 0.0, 0.30


def _linear_baseline(serie: np.ndarray, horizon: int, context: dict = None) -> tuple[float, float]:
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

        X, s = _build_features(serie, context)
        if X is None:
            return 0.0, 0.25

        y_raw = s.pct_change(horizon).shift(-horizon).fillna(0)
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

def predict_ticker(ticker: str, serie: pd.Series, context: dict = None) -> dict:
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
        gb5,  conf_gb5  = _gradient_boosting(arr, 5,  context=context)
        gb21, conf_gb21 = _gradient_boosting(arr, 21, context=context)

        # ── Modelo 3: Random Forest (mejora 2.4 — diversidad real vs GBR)
        rf5,  conf_rf5  = _random_forest(arr, 5,  context=context)
        rf21, conf_rf21 = _random_forest(arr, 21, context=context)

        # ── Modelo 4: Baseline lineal (mejora 2.4 — control de cordura/overfitting)
        ln5,  conf_ln5  = _linear_baseline(arr, 5,  context=context)
        ln21, conf_ln21 = _linear_baseline(arr, 21, context=context)

        # ── Ensemble ponderado por confianza (generalizado a N modelos)
        def ensemble(*pairs):
            """pairs: lista de (valor, confianza). Promedio ponderado por
            confianza + bonus si la mayoría coincide en dirección."""
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
        gb10, conf_gb10 = _gradient_boosting(arr, 10, context=context)
        hw10, conf_hw10 = _holt_winters(arr, 10)
        rf10, conf_rf10 = _random_forest(arr, 10, context=context)
        ln10, conf_ln10 = _linear_baseline(arr, 10, context=context)
        p10, c10 = ensemble((hw10, conf_hw10), (gb10, conf_gb10), (rf10, conf_rf10), (ln10, conf_ln10))

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

def run_predictions(signals: list[dict], price_data: dict, ticker_cols: dict = None, context: dict = None) -> list[dict]:
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

        pred = predict_ticker(ticker, serie, context=context)

        # Determinar si la predicción coincide con la señal del modelo
        signal_up = "COMPRA" in s.get("signal", "")
        pred_up   = (pred.get("pred_21d") or 0) > 2
        pred["pred_direction_agree"] = (signal_up == pred_up)

        s.update(pred)
        enriched += 1

    logger.info(f"Predicciones: {enriched} calculadas, {skipped} sin datos")
    return signals
