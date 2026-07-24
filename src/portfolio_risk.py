"""
src/portfolio_risk.py — Portfolio Risk Engine (roadmap "Institucional PRO", 24/07/2026)

Correlaciones entre posiciones, VaR paramétrico, y exposición por
mercado/sector del portfolio ACTUAL -- a diferencia de portfolio_optimizer.py
(que calcula asignación de capital para señales de COMPRA nuevas), esto
analiza lo que ya está en cartera.

LIMITACIÓN CONOCIDA Y DELIBERADA: VaR PARAMÉTRICO (varianza-covarianza), no
histórico. data/portfolio_value_history.json recién empezó a acumularse el
23/07/2026 y no alcanza para un VaR histórico confiable (necesita ~90+ días
para ser mínimamente sólido, ~250 para el estándar institucional
RiskMetrics). El paramétrico no necesita esa historia -- se calcula con la
matriz de correlación/volatilidad de los activos (ya disponible en los CSVs
de precios) y los pesos actuales del portfolio, ambos disponibles hoy. La
idea es sumar el histórico como una segunda capa más adelante, no
reemplazar esto -- son metodologías complementarias, no una mejor que la
otra en abstracto.
"""
import logging

import numpy as np

logger = logging.getLogger(__name__)

# z-scores estándar para VaR paramétrico bajo normalidad, hardcodeados para
# no sumar una dependencia nueva (scipy) solo por esto -- son constantes
# bien conocidas (percentiles de la normal estándar), no valores que cambien.
Z_SCORES = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}

# Mismo criterio que portfolio_optimizer.py::_covariance_adjustment (ventana
# de 60 días hábiles) -- consistencia entre los dos módulos que calculan
# covarianza sobre la misma clase de datos.
COV_WINDOW = 60


def _build_returns_matrix(tickers: list, price_data: dict, ticker_cols: dict, window: int = COV_WINDOW):
    """
    Extrae retornos diarios (pct_change) de los últimos `window` días para
    cada ticker pedido, a partir de los DataFrames de precios ya cargados
    en el pipeline (merval/bovespa/sp500).

    Tickers sin al menos `window` observaciones se EXCLUYEN, no se rellenan
    con ceros ni se interpolan -- eso distorsionaría la covarianza real.

    Devuelve (tickers_validos, matriz) -- matriz shape (n_validos, window).
    Si no hay ningún ticker con datos suficientes, devuelve ([], array vacío).
    """
    returns_dict = {}
    col_to_ticker = {v: k for k, v in ticker_cols.items()}

    for _, df in price_data.items():
        if df is None or df.empty:
            continue
        for col in df.columns:
            ticker = col_to_ticker.get(col, col)
            if ticker not in tickers:
                continue
            series = df[col].pct_change(fill_method=None).dropna()
            if len(series) >= window:
                returns_dict[ticker] = series.tail(window).values

    tickers_validos = [t for t in tickers if t in returns_dict]
    if not tickers_validos:
        return [], np.array([])
    matriz = np.array([returns_dict[t] for t in tickers_validos])
    return tickers_validos, matriz


def compute_correlation_matrix(portfolio: dict, price_data: dict, ticker_cols: dict,
                                 window: int = COV_WINDOW) -> dict:
    """Matriz de correlación entre las posiciones actuales del portfolio,
    más un ranking de los pares más correlacionados (positiva o
    negativamente) -- la forma más rápida de leer concentración de riesgo
    oculta entre posiciones que a simple vista parecen diversificadas."""
    tickers = [p["ticker"] for p in portfolio.get("positions", [])]
    if not tickers:
        return {"status": "portfolio_vacio"}

    tickers_validos, R = _build_returns_matrix(tickers, price_data, ticker_cols, window)
    n_sin_datos = len(tickers) - len(tickers_validos)

    if len(tickers_validos) < 2:
        return {
            "status": "insuficiente_historia",
            "tickers_con_datos": len(tickers_validos),
            "tickers_sin_datos": n_sin_datos,
            "nota": f"Necesita ≥2 posiciones con ≥{window} días de historia de precios",
        }

    corr = np.corrcoef(R)
    matriz = {
        tickers_validos[i]: {
            tickers_validos[j]: round(float(corr[i, j]), 3)
            for j in range(len(tickers_validos))
        }
        for i in range(len(tickers_validos))
    }

    pares = []
    for i in range(len(tickers_validos)):
        for j in range(i + 1, len(tickers_validos)):
            pares.append({
                "par": [tickers_validos[i], tickers_validos[j]],
                "correlacion": round(float(corr[i, j]), 3),
            })
    pares.sort(key=lambda x: abs(x["correlacion"]), reverse=True)

    return {
        "status": "ok",
        "tickers_con_datos": len(tickers_validos),
        "tickers_sin_datos": n_sin_datos,
        "ventana_dias": window,
        "matriz": matriz,
        "pares_mas_correlacionados": pares[:10],
    }


def compute_parametric_var(portfolio: dict, price_data: dict, ticker_cols: dict,
                             confidence: float = 0.95, horizon_days: int = 1,
                             window: int = COV_WINDOW) -> dict:
    """
    VaR paramétrico (varianza-covarianza) del portfolio actual.

        VaR = z_alpha · sqrt(w^T · Σ · w) · valor_portfolio · sqrt(horizon_days)

    Σ = matriz de covarianza DIARIA de retornos (ventana `window`).
    w = pesos actuales por posición (valor_actual_usd / valor total).
    El factor sqrt(horizon_days) escala de 1 día a un horizonte mayor,
    asumiendo retornos i.i.d. (supuesto estándar de la convención
    RiskMetrics, no exacto pero es lo habitual para este tipo de cálculo).

    Interpretación: "con `confidence`% de confianza, el portfolio no
    debería perder más de VaR_usd en los próximos `horizon_days` días
    hábiles" -- bajo el supuesto de que los retornos futuros se parecen a
    los últimos `window` días (la limitación central de cualquier VaR
    paramétrico: es tan bueno como lo estable que sea el régimen actual).
    """
    if confidence not in Z_SCORES:
        return {"status": "confidence_no_soportada", "soportadas": list(Z_SCORES.keys())}

    positions = portfolio.get("positions", [])
    valor_total = sum(p.get("valor_actual_usd", 0) or 0 for p in positions)
    if valor_total <= 0:
        return {"status": "portfolio_vacio"}

    tickers = [p["ticker"] for p in positions]
    tickers_validos, R = _build_returns_matrix(tickers, price_data, ticker_cols, window)

    if len(tickers_validos) < 1:
        return {
            "status": "insuficiente_historia",
            "tickers_con_datos": 0,
            "nota": f"Necesita ≥1 posición con ≥{window} días de historia de precios",
        }

    valores_por_ticker = {p["ticker"]: p.get("valor_actual_usd", 0) or 0 for p in positions}
    valor_con_datos = sum(valores_por_ticker.get(t, 0) for t in tickers_validos)
    if valor_con_datos <= 0:
        return {"status": "sin_valor_en_posiciones_con_datos"}

    w = np.array([valores_por_ticker.get(t, 0) / valor_con_datos for t in tickers_validos])
    cov = np.cov(R)
    if cov.ndim == 0:
        cov = np.array([[float(cov)]])

    var_portfolio_diario_frac = float(np.sqrt(max(w @ cov @ w, 0.0)))
    z = Z_SCORES[confidence]
    var_frac = z * var_portfolio_diario_frac * np.sqrt(horizon_days)
    var_usd = var_frac * valor_con_datos
    pct_cubierto = round(valor_con_datos / valor_total * 100, 1)

    return {
        "status": "ok",
        "confidence": confidence,
        "horizon_days": horizon_days,
        "var_pct": round(var_frac * 100, 2),
        "var_usd": round(var_usd, 2),
        "valor_portfolio_cubierto_usd": round(valor_con_datos, 2),
        "valor_portfolio_total_usd": round(valor_total, 2),
        "pct_portfolio_cubierto": pct_cubierto,
        "tickers_con_datos": len(tickers_validos),
        "tickers_sin_datos": len(tickers) - len(tickers_validos),
        "metodo": "parametrico_varianza_covarianza",
        "nota": (
            f"VaR paramétrico, no histórico (ver limitación en el docstring del módulo). "
            f"Cubre el {pct_cubierto}% del valor del portfolio "
            f"(posiciones con ≥{window} días de historia de precios)."
        ),
    }


def compute_exposure(portfolio: dict, sector_by_ticker: dict = None) -> dict:
    """
    Exposición del portfolio por mercado (país) y, si se provee
    `sector_by_ticker`, también por sector -- como % del valor total.
    portfolio.json no guarda el sector directamente por posición, así que
    esa parte es opcional (analyzer.SECTOR_MAP es la fuente natural).
    """
    positions = portfolio.get("positions", [])
    valor_total = sum(p.get("valor_actual_usd", 0) or 0 for p in positions)
    if valor_total <= 0:
        return {"status": "portfolio_vacio"}

    por_mercado: dict = {}
    por_sector: dict = {}
    for p in positions:
        v = p.get("valor_actual_usd", 0) or 0
        mkt = p.get("mercado", "DESCONOCIDO")
        por_mercado[mkt] = por_mercado.get(mkt, 0) + v

        if sector_by_ticker:
            sec = sector_by_ticker.get(p["ticker"], "DESCONOCIDO")
            por_sector[sec] = por_sector.get(sec, 0) + v

    resultado = {
        "status": "ok",
        "valor_total_usd": round(valor_total, 2),
        "por_mercado": {
            k: {"valor_usd": round(v, 2), "pct": round(v / valor_total * 100, 1)}
            for k, v in sorted(por_mercado.items(), key=lambda kv: -kv[1])
        },
    }
    if sector_by_ticker:
        resultado["por_sector"] = {
            k: {"valor_usd": round(v, 2), "pct": round(v / valor_total * 100, 1)}
            for k, v in sorted(por_sector.items(), key=lambda kv: -kv[1])
        }
    return resultado


def compute_portfolio_risk(portfolio: dict, price_data: dict, ticker_cols: dict,
                             sector_by_ticker: dict = None, confidence: float = 0.95,
                             horizon_days: int = 1, window: int = COV_WINDOW) -> dict:
    """Punto de entrada único: corre las 3 piezas (correlaciones, VaR
    paramétrico, exposición) y las junta en un solo dict, pensado para
    persistir en un archivo propio o exponer en el dashboard."""
    return {
        "correlaciones":   compute_correlation_matrix(portfolio, price_data, ticker_cols, window),
        "var_parametrico": compute_parametric_var(portfolio, price_data, ticker_cols, confidence, horizon_days, window),
        "exposicion":      compute_exposure(portfolio, sector_by_ticker),
    }
