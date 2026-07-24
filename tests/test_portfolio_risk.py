"""
tests/test_portfolio_risk.py

Cobertura para src/portfolio_risk.py (Portfolio Risk Engine, roadmap
"Institucional PRO", 24/07/2026): correlaciones, VaR paramétrico, y
exposición por mercado/sector del portfolio ACTUAL.

Casos de referencia validados con resultados matemáticamente conocidos de
antemano (no solo "no crashea"): correlación perfecta entre dos series que
se mueven idénticas, y que diversificar entre activos independientes
reduce el VaR en USD frente a concentrar el mismo capital en uno solo --
son las dos propiedades básicas que cualquier motor de riesgo de portfolio
tiene que cumplir para ser confiable.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.portfolio_risk import (
    compute_correlation_matrix,
    compute_parametric_var,
    compute_exposure,
    compute_portfolio_risk,
    _build_returns_matrix,
)


def _price_df(**series):
    """Arma un DataFrame de precios a partir de series numpy/list por columna."""
    return pd.DataFrame(series)


@pytest.fixture
def series_correlacionadas():
    """A y B se mueven idénticas (correlación ~1.0), C es independiente."""
    np.random.seed(1)
    n = 100
    base = np.cumsum(np.random.normal(0, 1, n))
    price_a = 100 + base
    price_b = 200 + base * 2
    price_c = 50 + np.cumsum(np.random.normal(0, 1, n))
    df = _price_df(TICKER_A=price_a, TICKER_B=price_b, TICKER_C=price_c)
    price_data = {"MERVAL": df}
    ticker_cols = {"TICKER_A": "TICKER_A", "TICKER_B": "TICKER_B", "TICKER_C": "TICKER_C"}
    return price_data, ticker_cols


class TestBuildReturnsMatrix:

    def test_excluye_tickers_con_poca_historia(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        validos, R = _build_returns_matrix(["TICKER_A", "TICKER_NO_EXISTE"], price_data, ticker_cols, window=60)
        assert validos == ["TICKER_A"]
        assert R.shape[0] == 1

    def test_sin_tickers_pedidos_devuelve_vacio(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        validos, R = _build_returns_matrix([], price_data, ticker_cols)
        assert validos == []
        assert R.size == 0


class TestComputeCorrelationMatrix:

    def test_portfolio_vacio(self):
        r = compute_correlation_matrix({"positions": []}, {}, {})
        assert r["status"] == "portfolio_vacio"

    def test_una_sola_posicion_es_insuficiente(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [{"ticker": "TICKER_A", "valor_actual_usd": 100}]}
        r = compute_correlation_matrix(portfolio, price_data, ticker_cols)
        assert r["status"] == "insuficiente_historia"

    def test_correlacion_perfecta_se_detecta(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [
            {"ticker": "TICKER_A", "valor_actual_usd": 1000},
            {"ticker": "TICKER_B", "valor_actual_usd": 1000},
            {"ticker": "TICKER_C", "valor_actual_usd": 500},
        ]}
        r = compute_correlation_matrix(portfolio, price_data, ticker_cols)
        assert r["status"] == "ok"
        assert r["matriz"]["TICKER_A"]["TICKER_B"] > 0.99
        # El par más correlacionado debe ser A-B, no A-C ni B-C
        top_par = set(r["pares_mas_correlacionados"][0]["par"])
        assert top_par == {"TICKER_A", "TICKER_B"}

    def test_tickers_sin_datos_se_cuentan_pero_no_rompen(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [
            {"ticker": "TICKER_A", "valor_actual_usd": 1000},
            {"ticker": "TICKER_B", "valor_actual_usd": 1000},
            {"ticker": "NO_EXISTE", "valor_actual_usd": 500},
        ]}
        r = compute_correlation_matrix(portfolio, price_data, ticker_cols)
        assert r["status"] == "ok"
        assert r["tickers_con_datos"] == 2
        assert r["tickers_sin_datos"] == 1


class TestComputeParametricVar:

    def test_portfolio_vacio(self):
        r = compute_parametric_var({"positions": []}, {}, {})
        assert r["status"] == "portfolio_vacio"

    def test_confidence_no_soportada(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [{"ticker": "TICKER_A", "valor_actual_usd": 1000}]}
        r = compute_parametric_var(portfolio, price_data, ticker_cols, confidence=0.80)
        assert r["status"] == "confidence_no_soportada"

    def test_un_solo_ticker_es_valido(self, series_correlacionadas):
        """Matemáticamente válido: VaR de 1 activo es solo su propia
        volatilidad -- no debería exigirse un mínimo de 2 posiciones."""
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [{"ticker": "TICKER_A", "valor_actual_usd": 2000}]}
        r = compute_parametric_var(portfolio, price_data, ticker_cols)
        assert r["status"] == "ok"
        assert r["var_usd"] > 0

    def test_diversificar_reduce_var_en_usd(self, series_correlacionadas):
        """Propiedad matemática básica: mismo capital total, activos
        independientes -> diversificar reduce el VaR en USD frente a
        concentrar todo en un solo activo."""
        price_data, ticker_cols = series_correlacionadas
        concentrado = {"positions": [{"ticker": "TICKER_C", "valor_actual_usd": 2000}]}
        # TICKER_A y TICKER_C no están perfectamente correlacionados entre sí
        diversificado = {"positions": [
            {"ticker": "TICKER_A", "valor_actual_usd": 1000},
            {"ticker": "TICKER_C", "valor_actual_usd": 1000},
        ]}
        var_conc = compute_parametric_var(concentrado, price_data, ticker_cols)
        var_div = compute_parametric_var(diversificado, price_data, ticker_cols)
        assert var_div["var_usd"] < var_conc["var_usd"]

    def test_horizonte_mas_largo_da_var_mayor(self, series_correlacionadas):
        """sqrt(horizon_days) debe escalar el VaR hacia arriba con el horizonte."""
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [{"ticker": "TICKER_A", "valor_actual_usd": 1000}]}
        var_1d = compute_parametric_var(portfolio, price_data, ticker_cols, horizon_days=1)
        var_10d = compute_parametric_var(portfolio, price_data, ticker_cols, horizon_days=10)
        assert var_10d["var_usd"] > var_1d["var_usd"]
        # Relación exacta esperada: sqrt(10) ≈ 3.162
        assert abs(var_10d["var_usd"] / var_1d["var_usd"] - (10 ** 0.5)) < 0.01

    def test_nivel_de_confianza_mas_alto_da_var_mayor(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [{"ticker": "TICKER_A", "valor_actual_usd": 1000}]}
        var_90 = compute_parametric_var(portfolio, price_data, ticker_cols, confidence=0.90)
        var_99 = compute_parametric_var(portfolio, price_data, ticker_cols, confidence=0.99)
        assert var_99["var_usd"] > var_90["var_usd"]

    def test_posiciones_sin_datos_no_rompen_pero_se_reportan(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [
            {"ticker": "TICKER_A", "valor_actual_usd": 1000},
            {"ticker": "NO_EXISTE", "valor_actual_usd": 500},
        ]}
        r = compute_parametric_var(portfolio, price_data, ticker_cols)
        assert r["status"] == "ok"
        assert r["tickers_sin_datos"] == 1
        assert r["pct_portfolio_cubierto"] < 100.0


class TestComputeExposure:

    def test_portfolio_vacio(self):
        assert compute_exposure({"positions": []})["status"] == "portfolio_vacio"

    def test_exposicion_por_mercado(self):
        portfolio = {"positions": [
            {"ticker": "A", "mercado": "MERVAL", "valor_actual_usd": 600},
            {"ticker": "B", "mercado": "SP500", "valor_actual_usd": 400},
        ]}
        r = compute_exposure(portfolio)
        assert r["por_mercado"]["MERVAL"]["pct"] == 60.0
        assert r["por_mercado"]["SP500"]["pct"] == 40.0

    def test_exposicion_por_sector_opcional(self):
        portfolio = {"positions": [
            {"ticker": "A", "mercado": "MERVAL", "valor_actual_usd": 600},
            {"ticker": "B", "mercado": "MERVAL", "valor_actual_usd": 400},
        ]}
        sin_sector = compute_exposure(portfolio)
        assert "por_sector" not in sin_sector

        con_sector = compute_exposure(portfolio, sector_by_ticker={"A": "Financiero", "B": "Energía"})
        assert con_sector["por_sector"]["Financiero"]["pct"] == 60.0

    def test_ticker_sin_sector_mapeado_cae_en_desconocido(self):
        portfolio = {"positions": [{"ticker": "X", "mercado": "MERVAL", "valor_actual_usd": 100}]}
        r = compute_exposure(portfolio, sector_by_ticker={"OTRO_TICKER": "Financiero"})
        assert r["por_sector"]["DESCONOCIDO"]["pct"] == 100.0


class TestComputePortfolioRisk:

    def test_junta_las_3_piezas(self, series_correlacionadas):
        price_data, ticker_cols = series_correlacionadas
        portfolio = {"positions": [
            {"ticker": "TICKER_A", "mercado": "MERVAL", "valor_actual_usd": 1000},
            {"ticker": "TICKER_B", "mercado": "MERVAL", "valor_actual_usd": 1000},
        ]}
        r = compute_portfolio_risk(portfolio, price_data, ticker_cols)
        assert set(r.keys()) == {"correlaciones", "var_parametrico", "exposicion"}
        assert r["correlaciones"]["status"] == "ok"
        assert r["var_parametrico"]["status"] == "ok"
        assert r["exposicion"]["status"] == "ok"
