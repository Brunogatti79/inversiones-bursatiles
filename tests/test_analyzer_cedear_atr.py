"""
tests/test_analyzer_cedear_atr.py

Tests para el fix de CEDEAR ATR real (auditoría externa v20, prioridad #2,
10/08/2026): data/cedear_cierres.csv se pushea desde el 29/06/2026 pero
ningún módulo lo leía -- el ATR de todo el universo SP500/CEDEARs se
calculaba sobre el precio del subyacente NYSE, correcto para el negocio
pero en la escala/mercado equivocado para dimensionar stops.

Cubre:
  - Sin cedear_close_extra -> comportamiento IDÉNTICO a antes del fix
    (retrocompatibilidad, mismo criterio que test_analyzer_ohlc_resolve.py)
  - Con cedear_close_extra y el ticker con cobertura -> atr_fuente_precio
    cambia a "cedear_proxy" y atr_metodo a "cedear_pct_proxy"
  - MERVAL/BOVESPA nunca tocan cedear_close_extra aunque se les pase
  - Ticker SP500 sin cobertura en cedear_close_extra -> cae al camino NYSE
    de siempre, no crashea
  - Cobertura insuficiente (<15 puntos) -> cae al camino NYSE, no crashea
  - BUG real encontrado durante la validación de este mismo fix: el ATR
    de la serie CEDEAR queda en la escala de precio del CEDEAR (ej. ~$16
    para AAPL por el ratio de conversión), no en la escala NYSE (~$306)
    que usa precio_actual -- sumar/restar directamente daba stops pegados
    al precio. El fix corrige esto convirtiendo a ATR porcentual antes de
    aplicarlo sobre precio_actual. Este archivo verifica que el rango
    resultante (atr_target - atr_stop) sea del mismo orden de magnitud
    relativo que el camino NYSE, no un porcentaje ínfimo del precio.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.analyzer import analyze_market


def _mock_sp500_df(n=300, seed=7, ticker_name="Apple Test"):
    np.random.seed(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    close = 300 * np.cumprod(1 + np.random.normal(0.0005, 0.015, n))
    indice = 6000 * np.cumprod(1 + np.random.normal(0.0003, 0.01, n))
    df = pd.DataFrame({ticker_name: close, "INDICE S&P 500": indice}, index=dates)
    df.index.name = "Fecha"
    return df, dates, close


TICKER_NAMES = {"AAPL": "Apple Test"}


def _mock_cedear_close_extra(n=43, seed=11, precio_base=16.0):
    """Simula cedear_cierres.csv: pocas filas (semanas recientes), precio
    en una escala completamente distinta al subyacente NYSE (ratio de
    conversión de CEDEARs por acción real)."""
    np.random.seed(seed)
    dates = pd.date_range("2026-06-29", periods=n, freq="D")
    close = precio_base * np.cumprod(1 + np.random.normal(0.0, 0.02, n))
    return pd.DataFrame({"AAPL": close}, index=dates)


class TestSinCedearCloseExtraComportamientoIdentico:

    def test_sin_cedear_close_extra_atr_fuente_es_nyse(self):
        df, dates, close = _mock_sp500_df()
        signals = analyze_market(df, "SP500", TICKER_NAMES)
        assert signals[0]["atr_fuente_precio"] in ("nyse_proxy", "nyse_ohlc")

    def test_cedear_close_extra_none_no_rompe_nada(self):
        df, dates, close = _mock_sp500_df()
        signals = analyze_market(df, "SP500", TICKER_NAMES, cedear_close_extra=None)
        assert len(signals) == 1
        assert signals[0]["atr_stop"] > 0


class TestConCedearCloseExtra:

    def test_ticker_con_cobertura_usa_fuente_cedear(self):
        df, dates, close = _mock_sp500_df()
        cedear_extra = _mock_cedear_close_extra()
        signals = analyze_market(df, "SP500", TICKER_NAMES, cedear_close_extra=cedear_extra)
        assert signals[0]["atr_fuente_precio"] == "cedear_proxy"
        assert signals[0]["atr_metodo"] == "cedear_pct_proxy"

    def test_ticker_sin_cobertura_cedear_cae_a_nyse_no_crashea(self):
        """Ticker SP500 agregado después de que cedear_cierres.csv empezó
        a acumularse (ej. Netflix, Mastercard) -- debe caer al camino NYSE
        de siempre para ESE ticker, sin afectar a los demás."""
        df, dates, close = _mock_sp500_df()
        cedear_extra = _mock_cedear_close_extra()  # solo tiene columna AAPL
        ticker_names_otro = {"NFLX": "Apple Test"}  # reusa la misma serie de precio, solo cambia el ticker
        signals = analyze_market(df, "SP500", ticker_names_otro, cedear_close_extra=cedear_extra)
        assert signals[0]["atr_fuente_precio"] in ("nyse_proxy", "nyse_ohlc")
        assert signals[0]["atr_stop"] > 0

    def test_cobertura_insuficiente_cae_a_nyse_no_crashea(self):
        """Menos de 15 puntos (mínimo que pide _atr() para period=14) ->
        no debe intentar usar la serie CEDEAR, cae al camino NYSE."""
        df, dates, close = _mock_sp500_df()
        cedear_extra = _mock_cedear_close_extra(n=10)  # por debajo del umbral
        signals = analyze_market(df, "SP500", TICKER_NAMES, cedear_close_extra=cedear_extra)
        assert signals[0]["atr_fuente_precio"] in ("nyse_proxy", "nyse_ohlc")

    def test_merval_nunca_usa_cedear_close_extra_aunque_se_pase(self):
        """cedear_close_extra solo aplica a SP500 -- MERVAL/BOVESPA no
        tienen CEDEARs de sí mismos, pasar el parámetro no debe afectarlos."""
        np.random.seed(3)
        dates = pd.date_range("2025-01-01", periods=300, freq="B")
        close = 1000 * np.cumprod(1 + np.random.normal(0.0005, 0.02, 300))
        indice = 3000000 * np.cumprod(1 + np.random.normal(0.0003, 0.01, 300))
        df_merval = pd.DataFrame({"Grupo Financiero Galicia": close, "INDICE MERVAL": indice}, index=dates)
        df_merval.index.name = "Fecha"
        cedear_extra = _mock_cedear_close_extra()
        signals = analyze_market(
            df_merval, "MERVAL", {"GGAL.BA": "Grupo Financiero Galicia"},
            cedear_close_extra=cedear_extra,
        )
        assert signals[0]["atr_fuente_precio"] in ("nyse_proxy", "nyse_ohlc")

    def test_rango_stop_target_no_queda_pegado_al_precio(self):
        """Regresión del bug real encontrado en la validación de este fix:
        antes de convertir a ATR porcentual, el rango (target - stop)
        quedaba en words de centavos sobre un precio de cientos de
        dólares porque se mezclaban escalas (ATR en pesos-CEDEAR aplicado
        sobre precio en USD-NYSE). El rango relativo debe ser un
        porcentaje razonable del precio (no < 0.5%, no > 100%)."""
        df, dates, close = _mock_sp500_df()
        cedear_extra = _mock_cedear_close_extra()
        signals = analyze_market(df, "SP500", TICKER_NAMES, cedear_close_extra=cedear_extra)
        sig = signals[0]
        precio = sig["precio_actual"]
        rango_pct = (sig["atr_target"] - sig["atr_stop"]) / precio * 100
        assert 0.5 < rango_pct < 100, (
            f"rango stop-target sospechoso: {rango_pct:.2f}% del precio "
            f"(stop={sig['atr_stop']}, target={sig['atr_target']}, precio={precio})"
        )
