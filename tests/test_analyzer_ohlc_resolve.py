"""
tests/test_analyzer_ohlc_resolve.py

Tests para _resolve_high_low() y su integración en analyze_market()
(mejora 27/07/2026, roadmap externo P6).

Cubre:
  - Con ohlc_extra real -> ATR/ADX usan metodo="ohlc" (no el proxy)
  - Sin ohlc_extra -> comportamiento IDÉNTICO a antes de este fix
    (metodo="close_proxy")
  - ohlc_extra con el ticker ausente (ej. agregado recién, sin historia
    todavía) -> cae a close_proxy para ESE ticker sin romper los demás
  - ohlc_extra con muy pocas observaciones reales (<20) -> cae a
    close_proxy en vez de usar una serie casi vacía
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.analyzer import analyze_market, _resolve_high_low


def _mock_df(n=300, seed=5):
    np.random.seed(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    close = 100 * np.cumprod(1 + np.random.normal(0.0005, 0.015, n))
    indice = 1000 * np.cumprod(1 + np.random.normal(0.0003, 0.01, n))
    df = pd.DataFrame({"Empresa Test": close, "INDICE MERVAL": indice}, index=dates)
    df.index.name = "Fecha"
    return df, dates, close


TICKER_NAMES = {"TEST.BA": "Empresa Test"}


def _mock_ohlc_extra(dates, close, factor_high=1.02, factor_low=0.98):
    high = pd.Series(close * factor_high, index=dates, name="Empresa Test")
    low = pd.Series(close * factor_low, index=dates, name="Empresa Test")
    return {
        "high": pd.DataFrame({"Empresa Test": high}),
        "low":  pd.DataFrame({"Empresa Test": low}),
    }


def test_analyze_market_sin_ohlc_extra_usa_close_proxy():
    df, dates, close = _mock_df()
    signals = analyze_market(df, "MERVAL", TICKER_NAMES)
    assert signals[0]["atr_metodo"] == "close_proxy"


def test_analyze_market_con_ohlc_extra_real_usa_ohlc():
    df, dates, close = _mock_df()
    ohlc_extra = _mock_ohlc_extra(dates, close)
    signals = analyze_market(df, "MERVAL", TICKER_NAMES, ohlc_extra=ohlc_extra)
    assert signals[0]["atr_metodo"] == "ohlc"


def test_analyze_market_ticker_sin_ohlc_cae_a_proxy_sin_romper():
    """ohlc_extra existe (otro ticker sí tiene datos) pero NO tiene la
    columna de este ticker en particular -- debe degradar solo para este,
    no levantar excepción."""
    df, dates, close = _mock_df()
    ohlc_extra = {
        "high": pd.DataFrame({"Otra Empresa": close * 1.02}, index=dates),
        "low":  pd.DataFrame({"Otra Empresa": close * 0.98}, index=dates),
    }
    signals = analyze_market(df, "MERVAL", TICKER_NAMES, ohlc_extra=ohlc_extra)
    assert signals[0]["atr_metodo"] == "close_proxy"


def test_resolve_high_low_con_pocas_observaciones_cae_a_proxy():
    """Menos de 20 observaciones reales tras reindexar -> no usar esa serie
    (mejor un proxy confiable que un OHLC casi vacío)."""
    dates_completas = pd.date_range("2025-01-01", periods=100, freq="B")
    dates_pocas = dates_completas[:10]  # solo 10 fechas con dato real
    df_12m = pd.DataFrame({"Empresa Test": np.arange(100.0)}, index=dates_completas)

    high_df = pd.DataFrame({"Empresa Test": np.arange(10.0) + 1}, index=dates_pocas)
    low_df  = pd.DataFrame({"Empresa Test": np.arange(10.0) - 1}, index=dates_pocas)
    ohlc_extra = {"high": high_df, "low": low_df}

    high_s, low_s = _resolve_high_low("Empresa Test", df_12m, ohlc_extra)
    # Con <20 observaciones reales tras el reindex, debe caer al segundo
    # intento (col.replace, que tampoco resuelve acá) -> None, None
    assert high_s is None
    assert low_s is None


def test_resolve_high_low_sin_ohlc_extra_devuelve_none():
    df_12m = pd.DataFrame({"Empresa Test": [1.0, 2.0, 3.0]})
    high_s, low_s = _resolve_high_low("Empresa Test", df_12m, None)
    assert high_s is None and low_s is None
