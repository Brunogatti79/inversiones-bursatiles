"""
tests/test_download_data_ohlc.py

Tests para scripts/download_data.py (mejora 27/07/2026, roadmap externo P6):
download_single/download_market ahora devuelven también High/Low reales,
sin llamadas extra a Yahoo Finance (vienen del mismo hist() que ya se pedía
para Close).

Sin acceso a red real desde este entorno -- todo mockeado, nunca se llama
a Yahoo Finance de verdad.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import numpy as np
import pandas as pd
import pytest

import download_data as dd


def _mock_hist(n=30, seed=0):
    """DataFrame en el mismo formato que devuelve yf.Ticker(...).history():
    columnas Open/High/Low/Close/Volume, index de fechas."""
    np.random.seed(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    close = 100 * np.cumprod(1 + np.random.normal(0.0005, 0.01, n))
    high = close * 1.01
    low = close * 0.99
    return pd.DataFrame({
        "Open": close, "High": high, "Low": low, "Close": close,
        "Volume": np.random.randint(1000, 5000, n),
    }, index=dates)


class _FakeTicker:
    def __init__(self, hist_df):
        self._hist_df = hist_df

    def history(self, start=None, end=None, auto_adjust=None):
        return self._hist_df


def test_download_single_devuelve_close_high_low(monkeypatch):
    hist = _mock_hist()
    monkeypatch.setattr(dd.yf, "Ticker", lambda ticker: _FakeTicker(hist))

    serie, serie_high, serie_low = dd.download_single("FAKE", "2025-01-01", "2025-12-31", "MERVAL")

    assert serie is not None and serie_high is not None and serie_low is not None
    assert len(serie) == len(serie_high) == len(serie_low) == len(hist)
    # High siempre >= Close >= Low para cada fecha (por construcción del mock)
    assert (serie_high.values >= serie.values).all()
    assert (serie_low.values <= serie.values).all()


def test_download_single_ticker_sin_datos_devuelve_none_none_none(monkeypatch):
    hist_vacio = pd.DataFrame()
    monkeypatch.setattr(dd.yf, "Ticker", lambda ticker: _FakeTicker(hist_vacio))
    monkeypatch.setattr(dd.time, "sleep", lambda *a, **kw: None)  # no esperar en el test

    serie, serie_high, serie_low = dd.download_single("FAKE", "2025-01-01", "2025-12-31", "MERVAL")
    assert serie is None and serie_high is None and serie_low is None


def test_download_market_arma_3_dataframes_alineados(monkeypatch):
    hist = _mock_hist(n=50, seed=1)
    monkeypatch.setattr(dd.yf, "Ticker", lambda ticker: _FakeTicker(hist))
    monkeypatch.setattr(dd.time, "sleep", lambda *a, **kw: None)

    tickers = {"AAA.BA": "Empresa A", "BBB.BA": "Empresa B"}
    df, df_high, df_low = dd.download_market(tickers, "^MERV", "MERVAL")

    assert df is not None and df_high is not None and df_low is not None
    assert set(df.columns) == set(df_high.columns) == set(df_low.columns)
    assert list(df.index) == list(df_high.index) == list(df_low.index)
    # High/Low deben ser consistentes con el Close para cada columna
    for col in df.columns:
        assert (df_high[col].values >= df[col].values).all()
        assert (df_low[col].values <= df[col].values).all()


def test_download_market_ticker_parcialmente_fallido_no_rompe_high_low(monkeypatch):
    """Si un ticker falla, download_market sigue funcionando con los que sí
    trajeron datos -- Close/High/Low deben quedar con las MISMAS columnas
    entre sí (nunca Close con más columnas que High/Low, lo que rompería
    _resolve_high_low en analyzer.py silenciosamente)."""
    hist_ok = _mock_hist(n=50, seed=2)
    hist_vacio = pd.DataFrame()

    def fake_ticker(ticker):
        if ticker == "BBB.BA":
            return _FakeTicker(hist_vacio)
        return _FakeTicker(hist_ok)

    monkeypatch.setattr(dd.yf, "Ticker", fake_ticker)
    monkeypatch.setattr(dd.time, "sleep", lambda *a, **kw: None)

    tickers = {"AAA.BA": "Empresa A", "BBB.BA": "Empresa B", "CCC.BA": "Empresa C"}
    df, df_high, df_low = dd.download_market(tickers, "^MERV", "MERVAL")

    assert "Empresa B" not in df.columns
    assert set(df.columns) == set(df_high.columns) == set(df_low.columns)


def test_save_csv_usa_sufijo_correcto(tmp_path, monkeypatch):
    monkeypatch.setattr(dd, "DATA_DIR", str(tmp_path))
    df = pd.DataFrame({"A": [1.0, 2.0]}, index=pd.date_range("2025-01-01", periods=2))

    path_cierres = dd.save_csv(df, "MERVAL")  # default: sin cambios de comportamiento
    path_high    = dd.save_csv(df, "MERVAL", suffix="high")
    path_low     = dd.save_csv(df, "MERVAL", suffix="low")

    assert path_cierres.endswith("merval_cierres.csv")
    assert path_high.endswith("merval_high.csv")
    assert path_low.endswith("merval_low.csv")
    assert os.path.exists(path_cierres) and os.path.exists(path_high) and os.path.exists(path_low)


def test_download_market_no_index_no_rompe_con_el_nuevo_return_de_3_tuplas(monkeypatch):
    """
    Regresión real encontrada en producción el 27/07/2026: download_single()
    pasó a devolver (serie, serie_high, serie_low) para MERVAL/BOVESPA/SP500,
    pero download_market_no_index() (usado para CEDEARs) seguía haciendo
    'serie = download_single(...)' sin desempaquetar -- serie terminaba
    siendo la tupla completa, y 'serie.name = name' reventaba con
    "'tuple' object has no attribute 'name'". CEDEAR quedó marcado
    ok=False en download_status.json de una corrida real. Este test evita
    que se repita si algún día se vuelve a tocar download_single().
    """
    hist = _mock_hist(n=30, seed=4)
    monkeypatch.setattr(dd.yf, "Ticker", lambda ticker: _FakeTicker(hist))
    monkeypatch.setattr(dd.time, "sleep", lambda *a, **kw: None)

    tickers = {"MELI": "MercadoLibre", "GLOB": "Globant"}
    df = dd.download_market_no_index(tickers, "CEDEAR")

    assert df is not None
    assert set(df.columns) == {"MercadoLibre", "Globant"}
    assert len(df) == len(hist)
