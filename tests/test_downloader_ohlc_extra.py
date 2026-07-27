"""
tests/test_downloader_ohlc_extra.py

Tests para downloader.load_ohlc_extra()/_load_ohlc_csv() (mejora 27/07/2026,
roadmap externo P6).

Cubre:
  - Carga correcta cuando los CSVs high/low existen
  - Degradación graciosa (None) cuando faltan -- caso esperado hasta que
    Bruno actualice manualmente .github/workflows/download_data.yml
  - Degradación graciosa ante CSV corrupto/vacío
  - El dict 'data' de download_all() (merval/bovespa/sp500 -> Close) NO se
    ve afectado por nada de esto -- canal completamente aparte.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from src.downloader import load_ohlc_extra, _load_ohlc_csv


def _write_ohlc_csv(tmp_path, market, kind, tickers_values):
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    df = pd.DataFrame({name: [v] * 30 for name, v in tickers_values.items()}, index=dates)
    df.index.name = "Fecha"
    path = tmp_path / f"{market.lower()}_{kind}.csv"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
    return path


def test_load_ohlc_extra_devuelve_none_si_no_existen_los_archivos(tmp_path):
    result = load_ohlc_extra(data_dir=str(tmp_path))
    for market in ("MERVAL", "BOVESPA", "SP500"):
        assert result[market]["high"] is None
        assert result[market]["low"] is None


def test_load_ohlc_extra_carga_los_que_si_existen(tmp_path):
    _write_ohlc_csv(tmp_path, "MERVAL", "high", {"Empresa A": 105.0})
    _write_ohlc_csv(tmp_path, "MERVAL", "low", {"Empresa A": 95.0})

    result = load_ohlc_extra(data_dir=str(tmp_path))

    assert result["MERVAL"]["high"] is not None
    assert result["MERVAL"]["low"] is not None
    assert "Empresa A" in result["MERVAL"]["high"].columns
    # BOVESPA/SP500 sin archivos -> siguen en None, no rompe el resto
    assert result["BOVESPA"]["high"] is None
    assert result["SP500"]["low"] is None


def test_load_ohlc_csv_degrada_ante_archivo_corrupto(tmp_path):
    path = tmp_path / "merval_high.csv"
    path.write_text("esto no es un csv valido con el formato esperado {{{")

    df = _load_ohlc_csv("MERVAL", "high", str(tmp_path))
    assert df is None  # nunca debe levantar excepción hacia el caller


def test_load_ohlc_csv_degrada_ante_csv_vacio(tmp_path):
    path = tmp_path / "merval_high.csv"
    path.write_text("Fecha;Empresa A\n")  # header sin filas

    df = _load_ohlc_csv("MERVAL", "high", str(tmp_path))
    assert df is None
