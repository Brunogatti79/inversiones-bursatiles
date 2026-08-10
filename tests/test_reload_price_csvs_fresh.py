"""
tests/test_reload_price_csvs_fresh.py

Tests para downloader.reload_price_csvs_fresh() (auditoría externa v20,
prioridad #1, 10/08/2026).

CONTEXTO DEL INCIDENTE REAL que motivó este fix: el backtester corría
siempre sobre los DataFrames de precio cargados en la Fase 1/8 del
pipeline (al arrancar la corrida) y nunca se volvían a leer. Como
download_data.yml (GitHub Actions) pushea CSVs actualizados de forma
independiente, sin ninguna sincronización con el pipeline de Railway, el
backtest podía terminar evaluando retornos con precios de horas antes.
Confirmado en producción el 10/08/2026: recalcular el backtest con los
CSVs más frescos del mismo commit dio ranking_top_vs_rest.samples=1876
vs 1742 persistido, y el EV del top 20% cambió de signo completo
(-3.52% -> +3.48%).

Cubre:
  - pull_file() exitoso para los 3 mercados -> devuelve dict con los 3
    DataFrames parseados
  - pull_file() falla para cualquiera de los 3 -> degrada a None (todo
    o nada -- no se arma un dict parcial con solo 1 o 2 mercados
    refrescados, eso generaría inconsistencia entre mercados)
  - CSV traído pero vacío/corrupto -> degrada a None, no crashea
  - Sin GH_TOKEN (o sin red) -> pull_file() falla de entrada -> None,
    sin lanzar excepción
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from src.downloader import reload_price_csvs_fresh


def _write_price_csv(path, tickers_values, n=30):
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    df = pd.DataFrame({name: [v] * n for name, v in tickers_values.items()}, index=dates)
    df.index.name = "Fecha"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")


class TestReloadPriceCsvsFresh:

    def test_pull_exitoso_para_los_3_mercados_devuelve_dataframes(self, tmp_path, monkeypatch):
        for market in ("merval", "bovespa", "sp500"):
            _write_price_csv(tmp_path / f"{market}_cierres.csv", {"TICKER_TEST": 100.0})

        # pull_file() simulado: el archivo "ya está" en tmp_path (como si
        # se acabara de traer fresco de GitHub) -- no pega a la red real.
        monkeypatch.setattr(
            "src.github_persistence.pull_file",
            lambda path: os.path.exists(path),
        )
        result = reload_price_csvs_fresh(data_dir=str(tmp_path))
        assert result is not None
        assert set(result.keys()) == {"merval", "bovespa", "sp500"}
        for df in result.values():
            assert not df.empty
            assert "TICKER_TEST" in df.columns

    def test_pull_falla_para_un_mercado_degrada_a_none_para_todos(self, tmp_path, monkeypatch):
        """Todo o nada: si falla aunque sea 1 de los 3, no se arma un dict
        parcial -- generaría inconsistencia (ej. MERVAL fresco de hace 1
        minuto, SP500 de hace 3 horas) peor que no refrescar nada."""
        for market in ("merval", "bovespa", "sp500"):
            _write_price_csv(tmp_path / f"{market}_cierres.csv", {"TICKER_TEST": 100.0})

        def _pull_fail_sp500(path):
            return "sp500" not in path  # falla específicamente para sp500

        monkeypatch.setattr("src.github_persistence.pull_file", _pull_fail_sp500)
        result = reload_price_csvs_fresh(data_dir=str(tmp_path))
        assert result is None

    def test_sin_gh_token_o_red_degrada_a_none_sin_crashear(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.github_persistence.pull_file", lambda path: False)
        result = reload_price_csvs_fresh(data_dir=str(tmp_path))
        assert result is None

    def test_csv_traido_pero_vacio_degrada_a_none(self, tmp_path, monkeypatch):
        # merval y bovespa con datos válidos, sp500 vacío (0 filas)
        _write_price_csv(tmp_path / "merval_cierres.csv", {"TICKER_TEST": 100.0})
        _write_price_csv(tmp_path / "bovespa_cierres.csv", {"TICKER_TEST": 100.0})
        (tmp_path / "sp500_cierres.csv").write_text("Fecha\n", encoding="utf-8-sig")

        monkeypatch.setattr(
            "src.github_persistence.pull_file",
            lambda path: os.path.exists(path),
        )
        result = reload_price_csvs_fresh(data_dir=str(tmp_path))
        assert result is None

    def test_pull_file_lanza_excepcion_no_propaga(self, tmp_path, monkeypatch):
        """Regla del proyecto: nada de este refresh debe poder tumbar el
        pipeline -- backtester ya corre en su propio try/except, pero
        esta función debe degradar sola, no depender de eso."""
        def _pull_raises(path):
            raise ConnectionError("simulado")

        monkeypatch.setattr("src.github_persistence.pull_file", _pull_raises)
        result = reload_price_csvs_fresh(data_dir=str(tmp_path))
        assert result is None
