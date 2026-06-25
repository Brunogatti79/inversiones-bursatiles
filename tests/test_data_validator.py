"""
tests/test_data_validator.py

Primera suite de tests de src/data_validator.py (antes solo tenía 1 smoke
test incidental en test_pct_change_deprecation.py, que no ejercía la lógica
de validación real).

Encontrado al escribir estos tests (Prioridad 1, roadmap externo,
25/06/2026): con datos perfectamente sanos, nivel/nivel_global NUNCA
llegaba a "OK" -- se quedaba en "WARNING" para siempre, porque los mensajes
de confirmación ("✅ Dato fresco", "✅ Consistencia OK", "✅ Integridad OK")
vivían en la misma lista 'warnings' que las advertencias reales. Esto
afectaba directamente:
  - el banner del dashboard (siempre 🟡 "Advertencia de datos", nunca 🟢)
  - confidence_score.py: components["integridad_datos"] pegado en 50.0
    (WARNING) en vez de 100.0 (OK) -- ~5 puntos perdidos en el confidence
    score global TODOS LOS DÍAS, incluso con datos perfectos.
Corregido en este mismo fix: los ✅ ahora van a una lista 'info' separada.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from src.data_validator import (
    ultimo_dia_habil,
    validar_frescura,
    validar_consistencia,
    validar_integridad,
    validar_mercado,
    validar_todos,
)


def _healthy_df(market="SP500", n=30, index_col="INDICE", n_tickers=3, seed=0):
    """DataFrame sano: fresco, sin saltos, 100% integridad."""
    fecha = ultimo_dia_habil(market)
    dates = pd.date_range(end=pd.Timestamp(fecha), periods=n, freq="B")
    rng = np.random.default_rng(seed)
    prices = 100 * np.cumprod(1 + rng.normal(0.0003, 0.006, n))
    data = {index_col: prices}
    for i in range(n_tickers - 1):
        data[f"TICKER{i}"] = prices * (1 + i * 0.05)
    return pd.DataFrame(data, index=dates)


# ── Regresión central: nivel debe poder llegar a OK ──────────────────────

class TestNivelOkAlcanzable:

    def test_datos_sanos_dan_nivel_ok_no_warning(self):
        """El bug central: antes de este fix, esto SIEMPRE devolvía
        'WARNING' sin importar cuán sanos estuvieran los datos."""
        df = _healthy_df(n_tickers=3)
        r = validar_mercado(df, "SP500", "INDICE", n_tickers=3)
        assert r["nivel"] == "OK"
        assert r["ok"] is True
        assert r["warnings"] == []
        assert len(r["info"]) == 3  # frescura + consistencia + integridad, los 3 OK

    def test_validar_todos_con_3_mercados_sanos_da_ok_global(self):
        dfs = {m: _healthy_df(market=M, index_col=f"INDICE_{m}")
               for m, M in [("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500")]}
        index_cols = {m: f"INDICE_{m}" for m in dfs}
        n_tickers  = {m: 3 for m in dfs}

        result = validar_todos(dfs, index_cols, n_tickers)
        assert result["nivel_global"] == "OK"
        assert result["hay_warnings"] is False
        assert result["hay_errores"] is False

    def test_info_messages_no_cuentan_como_warning(self):
        """Sanity check directo del bug: los mensajes ✅ deben vivir en
        'info', nunca en 'warnings'."""
        df = _healthy_df()
        r = validar_mercado(df, "SP500", "INDICE", n_tickers=3)
        for msg in r["warnings"]:
            assert "✅" not in msg
        for msg in r["info"]:
            assert "✅" in msg


# ── validar_frescura ──────────────────────────────────────────────────────

class TestValidarFrescura:

    def test_dato_fresco_va_a_info(self):
        df = _healthy_df()
        r = validar_frescura(df, "SP500")
        assert r["ok"] is True
        assert r["warnings"] == []
        assert any("Dato fresco" in m for m in r["info"])

    def test_atraso_leve_es_warning_no_error(self):
        fecha = ultimo_dia_habil("SP500")
        dates = pd.date_range(end=pd.Timestamp(fecha - timedelta(days=2)), periods=10, freq="D")
        df = pd.DataFrame({"INDICE": np.linspace(100, 105, 10)}, index=dates)
        r = validar_frescura(df, "SP500")
        assert r["ok"] is True
        assert any("atraso" in m for m in r["warnings"])

    def test_atraso_severo_es_error(self):
        fecha = ultimo_dia_habil("SP500")
        dates = pd.date_range(end=pd.Timestamp(fecha - timedelta(days=10)), periods=10, freq="D")
        df = pd.DataFrame({"INDICE": np.linspace(100, 105, 10)}, index=dates)
        r = validar_frescura(df, "SP500")
        assert r["ok"] is False
        assert any("MUY desactualizado" in m for m in r["errors"])

    def test_dataframe_vacio_es_error(self):
        r = validar_frescura(pd.DataFrame(), "SP500")
        assert r["ok"] is False
        assert "vacío" in r["errors"][0]

    def test_dataframe_none_es_error(self):
        r = validar_frescura(None, "SP500")
        assert r["ok"] is False


# ── validar_consistencia ──────────────────────────────────────────────────

class TestValidarConsistencia:

    def test_serie_sana_va_a_info(self):
        df = _healthy_df()
        r = validar_consistencia(df, "SP500", "INDICE")
        assert r["ok"] is True
        assert any("Consistencia OK" in m for m in r["info"])

    def test_variacion_extrema_es_error(self):
        dates = pd.date_range("2026-01-01", periods=10, freq="D")
        prices = [100, 101, 102, 103, 104, 105, 200, 201, 202, 203]  # salto de +90%
        df = pd.DataFrame({"INDICE": prices}, index=dates)
        r = validar_consistencia(df, "SP500", "INDICE")
        assert r["ok"] is False
        assert any("extrema" in m for m in r["errors"])

    def test_variacion_identica_dos_dias_es_warning(self):
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        # Dos variaciones idénticas no triviales (+2.0% exacto, generado por
        # cómputo en vez de tipeado a mano para evitar errores de redondeo
        # que pct_change() después detecta como "no idéntico").
        prices = [100.0]
        for _ in range(7):
            prices.append(prices[-1] * 1.02)
        df = pd.DataFrame({"INDICE": prices}, index=dates)
        r = validar_consistencia(df, "SP500", "INDICE")
        assert any("idéntica" in m for m in r["warnings"])

    def test_columna_no_encontrada_da_warning_no_crash(self):
        df = pd.DataFrame({"OTRA": [1, 2, 3]})
        r = validar_consistencia(df, "SP500", "INDICE")
        assert "no encontrada" in r["warnings"][0]

    def test_serie_muy_corta_da_warning_no_crash(self):
        df = pd.DataFrame({"INDICE": [100, 101]})
        r = validar_consistencia(df, "SP500", "INDICE")
        assert "muy corta" in r["warnings"][0]


# ── validar_integridad ─────────────────────────────────────────────────────

class TestValidarIntegridad:

    def test_100pct_integridad_va_a_info(self):
        df = pd.DataFrame({"A": [1, 2], "B": [1, 2], "C": [1, 2]})
        r = validar_integridad(df, "SP500", n_tickers_esperados=3)
        assert r["ok"] is True
        assert any("Integridad OK" in m for m in r["info"])

    def test_integridad_parcial_es_warning(self):
        df = pd.DataFrame({"A": [1, 2], "B": [1, 2], "C": [1, np.nan], "D": [1, np.nan]})
        r = validar_integridad(df, "SP500", n_tickers_esperados=4)
        assert r["ok"] is True
        assert any("parcial" in m for m in r["warnings"])

    def test_integridad_baja_es_error(self):
        df = pd.DataFrame({"A": [1, 2], "B": [1, np.nan], "C": [1, np.nan], "D": [1, np.nan]})
        r = validar_integridad(df, "SP500", n_tickers_esperados=4)
        assert r["ok"] is False
        assert any("baja" in m for m in r["errors"])


# ── ultimo_dia_habil ────────────────────────────────────────────────────────

class TestUltimoDiaHabil:

    def test_nunca_devuelve_fin_de_semana(self):
        for market in ["MERVAL", "BOVESPA", "SP500"]:
            d = ultimo_dia_habil(market, ref_date=date(2026, 6, 22))  # un lunes
            assert d.weekday() < 5

    def test_nunca_devuelve_feriado_conocido(self):
        # 9 de julio (feriado ARG) cae lunes en 2026 según FERIADOS_ARG
        d = ultimo_dia_habil("MERVAL", ref_date=date(2026, 7, 10))
        assert d != date(2026, 7, 9)
