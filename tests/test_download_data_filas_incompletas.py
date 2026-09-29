"""FIX 29/09/2026: la última fila con cobertura parcial no debe quedar en el CSV."""
import numpy as np
import pandas as pd
import pytest

import scripts.download_data as dd


def _serie(nombre, fechas, valor=100.0):
    return pd.Series(valor, index=pd.DatetimeIndex(fechas), name=nombre), \
        pd.Series(valor + 1, index=pd.DatetimeIndex(fechas), name=nombre), \
        pd.Series(valor - 1, index=pd.DatetimeIndex(fechas), name=nombre)


@pytest.fixture
def _yahoo_parcial(monkeypatch):
    """Índice con barra del 28/09, acciones solo hasta el 25/09 (caso real)."""
    completas = pd.bdate_range("2026-09-01", "2026-09-25")
    con_hoy = pd.bdate_range("2026-09-01", "2026-09-28")

    def fake(ticker, start, end, market):
        return _serie(ticker, con_hoy if ticker.startswith("^") else completas)
    monkeypatch.setattr(dd, "download_single", fake)
    monkeypatch.setattr(dd.time, "sleep", lambda *_: None)


def test_descarta_ultima_fila_con_solo_el_indice(_yahoo_parcial):
    tickers = {f"T{i}": f"Empresa {i}" for i in range(39)}
    df, hi, lo = dd.download_market(tickers, "^GSPC", "SP500")
    assert str(df.index[-1].date()) == "2026-09-25"
    assert df.iloc[-1].notna().all()
    assert hi.index[-1] == df.index[-1] and lo.index[-1] == df.index[-1]


def test_no_toca_fila_completa(monkeypatch):
    fechas = pd.bdate_range("2026-09-01", "2026-09-28")
    monkeypatch.setattr(dd, "download_single", lambda t, s, e, m: _serie(t, fechas))
    monkeypatch.setattr(dd.time, "sleep", lambda *_: None)
    df, _, _ = dd.download_market({"A": "A", "B": "B"}, "^X", "TEST")
    assert str(df.index[-1].date()) == "2026-09-28"


def test_umbral_mitad_y_nunca_vacio():
    idx = pd.bdate_range("2026-09-24", periods=3)
    df = pd.DataFrame({"a": [1, 1, 1], "b": [1, 1, np.nan], "c": [1, 1, np.nan], "d": [1, 1, 1]}, index=idx)
    assert len(dd._recortar_filas_incompletas(df, "T")) == 3        # 2/4 = 50%: se queda
    df.iloc[-1, 0] = np.nan
    assert len(dd._recortar_filas_incompletas(df, "T")) == 2        # 1/4: se descarta
    solo = pd.DataFrame({"a": [np.nan]}, index=idx[:1])
    assert len(dd._recortar_filas_incompletas(solo, "T")) == 1      # nunca vacío


def test_cedear_tambien(monkeypatch):
    completas = pd.bdate_range("2026-09-01", "2026-09-25")
    con_hoy = pd.bdate_range("2026-09-01", "2026-09-28")
    monkeypatch.setattr(dd, "download_single",
                        lambda t, s, e, m: _serie(t, con_hoy if t == "T0" else completas))
    monkeypatch.setattr(dd.time, "sleep", lambda *_: None)
    df = dd.download_market_no_index({f"T{i}": f"E{i}" for i in range(10)}, "CEDEAR")
    assert str(df.index[-1].date()) == "2026-09-25"
