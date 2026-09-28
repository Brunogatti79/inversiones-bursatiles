"""Tests de scripts/backfill_precios_10y.py con yfinance simulado.
Nunca pegan a Yahoo ni escriben en data/ real."""
import json
import os
import zlib

import numpy as np
import pandas as pd
import pytest

from scripts import backfill_precios_10y as bf


class _FakeTicker:
    FALLAN = {"MIRG.BA"}          # simula un ticker sin datos
    N_DIAS = 400

    def __init__(self, ticker):
        self.ticker = ticker

    def history(self, period="10y", interval="1d", auto_adjust=True):
        if self.ticker in self.FALLAN:
            return pd.DataFrame()
        rng = np.random.default_rng(zlib.crc32(self.ticker.encode()))
        idx = pd.bdate_range("2024-01-01", periods=self.N_DIAS, tz="America/New_York")
        base = {"GGAL": 50.0, "GGAL.BA": 5000.0, "YPF": 30.0, "YPFD.BA": 30000.0}.get(self.ticker, 100.0)
        close = base * np.cumprod(1 + rng.normal(0, 0.01, len(idx)))
        if self.ticker == "GGAL.BA":   # CCL coherente: GGAL.BA*10/GGAL ≈ 1000
            close = _FakeTicker("GGAL").history()["Close"].to_numpy() * 100
        if self.ticker == "YPFD.BA":   # YPFD.BA/YPF ≈ 1000
            close = _FakeTicker("YPF").history()["Close"].to_numpy() * 1000
        if self.ticker == "PAMP.BA":   # split mal ajustado
            close[200:] = close[200:] / 10
        return pd.DataFrame({"Open": close, "Close": close, "Volume": rng.integers(1e3, 1e5, len(idx))},
                            index=idx)


class _FakeYF:
    Ticker = _FakeTicker


@pytest.fixture(autouse=True)
def _aislar(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(bf, "yf", _FakeYF)
    monkeypatch.setattr(bf.time, "sleep", lambda *_: None)
    monkeypatch.setattr(bf, "_universo", lambda: {
        "merval": ({"GGAL.BA": "Galicia", "PAMP.BA": "Pampa", "MIRG.BA": "Mirgor"}, "^MERV"),
        "bovespa": ({"PETR4.SA": "Petrobras", "VALE3.SA": "Vale"}, "^BVSP"),
        "sp500": ({"AAPL": "Apple", "MSFT": "Microsoft"}, "^GSPC"),
    })
    yield


def test_prueba_no_escribe_en_research_ni_pushea(monkeypatch):
    pushes = []
    monkeypatch.setattr("src.github_persistence.push_file", lambda p, message=None: pushes.append(p) or True)
    r = bf.main(aplicar=False)
    assert r["modo"] == "prueba"
    assert not pushes
    assert all("_prueba" in p for p in r["archivos"])
    assert r["mercados"]["merval"]["tickers_ok"] == 3  # 2 tickers + INDICE


def test_completo_archivos_formato_y_calidad(monkeypatch):
    pushes = []
    monkeypatch.setattr("src.github_persistence.push_file", lambda p, message=None: pushes.append(p) or True)
    r = bf.main(aplicar=True)
    m = r["mercados"]["merval"]
    assert "MIRG.BA" in m["fallidos"]
    assert m["por_ticker"]["PAMP.BA"]["saltos_sospechosos"] == 1
    df = pd.read_csv("data/research/merval_10y.csv", sep=";", decimal=",", encoding="utf-8-sig", index_col=0)
    assert "INDICE" in df.columns and "GGAL.BA" in df.columns
    assert pd.to_datetime(df.index).tz is None
    assert len(pushes) == len(r["archivos"])


def test_ccl_coherente():
    r = bf.main(aplicar=True, push=False)
    fx = pd.read_csv("data/research/fx_10y.csv", sep=";", decimal=",", encoding="utf-8-sig", index_col=0)
    assert fx["CCL"].median() == pytest.approx(1000, rel=0.01)
    assert r["fx"]["CCL"]["ratio_ggal_vs_ypf_mediana"] == pytest.approx(1.0, rel=0.01)
    assert (fx["CCL"] == fx["CCL_GGAL"]).all()


def test_motor_lee_dataset_research(monkeypatch):
    """El dataset que escribe el backfill lo tiene que poder leer diagnostico_ic."""
    from scripts import diagnostico_ic as dg
    _FakeTicker.N_DIAS = 400
    monkeypatch.setattr(bf, "_universo", lambda: {
        "merval": ({f"T{i}.BA": f"T{i}" for i in range(12)}, "^MERV"),
        "bovespa": ({f"B{i}.SA": f"B{i}" for i in range(12)}, "^BVSP"),
        "sp500": ({f"S{i}": f"S{i}" for i in range(12)}, "^GSPC"),
    })
    bf.main(aplicar=True, push=False)
    precios = dg.load_prices_research()
    assert set(precios) == {"MERVAL", "BOVESPA", "SP500"}
    P, I = precios["MERVAL"]
    assert "INDICE" not in P.columns and I is not None and P.shape[1] == 12
    monkeypatch.setattr(dg, "_costo_pata_pct", lambda mk: 0.605)
    res = dg.motor_mercado("MERVAL", P, I)
    assert "resumen_oos" in res


def test_yahoo_caido_no_rompe_y_no_pushea(monkeypatch):
    """Regresión 28/09: con 0 descargas, pd.DataFrame({}) traía RangeIndex y
    rompía en .dayofweek. Ahora tiene que devolver un error claro."""
    class _Vacio:
        def __init__(self, t): pass
        def history(self, **kw): return pd.DataFrame()
    monkeypatch.setattr(bf, "yf", type("YF", (), {"Ticker": _Vacio, "__version__": "0.2.54"}))
    pushes = []
    monkeypatch.setattr("src.github_persistence.push_file", lambda p, message=None: pushes.append(p) or True)
    r = bf.main(aplicar=True)
    assert "error_global" in r and "0.2.54" in r["error_global"]
    assert not pushes and not os.path.exists("data/research/merval_10y.csv")
    assert any("❌" in l for l in r["telegram_lines"])


def test_un_mercado_caido_no_frena_a_los_otros(monkeypatch):
    class _SoloSP(_FakeTicker):
        def history(self, **kw):
            if self.ticker.endswith(".BA") or self.ticker.endswith(".SA") or self.ticker in ("^MERV", "^BVSP"):
                return pd.DataFrame()
            return super().history(**kw)
    monkeypatch.setattr(bf, "yf", type("YF", (), {"Ticker": _SoloSP}))
    r = bf.main(aplicar=True, push=False)
    assert r["mercados"]["merval"]["error"] == "sin datos"
    assert r["mercados"]["sp500"]["tickers_ok"] == 3
    assert "error_global" not in r
