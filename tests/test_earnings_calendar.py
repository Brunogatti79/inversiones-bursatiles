"""
tests/test_earnings_calendar.py

Shadow de blackout pre-earnings (25/09/2026). Nunca escribe en data/ real
ni pega a GitHub/Yahoo: fixture autouse redirige CACHE_PATH a tmp_path y
anula el push.
"""
import ast
import json
import os
from datetime import datetime, date

import numpy as np
import pandas as pd
import pytest

import src.earnings_calendar as ec

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _aislar(tmp_path, monkeypatch):
    monkeypatch.setattr(ec, "CACHE_PATH", str(tmp_path / "earnings_calendar.json"))
    import src.github_persistence as gp
    monkeypatch.setattr(gp, "push_file", lambda *a, **k: True)
    yield
    assert not os.path.exists(os.path.join(REPO, "data", "earnings_calendar.json")), \
        "un test escribió en data/ real"


def _cal(dates, source="yf"):
    return {"generated": "2026-09-25T10:00:00", "tickers": {"HAPV3.SA": {"dates": dates, "source": source}}}


# ── compute_earnings_fields ──────────────────────────────────────────────────

class TestComputeFields:
    def test_dias_habiles_y_blackout(self):
        # 28/07/2026 (mar) -> 13/08/2026 (jue): 12 ruedas hábiles
        f = ec.compute_earnings_fields("HAPV3.SA", "2026-07-28", _cal(["2026-05-10", "2026-08-13"]))
        assert f["earnings_days_to"] == 12
        assert f["earnings_blackout_shadow"] is False
        assert f["earnings_next_date"] == "2026-08-13"
        assert f["earnings_last_date"] == "2026-05-10"

    def test_dentro_del_umbral(self):
        f = ec.compute_earnings_fields("HAPV3.SA", "2026-08-04", _cal(["2026-08-13"]))
        assert f["earnings_days_to"] == 7
        assert f["earnings_blackout_shadow"] is True

    def test_mismo_dia_es_cero(self):
        f = ec.compute_earnings_fields("HAPV3.SA", date(2026, 8, 13), _cal(["2026-08-13"]))
        assert f["earnings_days_to"] == 0 and f["earnings_blackout_shadow"] is True

    def test_desconocido_es_none_no_false(self):
        f = ec.compute_earnings_fields("HAPV3.SA", "2026-09-25", _cal(["2026-05-10"]))
        assert f["earnings_blackout_shadow"] is None
        assert f["earnings_days_to"] is None
        g = ec.compute_earnings_fields("XXXX", "2026-09-25", _cal([]))
        assert g["earnings_blackout_shadow"] is None and g["earnings_source"] is None

    def test_umbral_es_el_preregistrado(self):
        assert ec.PRE_BLACKOUT_BDAYS == 10


# ── refresh / cache ──────────────────────────────────────────────────────────

class TestRefresh:
    def test_merge_conserva_fechas_viejas_si_fetch_falla(self):
        now = datetime(2026, 9, 25)
        ec.refresh_earnings_calendar(["A"], now=now, fetcher=lambda s: ["2026-05-01"], sleep=0)
        cal = ec.refresh_earnings_calendar(["A"], now=now, force=True,
                                           fetcher=lambda s: [], sleep=0)
        assert cal["tickers"]["A"]["dates"] == ["2026-05-01"]
        assert cal["tickers"]["A"]["source"] == "yf"

    def test_merge_une_fechas(self):
        now = datetime(2026, 9, 25)
        ec.refresh_earnings_calendar(["A"], now=now, fetcher=lambda s: ["2026-05-01"], sleep=0)
        cal = ec.refresh_earnings_calendar(["A"], now=now, force=True,
                                           fetcher=lambda s: ["2026-08-01"], sleep=0)
        assert cal["tickers"]["A"]["dates"] == ["2026-05-01", "2026-08-01"]

    def test_fallback_adr(self):
        calls = []
        def fake(sym):
            calls.append(sym)
            return ["2026-11-10"] if sym == "PBR" else []
        cal = ec.refresh_earnings_calendar(["PETR4.SA"], now=datetime(2026, 9, 25),
                                           fetcher=fake, sleep=0)
        assert calls == ["PETR4.SA", "PBR"]
        assert cal["tickers"]["PETR4.SA"]["source"] == "yf_adr:PBR"

    def test_txar_no_mapea_a_ternium_sa(self):
        assert "TXAR.BA" not in ec.ADR_FALLBACK

    def test_no_refresca_si_es_reciente(self):
        now = datetime(2026, 9, 25)
        ec.refresh_earnings_calendar(["A"], now=now, fetcher=lambda s: ["2026-05-01"], sleep=0)
        boom = lambda s: (_ for _ in ()).throw(AssertionError("no debía fetchear"))
        ec.refresh_earnings_calendar(["A"], now=datetime(2026, 9, 28), fetcher=boom, sleep=0)

    def test_refresca_si_hay_ticker_nuevo_o_vencido(self):
        now = datetime(2026, 9, 25)
        ec.refresh_earnings_calendar(["A"], now=now, fetcher=lambda s: ["2026-05-01"], sleep=0)
        cal = ec.load_calendar(ec.CACHE_PATH)
        assert ec._needs_refresh(cal, ["A", "B"], now)
        assert ec._needs_refresh(cal, ["A"], datetime(2026, 10, 3))
        assert not ec._needs_refresh(cal, ["A"], datetime(2026, 9, 30))

    def test_fetcher_que_lanza_no_rompe(self):
        def bad(s):
            raise RuntimeError("yahoo 429")
        cal = ec.refresh_earnings_calendar(["A"], now=datetime(2026, 9, 25), fetcher=bad, sleep=0)
        assert cal["tickers"]["A"]["dates"] == []
        assert cal["last_refresh_ok"] == 0

    def test_cache_corrupto_se_trata_como_vacio(self):
        with open(ec.CACHE_PATH, "w") as f:
            f.write("{no json")
        assert ec.load_calendar(ec.CACHE_PATH) == {"generated": None, "tickers": {}}


# ── inject ───────────────────────────────────────────────────────────────────

class TestInject:
    def test_no_toca_senales_ni_scores(self):
        s = {"ticker": "HAPV3.SA", "mercado": "BOVESPA", "signal": "🟢 COMPRA",
             "signal_v2": "🟢 COMPRA", "score_v2": 61.0, "ranking": 55.0}
        before = dict(s)
        cov = ec.inject_earnings_shadow([s], _cal(["2026-08-13"]), as_of="2026-08-04")
        for k, v in before.items():
            assert s[k] == v
        assert s["earnings_blackout_shadow"] is True
        assert cov == {"BOVESPA": {"n": 1, "conocido": 1, "blackout": 1}}

    def test_cobertura_cuenta_desconocidos(self):
        sigs = [{"ticker": "HAPV3.SA", "mercado": "BOVESPA"},
                {"ticker": "ZZZ", "mercado": "BOVESPA"}]
        cov = ec.inject_earnings_shadow(sigs, _cal(["2026-12-01"]), as_of="2026-09-25")
        assert cov["BOVESPA"] == {"n": 2, "conocido": 1, "blackout": 0}


# ── integración (estática, sin importar pipeline/start_server) ──────────────

class TestIntegracion:
    def _src(self, rel):
        with open(os.path.join(REPO, rel), encoding="utf-8") as f:
            return f.read()

    def test_tracker_persiste_campos(self):
        src = self._src("src/tracker.py")
        for k in ["earnings_next_date", "earnings_days_to", "earnings_blackout_shadow",
                  "earnings_last_date", "earnings_source"]:
            assert f'"{k}"' in src

    def test_pipeline_inyecta_en_try_propio(self):
        src = self._src("src/pipeline.py")
        assert "inject_earnings_shadow(all_signals" in src
        assert "Shadow earnings no disponible" in src
        ast.parse(src)

    def test_start_server_sincroniza_calendario(self):
        tree = ast.parse(self._src("start_server.py"))
        names = []
        for n in ast.walk(tree):
            if isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", "")) == "sync_all_at_startup":
                names = [e.value for e in n.args[0].elts if isinstance(e, ast.Constant)]
        assert "earnings_calendar.json" in names


# ── script diagnóstico sobre datos sintéticos ────────────────────────────────

class TestDiagnostico:
    def test_detecta_efecto_sintetico(self):
        from scripts import diagnostico_earnings_blackout as dg
        idx = pd.bdate_range("2025-09-01", periods=260)
        rng = np.random.default_rng(0)
        tks = [f"T{i}" for i in range(8)]
        P = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.01, (260, 8)), 0)),
                         index=idx, columns=tks)
        cal = {"tickers": {}}
        for j, t in enumerate(tks):
            evs = idx[20 + 7 * j::63]   # escalonados: el universo no cae todo junto
            cal["tickers"][t] = {"dates": [d.strftime("%Y-%m-%d") for d in evs]}
            for e in evs:                       # caída del 15% el día del resultado
                P.loc[e:, t] *= 0.85
        colmap = {t: ("BOVESPA", t) for t in tks}
        panel = dg.build_panel({"BOVESPA": P}, colmap, cal)
        res = dg.evaluate(panel)["BOVESPA"]
        assert res["diff_pp"] < -5
        assert res["material"] is True
        assert res["p5_blackout"] < res["p5_resto"]

    def test_days_to_next(self):
        from scripts import diagnostico_earnings_blackout as dg
        idx = pd.DatetimeIndex(["2026-08-04", "2026-08-13", "2026-08-14"])
        d = dg.days_to_next(idx, ["2026-08-13"])
        assert d[0] == 7 and d[1] == 0 and np.isnan(d[2])
