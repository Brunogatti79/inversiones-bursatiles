"""
tests/test_signals_archive.py

Invariantes del archivo permanente de señales (plan semana 1, 25/09/2026).
El riesgo que cubren es el mismo del incidente 27-28/07/2026: que un
redeploy con data/ vacío o una lectura fallida del remoto termine pisando
historia real.
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src import signals_archive as sa


def _row(fecha, ticker, **kw):
    r = {"fecha": fecha, "ticker": ticker}
    r.update(kw)
    return r


# ── merge_day ────────────────────────────────────────────────────────────────

def test_merge_agrega_dia_nuevo_sin_tocar_los_anteriores():
    base = [_row("2026-09-24", "AAPL", precio=1), _row("2026-09-24", "GGAL.BA", precio=2)]
    out = sa.merge_day(base, "2026-09-25", [_row("2026-09-25", "AAPL", precio=3)])
    assert out[:2] == base
    assert len(out) == 3


def test_merge_reemplaza_solo_el_dia_en_curso():
    base = [_row("2026-09-24", "AAPL", precio=1),
            _row("2026-09-25", "AAPL", precio=10), _row("2026-09-25", "MSFT", precio=11)]
    out = sa.merge_day(base, "2026-09-25", [_row("2026-09-25", "AAPL", precio=99)])
    assert [r for r in out if r["fecha"] == "2026-09-24"] == [base[0]]
    hoy = [r for r in out if r["fecha"] == "2026-09-25"]
    assert hoy == [_row("2026-09-25", "AAPL", precio=99)]


def test_merge_rechaza_fecha_anterior_a_la_ultima():
    base = [_row("2026-09-25", "AAPL")]
    assert sa.merge_day(base, "2026-09-24", [_row("2026-09-24", "AAPL")]) is None


def test_merge_sobre_base_vacia():
    out = sa.merge_day([], "2026-09-25", [_row("2026-09-25", "AAPL")])
    assert len(out) == 1


# ── union_cerradas ───────────────────────────────────────────────────────────

def test_union_suma_fechas_cerradas_que_solo_estan_en_local():
    remoto = [_row("2026-09-23", "AAPL")]
    local = [_row("2026-09-23", "AAPL"), _row("2026-09-24", "AAPL"), _row("2026-09-25", "AAPL")]
    out = sa.union_cerradas(remoto, local, "2026-09-25")
    assert [r["fecha"] for r in out] == ["2026-09-23", "2026-09-24"]  # el día en curso no se trae de local


def test_union_nunca_borra_remoto_aunque_local_este_vacio():
    remoto = [_row("2026-09-23", "AAPL"), _row("2026-09-24", "AAPL")]
    assert sa.union_cerradas(remoto, [], "2026-09-25") == remoto


# ── archive_day (I/O simulado) ──────────────────────────────────────────────

@pytest.fixture
def entorno(tmp_path, monkeypatch):
    monkeypatch.setattr(sa, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sa, "RUNS_PATH", str(tmp_path / "signals_archive_runs.jsonl"))
    monkeypatch.setattr(sa, "WEIGHTS_PATH", str(tmp_path / "optimized_weights.json"))
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "abc123")
    monkeypatch.setenv("GH_TOKEN", "test-token")
    pushes = []

    def fake_push(path, rows, msg):
        with open(path, "w", encoding="utf-8") as f:
            f.write(sa.dump_jsonl(rows))
        pushes.append(path)
        return True
    monkeypatch.setattr(sa, "_write_and_push", fake_push)
    return tmp_path, pushes


def test_archive_no_pushea_si_el_remoto_no_se_puede_leer(entorno):
    _, pushes = entorno

    def falla(path):
        raise sa.RemoteUnavailable("rate limit")
    ok = sa.archive_day("2026-09-25", [{"ticker": "AAPL"}], fetch=falla)
    assert ok is False and pushes == []


def test_archive_saltea_fines_de_semana(entorno):
    _, pushes = entorno
    assert sa.archive_day("2026-09-26", [{"ticker": "AAPL"}], fetch=lambda p: "") is False
    assert pushes == []


def test_archive_preserva_historia_remota_con_local_vacio(entorno):
    """Escenario del incidente de julio: container recién redeployado, data/ vacío."""
    tmp, _ = entorno
    remoto = sa.dump_jsonl([_row("2026-09-24", "AAPL", precio=1), _row("2026-09-24", "MSFT", precio=2)])
    fetch = lambda p: remoto if "2026-09" in p else ""
    assert sa.archive_day("2026-09-25", [{"ticker": "AAPL", "model_version": "4.16"}], fetch=fetch)
    rows = sa.parse_jsonl(open(tmp / "signals_archive_2026-09.jsonl", encoding="utf-8").read())
    assert [r["fecha"] for r in rows] == ["2026-09-24", "2026-09-24", "2026-09-25"]
    assert rows[-1]["code_sha"] == "abc123" and rows[-1]["origen"] == "pipeline"


def test_archive_registra_run_con_code_sha_y_pesos(entorno):
    tmp, _ = entorno
    (tmp / "optimized_weights.json").write_text(json.dumps({"MERVAL": {"macro": 0.35}}))
    sa.archive_day("2026-09-25", [{"ticker": "AAPL", "model_version": "4.16"}], fetch=lambda p: "")
    runs = sa.parse_jsonl(open(tmp / "signals_archive_runs.jsonl", encoding="utf-8").read())
    assert runs == [runs[0]] and runs[0]["code_sha"] == "abc123"
    assert runs[0]["pesos_optimizados"] == {"MERVAL": {"macro": 0.35}}
    assert runs[0]["model_version"] == "4.16"


def test_archive_rechaza_fecha_vieja(entorno):
    remoto = sa.dump_jsonl([_row("2026-09-25", "AAPL")])
    assert sa.archive_day("2026-09-24", [{"ticker": "AAPL"}],
                          fetch=lambda p: remoto if "2026-09" in p else "") is False


def test_archive_sin_token_no_toca_la_red(entorno, monkeypatch):
    monkeypatch.delenv("GH_TOKEN")

    def no_deberia_llamarse(path):
        raise AssertionError("tocó la red sin token")
    assert sa.archive_day("2026-09-25", [{"ticker": "AAPL"}], fetch=no_deberia_llamarse) is False


def test_load_archive_filtra_por_rango(entorno):
    tmp, _ = entorno
    (tmp / "signals_archive_2026-08.jsonl").write_text(sa.dump_jsonl([_row("2026-08-31", "AAPL")]))
    (tmp / "signals_archive_2026-09.jsonl").write_text(sa.dump_jsonl([_row("2026-09-01", "AAPL")]))
    assert len(sa.load_archive(data_dir=str(tmp))) == 2
    assert [r["fecha"] for r in sa.load_archive(desde="2026-09-01", data_dir=str(tmp))] == ["2026-09-01"]


# ── Integración con tracker.update_history ──────────────────────────────────

def test_update_history_no_se_rompe_si_el_archivo_falla(tmp_path, monkeypatch):
    from src import tracker
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tracker, "_push_signals_history_to_github", lambda: True)

    def explota(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(sa, "archive_day", explota)
    hist = tracker.update_history([{"ticker": "AAPL", "mercado": "SP500"}])
    assert len(hist) == 1
