"""
tests/test_kill_switch_log.py

Tests para src/kill_switch_log.py (Prioridad 2, roadmap externo).

Dos clases de tests, una por responsabilidad del módulo:
  - TestLogKillSwitchEvent: el log es append-only, agrupa por fecha,
    nunca rompe el pipeline aunque falle el push a GitHub.
  - TestEvaluateKillSwitchHistory: el cálculo de forward return es
    correcto, separa activo/inactivo, respeta el gate de staleness, y no
    explota con historia insuficiente (caso esperado hoy: pocos o ningún
    día con kill switch activo).
"""
import json

import pandas as pd
import pytest

import src.kill_switch_log as ksl


# ── Fixtures comunes ─────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(ksl, "HISTORY_PATH", str(tmp_path / "kill_switch_history.json"))
    monkeypatch.setattr(ksl, "VALIDATION_PATH", str(tmp_path / "kill_switch_validation.json"))
    # Sin GH_TOKEN en el entorno de test, push_file devuelve False y
    # contaminaría todos los asserts sobre el valor de retorno -- se aísla
    # igual que test_kill_switch_monitor.py aísla _push_global_confidence_to_github.
    monkeypatch.setattr("src.github_persistence.push_file", lambda *a, **kw: True)


def _gc(active, score=80.0, label="🟢 Confiable", reasons=None):
    return {
        "global_score": score,
        "label": label,
        "kill_switch_active": active,
        "kill_switch_reasons": reasons or [],
    }


def _read_history(path):
    with open(path) as f:
        return json.load(f)


# ── 1. Logging ───────────────────────────────────────────────────────────

class TestLogKillSwitchEvent:

    def test_writes_first_entry(self, tmp_path):
        ok = ksl.log_kill_switch_event(_gc(active=False), n_signals=67)
        assert ok is True
        log = _read_history(ksl.HISTORY_PATH)
        assert len(log) == 1
        day = next(iter(log.values()))
        assert len(day) == 1
        assert day[0]["kill_switch_active"] is False
        assert day[0]["n_signals"] == 67

    def test_multiple_runs_same_day_accumulate_not_overwrite(self):
        """El pipeline corre 4x/día -- las 4 corridas del mismo día deben
        quedar todas en el log, no pisarse entre sí (a diferencia de
        append_by_date(), que sí pisa el día completo)."""
        for _ in range(4):
            ksl.log_kill_switch_event(_gc(active=False))

        log = _read_history(ksl.HISTORY_PATH)
        assert len(log) == 1
        day = next(iter(log.values()))
        assert len(day) == 4

    def test_quality_resumen_fields_persisted(self):
        ksl.log_kill_switch_event(
            _gc(active=True, score=20.0, reasons=["score bajo"]),
            quality_resumen={"criticas": 3, "criticas_estructurales": 1, "advertencias": 5},
            validacion_nivel="WARNING",
            sla_status="OK",
        )
        entry = next(iter(_read_history(ksl.HISTORY_PATH).values()))[0]
        assert entry["criticas"] == 3
        assert entry["criticas_estructurales"] == 1
        assert entry["advertencias"] == 5
        assert entry["validacion_nivel"] == "WARNING"
        assert entry["kill_switch_reasons"] == ["score bajo"]

    def test_criticas_estructurales_falls_back_to_criticas_if_missing(self):
        """Compatibilidad con quality_resumen viejo (pre fix 4.3) que no
        tiene el campo separado -- mismo criterio que confidence_score.py."""
        ksl.log_kill_switch_event(_gc(active=False), quality_resumen={"criticas": 2})
        entry = next(iter(_read_history(ksl.HISTORY_PATH).values()))[0]
        assert entry["criticas_estructurales"] == 2

    def test_never_raises_even_if_github_persistence_broken(self, monkeypatch):
        def _boom(*a, **kw):
            raise RuntimeError("github caído")
        monkeypatch.setattr(
            "src.github_persistence.load_json",
            _boom,
        )
        # No debe lanzar excepción -- esto es observabilidad, no puede
        # tumbar una corrida del pipeline.
        ok = ksl.log_kill_switch_event(_gc(active=True))
        assert ok is False

    def test_retention_cutoff_drops_old_days(self, monkeypatch, tmp_path):
        from datetime import datetime, timedelta
        monkeypatch.setattr(ksl, "MAX_DAYS", 10)

        old_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        seed = {old_date: [{"kill_switch_active": False}]}
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        ksl.log_kill_switch_event(_gc(active=False))
        log = _read_history(ksl.HISTORY_PATH)
        assert old_date not in log


# ── 2. Evaluación retroactiva ────────────────────────────────────────────

def _make_serie(start="2025-01-01", n=40, start_price=100.0, daily_change=0.0):
    """Serie de precios sintética con cambio diario constante (% sobre el
    día anterior), para poder predecir el forward return exacto en los
    tests sin tener que calcularlo a mano para cada caso."""
    dates = pd.date_range(start, periods=n, freq="D")
    prices = [start_price]
    for _ in range(n - 1):
        prices.append(prices[-1] * (1 + daily_change / 100))
    return pd.Series(prices, index=dates, name="INDICE TEST")


class TestEvaluateKillSwitchHistory:

    def test_no_history_returns_existing_without_crashing(self):
        result = ksl.evaluate_kill_switch_history(price_data={}, index_cols={})
        assert result == {}

    def test_forward_return_active_day_followed_by_decline(self, tmp_path):
        """Caso central: un día con kill switch activo seguido de una baja
        del índice -> debe contar como 'helped' (forward_return_5d < 0)."""
        serie = _make_serie(daily_change=-1.0)  # cae ~1%/día
        active_date = serie.index[5].strftime("%Y-%m-%d")

        seed = {active_date: [{"kill_switch_active": True}]}
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        result = ksl.evaluate_kill_switch_history(
            price_data={"merval": pd.DataFrame({"INDICE TEST": serie})},
            index_cols={"merval": "INDICE TEST"},
        )

        assert result["n_records"] == 1
        active_stats = result["global"]["active"]
        assert active_stats["n"] == 1
        assert active_stats["avg_return_5d"] < 0
        assert active_stats["pct_decline"] == 100.0

    def test_active_vs_inactive_baseline_contrast(self):
        """Días activos en una serie que cae + días inactivos en la misma
        serie (mismo proxy de mercado) deben separarse correctamente en
        'active' vs 'inactive', no mezclarse."""
        serie = _make_serie(n=40, daily_change=-0.5)
        active_date   = serie.index[3].strftime("%Y-%m-%d")
        inactive_date = serie.index[10].strftime("%Y-%m-%d")

        seed = {
            active_date:   [{"kill_switch_active": True}],
            inactive_date: [{"kill_switch_active": False}],
        }
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        result = ksl.evaluate_kill_switch_history(
            price_data={"merval": pd.DataFrame({"INDICE TEST": serie})},
            index_cols={"merval": "INDICE TEST"},
        )

        assert result["global"]["active"]["n"] == 1
        assert result["global"]["inactive"]["n"] == 1

    def test_day_counts_as_active_if_any_run_that_day_was_active(self):
        """Un día con 4 corridas donde solo 1 tuvo kill switch activo debe
        contar el día completo como activo -- es un circuit breaker de
        sistema, alcanza con que se haya frenado capital una vez en el día."""
        serie = _make_serie(n=40, daily_change=-0.5)
        d = serie.index[5].strftime("%Y-%m-%d")
        seed = {d: [
            {"kill_switch_active": False},
            {"kill_switch_active": False},
            {"kill_switch_active": True},
            {"kill_switch_active": False},
        ]}
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        result = ksl.evaluate_kill_switch_history(
            price_data={"merval": pd.DataFrame({"INDICE TEST": serie})},
            index_cols={"merval": "INDICE TEST"},
        )
        assert result["global"]["active"]["n"] == 1

    def test_recent_date_without_enough_future_data_is_skipped(self):
        """Un evento de hace 2 días no tiene todavía 5 ruedas futuras -- no
        debe inventar un resultado, debe descartar ese registro."""
        serie = _make_serie(n=10, daily_change=-1.0)
        recent_date = serie.index[-2].strftime("%Y-%m-%d")  # casi al final
        seed = {recent_date: [{"kill_switch_active": True}]}
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        result = ksl.evaluate_kill_switch_history(
            price_data={"merval": pd.DataFrame({"INDICE TEST": serie})},
            index_cols={"merval": "INDICE TEST"},
        )
        # Sin registros evaluables -> devuelve el existing (vacío), no rompe
        assert result == {}

    def test_staleness_gate_skips_recompute_within_window(self, monkeypatch):
        from datetime import datetime
        existing = {"generated": datetime.now().isoformat(), "n_records": 99}
        with open(ksl.VALIDATION_PATH, "w") as f:
            json.dump(existing, f)

        called = []
        monkeypatch.setattr(
            "src.github_persistence.push_file",
            lambda *a, **kw: called.append(True),
        )

        result = ksl.evaluate_kill_switch_history(price_data={}, index_cols={})
        assert result["n_records"] == 99
        assert called == []  # no debe recalcular ni pushear nada

    def test_missing_index_column_skips_market_gracefully(self):
        """Si index_cols no tiene una entrada válida para un mercado (CSV
        vacío, columna no encontrada por _idx_col), no debe romper -- debe
        saltear ese mercado."""
        seed = {"2026-01-01": [{"kill_switch_active": True}]}
        with open(ksl.HISTORY_PATH, "w") as f:
            json.dump(seed, f)

        result = ksl.evaluate_kill_switch_history(
            price_data={"merval": pd.DataFrame({"OTRA_COL": [1, 2, 3]})},
            index_cols={"merval": ""},
        )
        assert result == {}


class TestForwardReturnHelper:

    def test_exact_calculation(self):
        serie = _make_serie(n=20, start_price=100.0, daily_change=0.0)
        serie.iloc[5] = 100.0
        serie.iloc[10] = 110.0  # +10% a 5 ruedas
        date_str = serie.index[5].strftime("%Y-%m-%d")
        ret = ksl._forward_return(serie, date_str, horizon=5)
        assert ret == pytest.approx(10.0, abs=0.01)

    def test_date_not_in_series_returns_none(self):
        serie = _make_serie(n=10)
        ret = ksl._forward_return(serie, "1999-01-01", horizon=5)
        assert ret is None

    def test_not_enough_future_data_returns_none(self):
        serie = _make_serie(n=10)
        last_date = serie.index[-1].strftime("%Y-%m-%d")
        ret = ksl._forward_return(serie, last_date, horizon=5)
        assert ret is None
