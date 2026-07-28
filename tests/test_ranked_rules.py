"""
tests/test_ranked_rules.py

Tests para _rank_discovered_rules() -- meta-backtester (auditoría externa
28/07/2026, punto 6 del roadmap): "construiría una capa que rankee
automáticamente: Regla, N, EV, Sharpe, Drawdown, Estabilidad, Cambio
reciente -- y genere un ranking".

No usa datos reales de mercado -- opera directamente sobre la forma que
ya producen _aggregate_cross / _metrics_from_rets (celdas con h5d/h10d),
así que se construyen celdas sintéticas directamente en ese formato en vez
de pasar por todo el pipeline de precios.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.backtester import _rank_discovered_rules


def _cell(n=20, ev=1.5, sharpe=0.8, max_drawdown=-3.0, profit_factor=1.8,
          significativo_95=True, horizon="h5d"):
    """Celda sintética con la forma real que arma _metrics_from_rets."""
    metrics = {
        "samples": n, "win_rate": 0.6, "avg_ret": ev, "avg_win": 3.0,
        "avg_loss": 2.0, "expected_value": ev, "sharpe": sharpe,
        "max_drawdown": max_drawdown, "profit_factor": profit_factor,
        "p_value": 0.01, "ic95": [ev - 1, ev + 1], "significativo_95": significativo_95,
    }
    other = "h10d" if horizon == "h5d" else "h5d"
    return {horizon: metrics, other: None}


class TestRankDiscoveredRulesBasics:

    def test_excluye_celdas_sin_muestra_suficiente(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=5)}}  # < min_samples=15
        rules = _rank_discovered_rules(cross)
        assert rules == []

    def test_incluye_celdas_con_muestra_suficiente(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20)}}
        rules = _rank_discovered_rules(cross)
        assert len(rules) == 1
        r = rules[0]
        assert r["regla"] == "🟢 Alta + 🟢 COMPRA"
        assert r["n"] == 20
        assert r["ev"] == 1.5
        assert r["sharpe"] == 0.8
        assert r["max_drawdown"] == -3.0
        assert r["profit_factor"] == 1.8
        assert r["significativo_95"] is True
        assert r["walk_forward_estado"] == "no_evaluado"  # sin walk_forward pasado
        assert r["cambio_reciente"] is False

    def test_min_samples_parametrizable(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=5)}}
        rules = _rank_discovered_rules(cross, min_samples=3)
        assert len(rules) == 1

    def test_horizonte_10d_usado_cuando_tiene_mas_muestra(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20, horizon="h10d")}}
        rules = _rank_discovered_rules(cross)
        assert rules[0]["horizonte"] == "10d"


class TestRankDiscoveredRulesWalkForward:

    def test_walk_forward_estado_conectado(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20)}}
        walk_forward = {"combinaciones": {
            "🟢 Alta + 🟢 COMPRA": {"estado": "confirmado"}
        }}
        rules = _rank_discovered_rules(cross, walk_forward=walk_forward)
        assert rules[0]["walk_forward_estado"] == "confirmado"

    def test_cambio_reciente_conectado(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20)}}
        recent_events = [{"tipo": "nueva_evidencia", "combinacion": "🟢 Alta + 🟢 COMPRA"}]
        rules = _rank_discovered_rules(cross, recent_events=recent_events)
        assert rules[0]["cambio_reciente"] is True

    def test_regla_sin_evento_reciente_queda_false(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20)}}
        recent_events = [{"tipo": "nueva_evidencia", "combinacion": "🟡 Media + 🟢 COMPRA"}]
        rules = _rank_discovered_rules(cross, recent_events=recent_events)
        assert rules[0]["cambio_reciente"] is False


class TestRankDiscoveredRulesOrdering:

    def test_confirmado_va_antes_que_no_confirmado(self):
        cross = {
            "🟢 Alta": {"🟢 COMPRA": _cell(n=20, ev=1.0)},
            "🟡 Media": {"🟢 COMPRA": _cell(n=20, ev=5.0)},  # EV mayor pero no_confirmado
        }
        walk_forward = {"combinaciones": {
            "🟢 Alta + 🟢 COMPRA": {"estado": "confirmado"},
            "🟡 Media + 🟢 COMPRA": {"estado": "no_confirmado"},
        }}
        rules = _rank_discovered_rules(cross, walk_forward=walk_forward)
        assert rules[0]["regla"] == "🟢 Alta + 🟢 COMPRA"  # confirmado gana pese a EV menor
        assert rules[1]["regla"] == "🟡 Media + 🟢 COMPRA"

    def test_dentro_del_mismo_estado_ordena_por_ev_descendente(self):
        cross = {
            "🟢 Alta": {"🟢 COMPRA": _cell(n=20, ev=1.0)},
            "🟡 Media": {"🟢 COMPRA": _cell(n=20, ev=3.0)},
        }
        rules = _rank_discovered_rules(cross)  # ambas "no_evaluado" -- mismo estado
        assert rules[0]["ev"] == 3.0
        assert rules[1]["ev"] == 1.0

    def test_desempate_por_n_descendente(self):
        cross = {
            "🟢 Alta": {"🟢 COMPRA": _cell(n=20, ev=2.0)},
            "🟡 Media": {"🟢 COMPRA": _cell(n=50, ev=2.0)},
        }
        rules = _rank_discovered_rules(cross)
        assert rules[0]["n"] == 50
        assert rules[1]["n"] == 20

    def test_empty_cross_devuelve_lista_vacia(self):
        assert _rank_discovered_rules({}) == []

    def test_walk_forward_none_no_rompe(self):
        cross = {"🟢 Alta": {"🟢 COMPRA": _cell(n=20)}}
        rules = _rank_discovered_rules(cross, walk_forward=None, recent_events=None)
        assert rules[0]["walk_forward_estado"] == "no_evaluado"
        assert rules[0]["cambio_reciente"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
