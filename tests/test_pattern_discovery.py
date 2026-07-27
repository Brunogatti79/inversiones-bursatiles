"""
tests/test_pattern_discovery.py

Instrucción permanente (pedido de Bruno, 27/07/2026): "a partir de los
resultados, que el modelo vaya aprendiendo -- veamos qué posibilidades se
pueden ir poniendo a la luz en el dashboard para ir mejorando". Este es el
mecanismo real: backtester._detect_pattern_discoveries() compara el cruce
confidence_x_signal de cada corrida contra la anterior y detecta 2 eventos
dignos de revisión humana (nueva_evidencia, cambio_de_signo), sin esperar a
que alguien los busque a mano -- como se hizo el 27/07/2026 con Alta+Compra.

Cubre:
  - backtester._detect_pattern_discoveries(): lógica de detección pura
  - generator._render_model_conclusions_panel(): banner visual en Panorama
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import unittest.mock as mock

import src.backtester as bt
import src.github_persistence as ghp
from src.generator import _render_model_conclusions_panel


@pytest.fixture
def _fake_store():
    """Simula github_persistence sin depender de GH_TOKEN ni red real."""
    store = {}

    def fake_load(path, default=None):
        return store.get(path, default)

    def fake_save(path, data, message=None, push=True):
        store[path] = data
        return True

    with mock.patch.object(ghp, "load_json", fake_load), \
         mock.patch.object(ghp, "save_json", fake_save):
        yield store


class TestDetectPatternDiscoveries:

    def test_combinacion_nueva_con_muestra_suficiente_se_reporta(self, _fake_store):
        cross = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 20, "expected_value": 1.5}, "h10d": None}}}
        eventos = bt._detect_pattern_discoveries(cross)
        assert len(eventos) == 1
        assert eventos[0]["tipo"] == "nueva_evidencia"
        assert eventos[0]["combinacion"] == "🟢 Alta + 🟢 COMPRA"
        assert eventos[0]["n"] == 20

    def test_combinacion_con_poca_muestra_no_se_reporta(self, _fake_store):
        cross = {"🟢 Alta": {"⭐ COMPRA FUERTE": {"h5d": {"samples": 3, "expected_value": 8.0}, "h10d": None}}}
        eventos = bt._detect_pattern_discoveries(cross, min_samples=15)
        assert eventos == []

    def test_misma_combinacion_dos_corridas_seguidas_no_repite_el_evento(self, _fake_store):
        cross = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 20, "expected_value": 1.5}, "h10d": None}}}
        eventos1 = bt._detect_pattern_discoveries(cross)
        eventos2 = bt._detect_pattern_discoveries(cross)
        assert len(eventos1) == 1
        assert eventos2 == []  # ya se reportó una vez, no de nuevo

    def test_cambio_de_signo_se_detecta(self, _fake_store):
        cross_positivo = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 20, "expected_value": 1.5}, "h10d": None}}}
        cross_negativo = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 25, "expected_value": -0.8}, "h10d": None}}}

        bt._detect_pattern_discoveries(cross_positivo)
        eventos = bt._detect_pattern_discoveries(cross_negativo)

        assert len(eventos) == 1
        assert eventos[0]["tipo"] == "cambio_de_signo"
        assert eventos[0]["ev_antes"] == 1.5
        assert eventos[0]["ev_ahora"] == -0.8

    def test_mismo_signo_entre_corridas_no_dispara_cambio_de_signo(self, _fake_store):
        cross_v1 = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 20, "expected_value": 1.5}, "h10d": None}}}
        cross_v2 = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 30, "expected_value": 0.3}, "h10d": None}}}  # sigue positivo

        bt._detect_pattern_discoveries(cross_v1)
        eventos = bt._detect_pattern_discoveries(cross_v2)
        assert eventos == []

    def test_usa_el_horizonte_con_mas_muestra_real(self, _fake_store):
        cross = {
            "🟡 Media": {
                "🟢 COMPRA": {
                    "h5d":  {"samples": 10, "expected_value": 5.0},
                    "h10d": {"samples": 20, "expected_value": -1.0},
                },
            },
        }
        eventos = bt._detect_pattern_discoveries(cross, min_samples=15)
        assert len(eventos) == 1
        assert eventos[0]["ev"] == -1.0  # h10d tiene mas muestra (20 > 10) -> se usa ese

    def test_cross_vacio_o_none_no_rompe(self, _fake_store):
        assert bt._detect_pattern_discoveries({}) == []
        assert bt._detect_pattern_discoveries(None) == []

    def test_log_persiste_snapshot_para_la_proxima_corrida(self, _fake_store):
        cross = {"🟢 Alta": {"🟢 COMPRA": {"h5d": {"samples": 20, "expected_value": 1.5}, "h10d": None}}}
        bt._detect_pattern_discoveries(cross, path="data/pattern_discovery_log.json")
        log = _fake_store["data/pattern_discovery_log.json"]
        assert "🟢 Alta + 🟢 COMPRA" in log["_ultimo_snapshot"]
        assert log["_ultimo_snapshot"]["🟢 Alta + 🟢 COMPRA"]["n"] == 20

    def test_historial_de_eventos_queda_acotado_a_200(self, _fake_store):
        # Generar más de 200 eventos reales via 210 combinaciones distintas
        # que superan el umbral en su primera aparición cada una.
        for i in range(210):
            cross = {f"combo_{i}": {"sig": {"h5d": {"samples": 20, "expected_value": 1.0}, "h10d": None}}}
            bt._detect_pattern_discoveries(cross, path="data/pattern_discovery_log.json")
        log = _fake_store["data/pattern_discovery_log.json"]
        assert len(log["eventos"]) <= 200


class TestBannerEnDashboard:

    def test_banner_aparece_con_discoveries(self):
        backtest = {
            "days_history": 19, "total_trades": 938,
            "by_signal": {}, "confidence_calibration": {}, "confidence_calibration_curve": {},
            "by_confidence_label": {}, "by_consenso": {}, "by_market": {}, "by_sector": {},
            "stop_target": {}, "confidence_quantiles": {}, "ranking_top_vs_rest": {},
            "pattern_discoveries_nuevas": [
                {"tipo": "nueva_evidencia", "combinacion": "🟢 Alta + 🟢 COMPRA", "n": 41, "ev": 0.95},
            ],
        }
        html = _render_model_conclusions_panel(backtest, [])
        assert "Patrones nuevos detectados esta corrida" in html
        assert "🟢 Alta + 🟢 COMPRA" in html

    def test_banner_ausente_sin_discoveries(self):
        backtest = {
            "days_history": 19, "total_trades": 938,
            "by_signal": {}, "confidence_calibration": {}, "confidence_calibration_curve": {},
            "by_confidence_label": {}, "by_consenso": {}, "by_market": {}, "by_sector": {},
            "stop_target": {}, "confidence_quantiles": {}, "ranking_top_vs_rest": {},
            "pattern_discoveries_nuevas": [],
        }
        html = _render_model_conclusions_panel(backtest, [])
        assert "Patrones nuevos detectados" not in html

    def test_clave_ausente_no_rompe(self):
        """Compatibilidad hacia atrás: un backtest_results.json de antes de
        este fix no tiene la clave -- no debe romper el panel."""
        backtest = {
            "days_history": 19, "total_trades": 938,
            "by_signal": {}, "confidence_calibration": {}, "confidence_calibration_curve": {},
            "by_confidence_label": {}, "by_consenso": {}, "by_market": {}, "by_sector": {},
            "stop_target": {}, "confidence_quantiles": {}, "ranking_top_vs_rest": {},
        }
        html = _render_model_conclusions_panel(backtest, [])
        assert "Patrones nuevos detectados" not in html
        assert "📊 Conclusiones del Modelo" in html
