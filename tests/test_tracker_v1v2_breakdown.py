"""
tests/test_tracker_v1v2_breakdown.py

Mejora 4.3.2 — persistir los componentes V1/V2 desagregados.

Punto de partida real (verificado contra data/signals_history.json del
repo, no asumido): analyzer.py YA expone score_macro/score_tecnico/
score_sectorial/score_fundamental/asset_quality/entry_score/score_final_v2
en la señal en vivo, y tracker.update_history() YA los persistía -- esta
parte del roadmap estaba más avanzada de lo que la planificación original
asumía. Lo único que faltaba: aq_weight_used/es_weight_used (cuánto pesó
AQ vs ES por mercado ese día -- necesario para auditar weight_optimizer
después del hecho) y consenso (si V1 y V2 coincidieron). Estos tests
cubren esos 3 campos nuevos y, de paso, fijan que el resto del breakdown
no se pierda en un futuro refactor de tracker.py.
"""
import json

import pytest

import src.tracker as tracker


@pytest.fixture(autouse=True)
def _isolate_history_file(tmp_path, monkeypatch):
    monkeypatch.setattr(tracker, "HISTORY_PATH", str(tmp_path / "signals_history.json"))
    monkeypatch.setattr(tracker, "_push_signals_history_to_github", lambda: None)


def _signal(**overrides):
    base = {
        "ticker": "GGAL.BA", "mercado": "MERVAL", "sector": "Financiero",
        "precio_actual": 150.0, "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA",
        "score_final": 62.0, "score_final_v2": 58.5,
        "score_macro": 60.9, "score_tecnico": 55.0, "score_fundamental": 70.0,
        "score_sectorial": 50.0, "ranking_accionable": 61.2, "rr_ratio": 2.3,
        "asset_quality": 64.0, "entry_score": 55.0,
        "aq_weight_used": 0.60, "es_weight_used": 0.40,
        "consenso": "✅ Consenso",
        "atr_stop": 140.0, "atr_target": 165.0, "atr": 5.2,
        "pred_5d": 1.2, "pred_21d": 4.5, "pred_signal": "📈 SUBA", "pred_confidence": 0.71,
        "rsi": 48.0, "ret_anual": 12.0, "ret_mes": 3.0,
        "factor_contrib": {"macro": 9.1, "tecnico": 19.3, "fundamental": 14.0, "sector": 5.0},
        "factor_dominante": "tecnico",
    }
    base.update(overrides)
    return base


class TestV1V2BreakdownPersistence:

    def test_aq_es_weights_are_persisted(self):
        tracker.update_history([_signal()])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today_entry = next(iter(history.values()))
        assert today_entry[0]["aq_weight_used"] == 0.60
        assert today_entry[0]["es_weight_used"] == 0.40

    def test_consenso_is_persisted(self):
        tracker.update_history([_signal(consenso="V1↑/V2↓ activo fuerte, mal entry")])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today_entry = next(iter(history.values()))
        assert today_entry[0]["consenso"] == "V1↑/V2↓ activo fuerte, mal entry"

    def test_missing_aq_es_weight_defaults_to_zero_without_raising(self):
        """Señales viejas (de antes de esta mejora) o de un ticker donde el
        cálculo V2 falló no tienen estos campos -- no debe romper el
        guardado del historial."""
        signal = _signal()
        del signal["aq_weight_used"]
        del signal["es_weight_used"]
        del signal["consenso"]

        tracker.update_history([signal])  # no debe lanzar excepción

        history = json.loads(open(tracker.HISTORY_PATH).read())
        today_entry = next(iter(history.values()))
        assert today_entry[0]["aq_weight_used"] == 0
        assert today_entry[0]["es_weight_used"] == 0
        assert today_entry[0]["consenso"] == ""

    def test_full_v1_v2_breakdown_survives_round_trip(self):
        """Regresión amplia: todos los campos del breakdown V1/V2 que ya
        existían siguen ahí después de esta mejora -- no se perdió nada al
        agregar los 3 campos nuevos."""
        tracker.update_history([_signal()])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        entry = next(iter(history.values()))[0]

        expected_fields = {
            "score_v1", "score_v2", "score_macro", "score_tecnico",
            "score_fund", "score_sectorial", "ranking", "rr_ratio",
            "asset_quality", "entry_score", "aq_weight_used", "es_weight_used",
            "consenso", "atr_stop", "atr_target", "atr",
            "pred_5d", "pred_21d", "pred_signal", "pred_confidence",
            "rsi", "ret_anual", "ret_mes", "factor_contrib", "factor_dominante",
        }
        assert expected_fields.issubset(entry.keys())

    def test_factor_contrib_breakdown_sums_roughly_to_score_v1_components(self):
        """No es una igualdad exacta (los pesos pueden venir optimizados),
        pero la suma de factor_contrib debería quedar en el orden de
        magnitud del score_final -- si algún día se desalinean por un bug
        de unidades, esto debería notarlo."""
        sig = _signal(score_final=62.0, factor_contrib={
            "macro": 9.1, "tecnico": 19.3, "fundamental": 14.0, "sector": 5.0,
        })
        tracker.update_history([sig])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        entry = next(iter(history.values()))[0]
        total_contrib = sum(entry["factor_contrib"].values())
        assert abs(total_contrib - sig["score_final"]) < 20  # mismo orden de magnitud
