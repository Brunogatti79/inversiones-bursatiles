"""
tests/test_tracker_ohlc_regime_fields.py

Regresión real encontrada en producción el 27/07/2026: analyzer.py (v4.10/
v4.11) ya calculaba atr_metodo, vol_regime_mercado y v2_regime_mult en cada
señal en vivo, pero tracker.update_history() tiene un allowlist explícito de
campos (para no inflar signals_history.json) y no se actualizó al agregar
esos campos a analyzer.py -- quedaban siempre en None en el historial real,
pese a que la señal en vivo sí los traía bien. Confirmado contra la primera
corrida real post-deploy (commit d06b2f0, 27/07/2026 17:38 UTC): los 67
registros de ese día tenían atr_metodo=None y vol_regime_mercado=None.

Estos tests fijan que esos 3 campos queden en el historial de ahora en más,
y evitan que un futuro refactor de tracker.py los vuelva a perder en
silencio (el bug anterior no rompía nada, no lanzaba excepción -- por eso
pasó desapercibido hasta auditar signals_history.json real).
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
        "atr_metodo": "ohlc",
        "vol_regime_mercado": "HIGH",
        "v2_regime_mult": 0.90,
    }
    base.update(overrides)
    return base


class TestOhlcRegimeFieldsPersistence:

    def test_atr_metodo_se_persiste(self):
        tracker.update_history([_signal(atr_metodo="ohlc")])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today = list(history.keys())[0]
        assert history[today][0]["atr_metodo"] == "ohlc"

    def test_atr_metodo_close_proxy_tambien_se_persiste(self):
        tracker.update_history([_signal(atr_metodo="close_proxy")])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today = list(history.keys())[0]
        assert history[today][0]["atr_metodo"] == "close_proxy"

    def test_vol_regime_mercado_se_persiste(self):
        tracker.update_history([_signal(vol_regime_mercado="LOW")])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today = list(history.keys())[0]
        assert history[today][0]["vol_regime_mercado"] == "LOW"

    def test_v2_regime_mult_se_persiste(self):
        tracker.update_history([_signal(v2_regime_mult=1.05)])
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today = list(history.keys())[0]
        assert history[today][0]["v2_regime_mult"] == 1.05

    def test_faltantes_no_rompen_y_usan_default_seguro(self):
        """Una señal que por algún motivo no trajo estos campos (ej. un
        modulo viejo en cache) no debe romper update_history -- debe caer a
        un default seguro en vez de lanzar excepción."""
        signal_incompleta = {
            "ticker": "BMA.BA", "mercado": "MERVAL", "sector": "Financiero",
            "precio_actual": 100.0, "signal": "🟡 NEUTRAL", "signal_v2": "🟡 NEUTRAL",
            "score_final": 50.0, "score_final_v2": 50.0,
        }
        tracker.update_history([signal_incompleta])  # no debe lanzar excepción
        history = json.loads(open(tracker.HISTORY_PATH).read())
        today = list(history.keys())[0]
        assert history[today][0]["atr_metodo"] == ""
        assert history[today][0]["vol_regime_mercado"] == ""
        assert history[today][0]["v2_regime_mult"] == 1.0
