"""
tests/test_kill_switch_monitor.py

Tests para src/monitor.persist_global_confidence() (mejora 3.1 + 3.5).

El comportamiento que más importa blindar: la alerta Telegram debe
dispararse SOLO en una transición de estado (inactivo->activo o
activo->recuperado), nunca en cada run -- el pipeline corre 4x/día y un
mensaje nuevo en cada corrida mientras el kill switch sigue activo sería
puro ruido.
"""
import json

import pytest

import src.monitor as monitor


@pytest.fixture(autouse=True)
def _isolate_state_file(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, "GLOBAL_CONFIDENCE_PATH", str(tmp_path / "system_confidence.json"))
    monkeypatch.setattr(monitor, "_push_global_confidence_to_github", lambda: None)


def _conf(active, score=80.0, label="🟢 Confiable", reasons=None):
    return {
        "global_score": score,
        "label": label,
        "kill_switch_active": active,
        "kill_switch_reasons": reasons or [],
    }


class TestPersistGlobalConfidence:

    def test_writes_state_file(self, tmp_path):
        global_conf = _conf(active=False)
        monitor.persist_global_confidence(global_conf, send_telegram=False)
        saved = json.loads((tmp_path / "system_confidence.json").read_text())
        assert saved["global_score"] == 80.0

    def test_first_run_inactive_does_not_alert(self, monkeypatch):
        """Sin estado previo (primera corrida con esto wireado), inactivo
        no debe disparar nada -- no hay transición real, fue siempre inactivo."""
        alerts = []
        monkeypatch.setattr(monitor, "_send_kill_switch_alert", lambda gc, recovered: alerts.append(recovered))
        monitor.persist_global_confidence(_conf(active=False), send_telegram=True)
        assert alerts == []

    def test_transition_inactive_to_active_sends_activation_alert(self, monkeypatch):
        alerts = []
        monkeypatch.setattr(monitor, "_send_kill_switch_alert", lambda gc, recovered: alerts.append(recovered))

        monitor.persist_global_confidence(_conf(active=False), send_telegram=True)
        monitor.persist_global_confidence(_conf(active=True, score=20.0, label="🔴 Crítica",
                                                  reasons=["score bajo"]), send_telegram=True)

        assert alerts == [False]  # recovered=False == alerta de activación

    def test_transition_active_to_inactive_sends_recovery_alert(self, monkeypatch):
        alerts = []
        monkeypatch.setattr(monitor, "_send_kill_switch_alert", lambda gc, recovered: alerts.append(recovered))

        # Seedear el estado previo "activo" en silencio (no es la transición
        # que este test quiere medir) y recién ahí encender las alertas.
        monitor.persist_global_confidence(_conf(active=True, score=20.0), send_telegram=False)
        monitor.persist_global_confidence(_conf(active=False, score=85.0), send_telegram=True)

        assert alerts == [True]  # recovered=True == alerta de recuperación

    def test_staying_active_across_multiple_runs_does_not_repeat_alert(self, monkeypatch):
        """El caso que más importa: 4 corridas seguidas con el kill switch
        activo deben generar UNA sola alerta (la de activación), no 4."""
        alerts = []
        monkeypatch.setattr(monitor, "_send_kill_switch_alert", lambda gc, recovered: alerts.append(recovered))

        monitor.persist_global_confidence(_conf(active=False), send_telegram=True)
        for _ in range(4):
            monitor.persist_global_confidence(_conf(active=True, score=10.0), send_telegram=True)

        assert alerts == [False]

    def test_send_telegram_false_never_alerts_even_on_transition(self, monkeypatch):
        alerts = []
        monkeypatch.setattr(monitor, "_send_kill_switch_alert", lambda gc, recovered: alerts.append(recovered))

        monitor.persist_global_confidence(_conf(active=False), send_telegram=False)
        monitor.persist_global_confidence(_conf(active=True, score=10.0), send_telegram=False)

        assert alerts == []


class TestSendKillSwitchAlertGuards:

    def test_no_telegram_credentials_does_not_raise(self, monkeypatch):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        # No debe lanzar excepción aunque no haya credenciales configuradas
        monitor._send_kill_switch_alert(_conf(active=True), recovered=False)
