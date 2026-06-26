"""
tests/test_portfolio_alerts.py

Primera cobertura de tracker.check_portfolio_alerts() -- no tenía ningún
test antes de esta sesión (26/06/2026), pese a ser la función que decide
si se manda una alerta de stop-loss/target por Telegram sobre dinero real.

Foco principal: el fix de moneda. Antes de este fix, "Stop loss manual" y
"Target manual" comparaban `precio_actual` (moneda NATIVA de la señal --
ARS para MERVAL/BOVESPA) contra `pos["stop_loss"]`/`pos["target"]` (que se
guardan en USD, ver risk_engine.compute_initial_stop_target). Esto nunca
se notó en producción porque stop_loss/target eran siempre None -- recién
ahora que se les asigna un valor real, el descalce de moneda importa.
"ATR stop"/"ATR target" sí están en moneda nativa de la señal en ambos
lados (no tienen este problema) y se cubren acá para no romperlos al lado.
"""
import json

import pytest

from src.tracker import check_portfolio_alerts
import src.tracker as tracker


def _signal(**overrides):
    base = {"ticker": "GGAL.BA", "precio_actual": 5100.0, "signal_v2": "🟢 COMPRA",
            "atr_stop": 0, "atr_target": 0, "max_12m": 0}
    base.update(overrides)
    return base


def _portfolio_with(pos: dict) -> dict:
    base = {
        "ticker": "GGAL.BA", "cantidad": 10,
        "precio_compra": 3.0, "precio_compra_usd": 3.0,
        "precio_actual_usd": 0, "stop_loss": None, "target": None,
    }
    base.update(pos)
    return {"positions": [base]}


@pytest.fixture
def with_portfolio(tmp_path, monkeypatch):
    def _setup(pos: dict):
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps(_portfolio_with(pos)))
        monkeypatch.setattr(tracker, "PORTFOLIO_PATH", str(path))
    return _setup


class TestStopLossManualEnUSD:
    def test_stop_loss_en_usd_dispara_correctamente(self, with_portfolio):
        """Caso real post-fix: precio_actual_usd (3.0 -> 1.0) cruza el stop
        en USD (1.5) -- debe disparar la alerta."""
        with_portfolio({"precio_actual_usd": 1.0, "stop_loss": 1.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=1500.0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🔴 STOP LOSS" in tipos

    def test_stop_loss_no_dispara_si_precio_usd_sigue_arriba(self, with_portfolio):
        with_portfolio({"precio_actual_usd": 3.5, "stop_loss": 1.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=5300.0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🔴 STOP LOSS" not in tipos

    def test_fix_de_moneda_no_dispara_falso_positivo_por_precio_nativo_ars(self, with_portfolio):
        """ANTES del fix: comparaba precio_actual (ARS, ej. 5100) contra
        stop_loss (USD, ej. 1.5) -- 5100 <= 1.5 es False, así que de hecho
        nunca disparaba (quedaba mudo, no falso positivo) mientras el ARS
        sea mayor al stop USD. El riesgo real era el inverso: que JAMÁS
        dispare aunque el precio real haya caído por debajo del stop en
        USD, porque la comparación estaba en la moneda equivocada. Este
        test confirma que ahora SÍ dispara cuando corresponde en USD,
        incluso con un precio nativo ARS que sería gigante en términos
        absolutos comparado al stop USD."""
        with_portfolio({"precio_actual_usd": 1.2, "stop_loss": 1.5})
        # precio nativo ARS sigue siendo "grande" en términos absolutos
        alerts = check_portfolio_alerts([_signal(precio_actual=1800.0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🔴 STOP LOSS" in tipos


class TestTargetManualEnUSD:
    def test_target_en_usd_dispara_correctamente(self, with_portfolio):
        with_portfolio({"precio_actual_usd": 5.0, "target": 4.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=7500.0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🟢 TARGET" in tipos

    def test_target_no_dispara_si_no_se_alcanzo(self, with_portfolio):
        with_portfolio({"precio_actual_usd": 3.5, "target": 4.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=5300.0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🟢 TARGET" not in tipos


class TestAtrStopTargetEnMonedaNativa:
    """Estos SÍ están en la misma moneda en ambos lados (precio_actual de
    la señal vs atr_stop/atr_target de la señal) -- no deberían verse
    afectados por el fix de moneda de arriba."""

    def test_atr_stop_dispara_en_moneda_nativa(self, with_portfolio):
        with_portfolio({"precio_actual_usd": 3.5})  # USD sin tocar, no importa para este check
        alerts = check_portfolio_alerts([_signal(precio_actual=3590.0, atr_stop=3595.36)])
        tipos = [a["tipo"] for a in alerts]
        assert "🟠 ATR STOP" in tipos

    def test_atr_target_dispara_en_moneda_nativa(self, with_portfolio):
        with_portfolio({"precio_actual_usd": 3.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=3850.0, atr_target=3800.71)])
        tipos = [a["tipo"] for a in alerts]
        assert "🟢 ATR TARGET" in tipos

    def test_atr_en_cero_no_dispara_nada(self, with_portfolio):
        """Antes del fix de causa raíz en analyzer.py (_atr), atr_stop/
        atr_target eran 0.0 siempre -- confirmar que sigue sin disparar
        falsos positivos cuando de verdad no hay ATR disponible (ticker
        con muy pocos datos, por ejemplo)."""
        with_portfolio({"precio_actual_usd": 3.5})
        alerts = check_portfolio_alerts([_signal(precio_actual=100.0, atr_stop=0, atr_target=0)])
        tipos = [a["tipo"] for a in alerts]
        assert "🟠 ATR STOP" not in tipos
        assert "🟢 ATR TARGET" not in tipos
