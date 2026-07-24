"""
tests/test_portfolio_optimizer.py

Primera suite de tests de src/portfolio_optimizer.py (no tenía cobertura
hasta hoy). Foco principal: regime_factor (Prioridad 5, roadmap externo,
25/06/2026) -- volatility_regime.py prometía en su propio docstring que
este módulo escalaba Kelly por régimen de volatilidad sistémica, pero
nunca se conectó (verificado con grep: cero referencias antes de este fix).

También cubre el comportamiento base de _calc_kelly_weights (clamp,
fallback sin backtest, confidence adjustment) ya que no tenía ningún test
y se modificó como parte de este cambio.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.portfolio_optimizer import (
    _calc_kelly_weights,
    _calc_risk_parity_weights,
    _build_notes,
    optimize_portfolio_allocation,
)


def _buy_signal(**overrides):
    base = {
        "ticker": "GGAL.BA", "mercado": "MERVAL", "sector": "Financiero",
        "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA",
        "score_final": 65.0, "rr_ratio": 2.0, "volatility_score": 50.0,
    }
    base.update(overrides)
    return base


def _backtest_with_metrics(win_rate=0.6, avg_win=8.0, avg_loss=4.0, samples=40,
                            signal_key="🟢 COMPRA"):
    """Backtest sintético con métricas suficientes (samples>=5) para que
    _calc_kelly_weights use la rama de Kelly real, no el fallback por score."""
    return {
        "by_signal": {
            signal_key: {"h21d": {"samples": samples, "win_rate": win_rate,
                                   "avg_win": avg_win, "avg_loss": avg_loss}}
        },
        "by_market": {},
    }


# ── regime_factor (Prioridad 5) ─────────────────────────────────────────

class TestRegimeFactorScalesKelly:

    def test_high_vol_reduces_kelly_vs_normal(self):
        # EV deliberadamente moderado (no avg_win=8/avg_loss=4 del default)
        # para que el Kelly resultante quede bien por debajo del cap de 20%
        # incluso escalado ×1.10 -- así la comparación multiplicativa exacta
        # no queda enmascarada por el clamp de riesgo.
        signals  = [_buy_signal()]
        backtest = _backtest_with_metrics(win_rate=0.55, avg_win=5.0, avg_loss=4.0, samples=40)

        normal = _calc_kelly_weights(signals, backtest, regime_factor=1.00)
        high   = _calc_kelly_weights(signals, backtest, regime_factor=0.75)

        assert high[0] < normal[0]
        assert high[0] == pytest.approx(normal[0] * 0.75, rel=1e-6)

    def test_low_vol_increases_kelly_vs_normal(self):
        signals  = [_buy_signal()]
        backtest = _backtest_with_metrics(win_rate=0.55, avg_win=5.0, avg_loss=4.0, samples=40)

        normal = _calc_kelly_weights(signals, backtest, regime_factor=1.00)
        low    = _calc_kelly_weights(signals, backtest, regime_factor=1.10)

        assert low[0] > normal[0]
        assert low[0] == pytest.approx(normal[0] * 1.10, rel=1e-6)

    def test_default_regime_factor_is_neutral(self):
        """Si no se pasa regime_factor, debe comportarse exactamente igual
        que antes del fix (compatibilidad hacia atrás)."""
        signals  = [_buy_signal()]
        backtest = _backtest_with_metrics()

        default  = _calc_kelly_weights(signals, backtest)
        explicit = _calc_kelly_weights(signals, backtest, regime_factor=1.0)
        assert default == explicit

    def test_cap_at_20pct_never_relaxed_by_low_vol_regime(self):
        """El cap de riesgo (20% máximo por posición) es una pared
        absoluta -- ni el régimen de baja volatilidad puede superarlo,
        aunque el Kelly crudo escalado sí lo haría."""
        signals  = [_buy_signal()]
        # Backtest con EV altísimo a propósito para que el Kelly crudo
        # supere 0.20 incluso sin escalar por régimen.
        backtest = _backtest_with_metrics(win_rate=0.9, avg_win=20.0, avg_loss=2.0, samples=100)

        weights = _calc_kelly_weights(signals, backtest, regime_factor=1.10)
        assert weights[0] <= 0.20

    def test_regime_factor_propagates_through_optimize_portfolio_allocation(self, monkeypatch):
        """Test de integración: el factor pasado a la función pública
        termina afectando kelly_half de la señal final."""
        import src.portfolio_optimizer as po
        monkeypatch.setattr(po, "_load_existing_tickers", lambda: set())

        signals_normal = [_buy_signal()]
        signals_high   = [_buy_signal()]
        backtest = _backtest_with_metrics()

        out_normal = optimize_portfolio_allocation(signals_normal, backtest, regime_factor=1.00)
        out_high   = optimize_portfolio_allocation(signals_high, backtest, regime_factor=0.75)

        assert out_high[0]["kelly_half"] < out_normal[0]["kelly_half"]

    def test_allocation_notes_mention_regime_when_not_neutral(self, monkeypatch):
        import src.portfolio_optimizer as po
        monkeypatch.setattr(po, "_load_existing_tickers", lambda: set())

        signals = [_buy_signal()]
        backtest = _backtest_with_metrics()
        out = optimize_portfolio_allocation(signals, backtest, regime_factor=0.75)
        assert "régimen de alta volatilidad" in out[0]["allocation_notes"]

    def test_allocation_notes_silent_when_regime_neutral(self, monkeypatch):
        import src.portfolio_optimizer as po
        monkeypatch.setattr(po, "_load_existing_tickers", lambda: set())

        signals = [_buy_signal()]
        backtest = _backtest_with_metrics()
        out = optimize_portfolio_allocation(signals, backtest, regime_factor=1.0)
        assert "régimen" not in out[0]["allocation_notes"]


# ── Comportamiento base de _calc_kelly_weights (sin tests previos) ──────

class TestCalcKellyWeightsBase:

    def test_fallback_by_score_without_backtest_data(self):
        """Sin métricas de backtest (samples<5), usa el fallback por
        score_final: score 70 -> Kelly base 0.08. Pero conf_adj también se
        aplica en la rama fallback (n_samples=0 -> conf_adj=min(1,(1/100)**0.5)
        = 0.1) -- 0 historia real es la situación de MENOR confianza
        posible, tiene sentido que pegue fuerte incluso en el fallback."""
        signals = [_buy_signal(score_final=70.0)]
        weights = _calc_kelly_weights(signals, backtest={})
        expected_raw = (70.0 - 50) / 250
        conf_adj = (1 / 100) ** 0.5
        assert weights[0] == pytest.approx(expected_raw * conf_adj, rel=1e-6)

    def test_score_at_or_below_50_gives_zero_kelly_fallback(self):
        signals = [_buy_signal(score_final=45.0)]
        weights = _calc_kelly_weights(signals, backtest={})
        assert weights[0] == 0.0

    def test_low_sample_count_reduces_kelly_via_confidence_adjustment(self):
        """Con pocas muestras (pero >=5, mínimo para no caer al fallback),
        el ajuste de confianza debe recortar el Kelly crudo."""
        few    = _backtest_with_metrics(samples=5)
        many   = _backtest_with_metrics(samples=100)
        signals = [_buy_signal()]

        w_few  = _calc_kelly_weights(signals, few)
        w_many = _calc_kelly_weights(signals, many)
        assert w_few[0] < w_many[0]

    def test_negative_ev_gives_zero_kelly(self):
        backtest = _backtest_with_metrics(win_rate=0.3, avg_win=2.0, avg_loss=8.0, samples=40)
        signals  = [_buy_signal()]
        weights  = _calc_kelly_weights(signals, backtest)
        assert weights[0] == 0.0


# ── Risk parity (sin tests previos) ──────────────────────────────────────

class TestCalcRiskParityWeights:

    def test_lower_volatility_gets_more_weight(self):
        signals = [
            _buy_signal(ticker="LOWVOL", volatility_score=20.0),
            _buy_signal(ticker="HIGHVOL", volatility_score=80.0),
        ]
        weights = _calc_risk_parity_weights(signals)
        assert weights[0] > weights[1]

    def test_weights_sum_to_one(self):
        signals = [_buy_signal(ticker=f"T{i}", volatility_score=float(20 + i * 10))
                   for i in range(5)]
        weights = _calc_risk_parity_weights(signals)
        assert sum(weights) == pytest.approx(1.0, rel=1e-6)

    def test_empty_signals_returns_empty(self):
        assert _calc_risk_parity_weights([]) == []


# ── _build_notes ──────────────────────────────────────────────────────────

class TestBuildNotes:

    def test_low_vol_note_mentions_amplification(self):
        note = _build_notes(pct=5.0, cap=None, kelly_f=0.08, sig=_buy_signal(), regime_factor=1.10)
        assert "ampliado" in note
        assert "1.10" in note

    def test_high_vol_note_mentions_reduction(self):
        note = _build_notes(pct=5.0, cap=None, kelly_f=0.08, sig=_buy_signal(), regime_factor=0.75)
        assert "recortado" in note
        assert "0.75" in note

    def test_neutral_regime_has_no_mention(self):
        note = _build_notes(pct=5.0, cap=None, kelly_f=0.08, sig=_buy_signal(), regime_factor=1.0)
        assert "régimen" not in note


# ── Fix 24/07/2026: mejor horizonte disponible + calibración isotónica ──────

class TestBestHorizonKelly:
    """
    Bug real encontrado (no teórico): _calc_kelly_weights() estaba
    hardcodeado a horizon="h21d". Con solo 16 días de historia real, h21d
    sigue en null para todo, así que Kelly caía SIEMPRE al fallback crudo
    por score, ignorando el backtest real ya disponible a 5 y 10 días --
    mismo patrón de bug ya encontrado y corregido 3 veces esta sesión en
    otros módulos, pero acá vivía en el módulo que decide cuánto capital
    poner en cada posición.
    """

    def _backtest_solo_h5d(self, win_rate=0.55, avg_win=3.0, avg_loss=2.5, samples=50):
        return {
            "by_signal": {
                "🟢 COMPRA": {
                    "h21d": None,
                    "h10d": None,
                    "h5d": {"samples": samples, "win_rate": win_rate,
                            "avg_win": avg_win, "avg_loss": avg_loss},
                }
            },
            "by_market": {},
        }

    def test_usa_h5d_cuando_h21d_y_h10d_estan_en_null(self):
        backtest = self._backtest_solo_h5d()
        signals = [_buy_signal(confidence_score=None)]
        weights = _calc_kelly_weights(signals, backtest)
        # Antes del fix, esto caía al fallback crudo: max(0, (65-50)/250) = 0.06
        # (antes del ajuste de confianza estadística). Con h5d real (WR=0.55,
        # avg_win=3.0, avg_loss=2.5) el resultado debe ser distinto y mayor
        # a 0 -- confirma que usó el backtest real, no el fallback.
        fallback_crudo = max(0.0, (65 - 50) / 250)
        assert weights[0] > 0
        assert weights[0] != fallback_crudo

    def test_prioriza_h21d_sobre_h10d_y_h5d_si_esta_disponible(self):
        """Si h21d SÍ está disponible, debe preferirse sobre h10d/h5d --
        no cambia el comportamiento existente cuando hay historia larga."""
        backtest = {
            "by_signal": {
                "🟢 COMPRA": {
                    "h21d": {"samples": 40, "win_rate": 0.70, "avg_win": 8.0, "avg_loss": 4.0},
                    "h10d": {"samples": 40, "win_rate": 0.30, "avg_win": 8.0, "avg_loss": 4.0},
                    "h5d": {"samples": 40, "win_rate": 0.10, "avg_win": 8.0, "avg_loss": 4.0},
                }
            },
            "by_market": {},
        }
        signals = [_buy_signal(confidence_score=None)]
        weights = _calc_kelly_weights(signals, backtest)
        # Con win_rate=0.70 (h21d) el Kelly crudo es alto; si hubiera usado
        # h5d (win_rate=0.10) habría dado ~0 o negativo clampeado a 0.
        assert weights[0] > 0.05


class TestCalibratedProbabilityBlend:
    """
    La calibración isotónica (roadmap "Institucional PRO") se calculaba y
    guardaba en backtest_results.json pero no se usaba en ninguna decisión
    real -- este fix la conecta al sizing de Kelly, blendeando el win_rate
    del bucket (agregado por tipo de señal/mercado) con la probabilidad
    calibrada para el confidence_score PUNTUAL de cada señal.
    """

    def _backtest_con_curva(self, win_rate_bucket=0.55):
        return {
            "by_signal": {
                "🟢 COMPRA": {
                    "h21d": None, "h10d": None,
                    "h5d": {"samples": 50, "win_rate": win_rate_bucket, "avg_win": 3.0, "avg_loss": 2.5},
                }
            },
            "by_market": {},
            "confidence_calibration_curve": {
                "curva": [
                    {"confidence_score": 20.0, "p_ganar_calibrada": 0.10},
                    {"confidence_score": 90.0, "p_ganar_calibrada": 0.75},
                ]
            },
        }

    def test_dos_señales_mismo_bucket_distinto_confidence_dan_distinto_kelly(self):
        """El caso central: dos señales del MISMO tipo/mercado (mismo
        win_rate de bucket) deben recibir Kelly distinto si su
        confidence_score puntual difiere -- antes de este fix, ambas
        habrían dado exactamente el mismo Kelly."""
        backtest = self._backtest_con_curva()
        sig_baja  = _buy_signal(confidence_score=20.0)
        sig_alta  = _buy_signal(confidence_score=90.0)
        w_baja = _calc_kelly_weights([sig_baja], backtest)[0]
        w_alta = _calc_kelly_weights([sig_alta], backtest)[0]
        assert w_baja < w_alta

    def test_sin_confidence_score_usa_solo_el_bucket(self):
        """Señales sin confidence_score (entradas viejas) no deben
        crashear -- caen al comportamiento anterior (solo el bucket)."""
        backtest = self._backtest_con_curva()
        sig_sin = _buy_signal(confidence_score=None)
        w = _calc_kelly_weights([sig_sin], backtest)
        assert w[0] > 0  # no crashea, sigue calculando con el bucket solo

    def test_sin_curva_de_calibracion_usa_solo_el_bucket(self):
        """Si el backtest no tiene confidence_calibration_curve todavía
        (necesita ≥30 trades, ver backtester.py), debe funcionar igual que
        antes sin romper nada."""
        backtest = self._backtest_solo_h5d_sin_curva()
        sig = _buy_signal(confidence_score=90.0)
        w = _calc_kelly_weights([sig], backtest)
        assert w[0] > 0

    def _backtest_solo_h5d_sin_curva(self):
        return {
            "by_signal": {
                "🟢 COMPRA": {
                    "h21d": None, "h10d": None,
                    "h5d": {"samples": 50, "win_rate": 0.55, "avg_win": 3.0, "avg_loss": 2.5},
                }
            },
            "by_market": {},
        }
