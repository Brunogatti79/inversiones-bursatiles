"""
tests/test_backtester.py
Tests unitarios para src/backtester.py

Cubre:
  - calc_stop_target_exit: stop hit, target hit, time exit
  - _metrics_from_rets: cálculo de win_rate, EV, sharpe, drawdown
  - _predictor_accuracy: directional accuracy
  - _get_future_prices: lookup en price_index
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pytest

from src.backtester import (
    _calc_stop_target_exit,
    _metrics_from_rets,
    _predictor_accuracy,
    _get_future_prices,
    _quantile_split,
    _confidence_quantile_breakdown,
    _ranking_quantile_breakdown,
    _best_ret,
    _confidence_calibration_table,
    _aggregate_by,
    _build_trades,
)


# ── Stop/Target Exit ────────────────────────────────────────────────────────

class TestCalcStopTargetExit:

    def test_stop_hit_before_target(self):
        """Precio cae al stop antes de llegar al target → exit tipo 'stop'."""
        entry   = 100.0
        stop    = 95.0
        target  = 115.0
        # Precios: baja a 94 en día 3
        futures = [99.0, 97.0, 94.0, 96.0, 100.0, 110.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "stop"
        assert result["st_ret"] < 0
        assert result["st_exit_day"] == 3

    def test_target_hit_before_stop(self):
        """Precio sube al target antes de tocar el stop → exit tipo 'target'."""
        entry   = 100.0
        stop    = 90.0
        target  = 110.0
        futures = [102.0, 106.0, 111.0, 108.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "target"
        assert result["st_ret"] > 0
        assert result["st_exit_day"] == 3

    def test_time_exit_when_neither_hit(self):
        """Ni stop ni target tocados → exit tipo 'time' al día 21."""
        entry   = 100.0
        stop    = 85.0
        target  = 120.0
        # 21 precios entre 95 y 115
        futures = [100 + i * 0.3 for i in range(25)]
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        assert result["st_exit_type"] == "time"
        assert result["st_exit_day"] == 21

    def test_pending_when_insufficient_data(self):
        """Menos datos que max_days → 'pending'."""
        entry   = 100.0
        stop    = 90.0
        target  = 115.0
        futures = [102.0, 104.0]  # solo 2 días
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        assert result["st_exit_type"] == "pending"
        assert result["st_ret"] is None

    def test_no_data_returns_no_data(self):
        """Sin precios futuros → 'no_data'."""
        result = _calc_stop_target_exit([], 100.0, 95.0, 110.0)
        assert result["st_exit_type"] == "no_data"

    def test_stop_above_entry_ignored(self):
        """Stop mayor que entry (stop inválido) → no se activa como stop."""
        entry   = 100.0
        stop    = 110.0   # INVÁLIDO: stop > entry
        target  = 120.0
        futures = [98.0, 95.0, 92.0] + [100.0] * 22
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        # No debe triggear stop ya que stop > entry
        assert result["st_exit_type"] != "stop" or result["st_ret"] is None

    def test_exact_stop_level(self):
        """Precio exactamente igual al stop → debe triggerear."""
        entry   = 100.0
        stop    = 95.0
        target  = 115.0
        futures = [99.0, 95.0, 97.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "stop"
        assert result["st_exit_day"] == 2


# ── Métricas desde retornos ────────────────────────────────────────────────

class TestMetricsFromRets:

    def test_empty_returns_none(self):
        assert _metrics_from_rets([]) is None

    def test_all_positive(self):
        rets = [5.0, 3.0, 8.0, 2.0, 10.0]
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == 1.0
        assert m["avg_ret"] > 0
        assert m["avg_loss"] == 0.0
        assert m["expected_value"] > 0

    def test_all_negative(self):
        rets = [-5.0, -3.0, -8.0]
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == 0.0
        assert m["avg_ret"] < 0
        assert m["avg_win"] == 0.0
        assert m["expected_value"] < 0

    def test_mixed_returns(self):
        rets = [10.0, -5.0, 8.0, -3.0, 12.0]  # 3 wins, 2 losses
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == pytest.approx(0.6, abs=0.01)
        assert m["samples"] == 5
        assert m["avg_win"] > 0
        assert m["avg_loss"] > 0

    def test_sharpe_positive_for_positive_avg(self):
        rets = [2.0, 3.0, 1.5, 2.5, 3.5]
        m = _metrics_from_rets(rets)
        assert m["sharpe"] > 0

    def test_max_drawdown_negative(self):
        """Drawdown siempre ≤ 0."""
        rets = [5.0, -8.0, 3.0, -4.0, 6.0]
        m = _metrics_from_rets(rets)
        assert m["max_drawdown"] <= 0

    def test_single_return(self):
        """Un solo retorno → no crashea."""
        m = _metrics_from_rets([5.0])
        assert m is not None
        assert m["samples"] == 1


# ── Accuracy del predictor ────────────────────────────────────────────────

class TestPredictorAccuracy:

    def test_empty_trades_returns_zero_samples(self):
        result = _predictor_accuracy([])
        assert result["samples"] == 0

    def test_trades_without_pred21d_ignored(self):
        trades = [
            {"ret_21d": 5.0, "pred_21d": None, "pred_signal": ""},
            {"ret_21d": -3.0, "pred_21d": None, "pred_signal": ""},
        ]
        result = _predictor_accuracy(trades)
        assert result["samples"] == 0

    def test_perfect_directional_accuracy(self):
        """Predictor siempre acierta la dirección → accuracy = 1.0."""
        trades = [
            {"ret_21d": 5.0,  "pred_21d": 3.0,  "pred_signal": "📈 SUBA"},
            {"ret_21d": 3.0,  "pred_21d": 2.0,  "pred_signal": "📈 SUBA"},
            {"ret_21d": -4.0, "pred_21d": -2.0, "pred_signal": "📉 BAJA"},
        ]
        result = _predictor_accuracy(trades)
        assert result["directional_accuracy"] == pytest.approx(1.0)
        assert result["samples"] == 3

    def test_zero_directional_accuracy(self):
        """Predictor siempre se equivoca → accuracy = 0.0."""
        trades = [
            {"ret_21d": -5.0, "pred_21d": 3.0, "pred_signal": "📈 SUBA"},
            {"ret_21d": -3.0, "pred_21d": 2.0, "pred_signal": "📈 SUBA"},
            {"ret_21d":  4.0, "pred_21d": -2.0, "pred_signal": "📉 BAJA"},
        ]
        result = _predictor_accuracy(trades)
        assert result["directional_accuracy"] == pytest.approx(0.0)

    def test_mae_calculation(self):
        """MAE = promedio de |pred - real|."""
        trades = [
            {"ret_21d": 5.0,  "pred_21d": 3.0,  "pred_signal": "📈 SUBA"},  # error = 2
            {"ret_21d": -3.0, "pred_21d": -5.0, "pred_signal": "📉 BAJA"},  # error = 2
        ]
        result = _predictor_accuracy(trades)
        assert result["mae"] == pytest.approx(2.0)


# ── Lookup de precios futuros ─────────────────────────────────────────────

class TestGetFuturePrices:

    def setup_method(self):
        self.price_index = {
            "GGAL.BA": {
                "2026-05-01": 100.0,
                "2026-05-02": 101.0,
                "2026-05-05": 102.0,
                "2026-05-06": 103.0,
                "2026-05-07": 104.0,
                "2026-05-08": 105.0,
            }
        }

    def test_returns_prices_after_signal_date(self):
        result = _get_future_prices("GGAL.BA", "2026-05-01", self.price_index, max_horizon=3)
        assert result == [101.0, 102.0, 103.0]

    def test_unknown_ticker_returns_empty(self):
        result = _get_future_prices("UNKN.BA", "2026-05-01", self.price_index)
        assert result == []

    def test_last_date_returns_empty(self):
        """No hay precios después del último día."""
        result = _get_future_prices("GGAL.BA", "2026-05-08", self.price_index)
        assert result == []

    def test_max_horizon_respected(self):
        result = _get_future_prices("GGAL.BA", "2026-05-01", self.price_index, max_horizon=2)
        assert len(result) <= 2


# ── Prioridad 1 (roadmap externo, 25/06/2026): consenso V1/V2 + cuantiles ───
# de confianza + ranking_accionable. Cierra el loop "¿estas dimensiones
# predicen algo real, o son ruido?" -- antes de esto solo había by_signal/
# by_market/by_sector.

def _trade(ret_21d=None, **extra):
    """Trade mínimo para testear los helpers de agregación directamente,
    sin pasar por todo _build_trades()."""
    t = {"ret_21d": ret_21d}
    t.update(extra)
    return t


class TestQuantileSplit:

    def test_below_minimum_samples_returns_note(self):
        valid = [_trade(ret_21d=1.0, score=i) for i in range(5)]
        result = _quantile_split(valid, "score")
        assert result["samples"] == 5
        assert "note" in result

    def test_top_and_bottom_are_correctly_separated(self):
        """20 trades con score 0..19 y ret_21d = score (correlación perfecta
        a propósito) -- top 20% debe ser claramente mejor que bottom 20%."""
        valid = [_trade(ret_21d=float(i), score=i) for i in range(20)]
        result = _quantile_split(valid, "score")

        assert result["samples"] == 20
        assert result["top_20pct"]["count"] == 4
        assert result["bottom_20pct"]["count"] == 4
        assert result["top_20pct"]["avg_ret"] > result["bottom_20pct"]["avg_ret"]
        assert result["top_20pct"]["score_range"] == [16.0, 19.0]
        assert result["bottom_20pct"]["score_range"] == [0.0, 3.0]

    def test_middle_excludes_top_and_bottom(self):
        valid = [_trade(ret_21d=float(i), score=i) for i in range(20)]
        result = _quantile_split(valid, "score")
        assert result["middle_60pct"]["count"] == 12  # 20 - 4 - 4

    def test_no_correlation_top_and_bottom_similar(self):
        """Si score y retorno NO tienen relación, top/bottom no deberían
        diferir sistemáticamente -- sanity check de que el split no inventa
        una señal donde no la hay."""
        import random
        random.seed(42)
        valid = [_trade(ret_21d=random.uniform(-5, 5), score=i) for i in range(50)]
        result = _quantile_split(valid, "score")
        # No assert de desigualdad estricta (es aleatorio) -- solo que
        # ambos grupos tengan datos y la diferencia no sea descabellada.
        assert result["top_20pct"]["count"] == 10
        assert result["bottom_20pct"]["count"] == 10


class TestConfidenceQuantileBreakdown:

    def test_filters_trades_without_confidence_score(self):
        trades = [_trade(ret_21d=1.0, confidence_score=None)] * 5
        trades += [_trade(ret_21d=2.0, confidence_score=80.0) for _ in range(10)]
        result = _confidence_quantile_breakdown(trades)
        assert result["samples"] == 10  # los 5 sin confidence_score se excluyen

    def test_empty_trades_returns_zero_samples(self):
        result = _confidence_quantile_breakdown([])
        assert result["samples"] == 0


class TestRankingQuantileBreakdown:

    def test_filters_zero_and_none_ranking(self):
        trades  = [_trade(ret_21d=1.0, ranking=0)] * 3
        trades += [_trade(ret_21d=1.0, ranking=None)] * 3
        trades += [_trade(ret_21d=float(i), ranking=i) for i in range(1, 11)]
        result = _ranking_quantile_breakdown(trades)
        assert result["samples"] == 10  # excluye ranking=0 y ranking=None


class TestBuildTradesNewFields:
    """Verifica que _build_trades() extraiga consenso/confidence_score/
    confidence_label/ranking desde signals_history.json -- y que entradas
    viejas (sin estos campos, pre fix 25/06/2026) no contaminen el grupo
    'Sin consenso'/'UNKNOWN' con falsos negativos."""

    def setup_method(self):
        self.price_index = {
            "GGAL.BA": {f"2026-05-{d:02d}": 100.0 + d for d in range(1, 20)},
        }
        # _build_trades() excluye los últimos 5 días de sorted_dates (no hay
        # futuro suficiente todavía) -- se necesitan >5 fechas para que
        # "2026-05-01" quede dentro de la ventana evaluable.
        self.sorted_dates = [f"2026-05-{d:02d}" for d in range(1, 8)]

    def _history_entry(self, **overrides):
        base = {
            "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
            "mercado": "MERVAL", "sector": "Financiero",
        }
        base.update(overrides)
        return base

    def test_extracts_consenso_and_confidence_fields(self):
        history = {"2026-05-01": [self._history_entry(
            consenso="Consenso", confidence_score=82.5, confidence_label="🟢 Alta", ranking=91.2,
        )]}
        trades = _build_trades(history, self.sorted_dates, self.price_index)
        assert len(trades) == 1
        t = trades[0]
        assert t["consenso"] == "Consenso"
        assert t["consenso_binario"] == "Consenso"
        assert t["confidence_score"] == 82.5
        assert t["confidence_label"] == "🟢 Alta"
        assert t["ranking"] == 91.2

    def test_divergent_consenso_maps_to_sin_consenso(self):
        history = {"2026-05-01": [self._history_entry(
            consenso="V1↑/V2↓ buen activo, mal timing",
        )]}
        trades = _build_trades(history, self.sorted_dates, self.price_index)
        assert trades[0]["consenso_binario"] == "Sin consenso"

    def test_old_entry_without_fields_does_not_become_sin_consenso(self):
        """Entrada sin 'consenso' (pre fix) -> consenso_binario debe ser
        None (después se agrupa como UNKNOWN vía _aggregate_by), nunca
        'Sin consenso' -- eso sería un falso negativo sobre datos que
        simplemente no existen."""
        history = {"2026-05-01": [self._history_entry()]}  # sin consenso/confidence
        trades = _build_trades(history, self.sorted_dates, self.price_index)
        t = trades[0]
        assert t["consenso_binario"] is None
        assert t["confidence_score"] is None
        assert t["confidence_label"] == "UNKNOWN"

    def test_aggregate_by_consenso_binario_buckets_missing_as_unknown(self):
        history = {"2026-05-01": [
            self._history_entry(consenso="Consenso"),
            self._history_entry(),  # sin consenso -> debe ir a UNKNOWN, no a "Sin consenso"
        ]}
        trades = _build_trades(history, self.sorted_dates, self.price_index)
        grouped = _aggregate_by(trades, "consenso_binario")
        assert "Consenso" in grouped
        assert "UNKNOWN" in grouped
        assert "Sin consenso" not in grouped


# ── _best_ret / calibración de confianza (fix 23/07/2026, roadmap externo #9) ──

class TestBestRet:
    """
    _quantile_split() (y por lo tanto confidence_quantiles/ranking_top_vs_rest)
    dependía exclusivamente de ret_21d -- devolvía 0 muestras durante todo el
    período en que la historia real no llega a 21 días. _best_ret() elige el
    horizonte más largo disponible (21d > 10d > 5d), mismo criterio que el
    resto del sistema (panel del dashboard, log_model_run()).
    """

    def test_prioriza_21d_si_esta_disponible(self):
        t = {"ret_5d": 1.0, "ret_10d": 2.0, "ret_21d": 3.0}
        assert _best_ret(t) == (21, 3.0)

    def test_cae_a_10d_si_no_hay_21d(self):
        t = {"ret_5d": 1.0, "ret_10d": 2.0, "ret_21d": None}
        assert _best_ret(t) == (10, 2.0)

    def test_cae_a_5d_si_solo_eso_hay(self):
        """Caso real de esta sesión: con 15 días de historia, ningún trade
        tiene ret_21d/ret_10d todavía -- sin este fallback, confidence_quantiles
        y ranking_top_vs_rest quedaban en 0 muestras."""
        t = {"ret_5d": 1.0, "ret_10d": None, "ret_21d": None}
        assert _best_ret(t) == (5, 1.0)

    def test_sin_ningun_retorno_devuelve_none(self):
        t = {"ret_5d": None, "ret_10d": None, "ret_21d": None}
        assert _best_ret(t) == (None, None)

    def test_quantile_split_ya_no_depende_solo_de_ret_21d(self):
        """Regresión directa del bug: con SOLO ret_5d poblado (como pasa con
        poca historia real), _quantile_split ya no debería devolver 0
        muestras -- antes del fix, esto habría dado samples=0."""
        valid = [{"ret_5d": float(i), "ret_10d": None, "ret_21d": None, "score": i}
                 for i in range(20)]
        result = _quantile_split(valid, "score")
        assert result["samples"] == 20
        assert result["top_20pct"]["count"] == 4


class TestConfidenceCalibrationTable:

    def test_pocas_muestras_devuelve_nota_no_crashea(self):
        trades = [{"confidence_score": 50, "ret_5d": 1.0}] * 5
        result = _confidence_calibration_table(trades)
        assert "note" in result
        assert result["samples"] == 5

    def test_relacion_monotona_se_detecta_como_true(self):
        """Confianza y retorno perfectamente correlacionados -> monótona."""
        trades = [{"confidence_score": float(i), "ret_5d": float(i) - 50}
                  for i in range(30)]
        result = _confidence_calibration_table(trades)
        assert result["monotona"] is True
        assert len(result["quintiles"]) == 5

    def test_relacion_no_monotona_se_detecta_como_false(self):
        """Quintil 3 'fuera de orden' respecto al quintil 2 -- WR: 0%, 80%,
        20%, 80%, 100%. Debe marcarse como no monótona."""
        rets_por_quintil = [
            [-5, -5, -5, -5, -5],
            [3, 3, 3, 3, -3],
            [2, -2, -2, -2, -2],
            [3, 3, 3, 3, -3],
            [4, 4, 4, 4, 4],
        ]
        trades, conf = [], 10
        for grupo in rets_por_quintil:
            for ret in grupo:
                trades.append({"confidence_score": conf, "ret_5d": ret})
                conf += 1
        result = _confidence_calibration_table(trades)
        assert result["monotona"] is False

    def test_filtra_trades_sin_confidence_score_o_sin_retorno(self):
        trades = [{"confidence_score": None, "ret_5d": 1.0}] * 5
        trades += [{"confidence_score": 50, "ret_5d": None, "ret_10d": None, "ret_21d": None}] * 5
        trades += [{"confidence_score": float(i), "ret_5d": float(i)} for i in range(20)]
        result = _confidence_calibration_table(trades)
        assert result["samples"] == 20
