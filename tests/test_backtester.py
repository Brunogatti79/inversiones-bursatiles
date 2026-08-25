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
    _ranking_quantile_breakdown_by_market,
    _best_ret,
    _confidence_calibration_table,
    _isotonic_calibration_curve,
    calibrated_win_probability,
    _aggregate_by,
    _aggregate_cross,
    _build_trades,
    _detect_split_horizon,
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


class TestRankingQuantileBreakdownByMarket:
    """
    FIX 25/08/2026 (auditoría con Claude): ranking_top_vs_rest GLOBAL
    puede mostrar una paradoja de Simpson si las escalas de ranking no
    son comparables entre mercados -- ranking alto puede correlacionar
    con mal resultado en el pool global aunque, DENTRO de cada mercado,
    ranking alto sí prediga mejor resultado. Caso real detectado: MERVAL/
    BOVESPA rankean más alto en promedio que SP500 pese a tener peor EV
    real, así que top_20pct global queda dominado por MERVAL/BOVESPA y
    bottom_20pct por SP500 -- sin que el modelo esté necesariamente
    ordenando mal DENTRO de ningún mercado.
    """

    def test_separa_por_mercado_correctamente(self):
        trades  = [_trade(ret_21d=float(i), ranking=i, mercado="MERVAL") for i in range(1, 11)]
        trades += [_trade(ret_21d=float(i), ranking=i, mercado="SP500") for i in range(1, 11)]
        result = _ranking_quantile_breakdown_by_market(trades)
        assert result["MERVAL"]["samples"] == 10
        assert result["SP500"]["samples"] == 10
        assert "BOVESPA" in result  # siempre presentes los 3, aunque samples=0

    def test_mercado_sin_datos_devuelve_samples_cero_no_kError(self):
        trades = [_trade(ret_21d=1.0, ranking=5, mercado="MERVAL")]
        result = _ranking_quantile_breakdown_by_market(trades)
        assert result["BOVESPA"]["samples"] == 0
        assert result["SP500"]["samples"] == 0

    def test_reproduce_paradoja_de_simpson_del_caso_real(self):
        """Caso real (verificado contra backtest_results.json 24/08/2026):
        globalmente, ranking alto = peor resultado (porque son señales
        MERVAL/BOVESPA, que rankean más alto en promedio pero rindieron
        peor). Pero DENTRO de MERVAL, ranking sí predice bien. El desglose
        por mercado tiene que mostrar esta segunda verdad, que
        _ranking_quantile_breakdown (global) no puede mostrar por
        diseño."""
        # MERVAL: ranking y retorno correlacionan perfecto (bien ordenado
        # DENTRO del mercado), pero con rankings altos (55-74) y retornos
        # todos negativos (mercado que cayó en el período)
        merval_trades = [
            _trade(ret_21d=-15.0 + i, ranking=55.0 + i, mercado="MERVAL")
            for i in range(20)
        ]
        # SP500: ranking más bajo en promedio (25-44) pero retornos
        # positivos (mercado que subió) y SIN relación fuerte con ranking
        sp500_trades = [
            _trade(ret_21d=5.0, ranking=25.0 + i, mercado="SP500")
            for i in range(20)
        ]
        trades = merval_trades + sp500_trades

        global_result = _ranking_quantile_breakdown(trades)
        by_market = _ranking_quantile_breakdown_by_market(trades)

        # Global: top20% (ranking alto) = MERVAL = peor EV que bottom20% (SP500)
        assert (global_result["top_20pct"]["expected_value"]
                < global_result["bottom_20pct"]["expected_value"])

        # Pero DENTRO de MERVAL, top20% (ranking más alto) debe tener
        # mejor EV que bottom20% -- el ordenamiento interno SÍ funciona.
        mv = by_market["MERVAL"]
        assert mv["top_20pct"]["expected_value"] > mv["bottom_20pct"]["expected_value"]


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


# ── Calibración probabilística + attribution engine (roadmap "Institucional PRO", 24/07/2026) ──

class TestIsotonicCalibrationCurve:
    """
    _isotonic_calibration_curve() responde al hallazgo real de esta sesión
    (confidence_calibration con monotona=False, quintil 4 rompiendo la
    tendencia: WR 33%/52%/50%/39%/62%) -- en vez de esperar más historia
    para que "se resuelva solo", ajusta una curva no-decreciente vía
    regresión isotónica, que es la herramienta correcta quando el problema
    es de FORMA de la relación, no de tamaño de muestra.
    """

    def test_pocas_muestras_devuelve_nota_no_crashea(self):
        trades = [{"confidence_score": 50, "ret_5d": 1.0}] * 10
        r = _isotonic_calibration_curve(trades, min_samples=30)
        assert r["status"] == "insuficiente_historia"
        assert r["samples"] == 10

    def test_curva_es_no_decreciente_incluso_con_dato_crudo_no_monotono(self):
        """Reproduce el hallazgo real: 5 grupos con WR 33/52/50/39/62% en
        confianza creciente -- el dato crudo NO es monótono (39 < 50), pero
        la curva calibrada debe serlo siempre, por construcción."""
        import random
        random.seed(42)
        rangos = [
            (28.3, 48.0, 0.33), (48.0, 51.1, 0.521), (51.1, 53.9, 0.50),
            (54.7, 64.8, 0.394), (64.8, 90.8, 0.624),
        ]
        trades = []
        for lo, hi, wr in rangos:
            for _ in range(94):
                conf = random.uniform(lo, hi)
                gano = random.random() < wr
                ret = random.uniform(0.5, 5.0) if gano else random.uniform(-5.0, -0.5)
                trades.append({"confidence_score": conf, "ret_5d": ret})

        r = _isotonic_calibration_curve(trades)
        assert r["status"] == "ok"
        ps = [p["p_ganar_calibrada"] for p in r["curva"]]
        assert all(ps[i] <= ps[i + 1] for i in range(len(ps) - 1)), \
            "la curva calibrada debe ser no-decreciente por construcción, aunque el dato crudo no lo sea"

    def test_todos_los_valores_iguales_no_crashea(self):
        """Sin varianza en confidence_score -- correlación indefinida, no
        debería romper el cálculo."""
        trades = [{"confidence_score": 50.0, "ret_5d": (1.0 if i % 2 == 0 else -1.0)}
                  for i in range(40)]
        r = _isotonic_calibration_curve(trades)
        assert r["status"] == "ok"
        assert r["correlacion_score_crudo_vs_resultado"] is None


class TestCalibratedWinProbability:

    def test_interpola_entre_puntos_de_la_curva(self):
        curva = [
            {"confidence_score": 30.0, "p_ganar_calibrada": 0.2},
            {"confidence_score": 70.0, "p_ganar_calibrada": 0.8},
        ]
        p = calibrated_win_probability(50.0, curva)
        assert abs(p - 0.5) < 0.01  # punto medio, interpolación lineal

    def test_clampea_fuera_de_rango(self):
        curva = [
            {"confidence_score": 30.0, "p_ganar_calibrada": 0.2},
            {"confidence_score": 70.0, "p_ganar_calibrada": 0.8},
        ]
        assert calibrated_win_probability(10.0, curva) == 0.2
        assert calibrated_win_probability(90.0, curva) == 0.8

    def test_curva_vacia_devuelve_none(self):
        assert calibrated_win_probability(50.0, []) is None


class TestAttributionByFactor:
    """factor_dominante ya se calculaba en analyzer.py (jul-2026) pero
    nunca había llegado al backtester -- sin esto no se puede responder
    '¿el modelo gana/pierde según qué factor domina la señal?'."""

    def test_distingue_factores_con_resultados_distintos(self):
        trades = (
            [{"factor_dominante": "macro", "ret_5d": 3.0} for _ in range(20)]
            + [{"factor_dominante": "tecnico", "ret_5d": -3.0} for _ in range(20)]
        )
        attrib = _aggregate_by(trades, "factor_dominante")
        assert attrib["macro"]["h5d"]["win_rate"] == 1.0
        assert attrib["tecnico"]["h5d"]["win_rate"] == 0.0

    def test_factor_dominante_ausente_cae_en_unknown(self):
        trades = [{"ret_5d": 1.0}] * 5  # sin factor_dominante en absoluto
        attrib = _aggregate_by(trades, "factor_dominante")
        assert "UNKNOWN" in attrib
        assert attrib["UNKNOWN"]["count"] == 5


class TestBuildTradesFactorDominante:
    """Confirma que _build_trades() ahora propaga factor_dominante desde
    signals_history.json hasta el trade final (antes no lo hacía)."""

    def test_factor_dominante_se_propaga_desde_signals_history(self):
        history = {
            "2026-07-01": [{
                "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL", "factor_dominante": "fundamental",
            }],
        }
        price_index = {
            "GGAL.BA": {f"2026-07-{d:02d}": 100.0 + d for d in range(1, 15)}
        }
        trades = _build_trades(history, ["2026-07-01"] + [f"2026-07-{d:02d}" for d in range(2, 15)], price_index)
        assert len(trades) == 1
        assert trades[0]["factor_dominante"] == "fundamental"

    def test_split_detectado_se_marca_en_el_trade(self):
        """Regresión del incidente real 03/08/2026: YPF/Mirgor split 1:10,
        precio_entry (pre-split) comparado contra precio futuro (post-split)
        sin ajustar daba retornos artificiales de ~-90%. Ver docstring de
        _detect_split_horizon()."""
        history = {
            "2026-08-01": [{
                "ticker": "YPFD.BA", "signal": "🟢 COMPRA", "precio": 81325.0,
                "atr_stop": 75000.0, "atr_target": 90000.0, "sector": "Energía",
                "mercado": "MERVAL",
            }],
        }
        # Split real: 03/08 el precio cae ~90% de golpe (evento corporativo)
        price_index = {
            "YPFD.BA": {
                "2026-08-01": 81325.0, "2026-08-02": 81325.0,
                "2026-08-03": 8105.0, "2026-08-04": 8105.0, "2026-08-05": 7840.0,
                "2026-08-06": 7835.0, "2026-08-07": 7775.0, "2026-08-08": 7775.0,
                "2026-08-09": 7775.0, "2026-08-10": 8105.0, "2026-08-11": 8000.0,
                "2026-08-12": 8010.0, "2026-08-13": 7990.0, "2026-08-14": 8020.0,
            }
        }
        sorted_dates = list(price_index["YPFD.BA"].keys())
        trades = _build_trades(history, sorted_dates, price_index)
        assert len(trades) == 1
        t = trades[0]
        assert t["split_detectado"] is True
        # Todos los horizontes (5/10/21d) deben quedar en None -- el split
        # ocurre en la primera observación futura (day 1), no hay ningún
        # precio "limpio" (pre-split) para ese trade después de la entrada.
        assert t["ret_5d"] is None
        assert t["ret_10d"] is None
        assert t["ret_21d"] is None

    def test_sin_split_no_se_marca_y_retornos_se_calculan_normal(self):
        history = {
            "2026-08-01": [{
                "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL",
            }],
        }
        price_index = {
            "GGAL.BA": {f"2026-08-{d:02d}": 100.0 + d for d in range(1, 15)}
        }
        sorted_dates = list(price_index["GGAL.BA"].keys())
        trades = _build_trades(history, sorted_dates, price_index)
        assert trades[0]["split_detectado"] is False
        assert trades[0]["ret_5d"] is not None

    def test_split_a_mitad_de_camino_trunca_solo_horizontes_posteriores(self):
        """Si el split ocurre entre 5d y 10d, ret_5d debe seguir siendo
        válido (todo pre-split), y ret_10d/ret_21d deben quedar en None."""
        history = {
            "2026-08-01": [{
                "ticker": "TEST.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL",
            }],
        }
        precios = {"2026-08-01": 100.0}
        base_date = 2
        # 7 dias organicos post-entry (index 0-6 -> day 1-7), split en day 8
        organicos = [101, 102, 101.5, 103, 102.5, 104, 103.5]
        for i, p in enumerate(organicos):
            precios[f"2026-08-{base_date+i:02d}"] = p
        # split (90% caida) en el 8vo dia futuro
        precios["2026-08-10"] = 10.35
        for i in range(11, 25):
            precios[f"2026-08-{i:02d}"] = 10.0 + (i % 3)
        sorted_dates = list(precios.keys())
        trades = _build_trades(history, sorted_dates, {"TEST.BA": precios})
        t = trades[0]
        assert t["split_detectado"] is True
        assert t["ret_5d"] is not None   # dia 5 < dia 8 (split) -- pre-split, OK
        assert t["ret_10d"] is None      # dia 10 > dia 8 (split) -- truncado

    def test_factor_dominante_ausente_cae_en_unknown_no_crashea(self):
        history = {
            "2026-07-01": [{
                "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL",
                # sin factor_dominante -- entrada "vieja"
            }],
        }
        price_index = {
            "GGAL.BA": {f"2026-07-{d:02d}": 100.0 + d for d in range(1, 15)}
        }
        trades = _build_trades(history, ["2026-07-01"] + [f"2026-07-{d:02d}" for d in range(2, 15)], price_index)
        assert trades[0]["factor_dominante"] == "UNKNOWN"


# ── cross_market_regime × EV (auditoría externa v20, 10/08/2026) ───────────
# cross_market.py calcula y persiste RISK_ON/RISK_OFF/NEUTRAL por señal
# desde hace semanas, pero nunca había llegado al backtester -- se usaba
# para ajustar el score hacia adelante, nunca para medir EV hacia atrás.

class TestCrossMarketRegimeEnBacktester:

    def test_build_trades_extrae_cross_market_regime(self):
        history = {
            "2026-07-01": [{
                "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL", "cross_market_regime": "RISK_ON",
            }],
        }
        price_index = {
            "GGAL.BA": {f"2026-07-{d:02d}": 100.0 + d for d in range(1, 15)}
        }
        trades = _build_trades(history, ["2026-07-01"] + [f"2026-07-{d:02d}" for d in range(2, 15)], price_index)
        assert trades[0]["cross_market_regime"] == "RISK_ON"

    def test_cross_market_regime_ausente_cae_en_unknown_no_crashea(self):
        """Mismo criterio que factor_dominante/consenso/confidence_label:
        entradas viejas de signals_history.json (previas a que
        cross_market.py empezara a persistir el campo) no deben
        interpretarse como un régimen inventado."""
        history = {
            "2026-07-01": [{
                "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "precio": 100.0,
                "atr_stop": 90.0, "atr_target": 120.0, "sector": "Financiero",
                "mercado": "MERVAL",
                # sin cross_market_regime -- entrada "vieja"
            }],
        }
        price_index = {
            "GGAL.BA": {f"2026-07-{d:02d}": 100.0 + d for d in range(1, 15)}
        }
        trades = _build_trades(history, ["2026-07-01"] + [f"2026-07-{d:02d}" for d in range(2, 15)], price_index)
        assert trades[0]["cross_market_regime"] == "UNKNOWN"

    def test_by_regime_separa_ev_por_regimen(self):
        trades = (
            [{"signal": "🟢 COMPRA", "cross_market_regime": "RISK_ON", "ret_5d": r}
             for r in (3.0, 2.0, -1.0, 1.5, 0.5)] +
            [{"signal": "🟢 COMPRA", "cross_market_regime": "NEUTRAL", "ret_5d": r}
             for r in (-3.0, -2.0, -1.5, -0.5, 1.0)]
        )
        by_regime = _aggregate_by(trades, "cross_market_regime")
        assert by_regime["RISK_ON"]["h5d"]["expected_value"] > 0
        assert by_regime["NEUTRAL"]["h5d"]["expected_value"] < 0

    def test_regime_x_signal_cruza_ambas_dimensiones(self):
        trades = [
            {"signal": "🟢 COMPRA", "cross_market_regime": "RISK_ON", "ret_5d": 2.0},
            {"signal": "🔴 VENTA",  "cross_market_regime": "RISK_ON", "ret_5d": -1.0},
        ]
        rxs = _aggregate_cross(trades, "cross_market_regime", "signal")
        assert "🟢 COMPRA" in rxs["RISK_ON"]
        assert "🔴 VENTA" in rxs["RISK_ON"]


# ── _detect_split_horizon: unitarios directos (auditoría v20, 10/08/2026) ──

class TestDetectSplitHorizon:

    def test_split_10a1_real_ypf(self):
        """Caso real de producción: YPFD.BA, entry 81325 (02/08), precio
        futuro 8105 (03/08) -- split 1:10, cambio -90.0%."""
        future = [8105.0, 8105.0, 7840.0]
        assert _detect_split_horizon(81325.0, future) == 0

    def test_sin_movimiento_organico_no_detecta_nada(self):
        future = [101.0, 99.0, 103.0, 97.5, 102.0]
        assert _detect_split_horizon(100.0, future) == len(future)

    def test_umbral_60pct_no_confunde_volatilidad_alta_real(self):
        """Un movimiento fuerte pero orgánico (ej. -40% en una crisis
        puntual de MERVAL) NO debe confundirse con un split -- el umbral
        es 60%, por debajo de cualquier split típico (2:1=-50% ya lo
        agarra en el limite, 3:1=-66%, 10:1=-90%) pero por encima de
        crashes orgánicos plausibles."""
        future = [70.0]  # -30% en un dia, fuerte pero no split
        assert _detect_split_horizon(100.0, future) == len(future)

    def test_split_2a1_no_alcanza_el_umbral_deliberadamente(self):
        """Split 2:1 da exactamente -50%, por DEBAJO del umbral de 60% --
        decisión deliberada: 60% evita falsos positivos con movimientos
        orgánicos fuertes pero reales (ej. -45% a -55% en un día por una
        noticia puntual), a costa de no capturar splits 2:1 exactos. Los
        2 casos reales confirmados en producción (YPF, Mirgor) fueron
        10:1 (-90%), muy por encima de cualquier umbral razonable."""
        future = [50.0]  # exactamente -50%, split 2:1 -- no se detecta
        assert _detect_split_horizon(100.0, future) == 1

    def test_entry_price_cero_o_negativo_no_crashea(self):
        assert _detect_split_horizon(0.0, [10.0, 20.0]) == 2
        assert _detect_split_horizon(-5.0, [10.0, 20.0]) == 2

    def test_future_prices_vacio_no_crashea(self):
        assert _detect_split_horizon(100.0, []) == 0

    def test_precio_none_en_la_lista_no_crashea(self):
        future = [None, 101.0, 99.0]
        assert _detect_split_horizon(100.0, future) == 3  # sin split real, no trunca
