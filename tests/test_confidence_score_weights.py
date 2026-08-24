"""
tests/test_confidence_score_weights.py

FIX 24/08/2026 (auditoría con Claude): peso del componente "predictor
confirma/contradice" bajado de 35% a 15% en _calc_confidence(), con los
20pts liberados redistribuidos proporcionalmente a los otros 4 componentes.
Motivo completo en el docstring de src/confidence_score.py y en
backtest_results.json (confidence_calibration.monotona=false,
predictor_contribution: confirma EV=-3.15% peor que contradice EV=-1.92%
a 21d, efecto persistente en ventanas de 30d y 45d).

Estos tests verifican:
  1. El máximo teórico de cada componente sigue sumando 100 (nadie se
     "come" puntos de más ni deja huecos).
  2. El componente predictor pesa 15, no 35 -- el cambio central de este
     fix.
  3. El caso extremo (todo perfecto) sigue dando 100, y el caso extremo
     (todo pésimo) sigue dando 0 -- los rangos no se rompieron con la
     redistribución.
  4. Que confirmar vs. contradecir el predictor ahora mueve el score menos
     que antes en términos absolutos (era la causa del problema: dominaba
     el score final).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.confidence_score import _calc_confidence


def _base_signal(**overrides):
    sig = {
        "signal_v2":        "🟢 COMPRA",
        "pred_21d":         None,
        "pred_confidence":  0.5,
        "alignment_label":  "SIN DATOS",
        "quality_flag":     "🟢",
        "score_final":      60.0,
        "score_final_v2":   60.0,
    }
    sig.update(overrides)
    return sig


class TestComponentWeights:

    def test_perfect_signal_hits_100(self):
        """Todo lo mejor posible en cada componente → score = 100."""
        sig = _base_signal(
            pred_21d=5.0, pred_confidence=1.0,           # predictor confirma con máxima confianza
            alignment_label="TRIPLE CONFIRMACIÓN",
            quality_flag="🟢",
            score_final=60.0, score_final_v2=61.0,        # diff < 10
        )
        score, label, _bd = _calc_confidence(sig, global_regime="LOW")
        assert score == 100.0
        assert label == "🟢 Alta"

    def test_worst_signal_hits_near_zero(self):
        """Todo lo peor posible en cada componente → score cercano a 0."""
        sig = _base_signal(
            pred_21d=-5.0, pred_confidence=1.0,           # COMPRA pero predictor dice fuerte BAJA
            alignment_label="CONFLICTO TOTAL",
            quality_flag="🔴",
            score_final=90.0, score_final_v2=10.0,        # diff > 30
        )
        score, label, _bd = _calc_confidence(sig, global_regime="HIGH")
        assert score < 10.0
        assert label == "🔴 Muy baja"

    def test_predictor_component_max_is_15_not_35(self):
        """El componente predictor, aislado (resto de componentes en su
        mínimo posible), no puede aportar más de 15 puntos -- antes del
        fix del 24/08/2026 aportaba hasta 35."""
        sig_confirma_fuerte = _base_signal(
            pred_21d=5.0, pred_confidence=1.0,
            alignment_label="CONFLICTO TOTAL",   # 0 pts
            quality_flag="🔴",                    # 4 pts
            score_final=90.0, score_final_v2=10.0,  # 1 pt
        )
        score, _, _bd = _calc_confidence(sig_confirma_fuerte, global_regime="HIGH")
        # vol HIGH = 4 pts. Resto mínimo = 0+4+1+4 = 9. Con predictor a
        # pleno (15) el total debe quedar en 24, nunca en 44 (que sería
        # el resultado si el predictor siguiera pesando 35).
        assert score == 24.0

    def test_confirma_vs_contradice_delta_is_smaller_than_before_fix(self):
        """La diferencia de score entre 'predictor confirma' y 'predictor
        contradice', manteniendo todo lo demás igual, debe ser <= 15pts
        (el nuevo máximo del componente), nunca los ~35pts de antes."""
        sig_confirma = _base_signal(pred_21d=5.0, pred_confidence=0.9)
        sig_contradice = _base_signal(pred_21d=-5.0, pred_confidence=0.9)
        score_confirma, _, _bd1 = _calc_confidence(sig_confirma, global_regime="NORMAL")
        score_contradice, _, _bd2 = _calc_confidence(sig_contradice, global_regime="NORMAL")
        delta = score_confirma - score_contradice
        assert 0 < delta <= 15.0

    def test_alignment_is_now_the_largest_single_component(self):
        """Alignment de timeframes (33) pasa a ser el componente de mayor
        peso individual, no el predictor (15)."""
        sig_solo_triple = _base_signal(
            pred_21d=None,                        # 6.4 pts (sin dato)
            alignment_label="TRIPLE CONFIRMACIÓN", # 33 pts
            quality_flag="🟡",                     # 16 pts
            score_final=60.0, score_final_v2=85.0,  # diff=25 -> 5 pts
        )
        score, _, _bd = _calc_confidence(sig_solo_triple, global_regime="NORMAL")
        # 6.4 + 33 + 16 + 5 + 9(NORMAL) = 69.4
        assert abs(score - 69.4) < 0.5

    def test_no_pred_data_contribution_scaled_proportionally(self):
        """Sin dato de predictor, la contribución parcial también se
        redujo proporcionalmente (15/35 * 15 ≈ 6.4), no se quedó en 15
        fijo (que hubiera sido más que el máximo posible del componente)."""
        sig = _base_signal(pred_21d=None)
        score, _, _bd = _calc_confidence(sig, global_regime="NORMAL")
        # SIN DATOS align=16, quality verde=26, diff<10=13, NORMAL=9
        # + predictor sin dato = 6.4
        assert abs(score - (6.4 + 16 + 26 + 13 + 9)) < 0.1


class TestBreakdownInstrumentation:
    """FIX 24/08/2026: antes de esto, alignment_label/quality_flag se
    usaban para calcular confidence_score pero no quedaban persistidos en
    ningún lado -- sin esto, un contrafactual retroactivo sobre los pesos
    no se puede reconstruir con precisión. Estos tests verifican que el
    breakdown expone exactamente lo que se usó para el cálculo."""

    def test_breakdown_sums_to_total_score(self):
        sig = _base_signal(
            pred_21d=3.0, pred_confidence=0.8,
            alignment_label="DOBLE CONFIRMACIÓN",
            quality_flag="🟡",
            score_final=60.0, score_final_v2=72.0,  # diff=12 -> 9 pts
        )
        score, _, bd = _calc_confidence(sig, global_regime="NORMAL")
        suma = (bd["predictor_pts"] + bd["alignment_pts"] + bd["quality_pts"]
                + bd["consistency_pts"] + bd["vol_pts"])
        assert abs(score - round(suma, 1)) < 0.15  # tolerancia por redondeos intermedios

    def test_breakdown_exposes_raw_alignment_and_quality(self):
        """alignment_label y quality_flag deben quedar en el breakdown tal
        cual se usaron, no solo los puntos derivados -- son justo los dos
        campos que hoy no se persisten en signals_history.json."""
        sig = _base_signal(alignment_label="CONFLICTO PARCIAL", quality_flag="🔴")
        _, _, bd = _calc_confidence(sig, global_regime="NORMAL")
        assert bd["alignment_label"] == "CONFLICTO PARCIAL"
        assert bd["quality_flag"] == "🔴"

    def test_breakdown_records_whether_predictor_confirmed(self):
        sig_confirma = _base_signal(pred_21d=2.0, pred_confidence=0.6)
        sig_contradice = _base_signal(pred_21d=-2.0, pred_confidence=0.6)
        sig_sin_dato = _base_signal(pred_21d=None)
        _, _, bd1 = _calc_confidence(sig_confirma, global_regime="NORMAL")
        _, _, bd2 = _calc_confidence(sig_contradice, global_regime="NORMAL")
        _, _, bd3 = _calc_confidence(sig_sin_dato, global_regime="NORMAL")
        assert bd1["predictor_confirma"] is True
        assert bd2["predictor_confirma"] is False
        assert bd3["predictor_confirma"] is None

    def test_enrich_confidence_scores_attaches_breakdown_to_signal(self):
        from src.confidence_score import enrich_confidence_scores
        signals = [_base_signal(pred_21d=1.0, pred_confidence=0.7)]
        enriched = enrich_confidence_scores(signals, vol_regime={"global_regime": "NORMAL"})
        assert "confidence_breakdown" in enriched[0]
        assert "predictor_pts" in enriched[0]["confidence_breakdown"]
