"""
tests/test_regla_compra_validada.py

Pedido de Bruno (27/07/2026): "comprar cuando confianza Alta y Compra/Compra
Fuerte, siempre que la historia diga que se gana". A propósito NO se asume
que COMPRA FUERTE rinde igual que COMPRA solo porque ambas son señales de
compra -- se chequea la celda REAL de confidence_x_signal (backtester.py
v4.14) para cada combinación por separado.

Estos tests cubren _estado_regla_compra() (lógica pura) y su integración
end-to-end en generate_dashboard() (badge ✅/🔵/⚠️ en la celda de Conf.).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pandas as pd
import pytest

from src.generator import generate_dashboard, _estado_regla_compra


CONF_X_SIGNAL_REAL = {
    "🟢 Alta": {
        "🟢 COMPRA": {
            "count": 60, "muestra_insuficiente": False,
            "h5d":  {"samples": 41, "win_rate": 0.634, "expected_value": 0.95},
            "h10d": {"samples": 14, "win_rate": 0.929, "expected_value": 2.74},
        },
    },
    "🟡 Media": {
        "🟢 COMPRA": {
            "count": 70, "muestra_insuficiente": False,
            "h5d":  {"samples": 59, "win_rate": 0.492, "expected_value": -0.61},
            "h10d": {"samples": 24, "win_rate": 0.583, "expected_value": -2.37},
        },
    },
}


class TestEstadoReglaCompraLogicaPura:

    def test_compra_mas_alta_con_muestra_real_da_validada(self):
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", CONF_X_SIGNAL_REAL)
        assert estado == "validada"
        assert detalle["n"] == 41  # h5d tiene más muestra que h10d -> se usa h5d
        assert detalle["ev"] == 0.95

    def test_compra_fuerte_mas_alta_sin_datos_da_sin_evidencia(self):
        """El caso real de hoy: 0 casos de COMPRA FUERTE + Alta en el
        historial -- no está en el dict de conf_x_signal en absoluto."""
        estado, detalle = _estado_regla_compra("⭐ COMPRA FUERTE", "🟢 Alta", CONF_X_SIGNAL_REAL)
        assert estado == "sin_evidencia"

    def test_compra_mas_media_no_aplica(self):
        """La regla es específicamente sobre confianza Alta -- Media no
        cuenta, aunque también sea COMPRA."""
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟡 Media", CONF_X_SIGNAL_REAL)
        assert estado == "no_aplica"

    def test_neutral_mas_alta_no_aplica(self):
        """La regla es específicamente sobre señales de compra -- NEUTRAL
        no cuenta, aunque la confianza sea Alta."""
        estado, detalle = _estado_regla_compra("🟡 NEUTRAL/ESPERAR", "🟢 Alta", CONF_X_SIGNAL_REAL)
        assert estado == "no_aplica"

    def test_combinacion_con_resultado_historico_negativo_da_no_valida(self):
        """Si algún día una combinación Alta+Compra/Compra Fuerte tuviera
        EV negativo con muestra real suficiente, debe marcarse no_valida,
        NO validada -- la regla depende del signo real, no de la etiqueta."""
        conf_negativo = {
            "🟢 Alta": {
                "⭐ COMPRA FUERTE": {
                    "count": 20, "muestra_insuficiente": False,
                    "h5d": {"samples": 18, "win_rate": 0.3, "expected_value": -1.5},
                    "h10d": {"samples": 5, "win_rate": 0.2, "expected_value": -2.0},
                },
            },
        }
        estado, detalle = _estado_regla_compra("⭐ COMPRA FUERTE", "🟢 Alta", conf_negativo)
        assert estado == "no_valida"
        assert detalle["ev"] == -1.5

    def test_muestra_insuficiente_explicita_da_sin_evidencia_aunque_haya_celda(self):
        conf_chico = {
            "🟢 Alta": {
                "🟢 COMPRA": {
                    "count": 3, "muestra_insuficiente": True,
                    "h5d": {"samples": 2, "win_rate": 1.0, "expected_value": 5.0},
                    "h10d": None,
                },
            },
        }
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", conf_chico)
        assert estado == "sin_evidencia"

    def test_sin_conf_x_signal_no_rompe(self):
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", {})
        assert estado == "sin_evidencia"
        estado2, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", None)
        assert estado2 == "sin_evidencia"

    def test_confidence_label_none_no_rompe(self):
        """Una señal sin confidence_label calculado (ej. bug viejo, o
        campo faltante) no debe romper -- simplemente no_aplica."""
        estado, _ = _estado_regla_compra("🟢 COMPRA", None, CONF_X_SIGNAL_REAL)
        assert estado == "no_aplica"


class TestBadgeEndToEndEnDashboard:

    def _signal(self, **overrides):
        base = {
            "ticker": "GGAL.BA", "empresa": "Grupo Galicia", "mercado": "MERVAL",
            "sector": "Financiero", "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA",
            "score_final": 65.0, "score_final_v2": 62.0, "precio_actual": 150.0,
            "ret_sem": 1.2, "ret_mes": 3.0, "ret_anual": 10.0, "rsi": 55.0,
            "max_12m": 180.0, "min_12m": 100.0, "rr_ratio": 2.0, "volatility_score": 50.0,
            "pred_5d": 1.0, "pred_21d": 2.0, "pred_signal": "📈 SUBA", "pred_confidence": 0.6,
            "pred_direction_agree": True, "atr_stop": 140.0, "atr_target": 170.0,
            "quality_flag": "🟢", "quality_detail": "Datos consistentes", "quality_alerts": [],
            "asset_quality": 60.0, "entry_score": 55.0, "ranking_accionable": 61.0,
        }
        base.update(overrides)
        return base

    @pytest.fixture
    def _args_con_backtest_real(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        os.makedirs("data", exist_ok=True)
        with open("data/backtest_results.json", "w") as f:
            json.dump({"confidence_x_signal": CONF_X_SIGNAL_REAL}, f)

        merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
        return dict(
            index_stats={
                "merval": {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
                "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
                "sp500": {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
            },
            output_path=str(tmp_path / "dashboard.html"),
            run_date="27/07/2026 20:00",
            price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
        )

    def test_signals_quedan_enriquecidas_con_los_4_estados(self, _args_con_backtest_real):
        import re
        signals = [
            self._signal(ticker="AAA.BA", signal_v2="🟢 COMPRA", confidence_label="🟢 Alta"),
            self._signal(ticker="BBB.BA", signal_v2="⭐ COMPRA FUERTE", confidence_label="🟢 Alta"),
            self._signal(ticker="CCC.BA", signal_v2="🟢 COMPRA", confidence_label="🟡 Media"),
            self._signal(ticker="DDD.BA", signal_v2="🟡 NEUTRAL/ESPERAR", confidence_label="🟢 Alta"),
        ]
        generate_dashboard(signals=signals, **_args_con_backtest_real)
        html = open(_args_con_backtest_real["output_path"], encoding="utf-8").read()

        m = re.search(r"var SIGNALS = (\[.*?\]);", html, re.DOTALL)
        rendered = {s["ticker"]: s["regla_compra_estado"] for s in json.loads(m.group(1))}

        assert rendered["AAA.BA"] == "validada"
        assert rendered["BBB.BA"] == "sin_evidencia"
        assert rendered["CCC.BA"] == "no_aplica"
        assert rendered["DDD.BA"] == "no_aplica"

    def test_badge_js_presente_y_html_estructuralmente_intacto(self, _args_con_backtest_real):
        generate_dashboard(signals=[self._signal(confidence_label="🟢 Alta")], **_args_con_backtest_real)
        html = open(_args_con_backtest_real["output_path"], encoding="utf-8").read()

        assert "_reglaBadge" in html
        assert html.rstrip().endswith("</html>")
        assert html.count("<div") == html.count("</div>")
        assert html.count("<script") == html.count("</script>")

    def test_sin_backtest_results_no_rompe(self, tmp_path, monkeypatch):
        """Si data/backtest_results.json no existe todavía (pipeline
        nuevo, primer día), el dashboard debe generarse igual -- todas las
        señales quedan en no_aplica/sin_evidencia, nunca una excepción."""
        monkeypatch.chdir(tmp_path)
        merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
        generate_dashboard(
            signals=[self._signal(confidence_label="🟢 Alta")],
            index_stats={
                "merval": {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
                "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
                "sp500": {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
            },
            output_path=str(tmp_path / "dashboard.html"),
            run_date="27/07/2026 20:00",
            price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
        )
        html = open(tmp_path / "dashboard.html", encoding="utf-8").read()
        assert html.rstrip().endswith("</html>")
