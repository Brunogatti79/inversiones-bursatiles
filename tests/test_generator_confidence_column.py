"""
tests/test_generator_confidence_column.py

Pedido de Bruno (27/07/2026): había una confusión real entre dos "confianzas"
distintas del dashboard -- pred_confidence (columna 🎯, solo la confianza del
predictor sobre su propio pronóstico de precio) y confidence_score (0-100,
5 factores, el que realmente determina las etiquetas Alta/Media/Baja/Muy
baja del panel "Por confianza del modelo"). No había forma de ver, por
ticker individual, en qué bucket caía -- y el panel agregado tampoco
mostraba los rangos numéricos de cada bucket, lo que invitaba a confundirlo
con la otra escala.

Este test cubre los 2 cambios de esa sesión:
  1. Columna nueva "Conf." en la tabla de señales (TBL_COLS + celda),
     mostrando confidence_label por ticker.
  2. Rangos numéricos reales agregados a las etiquetas del panel agregado
     (≥75 / 55-74 / 35-54 / <35 -- tomados de confidence_score.py, no
     inventados).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from src.generator import generate_dashboard


def _signal(**overrides):
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
        "confidence_score": 82.0, "confidence_label": "🟢 Alta",
    }
    base.update(overrides)
    return base


@pytest.fixture
def _basic_args(tmp_path):
    merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
    return dict(
        index_stats={
            "merval":  {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
            "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
            "sp500":   {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
        },
        output_path=str(tmp_path / "dashboard.html"),
        run_date="27/07/2026 20:00",
        price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
    )


def _html_structurally_intact(html: str) -> bool:
    return (
        html.rstrip().endswith("</html>")
        and html.count("<div") == html.count("</div>")
        and html.count("<script") == html.count("</script>")
    )


class TestConfidenceColumnInTable:

    def test_columna_conf_aparece_en_tbl_cols(self, _basic_args):
        generate_dashboard(signals=[_signal()], **_basic_args)
        html = open(_basic_args["output_path"], encoding="utf-8").read()
        assert _html_structurally_intact(html)
        assert "label:'Conf.'" in html
        assert "s.confidence_score" in html

    def test_celda_usa_confidence_label_con_fallback_seguro(self, _basic_args):
        generate_dashboard(signals=[_signal()], **_basic_args)
        html = open(_basic_args["output_path"], encoding="utf-8").read()
        assert "s.confidence_label?" in html
        # Fallback explícito cuando falta el dato (ticker sin confidence_label)
        assert "'<td style=\\\"color:#444\\\">" not in html  # no debe haber quedado el bug de doble-escape

    def test_tooltip_explica_los_5_factores_y_distingue_de_pred_confidence(self, _basic_args):
        generate_dashboard(signals=[_signal()], **_basic_args)
        html = open(_basic_args["output_path"], encoding="utf-8").read()
        assert "Confianza compuesta de la señal" in html
        assert "predictor+dirección" in html
        assert "NO es lo mismo que la columna" in html

    def test_no_rompe_con_senal_sin_confidence_score(self, _basic_args):
        """Una señal vieja/incompleta sin confidence_score no debe romper
        la generación del dashboard."""
        sig = _signal()
        del sig["confidence_score"]
        del sig["confidence_label"]
        generate_dashboard(signals=[sig], **_basic_args)
        html = open(_basic_args["output_path"], encoding="utf-8").read()
        assert _html_structurally_intact(html)


class TestConfidencePanelRanges:

    def test_panel_agregado_muestra_rangos_numericos_reales(self, _basic_args, monkeypatch, tmp_path):
        """Los rangos deben coincidir EXACTO con los umbrales hardcodeados
        en confidence_score.py (_label): ≥75 Alta, 55-74 Media, 35-54 Baja,
        <35 Muy baja -- si alguna vez se recalibran esos umbrales ahí, hay
        que actualizar este texto también (o mejor, leerlo de una constante
        compartida en vez de tenerlo repetido en 2 archivos)."""
        # Aislar el backtest_results.json real del repo -- este test no debe
        # depender de qué haya hoy en data/, solo de que el texto de rango
        # aparezca cuando SÍ hay datos de by_confidence_label.
        backtest_path = tmp_path.parent / "data_test_backtest.json"
        monkeypatch.chdir(tmp_path)
        os.makedirs("data", exist_ok=True)
        import json
        with open("data/backtest_results.json", "w") as f:
            json.dump({
                "by_confidence_label": {
                    "🟢 Alta": {"10d": {"samples": 11, "win_rate": 0.55, "expected_value": 0.7}},
                    "🟡 Media": {"10d": {"samples": 66, "win_rate": 0.59, "expected_value": 0.7}},
                    "🟠 Baja": {"10d": {"samples": 123, "win_rate": 0.46, "expected_value": -0.5}},
                    "🔴 Muy baja": {"10d": {"samples": 8, "win_rate": 0.50, "expected_value": -5.0}},
                }
            }, f)

        args = dict(_basic_args)
        args["output_path"] = str(tmp_path / "dashboard.html")
        generate_dashboard(signals=[_signal()], **args)
        html = open(args["output_path"], encoding="utf-8").read()

        assert "🟢 Confianza Alta (≥75)" in html
        assert "🟡 Confianza Media (55-74)" in html
        assert "🟠 Confianza Baja (35-54)" in html
        assert "🔴 Confianza Muy baja (&lt;35)" in html
