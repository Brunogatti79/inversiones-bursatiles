"""
tests/test_generator_banners.py

Primer test de src/generator.py (2590 líneas, 0 tests hasta hoy — el
módulo más grande y más frágil del repo, f-string único de Python donde
cualquier llave sin escapar rompe la compilación entera). Alcance
deliberadamente acotado: solo cubre el cambio de esta sesión (banners de
estado), no un intento de cubrir las 2590 líneas de una vez.

Hallazgo de esta sesión: validacion_banner se calculaba pero el HTML
final tenía un comentario literal `<!-- banner -->` en vez de la
interpolación `{validacion_banner}` -- el banner de validación de datos
NUNCA se mostró en el dashboard, en ninguna versión anterior. Corregido
junto con el agregado del banner nuevo (pesos V1 sintéticos + Exposure
Total), mismo bloque, mismo fix.
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
        "score_final": 65.0, "score_v2": 62.0, "precio_actual": 150.0,
        "ret_sem": 1.2, "ret_mes": 3.0, "ret_anual": 10.0, "rsi": 55.0,
        "max_12m": 180.0, "min_12m": 100.0, "rr_ratio": 2.0, "volatility_score": 50.0,
        "pred_5d": 1.0, "pred_21d": 2.0, "pred_signal": "📈 SUBA", "pred_confidence": 0.6,
        "pred_direction_agree": True, "atr_stop": 140.0, "atr_target": 170.0,
        "quality_flag": "🟢", "quality_detail": "Datos consistentes", "quality_alerts": [],
    }
    base.update(overrides)
    return base


@pytest.fixture
def _basic_args(tmp_path):
    merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
    return dict(
        signals=[_signal()],
        index_stats={
            "merval":  {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
            "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
            "sp500":   {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
        },
        output_path=str(tmp_path / "dashboard.html"),
        run_date="25/06/2026 20:00",
        price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
    )


def _html_structurally_intact(html: str) -> bool:
    """Sanity check barato (no un parser HTML completo): tags balanceados
    y cierre correcto del documento -- suficiente para detectar una llave
    de f-string mal escapada que rompió la estructura general."""
    return (
        html.rstrip().endswith("</html>")
        and html.count("<div") == html.count("</div>")
        and html.count("<script") == html.count("</script>")
    )


class TestValidacionBannerRendersForReal:
    """Regresión directa del hallazgo: antes de este fix, <!-- banner -->
    era un comentario HTML literal -- validacion_banner se calculaba pero
    nunca llegaba al archivo final."""

    def test_warning_banner_text_appears_in_output(self, _basic_args):
        validacion = {
            "nivel_global": "WARNING",
            "mercados": {
                "merval":  {"nivel": "OK", "ultima_fecha": "2026-06-24"},
                "bovespa": {"nivel": "WARNING", "ultima_fecha": "2026-06-24"},
                "sp500":   {"nivel": "OK", "ultima_fecha": "2026-06-24"},
            },
        }
        generate_dashboard(validacion=validacion, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Advertencia — Revisar frescura de datos" in html
        assert "BOVESPA" in html
        assert _html_structurally_intact(html)

    def test_ok_banner_shows_datos_ok(self, _basic_args):
        validacion = {
            "nivel_global": "OK",
            "mercados": {m: {"nivel": "OK", "ultima_fecha": "2026-06-24"}
                         for m in ("merval", "bovespa", "sp500")},
        }
        generate_dashboard(validacion=validacion, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Datos OK" in html

    def test_no_validacion_omits_banner_without_crashing(self, _basic_args):
        generate_dashboard(validacion=None, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Datos OK" not in html
        assert "Advertencia" not in html
        assert _html_structurally_intact(html)

    def test_literal_banner_comment_placeholder_is_gone(self, _basic_args):
        """El bug exacto: si esto vuelve a aparecer literal en el output,
        el placeholder volvió a romperse."""
        generate_dashboard(
            validacion={"nivel_global": "OK", "mercados": {}},
            **_basic_args,
        )
        html = open(_basic_args["output_path"]).read()
        assert "<!-- banner -->" not in html


class TestSystemStatusBanner:
    """Banner nuevo de esta sesión: pesos V1 sintéticos + Exposure Total."""

    def test_synthetic_weights_shown(self, _basic_args):
        wp = {
            "available": True, "is_synthetic": True, "mode": "sensitivity", "days_history": 3,
            "markets": {m: {"is_synthetic": True} for m in ("MERVAL", "BOVESPA", "SP500")},
        }
        generate_dashboard(weights_provenance=wp, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Pesos V1 sintéticos" in html
        assert "sensitivity" in html
        assert _html_structurally_intact(html)

    def test_non_synthetic_weights_not_shown(self, _basic_args):
        wp = {"available": True, "is_synthetic": False, "markets": {}}
        generate_dashboard(weights_provenance=wp, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Pesos V1 sintéticos" not in html

    def test_reduced_exposure_shown(self, _basic_args):
        exposure = {"exposure_factor": 0.842, "confidence_component": 0.842, "regime_component": 1.0}
        generate_dashboard(exposure=exposure, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Exposure Total: 84%" in html
        assert _html_structurally_intact(html)

    def test_full_exposure_not_shown(self, _basic_args):
        exposure = {"exposure_factor": 1.0, "confidence_component": 1.0, "regime_component": 1.0}
        generate_dashboard(exposure=exposure, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Exposure Total:" not in html

    def test_both_synthetic_and_reduced_exposure_shown_together(self, _basic_args):
        wp = {"available": True, "is_synthetic": True, "mode": "sensitivity", "days_history": 3,
              "markets": {"MERVAL": {"is_synthetic": True}}}
        exposure = {"exposure_factor": 0.75, "confidence_component": 0.75, "regime_component": 1.0}
        generate_dashboard(weights_provenance=wp, exposure=exposure, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Pesos V1 sintéticos" in html
        assert "Exposure Total: 75%" in html
        assert _html_structurally_intact(html)

    def test_neither_present_means_no_banner_and_no_crash(self, _basic_args):
        generate_dashboard(weights_provenance=None, exposure=None, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Pesos V1 sintéticos" not in html
        assert "Exposure Total:" not in html
        assert _html_structurally_intact(html)

    def test_kill_switch_zero_exposure_shown_as_0pct(self, _basic_args):
        exposure = {"exposure_factor": 0.0, "confidence_component": 0.0, "regime_component": 1.0}
        generate_dashboard(exposure=exposure, **_basic_args)
        html = open(_basic_args["output_path"]).read()
        assert "Exposure Total: 0%" in html


class TestBackwardCompatibility:

    def test_call_without_new_params_still_works(self, _basic_args):
        """Llamada en el estilo viejo (sin weights_provenance/exposure)
        no debe romper -- son parámetros opcionales nuevos."""
        result = generate_dashboard(**_basic_args)
        assert os.path.exists(_basic_args["output_path"])
        html = open(_basic_args["output_path"]).read()
        assert _html_structurally_intact(html)
