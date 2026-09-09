"""
tests/test_regla_compra_validada.py

Pedido de Bruno (27/07/2026): "comprar cuando confianza X y Compra/Compra
Fuerte, siempre que la historia diga que se gana". A propósito NO se asume
que COMPRA FUERTE rinde igual que COMPRA solo porque ambas son señales de
compra -- se chequea la celda REAL de confidence_x_signal_x_mercado
(backtester.py) para cada combinación por separado.

FIX 09/09/2026 (hallazgo real, sesión con Bruno -- Paradoja de Simpson
confirmada en producción): la versión anterior de estos tests usaba
confidence_x_signal (celda GLOBAL, MERVAL+BOVESPA+SP500 mezclados). Esa
celda daba "Alta+Compra" como no_valida globalmente mientras que, segmentada
por mercado, SP500 mostraba EV +4.2% (n=24, p=0.0001) y MERVAL mostraba EV
-4.76% (n=324, p=0.0) -- resultados opuestos escondidos en un solo promedio.
_estado_regla_compra() ahora exige mercado explícito y usa
confidence_x_signal_x_mercado; además exige significancia estadística
(significativo_95) y diversidad de tickers (no concentracion_alta, >=
min_tickers) antes de afirmar "validada" o "no_valida" -- si no se cumple
todo junto, el estado es "sin_evidencia_solida" (no se muestra como opción
de compra, pero tampoco se descarta como mala).

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


def _cell(count, tickers_unicos, concentracion_alta=False, muestra_insuficiente=False, **horizontes):
    c = {
        "count": count, "muestra_insuficiente": muestra_insuficiente,
        "tickers_unicos": tickers_unicos, "concentracion_alta": concentracion_alta,
    }
    c.update(horizontes)
    return c


def _horiz(samples, win_rate, expected_value, significativo_95, p_value=0.01):
    return {
        "samples": samples, "win_rate": win_rate, "expected_value": expected_value,
        "significativo_95": significativo_95, "p_value": p_value,
    }


# Réplica de la forma real de confidence_x_signal_x_mercado (backtester.py):
# {confidence_label: {signal_v2: {mercado: celda}}}
CXSM_REAL = {
    "🟢 Alta": {
        "🟢 COMPRA": {
            # Caso real SP500 (06/09/2026): validada a h21d, n y significancia reales.
            "SP500": _cell(70, 13, h21d=_horiz(24, 0.792, 4.2, True, 0.0001),
                                    h10d=_horiz(36, 0.583, 2.44, True, 0.0275),
                                    h5d=_horiz(61, 0.475, 0.8, False, 0.1197)),
            # Caso real MERVAL: mismo label+señal, resultado real negativo.
            "MERVAL": _cell(324, 18, h21d=_horiz(175, 0.36, -2.8, True, 0.0),
                                      h10d=_horiz(385, 0.501, -0.14, False, 0.7077),
                                      h5d=_horiz(476, 0.468, -0.7, True, 0.0098)),
        },
        "⭐ COMPRA FUERTE": {
            # Caso real SP500: n=1, sin evidencia posible.
            "SP500": _cell(1, 1, h21d=None, h10d=None, h5d=None),
        },
    },
    "🟡 Media": {
        "🟢 COMPRA": {
            # Caso real SP500: ningún horizonte es significativo -> sin evidencia sólida.
            "SP500": _cell(58, 7, h21d=_horiz(31, 0.355, -2.2, False, 0.2854),
                                   h10d=_horiz(33, 0.697, -1.08, False, 0.616),
                                   h5d=_horiz(48, 0.625, 0.44, False, 0.6425)),
        },
    },
}


class TestEstadoReglaCompraLogicaPura:

    def test_alta_compra_sp500_con_evidencia_real_da_validada(self):
        """El caso que disparó el fix: Alta+Compra en SP500 tiene evidencia
        sólida y positiva -- debe dar validada usando h21d (el horizonte
        preferido), no una mezcla ni la celda global."""
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", CXSM_REAL)
        assert estado == "validada"
        assert detalle["horizonte"] == "h21d"
        assert detalle["n"] == 24
        assert detalle["ev"] == 4.2
        assert detalle["tickers_unicos"] == 13

    def test_misma_combinacion_en_merval_da_no_valida(self):
        """La MISMA etiqueta+señal (Alta+Compra) en MERVAL tiene evidencia
        real negativa -- tiene que dar no_valida, sin contaminar ni
        heredar nada del resultado de SP500. Este es el corazón del fix:
        dos mercados, misma combinación, estados opuestos y ambos correctos."""
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "MERVAL", CXSM_REAL)
        assert estado == "no_valida"
        assert detalle["horizonte"] == "h21d"
        assert detalle["ev"] == -2.8

    def test_compra_fuerte_alta_sp500_muestra_minima_da_sin_evidencia_solida(self):
        """n=1 en SP500 -- no hay ningún horizonte con datos, sin_evidencia_solida."""
        estado, detalle = _estado_regla_compra("⭐ COMPRA FUERTE", "🟢 Alta", "SP500", CXSM_REAL)
        assert estado == "sin_evidencia_solida"

    def test_media_compra_sp500_sin_significancia_da_sin_evidencia_solida(self):
        """Ningún horizonte es significativo_95 (aunque h5d tenga EV
        positivo) -- no debe leerse como validada solo por el signo, tiene
        que exigir significancia real."""
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟡 Media", "SP500", CXSM_REAL)
        assert estado == "sin_evidencia_solida"

    def test_neutral_no_aplica_sin_importar_mercado(self):
        estado, _ = _estado_regla_compra("🟡 NEUTRAL/ESPERAR", "🟢 Alta", "SP500", CXSM_REAL)
        assert estado == "no_aplica"

    def test_prioriza_h21d_sobre_h10d_y_h5d_cuando_todos_califican(self):
        """Si h21d tiene muestra+significancia, se usa h21d aunque h10d
        también califique -- nunca 'el que tenga más muestra'."""
        cxsm = {
            "🟢 Alta": {"🟢 COMPRA": {"SP500": _cell(
                200, 10,
                h21d=_horiz(20, 0.7, 3.0, True, 0.01),
                h10d=_horiz(150, 0.6, 5.0, True, 0.001),   # más muestra, pero NO debe ganarle a h21d
                h5d=_horiz(180, 0.55, 1.0, True, 0.02),
            )}}
        }
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", cxsm)
        assert estado == "validada"
        assert detalle["horizonte"] == "h21d"
        assert detalle["ev"] == 3.0

    def test_cae_a_h10d_si_h21d_no_tiene_muestra_suficiente(self):
        cxsm = {
            "🟢 Alta": {"🟢 COMPRA": {"BOVESPA": _cell(
                101, 15,
                h21d=_horiz(8, 0.5, 1.0, False, 0.4),     # muestra chica, no significativo
                h10d=_horiz(101, 0.769, 3.7, True, 0.0),
                h5d=_horiz(101, 0.7, 2.0, True, 0.0),
            )}}
        }
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "BOVESPA", cxsm)
        assert estado == "validada"
        assert detalle["horizonte"] == "h10d"
        assert detalle["ev"] == 3.7

    def test_concentracion_alta_da_sin_evidencia_solida_aunque_ev_sea_positivo(self):
        """Bruno: 'si la muestra es chica y no representativa, tampoco la
        ponemos como opción' -- un resultado sostenido por 1-2 tickers no
        cuenta como regla validada aunque el EV y la significancia estén ok."""
        cxsm = {
            "🟢 Alta": {"🟢 COMPRA": {"SP500": _cell(
                50, 2, concentracion_alta=True,
                h21d=_horiz(30, 0.9, 8.0, True, 0.0001),
            )}}
        }
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", cxsm)
        assert estado == "sin_evidencia_solida"

    def test_pocos_tickers_unicos_da_sin_evidencia_solida_aunque_no_marque_concentracion_alta(self):
        cxsm = {
            "🟢 Alta": {"🟢 COMPRA": {"SP500": _cell(
                50, 3, concentracion_alta=False,
                h21d=_horiz(30, 0.9, 8.0, True, 0.0001),
            )}}
        }
        estado, detalle = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", cxsm)
        assert estado == "sin_evidencia_solida"

    def test_muestra_insuficiente_explicita_da_sin_evidencia_solida(self):
        cxsm = {
            "🟢 Alta": {"🟢 COMPRA": {"SP500": _cell(
                3, 2, muestra_insuficiente=True,
                h5d=_horiz(2, 1.0, 5.0, False, None),
            )}}
        }
        estado, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", cxsm)
        assert estado == "sin_evidencia_solida"

    def test_sin_mercado_en_la_celda_da_sin_evidencia_solida_no_cae_a_global(self):
        """Si el mercado pedido no está en la celda (ej. archivo viejo,
        o mercado nuevo sin historia todavía), NUNCA debe caer a un
        promedio global -- eso reintroduciría la Paradoja de Simpson."""
        estado, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "BOVESPA_NUEVO", CXSM_REAL)
        assert estado == "sin_evidencia_solida"

    def test_sin_conf_x_signal_x_mercado_no_rompe(self):
        estado, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", {})
        assert estado == "sin_evidencia_solida"
        estado2, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", "SP500", None)
        assert estado2 == "sin_evidencia_solida"

    def test_confidence_label_none_no_rompe(self):
        estado, _ = _estado_regla_compra("🟢 COMPRA", None, "SP500", CXSM_REAL)
        assert estado == "sin_evidencia_solida"

    def test_mercado_none_no_rompe(self):
        estado, _ = _estado_regla_compra("🟢 COMPRA", "🟢 Alta", None, CXSM_REAL)
        assert estado == "sin_evidencia_solida"


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
            json.dump({"confidence_x_signal_x_mercado": CXSM_REAL}, f)

        merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
        return dict(
            index_stats={
                "merval": {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
                "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
                "sp500": {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
            },
            output_path=str(tmp_path / "dashboard.html"),
            run_date="09/09/2026 20:00",
            price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
        )

    def test_signals_quedan_enriquecidas_segun_su_propio_mercado(self, _args_con_backtest_real):
        """El mismo par (Alta, Compra) da estados opuestos según el mercado
        real de la señal -- prueba end-to-end de que ya no hay Paradoja de
        Simpson en el dashboard generado. Además, desde el fix del
        09/09/2026, verifica que signal_v2 (lo único que lee el resto del
        dashboard) queda REESCRITO a 'SIN CONFIRMAR' cuando no está
        validado -- no alcanza con el badge/orden, "si no está confirmado
        no debe informar ningún tipo de acción a seguir" (pedido explícito
        de Bruno)."""
        import re
        signals = [
            self._signal(ticker="KO", mercado="SP500", signal_v2="🟢 COMPRA", signal="🟢 COMPRA", confidence_label="🟢 Alta"),
            self._signal(ticker="AAA.BA", mercado="MERVAL", signal_v2="🟢 COMPRA", signal="🟢 COMPRA", confidence_label="🟢 Alta"),
            self._signal(ticker="BBB", mercado="SP500", signal_v2="⭐ COMPRA FUERTE", signal="⭐ COMPRA FUERTE", confidence_label="🟢 Alta"),
            self._signal(ticker="CCC", mercado="SP500", signal_v2="🟢 COMPRA", signal="🟢 COMPRA", confidence_label="🟡 Media"),
            self._signal(ticker="DDD.BA", mercado="MERVAL", signal_v2="🟡 NEUTRAL/ESPERAR", signal="🟡 NEUTRAL/ESPERAR", confidence_label="🟢 Alta"),
        ]
        generate_dashboard(signals=signals, **_args_con_backtest_real)
        html = open(_args_con_backtest_real["output_path"], encoding="utf-8").read()

        m = re.search(r"var SIGNALS = (\[.*?\]);", html, re.DOTALL)
        rendered = {s["ticker"]: s for s in json.loads(m.group(1))}

        # Estados (sin cambios respecto al fix anterior)
        assert rendered["KO"]["regla_compra_estado"] == "validada"
        assert rendered["AAA.BA"]["regla_compra_estado"] == "no_valida"
        assert rendered["BBB"]["regla_compra_estado"] == "sin_evidencia_solida"
        assert rendered["CCC"]["regla_compra_estado"] == "sin_evidencia_solida"
        assert rendered["DDD.BA"]["regla_compra_estado"] == "no_aplica"

        # signal_v2 (lo que efectivamente se muestra/filtra en TODO el
        # dashboard): validada mantiene el texto real, el resto queda
        # gateado -- y NUNCA con la palabra "COMPRA" en el texto nuevo.
        assert rendered["KO"]["signal_v2"] == "🟢 COMPRA"
        assert rendered["AAA.BA"]["signal_v2"] == "🔵 SIN CONFIRMAR ⚠️"
        assert "COMPRA" not in rendered["AAA.BA"]["signal_v2"]
        assert rendered["BBB"]["signal_v2"] == "🔵 SIN CONFIRMAR"
        assert "COMPRA" not in rendered["BBB"]["signal_v2"]
        assert rendered["CCC"]["signal_v2"] == "🔵 SIN CONFIRMAR"
        assert rendered["DDD.BA"]["signal_v2"] == "🟡 NEUTRAL/ESPERAR"  # no_aplica: sin cambios

        # El original queda trazable en un campo aparte, nunca perdido.
        assert rendered["AAA.BA"]["signal_v2_original"] == "🟢 COMPRA"
        assert rendered["BBB"]["signal_v2_original"] == "⭐ COMPRA FUERTE"

        # signal (V1) NUNCA se toca -- lo usa generate_excel() de forma
        # independiente, no es lo mismo que el estado de confirmación de V2.
        assert rendered["AAA.BA"]["signal"] == "🟢 COMPRA"
        assert rendered["BBB"]["signal"] == "⭐ COMPRA FUERTE"

    def test_oportunidades_excluye_automaticamente_lo_no_confirmado(self, _args_con_backtest_real):
        """Consecuencia directa del gate: como _build_oportunidades() filtra
        por 'COMPRA' in signal_v2, y signal_v2 ya no dice COMPRA para lo no
        confirmado, la tab Oportunidades (y el radar, que usa el mismo
        campo) los excluye SOLOS, sin necesitar un filtro aparte. Prueba
        que "no informar ninguna acción a seguir" se cumple de punta a
        punta, no solo en el badge."""
        signals = [
            self._signal(ticker="KO", mercado="SP500", signal_v2="🟢 COMPRA", signal="🟢 COMPRA", confidence_label="🟢 Alta"),
            self._signal(ticker="BBB", mercado="SP500", signal_v2="⭐ COMPRA FUERTE", signal="⭐ COMPRA FUERTE", confidence_label="🟢 Alta"),
        ]
        generate_dashboard(signals=signals, **_args_con_backtest_real)
        html = open(_args_con_backtest_real["output_path"], encoding="utf-8").read()

        assert "KO" in html          # validada: sigue apareciendo
        assert "SIN CONFIRMAR" in html  # el gate se aplicó a BBB

    def test_badge_js_presente_y_html_estructuralmente_intacto(self, _args_con_backtest_real):
        generate_dashboard(
            signals=[self._signal(mercado="SP500", confidence_label="🟢 Alta")],
            **_args_con_backtest_real,
        )
        html = open(_args_con_backtest_real["output_path"], encoding="utf-8").read()

        assert "_reglaBadge" in html
        assert html.rstrip().endswith("</html>")
        assert html.count("<div") == html.count("</div>")
        assert html.count("<script") == html.count("</script>")

    def test_sin_backtest_results_no_rompe(self, tmp_path, monkeypatch):
        """Si data/backtest_results.json no existe todavía (pipeline
        nuevo, primer día), el dashboard debe generarse igual -- todas las
        señales quedan en no_aplica/sin_evidencia_solida, nunca una excepción."""
        monkeypatch.chdir(tmp_path)
        merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
        generate_dashboard(
            signals=[self._signal(mercado="SP500", confidence_label="🟢 Alta")],
            index_stats={
                "merval": {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
                "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
                "sp500": {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
            },
            output_path=str(tmp_path / "dashboard.html"),
            run_date="09/09/2026 20:00",
            price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
        )
        html = open(tmp_path / "dashboard.html", encoding="utf-8").read()
        assert html.rstrip().endswith("</html>")
