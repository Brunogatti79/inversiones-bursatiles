"""
tests/test_quality_check.py

Primera suite de tests de src/quality_check.py (no tenía cobertura hasta
hoy). Cubre dos cosas:

  1. El refactor de severidad/categoría (Prioridad 4, roadmap externo,
     25/06/2026): cada check ahora declara "categoria" (data/model/signal)
     y "es_estructural" (bool) en su propio dict, en vez de inferirse por
     fuera comparando el nombre del check contra un string hardcodeado.

  2. Regresión explícita del incidente real de v7.0 (24/06/2026): el kill
     switch se activó por 8 tickers en desacuerdo V1/V2 (comportamiento
     esperado del modelo), no por datos rotos. Si alguien reintroduce esa
     clase de bug (ej. agrega un check crítico nuevo sin declarar
     es_estructural=False explícitamente, o vuelve a inferir por nombre),
     estos tests deben fallar.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.quality_check import validar_señales, inyectar_semaforo, generar_reporte_calidad, _tally


def _signal(**overrides):
    base = {
        "ticker": "GGAL.BA", "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA",
        "precio_actual": 150.0, "rsi": 50.0, "ret_anual": 10.0, "ret_mes": 2.0,
        "ret_sem": 1.0, "score_macro": 55.0, "score_fundamental": 60.0,
        "rr_ratio": 2.0, "relative_strength": 1.0, "atr_percentile": 50.0,
        "momentum_21d": 0, "max_12m": 0, "min_12m": 0,
    }
    base.update(overrides)
    return base


# ── CHECK 5 / 6 — el incidente real de v7.0 ─────────────────────────────

class TestEstructuralVsModelo:

    def test_v1_v2_contradiccion_no_es_estructural(self):
        """Regresión directa del incidente 24/06/2026: V1 COMPRA / V2 VENTA
        es CRÍTICA pero NO estructural -- no debe poder gatillar el kill
        switch por sí sola."""
        q = validar_señales([_signal(signal="🟢 COMPRA", signal_v2="🟠 VENTA PARCIAL")])
        resumen = q["resumen"]
        assert resumen["criticas"] == 1
        assert resumen["criticas_estructurales"] == 0

        check = q["alertas"]["GGAL.BA"][0]
        assert check["categoria"] == "model"
        assert check["es_estructural"] is False

    def test_precio_invalido_si_es_estructural(self):
        """Precio<=0 SÍ es un dato roto -- debe contar como estructural."""
        q = validar_señales([_signal(precio_actual=0)])
        resumen = q["resumen"]
        assert resumen["criticas"] == 1
        assert resumen["criticas_estructurales"] == 1

        check = q["alertas"]["GGAL.BA"][0]
        assert check["categoria"] == "data"
        assert check["es_estructural"] is True

    def test_indice_sin_datos_es_estructural(self):
        q = validar_señales([_signal()], index_stats={"merval": {"actual": 0}})
        resumen = q["resumen"]
        assert resumen["criticas_estructurales"] == 1
        check = q["alertas"]["INDICE_MERVAL"][0]
        assert check["es_estructural"] is True
        assert check["categoria"] == "data"

    def test_incidente_real_8_tickers_v1v2_mas_1_precio_roto(self):
        """Reproduce exactamente el escenario de producción del 24/06: 8
        tickers en desacuerdo V1/V2 (esperado) + 1 con dato realmente roto.
        Solo el segundo debe activar el trigger duro del kill switch."""
        signals = [
            _signal(ticker=t, signal="🟢 COMPRA", signal_v2="🟠 VENTA PARCIAL")
            for t in ["JNJ", "KO", "GS", "GE", "UNH", "BAC", "JPM", "XOM"]
        ]
        signals.append(_signal(ticker="BAD.BA", precio_actual=0))

        q = validar_señales(signals)
        resumen = q["resumen"]
        assert resumen["criticas"] == 9
        assert resumen["criticas_estructurales"] == 1  # SOLO BAD.BA

    def test_check_sin_es_estructural_falla_cerrado(self):
        """_tally() debe defaultear a es_estructural=False si un check
        nuevo se agrega sin declarar el campo -- fallar cerrado (no suma al
        kill switch) es la opción segura, no fallar abierto."""
        resumen = {"criticas": 0, "criticas_estructurales": 0, "advertencias": 0,
                   "por_categoria": {"data": 0, "model": 0, "signal": 0}}
        _tally(resumen, {"nivel": "critical", "check": "check nuevo sin metadata"})
        assert resumen["criticas"] == 1
        assert resumen["criticas_estructurales"] == 0


# ── Categorización ───────────────────────────────────────────────────────

class TestPorCategoria:

    def test_resumen_incluye_conteo_por_categoria(self):
        q = validar_señales([_signal(precio_actual=0, signal_v2="🔴 VENTA")])
        assert "por_categoria" in q["resumen"]
        assert q["resumen"]["por_categoria"]["data"] >= 1   # precio inválido
        assert q["resumen"]["por_categoria"]["model"] >= 1  # V1 vs V2

    def test_todas_las_categorias_son_validas(self):
        """Ningún check debe colarse con una categoría fuera del taxonomy
        data/model/signal -- si se agrega un check nuevo sin categoría
        válida, este test debe notarlo."""
        signals = [_signal(
            ticker="X", precio_actual=0, signal="🟢 COMPRA", signal_v2="🔴 VENTA",
            rsi=80, ret_anual=-60, rr_ratio=10, atr_percentile=5,
            relative_strength=0.5, stress_index=80, score_macro=0,
            score_fundamental=50, upside_graham=None, score_cuant=None,
            ret_sem=20, max_12m=100, min_12m=50,
        )]
        q = validar_señales(signals)
        for checks in q["alertas"].values():
            for c in checks:
                assert c["categoria"] in {"data", "model", "signal"}
                assert isinstance(c["es_estructural"], bool)


# ── Sin alertas → todo OK ────────────────────────────────────────────────

class TestSinAlertas:

    def test_señal_limpia_no_genera_checks(self):
        q = validar_señales([_signal()])
        resumen = q["resumen"]
        assert resumen["total_alertas"] == 0
        assert resumen["ok"] == 1
        assert resumen["nivel_global"] == "✅ OK"
        assert "GGAL.BA" not in q["alertas"]


# ── Semáforo (Nivel 2) ───────────────────────────────────────────────────

class TestInyectarSemaforo:

    def test_sin_checks_queda_verde(self):
        signals = [_signal()]
        q = validar_señales(signals)
        out = inyectar_semaforo(signals, q)
        assert out[0]["quality_flag"] == "🟢"
        assert out[0]["quality_alerts"] == []

    def test_critical_pinta_rojo(self):
        signals = [_signal(precio_actual=0)]
        q = validar_señales(signals)
        out = inyectar_semaforo(signals, q)
        assert out[0]["quality_flag"] == "🔴"

    def test_solo_warning_pinta_amarillo(self):
        signals = [_signal(score_macro=44.0)]  # check 7: warning
        q = validar_señales(signals)
        out = inyectar_semaforo(signals, q)
        assert out[0]["quality_flag"] == "🟡"

    def test_solo_info_sigue_verde(self):
        signals = [_signal(rr_ratio=10.0)]  # check 8: info
        q = validar_señales(signals)
        out = inyectar_semaforo(signals, q)
        assert out[0]["quality_flag"] == "🟢"
        assert len(out[0]["quality_alerts"]) == 1


# ── Reporte Telegram (Nivel 3) ───────────────────────────────────────────

class TestGenerarReporteCalidad:

    def test_todo_ok_no_genera_reporte(self):
        q = validar_señales([_signal()])
        assert generar_reporte_calidad(q) is None

    def test_con_alertas_genera_texto_con_secciones(self):
        signals = [_signal(precio_actual=0), _signal(ticker="OTRO.BA", score_macro=44.0)]
        q = validar_señales(signals)
        report = generar_reporte_calidad(q)
        assert report is not None
        assert "CRÍTICAS" in report
        assert "ADVERTENCIAS" in report
        assert "GGAL.BA" in report
        assert "OTRO.BA" in report

    def test_trunca_listas_largas(self):
        signals = [_signal(ticker=f"T{i}.BA", precio_actual=0) for i in range(15)]
        q = validar_señales(signals)
        report = generar_reporte_calidad(q)
        assert "más" in report
