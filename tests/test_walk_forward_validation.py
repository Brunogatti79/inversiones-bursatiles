"""
tests/test_walk_forward_validation.py

Tests para _walk_forward_validate_patterns() (auditoría externa 28/07/2026,
punto 5 del roadmap: separar ventana de descubrimiento de ventana de
validación para los patrones de confidence_x_signal -- distinto del
walk-forward que ya tiene weight_optimizer.py, que valida PESOS no
PATRONES).

Estrategia de datos sintéticos: en vez de un único precio en tendencia
continua (que acopla el horizonte de retorno de las señales del final de
"descubrimiento" con el arranque de "validación" de forma difícil de
controlar), se usa un precio con un único quiebre de tendencia (sube hasta
el día `peak`, baja después) posicionado a propósito ANTES del corte de
validación -- así las señales de descubrimiento quedan limpiamente en la
suba y las de validación limpiamente en la baja, sin zona gris.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta

import pytest

from src.backtester import _walk_forward_validate_patterns


def _dates(n, start="2026-01-01"):
    base = datetime.strptime(start, "%Y-%m-%d")
    return [(base + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n)]


def _build_history_and_prices(n_signal_days, price_fn, ticker="TEST.BA",
                               confidence_label="🟢 Alta", signal="🟢 COMPRA",
                               mercado="MERVAL", extra_future_days=40):
    """
    Construye (history, sorted_dates, price_index) sintéticos.

    price_fn(day_idx) -> precio en ese día (día 0 = primera fecha).
    price_index cubre n_signal_days + extra_future_days días, para que
    ninguna señal (ni siquiera la última) se quede sin precios futuros por
    quedarse corto el índice -- eso sería un artefacto del test, no algo
    que se quiera medir acá.
    """
    all_dates = _dates(n_signal_days + extra_future_days)
    price_index = {ticker: {d: price_fn(i) for i, d in enumerate(all_dates)}}

    history = {}
    for i, d in enumerate(all_dates[:n_signal_days]):
        history[d] = [{
            "ticker": ticker, "mercado": mercado, "sector": "Financiero",
            "signal_v2": signal, "precio": price_fn(i),
            "confidence_label": confidence_label,
            "atr_stop": 0, "atr_target": 0,
        }]

    sorted_dates = sorted(history.keys())
    return history, sorted_dates, price_index


def _rising(day_idx):
    return 100 + day_idx


def _peak_then_falling(peak=30):
    def _fn(day_idx):
        if day_idx <= peak:
            return 100 + day_idx
        return 100 + peak - (day_idx - peak)
    return _fn


class TestWalkForwardBasics:

    def test_sin_datos_suficientes_total_muy_corto(self):
        history, sorted_dates, price_index = _build_history_and_prices(1, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index)
        assert result["status"] == "sin_datos_suficientes"
        assert result["combinaciones"] == {}

    def test_split_respeta_split_ratio_pedido(self):
        history, sorted_dates, price_index = _build_history_and_prices(40, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index,
                                                   split_ratio=0.5)
        assert result["status"] == "ok"
        assert result["n_dias_descubrimiento"] == 20
        assert result["n_dias_validacion"] == 20
        assert result["n_dias_total"] == 40


class TestWalkForwardCombinaciones:

    def test_confirmado_cuando_mismo_signo_en_ambas_ventanas(self):
        # Precio en suba continua -- discovery y validation deberían dar
        # EV positivo por igual, sin quiebre de tendencia.
        history, sorted_dates, price_index = _build_history_and_prices(40, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index)
        key = "🟢 Alta + 🟢 COMPRA"
        assert key in result["combinaciones"]
        combo = result["combinaciones"][key]
        assert combo["estado"] == "confirmado"
        assert combo["descubrimiento"]["ev"] > 0
        assert combo["validacion"]["ev"] > 0

    def test_no_confirmado_cuando_cambia_signo_fuera_de_muestra(self):
        # Quiebre de tendencia día 30 -- split 70% de 40 días = día 28,
        # así que "descubrimiento" (días 0-27) queda en la suba, y
        # "validación" (días 28-39) entra ya en la caída para casi todas
        # sus señales evaluables.
        history, sorted_dates, price_index = _build_history_and_prices(
            40, _peak_then_falling(peak=30)
        )
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index)
        key = "🟢 Alta + 🟢 COMPRA"
        assert key in result["combinaciones"]
        combo = result["combinaciones"][key]
        assert combo["descubrimiento"]["ev"] > 0
        assert combo["validacion"]["ev"] < 0
        assert combo["estado"] == "no_confirmado"

    def test_sin_datos_validacion_cuando_ventana_de_validacion_es_muy_corta(self):
        # 30 días totales, split 70/30 -> validación = 9 días. _build_trades
        # descarta los últimos 5 días de CUALQUIER ventana que reciba (no
        # hay futuro para evaluarlos) -- 9-5=4 señales evaluables, por
        # debajo de min_validation_samples=5 por defecto.
        history, sorted_dates, price_index = _build_history_and_prices(30, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index)
        key = "🟢 Alta + 🟢 COMPRA"
        assert key in result["combinaciones"]
        combo = result["combinaciones"][key]
        assert combo["estado"] == "sin_datos_validacion"
        assert combo["descubrimiento"]["n"] >= 15  # sí hay patrón que "descubrir"

    def test_combinacion_sin_muestra_en_discovery_no_aparece(self):
        # Muy pocos días de señales -> ni siquiera llega a min_samples=15 en
        # la ventana de descubrimiento. No debe aparecer en absoluto (no
        # tiene sentido "validar" algo que nunca se llegó a descubrir).
        history, sorted_dates, price_index = _build_history_and_prices(10, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index)
        assert result["combinaciones"] == {}

    def test_min_samples_parametrizable(self):
        history, sorted_dates, price_index = _build_history_and_prices(10, _rising)
        result = _walk_forward_validate_patterns(history, sorted_dates, price_index,
                                                   min_samples=2, min_validation_samples=1)
        key = "🟢 Alta + 🟢 COMPRA"
        # Con el umbral bajado, ahora sí debería alcanzar para aparecer
        assert key in result["combinaciones"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
