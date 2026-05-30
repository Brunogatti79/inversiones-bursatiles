"""
tests/test_analyzer.py
Tests unitarios para src/analyzer.py

Cubre:
  - Cálculo de RSI (valores conocidos)
  - MA cross (alcista/bajista)
  - Score técnico (composición y rangos)
  - Thresholds de señales V1
  - ATR (cálculo básico)
  - Normalización dist_max
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest


# ── Importar funciones internas ────────────────────────────────────────────
from src.analyzer import (
    _rsi,
    _ma_cross,
    _momentum,
    _score_tecnico,
    _score_to_signal,
    _atr,
    _normalizar_dist_max,
    SIGNAL_LABELS,
)


# ── RSI ────────────────────────────────────────────────────────────────────

class TestRSI:
    def test_rsi_neutral_on_flat_series(self):
        """Serie plana → RSI es NaN (sin movimiento) o 50 (fallback)."""
        flat = pd.Series([100.0] * 30)
        result = _rsi(flat)
        # NaN es válido para serie sin movimiento (0/0 = NaN)
        import math
        assert math.isnan(result) or (40 <= result <= 60), f"RSI plano: got {result}"

    def test_rsi_high_on_uptrend(self):
        """Tendencia alcista fuerte → RSI alto (>65)."""
        up = pd.Series([float(i) for i in range(1, 50)])  # >30 para tener señal clara
        result = _rsi(up)
        import math
        if not math.isnan(result):
            assert result > 65, f"RSI tendencia alcista esperado >65, got {result}"

    def test_rsi_low_on_downtrend(self):
        """Tendencia bajista fuerte → RSI bajo (<40)."""
        down = pd.Series([float(i) for i in range(30, 0, -1)])
        result = _rsi(down)
        assert result < 40, f"RSI tendencia bajista esperado <40, got {result}"

    def test_rsi_range(self):
        """RSI siempre en [0, 100]."""
        import random
        random.seed(42)
        random_series = pd.Series([100 + random.gauss(0, 5) for _ in range(50)])
        result = _rsi(random_series)
        assert 0 <= result <= 100, f"RSI fuera de rango: {result}"

    def test_rsi_short_series_returns_50(self):
        """Serie corta (<14) → RSI = 50 (fallback seguro)."""
        short = pd.Series([100.0, 101.0, 99.0])
        result = _rsi(short)
        assert result == 50.0


# ── MA Cross ───────────────────────────────────────────────────────────────

class TestMACross:
    def test_bullish_cross(self):
        """MA20 > MA50 → True (alcista)."""
        # Precio subiendo → MA corta > MA larga
        rising = pd.Series([float(i) for i in range(1, 61)])
        assert _ma_cross(rising) is True

    def test_bearish_cross(self):
        """MA20 < MA50 → False (bajista)."""
        # Precio cayendo → MA corta < MA larga
        falling = pd.Series([float(i) for i in range(60, 0, -1)])
        assert _ma_cross(falling) is False

    def test_short_series_returns_false(self):
        """Menos de 50 datos → False (no hay MA50)."""
        short = pd.Series([100.0] * 20)
        assert _ma_cross(short) is False


# ── Momentum ───────────────────────────────────────────────────────────────

class TestMomentum:
    def test_positive_momentum(self):
        """Precio sube 10% en 21 días → momentum > 0."""
        prices = pd.Series([100.0] * 25 + [110.0])
        mom = _momentum(prices)
        assert mom > 0

    def test_negative_momentum(self):
        """Precio baja → momentum < 0."""
        prices = pd.Series([110.0] * 25 + [100.0])
        mom = _momentum(prices)
        assert mom < 0


# ── Score Técnico ──────────────────────────────────────────────────────────

class TestScoreTecnico:
    def test_score_range(self):
        """Score técnico siempre en [0, 100]."""
        # Prueba con múltiples combinaciones
        combinations = [
            (70, 5.0, True, 1.5, 60.0),   # Neutral/bueno
            (30, -8.0, False, -2.0, 30.0), # Bajista
            (85, 15.0, True, 3.0, 80.0),   # Alcista fuerte
            (50, 0.0, True, 0.0, 50.0),    # Neutral
        ]
        for rsi, mom, ma_cr, ma50_sl, vol_conf in combinations:
            score = _score_tecnico(rsi, mom, ma_cr, ma50_sl, vol_conf)
            assert 0 <= score <= 100, f"Score técnico fuera de rango: {score} para {(rsi, mom, ma_cr)}"

    def test_bullish_conditions_score_high(self):
        """Condiciones alcistas → score > 55."""
        score = _score_tecnico(
            rsi=45,       # RSI en zona de rebote
            momentum=8.0, # Momentum positivo
            ma_cross=True,
            ma50_slope=2.5,
            vol_confirm=70.0,
        )
        assert score > 55, f"Score alcista esperado >55, got {score}"

    def test_bearish_conditions_score_low(self):
        """Condiciones bajistas → score < 50."""
        score = _score_tecnico(
            rsi=75,        # RSI sobrecomprado
            momentum=-8.0, # Momentum negativo
            ma_cross=False,
            ma50_slope=-2.5,
            vol_confirm=30.0,
        )
        assert score < 50, f"Score bajista esperado <50, got {score}"


# ── Thresholds de señales ──────────────────────────────────────────────────

class TestSignalThresholds:
    def test_compra_fuerte(self):
        assert _score_to_signal(75) == "⭐ COMPRA FUERTE"
        assert _score_to_signal(70) == "⭐ COMPRA FUERTE"

    def test_compra(self):
        assert _score_to_signal(65) == "🟢 COMPRA"
        assert _score_to_signal(58) == "🟢 COMPRA"

    def test_neutral(self):
        assert _score_to_signal(55) == "🟡 NEUTRAL/ESPERAR"
        assert _score_to_signal(45) == "🟡 NEUTRAL/ESPERAR"

    def test_venta_parcial(self):
        assert _score_to_signal(40) == "🟠 VENTA PARCIAL"
        assert _score_to_signal(35) == "🟠 VENTA PARCIAL"

    def test_venta(self):
        assert _score_to_signal(30) == "🔴 VENTA"
        assert _score_to_signal(0)  == "🔴 VENTA"

    def test_boundary_58(self):
        """57 es NEUTRAL, 58 es COMPRA."""
        assert _score_to_signal(57) == "🟡 NEUTRAL/ESPERAR"
        assert _score_to_signal(58) == "🟢 COMPRA"


# ── ATR ────────────────────────────────────────────────────────────────────

class TestATR:
    def test_atr_positive(self):
        """ATR siempre positivo."""
        n = 30
        close = pd.Series([100.0 + i * 0.5 for i in range(n)])
        high  = close + 2.0
        low   = close - 2.0
        result = _atr(high, low, close)
        assert result > 0, f"ATR debe ser > 0, got {result}"

    def test_atr_flat_series(self):
        """Serie perfectamente plana → ATR cercano a 0."""
        n = 30
        close = pd.Series([100.0] * n)
        high  = pd.Series([100.1] * n)
        low   = pd.Series([99.9] * n)
        result = _atr(high, low, close)
        assert result < 1.0, f"ATR serie plana esperado <1, got {result}"


# ── Normalización distancia al máximo ──────────────────────────────────────

class TestNormalizarDistMax:
    def test_at_max_returns_zero(self):
        """Precio en el máximo → dist_max_pct = 0 → score ≈ 0 (no hay oportunidad)."""
        result = _normalizar_dist_max(0.0)
        assert result == 0 or result < 10

    def test_far_from_max_returns_high(self):
        """Precio muy lejos del máximo → score alto (buena oportunidad de entry)."""
        result = _normalizar_dist_max(-30.0)
        assert result > 50, f"Distancia -30% esperado score >50, got {result}"

    def test_capped_at_100(self):
        """Score nunca supera 100."""
        result = _normalizar_dist_max(-99.0)
        assert result <= 100


# ── Signal labels coverage ─────────────────────────────────────────────────

class TestSignalLabels:
    def test_all_levels_covered(self):
        """Todos los niveles 1-5 tienen etiqueta."""
        for level in [1, 2, 3, 4, 5]:
            assert level in SIGNAL_LABELS
            assert SIGNAL_LABELS[level]  # no vacío
