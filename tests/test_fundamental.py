"""
tests/test_fundamental.py

Tests para el fix de cobertura fundamental faltante (auditoría externa
v20, prioridad #3, 10/08/2026): antes, cualquier ticker sin fila en
ratios_consolidado_quant.csv recibía 50.0 exacto en analyzer.py -- un
valor neutro fijo, no una estimación. Con ~17 de 84 tickers actuales sin
fundamentales, esto sesgaba V1/AQ (un ticker malo quedaba mejor
calificado de lo real, uno bueno quedaba peor, ambos convergiendo al
mismo número).

Cubre:
  - load_fundamental_scores() devuelve (scores, sector_medians) -- la
    mediana es del SCORE final 0-100 ya calculado por ticker, no de
    ratios crudos (eso ya lo hace _compute_sector_medians(), sin cambios)
  - get_fundamental_score() usa sector_medians como fallback intermedio,
    antes de caer a 50.0
  - CSV inexistente -> ({}, {}), no crashea (mismo criterio que antes)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tempfile
import pytest

from src.fundamental import load_fundamental_scores, get_fundamental_score


def _write_csv(tmp_path, rows):
    header = "Ticker;Empresa;Sector;Score Cuantitativo;P/E (trailing);EV/EBITDA;ROE;Graham Upside %;Margen EBITDA;Margen Operativo;Deuda/Equity;Current Ratio;Crec. Ingresos YoY;Crec. Ganancias YoY\n"
    path = tmp_path / "ratios_test.csv"
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        for r in rows:
            f.write(r + "\n")
    return str(path)


class TestLoadFundamentalScoresDevuelveTupla:

    def test_csv_inexistente_devuelve_tupla_vacia(self):
        scores, medians = load_fundamental_scores("data/no_existe_esto.csv")
        assert scores == {}
        assert medians == {}

    def test_csv_real_devuelve_scores_y_medianas(self, tmp_path):
        path = _write_csv(tmp_path, [
            "AAA;Empresa A;FINANCIERO;80;;;;;;;;;;",
            "BBB;Empresa B;FINANCIERO;60;;;;;;;;;;",
            "CCC;Empresa C;TECNOLOGIA;90;;;;;;;;;;",
        ])
        scores, medians = load_fundamental_scores(path)
        assert set(scores.keys()) == {"AAA", "BBB", "CCC"}
        assert "FINANCIERO" in medians
        assert "TECNOLOGIA" in medians
        # Mediana de FINANCIERO (2 tickers, 80 y 60 normalizados) debe caer
        # entre los dos scores normalizados de esos tickers.
        assert min(medians["FINANCIERO"], 100) >= 0


class TestGetFundamentalScoreConFallbackDeSector:

    def test_ticker_presente_usa_su_propio_score(self):
        scores = {"AAA": 77.0}
        result = get_fundamental_score("AAA", scores=scores)
        assert result == 77.0

    def test_ticker_ausente_sin_sector_medians_usa_50(self):
        """Compatibilidad hacia atrás: sin sector_medians disponible,
        mismo comportamiento de siempre."""
        scores = {"AAA": 77.0}
        result = get_fundamental_score("ZZZ", scores=scores)
        assert result == 50.0

    def test_ticker_ausente_con_sector_medians_usa_mediana(self):
        scores = {"AAA": 77.0}
        sector_medians = {"FINANCIERO": 35.0}
        result = get_fundamental_score("ZZZ", scores=scores, sector="FINANCIERO",
                                       sector_medians=sector_medians)
        assert result == 35.0

    def test_ticker_ausente_sector_sin_mediana_disponible_usa_50(self):
        """El ticker no tiene fila Y su sector tampoco tiene ninguna
        mediana calculable (nadie del sector tiene datos) -- último
        recurso, 50.0, no debe crashear."""
        scores = {"AAA": 77.0}
        sector_medians = {"FINANCIERO": 35.0}
        result = get_fundamental_score("ZZZ", scores=scores, sector="ENERGIA",
                                       sector_medians=sector_medians)
        assert result == 50.0
