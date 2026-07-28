"""
tests/test_history_depth_banner.py

Tests para _render_history_depth_banner() (pedido de Bruno, 28/07/2026,
tras el incidente real de pérdida de signals_history.json -- colapsó 2
veces en menos de 24hs). Bruno pidió explícitamente que el conteo de días
de historial acumulado sea visible en el dashboard, no algo que haya que
auditar el repo a mano para descubrir.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

import pytest

from src.generator import _render_history_depth_banner


def _write_history(path, n_dias):
    history = {f"2026-07-{d:02d}": [{"ticker": "GGAL.BA"}] for d in range(1, n_dias + 1)}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f)


class TestHistoryDepthBannerThresholds:

    def test_sin_archivo_no_rompe_y_no_muestra_nada(self, tmp_path):
        result = _render_history_depth_banner(str(tmp_path / "no_existe.json"))
        assert result == ""

    def test_menos_de_6_dias_es_critico(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 2)
        html = _render_history_depth_banner(str(path))
        assert "🔴" in html
        assert "Crítico" in html
        assert "2 día" in html

    def test_entre_6_y_14_dias_es_acumulando(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 10)
        html = _render_history_depth_banner(str(path))
        assert "🟡" in html
        assert "Acumulando" in html
        assert "10 días" in html

    def test_entre_15_y_20_dias_es_cerca(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 18)
        html = _render_history_depth_banner(str(path))
        assert "🟡" in html
        assert "Cerca del horizonte 21d" in html
        assert "18 días" in html

    def test_21_dias_o_mas_es_completo(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 21)
        html = _render_history_depth_banner(str(path))
        assert "🟢" in html
        assert "Completo" in html
        assert "21 días" in html

    def test_mas_de_21_dias_sigue_completo(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 45)
        html = _render_history_depth_banner(str(path))
        assert "🟢" in html
        assert "Completo" in html

    def test_singular_para_un_dia(self, tmp_path):
        path = tmp_path / "h.json"
        _write_history(path, 1)
        html = _render_history_depth_banner(str(path))
        assert "1 día<" in html or "1 día " in html  # sin la "s" de plural

    def test_json_corrupto_no_rompe(self, tmp_path):
        path = tmp_path / "h.json"
        path.write_text("esto no es json valido {{{")
        html = _render_history_depth_banner(str(path))
        assert html == ""

    def test_umbrales_frontera_exactos(self, tmp_path):
        # Los límites son estrictos (< no <=) -- confirmar que 6, 15 y 21
        # caen del lado correcto.
        path = tmp_path / "h.json"
        _write_history(path, 6)
        assert "Crítico" not in _render_history_depth_banner(str(path))
        assert "Acumulando" in _render_history_depth_banner(str(path))

        _write_history(path, 15)
        assert "Acumulando" not in _render_history_depth_banner(str(path))
        assert "Cerca" in _render_history_depth_banner(str(path))

        _write_history(path, 21)
        assert "Cerca" not in _render_history_depth_banner(str(path))
        assert "Completo" in _render_history_depth_banner(str(path))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
