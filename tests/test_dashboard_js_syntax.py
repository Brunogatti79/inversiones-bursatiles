"""
tests/test_dashboard_js_syntax.py

P3 (auditoría externa 28/07/2026, riesgo #5 "dashboard monolítico"): el
único chequeo automático que existía sobre el <script> gigante de
generator.py (~1650 líneas en un solo try/catch, generator.py ~3300
líneas totales) era contar que la cantidad de "<script" y "</script>"
coincidiera -- un chequeo de tags balanceados, NO de sintaxis JS válida.
Eso no habría detectado, por ejemplo, una llave de f-string mal escapada
que produce JS sintácticamente roto pero con tags bien cerrados: el
navegador aborta el try/catch en silencio y el dashboard se ve vacío sin
ningún error visible -- exactamente la clase de bug que causó que la tab
"Conclusiones" quedara huérfana sin que nadie lo notara.

Antes de emprender cualquier modularización de generator.py (propuesta
externa: separar en tabs/panorama.py, oportunidades.py, etc. -- alcance
grande, arriesgado, pendiente de confirmación explícita de Bruno sobre el
enfoque), este test cierra el hueco de detección más barato y de mayor
valor: correr el JS realmente generado a través de `node --check`, que sí
valida sintaxis real (no ejecuta el código, no requiere un DOM/browser).
Ya era una práctica MANUAL documentada ("generate_dashboard() con mock
data + node --check on extracted JS" antes de entregar cualquier cambio
a generator.py) -- este archivo la automatiza para que corra en cada
ejecución de la suite, no solo cuando alguien se acuerda de hacerlo a
mano.
"""
import subprocess
import shutil
import sys
import os
import re

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


@pytest.fixture(autouse=True)
def _aislar_opportunities_log(tmp_path, monkeypatch):
    """Mismo motivo que en test_generator_banners.py: generate_dashboard()
    llama a log_opportunities() con un path de producción hardcodeado --
    sin aislarlo, cada corrida de este archivo ensucia data/ real."""
    import src.opportunities_log as opportunities_log
    monkeypatch.setattr(opportunities_log, "LOG_PATH", str(tmp_path / "opportunities_log.json"))


@pytest.fixture
def _basic_args(tmp_path):
    merval_df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
    return dict(
        signals=[_signal(), _signal(ticker="PETR4.SA", mercado="BOVESPA", signal="🔴 VENTA", signal_v2="🔴 VENTA"),
                  _signal(ticker="AAPL", mercado="SP500", signal="⭐ COMPRA FUERTE", signal_v2="⭐ COMPRA FUERTE")],
        index_stats={
            "merval":  {"actual": 1_500_000.0, "ret_anual": 15.0, "volatilidad": 30.0},
            "bovespa": {"actual": 130_000.0, "ret_anual": 10.0, "volatilidad": 20.0},
            "sp500":   {"actual": 5800.0, "ret_anual": 12.0, "volatilidad": 14.0},
        },
        output_path=str(tmp_path / "dashboard.html"),
        run_date="28/07/2026 20:00",
        price_data={"merval": merval_df, "bovespa": merval_df, "sp500": merval_df},
    )


def _extract_inline_script_blocks(html: str) -> list[str]:
    """Extrae el contenido de cada <script>...</script> SIN atributo src
    (los <script src="https://cdn..."> no tienen cuerpo que validar).
    No usa un parser HTML completo -- alcanza para este propósito porque
    generator.py controla el 100% del HTML emitido (no hay contenido de
    terceros que pueda confundir al regex)."""
    pattern = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.DOTALL | re.IGNORECASE)
    return [m.group(1) for m in pattern.finditer(html) if m.group(1).strip()]


@pytest.mark.skipif(shutil.which("node") is None, reason="node no disponible en este entorno")
class TestDashboardJsSyntax:

    def test_inline_script_blocks_are_valid_js(self, _basic_args, tmp_path):
        """Corre cada bloque <script> inline del dashboard generado a
        través de `node --check` (valida sintaxis sin ejecutar). Si esto
        falla, el dashboard real se ve vacío en el navegador sin ningún
        error visible -- este test existe para que ese fallo aparezca acá,
        en CI/local, no en producción."""
        generate_dashboard(**_basic_args)
        with open(_basic_args["output_path"], encoding="utf-8") as f:
            html = f.read()

        blocks = _extract_inline_script_blocks(html)
        assert blocks, (
            "No se encontró ningún bloque <script> inline en el HTML generado -- "
            "esto probablemente significa que el regex de extracción quedó "
            "desalineado con la estructura real de generator.py, no que el "
            "dashboard dejó de tener JS. Revisar antes de asumir que pasa."
        )

        for i, js in enumerate(blocks):
            js_path = tmp_path / f"_extracted_block_{i}.js"
            js_path.write_text(js, encoding="utf-8")
            result = subprocess.run(
                ["node", "--check", str(js_path)],
                capture_output=True, text=True, timeout=30,
            )
            assert result.returncode == 0, (
                f"Bloque <script> #{i} del dashboard generado tiene JS "
                f"sintácticamente inválido -- node --check dice:\n{result.stderr}\n\n"
                f"Esto es exactamente la clase de bug (llave de f-string mal "
                f"escapada, comilla sin escapar en un onclick, etc.) que deja "
                f"el dashboard en blanco sin error visible en producción."
            )

    def test_al_menos_un_bloque_es_el_script_grande_del_dashboard(self, _basic_args):
        """Sanity check de que el regex realmente está agarrando el bloque
        grande (~1650 líneas en producción) y no solo bloques chicos --
        si generator.py se reestructura y este test sigue viendo solo
        bloques triviales, el chequeo de arriba dejaría de aportar nada
        sin que se note."""
        generate_dashboard(**_basic_args)
        with open(_basic_args["output_path"], encoding="utf-8") as f:
            html = f.read()

        blocks = _extract_inline_script_blocks(html)
        longest = max((len(b) for b in blocks), default=0)
        assert longest > 2000, (
            f"El bloque <script> inline más largo encontrado tiene solo "
            f"{longest} caracteres -- esperaba encontrar el script principal "
            f"del dashboard (miles de líneas en producción). Si generator.py "
            f"cambió de estructura, revisar que el regex de extracción siga "
            f"apuntando al lugar correcto."
        )
