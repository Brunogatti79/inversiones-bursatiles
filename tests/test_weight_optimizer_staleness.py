"""
tests/test_weight_optimizer_staleness.py

Cobertura para _weight_optimizer_esta_stale() (src/pipeline.py), fix real
del 24/07/2026: el chequeo de antigüedad usaba os.path.getmtime() -- la
fecha de modificación del archivo EN EL DISCO de Railway. El filesystem es
efímero (ver arquitectura v4 §3): en cada redeploy se re-sincroniza el
archivo desde GitHub, dándole una fecha local fresca aunque el CONTENIDO
tenga días. Con los redeploys frecuentes de una sesión de desarrollo
activa, esto dejó al weight_optimizer sin correr 5 días a pesar de haber
suficiente historia real para reoptimizar -- literalmente el mecanismo que
"aprende de aciertos y desvíos pasados" bloqueado en silencio.
"""
import json
import os
import time
from datetime import datetime, timedelta

import pytest

from src.pipeline import _weight_optimizer_esta_stale


@pytest.fixture
def opt_file(tmp_path):
    def _make(generated_iso: str = None, sin_generated: bool = False):
        path = tmp_path / "optimized_weights.json"
        content = {} if sin_generated else {"generated": generated_iso}
        path.write_text(json.dumps(content))
        return str(path)
    return _make


class TestWeightOptimizerEstaStale:

    def test_archivo_inexistente_es_stale(self, tmp_path):
        assert _weight_optimizer_esta_stale(str(tmp_path / "no_existe.json")) is True

    def test_archivo_reciente_no_es_stale(self, opt_file):
        reciente = (datetime.now() - timedelta(hours=2)).isoformat()
        path = opt_file(generated_iso=reciente)
        assert _weight_optimizer_esta_stale(path) is False

    def test_archivo_viejo_es_stale(self, opt_file):
        """Caso real de esta sesión: 'generated' de hace 5 días (121 horas)."""
        viejo = (datetime.now() - timedelta(hours=121)).isoformat()
        path = opt_file(generated_iso=viejo)
        assert _weight_optimizer_esta_stale(path) is True

    def test_respeta_el_umbral_de_20_horas(self, opt_file):
        justo_debajo = (datetime.now() - timedelta(hours=19)).isoformat()
        justo_encima = (datetime.now() - timedelta(hours=21)).isoformat()
        assert _weight_optimizer_esta_stale(opt_file(generated_iso=justo_debajo)) is False
        assert _weight_optimizer_esta_stale(opt_file(generated_iso=justo_encima)) is True

    def test_umbral_personalizado(self, opt_file):
        hace_5h = (datetime.now() - timedelta(hours=5)).isoformat()
        path = opt_file(generated_iso=hace_5h)
        assert _weight_optimizer_esta_stale(path, max_age_hours=4) is True
        assert _weight_optimizer_esta_stale(path, max_age_hours=10) is False

    def test_sin_campo_generated_es_stale(self, opt_file):
        path = opt_file(sin_generated=True)
        assert _weight_optimizer_esta_stale(path) is True

    def test_ignora_mtime_local_del_filesystem(self, opt_file):
        """
        El caso central del bug real: el archivo puede tener un mtime local
        recién sincronizado (como pasa en cada redeploy de Railway) mientras
        el contenido ('generated') dice que es viejo -- debe usar el
        contenido, no el mtime del filesystem.
        """
        viejo = (datetime.now() - timedelta(hours=121)).isoformat()
        path = opt_file(generated_iso=viejo)
        # Tocar el archivo ahora mismo -- simula la re-sincronización de
        # Railway al arrancar el container, que le da un mtime fresco
        os.utime(path, (time.time(), time.time()))
        assert _weight_optimizer_esta_stale(path) is True, (
            "debería seguir marcando stale por el contenido, aunque el "
            "mtime del archivo diga que se tocó recién"
        )

    def test_json_corrupto_cae_a_mtime_sin_crashear(self, tmp_path):
        path = tmp_path / "optimized_weights.json"
        path.write_text("{esto no es json valido")
        # No debería lanzar excepción -- cae al fallback de mtime
        resultado = _weight_optimizer_esta_stale(str(path))
        assert isinstance(resultado, bool)
