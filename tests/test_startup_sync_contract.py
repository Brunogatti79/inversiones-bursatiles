"""
tests/test_startup_sync_contract.py

Regression test sobre qué archivos se sincronizan desde GitHub al arrancar
Railway (start_server.py::_sync_all_data_from_github ->
github_persistence.sync_all_at_startup).

IMPORTANTE: este test NO importa start_server.py. Ese módulo, a nivel de
módulo (no dentro de `if __name__ == "__main__"`), hace `git pull`, arranca
un thread en background, y levanta un HTTPServer.serve_forever() bloqueante.
Importarlo en un test colgaría el proceso de test y/o dispararía side
effects reales (git, red, threads). En su lugar, parseamos el archivo con
`ast` y leemos la lista de literales pasada a sync_all_at_startup() como
dato estático -- el mismo enfoque que test_railway_config.py usa para
railway.toml.

Contexto: hasta junio 2026, data/signals_history.json NO estaba en este
ciclo de persistencia -- por lo que backtester.py, weight_optimizer.py,
optimizer.py e historical_replay.py nunca acumulaban más de ~1 día de
historia real entre redeploys de Railway (que ocurren varias veces por día,
incluso sin caídas, simplemente porque cada push a main redeploya). Si
alguien vuelve a sacar esa línea "para limpiar" sin entender por qué está
ahí, este test debe fallar de forma explícita y explicar por qué.
"""
import ast
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
START_SERVER_PATH = os.path.join(REPO_ROOT, "start_server.py")

# Archivos cuya ausencia del ciclo de sync ya causó pérdida de datos real,
# o que alimentan directamente a un módulo crítico (backtester/optimizer/SLA).
CRITICAL_FILES = {
    "signals_history.json",   # única fuente de backtester/weight_optimizer/historical_replay
    "health_metrics.json",    # SLA y /api/health
    "backtest_results.json",  # métricas que alimentan portfolio_optimizer
    "portfolio.json",         # posiciones reales de Bruno
}


def _extract_sync_filenames():
    """Parsea start_server.py y devuelve la lista de strings pasada como
    primer argumento a sync_all_at_startup(...), sin ejecutar el módulo.
    Devuelve None si no encuentra la llamada (ej. se renombró la función)."""
    with open(START_SERVER_PATH, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=START_SERVER_PATH)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        call_name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if call_name != "sync_all_at_startup" or not node.args:
            continue
        first_arg = node.args[0]
        if isinstance(first_arg, ast.List):
            return [
                elt.value for elt in first_arg.elts
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
            ]
    return None


class TestStartupSyncContract:

    def test_start_server_file_exists(self):
        assert os.path.exists(START_SERVER_PATH)

    def test_sync_call_found_and_nonempty(self):
        filenames = _extract_sync_filenames()
        assert filenames is not None, (
            "No se encontró la llamada a sync_all_at_startup([...]) en "
            "start_server.py -- ¿se renombró la función o se movió la "
            "lista a una variable? Si es un cambio intencional, actualizar "
            "_extract_sync_filenames() en este test junto con ese cambio."
        )
        assert len(filenames) > 0

    def test_critical_files_present_in_startup_sync(self):
        filenames = set(_extract_sync_filenames() or [])
        missing = CRITICAL_FILES - filenames

        assert not missing, (
            f"Archivo(s) crítico(s) ausentes del sync de arranque: {missing}. "
            f"Esto reproduce el bug de pérdida de historia real entre "
            f"redeploys (signals_history.json no persistido hasta junio "
            f"2026 -- ver docstring de este módulo)."
        )

    def test_filenames_passed_without_data_prefix(self):
        """sync_all_at_startup antepone 'data/' por su cuenta. Pasar el
        prefijo desde acá no rompe nada (github_persistence detecta el
        prefijo ya presente y no lo duplica), pero mezclar estilos en la
        misma lista es una fuente de confusión innecesaria -- mejor que
        ningún nombre lo incluya."""
        filenames = _extract_sync_filenames() or []
        prefixed = [f for f in filenames if f.startswith("data/")]
        assert not prefixed, (
            f"{prefixed} ya incluyen el prefijo data/ en la lista de "
            f"start_server.py -- pasar solo el nombre de archivo."
        )

    def test_historical_replay_and_system_confidence_in_startup_sync(self):
        """Regresión del fix de hoy (24/06/2026): historical_replay.json se
        escribía solo localmente y se perdía en cada redeploy -- el chequeo
        de '1x/semana' del módulo nunca tenía efecto real porque el archivo
        nunca sobrevivía para que ese chequeo encontrara algo. system_confidence.json
        (mejora 4.3, kill switch) tenía el mismo gap del lado del pull. No son
        tan críticos como CRITICAL_FILES (ambos son recalculables desde cero
        sin pérdida de información irrecuperable), pero perderlos en cada
        redeploy desperdicia el trabajo de la corrida anterior sin necesidad."""
        filenames = set(_extract_sync_filenames() or [])
        assert "historical_replay.json" in filenames
        assert "system_confidence.json" in filenames

    def test_predictor_validation_in_startup_sync_from_day_one(self):
        """predictor_validation.json (Prioridad 3) se agregó al sync de
        arranque desde su primer commit -- a diferencia de historical_replay.json
        y system_confidence.json, que se agregaron como fix después de
        encontrarlos rotos en producción. Este test existe para que, si
        alguna vez se refactoriza esta lista, no se pierda por descuido."""
        filenames = set(_extract_sync_filenames() or [])
        assert "predictor_validation.json" in filenames

    def test_kill_switch_log_files_in_startup_sync_from_day_one(self):
        """kill_switch_history.json / kill_switch_validation.json
        (Prioridad 2, 25/06/2026) se agregaron al sync desde su primer
        commit -- mismo criterio que predictor_validation.json arriba.
        kill_switch_history.json es además append-only: perderlo en un
        redeploy no es recalculable como historical_replay.json, es
        historia real que no vuelve."""
        filenames = set(_extract_sync_filenames() or [])
        assert "kill_switch_history.json" in filenames
        assert "kill_switch_validation.json" in filenames
