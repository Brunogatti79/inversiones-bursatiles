"""
tests/test_railway_config.py

Regression test para el incidente de 36hs de caída (22-23/06/2026):
railway.toml sin `watchPatterns` hacía que Railway redeployara en CADA push
a `main` -- incluidos los ~10 commits por corrida que el propio pipeline
genera al persistir data/ (vía github_persistence.push_file). Cada push
intra-pipeline disparaba un redeploy nuevo que mataba el contenedor en
ejecución a mitad de paso, sin excepción de Python visible y en un punto
distinto cada vez -- lo que hizo el diagnóstico mucho más largo de lo que
debería haber sido.

Estos tests no ejecutan código del pipeline: parsean railway.toml como dato
estático, para blindar la config contra una regresión silenciosa (alguien
borra watchPatterns "para simplificar", o agrega data/ y vuelve a abrir la
misma puerta).
"""
import os
import tomllib

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAILWAY_TOML_PATH = os.path.join(REPO_ROOT, "railway.toml")


def _load_config():
    with open(RAILWAY_TOML_PATH, "rb") as f:
        return tomllib.load(f)


class TestWatchPatterns:

    def test_railway_toml_exists(self):
        assert os.path.exists(RAILWAY_TOML_PATH), (
            "railway.toml no encontrado en la raíz del repo"
        )

    def test_watch_patterns_present(self):
        """Causa raíz del incidente 22-23/06/2026: sin watchPatterns, Railway
        redeploya en CADA push -- incluidos los que hace el propio pipeline
        al persistir data/. No remover esta clave."""
        config = _load_config()
        build = config.get("build", {})
        assert "watchPatterns" in build, (
            "watchPatterns ausente en [build] -- esto es exactamente lo que "
            "causó la caída de 36hs del 22-23/06/2026."
        )
        assert isinstance(build["watchPatterns"], list)
        assert len(build["watchPatterns"]) > 0

    def test_watch_patterns_cover_source_code(self):
        """Los patterns deben seguir disparando redeploy ante cambios reales
        de código -- si no, un fix deployado nunca se aplicaría."""
        patterns = _load_config()["build"]["watchPatterns"]
        assert any(p in ("src/**", "src/*", "src") for p in patterns), (
            "watchPatterns no cubre src/** -- los cambios de código no "
            "dispararían redeploy"
        )
        assert any("railway.toml" in p for p in patterns), (
            "railway.toml debería watchearse a sí mismo (si alguien rompe "
            "watchPatterns, al menos ese cambio sí se aplicaría)"
        )

    def test_watch_patterns_excludes_data_directory(self):
        """Núcleo de la regresión: data/ es donde el pipeline commitea
        ~10 archivos por corrida (signals_history.json, portfolio.json,
        etc. vía github_persistence.push_file). Si data/ vuelve a entrar en
        watchPatterns, cada push del propio pipeline dispara un redeploy y
        mata el contenedor a mitad de ejecución -- el bug original, de
        nuevo."""
        patterns = _load_config()["build"]["watchPatterns"]
        for p in patterns:
            assert not (p == "data" or p.startswith("data/")), (
                f"Pattern '{p}' incluye data/ en watchPatterns -- esto "
                f"reproduce el incidente del 22-23/06/2026 (redeploy en "
                f"cada commit de datos del propio pipeline)"
            )

    def test_watch_patterns_excludes_outputs_directory(self):
        """generator.py escribe el HTML del dashboard en outputs/ y eso
        también se commitea -- mismo riesgo que data/ si quedara watcheado."""
        patterns = _load_config()["build"]["watchPatterns"]
        for p in patterns:
            assert not (p == "outputs" or p.startswith("outputs/")), (
                f"Pattern '{p}' incluye outputs/ en watchPatterns -- riesgo "
                f"de redeploy en cada publicación de dashboard"
            )

    def test_start_command_present(self):
        deploy = _load_config().get("deploy", {})
        assert deploy.get("startCommand") == "python start_server.py"

    def test_restart_policy_configured(self):
        """Si el contenedor muere por otra causa (OOM, etc.), que al menos
        reintente en vez de quedar caído silenciosamente hasta el próximo
        push."""
        deploy = _load_config().get("deploy", {})
        assert deploy.get("restartPolicyType") == "ON_FAILURE"
        assert isinstance(deploy.get("restartPolicyMaxRetries"), int)
        assert deploy["restartPolicyMaxRetries"] >= 1
