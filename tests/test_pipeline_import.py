"""
tests/test_pipeline_import.py

Smoke test propuesto por la auditoría externa v19 (29/07/2026, punto #5),
motivado por un incidente real horas antes: el commit 73f4156 (fix de CCL)
borró por error la definición de macro_auto.get_cached_macro() al hacer un
reemplazo de texto que se comió esa línea. get_cached_macro() no generaba
SyntaxError (su cuerpo quedó como código muerto e inalcanzable dentro de
get_ccl_data() -- Python permite código inalcanzable), así que py_compile no
lo detectó. La suite de tests tampoco lo agarró en esa corrida porque los
archivos que sí lo hubieran detectado (test_bot_commands.py,
test_weight_optimizer_staleness.py) ya estaban excluidos por dependencias
faltantes en el sandbox (python-telegram-bot, etc.) -- una coincidencia que
enmascaró la regresión.

Impacto real durante la ventana entre el commit roto y el hotfix (0d1954c,
mismo día): pipeline.py importa get_cached_macro a nivel de módulo (línea 19)
-- con la función ausente, `import src.pipeline` fallaba por completo, lo que
habría roto cualquier corrida real del pipeline en producción.

Este test no reemplaza la suite funcional existente (test_analyzer.py,
test_backtester.py, etc.) -- esos verifican que la lógica sea correcta. Este
verifica algo más básico y más barato de comprobar: que el módulo completo
sea importable, y que cada símbolo que pipeline.py importa de sus
dependencias siga existiendo con ese nombre exacto. Es la red que hubiera
agarrado este incidente en segundos en vez de en producción.
"""
import importlib


class TestPipelineImport:

    def test_pipeline_module_imports_without_error(self):
        """El síntoma real del incidente: import src.pipeline fallando."""
        import src.pipeline  # noqa: F401 -- el import en sí es la aserción

    def test_pipeline_reimports_cleanly(self):
        """Recarga forzada para detectar problemas de import que un import
        cacheado (ya resuelto por otro test) podría esconder."""
        import src.pipeline
        importlib.reload(src.pipeline)

    def test_get_cached_macro_exists_and_is_callable(self):
        """Regresión específica del incidente 29/07/2026: get_cached_macro()
        quedó como código muerto e inalcanzable dentro de get_ccl_data()
        tras un reemplazo de texto mal hecho."""
        from src.macro_auto import get_cached_macro
        assert callable(get_cached_macro)

    def test_critical_pipeline_dependencies_importable(self):
        """Cada módulo que pipeline.py importa a nivel de archivo, verificado
        individualmente -- si uno solo se rompe, el traceback de este test
        señala exactamente cuál, en vez de un fallo genérico de colección
        de test_pipeline.py (que ni siquiera existe todavía)."""
        modules = [
            "src.downloader",
            "src.analyzer",
            "src.macro_loader",
            "src.macro_auto",
            "src.fundamental",
            "src.data_validator",
            "src.notifier",
            "src.generator",
            "src.tracker",
            "src.backtester",
            "src.cross_market",
            "src.exit_model",
            "src.weight_optimizer",
            "src.monitor",
            "src.kill_switch_log",
            "src.historical_replay",
            "src.predictor_validation",
            "src.volatility_regime",
            "src.confidence_score",
            "src.quality_check",
            "src.trailing_stop",
            "src.predictor_health",
            "src.portfolio_optimizer",
        ]
        failures = {}
        for mod_name in modules:
            try:
                importlib.import_module(mod_name)
            except Exception as e:  # pragma: no cover - queremos el detalle exacto
                failures[mod_name] = repr(e)

        assert not failures, (
            f"Módulo(s) que pipeline.py depende a nivel de archivo no "
            f"importan: {failures}"
        )

    def test_pipeline_public_symbols_present(self):
        """Verifica que los símbolos concretos que pipeline.py importa de
        cada dependencia (no solo el módulo) sigan existiendo con ese
        nombre exacto -- esto es lo que un reemplazo de texto descuidado
        como el del incidente puede romper sin tocar el nombre del módulo."""
        from src.downloader import (
            download_all, save_csvs, load_ohlc_extra,
            MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS,
        )
        from src.analyzer import (
            analyze_market, detect_signal_changes, save_signals, get_index_stats,
        )
        from src.macro_loader import load_xlsx_signals
        from src.macro_auto import fetch_all_macro, get_cached_macro
        from src.fundamental import load_fundamental_scores
        from src.data_validator import validar_todos
        from src.notifier import (
            send_daily_report, send_signal_change_alerts,
            send_excel, send_error_notification, publish_dashboard,
            publish_index_html,
        )
        from src.generator import generate_dashboard, generate_excel
        from src.tracker import update_history, compute_accuracy
        from src.backtester import run_backtest
        from src.cross_market import compute_cross_market_context
        from src.exit_model import enrich_exit_levels
        from src.weight_optimizer import (
            run_weight_optimization, load_optimized_weights, apply_optimized_weights,
        )
        from src.monitor import update_health_metrics, check_sla, persist_global_confidence
        from src.kill_switch_log import log_kill_switch_event, evaluate_kill_switch_history
        from src.historical_replay import run_historical_replay
        from src.predictor_validation import run_predictor_validation
        from src.volatility_regime import compute_volatility_regime
        from src.confidence_score import (
            enrich_confidence_scores, compute_global_confidence,
            apply_kill_switch, compute_exposure_factor, apply_exposure_factor,
        )
        from src.quality_check import validar_señales, inyectar_semaforo
        from src.trailing_stop import apply_trailing_stops
        from src.predictor_health import compute_predictor_health, apply_health_to_signals
        from src.portfolio_optimizer import optimize_portfolio_allocation

        # Si llegamos hasta acá sin ImportError, todos los símbolos existen.
        assert callable(download_all)
        assert callable(apply_trailing_stops)
        assert callable(get_cached_macro)
