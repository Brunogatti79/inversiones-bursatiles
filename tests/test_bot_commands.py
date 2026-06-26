"""
tests/test_bot_commands.py

Cobertura de los comandos de Telegram que tocó la sesión del 26/06/2026:

  - cmd_compra / cmd_venta: eran una TERCERA implementación independiente
    de "registrar compra/venta" (las otras dos, en tracker.py y
    start_server.py, ya se habían consolidado en src/execution/order_engine
    .py) -- también hardcodeaban stop_loss=None. Se delegan a order_engine
    para que una compra por Telegram tenga el mismo stop/target real que
    una por el dashboard.
  - cmd_backfill_stops: comando nuevo para asignar retroactivamente
    stop_loss/target a las posiciones que se abrieron antes del fix de
    causa raíz del ATR (26/06/2026).

No se testea la plomería de python-telegram-bot en sí (Application,
dispatch, etc.) -- se llaman las funciones cmd_* directamente con un
Update/Context mockeado mínimo, que es lo único que estas funciones
realmente leen (update.message.reply_text, context.args).

Nota técnica: las funciones cmd_* son `async def`, pero el resto de la
suite del proyecto es 100% sync y no depende de pytest-asyncio -- en vez
de agregar esa dependencia nueva solo para este archivo, cada test sync
corre la coroutine con asyncio.run().
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.bot as bot


def _make_update_context(args):
    update = MagicMock()
    update.message.reply_text = AsyncMock()
    context = MagicMock()
    context.args = args
    return update, context


def _run(coro):
    return asyncio.run(coro)


def _last_msg(update) -> str:
    return update.message.reply_text.call_args[0][0]


@pytest.fixture
def fake_portfolio_path(tmp_path, monkeypatch):
    path = tmp_path / "portfolio.json"
    monkeypatch.setattr(bot, "PORTFOLIO_PATH", str(path))
    monkeypatch.setattr("src.execution.order_engine.PORTFOLIO_PATH", str(path))
    monkeypatch.setattr("src.github_persistence.push_file", lambda *a, **k: True)
    return path


def _write_portfolio(path, data):
    path.write_text(json.dumps(data))


# ── cmd_compra ────────────────────────────────────────────────────────────

class TestCmdCompra:
    def test_formato_invalido_pide_ayuda_sin_tocar_archivo(self, fake_portfolio_path):
        update, context = _make_update_context(args=["GGAL.BA"])  # faltan args
        _run(bot.cmd_compra(update, context))
        update.message.reply_text.assert_awaited_once()
        assert not fake_portfolio_path.exists()

    def test_precio_no_numerico_da_error(self, fake_portfolio_path):
        update, context = _make_update_context(args=["GGAL.BA", "abc", "10"])
        _run(bot.cmd_compra(update, context))
        assert "número" in _last_msg(update)

    def test_compra_delega_en_order_engine_y_asigna_stop(self, fake_portfolio_path, monkeypatch):
        _write_portfolio(fake_portfolio_path, {"positions": []})
        monkeypatch.setattr(
            "src.execution.risk_engine.compute_initial_stop_target",
            lambda *a, **k: (1.2, 1.8, "atr_close_proxy"),
        )
        update, context = _make_update_context(args=["GGAL.BA", "1.59", "100"])
        _run(bot.cmd_compra(update, context))

        pf = json.loads(fake_portfolio_path.read_text())
        pos = pf["positions"][0]
        assert pos["ticker"] == "GGAL.BA"
        assert pos["cantidad"] == 100
        assert pos["stop_loss"] == 1.2  # FIX: antes esto era siempre None
        assert pos["target"] == 1.8
        assert "✅" in _last_msg(update)

    def test_compra_sin_stop_disponible_no_rompe(self, fake_portfolio_path, monkeypatch):
        """Si todavía no hay señal/ATR disponible, debe registrar la compra
        igual (sin stop), no fallar."""
        _write_portfolio(fake_portfolio_path, {"positions": []})
        monkeypatch.setattr(
            "src.execution.risk_engine.compute_initial_stop_target",
            lambda *a, **k: (None, None, "sin_senal"),
        )
        update, context = _make_update_context(args=["GGAL.BA", "1.59", "100"])
        _run(bot.cmd_compra(update, context))
        pf = json.loads(fake_portfolio_path.read_text())
        assert pf["positions"][0]["stop_loss"] is None


# ── cmd_venta ─────────────────────────────────────────────────────────────

class TestCmdVenta:
    def test_venta_sin_posicion_da_error(self, fake_portfolio_path):
        _write_portfolio(fake_portfolio_path, {"positions": []})
        update, context = _make_update_context(args=["GGAL.BA", "2.0", "10"])
        _run(bot.cmd_venta(update, context))
        assert "❌" in _last_msg(update)

    def test_venta_total_delega_en_order_engine(self, fake_portfolio_path):
        _write_portfolio(fake_portfolio_path, {"positions": [{
            "ticker": "GGAL.BA", "precio_compra_usd": 1.0, "cantidad": 10,
            "valor_inicial_usd": 10.0,
        }]})
        update, context = _make_update_context(args=["GGAL.BA", "2.0", "10"])
        _run(bot.cmd_venta(update, context))
        pf = json.loads(fake_portfolio_path.read_text())
        assert pf["positions"] == []
        assert "💰" in _last_msg(update)


# ── cmd_backfill_stops ────────────────────────────────────────────────────

class TestCmdBackfillStops:
    def test_sin_posiciones_avisa_y_no_rompe(self, fake_portfolio_path):
        _write_portfolio(fake_portfolio_path, {"positions": []})
        update, context = _make_update_context(args=[])
        _run(bot.cmd_backfill_stops(update, context))
        assert "No tenés posiciones" in _last_msg(update)

    def test_dry_run_no_modifica_el_archivo(self, fake_portfolio_path, monkeypatch):
        _write_portfolio(fake_portfolio_path, {"positions": [{
            "ticker": "GGAL.BA", "mercado": "MERVAL", "precio_fuente": "MERVAL_CSV",
            "stop_loss": None, "target": None, "precio_actual_usd": 1.0,
        }]})
        monkeypatch.setattr(
            "src.execution.risk_engine.compute_initial_stop_target",
            lambda *a, **k: (0.8, 1.3, "atr_close_proxy"),
        )
        update, context = _make_update_context(args=[])  # sin "aplicar" -> dry run
        _run(bot.cmd_backfill_stops(update, context))

        pf = json.loads(fake_portfolio_path.read_text())
        assert pf["positions"][0]["stop_loss"] is None  # no se tocó

        msg = _last_msg(update)
        assert "vista previa" in msg
        assert "0.8" in msg

    def test_aplicar_persiste_los_stops(self, fake_portfolio_path, monkeypatch):
        _write_portfolio(fake_portfolio_path, {"positions": [{
            "ticker": "GGAL.BA", "mercado": "MERVAL", "precio_fuente": "MERVAL_CSV",
            "stop_loss": None, "target": None, "precio_actual_usd": 1.0,
        }]})
        monkeypatch.setattr(
            "src.execution.risk_engine.compute_initial_stop_target",
            lambda *a, **k: (0.8, 1.3, "atr_close_proxy"),
        )
        update, context = _make_update_context(args=["aplicar"])
        _run(bot.cmd_backfill_stops(update, context))

        pf = json.loads(fake_portfolio_path.read_text())
        assert pf["positions"][0]["stop_loss"] == 0.8
        assert pf["positions"][0]["target"] == 1.3
        assert "APLICADO" in _last_msg(update)

    def test_posicion_ya_por_debajo_del_stop_se_destaca(self, fake_portfolio_path, monkeypatch):
        """Caso de riesgo real: el stop calculado retroactivamente ya fue
        cruzado por el precio actual -- debe quedar bien visible, no
        mezclado silenciosamente con el resto."""
        _write_portfolio(fake_portfolio_path, {"positions": [{
            "ticker": "GLOB_FAKE", "mercado": "SP500", "precio_fuente": "MERVAL_CSV",
            "stop_loss": None, "target": None, "precio_actual_usd": 0.5,
        }]})
        monkeypatch.setattr(
            "src.execution.risk_engine.compute_initial_stop_target",
            lambda *a, **k: (1.5, 2.0, "atr_close_proxy"),  # stop 1.5 > precio actual 0.5
        )
        update, context = _make_update_context(args=[])
        _run(bot.cmd_backfill_stops(update, context))
        assert "YA están por debajo del stop" in _last_msg(update)

    def test_todas_las_posiciones_ya_tienen_stop_no_propone_nada(self, fake_portfolio_path):
        _write_portfolio(fake_portfolio_path, {"positions": [{
            "ticker": "GGAL.BA", "stop_loss": 1.0, "target": 2.0,
        }]})
        update, context = _make_update_context(args=[])
        _run(bot.cmd_backfill_stops(update, context))
        assert "nada para backfillear" in _last_msg(update)
