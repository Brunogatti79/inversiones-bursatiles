"""
tests/test_execution_layer.py

Cobertura de src/execution/ (pricing_engine, risk_engine, order_engine) --
la capa de ejecución creada en la auditoría del 26/06/2026 para resolver:

  1. Pricing del portfolio limitado a 14/78 tickers vía diccionarios
     hardcodeados (MERVAL_MAP/BOVESPA_MAP/CEDEAR_MAP en tracker.py) --
     confirmado en datos reales: GGAL.BA, BMA.BA, EDN.BA, GOOGL mostraban
     0,00% de rendimiento durante semanas.
  2. stop_loss/target siempre None en posiciones nuevas (/api/compra
     hardcodeaba None sin leer el atr_stop/atr_target ya calculado).
  3. Lógica de compra/venta embebida en el handler HTTP de start_server.py.

Estos tests NO dependen de archivos reales en data/ -- todo se mockea con
tmp_path y monkeypatch para que la suite siga siendo rápida y determinística.
"""
import json
import os

import pytest

from src.execution import pricing_engine, risk_engine, order_engine


# ── pricing_engine.resolve_position_price ───────────────────────────────

class TestResolvePositionPriceMerval:
    def test_merval_resuelve_via_signal_generico_sin_diccionario(self):
        """El fix central: CUALQUIER ticker MERVAL_CSV debe resolver, no
        solo los 4 que antes estaban hardcodeados en MERVAL_MAP."""
        precio_usd, precio_ars, metodo = pricing_engine.resolve_position_price(
            ticker="GGAL.BA", mercado="MERVAL", precio_fuente="MERVAL_CSV",
            ratio_cedear=1.0, local_prices={"GGAL.BA": 5100.0},
            ccl=1487.0, brl_usd=5.70,
        )
        assert metodo == "merval_signal_ccl"
        assert precio_usd == pytest.approx(5100.0 / 1487.0, rel=1e-4)
        assert precio_ars == 5100.0

    def test_merval_sin_precio_local_no_resuelve(self):
        precio_usd, _, metodo = pricing_engine.resolve_position_price(
            ticker="ALUA.BA", mercado="MERVAL", precio_fuente="MERVAL_CSV",
            ratio_cedear=1.0, local_prices={}, ccl=1487.0, brl_usd=5.70,
        )
        assert precio_usd == 0.0
        assert metodo == "sin_precio"

    def test_merval_sin_ccl_no_resuelve(self):
        precio_usd, _, metodo = pricing_engine.resolve_position_price(
            ticker="GGAL.BA", mercado="MERVAL", precio_fuente="MERVAL_CSV",
            ratio_cedear=1.0, local_prices={"GGAL.BA": 5100.0}, ccl=0, brl_usd=5.70,
        )
        assert precio_usd == 0.0


class TestResolvePositionPriceBovespa:
    def test_bovespa_resuelve_via_signal_generico(self):
        """Antes solo HAPV3.SA estaba en BOVESPA_MAP -- cualquier otro
        ticker BOVESPA debe resolver igual."""
        precio_usd, precio_ars, metodo = pricing_engine.resolve_position_price(
            ticker="VALE3.SA", mercado="BOVESPA", precio_fuente="BOVESPA_CSV",
            ratio_cedear=1.0, local_prices={"VALE3.SA": 57.0},
            ccl=1487.0, brl_usd=5.70,
        )
        assert metodo == "bovespa_signal_brlusd"
        assert precio_usd == pytest.approx(57.0 / 5.70, rel=1e-4)


class TestResolvePositionPriceCedear:
    def test_cedear_sin_fuente_confiable_no_inventa_numero(self):
        """Sin precio en cedear_prices (data912 caído y sin snapshot previo)
        -> queda explícitamente sin resolver, no inventa un número."""
        precio_usd, precio_ars, metodo = pricing_engine.resolve_position_price(
            ticker="MELI", mercado="SP500", precio_fuente="SP500_CSV",
            ratio_cedear=0.3177, local_prices={"MELI": 1619.25},
            ccl=1487.0, brl_usd=5.70, cedear_prices={},
        )
        assert precio_usd == 0.0
        assert metodo == "cedear_sin_fuente_confiable"

    def test_cedear_usa_precio_real_de_data912(self):
        """FIX 26/06/2026 (sesión 2): cedear_prices ahora viene de
        fetch_live_cedear_usd_prices() (data912.com), keyed por ticker y
        YA en USD -- no necesita ratio_cedear ni CCL para resolver."""
        precio_usd, precio_ars, metodo = pricing_engine.resolve_position_price(
            ticker="GLOB", mercado="SP500", precio_fuente="SP500_CSV",
            ratio_cedear=1.9577, local_prices={"GLOB": 27.73},
            ccl=1487.0, brl_usd=5.70,
            cedear_prices={"GLOB": 1.61},  # GLOBD real, data912 26/06/2026
        )
        assert metodo == "cedear_real_data912"
        assert precio_usd == 1.61
        assert precio_ars == pytest.approx(1.61 * 1487.0, rel=1e-4)


class TestFetchLiveCedearUsdPrices:
    def test_data912_caido_devuelve_vacio_no_rompe(self, monkeypatch):
        """Si data912 no responde (timeout, 500, etc.), debe devolver {}
        sin levantar excepción -- el llamador cae al snapshot persistido."""
        import requests

        def _boom(*a, **k):
            raise requests.exceptions.Timeout("simulado")

        monkeypatch.setattr("requests.get", _boom)
        result = pricing_engine.fetch_live_cedear_usd_prices()
        assert result == {}

    def test_prioriza_linea_d_dolar_mep_sobre_ars(self, monkeypatch):
        """La línea {ticker}D (dólar MEP, instrumento real separado) tiene
        prioridad sobre la línea ARS / CCL -- no debería ni mirar el CCL
        si la línea D existe y es válida."""
        class FakeResp:
            def raise_for_status(self): pass
            def json(self):
                return [
                    {"symbol": "GLOB", "c": 2376.0},
                    {"symbol": "GLOBD", "c": 1.61},
                ]
        monkeypatch.setattr("requests.get", lambda *a, **k: FakeResp())
        monkeypatch.setattr(pricing_engine, "get_ccl", lambda signals=None: 999999.0)  # CCL absurdo a propósito

        from src import downloader
        monkeypatch.setattr(downloader, "SP500_TICKERS", {"GLOB": "Globant"})

        prices = pricing_engine.fetch_live_cedear_usd_prices()
        assert prices.get("GLOB") == 1.61  # vino de GLOBD, no de 2376/999999

    def test_fallback_a_linea_ars_dividido_ccl_si_no_hay_d(self, monkeypatch):
        class FakeResp:
            def raise_for_status(self): pass
            def json(self):
                return [{"symbol": "XYZ", "c": 1500.0}]  # sin XYZD
        monkeypatch.setattr("requests.get", lambda *a, **k: FakeResp())
        monkeypatch.setattr(pricing_engine, "get_ccl", lambda signals=None: 1500.0)

        from src import downloader
        monkeypatch.setattr(downloader, "SP500_TICKERS", {"XYZ": "Ticker Ficticio"})

        prices = pricing_engine.fetch_live_cedear_usd_prices()
        assert prices.get("XYZ") == pytest.approx(1.0)  # 1500/1500

    def test_respuesta_no_es_lista_no_rompe(self, monkeypatch):
        class FakeResp:
            def raise_for_status(self): pass
            def json(self):
                return {"error": "formato inesperado"}
        monkeypatch.setattr("requests.get", lambda *a, **k: FakeResp())
        result = pricing_engine.fetch_live_cedear_usd_prices()
        assert result == {}


class TestRefreshPortfolioPrices:
    @pytest.fixture
    def fake_portfolio(self, tmp_path, monkeypatch):
        pf = {
            "capital_usd_ref": 1000.0,
            "positions": [
                {"ticker": "GGAL.BA", "mercado": "MERVAL", "precio_fuente": "MERVAL_CSV",
                 "cantidad": 10, "valor_inicial_usd": 100.0, "precio_compra_usd": 10.0,
                 "ratio_cedear": 1.0},
                {"ticker": "MELI", "mercado": "SP500", "precio_fuente": "SP500_CSV",
                 "cantidad": 1, "valor_inicial_usd": 14.32, "precio_compra_usd": 14.32,
                 "precio_actual_usd": 14.32, "ratio_cedear": 0.3177},
            ],
        }
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps(pf))
        monkeypatch.setattr(pricing_engine, "PORTFOLIO_PATH", str(path))
        monkeypatch.setattr(pricing_engine, "get_latest_prices_by_ticker", lambda gh_token=None: {"GGAL.BA": 1500.0})
        monkeypatch.setattr(pricing_engine, "get_ccl", lambda signals=None: 1500.0)
        monkeypatch.setattr(pricing_engine, "get_brl_usd", lambda signals=None: 5.70)
        monkeypatch.setattr(pricing_engine, "_load_cedear_prices", lambda: {})
        return path

    def test_no_persist_no_escribe_archivo(self, fake_portfolio):
        original_mtime = os.path.getmtime(fake_portfolio)
        result = pricing_engine.refresh_portfolio_prices(signals=None, persist=False)
        assert os.path.getmtime(fake_portfolio) == original_mtime
        ggal = next(p for p in result["positions"] if p["ticker"] == "GGAL.BA")
        assert ggal["precio_metodo"] == "merval_signal_ccl"
        # precio actual: 1500 ARS / ccl 1500 = 1.0 USD/accion x 10 = 10 USD
        # vs valor_inicial_usd=100 (comprado a 10 USD/accion) => -90%
        assert ggal["rend_pct"] == pytest.approx(-90.0, abs=0.01)

    def test_posicion_sin_fuente_confiable_queda_marcada_no_rompe(self, fake_portfolio):
        result = pricing_engine.refresh_portfolio_prices(signals=None, persist=False)
        meli = next(p for p in result["positions"] if p["ticker"] == "MELI")
        assert meli["precio_metodo"] == "cedear_sin_fuente_confiable"
        # no se tocó el rend_pct/valor previo -- sigue como estaba
        assert meli["valor_inicial_usd"] == 14.32


# ── risk_engine.compute_initial_stop_target ──────────────────────────────

class TestComputeInitialStopTarget:
    def test_merval_convierte_atr_stop_a_usd(self):
        signal = {"atr_stop": 3595.36, "atr_target": 3800.71, "atr_metodo": "close_proxy"}
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "TRAN.BA", "MERVAL", "MERVAL_CSV", ccl=1487.0, signal=signal,
        )
        assert stop == pytest.approx(3595.36 / 1487.0, rel=1e-4)
        assert target == pytest.approx(3800.71 / 1487.0, rel=1e-4)
        assert metodo == "atr_close_proxy"

    def test_bovespa_convierte_atr_stop_con_brl(self):
        signal = {"atr_stop": 50.0, "atr_target": 60.0, "atr_metodo": "ohlc"}
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "VALE3.SA", "BOVESPA", "BOVESPA_CSV", brl_usd=5.70, signal=signal,
        )
        assert stop == pytest.approx(50.0 / 5.70, rel=1e-4)

    def test_sin_senal_no_calcula_nada(self):
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "XYZ.BA", "MERVAL", "MERVAL_CSV", signal=None,
        )
        # sin signal explícito y sin archivos reales en disco -> sin_senal
        assert stop is None
        assert metodo in ("sin_senal",)

    def test_atr_en_cero_no_calcula_nada(self):
        """Antes del fix de causa raíz en analyzer.py, atr_stop/atr_target
        eran 0.0 SIEMPRE -- confirmar que esto se sigue manejando con
        gracia (None, no un stop en 0 que dispararía falsas alertas)."""
        signal = {"atr_stop": 0.0, "atr_target": 0.0, "atr_metodo": "sin_datos"}
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "GGAL.BA", "MERVAL", "MERVAL_CSV", ccl=1487.0, signal=signal,
        )
        assert stop is None
        assert metodo == "atr_no_disponible"

    def test_cedear_ahora_si_asigna_stop_en_usd(self):
        """FIX 26/06/2026 (sesión 2): el pricing CEDEAR ahora tiene fuente
        real (data912), así que se habilita el stop/target -- calculado
        sobre el ATR de la señal NYSE. Con ratio_cedear=1.0 (default, ej.
        un ticker verdaderamente 1:1 como PBR) el valor numérico no cambia,
        aunque el método ahora se llama distinto (ver test de abajo para
        el caso con ratio != 1, que es donde estaba el bug real)."""
        signal = {"atr_stop": 25.0, "atr_target": 30.0, "atr_metodo": "close_proxy"}
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "GLOB", "SP500", "SP500_CSV", signal=signal,
        )
        assert stop == 25.0
        assert target == 30.0
        assert metodo == "atr_nyse_div_ratio1_close_proxy"

    def test_cedear_con_ratio_real_divide_correctamente(self):
        """FIX 23/07/2026: bug real encontrado corriendo /backfill_stops en
        producción -- esta rama NO dividía por el ratio de conversión
        CEDEAR, asignándole a COPX (CEDEAR ~$5.79) un stop de $75.83 (ATR
        de NYSE sin escalar), 13x el precio real. Con el ratio oficial de
        BYMA (14:1 para COPX, confirmado contra la tabla oficial actualizada
        3/2/2026) el stop/target ahora quedan en la escala correcta del
        CEDEAR, encerrando el precio real."""
        signal = {"atr_stop": 75.83, "atr_target": 90.5, "atr_metodo": "close_proxy"}
        stop, target, metodo = risk_engine.compute_initial_stop_target(
            "COPX", "SP500", "SP500_CSV", signal=signal, ratio_cedear=14.0,
        )
        precio_real_cedear = 5.79
        assert stop < precio_real_cedear < target, (
            f"El stop/target debería encerrar el precio real del CEDEAR: "
            f"stop={stop}, precio={precio_real_cedear}, target={target}"
        )
        assert metodo == "atr_nyse_div_ratio14_close_proxy"


class TestBackfillMissingStops:
    def test_dry_run_no_modifica_portfolio(self):
        portfolio = {
            "positions": [
                {"ticker": "GGAL.BA", "mercado": "MERVAL", "precio_fuente": "MERVAL_CSV",
                 "stop_loss": None, "target": None, "precio_actual_usd": 3.5},
            ]
        }
        propuestas = risk_engine.backfill_missing_stops(portfolio, dry_run=True)
        assert len(propuestas) == 1
        # no modificó el portfolio real
        assert portfolio["positions"][0]["stop_loss"] is None

    def test_posicion_con_stop_ya_asignado_se_ignora(self):
        portfolio = {
            "positions": [
                {"ticker": "GGAL.BA", "mercado": "MERVAL", "precio_fuente": "MERVAL_CSV",
                 "stop_loss": 3.2, "target": 4.0},
            ]
        }
        propuestas = risk_engine.backfill_missing_stops(portfolio, dry_run=True)
        assert propuestas == []


# ── order_engine.execute_compra / execute_venta ──────────────────────────

class TestExecuteCompra:
    @pytest.fixture
    def empty_portfolio(self, tmp_path, monkeypatch):
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps({"positions": []}))
        monkeypatch.setattr(order_engine, "PORTFOLIO_PATH", str(path))
        monkeypatch.setattr(
            "src.github_persistence.push_file", lambda *a, **k: True
        )
        # Sin señal disponible -> compute_initial_stop_target devuelve
        # (None, None, "sin_senal") de forma controlada, no explota.
        monkeypatch.setattr(
            risk_engine, "compute_initial_stop_target",
            lambda *a, **k: (None, None, "sin_senal"),
        )
        return path

    def test_nueva_posicion_se_crea_con_stop_metodo_explicito(self, empty_portfolio):
        result = order_engine.execute_compra(
            ticker="ggal.ba", precio_raw=5.0, total_usd_raw=0, cantidad=20,
        )
        assert result["status"] == "ok"
        pf = json.loads(empty_portfolio.read_text())
        pos = pf["positions"][0]
        assert pos["ticker"] == "GGAL.BA"
        assert pos["cantidad"] == 20
        assert pos["stop_loss"] is None
        assert pos["stop_metodo"] == "sin_senal"

    def test_nueva_posicion_con_stop_real_disponible(self, empty_portfolio, monkeypatch):
        monkeypatch.setattr(
            risk_engine, "compute_initial_stop_target",
            lambda *a, **k: (4.2, 6.0, "atr_close_proxy"),
        )
        result = order_engine.execute_compra(
            ticker="GGAL.BA", precio_raw=5.0, total_usd_raw=0, cantidad=20,
        )
        assert result["status"] == "ok"
        pf = json.loads(empty_portfolio.read_text())
        pos = pf["positions"][0]
        assert pos["stop_loss"] == 4.2
        assert pos["target"] == 6.0

    def test_compra_invalida_devuelve_error_sin_tocar_archivo(self, empty_portfolio):
        result = order_engine.execute_compra(ticker="", precio_raw=0, total_usd_raw=0, cantidad=0)
        assert result["status"] == "error"
        assert result["http_code"] == 400

    def test_promediado_de_posicion_existente(self, tmp_path, monkeypatch):
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps({"positions": [{
            "ticker": "GGAL.BA", "precio_compra": 5.0, "precio_compra_usd": 5.0,
            "cantidad": 10, "valor_inicial_usd": 50.0, "total_invertido": 50.0,
        }]}))
        monkeypatch.setattr(order_engine, "PORTFOLIO_PATH", str(path))
        monkeypatch.setattr("src.github_persistence.push_file", lambda *a, **k: True)

        result = order_engine.execute_compra(
            ticker="GGAL.BA", precio_raw=7.0, total_usd_raw=0, cantidad=10,
        )
        assert result["status"] == "ok"
        pf = json.loads(path.read_text())
        pos = pf["positions"][0]
        assert pos["cantidad"] == 20
        assert pos["precio_compra_usd"] == pytest.approx(6.0)  # promedio (50+70)/20


class TestExecuteVenta:
    @pytest.fixture
    def portfolio_con_posicion(self, tmp_path, monkeypatch):
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps({"positions": [{
            "ticker": "GGAL.BA", "precio_compra_usd": 5.0,
            "cantidad": 20, "valor_inicial_usd": 100.0,
        }]}))
        monkeypatch.setattr(order_engine, "PORTFOLIO_PATH", str(path))
        monkeypatch.setattr("src.github_persistence.push_file", lambda *a, **k: True)
        return path

    def test_venta_total_elimina_posicion(self, portfolio_con_posicion):
        result = order_engine.execute_venta(ticker="GGAL.BA", precio_raw=6.0, cantidad=20)
        assert result["status"] == "ok"
        pf = json.loads(portfolio_con_posicion.read_text())
        assert pf["positions"] == []

    def test_venta_parcial_actualiza_cantidad(self, portfolio_con_posicion):
        result = order_engine.execute_venta(ticker="GGAL.BA", precio_raw=6.0, cantidad=10)
        assert result["status"] == "ok"
        pf = json.loads(portfolio_con_posicion.read_text())
        assert pf["positions"][0]["cantidad"] == 10

    def test_venta_de_ticker_inexistente_da_error_404(self, portfolio_con_posicion):
        result = order_engine.execute_venta(ticker="NOEXISTE", precio_raw=6.0, cantidad=1)
        assert result["status"] == "error"
        assert result["http_code"] == 404

    def test_venta_mayor_a_la_cantidad_tenida_da_error(self, portfolio_con_posicion):
        result = order_engine.execute_venta(ticker="GGAL.BA", precio_raw=6.0, cantidad=999)
        assert result["status"] == "error"
        assert result["http_code"] == 400
