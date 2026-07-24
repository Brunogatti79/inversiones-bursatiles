"""
tests/test_portfolio_value_history.py

Cobertura para log_portfolio_value_history() y compute_real_drawdown()
(src/tracker.py), agregadas en la sesión del 24/07/2026 (roadmap externo
#8 -- drawdown real del portfolio). El proxy que ya existía en
backtester.py (max_drawdown por bucket de señales) usa una curva de
equity sintética, sin fechas de calendario reales ni posiciones
concurrentes -- esto complementa con el valor total real del portfolio
día a día. Sin este log no hay forma de reconstruir el pasado no
registrado, así que el primer resultado útil tarda varias semanas en
llegar -- mismo patrón que otros históricos de esta sesión.
"""
import json

import pytest

import src.tracker as tracker
import src.github_persistence as gp


@pytest.fixture
def with_portfolio(tmp_path, monkeypatch):
    def _setup(positions: list):
        path = tmp_path / "portfolio.json"
        path.write_text(json.dumps({"positions": positions}))
        monkeypatch.setattr(tracker, "PORTFOLIO_PATH", str(path))
    return _setup


@pytest.fixture(autouse=True)
def _in_memory_persistence(monkeypatch):
    store = {}

    def fake_load(path, default=None):
        return store.get(path, default if default is not None else [])

    def fake_save(path, data, message=None):
        store[path] = data
        return True

    monkeypatch.setattr(gp, "load_json", fake_load)
    monkeypatch.setattr(gp, "save_json", fake_save)
    return store


def _pos(ticker, valor_actual_usd, valor_inicial_usd, valor_actual_ars=0):
    return {
        "ticker": ticker,
        "valor_actual_usd": valor_actual_usd,
        "valor_inicial_usd": valor_inicial_usd,
        "valor_actual_ars": valor_actual_ars,
    }


class TestLogPortfolioValueHistory:

    def test_calcula_valor_total_correctamente(self, with_portfolio, _in_memory_persistence):
        with_portfolio([
            _pos("GGAL.BA", 1000.0, 900.0, 1_500_000),
            _pos("COPX", 500.0, 550.0, 750_000),
        ])
        entry = tracker.log_portfolio_value_history()
        assert entry["valor_total_usd"] == 1500.0
        assert entry["valor_inicial_usd"] == 1450.0
        assert entry["valor_total_ars"] == 2_250_000
        assert entry["n_posiciones"] == 2

    def test_rend_pct_calculado_correctamente(self, with_portfolio, _in_memory_persistence):
        with_portfolio([_pos("GGAL.BA", 900.0, 1000.0)])  # -10%
        entry = tracker.log_portfolio_value_history()
        assert entry["rend_pct"] == -10.0

    def test_sin_portfolio_devuelve_none_sin_crashear(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tracker, "PORTFOLIO_PATH", str(tmp_path / "no_existe.json"))
        entry = tracker.log_portfolio_value_history()
        assert entry is None

    def test_correr_dos_veces_el_mismo_dia_no_duplica(self, with_portfolio, _in_memory_persistence):
        with_portfolio([_pos("GGAL.BA", 1000.0, 900.0)])
        tracker.log_portfolio_value_history()
        tracker.log_portfolio_value_history()
        hist = _in_memory_persistence[tracker.PORTFOLIO_VALUE_HISTORY_PATH]
        assert len(hist) == 1

    def test_valor_inicial_cero_no_produce_division_por_cero(self, with_portfolio, _in_memory_persistence):
        with_portfolio([_pos("GGAL.BA", 1000.0, 0.0)])
        entry = tracker.log_portfolio_value_history()
        assert entry["rend_pct"] is None


class TestComputeRealDrawdown:

    def _seed(self, store, valores, fechas=None):
        fechas = fechas or [f"2026-07-{i+1:02d}" for i in range(len(valores))]
        store[tracker.PORTFOLIO_VALUE_HISTORY_PATH] = [
            {"date": f, "valor_total_usd": v} for f, v in zip(fechas, valores)
        ]

    def test_insuficiente_historia_no_crashea(self, _in_memory_persistence):
        self._seed(_in_memory_persistence, [1000, 1010])  # solo 2 puntos
        r = tracker.compute_real_drawdown()
        assert r["status"] == "insuficiente_historia"

    def test_drawdown_claro_se_calcula_correctamente(self, _in_memory_persistence):
        """10000 -> 8000 es un drawdown de -20% desde el pico."""
        valores = [9000, 9500, 10000, 9800, 9200, 8500, 8000, 8300, 8800, 9500]
        self._seed(_in_memory_persistence, valores)
        r = tracker.compute_real_drawdown()
        assert r["status"] == "ok"
        assert abs(r["max_drawdown_pct"] - (-20.0)) < 0.01
        assert r["fecha_pico"] == "2026-07-03"
        assert r["fecha_valle"] == "2026-07-07"

    def test_serie_siempre_creciente_da_drawdown_cero(self, _in_memory_persistence):
        self._seed(_in_memory_persistence, [1000, 1100, 1200, 1300, 1400, 1500])
        r = tracker.compute_real_drawdown()
        assert r["max_drawdown_pct"] == 0.0

    def test_respeta_ventana_de_dias(self, _in_memory_persistence):
        """Con dias=5, solo deberían contar las últimas 5 entradas -- el
        drawdown grande de hace mucho no debería aparecer si ya se recuperó
        fuera de la ventana pedida."""
        import datetime
        hoy = datetime.datetime.now()
        fechas_viejas = [(hoy - datetime.timedelta(days=100 - i)).strftime("%Y-%m-%d") for i in range(3)]
        fechas_recientes = [(hoy - datetime.timedelta(days=4 - i)).strftime("%Y-%m-%d") for i in range(5)]
        valores_viejos = [10000, 5000, 10000]  # -50% hace mucho, ya recuperado
        valores_recientes = [1000, 1010, 1020, 1030, 1040]  # estable, sin drawdown
        _in_memory_persistence[tracker.PORTFOLIO_VALUE_HISTORY_PATH] = [
            {"date": f, "valor_total_usd": v}
            for f, v in zip(fechas_viejas + fechas_recientes, valores_viejos + valores_recientes)
        ]
        r = tracker.compute_real_drawdown(dias=30)
        assert r["status"] == "ok"
        assert r["max_drawdown_pct"] == 0.0  # el -50% viejo no debería contar
