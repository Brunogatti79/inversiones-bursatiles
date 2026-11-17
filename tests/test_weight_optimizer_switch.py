"""
Interruptor del optimizador de pesos (11/11/2026).

Apagado por defecto: no optimiza, no reescribe optimized_weights.json y el
analyzer usa los pesos fijos W_POR_MERCADO. Se prende solo con
ENABLE_WEIGHT_OPTIMIZER=true.
"""
import json
from datetime import datetime

import pytest

import src.weight_optimizer as wo
import src.signals_archive as sa


PESOS_ARCHIVO = {
    "generated": datetime.now().isoformat(),
    "days_history": 61,
    "mode": "walk_forward",
    "MERVAL":  {"macro": 0.45, "tecnico": 0.20, "sector": 0.10, "fundamental": 0.25,
                "n_real_entries": 594, "n_replay_entries": 0},
    "BOVESPA": {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30,
                "n_real_entries": 598, "n_replay_entries": 0},
    "SP500":   {"macro": 0.15, "tecnico": 0.20, "sector": 0.10, "fundamental": 0.55,
                "n_real_entries": 1209, "n_replay_entries": 0},
}


@pytest.fixture
def archivo_pesos(tmp_path, monkeypatch):
    path = tmp_path / "optimized_weights.json"
    path.write_text(json.dumps(PESOS_ARCHIVO))
    monkeypatch.setattr(wo, "WEIGHTS_PATH", str(path))
    monkeypatch.setattr(sa, "WEIGHTS_PATH", str(path))
    return path


class TestApagadoPorDefecto:
    def test_sin_variable_esta_apagado(self, monkeypatch):
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)
        assert wo.optimizer_enabled() is False

    @pytest.mark.parametrize("valor", ["false", "0", "", "no", "False"])
    def test_valores_que_no_prenden(self, monkeypatch, valor):
        monkeypatch.setenv("ENABLE_WEIGHT_OPTIMIZER", valor)
        assert wo.optimizer_enabled() is False

    @pytest.mark.parametrize("valor", ["true", "TRUE", " True "])
    def test_valores_que_prenden(self, monkeypatch, valor):
        monkeypatch.setenv("ENABLE_WEIGHT_OPTIMIZER", valor)
        assert wo.optimizer_enabled() is True

    def test_load_devuelve_vacio_aunque_exista_archivo_valido(self, monkeypatch, archivo_pesos):
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)
        assert wo.load_optimized_weights() == {}

    def test_run_no_optimiza_ni_reescribe_el_archivo(self, monkeypatch, archivo_pesos):
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)

        def _no_deberia_llamarse(*a, **k):
            raise AssertionError("el optimizador apagado no debe optimizar")

        monkeypatch.setattr(wo, "_optimize_market", _no_deberia_llamarse)
        monkeypatch.setattr(wo, "_build_price_index", _no_deberia_llamarse)
        antes = archivo_pesos.read_text()

        assert wo.run_weight_optimization(price_data={}, ticker_cols={}) == {}
        assert archivo_pesos.read_text() == antes

    def test_provenance_informa_disabled(self, monkeypatch, archivo_pesos):
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)
        prov = wo.weights_provenance()
        assert prov["mode"] == "disabled"
        assert prov["available"] is False
        assert prov["is_synthetic"] is False

    def test_analyzer_usa_pesos_fijos(self, monkeypatch, archivo_pesos):
        """Con el archivo de pesos optimizados presente pero el interruptor
        apagado, ni la carga lazy del analyzer ni apply_optimized_weights()
        del pipeline cambian W_POR_MERCADO."""
        import src.analyzer as ana
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)
        monkeypatch.setattr(ana, "_OPTIMIZED_WEIGHTS", {})
        fijos = {m: dict(w) for m, w in ana.W_POR_MERCADO.items()}

        ana._load_optimized_weights_once()
        wo.apply_optimized_weights(wo.load_optimized_weights())

        assert ana._OPTIMIZED_WEIGHTS == {}
        assert ana.W_POR_MERCADO == fijos
        assert fijos["MERVAL"] == {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30}

    def test_archivo_de_corridas_registra_pesos_fijos(self, monkeypatch, archivo_pesos):
        import src.analyzer as ana
        monkeypatch.delenv("ENABLE_WEIGHT_OPTIMIZER", raising=False)
        snap = sa._weights_snapshot()
        assert snap["mode"] == "disabled_fixed"
        for m in ("MERVAL", "BOVESPA", "SP500"):
            assert snap[m] == ana.W_POR_MERCADO[m]


class TestPrendido:
    def test_load_devuelve_pesos_del_archivo(self, monkeypatch, archivo_pesos):
        monkeypatch.setenv("ENABLE_WEIGHT_OPTIMIZER", "true")
        w = wo.load_optimized_weights()
        assert w["MERVAL"]["macro"] == 0.45
        assert set(w) == {"MERVAL", "BOVESPA", "SP500"}

    def test_archivo_de_corridas_registra_pesos_del_archivo(self, monkeypatch, archivo_pesos):
        monkeypatch.setenv("ENABLE_WEIGHT_OPTIMIZER", "true")
        assert sa._weights_snapshot()["mode"] == "walk_forward"
