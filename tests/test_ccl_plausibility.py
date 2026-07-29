"""
tests/test_ccl_plausibility.py

Regresión del incidente real 29/07/2026: el fix de CCL del día anterior
(fetch real a Ámbito, antes descartado) empezó a persistir
data/ccl_cache.json sin ningún chequeo de orden de magnitud. Una lectura
de 118.18 (contra un fallback histórico ya validado de 1487.0 -- ~12.6x de
diferencia) se guardó y se usó igual: infló el precio USD de las 7
posiciones MERVAL de la cartera real de Bruno (que siempre dividen por
CCL) y de cualquier CEDEAR que ese día cayera al fallback ARS/CCL interno
de fetch_live_cedear_usd_prices() por no tener línea "D" en data912 (le
pasó a GOOGL esa corrida). Detectado por Bruno mirando el dashboard
("+1065.6%", "+1285.0%" de ganancia no correctas), no por ninguna alerta
del sistema.

Cubre los 4 puntos donde se agregó el guard de plausibilidad (banda
300-6000, deliberadamente amplia -- ver comentarios en el código fuente
para el razonamiento):
  1. pricing_engine.get_ccl() -- no confía en el cache si está fuera de rango
  2. trailing_stop._get_ccl() -- mismo criterio, usado para unrealized_R
  3. monitor._check_ccl_status() -- reporta IMPLAUSIBLE, no "OK", con un
     valor corrupto
  4. macro_auto -- no persiste un valor fetcheado fuera de rango en primer
     lugar (los dos call sites: fetch_argentina_macro y get_ccl_data)
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import src.execution.pricing_engine as pe
import src.trailing_stop as ts
import src.monitor as monitor


# ─────────────────────────────────────────────────────────────
# 1) pricing_engine.get_ccl()
# ─────────────────────────────────────────────────────────────

class TestPricingEngineGetCCL:

    def test_rejects_implausible_low_value_like_the_real_incident(self, tmp_path, monkeypatch):
        """Reproduce el valor exacto del incidente: 118.18 en vez de ~1487."""
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 118.18}))
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(cache_path))

        assert pe.get_ccl() == pe.FALLBACK_CCL

    def test_rejects_implausible_high_value(self, tmp_path, monkeypatch):
        """Simétrico: un valor absurdamente alto tampoco debe colarse."""
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 999999.0}))
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(cache_path))

        assert pe.get_ccl() == pe.FALLBACK_CCL

    def test_accepts_plausible_cached_value(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 1502.30}))
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(cache_path))

        assert pe.get_ccl() == 1502.30

    def test_falls_back_to_ggal_ratio_when_cache_implausible(self, tmp_path, monkeypatch):
        """Si el cache es basura pero hay señales GGAL.BA/GGAL válidas, se
        usa esa derivación -- no debe saltar directo al fallback fijo si
        hay una fuente mejor disponible."""
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 118.18}))
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(cache_path))

        signals = [
            {"ticker": "GGAL.BA", "precio_actual": 5000.0},
            {"ticker": "GGAL",    "precio_actual": 33.5},
        ]
        # 5000 / 33.5 * 10 = 1492.5 -- dentro de rango plausible
        ccl = pe.get_ccl(signals)
        assert 300.0 <= ccl <= 6000.0
        assert ccl != pe.FALLBACK_CCL

    def test_falls_back_to_fixed_value_when_both_cache_and_ggal_implausible(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 118.18}))
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(cache_path))

        signals = [
            {"ticker": "GGAL.BA", "precio_actual": 50.0},   # ratio absurdo a propósito
            {"ticker": "GGAL",    "precio_actual": 33.5},
        ]
        assert pe.get_ccl(signals) == pe.FALLBACK_CCL

    def test_no_cache_file_falls_back(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pe, "CCL_CACHE_PATH", str(tmp_path / "no_existe.json"))
        assert pe.get_ccl() == pe.FALLBACK_CCL


# ─────────────────────────────────────────────────────────────
# 2) trailing_stop._get_ccl()
# ─────────────────────────────────────────────────────────────

class TestTrailingStopGetCCL:

    def test_rejects_implausible_value(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 118.18}))
        monkeypatch.chdir(tmp_path)
        # _get_ccl() en trailing_stop.py usa la ruta relativa "data/ccl_cache.json"
        os.makedirs("data", exist_ok=True)
        with open("data/ccl_cache.json", "w") as f:
            json.dump({"compra": 118.18}, f)

        assert ts._get_ccl() == 0.0

    def test_accepts_plausible_value(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        os.makedirs("data", exist_ok=True)
        with open("data/ccl_cache.json", "w") as f:
            json.dump({"compra": 1480.0}, f)

        assert ts._get_ccl() == 1480.0


# ─────────────────────────────────────────────────────────────
# 3) monitor._check_ccl_status()
# ─────────────────────────────────────────────────────────────

class TestMonitorCCLStatus:

    def test_flags_implausible_as_such_not_ok(self, tmp_path, monkeypatch):
        """El hallazgo central del incidente: un valor corrupto no debe
        reportar 'OK' en health_metrics.json solo porque el archivo existe
        y el número es mayor a cero."""
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 118.18}))
        monkeypatch.setattr(monitor, "CCL_CACHE_PATH", str(cache_path))

        result = monitor._check_ccl_status()
        assert result["ccl_source_status"] == "IMPLAUSIBLE"
        assert result["last_ccl_value"] == 118.18

    def test_reports_ok_for_plausible_fresh_value(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 1490.0}))
        monkeypatch.setattr(monitor, "CCL_CACHE_PATH", str(cache_path))

        result = monitor._check_ccl_status()
        assert result["ccl_source_status"] == "OK"

    def test_reports_sin_valor_for_zero(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "ccl_cache.json"
        cache_path.write_text(json.dumps({"compra": 0}))
        monkeypatch.setattr(monitor, "CCL_CACHE_PATH", str(cache_path))

        result = monitor._check_ccl_status()
        assert result["ccl_source_status"] == "SIN_VALOR"

    def test_reports_sin_archivo_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(monitor, "CCL_CACHE_PATH", str(tmp_path / "no_existe.json"))

        result = monitor._check_ccl_status()
        assert result["ccl_source_status"] == "SIN_ARCHIVO"


# ─────────────────────────────────────────────────────────────
# 4) macro_auto -- no persistir un fetch implausible
# ─────────────────────────────────────────────────────────────

class TestMacroAutoDoesNotPersistImplausibleCCL:

    def test_get_ccl_data_does_not_persist_implausible_fetch(self, tmp_path, monkeypatch):
        import src.macro_auto as ma

        class _FakeResponse:
            status_code = 200
            def json(self):
                # Reproduce el valor exacto del incidente real.
                return {"compra": "118,18"}

        def _fake_get(*args, **kwargs):
            return _FakeResponse()

        cache_path = tmp_path / "ccl_cache.json"
        monkeypatch.setattr(ma, "CCL_CACHE_PATH", str(cache_path))
        monkeypatch.setattr(ma.requests, "get", _fake_get)

        result = ma.get_ccl_data(max_age_hours=0)  # fuerza fetch en vivo, ignora cache

        assert result.get("compra") is None
        assert not cache_path.exists(), (
            "get_ccl_data() persistió un CCL implausible (118.18) -- "
            "exactamente el incidente real del 29/07/2026."
        )

    def test_get_ccl_data_persists_plausible_fetch(self, tmp_path, monkeypatch):
        import src.macro_auto as ma

        class _FakeResponse:
            status_code = 200
            def json(self):
                return {"compra": "1.487,50"}

        def _fake_get(*args, **kwargs):
            return _FakeResponse()

        cache_path = tmp_path / "ccl_cache.json"
        monkeypatch.setattr(ma, "CCL_CACHE_PATH", str(cache_path))
        monkeypatch.setattr(ma.requests, "get", _fake_get)

        result = ma.get_ccl_data(max_age_hours=0)

        assert result.get("compra") == 1487.50
        assert cache_path.exists()
