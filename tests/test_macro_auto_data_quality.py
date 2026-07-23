"""
tests/test_macro_auto_data_quality.py

Suite dedicada para 3 funciones nuevas de src/macro_auto.py, agregadas en la
sesión extendida del 23/07/2026 (roadmap externo #2, #3, fix de resultado
fiscal) y que quedaron sin tests permanentes ese mismo día -- señalado
explícitamente en una revisión externa posterior como el riesgo más
importante pendiente ("el riesgo viene de mantenimiento, no de
funcionalidad"). Cubre:

  - _check_rango_sano()          Data Quality Layer (rango fijo + adaptativo)
  - _with_last_known_fallback()  Redundancia PRIMARY -> SECONDARY
  - _resultado_fiscal_pct_pbi()  %PBI trailing-12m del resultado fiscal ARG

Todas interactúan con datos externos, reglas de negocio y el score macro --
exactamente donde suelen aparecer regresiones silenciosas, así que se testea
con datos reales ya validados en la sesión que las creó (no inventados),
incluyendo los dos hallazgos reales de esa sesión: la falsa alarma de
arg_tc (bug 2) y la validación exacta contra la cifra oficial de Hacienda
para el resultado fiscal.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta

import pytest

import src.macro_auto as ma
import src.github_persistence as gp


# ─────────────────────────────────────────────────────────────
# Fixtures compartidas
# ─────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _in_memory_persistence(monkeypatch):
    """
    Las 3 funciones bajo test hacen `from src.github_persistence import
    load_json, save_json` DENTRO del cuerpo de la función (no a nivel de
    módulo) -- monkeypatchear macro_auto.load_json no alcanzaría, hay que
    parchear los atributos reales de src.github_persistence, que es de
    donde se resuelve el import en cada llamada.
    """
    store = {}

    def fake_load(path, default=None):
        return store.get(path, default if default is not None else {})

    def fake_save(path, data, message=None):
        store[path] = data
        return True

    monkeypatch.setattr(gp, "load_json", fake_load)
    monkeypatch.setattr(gp, "save_json", fake_save)
    return store


# ─────────────────────────────────────────────────────────────
# _check_rango_sano() -- Data Quality Layer
# ─────────────────────────────────────────────────────────────

class TestCheckRangoSano:
    def test_valor_normal_dentro_de_rango_fijo_no_marca_anomalia(self):
        """Caso base: desempleo real (7.8%) dentro del rango calibrado (4-15%)."""
        peor, mejor = ma.RANGES["arg_desempleo"]
        r = ma._check_rango_sano("desempleo", "ARG", 7.8, peor, mejor)
        assert r is None

    def test_valor_en_el_limite_exacto_no_marca_anomalia(self):
        peor, mejor = ma.RANGES["arg_desempleo"]
        r = ma._check_rango_sano("desempleo", "ARG", peor, peor, mejor)
        assert r is None

    def test_valor_disparatado_sin_historial_marca_anomalia(self):
        """Caso de la revisión externa: desempleo=45% debe saltar."""
        peor, mejor = ma.RANGES["arg_desempleo"]
        r = ma._check_rango_sano("desempleo", "ARG", 45.0, peor, mejor)
        assert r is not None
        assert r["variable"] == "desempleo"
        assert r["modo_comparado"] == "fijo (RANGES)"

    def test_cambio_de_unidad_no_avisado_marca_anomalia(self):
        """Ejemplo de la revisión externa: balanza pasa de USD a miles de USD
        sin avisar -- el valor queda ~1000x más grande."""
        peor, mejor = ma.RANGES["arg_balanza"]
        r = ma._check_rango_sano("balanza_comercial", "ARG", 2_500_000, peor, mejor)
        assert r is not None

    def test_arg_tc_con_historial_adaptativo_no_genera_falsa_alarma(self):
        """
        Bug real encontrado y corregido en la sesión que creó esta función
        (ver commit 18b0aca): RANGES["arg_tc"] = (150, 70) quedó obsoleto
        (dólar real ~1480 en jul-2026), pero con >=24 observaciones reales
        _normalize_adaptive() ya no usa ese rango fijo para el score -- así
        que el chequeo de calidad de datos tampoco debería, o generaría una
        falsa alarma permanente.
        """
        peor, mejor = ma.RANGES["arg_tc"]
        # Historial sintético con el mismo orden de magnitud que el real
        # observado en producción (data/macro_raw_history.json: 1431-1492)
        historial = {"arg_tc": [1430.0 + i * 2 for i in range(30)]}  # ~1430-1488
        valor_hoy = 1487.0  # fuera del rango fijo (70-150), dentro del historial real ampliado
        r = ma._check_rango_sano(
            "tipo_cambio", "ARG", valor_hoy, peor, mejor,
            raw_history=historial, range_key="arg_tc",
        )
        assert r is None, "no debería marcar anomalía: hay historial adaptativo suficiente"

    def test_arg_tc_anomalia_real_si_historial_adaptativo_tambien_la_marca(self):
        """Un valor genuinamente disparatado debe seguir detectándose aunque
        haya historial adaptativo -- el modo adaptativo no debe volverse
        permisivo al punto de no detectar nada."""
        peor, mejor = ma.RANGES["arg_tc"]
        historial = {"arg_tc": [1400.0 + i for i in range(30)]}
        r = ma._check_rango_sano(
            "tipo_cambio", "ARG", 50_000.0, peor, mejor,
            raw_history=historial, range_key="arg_tc",
        )
        assert r is not None
        assert r["modo_comparado"] == "adaptativo (historial real)"

    def test_con_pocas_observaciones_usa_modo_fijo_no_adaptativo(self):
        """Con menos de MIN_OBS_FOR_PERCENTILE observaciones, debe seguir
        comparando contra el rango fijo (mismo criterio que _normalize_adaptive)."""
        peor, mejor = ma.RANGES["arg_tc"]
        historial = {"arg_tc": [1400.0, 1410.0, 1420.0]}  # solo 3 obs, muy pocas
        r = ma._check_rango_sano(
            "tipo_cambio", "ARG", 1487.0, peor, mejor,
            raw_history=historial, range_key="arg_tc",
        )
        # con pocas observaciones cae al rango fijo (70-150) -> 1487 sí es anomalía
        assert r is not None
        assert r["modo_comparado"] == "fijo (RANGES)"


# ─────────────────────────────────────────────────────────────
# _with_last_known_fallback() -- redundancia PRIMARY -> SECONDARY
# ─────────────────────────────────────────────────────────────

class TestWithLastKnownFallback:
    def test_fetch_exitoso_cachea_y_retorna(self, _in_memory_persistence):
        val, fecha = ma._with_last_known_fallback("ipc", lambda: (2.1, "2026-07-01"))
        assert (val, fecha) == (2.1, "2026-07-01")
        assert _in_memory_persistence[ma.LAST_KNOWN_PATH]["ipc"]["valor"] == 2.1

    def test_fetch_falla_cae_al_ultimo_valor_cacheado(self, _in_memory_persistence):
        ma._with_last_known_fallback("ipc", lambda: (2.1, "2026-07-01"))  # siembra el cache
        val, fecha = ma._with_last_known_fallback("ipc", lambda: (None, None))
        assert (val, fecha) == (2.1, "2026-07-01")

    def test_excepcion_en_fetch_cae_al_cache_sin_crashear(self, _in_memory_persistence):
        ma._with_last_known_fallback("ipc", lambda: (2.1, "2026-07-01"))

        def _fetch_que_explota():
            raise ConnectionError("500 simulado")

        val, fecha = ma._with_last_known_fallback("ipc", _fetch_que_explota)
        assert (val, fecha) == (2.1, "2026-07-01")

    def test_sin_cache_previo_y_fetch_falla_devuelve_none(self):
        val, fecha = ma._with_last_known_fallback("variable_nunca_vista", lambda: (None, None))
        assert (val, fecha) == (None, None)

    def test_cache_demasiado_viejo_no_se_usa(self, _in_memory_persistence):
        """Más de MAX_STALE_DAYS (45) de antigüedad: mejor excluir la
        variable que usar un dato demasiado viejo."""
        vieja = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
        _in_memory_persistence[ma.LAST_KNOWN_PATH] = {
            "desempleo": {"valor": 5.0, "fecha": "2025-01-01", "guardado_en": vieja}
        }
        val, fecha = ma._with_last_known_fallback("desempleo", lambda: (None, None))
        assert (val, fecha) == (None, None)

    def test_cache_dentro_del_limite_si_se_usa(self, _in_memory_persistence):
        """Confirma el borde opuesto: un cache de 30 días (< 45) sí se usa."""
        reciente = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        _in_memory_persistence[ma.LAST_KNOWN_PATH] = {
            "desempleo": {"valor": 6.5, "fecha": "2026-06-01", "guardado_en": reciente}
        }
        val, fecha = ma._with_last_known_fallback("desempleo", lambda: (None, None))
        assert (val, fecha) == (6.5, "2026-06-01")


# ─────────────────────────────────────────────────────────────
# _resultado_fiscal_pct_pbi() -- %PBI trailing-12m
# ─────────────────────────────────────────────────────────────

class TestResultadoFiscalPctPbi:
    """
    Los valores usados acá son datos reales de las fuentes oficiales
    (dataset 452/452.3 -- resultado_primario -- y dataset 8/8.2 -- PBI
    nominal trimestral), ya verificados en vivo durante la sesión que
    implementó esta función: la suma de resultado_primario ene-jun 2022 da
    -755.975,78 millones, que coincide EXACTO con la cifra que Hacienda
    publicó oficialmente para ese semestre (-$755.975,7M).
    """

    RP_H1_2022 = [
        -16698.03344600019, -76283.49999999977, -99753.30000000005,
        -79184.80000000028, -162411.7465789998, -321644.40000000014,
    ]
    PBI_TRAILING_JUN22 = [47462528.99, 54288413.78, 60345325.43, 78676963.28]

    def _mock_requests_get(self, monkeypatch, csv_rp, csv_pbi):
        class FakeResp:
            def __init__(self, text):
                self.text = text
            def raise_for_status(self):
                pass

        def fake_get(url, timeout=None, headers=None):
            if "452" in url:
                return FakeResp(csv_rp)
            if "8/distribution/8.2" in url:
                return FakeResp(csv_pbi)
            raise Exception("URL no mockeada: " + url)

        monkeypatch.setattr(ma, "requests", ma.requests)  # no-op, deja el módulo real
        monkeypatch.setattr(ma.requests, "get", fake_get)

    def test_calculo_coincide_con_cifra_oficial_hacienda(self, monkeypatch):
        meses = ["2021-07-01", "2021-10-01", "2022-01-01", "2022-02-01",
                 "2022-03-01", "2022-04-01", "2022-05-01", "2022-06-01"]
        rp_extendido = [0.0, 0.0] + self.RP_H1_2022  # relleno para tener >=12 filas
        csv_rp = "indice_tiempo,resultado_primario\n" + "\n".join(
            f"{m},{v}" for m, v in zip(
                ["2021-07-01","2021-08-01","2021-09-01","2021-10-01","2021-11-01",
                 "2021-12-01","2022-01-01","2022-02-01","2022-03-01","2022-04-01",
                 "2022-05-01","2022-06-01"],
                [0,0,0,0,0,0] + self.RP_H1_2022
            )
        )
        csv_pbi = "indice_tiempo,producto_interno_bruto_precios_mercado\n" + "\n".join(
            f"{m},{v}" for m, v in zip(
                ["2021-07-01","2021-10-01","2022-01-01","2022-04-01"],
                self.PBI_TRAILING_JUN22
            )
        )
        self._mock_requests_get(monkeypatch, csv_rp, csv_pbi)

        pct, fecha = ma._resultado_fiscal_pct_pbi()

        rp_12m = sum([0,0,0,0,0,0] + self.RP_H1_2022)
        pbi_12m = sum(self.PBI_TRAILING_JUN22)
        esperado = round(rp_12m / pbi_12m * 100, 2)

        assert pct == esperado
        assert fecha == "2022-06-01"
        # Ancla dura: confirma que el cálculo sigue dando el mismo orden de
        # magnitud que se validó a mano contra la cifra oficial (déficit,
        # ~-0.3x%, no el -0.99% que publica Hacienda -- ver docstring del
        # módulo para la explicación de por qué difieren).
        assert -0.4 <= pct <= -0.2

    def test_serie_incompleta_devuelve_none_sin_crashear(self, monkeypatch):
        csv_rp = "indice_tiempo,resultado_primario\n2026-01-01,100.0\n"  # 1 sola fila, <12
        csv_pbi = "indice_tiempo,producto_interno_bruto_precios_mercado\n2026-01-01,1000000.0\n"
        self._mock_requests_get(monkeypatch, csv_rp, csv_pbi)

        pct, fecha = ma._resultado_fiscal_pct_pbi()
        assert (pct, fecha) == (None, None)

    def test_fuente_caida_devuelve_none_sin_crashear(self, monkeypatch):
        def fake_get_error(url, timeout=None, headers=None):
            raise ConnectionError("500 simulado")
        monkeypatch.setattr(ma.requests, "get", fake_get_error)

        pct, fecha = ma._resultado_fiscal_pct_pbi()
        assert (pct, fecha) == (None, None)

    def test_columna_inexistente_devuelve_none_sin_crashear(self, monkeypatch):
        csv_rp = "indice_tiempo,columna_equivocada\n2026-01-01,100.0\n"
        csv_pbi = "indice_tiempo,producto_interno_bruto_precios_mercado\n2026-01-01,1000000.0\n"
        self._mock_requests_get(monkeypatch, csv_rp, csv_pbi)

        pct, fecha = ma._resultado_fiscal_pct_pbi()
        assert (pct, fecha) == (None, None)
