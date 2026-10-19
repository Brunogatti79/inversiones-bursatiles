"""Tests del gate por mercado (19/10/2026) y su efecto en dashboard y Telegram.
Nunca escriben en data/ real."""
import json

import pytest

from src import gate_mercado as gm


def _bt(merval=(-1.13, True, 300, 3.0), bovespa=(-2.08, True, 70, 3.1), sp500=(1.5, True, 60, 3.2),
        nivel="racha"):
    def blk(a, sig, n, v):
        return {nivel: {"n": n, "ventanas_independientes": v, "ev_neto": 1.0, "ret_ew": 2.0,
                        "alpha_ew_neto": {"samples": n, "avg_ret": a, "p_value": 0.01 if sig else 0.4,
                                          "significativo_95": sig}}}
    return {"benchmark": {"by_market": {
        "MERVAL": {"compra": blk(*merval)}, "BOVESPA": {"compra": blk(*bovespa)},
        "SP500": {"compra": blk(*sp500)}}}}


def test_estado_por_mercado_reglas():
    e = gm.estado_por_mercado(_bt())
    assert e["MERVAL"]["habilitado"] is False and e["MERVAL"]["negativo_sig"] is True
    assert e["BOVESPA"]["habilitado"] is False
    assert e["SP500"]["habilitado"] is True
    assert e["SP500"]["nivel"] == "racha"


@pytest.mark.parametrize("sp", [(1.5, False, 60, 3.2),   # no significativo
                                (1.5, True, 10, 3.2),    # muestra chica
                                (1.5, True, 60, 2.0),    # pocas ventanas
                                (-0.1, False, 60, 3.2)])  # negativo
def test_no_habilita_sin_evidencia_completa(sp):
    assert gm.estado_por_mercado(_bt(sp500=sp))["SP500"]["habilitado"] is False


def test_sin_benchmark_cierra_todo():
    e = gm.estado_por_mercado({})
    assert not any(v["habilitado"] for v in e.values())
    assert gm.etiqueta_gateada("🟢 COMPRA", "SP500", e) == "🔵 SIN CONFIRMAR"


def test_usa_dia_si_no_hay_racha():
    e = gm.estado_por_mercado(_bt(nivel="dia"))
    assert e["SP500"]["nivel"] == "dia" and e["SP500"]["habilitado"] is True


def test_etiqueta_nunca_contiene_compra():
    e = gm.estado_por_mercado(_bt())
    for sig in gm.SENALES_COMPRA:
        lab = gm.etiqueta_gateada(sig, "MERVAL", e)
        assert lab == "🔵 SIN CONFIRMAR ⚠️" and "COMPRA" not in lab
    assert gm.etiqueta_gateada("🟢 COMPRA", "SP500", e) is None        # habilitado
    assert gm.etiqueta_gateada("🟡 NEUTRAL/ESPERAR", "MERVAL", e) is None


def test_panel_cartera_y_marcadores_privados():
    from src.generator import _render_cartera_riesgo_panel
    capa = {"fecha_datos": "2026-10-16", "pesos_objetivo": {"MERVAL": 0.22, "BOVESPA": 0.24, "SP500": 0.54},
            "cartera_real": {"pesos": {"MERVAL": 0.43, "BOVESPA": 0.10, "SP500": 0.47}, "total_usd": 6000},
            "vol_63d_pct": {"MERVAL": 30, "BOVESPA": 20, "SP500": 11}, "banda_pp": 5, "rebalancear": True,
            "montos_sugeridos_usd": {"MERVAL": -1260, "BOVESPA": 840, "SP500": 420},
            "seguimiento": {"inicio": "2026-09-30", "inversa_vol_banda": {"nav": 103.5, "dias": 12},
                            "tercios": {"nav": 104.4}, "sp500": {"nav": 99.5}}}
    html = _render_cartera_riesgo_panel(capa, gm.estado_por_mercado(_bt()))
    assert "22%" in html and "vender USD 1.260" in html and "no concluyente" in html
    assert "compras sin confirmar" in html and "compras habilitadas" in html
    import re
    pub = re.sub(r"<!--CARTERA_PRIVADA_INI-->.*?<!--CARTERA_PRIVADA_FIN-->", "", html, flags=re.S)
    assert "Tu cartera" not in pub and "USD 1.260" not in pub and "22%" in pub
    assert html.count("{") == html.count("}")  # no rompe el f-string que lo embebe


def test_panel_sin_datos_no_rompe():
    from src.generator import _render_cartera_riesgo_panel
    html = _render_cartera_riesgo_panel({}, {})
    assert "Sin datos de la capa" in html and "compras sin confirmar" in html


def test_telegram_compras_gateadas():
    from src import notifier as nt
    sigs = [{"mercado": "MERVAL", "signal": "🟢 COMPRA", "ticker": "GGAL.BA", "empresa": "Galicia",
             "score_final": 70, "ret_sem": 1.0},
            {"mercado": "SP500", "signal": "🟢 COMPRA", "ticker": "MCD", "empresa": "McDonalds",
             "score_final": 68, "ret_sem": 0.5}]
    txt = nt._compras_section(sigs, gm.estado_por_mercado(_bt()))
    assert "GGAL.BA" not in txt and "Sin compras confirmadas" in txt
    assert "MCD" in txt


def test_telegram_cambios_anotados(monkeypatch):
    from src import notifier as nt
    sent = []
    monkeypatch.setattr(nt, "_send_message", lambda t, **k: sent.append(t) or True)
    monkeypatch.setattr(nt, "_gate_estados", lambda: gm.estado_por_mercado(_bt()))
    nt.send_signal_change_alerts([{"mercado": "MERVAL", "ticker": "GGAL.BA", "empresa": "Galicia",
                                   "prev_signal": "🟡 NEUTRAL/ESPERAR", "new_signal": "🟢 COMPRA"}])
    assert "sin confirmar" in sent[0]


def test_cartera_section(tmp_path):
    from src import notifier as nt
    p = tmp_path / "capa.json"
    p.write_text(json.dumps({"pesos_objetivo": {"MERVAL": 0.2, "BOVESPA": 0.3, "SP500": 0.5},
                             "cartera_real": {"pesos": {"MERVAL": 0.4, "BOVESPA": 0.1, "SP500": 0.5},
                                              "total_usd": 100}, "rebalancear": True}))
    txt = nt._cartera_section(str(p))
    assert "Cartera por riesgo" in txt and "fuera de banda" in txt
    assert nt._cartera_section(str(tmp_path / "no.json")) == ""
