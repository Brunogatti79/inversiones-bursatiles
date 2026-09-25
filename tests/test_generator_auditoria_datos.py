"""
tests/test_generator_auditoria_datos.py

Auditoría de datos expuestos en el dashboard (25/09/2026, con Bruno):
predictor inactivo oculto, pesos V1 reales en la ficha, nota de Kelly
uniforme, sector global oculto y badge de resultados próximos.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import pytest

from src.generator import generate_dashboard


def _signal(**o):
    base = {
        "ticker": "GGAL.BA", "empresa": "Grupo Galicia", "mercado": "MERVAL", "sector": "Financiero",
        "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA", "score_final": 65.0, "score_v2": 62.0,
        "precio_actual": 150.0, "ret_sem": 1.2, "ret_mes": 3.0, "ret_anual": 10.0, "rsi": 55.0,
        "max_12m": 180.0, "min_12m": 100.0, "rr_ratio": 2.0, "volatility_score": 50.0,
        "pred_5d": 0.0, "pred_21d": 0.0, "pred_signal": "➡️ LATERAL", "pred_confidence": 0.252,
        "pred_direction_agree": False, "atr_stop": 140.0, "atr_target": 170.0,
        "quality_flag": "🟢", "quality_detail": "ok", "quality_alerts": [],
        "kelly_half": 5.0, "suggested_pct": 4.6,
    }
    base.update(o)
    return base


@pytest.fixture(autouse=True)
def _aislar(tmp_path, monkeypatch):
    import src.opportunities_log as ol
    monkeypatch.setattr(ol, "LOG_PATH", str(tmp_path / "opportunities_log.json"))


def _gen(tmp_path, signals):
    df = pd.DataFrame({"INDICE MERVAL": [100, 101, 102]})
    out = str(tmp_path / "d.html")
    generate_dashboard(signals=signals,
                       index_stats={"merval": {"actual": 1.0}, "bovespa": {"actual": 1.0}, "sp500": {"actual": 1.0}},
                       output_path=out, run_date="25/09/2026", price_data={"merval": df, "bovespa": df, "sp500": df})
    return open(out, encoding="utf-8").read()


def test_predictor_con_pesos_cero_queda_inactivo(tmp_path):
    html = _gen(tmp_path, [_signal(), _signal(ticker="AAPL", mercado="SP500")])
    assert "var PRED_ACTIVO = false;" in html


def test_predictor_con_valores_queda_activo(tmp_path):
    html = _gen(tmp_path, [_signal(pred_21d=3.2), _signal(ticker="AAPL", mercado="SP500")])
    assert "var PRED_ACTIVO = true;" in html


def test_ficha_usa_pesos_reales_no_hardcodeados(tmp_path):
    html = _gen(tmp_path, [_signal()])
    assert "35M+35T+10S+20F" not in html
    w = re.search(r"var W_V1 = (\{.*?\});\n", html).group(1)
    assert '"MERVAL"' in w and '"_DEFAULT"' in w


def test_nota_kelly_uniforme(tmp_path, monkeypatch):
    # Solo cuenta COMPRA confirmadas: las no validadas pasan a SIN CONFIRMAR
    # antes de este cálculo, así que se simula la regla validada.
    import src.generator as g
    monkeypatch.setattr(g, "_estado_regla_compra", lambda *a, **k: ("validada", {}))
    html = _gen(tmp_path, [_signal(), _signal(ticker="AAPL", mercado="SP500")])
    assert "Kelly asigna lo mismo a todas las compras (5.0%)" in html
    html2 = _gen(tmp_path, [_signal(), _signal(ticker="AAPL", mercado="SP500", kelly_half=8.0)])
    assert "Kelly asigna lo mismo" not in html2


def test_badge_resultados_definido_y_conectado(tmp_path):
    html = _gen(tmp_path, [_signal(earnings_blackout_shadow=True, earnings_days_to=4,
                                   earnings_next_date="2026-10-01")])
    assert "function earnBadge(s)" in html
    assert "+s.ticker+earnBadge(s)+" in html


def test_opportunity_score_no_depende_del_predictor(tmp_path, monkeypatch):
    """Fix 25/09/2026: el 40% de pred_21d se sacó de la fórmula (R/R 58% +
    confianza 42%). Con pred_21d muy distinto, el score y el orden de las
    fichas tienen que ser idénticos."""
    import json as _json
    import src.generator as g
    # sin esto las COMPRA pasan a SIN CONFIRMAR y no hay fichas: test trivial
    monkeypatch.setattr(g, "_estado_regla_compra", lambda *a, **k: ("validada", {}))
    base = [_signal(confidence_score=70.0, rr_ratio=2.5),
            _signal(ticker="AAPL", mercado="SP500", confidence_score=55.0, rr_ratio=3.5)]
    def fichas(pred):
        sigs = [dict(s, pred_21d=p) for s, p in zip(base, pred)]
        html = _gen(tmp_path, sigs)
        i = html.index("var FICHAS"); j = html.index("=", i) + 1
        f = _json.JSONDecoder().raw_decode(html[j:].lstrip())[0]
        return [(x["ticker"], x["opportunity_score"]) for x in f]
    ref = fichas([0.0, 0.0])
    assert len(ref) == 2 and all(sc > 0 for _, sc in ref)
    assert ref == fichas([14.0, -14.0]) == fichas([-14.0, 14.0])
    html = _gen(tmp_path, base)
    assert "pred:'+(predNorm" not in html and "0.40 + rrNorm" not in html
