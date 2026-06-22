"""
src/opportunities_log.py

Registro de "Oportunidades" — específicamente las que el dashboard muestra
en la solapa 🎯 Oportunidades (señal COMPRA/COMPRA FUERTE + opportunity_score
>= 50 y >= percentil 75 del día — el mismo filtro triple que usa el JS del
dashboard, replicado acá en Python).

Objetivo: poder medir más adelante qué tan efectivas fueron, en la realidad,
las recomendaciones concretas que se le mostraron a Bruno (no el universo
completo de señales, que ya cubre signals_history.json / backtester.py).

Se llama desde generator.py, justo después de construir las fichas de
Oportunidades, porque ahí ya está todo el contexto calculado (scores V1/V2,
niveles de índice del día, régimen cross-market).

Persistencia: igual que el resto del proyecto, el filesystem de Railway es
efímero, así que esto se pushea a GitHub después de cada escritura.
"""

import os
import json
import logging
import base64
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

LOG_PATH = "data/opportunities_log.json"
EFFECTIVENESS_PATH = "data/opportunities_effectiveness.json"
GH_REPO_FULL = "Brunogatti79/inversiones-bursatiles"
MAX_DAYS = 180  # ~6 meses de historial de oportunidades registradas


# ─────────────────────────────────────────────────────────────────────────────
# Filtro triple — debe quedar IDÉNTICO al de generator.py (sección JS
# "Aplicar filtro triple sobre FICHAS"). Si se cambia uno, cambiar el otro.
# ─────────────────────────────────────────────────────────────────────────────

def _triple_filter(fichas):
    candidatos = [
        f for f in fichas
        if 'COMPRA' in (f.get('signal_v2') or f.get('signal') or '')
        and 'VENTA' not in (f.get('signal_v2') or f.get('signal') or '')
    ]
    if not candidatos:
        return []
    scores = sorted(f.get('opportunity_score', 0) or 0 for f in candidatos)
    p75_idx = int(len(scores) * 0.75)
    p75 = scores[p75_idx] if p75_idx < len(scores) else scores[-1]
    return sorted(
        [f for f in candidatos if (f.get('opportunity_score', 0) or 0) >= 50
         and (f.get('opportunity_score', 0) or 0) >= p75],
        key=lambda f: -(f.get('opportunity_score', 0) or 0)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Registro diario
# ─────────────────────────────────────────────────────────────────────────────

def log_opportunities(fichas, signals, index_snapshot=None, cross_market_snapshot=None):
    """
    Guarda data/opportunities_log.json (dict {fecha: [oportunidad, ...]}) con
    el snapshot completo de cada ticker que el dashboard mostró ese día como
    Oportunidad real, incluyendo:
      - scores V1 (macro/técnico/fundamental/final) desde la ficha
      - scores V2 (AQ, ES, score_final_v2, ranking_accionable, alignment,
        weekly/monthly trend) cruzados desde `signals` por ticker
      - R/R, stop/target, predicción 5d/10d/21d + confianza
      - nivel del índice de su mercado ese día (actual/ret_dia/ret_anual)
      - régimen cross-market del día (RISK_ON/OFF/NEUTRAL, trend SP500)
    """
    if not fichas:
        return

    shown = _triple_filter(fichas)
    if not shown:
        logger.info("[opportunities_log] Sin oportunidades con convicción suficiente hoy — nada para registrar")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    sig_by_ticker = {s.get('ticker'): s for s in (signals or [])}
    index_snapshot = index_snapshot or {}
    cross_market_snapshot = cross_market_snapshot or {}

    records = []
    for f in shown:
        ticker = f.get('ticker')
        sd = sig_by_ticker.get(ticker, {})
        mkt = f.get('market')
        records.append({
            "ticker": ticker,
            "empresa": f.get('empresa'),
            "mercado": mkt,
            "signal_v2": f.get('signal_v2') or f.get('signal'),
            "precio": f.get('precio'),
            "entrada": f.get('entrada'), "stop": f.get('stop'), "target": f.get('target'),
            "riesgo_pct": f.get('riesgo'), "reward_pct": f.get('reward'), "rr_ratio": f.get('rr'),
            # ── Scores V1 (desde la ficha) ──
            "score_macro": f.get('score_macro'),
            "score_tecnico": f.get('score_tec'),
            "score_fundamental": f.get('score_fund'),
            "score_final_v1": f.get('score_final'),
            # ── Scores V2 (desde signals — no están en la ficha) ──
            "asset_quality": sd.get('asset_quality'),
            "entry_score": sd.get('entry_score'),
            "score_final_v2": sd.get('score_final_v2'),
            "ranking_accionable": sd.get('ranking_accionable'),
            "volatility_score": sd.get('volatility_score'),
            "adx": sd.get('adx'),
            "alignment_score": sd.get('alignment_score'),
            "alignment_label": sd.get('alignment_label'),
            "weekly_trend": sd.get('weekly_trend'),
            "monthly_trend": sd.get('monthly_trend'),
            "rsi": f.get('rsi'),
            "momentum_21d": f.get('momentum'),
            # ── Predictor ──
            "pred_5d": f.get('pred_5d'), "pred_10d": f.get('pred_10d'), "pred_21d": f.get('pred_21d'),
            "pred_confidence": f.get('pred_confidence'), "pred_signal": f.get('pred_signal'),
            "opportunity_score": f.get('opportunity_score'),
            # ── Contexto de mercado ese día ──
            "index_snapshot": index_snapshot.get(mkt, {}),
            "cross_market": {
                "regime": cross_market_snapshot.get('regime'),
                "sp500_trend": cross_market_snapshot.get('sp500_trend'),
                "sp500_trend_score": cross_market_snapshot.get('sp500_trend_score'),
            },
        })

    log = _load_log()
    log[today] = records  # overwrite si ya corrió hoy (dedupe por fecha, igual que tracker.py)
    cutoff = (datetime.now() - timedelta(days=MAX_DAYS)).strftime("%Y-%m-%d")
    log = {d: v for d, v in log.items() if d >= cutoff}

    os.makedirs("data", exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=1)

    logger.info(f"[opportunities_log] {len(records)} oportunidades registradas para {today} ({len(log)} días en historial)")
    _push_to_github(LOG_PATH, f"auto: opportunities_log {today} ({len(records)} oportunidades)")


def _load_log():
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _push_to_github(path, message):
    """Mismo patrón GET-sha/PUT que el resto del proyecto (ver §3 de la arquitectura)."""
    try:
        gh_token = os.environ.get("GH_TOKEN", "")
        if not gh_token:
            return
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        url = f"https://api.github.com/repos/{GH_REPO_FULL}/contents/{path}"
        headers = {"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"}
        r = requests.get(url, headers=headers, timeout=10)
        sha = r.json().get("sha") if r.status_code == 200 else None
        payload = {"message": message, "content": b64_content}
        if sha:
            payload["sha"] = sha
        r2 = requests.put(url, headers=headers, json=payload, timeout=20)
        if r2.status_code not in (200, 201):
            logger.warning(f"Push {path} falló: {r2.status_code} {r2.text[:200]}")
    except Exception as e:
        logger.warning(f"No se pudo pushear {path} a GitHub: {e}")


def sync_from_github():
    """Descarga opportunities_log.json fresco de GitHub. Llamar al arrancar Railway."""
    gh_token = os.environ.get("GH_TOKEN", "")
    if not gh_token:
        return
    try:
        url = f"https://api.github.com/repos/{GH_REPO_FULL}/contents/{LOG_PATH}"
        headers = {"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"}
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            content = base64.b64decode(r.json()["content"]).decode("utf-8")
            os.makedirs("data", exist_ok=True)
            with open(LOG_PATH, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info("opportunities_log.json sincronizado desde GitHub")
    except Exception as e:
        logger.warning(f"No se pudo sincronizar opportunities_log.json: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Evaluación de efectividad — cruza el log contra precios reales (CSVs)
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_opportunities(price_data: dict, ticker_cols: dict = None, horizons=(5, 10, 21)) -> dict:
    """
    Calcula el retorno REAL a 5/10/21 días de cada oportunidad registrada que
    ya tenga suficiente "futuro" en los CSVs, agrega win_rate/avg_ret global,
    por mercado y por tipo de señal, y guarda el resultado en
    data/opportunities_effectiveness.json (también persistido a GitHub).

    Se puede llamar en cualquier momento (ej. 1x/día desde pipeline.py) — si
    todavía no hay oportunidades con suficiente antigüedad, devuelve status
    "insuficiente_historia" sin romper nada.
    """
    from src.backtester import _build_price_index, _get_future_prices  # reuso — evita duplicar lógica

    log = _load_log()
    if not log:
        return {"status": "sin_datos", "detail": "Todavía no hay oportunidades registradas"}

    price_index = _build_price_index(price_data or {}, ticker_cols or {})

    evaluated = []
    for date, recs in log.items():
        for rec in recs:
            ticker = rec.get("ticker")
            precio_entry = rec.get("precio") or 0
            if not ticker or precio_entry <= 0:
                continue
            future_prices = _get_future_prices(ticker, date, price_index, max_horizon=max(horizons) + 4)
            if not future_prices:
                continue  # sin precios futuros suficientes todavía (o ticker sin datos)
            ev = dict(rec)
            ev["date"] = date
            for h in horizons:
                if len(future_prices) >= h:
                    exit_price = future_prices[h - 1]
                    ev[f"ret_real_{h}d"] = round((exit_price / precio_entry - 1) * 100, 2)
                else:
                    ev[f"ret_real_{h}d"] = None
            evaluated.append(ev)

    total_registradas = sum(len(v) for v in log.values())

    if not evaluated:
        result = {
            "status": "insuficiente_historia",
            "total_oportunidades_registradas": total_registradas,
            "evaluables_aun": 0,
            "detail": "Ninguna oportunidad registrada tiene todavía suficiente precio futuro para evaluar",
        }
    else:
        def _agg(items, horizon):
            rets = [e[f"ret_real_{horizon}d"] for e in items if e.get(f"ret_real_{horizon}d") is not None]
            if not rets:
                return None
            wins = sum(1 for r in rets if r > 0)
            return {
                "n": len(rets),
                "win_rate_pct": round(wins / len(rets) * 100, 1),
                "avg_ret_pct": round(sum(rets) / len(rets), 2),
                "best_pct": round(max(rets), 2),
                "worst_pct": round(min(rets), 2),
            }

        by_market = {
            mkt: {f"{h}d": _agg([e for e in evaluated if e.get("mercado") == mkt], h) for h in horizons}
            for mkt in ("MERVAL", "BOVESPA", "SP500")
        }
        by_signal = {
            sig: {f"{h}d": _agg([e for e in evaluated if e.get("signal_v2") == sig], h) for h in horizons}
            for sig in sorted(set(e.get("signal_v2") for e in evaluated if e.get("signal_v2")))
        }

        result = {
            "status": "ok",
            "generated": datetime.now().isoformat(),
            "total_oportunidades_registradas": total_registradas,
            "total_evaluadas": len(evaluated),
            "global": {f"{h}d": _agg(evaluated, h) for h in horizons},
            "by_market": by_market,
            "by_signal": by_signal,
        }

    try:
        os.makedirs("data", exist_ok=True)
        with open(EFFECTIVENESS_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        _push_to_github(EFFECTIVENESS_PATH, f"auto: opportunities_effectiveness {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        logger.warning(f"No se pudo guardar opportunities_effectiveness.json: {e}")

    logger.info(f"[opportunities_log] Evaluación: {result.get('status')} — {result.get('total_evaluadas', 0)} oportunidades evaluadas de {total_registradas} registradas")
    return result
