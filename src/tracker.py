"""
src/tracker.py
Mejoras 7 y 10:
  7. Histórico de señales (últimos 30 días)
  10. Tracking de aciertos del modelo
 
Se llama desde pipeline.py después de analyzer.
"""
 
import json
import os
import logging
from datetime import datetime, timedelta
 
logger = logging.getLogger(__name__)
 
HISTORY_PATH = "data/signals_history.json"
ACCURACY_PATH = "data/accuracy_report.json"
 
 
def update_history(signals: list[dict], max_days: int = 60):
    """
    Mejora 7: Acumula señales diarias en signals_history.json.
    Formato: { "2026-05-16": [ {ticker, signal, signal_v2, precio, score_final_v2, ranking_accionable}, ... ] }
    """
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
 
    history = {}
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH) as f:
                history = json.load(f)
        except Exception:
            history = {}
 
    today = datetime.now().strftime("%Y-%m-%d")
 
    # Guardar snapshot de hoy (solo campos esenciales para no inflar el JSON)
    history[today] = [
        {
            # ── Identificación ──────────────────────────────────────
            "ticker":        s["ticker"],
            "mercado":       s.get("mercado", ""),
            "sector":        s.get("sector", ""),
            # ── Precio ──────────────────────────────────────────────
            "precio":        s.get("precio_actual", 0),
            # ── Señales ─────────────────────────────────────────────
            "signal":        s.get("signal", ""),
            "signal_v2":     s.get("signal_v2", ""),
            # ── Scores ──────────────────────────────────────────────
            "score_v1":      s.get("score_final", 0),
            "score_v2":      s.get("score_final_v2", 0),
            "score_macro":   s.get("score_macro", 0),
            "score_tecnico": s.get("score_tecnico", 0),
            "score_fund":    s.get("score_fundamental", 0),
            "score_sectorial": s.get("score_sectorial", 0),
            "ranking":       s.get("ranking_accionable", 0),
            "rr_ratio":      s.get("rr_ratio", 0),
            "asset_quality": s.get("asset_quality", 0),
            "entry_score":   s.get("entry_score", 0),
            # ── Stops / Targets (para backtesting) ──────────────────
            "atr_stop":      s.get("atr_stop", 0),
            "atr_target":    s.get("atr_target", 0),
            "atr":           s.get("atr", 0),
            # ── Predictor (para backtesting accuracy) ───────────────
            "pred_5d":       s.get("pred_5d"),
            "pred_21d":      s.get("pred_21d"),
            "pred_signal":   s.get("pred_signal", ""),
            "pred_confidence": s.get("pred_confidence"),
            # ── Indicadores técnicos clave ───────────────────────────
            "rsi":           s.get("rsi", 0),
            "ret_anual":     s.get("ret_anual", 0),
            "ret_mes":       s.get("ret_mes", 0),
            # ── Factor decomposition (Fase 6) ──────────────────────────
            "factor_contrib":    s.get("factor_contrib", {}),
            "factor_dominante":  s.get("factor_dominante", ""),
        }
        for s in signals
    ]
 
    # Purgar días viejos
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    history = {d: v for d, v in history.items() if d >= cutoff}
 
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, ensure_ascii=False, indent=1)
 
    logger.info(f"Histórico actualizado: {len(history)} días guardados")
    return history
 
 
def compute_accuracy(history: dict) -> dict:
    """
    Mejora 10: Calcula hit rate por tipo de señal.
    
    Para cada señal emitida hace N días, compara el precio de ese día
    con el precio actual (último día en el historial).
    
    Retorna:
    {
      "⭐ COMPRA FUERTE": { "count": 5, "avg_ret_5d": 2.1, "avg_ret_20d": 5.3, "hit_rate_20d": 0.80 },
      "🟢 COMPRA": { ... },
      ...
    }
    """
    if not history:
        return {}
 
    sorted_dates = sorted(history.keys())
    if len(sorted_dates) < 6:
        logger.info("Menos de 6 días de historia, accuracy no disponible aún")
        return {}
 
    latest_date = sorted_dates[-1]
    latest_prices = {s["ticker"]: s["precio"] for s in history[latest_date]}
 
    results = {}
    lookback_windows = [5, 10, 20]
 
    for date_idx, date in enumerate(sorted_dates[:-5]):  # excluir últimos 5 días
        for s in history[date]:
            ticker = s["ticker"]
            signal = s.get("signal_v2") or s.get("signal", "")
            precio_entry = s["precio"]
 
            if not signal or precio_entry <= 0:
                continue
 
            if signal not in results:
                results[signal] = {"count": 0, "returns": {w: [] for w in lookback_windows}}
 
            results[signal]["count"] += 1
 
            for w in lookback_windows:
                future_idx = date_idx + w
                if future_idx < len(sorted_dates):
                    future_date = sorted_dates[future_idx]
                    future_prices = {ss["ticker"]: ss["precio"] for ss in history[future_date]}
                    future_price = future_prices.get(ticker)
                    if future_price and future_price > 0:
                        ret = ((future_price / precio_entry) - 1) * 100
                        results[signal]["returns"][w].append(ret)
 
    # Calcular estadísticas + Expected Value
    report = {}
    for signal, data in results.items():
        entry = {"count": data["count"]}
        for w in lookback_windows:
            rets = data["returns"][w]
            if rets:
                avg_ret = round(sum(rets) / len(rets), 2)
                win_rate = round(len([r for r in rets if r > 0]) / len(rets), 2)
                loss_rate = round(1 - win_rate, 2)
                avg_win = round(sum([r for r in rets if r > 0]) / max(1, len([r for r in rets if r > 0])), 2)
                avg_loss = round(sum([abs(r) for r in rets if r < 0]) / max(1, len([r for r in rets if r < 0])), 2)
                # Expected Value = (WinRate × AvgWin) - (LossRate × AvgLoss)
                expected_value = round(win_rate * avg_win - loss_rate * avg_loss, 2)
                entry[f"avg_ret_{w}d"] = avg_ret
                entry[f"hit_rate_{w}d"] = win_rate
                entry[f"avg_win_{w}d"] = avg_win
                entry[f"avg_loss_{w}d"] = avg_loss
                entry[f"expected_value_{w}d"] = expected_value
                entry[f"samples_{w}d"] = len(rets)
            else:
                entry[f"avg_ret_{w}d"] = None
                entry[f"hit_rate_{w}d"] = None
                entry[f"avg_win_{w}d"] = None
                entry[f"avg_loss_{w}d"] = None
                entry[f"expected_value_{w}d"] = None
                entry[f"samples_{w}d"] = 0
        report[signal] = entry
 
    # Guardar reporte
    os.makedirs(os.path.dirname(ACCURACY_PATH), exist_ok=True)
    accuracy_output = {
        "generated": datetime.now().isoformat(),
        "total_days_history": len(history),
        "signals": report,
    }
    with open(ACCURACY_PATH, "w") as f:
        json.dump(accuracy_output, f, ensure_ascii=False, indent=2)
 
    logger.info(f"Accuracy report generado: {len(report)} tipos de señal analizados")
    return report
 
 
# ─────────────────────────────────────────────
# Mejora: Alertas operativas sobre portfolio
# ─────────────────────────────────────────────
 
PORTFOLIO_PATH = "data/portfolio.json"
ALERTS_PATH = "data/portfolio_alerts.json"
 
 
def check_portfolio_alerts(signals: list[dict]) -> list[dict]:
    """
    Chequea posiciones del portfolio contra precios actuales y señales.
    Genera alertas de:
    - Stop loss tocado (precio ≤ stop)
    - Target alcanzado (precio ≥ target)
    - ATR stop tocado (precio ≤ atr_stop del modelo)
    - Cambio de señal V2 a Venta
    - P&L por posición
    """
    if not os.path.exists(PORTFOLIO_PATH):
        logger.info("No hay portfolio.json, saltando alertas")
        return []
 
    try:
        with open(PORTFOLIO_PATH) as f:
            portfolio = json.load(f)
    except Exception as e:
        logger.warning(f"Error leyendo portfolio: {e}")
        return []
 
    positions = portfolio.get("positions", [])
    if not positions:
        return []
 
    # Crear mapa de señales actuales por ticker
    signal_map = {}
    for s in signals:
        signal_map[s["ticker"]] = s
        # También mapear sin sufijo (.BA, .SA) para CEDEARs
        base_ticker = s["ticker"].replace(".BA", "").replace(".SA", "")
        signal_map[base_ticker] = s
 
    alerts = []
 
    for pos in positions:
        ticker = pos["ticker"]
        precio_compra = pos.get("precio_compra") or pos.get("precio_compra_usd", 0)
        cantidad = pos["cantidad"]
 
        # Buscar en señales
        sig = signal_map.get(ticker)
        if sig is None:
            # Intentar sin sufijo
            base = ticker.replace(".BA", "").replace(".SA", "")
            sig = signal_map.get(base)
 
        if sig is None:
            alerts.append({
                "ticker": ticker,
                "tipo": "⚠️ SIN DATOS",
                "mensaje": f"{ticker} no encontrado en señales del modelo",
            })
            continue
 
        precio_actual = sig.get("precio_actual", 0)
 
        # Usar precios USD del broker si disponibles (más confiable que ARS con CCL)
        precio_compra_usd = pos.get("precio_compra_usd", 0)
        precio_actual_usd = pos.get("precio_actual_usd", 0)
        rend_usd_broker   = pos.get("rend_usd", None)
 
        if precio_compra_usd > 0 and precio_actual_usd > 0:
            pnl_pct = round(((precio_actual_usd / precio_compra_usd) - 1) * 100, 2)
            pnl_abs = round(rend_usd_broker if rend_usd_broker is not None
                            else (precio_actual_usd - precio_compra_usd) * cantidad, 2)
        elif precio_actual <= 0:
            continue
        else:
            pnl_pct = round(((precio_actual / precio_compra) - 1) * 100, 2) if precio_compra > 0 else 0
            pnl_abs = round((precio_actual - precio_compra) * cantidad, 2)
 
        # Stop loss manual
        stop = pos.get("stop_loss")
        if stop and precio_actual <= stop:
            alerts.append({
                "ticker": ticker,
                "tipo": "🔴 STOP LOSS",
                "mensaje": f"{ticker}: precio {precio_actual} ≤ stop {stop} — SALIR",
                "pnl_pct": pnl_pct,
            })
 
        # ATR stop del modelo
        atr_stop = sig.get("atr_stop", 0)
        if atr_stop > 0 and precio_actual <= atr_stop:
            alerts.append({
                "ticker": ticker,
                "tipo": "🟠 ATR STOP",
                "mensaje": f"{ticker}: precio {precio_actual} ≤ ATR stop {atr_stop} — revisar salida",
                "pnl_pct": pnl_pct,
            })
 
        # Target manual
        target = pos.get("target")
        if target and precio_actual >= target:
            alerts.append({
                "ticker": ticker,
                "tipo": "🟢 TARGET",
                "mensaje": f"{ticker}: precio {precio_actual} ≥ target {target} — tomar ganancia",
                "pnl_pct": pnl_pct,
            })
 
        # ATR target del modelo
        atr_target = sig.get("atr_target", 0)
        if atr_target > 0 and precio_actual >= atr_target:
            alerts.append({
                "ticker": ticker,
                "tipo": "🟢 ATR TARGET",
                "mensaje": f"{ticker}: precio {precio_actual} ≥ ATR target {atr_target}",
                "pnl_pct": pnl_pct,
            })
 
        # Señal de venta
        sig_v2 = sig.get("signal_v2", "")
        if "VENTA" in sig_v2:
            alerts.append({
                "ticker": ticker,
                "tipo": "🟠 SEÑAL VENTA",
                "mensaje": f"{ticker}: señal V2 = {sig_v2} — considerar reducción",
                "pnl_pct": pnl_pct,
            })
 
        # ── REGLAS DE SALIDA ──
 
        # Partial take profit: si ganancia > 20%, sugerir vender 50%
        if pnl_pct >= 20:
            alerts.append({
                "ticker": ticker,
                "tipo": "⭐ TAKE PROFIT PARCIAL",
                "mensaje": f"{ticker}: ganancia {pnl_pct}% — considerar vender 50% y mover stop a breakeven",
                "pnl_pct": pnl_pct,
            })
 
        # Trailing stop: si ganó >10% pero cayó >5% desde el máximo reciente
        max_12m = sig.get("max_12m", 0)
        if max_12m > 0 and pnl_pct > 10:
            caida_desde_max = ((precio_actual - max_12m) / max_12m) * 100
            if caida_desde_max < -5:
                alerts.append({
                    "ticker": ticker,
                    "tipo": "🟠 TRAILING STOP",
                    "mensaje": f"{ticker}: en ganancia {pnl_pct}% pero cayó {caida_desde_max:.1f}% desde máximo ({max_12m}) — proteger ganancias",
                    "pnl_pct": pnl_pct,
                })
 
        # Time stop: si lleva >10 días y no avanza (P&L entre -3% y +3%)
        fecha_compra = pos.get("fecha_compra")
        if fecha_compra:
            try:
                if isinstance(fecha_compra, str):
                    fc = datetime.fromisoformat(fecha_compra)
                else:
                    fc = fecha_compra
                dias_en_posicion = (datetime.now() - fc).days
                if dias_en_posicion >= 10 and -3 <= pnl_pct <= 3:
                    alerts.append({
                        "ticker": ticker,
                        "tipo": "⏰ TIME STOP",
                        "mensaje": f"{ticker}: {dias_en_posicion} días en posición, P&L {pnl_pct}% — capital inmovilizado, evaluar salida",
                        "pnl_pct": pnl_pct,
                    })
            except Exception:
                pass
 
        # Acción sugerida para dashboard
        if pnl_pct <= -8 or (atr_stop > 0 and precio_actual <= atr_stop):
            if "COMPRA" in sig_v2:
                accion = "🔴 STOP (señal positiva, evaluar recompra)"
            else:
                accion = "🔴 VENDER"
        elif "VENTA" in sig_v2:
            accion = "🟡 REDUCIR"
        elif pnl_pct >= 20:
            accion = "⭐ PARCIAL"
        elif pnl_pct > 5 and sig_v2 and "COMPRA" in sig_v2:
            accion = "⭐ AGREGAR"
        else:
            accion = "🟢 HOLD"
 
        # Siempre agregar resumen de P&L
        alerts.append({
            "ticker": ticker,
            "tipo": "📊 P&L",
            "mensaje": f"{ticker}: compra {precio_compra} → actual {precio_actual} ({'+' if pnl_pct >= 0 else ''}{pnl_pct}%) — {sig.get('signal_v2', sig.get('signal', ''))}",
            "pnl_pct": pnl_pct,
            "pnl_abs": pnl_abs,
            "horizonte": sig.get("horizonte", ""),
            "consenso": sig.get("consenso", ""),
            "ranking": sig.get("ranking_accionable", 0),
            "accion": accion,
            "atr_stop": atr_stop,
            "atr_target": sig.get("atr_target", 0),
            "signal_v2": sig.get("signal_v2", ""),
            "rr_ratio": sig.get("rr_ratio", 0),
            "sector": sig.get("sector", ""),
            "mercado": sig.get("mercado", ""),
        })
 
    # Guardar alertas
    os.makedirs(os.path.dirname(ALERTS_PATH), exist_ok=True)
    output = {
        "generated": datetime.now().isoformat(),
        "total_positions": len(positions),
        "alerts": alerts,
    }
    with open(ALERTS_PATH, "w") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
 
    # Contar alertas críticas
    criticas = [a for a in alerts if a["tipo"] in ("🔴 STOP LOSS", "🟠 ATR STOP", "🟠 SEÑAL VENTA")]
    logger.info(f"Portfolio: {len(positions)} posiciones, {len(criticas)} alertas críticas")

    return alerts



def update_portfolio_usd(signals: list[dict] = None, brl_usd_ext: float = 0.0) -> None:
    """
    DESACTIVADO hasta implementar cedear_cierres.csv con precios ARS de BYMA.
    El portfolio.json se actualiza solo via /api/compra y /api/venta.
    Los precios actuales se calculan en _handle_get_portfolio desde CSVs.
    """
    logger.info("update_portfolio_usd: desactivado — portfolio no modificado por pipeline")
    return


def _push_portfolio_to_github():
    """Pushea portfolio.json actualizado a GitHub."""
    try:
        import requests as req_lib
        import base64 as b64

        gh_token = os.environ.get("GH_TOKEN", "")
        if not gh_token:
            return

        with open(PORTFOLIO_PATH) as f:
            content = f.read()

        b64_content = b64.b64encode(content.encode()).decode()

        repo = "Brunogatti79/inversiones-bursatiles"
        path = PORTFOLIO_PATH
        url  = f"https://api.github.com/repos/{repo}/contents/{path}"

        headers = {
            "Authorization": f"token {gh_token}",
            "Content-Type": "application/json"
        }

        r = req_lib.get(url, headers=headers, timeout=10)
        sha = r.json().get("sha", "") if r.ok else ""

        payload = {
            "message": f"auto: portfolio USD actualizado {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": b64_content
        }

        if sha:
            payload["sha"] = sha

        req_lib.put(url, json=payload, headers=headers, timeout=15)

        logger.info("Portfolio.json pusheado a GitHub")

    except Exception as e:
        logger.warning(f"No se pudo pushear portfolio: {e}")

 
