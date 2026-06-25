"""
src/quality_check.py — Control de Calidad por Oposición
Valida consistencia interna de señales, scores y datos antes de publicar.
3 niveles:
  1. Validación cruzada automática (pre-dashboard)
  2. Semáforo visual por acción en el dashboard
  3. Reporte de integridad para Telegram

REFACTOR DE SEVERIDAD (Prioridad 4, roadmap externo, 25/06/2026):
  Hasta hoy cada check solo tenía "nivel" (info/warning/critical), y
  "estructural" (¿esto es un dato roto, o comportamiento esperado del
  modelo?) se inferí­a por fuera, comparando el NOMBRE del check contra un
  string hardcodeado ("V1 vs V2 contradicción") en el loop de agregación.
  Esa es exactamente la clase de bug que causó el falso positivo del kill
  switch en su primera corrida real (24/06/2026, ver model_version.py
  changelog 4.3): la "estructuralidad" no era una propiedad del check, era
  una inferencia frágil hecha en otro lugar del código.

  Ahora cada check declara explícitamente, en su propio dict, 2 dimensiones
  independientes (mismo criterio que pidió la devolución externa):
    - "nivel"          : info | warning | critical   (severidad, sin cambios)
    - "categoria"      : data | model | signal        (NUEVO — de qué habla)
    - "es_estructural" : bool                         (NUEVO — ¿dato roto?)

  "categoria":
    data   -> el check sospecha de un dato faltante/corrupto/inusual
              (precio inválido, score macro en fallback, variación extrema)
    model  -> tensión esperada entre 2 sub-sistemas que miden cosas
              distintas a propósito (V1 vs V2, RSI contrarian vs señal,
              modelo vs predictor) -- no implica nada roto
    signal -> característica informativa de la señal en sí (R/R, RS,
              stress, volatilidad) -- contexto, no alerta de integridad

  "es_estructural" es True solo cuando el check detecta directamente un
  DATO roto/ausente que invalida la señal (precio<=0, índice sin datos).
  El kill switch (confidence_score.py) sigue contando SOLO estos -- el
  cómputo de resumen['criticas_estructurales'] abajo es ahora 100%
  data-driven (lee es_estructural de cada check), no infiere nada por
  nombre. Mismo resultado que antes para los checks existentes (V1vsV2
  sigue sin contar, precio inválido e índice sin datos siguen contando) --
  esto es un refactor de cómo se calcula, no un cambio de qué cuenta.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# NIVEL 1 — Validación cruzada de datos
# ─────────────────────────────────────────────

def validar_señales(signals: list[dict], index_stats: dict = None) -> dict:
    """
    Ejecuta todos los chequeos de consistencia sobre las señales generadas.
    Retorna dict con alertas por ticker y resumen global.
    """
    alertas = {}  # ticker -> list of alertas
    resumen = {
        "total_alertas": 0, "criticas": 0, "criticas_estructurales": 0,
        "advertencias": 0, "ok": 0,
        "por_categoria": {"data": 0, "model": 0, "signal": 0},
    }

    for s in signals:
        ticker = s.get("ticker", "???")
        checks = []

        # ── CHECK 1: Momentum vs Retorno Mensual ──
        mom = s.get("momentum_21d", 0) or 0
        ret_mes = s.get("ret_mes", 0) or 0
        if mom != 0 and ret_mes != 0:
            # Si tienen signos opuestos y diferencia > 10pp
            if (mom > 0 and ret_mes < 0) or (mom < 0 and ret_mes > 0):
                diff = abs(mom - ret_mes)
                if diff > 10:
                    checks.append({
                        "tipo": "⚠️ ADVERTENCIA",
                        "check": "Momentum vs Ret.Mensual",
                        "detalle": f"Momentum 21d={mom:+.1f}% pero Ret.Mes={ret_mes:+.1f}% (signos opuestos, diff={diff:.0f}pp)",
                        "nivel": "warning", "categoria": "data", "es_estructural": False,
                    })
                elif diff > 5:
                    checks.append({
                        "tipo": "ℹ️ INFO",
                        "check": "Momentum vs Ret.Mensual",
                        "detalle": f"Momentum 21d={mom:+.1f}% vs Ret.Mes={ret_mes:+.1f}% (leve divergencia)",
                        "nivel": "info", "categoria": "data", "es_estructural": False,
                    })

        # ── CHECK 2: Señal vs Retorno Anual extremo ──
        signal = s.get("signal", "")
        ret_anual = s.get("ret_anual", 0) or 0
        if "COMPRA" in signal and ret_anual < -50:
            checks.append({
                "tipo": "ℹ️ INFO",
                "check": "Señal vs Caída anual",
                "detalle": f"Señal={signal} pero Ret.Anual={ret_anual:+.1f}% — verificar tesis de valor",
                "nivel": "info", "categoria": "signal", "es_estructural": False,
            })
        if "VENTA" in signal and ret_anual > 80:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "Señal vs Suba anual",
                "detalle": f"Señal={signal} pero Ret.Anual={ret_anual:+.1f}% — posible toma de ganancias válida",
                "nivel": "warning", "categoria": "signal", "es_estructural": False,
            })

        # ── CHECK 3: RSI vs Señal ──
        rsi = s.get("rsi", 50) or 50
        if rsi > 75 and "COMPRA" in signal:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "RSI sobrecompra + Compra",
                "detalle": f"RSI={rsi:.0f} (sobrecompra) pero señal={signal}",
                "nivel": "warning", "categoria": "model", "es_estructural": False,
            })
        if rsi < 25 and "VENTA" in signal:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "RSI sobreventa + Venta",
                "detalle": f"RSI={rsi:.0f} (sobreventa) pero señal={signal}",
                "nivel": "warning", "categoria": "model", "es_estructural": False,
            })

        # ── CHECK 4: Predicción vs Señal del modelo ──
        pred_signal = s.get("pred_signal", "")
        if pred_signal:
            if "COMPRA" in signal and "BAJA" in pred_signal:
                checks.append({
                    "tipo": "⚠️ ADVERTENCIA",
                    "check": "Modelo vs Predicción",
                    "detalle": f"Modelo={signal} pero Predicción={pred_signal} — señales en conflicto",
                    "nivel": "warning", "categoria": "model", "es_estructural": False,
                })
            if "VENTA" in signal and "SUBA" in pred_signal:
                checks.append({
                    "tipo": "⚠️ ADVERTENCIA",
                    "check": "Modelo vs Predicción",
                    "detalle": f"Modelo={signal} pero Predicción={pred_signal} — señales en conflicto",
                    "nivel": "warning", "categoria": "model", "es_estructural": False,
                })

        # ── CHECK 5: V1 vs V2 contradicción fuerte ──
        # categoria="model", es_estructural=False a propósito: V1 y V2 miden
        # cosas distintas por diseño (calidad de activo vs timing de
        # entrada) -- que discrepen es comportamiento esperado, no un dato
        # roto. Ver incidente 24/06/2026 en model_version.py.
        signal_v2 = s.get("signal_v2", "")
        if signal_v2:
            if "COMPRA" in signal and "VENTA" in signal_v2:
                checks.append({
                    "tipo": "🔴 CRÍTICA",
                    "check": "V1 vs V2 contradicción",
                    "detalle": f"V1={signal} vs V2={signal_v2} — modelos contradictorios",
                    "nivel": "critical", "categoria": "model", "es_estructural": False,
                })
            elif "VENTA" in signal and "COMPRA" in signal_v2:
                checks.append({
                    "tipo": "🔴 CRÍTICA",
                    "check": "V1 vs V2 contradicción",
                    "detalle": f"V1={signal} vs V2={signal_v2} — modelos contradictorios",
                    "nivel": "critical", "categoria": "model", "es_estructural": False,
                })

        # ── CHECK 6: Precio sospechoso ──
        # categoria="data", es_estructural=True: esto SÍ es un dato roto
        # (precio<=0 invalida cualquier cálculo de retorno/stop/target).
        precio = s.get("precio_actual", 0) or 0
        if precio <= 0:
            checks.append({
                "tipo": "🔴 CRÍTICA",
                "check": "Precio inválido",
                "detalle": f"Precio={precio} — dato corrupto o faltante",
                "nivel": "critical", "categoria": "data", "es_estructural": True,
            })

        max_12m = s.get("max_12m", 0) or 0
        min_12m = s.get("min_12m", 0) or 0
        if max_12m > 0 and min_12m > 0 and precio > 0:
            if precio > max_12m * 1.05:
                checks.append({
                    "tipo": "⚠️ ADVERTENCIA",
                    "check": "Precio > Máx 12M",
                    "detalle": f"Precio={precio} supera Máx 12M={max_12m} — posible dato erróneo",
                    "nivel": "warning", "categoria": "data", "es_estructural": False,
                })
            if precio < min_12m * 0.95:
                checks.append({
                    "tipo": "⚠️ ADVERTENCIA",
                    "check": "Precio < Mín 12M",
                    "detalle": f"Precio={precio} debajo de Mín 12M={min_12m} — verificar dato",
                    "nivel": "warning", "categoria": "data", "es_estructural": False,
                })

        # ── CHECK 7: Score macro coherente ──
        score_macro = s.get("score_macro", 0) or 0
        if score_macro == 0 or score_macro == 44.0:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "Score macro default/cero",
                "detalle": f"Score macro={score_macro} — posible fallback, verificar xlsx",
                "nivel": "warning", "categoria": "data", "es_estructural": False,
            })

        # ── CHECK 8: R/R ratio extremo ──
        rr = s.get("rr_ratio", 0) or 0
        if rr > 5:
            checks.append({
                "tipo": "ℹ️ INFO",
                "check": "R/R muy alto",
                "detalle": f"R/R={rr:.2f} — verificar niveles de soporte/resistencia",
                "nivel": "info", "categoria": "signal", "es_estructural": False,
            })

        # ── CHECK 9: Variación diaria extrema (>15%) ──
        ret_sem = s.get("ret_sem", 0) or 0
        if abs(ret_sem) > 15:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "Variación semanal extrema",
                "detalle": f"Ret.Semanal={ret_sem:+.1f}% — movimiento inusual, verificar dato",
                "nivel": "warning", "categoria": "data", "es_estructural": False,
            })

        # ── CHECK 10: Score fundamental sin datos ──
        s_fund = s.get("score_fundamental", 50) or 50
        if s_fund == 50.0 and s.get("upside_graham") is None and s.get("score_cuant") is None:
            checks.append({
                "tipo": "ℹ️ INFO",
                "check": "Fundamental sin datos",
                "detalle": f"Score fund={s_fund} (default) — sin Graham ni Score Cuant",
                "nivel": "info", "categoria": "data", "es_estructural": False,
            })

        # ── CHECK 11: Relative Strength débil con señal de compra ──
        rs = s.get("relative_strength", 1.0) or 1.0
        if rs < 0.85 and "COMPRA" in signal:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "RS débil + Compra",
                "detalle": f"RS vs índice={rs:.3f} (subperforma) pero señal={signal}",
                "nivel": "warning", "categoria": "signal", "es_estructural": False,
            })
        elif rs > 1.20 and "VENTA" in signal:
            checks.append({
                "tipo": "ℹ️ INFO",
                "check": "RS fuerte + Venta",
                "detalle": f"RS vs índice={rs:.3f} (superforma) pero señal={signal}",
                "nivel": "info", "categoria": "signal", "es_estructural": False,
            })

        # ── CHECK 12: Stress Index alto con señal de compra (solo MERVAL) ──
        stress = s.get("stress_index")
        if stress is not None and stress > 70 and "COMPRA" in signal:
            checks.append({
                "tipo": "⚠️ ADVERTENCIA",
                "check": "Stress alto + Compra",
                "detalle": f"Stress Index ARG={stress:.0f} (tensión/crisis) pero señal={signal}",
                "nivel": "warning", "categoria": "signal", "es_estructural": False,
            })

        # ── CHECK 13: ATR Percentile extremo ──
        atr_pct = s.get("atr_percentile", 50) or 50
        if atr_pct < 15 and "COMPRA" in signal:
            checks.append({
                "tipo": "ℹ️ INFO",
                "check": "Volatilidad muy alta + Compra",
                "detalle": f"ATR percentile={atr_pct:.0f} (vol extrema) — mayor riesgo en entry",
                "nivel": "info", "categoria": "signal", "es_estructural": False,
            })

        # Guardar alertas del ticker
        if checks:
            alertas[ticker] = checks
            for c in checks:
                _tally(resumen, c)
            resumen["total_alertas"] += len(checks)
        else:
            resumen["ok"] += 1

    # Validar index_stats si están disponibles
    if index_stats:
        for mercado, stats in index_stats.items():
            if not stats or stats.get("actual", 0) == 0:
                idx_check = {
                    "tipo": "🔴 CRÍTICA",
                    "check": "Índice sin datos",
                    "detalle": f"index_stats['{mercado}'] vacío — gráficos no se renderizarán",
                    "nivel": "critical", "categoria": "data", "es_estructural": True,
                }
                alertas[f"INDICE_{mercado.upper()}"] = [idx_check]
                _tally(resumen, idx_check)
                resumen["total_alertas"] += 1

    resumen["nivel_global"] = (
        "🔴 CRÍTICO" if resumen["criticas"] > 0
        else "⚠️ ADVERTENCIAS" if resumen["advertencias"] > 0
        else "✅ OK"
    )

    logger.info(f"[QUALITY] {resumen['nivel_global']}: "
                f"{resumen['criticas']} críticas ({resumen['criticas_estructurales']} estructurales), "
                f"{resumen['advertencias']} advertencias, "
                f"{resumen['ok']} OK de {len(signals)} acciones")

    return {"alertas": alertas, "resumen": resumen}


def _tally(resumen: dict, check: dict) -> None:
    """
    Único lugar donde se cuenta un check hacia el resumen -- antes esta
    lógica estaba duplicada (una vez para señales, otra para index_stats)
    con la condición de "estructural" hardcodeada por nombre en cada copia.
    Ahora lee es_estructural/categoria directamente del check, una sola
    vez. Default es_estructural=False si un check nuevo se agrega sin el
    campo -- falla cerrado (no cuenta de más para el kill switch) en vez
    de fallar abierto.
    """
    nivel = check.get("nivel")
    if nivel == "critical":
        resumen["criticas"] += 1
        if check.get("es_estructural", False):
            resumen["criticas_estructurales"] += 1
    elif nivel == "warning":
        resumen["advertencias"] += 1

    categoria = check.get("categoria", "signal")
    resumen["por_categoria"][categoria] = resumen["por_categoria"].get(categoria, 0) + 1


# ─────────────────────────────────────────────
# NIVEL 2 — Semáforo visual por acción
# ─────────────────────────────────────────────

def inyectar_semaforo(signals: list[dict], quality: dict) -> list[dict]:
    """
    Agrega campo 'quality_flag' y 'quality_alerts' a cada señal
    para que el generator pueda mostrar el semáforo.
    """
    alertas = quality.get("alertas", {})

    for s in signals:
        ticker = s.get("ticker", "")
        checks = alertas.get(ticker, [])

        if not checks:
            s["quality_flag"] = "🟢"
            s["quality_detail"] = "Datos consistentes"
            s["quality_alerts"] = []
        else:
            niveles = [c["nivel"] for c in checks]
            if "critical" in niveles:
                s["quality_flag"] = "🔴"
                s["quality_detail"] = f"{len(checks)} inconsistencia(s) detectada(s)"
            elif "warning" in niveles:
                s["quality_flag"] = "🟡"
                s["quality_detail"] = f"{len(checks)} advertencia(s)"
            else:
                s["quality_flag"] = "🟢"
                s["quality_detail"] = f"{len(checks)} nota(s) informativa(s)"
            s["quality_alerts"] = [c["detalle"] for c in checks]

    return signals


# ─────────────────────────────────────────────
# NIVEL 3 — Reporte de integridad para Telegram
# ─────────────────────────────────────────────

def generar_reporte_calidad(quality: dict) -> Optional[str]:
    """
    Genera texto formateado para enviar por Telegram.
    Retorna None si todo está OK (no enviar mensaje innecesario).
    """
    resumen = quality.get("resumen", {})
    alertas = quality.get("alertas", {})

    if resumen.get("total_alertas", 0) == 0:
        return None  # Todo OK, no molestar

    lineas = []
    lineas.append(f"🔍 CONTROL DE CALIDAD — {resumen['nivel_global']}")
    lineas.append(f"Críticas: {resumen['criticas']} | Advertencias: {resumen['advertencias']}")
    lineas.append("")

    # Mostrar primero las críticas, luego advertencias
    criticas = []
    warnings = []
    for ticker, checks in alertas.items():
        for c in checks:
            entry = f"• {ticker}: {c['detalle']}"
            if c["nivel"] == "critical":
                criticas.append(entry)
            elif c["nivel"] == "warning":
                warnings.append(entry)

    if criticas:
        lineas.append("🔴 CRÍTICAS:")
        lineas.extend(criticas[:10])  # Max 10
        if len(criticas) > 10:
            lineas.append(f"  ... y {len(criticas)-10} más")
        lineas.append("")

    if warnings:
        lineas.append("⚠️ ADVERTENCIAS:")
        lineas.extend(warnings[:15])  # Max 15
        if len(warnings) > 15:
            lineas.append(f"  ... y {len(warnings)-15} más")

    return "\n".join(lineas)
