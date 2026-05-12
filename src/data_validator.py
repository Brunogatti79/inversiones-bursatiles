"""
src/data_validator.py
Validación de consistencia y frescura de datos de mercado.

Controles:
  1. Frescura: último cierre debe ser el día hábil anterior (no feriado)
  2. Consistencia: variación diaria no puede repetirse ni superar ±15%
  3. Integridad: mínimo 80% de tickers con datos en la última fecha
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import pytz

logger = logging.getLogger(__name__)

# Feriados Argentina, Brasil y EEUU más relevantes
FERIADOS_ARG = {
    date(2026, 1, 1), date(2026, 3, 2), date(2026, 3, 3),   # Año nuevo, Carnaval
    date(2026, 4, 2), date(2026, 4, 3),                       # Malvinas, Jueves/Viernes Santo
    date(2026, 5, 1), date(2026, 5, 25),                      # Día del trabajo, Revolución
    date(2026, 6, 15), date(2026, 6, 20),                     # Güemes, Belgrano
    date(2026, 7, 9), date(2026, 8, 17),                      # Independencia, San Martín
    date(2026, 10, 12), date(2026, 11, 20),                   # Diversidad, Soberanía
    date(2026, 12, 8), date(2026, 12, 25),                    # Inmaculada, Navidad
}

FERIADOS_USA = {
    date(2026, 1, 1), date(2026, 1, 19),                      # Año nuevo, MLK
    date(2026, 2, 16), date(2026, 4, 3),                      # Presidents, Good Friday
    date(2026, 5, 25), date(2026, 7, 3),                      # Memorial, Independence
    date(2026, 9, 7), date(2026, 11, 26),                     # Labor, Thanksgiving
    date(2026, 12, 25),                                        # Navidad
}

FERIADOS_BRA = {
    date(2026, 1, 1), date(2026, 3, 2), date(2026, 3, 3),
    date(2026, 4, 3), date(2026, 4, 21),
    date(2026, 5, 1), date(2026, 6, 4),
    date(2026, 9, 7), date(2026, 10, 12),
    date(2026, 11, 2), date(2026, 11, 15),
    date(2026, 12, 25),
}

FERIADOS = {
    'MERVAL':  FERIADOS_ARG,
    'BOVESPA': FERIADOS_BRA,
    'SP500':   FERIADOS_USA,
}


def ultimo_dia_habil(market: str, ref_date: date = None) -> date:
    """
    Retorna el último día hábil anterior a ref_date para el mercado dado.
    Si ref_date es hoy, retorna el cierre más reciente disponible.
    """
    if ref_date is None:
        ref_date = datetime.now(pytz.UTC).date()

    feriados = FERIADOS.get(market, set())
    d = ref_date

    # Si es fin de semana o feriado, retroceder
    # Para datos del día: si son antes de las 22:00 UTC, el último dato es de ayer
    hora_utc = datetime.now(pytz.UTC).hour
    if hora_utc < 22:  # mercados aún no han cerrado o no se actualizó YF
        d = d - timedelta(days=1)

    # Retroceder hasta encontrar día hábil
    while d.weekday() >= 5 or d in feriados:  # 5=Sábado, 6=Domingo
        d = d - timedelta(days=1)

    return d


def validar_frescura(df: pd.DataFrame, market: str) -> dict:
    """
    Control 1: El último cierre debe ser del último día hábil.
    Tolerancia: máximo 3 días hábiles de atraso.
    """
    resultado = {'control': 'frescura', 'ok': True, 'warnings': [], 'errors': []}

    if df is None or df.empty:
        resultado['ok'] = False
        resultado['errors'].append(f"[{market}] DataFrame vacío")
        return resultado

    ultima_fecha = df.index[-1].date() if hasattr(df.index[-1], 'date') else df.index[-1]
    esperado = ultimo_dia_habil(market)

    diff_dias = (esperado - ultima_fecha).days

    if diff_dias == 0:
        resultado['warnings'].append(f"[{market}] ✅ Dato fresco: {ultima_fecha} (esperado: {esperado})")
    elif diff_dias <= 3:
        resultado['warnings'].append(
            f"[{market}] ⚠️ Dato con {diff_dias}d de atraso: {ultima_fecha} (esperado: {esperado})"
        )
    else:
        resultado['ok'] = False
        resultado['errors'].append(
            f"[{market}] ❌ Dato MUY desactualizado: {ultima_fecha} (esperado: {esperado}, atraso: {diff_dias}d)"
        )

    return resultado


def validar_consistencia(df: pd.DataFrame, market: str, index_col: str) -> dict:
    """
    Control 2: Variación diaria del índice no puede:
    - Ser idéntica dos días consecutivos (dato congelado)
    - Superar ±15% (dato corrupto)
    - Ser exactamente 0.0000% dos días seguidos
    """
    resultado = {'control': 'consistencia', 'ok': True, 'warnings': [], 'errors': []}

    if df is None or df.empty or not index_col or index_col not in df.columns:
        resultado['warnings'].append(f"[{market}] No se pudo validar consistencia (columna índice no encontrada)")
        return resultado

    serie = df[index_col].dropna()
    if len(serie) < 3:
        resultado['warnings'].append(f"[{market}] Serie muy corta para validar consistencia")
        return resultado

    # Calcular variaciones diarias últimas 5 sesiones
    variaciones = serie.pct_change().dropna().tail(5) * 100

    # Control: variación > ±15%
    extremas = variaciones[variaciones.abs() > 15]
    if not extremas.empty:
        resultado['ok'] = False
        for fecha, var in extremas.items():
            resultado['errors'].append(
                f"[{market}] ❌ Variación extrema detectada: {var:.2f}% el {fecha.date()} — posible dato corrupto"
            )

    # Control: misma variación exacta dos días consecutivos
    if len(variaciones) >= 2:
        for i in range(1, len(variaciones)):
            v_hoy   = round(float(variaciones.iloc[i]), 4)
            v_ayer  = round(float(variaciones.iloc[i-1]), 4)
            if v_hoy == v_ayer and abs(v_hoy) > 0.001:
                resultado['warnings'].append(
                    f"[{market}] ⚠️ Variación idéntica dos días seguidos: {v_hoy:.4f}% — posible dato congelado"
                )

    # Control: variación 0.0000% dos días consecutivos
    ceros = (variaciones.abs() < 0.0001).sum()
    if ceros >= 2:
        resultado['warnings'].append(
            f"[{market}] ⚠️ {ceros} días con variación ~0% — verificar actualización"
        )

    if resultado['ok'] and not resultado['errors']:
        resultado['warnings'].insert(0, f"[{market}] ✅ Consistencia OK — variación hoy: {float(variaciones.iloc[-1]):.2f}%")

    return resultado


def validar_integridad(df: pd.DataFrame, market: str, n_tickers_esperados: int) -> dict:
    """
    Control 3: Mínimo 80% de tickers con datos en la última fecha.
    """
    resultado = {'control': 'integridad', 'ok': True, 'warnings': [], 'errors': []}

    if df is None or df.empty:
        resultado['ok'] = False
        resultado['errors'].append(f"[{market}] DataFrame vacío")
        return resultado

    ultima_fila = df.iloc[-1]
    tickers_con_dato = ultima_fila.notna().sum()
    total_cols = len(df.columns)
    tasa = tickers_con_dato / total_cols if total_cols > 0 else 0

    if tasa >= 0.80:
        resultado['warnings'].append(
            f"[{market}] ✅ Integridad OK: {tickers_con_dato}/{total_cols} tickers con dato ({tasa:.0%})"
        )
    elif tasa >= 0.50:
        resultado['warnings'].append(
            f"[{market}] ⚠️ Integridad parcial: {tickers_con_dato}/{total_cols} tickers ({tasa:.0%})"
        )
    else:
        resultado['ok'] = False
        resultado['errors'].append(
            f"[{market}] ❌ Integridad baja: solo {tickers_con_dato}/{total_cols} tickers ({tasa:.0%}) — datos insuficientes"
        )

    return resultado


def validar_mercado(df: pd.DataFrame, market: str, index_col: str, n_tickers: int) -> dict:
    """
    Ejecuta los 3 controles para un mercado y retorna resumen consolidado.
    """
    r1 = validar_frescura(df, market)
    r2 = validar_consistencia(df, market, index_col)
    r3 = validar_integridad(df, market, n_tickers)

    todos_ok = r1['ok'] and r2['ok'] and r3['ok']
    todos_warnings = r1['warnings'] + r2['warnings'] + r3['warnings']
    todos_errors   = r1['errors']   + r2['errors']   + r3['errors']

    nivel = 'OK' if todos_ok and not todos_warnings else \
            'WARNING' if todos_ok else 'ERROR'

    for msg in todos_warnings + todos_errors:
        if 'ERROR' in nivel:
            logger.error(msg)
        else:
            logger.info(msg)

    return {
        'market':   market,
        'ok':       todos_ok,
        'nivel':    nivel,
        'warnings': todos_warnings,
        'errors':   todos_errors,
        'ultima_fecha': str(df.index[-1].date()) if df is not None and not df.empty else '—',
    }


def validar_todos(data: dict, index_cols: dict, n_tickers: dict) -> dict:
    """
    Valida los 3 mercados y retorna resumen global.
    data = {'merval': df, 'bovespa': df, 'sp500': df}
    index_cols = {'merval': 'ÍNDICE MERVAL', ...}
    n_tickers  = {'merval': 21, 'bovespa': 20, 'sp500': 24}
    """
    MARKET_MAP = {'merval': 'MERVAL', 'bovespa': 'BOVESPA', 'sp500': 'SP500'}
    resultados = {}

    for key, df in data.items():
        market = MARKET_MAP.get(key, key.upper())
        icol   = index_cols.get(key, '')
        nt     = n_tickers.get(key, 20)
        resultados[key] = validar_mercado(df, market, icol, nt)

    hay_errores   = any(not r['ok'] for r in resultados.values())
    hay_warnings  = any(r['warnings'] for r in resultados.values())
    nivel_global  = 'ERROR' if hay_errores else 'WARNING' if hay_warnings else 'OK'

    logger.info(f"[VALIDACIÓN] Nivel global: {nivel_global}")

    return {
        'nivel_global': nivel_global,
        'hay_errores':  hay_errores,
        'hay_warnings': hay_warnings,
        'mercados':     resultados,
        'timestamp':    datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M UTC'),
    }
