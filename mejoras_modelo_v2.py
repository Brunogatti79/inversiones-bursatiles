#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════╗
║  MEJORAS MODELO INVERSIONES BURSÁTILES v2.0                        ║
║  Tres mejoras concretas al scoring system                          ║
║  Fecha: mayo 2026                                                  ║
╚══════════════════════════════════════════════════════════════════════╝

MEJORA 1: Separar ENTRY_SCORE de ASSET_QUALITY
MEJORA 2: R/R ponderado en ranking final (RANKING_ACCIONABLE)
MEJORA 3: Distancia al máximo 52s como variable técnica

Este módulo se importa desde modelo_macro_micro_senales_.py
o se ejecuta standalone con los CSVs/XLSX como input.
"""

import pandas as pd
import numpy as np

# ═══════════════════════════════════════════════════════════════
# MEJORA 1: SEPARAR ENTRY_SCORE vs ASSET_QUALITY
# ═══════════════════════════════════════════════════════════════
#
# ANTES (modelo actual):
#   Score Final = 40% Macro + 40% Técnico + 20% Sectorial
#   → Un solo número que mezcla "qué tan buena es la empresa"
#     con "qué tan buen momento es para comprar"
#
# DESPUÉS (modelo mejorado):
#   ASSET_QUALITY = 50% Macro + 30% Fundamental + 20% Sectorial
#     → Responde: "¿Es un buen activo?"
#     → Cambia lento (trimestral, con datos contables/macro)
#
#   ENTRY_SCORE = 60% Técnico + 25% R/R_normalizado + 15% Dist_Max_52s
#     → Responde: "¿Es buen momento para entrar?"
#     → Cambia rápido (diario/semanal, con precios)
#
#   SCORE_FINAL_V2 = 50% ASSET_QUALITY + 50% ENTRY_SCORE
#     → Equilibra ambas dimensiones
# ═══════════════════════════════════════════════════════════════

def calcular_asset_quality(score_macro, score_fundamental, score_sectorial):
    """
    Calidad intrínseca del activo.
    
    Parámetros:
    - score_macro (float): Score macro del país (0-100), del modelo existente
    - score_fundamental (float): Score cuantitativo de ratios (0-100), 
      derivado de ratios_consolidado_quant.csv → columna 'Score Cuantitativo'
    - score_sectorial (float): Score sectorial (0-100), del modelo existente
    
    Retorna: float (0-100)
    """
    return (0.50 * score_macro + 
            0.30 * score_fundamental + 
            0.20 * score_sectorial)


def calcular_entry_score(score_tecnico, rr_normalizado, dist_max_52s_norm):
    """
    Calidad del punto de entrada.
    
    Parámetros:
    - score_tecnico (float): Score técnico del modelo existente (0-100)
      Incluye RSI(14), Momentum 21d, cruces MA20/MA50
    - rr_normalizado (float): Ratio R/R normalizado a 0-100 (ver Mejora 2)
    - dist_max_52s_norm (float): Distancia al máximo 52s normalizada 0-100 (ver Mejora 3)
    
    Retorna: float (0-100)
    """
    return (0.60 * score_tecnico + 
            0.25 * rr_normalizado + 
            0.15 * dist_max_52s_norm)


def calcular_score_final_v2(asset_quality, entry_score):
    """
    Score final combinado v2.
    
    Retorna: float (0-100), señal (str)
    """
    score = 0.50 * asset_quality + 0.50 * entry_score
    
    if score >= 70:
        señal = "⭐ Compra Fuerte"
    elif score >= 60:
        señal = "🟢 Compra"
    elif score >= 45:
        señal = "🟡 Neutral/Esperar"
    elif score >= 35:
        señal = "🟠 Venta Parcial"
    else:
        señal = "🔴 Venta"
    
    return round(score, 1), señal


# ═══════════════════════════════════════════════════════════════
# MEJORA 2: RATIO RIESGO/RETORNO NORMALIZADO
# ═══════════════════════════════════════════════════════════════
#
# R/R = Upside potencial / Downside potencial
#
# Upside = (Máximo 52s - Precio actual) / Precio actual
# Downside = (Precio actual - Mínimo 52s) / Precio actual
#
# R/R > 1.5 → Excelente (mucho más para ganar que para perder)
# R/R = 1.0 → Neutral (simétrico)
# R/R < 0.5 → Pobre (poco para ganar, mucho para perder)
#
# Normalización: R/R se mapea a 0-100 con rango [0, 3]
#   R/R = 0 → Score 0 (estás en el máximo, sin upside)
#   R/R = 1.5 → Score 50
#   R/R ≥ 3.0 → Score 100 (gran asimetría favorable)
# ═══════════════════════════════════════════════════════════════

def calcular_rr(precio_actual, max_52s, min_52s):
    """
    Calcula ratio Riesgo/Retorno.
    
    Retorna: (rr_ratio, rr_normalizado)
    - rr_ratio: valor crudo (0 a infinito, típicamente 0-5)
    - rr_normalizado: mapeado a 0-100
    """
    if precio_actual <= 0 or max_52s <= 0 or min_52s <= 0:
        return 0.0, 0.0
    
    upside = (max_52s - precio_actual) / precio_actual
    downside = (precio_actual - min_52s) / precio_actual
    
    # Protección: si precio = mínimo, downside = 0
    if downside <= 0.001:
        rr_ratio = upside / 0.001  # cap artificial
        rr_ratio = min(rr_ratio, 5.0)
    else:
        rr_ratio = upside / downside
    
    # Normalizar a 0-100 con rango [0, 3]
    rr_normalizado = min(100.0, max(0.0, (rr_ratio / 3.0) * 100.0))
    
    return round(rr_ratio, 2), round(rr_normalizado, 1)


# ═══════════════════════════════════════════════════════════════
# MEJORA 3: DISTANCIA AL MÁXIMO 52s COMO VARIABLE
# ═══════════════════════════════════════════════════════════════
#
# dist_max_52s = (Precio actual - Máximo 52s) / Máximo 52s × 100
# Siempre negativo o cero (0% = estás en el máximo)
#
# Interpretación para ENTRY:
#   -40% o más → Score 100 (muy lejos del max, posible oportunidad)
#   -20% → Score 50 (corrección moderada)
#   0% → Score 0 (en el máximo, sin margen de entrada)
#
# Esto evita el problema de "comprar caro":
#   PETR4 con +52% y cerca del max → dist_max_52s ≈ -3% → Score ≈ 7
#   CYRE3 con caída -38% → dist_max_52s ≈ -38% → Score ≈ 95
# ═══════════════════════════════════════════════════════════════

def normalizar_dist_max(dist_max_pct):
    """
    Normaliza la distancia al máximo 52s a score 0-100.
    
    Parámetro:
    - dist_max_pct (float): valor negativo, ej: -22.5 significa 22.5% debajo del máximo
    
    Retorna: float (0-100)
    """
    # dist_max viene negativo (ej: -22.5)
    # Lo convertimos a positivo para la normalización
    distancia = abs(dist_max_pct)
    
    # Rango: 0% (en el max) = score 0, 40%+ (lejos del max) = score 100
    score = min(100.0, max(0.0, (distancia / 40.0) * 100.0))
    
    return round(score, 1)


# ═══════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL: PROCESAR DATAFRAME COMPLETO
# ═══════════════════════════════════════════════════════════════

def aplicar_mejoras(df):
    """
    Aplica las 3 mejoras a un DataFrame con las columnas del modelo actual.
    
    Columnas requeridas (del modelo existente):
    - 'Score Macro'
    - 'Score Técnico'  
    - 'Score Sectorial'
    - 'Score Cuantitativo' (de ratios_consolidado_quant.csv)
    - 'Precio actual'
    - 'Máximo 52s' (o 'Máx 52s')
    - 'Mínimo 52s' (o 'Mín 52s')
    - 'Dist. Máx 52s (%)' (opcional, se calcula si no existe)
    
    Columnas nuevas agregadas:
    - 'R/R Ratio'
    - 'R/R Norm (0-100)'
    - 'Dist Max Norm (0-100)'
    - 'ASSET_QUALITY'
    - 'ENTRY_SCORE'
    - 'SCORE_FINAL_V2'
    - 'SEÑAL_V2'
    - 'RANKING_ACCIONABLE'
    """
    df = df.copy()
    
    # --- Resolver nombres de columnas ---
    col_max52 = next((c for c in df.columns if 'x' in c.lower() and '52' in c), None)
    col_min52 = next((c for c in df.columns if 'n' in c.lower() and '52' in c and 'x' not in c.lower()), None)
    col_dist = next((c for c in df.columns if 'dist' in c.lower() and 'x' in c.lower()), None)
    col_score_quant = next((c for c in df.columns if 'cuantitativo' in c.lower()), None)
    
    # --- Asegurar tipos numéricos ---
    num_cols = ['Score Macro', 'Score Técnico', 'Score Sectorial', 'Precio actual']
    if col_score_quant:
        num_cols.append(col_score_quant)
    if col_max52:
        num_cols.append(col_max52)
    if col_min52:
        num_cols.append(col_min52)
    if col_dist:
        num_cols.append(col_dist)
    
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(' ', '').str.replace(',', '.'),
                errors='coerce'
            )
    
    # --- MEJORA 2: R/R ---
    if col_max52 and col_min52:
        rr_results = df.apply(
            lambda row: calcular_rr(
                row.get('Precio actual', 0),
                row.get(col_max52, 0),
                row.get(col_min52, 0)
            ), axis=1, result_type='expand'
        )
        df['R/R Ratio'] = rr_results[0]
        df['R/R Norm (0-100)'] = rr_results[1]
    else:
        df['R/R Ratio'] = 0.0
        df['R/R Norm (0-100)'] = 0.0
    
    # --- MEJORA 3: Dist Max normalizada ---
    if col_dist:
        df['Dist Max Norm (0-100)'] = df[col_dist].apply(normalizar_dist_max)
    elif col_max52:
        df['_dist_max_calc'] = ((df['Precio actual'] - df[col_max52]) / df[col_max52]) * 100
        df['Dist Max Norm (0-100)'] = df['_dist_max_calc'].apply(normalizar_dist_max)
        df.drop('_dist_max_calc', axis=1, inplace=True)
    else:
        df['Dist Max Norm (0-100)'] = 0.0
    
    # --- MEJORA 1: Asset Quality vs Entry Score ---
    score_fund = df[col_score_quant] if col_score_quant else pd.Series(50.0, index=df.index)
    
    df['ASSET_QUALITY'] = df.apply(
        lambda row: round(calcular_asset_quality(
            row['Score Macro'],
            score_fund.loc[row.name] if col_score_quant else 50.0,
            row['Score Sectorial']
        ), 1), axis=1
    )
    
    df['ENTRY_SCORE'] = df.apply(
        lambda row: round(calcular_entry_score(
            row['Score Técnico'],
            row['R/R Norm (0-100)'],
            row['Dist Max Norm (0-100)']
        ), 1), axis=1
    )
    
    # --- Score Final V2 y Señal ---
    results = df.apply(
        lambda row: calcular_score_final_v2(row['ASSET_QUALITY'], row['ENTRY_SCORE']),
        axis=1, result_type='expand'
    )
    df['SCORE_FINAL_V2'] = results[0]
    df['SEÑAL_V2'] = results[1]
    
    # --- MEJORA 2 (cont): Ranking Accionable ---
    # RANKING = 0.6 × Score_Final_V2 + 0.4 × R/R_Norm
    df['RANKING_ACCIONABLE'] = round(
        0.6 * df['SCORE_FINAL_V2'] + 0.4 * df['R/R Norm (0-100)'], 1
    )
    
    # Ordenar por ranking accionable
    df = df.sort_values('RANKING_ACCIONABLE', ascending=False)
    
    return df


# ═══════════════════════════════════════════════════════════════
# DEMO / TEST CON DATOS DE EJEMPLO
# ═══════════════════════════════════════════════════════════════

def demo():
    """
    Demostración con datos representativos del modelo actual
    (últimos valores conocidos de la sesión del 23/04/2026).
    """
    
    # Datos de ejemplo basados en sesiones previas
    data = [
        # Ticker, Mercado, Score_Macro, Score_Tecnico, Score_Sectorial, Score_Cuant, Precio, Max52, Min52
        ("GGAL.BA",   "MERVAL",  34.0, 42.0, 45.0, 62.0,  9450, 12200,  5900),
        ("TRAN.BA",   "MERVAL",  34.0, 48.0, 40.0, 38.0,  1850,  2400,  1100),
        ("YPFD.BA",   "MERVAL",  34.0, 35.0, 50.0, 55.0, 60750, 63700, 38500),
        ("PAMP.BA",   "MERVAL",  34.0, 40.0, 48.0, 58.0,  4490,  5900,  2850),
        ("CRES.BA",   "MERVAL",  34.0, 38.0, 42.0, 45.0,  2100,  2800,  1500),
        
        ("PETR4.SA",  "BOVESPA", 43.1, 55.0, 52.0, 59.5, 38.50, 40.20, 25.10),
        ("VALE3.SA",  "BOVESPA", 43.1, 50.0, 48.0, 65.0, 58.90, 68.50, 42.30),
        ("CYRE3.SA",  "BOVESPA", 43.1, 40.0, 45.0, 58.5, 18.20, 29.50, 14.80),
        ("EGIE3.SA",  "BOVESPA", 43.1, 45.0, 50.0, 59.5, 42.80, 58.60, 35.20),
        ("ABEV3.SA",  "BOVESPA", 43.1, 30.0, 46.0, 51.6, 12.50, 14.80, 10.20),
        ("BBDC4.SA",  "BOVESPA", 43.1, 48.0, 44.0, 55.0, 14.20, 16.50, 10.80),
        
        ("JNJ",       "S&P500",  45.3, 52.0, 55.0, 55.4,  158.0, 175.0, 140.0),
        ("NVDA",      "S&P500",  45.3, 60.0, 50.0, 48.0,  880.0, 950.0, 450.0),
        ("XOM",       "S&P500",  45.3, 45.0, 52.0, 54.0,  112.0, 125.0,  95.0),
        ("CAT",       "S&P500",  45.3, 55.0, 48.0, 52.0,  345.0, 410.0, 280.0),
    ]
    
    df = pd.DataFrame(data, columns=[
        'Ticker', 'Mercado', 'Score Macro', 'Score Técnico', 'Score Sectorial',
        'Score Cuantitativo', 'Precio actual', 'Máximo 52s', 'Mínimo 52s'
    ])
    
    # Aplicar mejoras
    df_mejorado = aplicar_mejoras(df)
    
    # --- Imprimir resultados ---
    print("=" * 100)
    print("  MODELO v2.0 — RESULTADO CON 3 MEJORAS APLICADAS")
    print("=" * 100)
    
    # Tabla comparativa: Score V1 vs V2
    print("\n📊 COMPARACIÓN: SCORE ORIGINAL vs SCORE V2")
    print("-" * 100)
    
    # Recalcular V1 para comparar
    df_mejorado['SCORE_V1'] = round(
        0.40 * df_mejorado['Score Macro'] + 
        0.40 * df_mejorado['Score Técnico'] + 
        0.20 * df_mejorado['Score Sectorial'], 1
    )
    
    cols_comparar = ['Ticker', 'Mercado', 'SCORE_V1', 'ASSET_QUALITY', 'ENTRY_SCORE', 
                     'SCORE_FINAL_V2', 'R/R Ratio', 'RANKING_ACCIONABLE', 'SEÑAL_V2']
    
    print(df_mejorado[cols_comparar].to_string(index=False))
    
    # --- Highlight: Dónde cambia la historia ---
    print("\n\n🔍 ANÁLISIS: DÓNDE CAMBIA LA HISTORIA")
    print("-" * 100)
    
    df_mejorado['DELTA_RANK'] = df_mejorado['RANKING_ACCIONABLE'] - df_mejorado['SCORE_V1']
    
    ganadores = df_mejorado.nlargest(3, 'DELTA_RANK')[['Ticker', 'SCORE_V1', 'RANKING_ACCIONABLE', 'DELTA_RANK', 'R/R Ratio']]
    perdedores = df_mejorado.nsmallest(3, 'DELTA_RANK')[['Ticker', 'SCORE_V1', 'RANKING_ACCIONABLE', 'DELTA_RANK', 'R/R Ratio']]
    
    print("\n🟢 MEJORAN con el nuevo modelo (tenían R/R favorable que el V1 ignoraba):")
    print(ganadores.to_string(index=False))
    
    print("\n🔴 EMPEORAN con el nuevo modelo (estaban inflados por momentum sin buen entry):")
    print(perdedores.to_string(index=False))
    
    return df_mejorado


if __name__ == "__main__":
    resultado = demo()
