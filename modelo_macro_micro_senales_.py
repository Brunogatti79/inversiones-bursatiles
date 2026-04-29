# ==============================================================================
#  MODELO MACRO → MICRO: SEÑALES DE COMPRA/VENTA
#  MERVAL (Argentina) + BOVESPA (Brasil) + S&P 500 (EE.UU.)
#
#  METODOLOGÍA:
#  1. Score Macro (0-100) basado en 5 variables por país
#  2. Score Técnico por acción (RSI, Momentum, Media Móvil)
#  3. Score Sectorial (sensibilidad sector al entorno macro)
#  4. Señal Final = ponderación de los 3 scores
#
#  Instalación:
#      pip install --upgrade yfinance pandas openpyxl numpy
# ==============================================================================

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ── PARÁMETROS ─────────────────────────────────────────────────────────────────
START_DATE = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
END_DATE   = datetime.today().strftime("%Y-%m-%d")

# ── DATOS MACROECONÓMICOS ──────────────────────────────────────────────────────
# Formato: "Variable": (valor_actual, peor, mejor, impacto, descripcion)
#   impacto  +1 = mayor valor → mejor entorno
#   impacto  -1 = mayor valor → peor entorno

MACRO_ARG = {
    # Variable                : (valor_actual,  peor,    mejor,  impacto, descripcion)
    # ── Actualizado: abril 2026 ──────────────────────────────────────────────
    # VARIABLES ORIGINALES (5)
    "Tasa de interés TAMAR" : (29.0,   60.0,    5.0,   -1, "TNA plazo fijo 30d - abr-2026 (ciclo desinflación activo)"),
    "Riesgo País (pb)"      : (730,    2000,    100,   -1, "EMBI Argentina abr-2026 - escalada post-acuerdo FMI / presión reservas"),
    "Inflación mensual (%)" : (2.9,    10.0,    0.0,   -1, "IPC feb-2026 confirmado INDEC 2.9% mensual"),
    "Desempleo (%)"         : (6.5,    20.0,    2.0,   -1, "Estimado Q1-2026 - leve mejora"),
    "Reservas BCRA (kUSD)"  : (26500,  5000,    60000,  1, "Reservas netas abr-2026 - presión pagos deuda externa"),
    # VARIABLES NUEVAS (4) — incorporadas para capturar régimen cambiario y ciclo fiscal
    # TCR: índice base 100. >120 = apreciado (negativo), <80 = depreciado (positivo competitividad)
    # Fuente: BCRA / BIS. Rango peor=150 (muy apreciado), mejor=70 (competitivo), impacto -1
    "Tipo de cambio real (TCR)" : (108.0, 150.0,  70.0,  -1, "TCR abr-2026 - apreciación moderada vs canasta; sostenida por superávit comercial"),
    # Brecha cambiaria CCL vs oficial en %. 0=sin brecha, >30=alto riesgo devaluatorio
    # Fuente: Rava/Ámbito. Rango peor=80%, mejor=0%, impacto -1
    "Brecha cambiaria (%)"      : (18.0,   80.0,   0.0,  -1, "Brecha CCL-oficial abr-2026 - contenida pero latente"),
    # Balanza comercial en USD millones (mensual). Superávit = positivo para reservas
    # Fuente: INDEC. Rango peor=-2000M, mejor=5000M, impacto +1
    "Balanza comercial (M USD)" : (2523,  -2000,   5000,  1, "Superávit USD 2,523M mar-2026 - INDEC; retorno al superávit sostenido"),
    # Resultado fiscal primario en % del PBI (anualizado). Superávit = ancla credibilidad
    # Fuente: Mecon. Rango peor=-5%, mejor=3%, impacto +1
    "Resultado fiscal primario (%PBI)" : (0.8,  -5.0,   3.0,  1, "Superávit primario acumulado 2026 - Mecon; ancla del programa económico"),
}

MACRO_BRA = {
    # Variable                : (valor_actual,  peor,    mejor,  impacto, descripcion)
    # ── Actualizado: abril 2026 ──────────────────────────────────────────────
    # VARIABLES ORIGINALES (5)
    "Tasa SELIC (%)"        : (14.75,  20.0,    5.0,   -1, "SELIC post-recorte 25bps COPOM mar-2026 - próximo recorte sep-2026"),
    "Riesgo País (pb)"      : (235,    800,     80,    -1, "EMBI Brasil abr-2026 - leve suba por presión fiscal"),
    "Inflación IPCA (%)"    : (3.81,   10.0,    0.0,   -1, "IPCA feb-2026 oficial IBGE 3.81% a/a - encuesta Focus proyecta 4.31% dic-2026"),
    "Desempleo (%)"         : (6.4,    15.0,    3.0,   -1, "Sin cambio - IBGE abr-2026"),
    "Reservas BCB (kUSD)"   : (350000, 100000,  500000, 1, "Reservas internacionales Brasil - estables y robustas"),
    # VARIABLES NUEVAS (4) — capturan régimen cambiario, carry, ciclo fiscal y actividad
    # BRL/USD: menor valor = BRL más fuerte = positivo para retornos USD
    # Fuente: BCB/Bloomberg. Rango peor=6.5 (BRL muy débil), mejor=4.0 (BRL fuerte), impacto -1
    "Tipo de cambio BRL/USD"          : (5.72,    6.5,    4.0,  -1, "BRL/USD abr-2026 - real debilitado, erosiona retornos en USD"),
    # Diferencial tasa real = SELIC real (SELIC-IPCA) menos Fed real (Fed-PCE). Positivo = carry atractivo
    # Fuente: BCB/Fed. Rango peor=0% (sin carry), mejor=15%, impacto +1
    "Diferencial tasa real (pp)"      : (10.4,    0.0,   15.0,   1, "SELIC real 10.9% vs Fed real 1.4% - diferencial 10.4pp, carry atractivo"),
    # Deuda pública bruta como % del PBI. Alta deuda = riesgo fiscal estructural
    # Fuente: Tesouro Nacional. Rango peor=100%, mejor=50%, impacto -1
    "Deuda pública / PIB (%)"         : (88.0,   100.0,  50.0,  -1, "Deuda bruta 88% PIB abr-2026 - Tesouro Nacional; presión fiscal persistente"),
    # PMI Manufacturero S&P Global. >50=expansión (positivo), <50=contracción
    # Fuente: S&P Global. Rango peor=40, mejor=60, impacto +1
    "PMI Manufacturero"               : (48.5,    40.0,  60.0,   1, "PMI Manufacturero Brasil abr-2026 - por debajo de 50, señal contracción industrial"),
}

MACRO_USA = {
    # Variable                : (valor_actual,  peor,   mejor,  impacto, descripcion)
    # ── Actualizado: abril 2026 ──────────────────────────────────────────────
    # VARIABLES ORIGINALES (5)
    "Fed Funds Rate (%)"      : (4.375,  8.0,   0.5,   -1, "Sin cambios - Fed en pausa abr-2026, pocas chances de recorte en 2026"),
    "Inflación CPI (%)"       : (3.1,    9.0,   0.0,   -1, "CPI mar-2026 presionado por shock energético Irán / Estrecho de Ormuz"),
    "Desempleo (%)"           : (4.3,    10.0,  2.0,   -1, "Leve deterioro abr-2026 - creación de empleo concentrada en salud y construcción"),
    "GDP Growth YoY (%)"      : (2.1,   -3.0,   5.0,    1, "Revisado a la baja por aranceles y presión energía - Vanguard/FMI abr-2026"),
    "Confianza consumidor"    : (88.0,   40.0,  140.0,  1, "Caída significativa abr-2026 por incertidumbre geopolítica Irán"),
    # VARIABLES NUEVAS (4) — capturan inflación subyacente, estrés crédito, dólar y ciclo industrial
    # PCE Core: métrica preferida de la Fed. >2.5% = restrictivo, <2% = neutral
    # Fuente: BEA. Rango peor=6%, mejor=0%, impacto -1
    "PCE Core (%)"            : (3.2,    6.0,   0.0,   -1, "PCE Core mar-2026 - por encima de meta Fed 2%; aleja recortes de tasas"),
    # Spread High Yield (OAS, ICE BofA). Ampliación = estrés crédito = señal anticipada caída S&P
    # Fuente: ICE BofA / FRED. Rango peor=900pb (crisis), mejor=200pb (expansión), impacto -1
    "Spread High Yield (pb)"  : (380,    900,   200,   -1, "Spread HY abr-2026 - ampliándose por riesgo geopolítico; lidera correcciones 4-8 sem"),
    # DXY (Índice Dólar). Dólar fuerte comprime earnings globales y valuaciones growth
    # Fuente: ICE. Rango peor=115 (muy fuerte), mejor=90 (neutral/débil), impacto -1
    "Índice DXY"              : (101.5,  115.0, 90.0,  -1, "DXY abr-2026 - en zona neutral-alta; presión sobre earnings globales de S&P 500"),
    # ISM Manufacturero. >50=expansión, <50=contracción. Adelanta ciclo 6-8 semanas
    # Fuente: ISM. Rango peor=40, mejor=60, impacto +1
    "ISM Manufacturero"       : (49.0,   40.0,  60.0,   1, "ISM Manufactura abr-2026 - rozando contracción; señal adelantada del ciclo industrial"),
}

# ── TICKERS Y SECTORES ─────────────────────────────────────────────────────────
# Formato: "TICKER": ("Nombre empresa", "SECTOR", sensibilidad_macro)
# Sensibilidad sectorial al macro:
#   > 1.0 → sector cíclico, amplifica el entorno macro
#   = 1.0 → sensibilidad neutra
#   < 1.0 → sector defensivo, amortigua el entorno macro

MERVAL = {
    "GGAL.BA":  ("Grupo Financiero Galicia",  "FINANCIERO",   1.5),
    "BMA.BA":   ("Banco Macro",               "FINANCIERO",   1.5),
    "SUPV.BA":  ("Supervielle",               "FINANCIERO",   1.5),
    "VALO.BA":  ("Grupo Supervielle VALO",    "FINANCIERO",   1.3),
    "BYMA.BA":  ("BYMA",                      "FINANCIERO",   1.2),
    "PAMP.BA":  ("Pampa Energía",             "ENERGÍA",      1.0),
    "CEPU.BA":  ("Central Puerto",            "ENERGÍA",      1.0),
    "TGSU2.BA": ("Transp. Gas del Sur",       "ENERGÍA",      0.9),
    "TRAN.BA":  ("Transener",                 "UTILITIES",    0.7),
    "EDN.BA":   ("Edenor",                    "UTILITIES",    0.7),
    "YPF.BA":   ("YPF",                       "ENERGÍA",      1.1),
    "VIST.BA":  ("Vista Energy",              "ENERGÍA",      1.2),
    "TXAR.BA":  ("Ternium Argentina",         "MATERIALES",   0.9),
    "ALUA.BA":  ("Aluar",                     "MATERIALES",   0.8),
    "LOMA.BA":  ("Loma Negra",                "MATERIALES",   0.8),
    "HARG.BA":  ("Holcim Argentina",          "MATERIALES",   0.8),
    "CRES.BA":  ("Cresud",                    "INMOBILIARIO", 0.9),
    "IRSA.BA":  ("IRSA",                      "INMOBILIARIO", 0.9),
    "MIRG.BA":  ("Mirgor",                    "CONSUMO",      1.1),
    "COME.BA":  ("Soc. Comercial del Plata",  "CONSUMO",      0.9),
    "MOLI.BA":  ("Molinos Río de la Plata",   "CONSUMO",      0.7),
    "TECO2.BA": ("Telecom Argentina",         "TELECOM",      0.6),
}

BOVESPA = {
    "ITUB4.SA":  ("Itaú Unibanco",   "FINANCIERO",   1.5),
    "BBDC4.SA":  ("Bradesco",        "FINANCIERO",   1.5),
    "BBAS3.SA":  ("Banco do Brasil", "FINANCIERO",   1.3),
    "BPAC11.SA": ("BTG Pactual",     "FINANCIERO",   1.4),
    "PETR4.SA":  ("Petrobras PN",    "ENERGÍA",      1.1),
    "RAIZ4.SA":  ("Raízen",          "ENERGÍA",      1.0),
    "VALE3.SA":  ("Vale",            "MATERIALES",   1.0),
    "SUZB3.SA":  ("Suzano",          "MATERIALES",   0.9),
    "CSNA3.SA":  ("CSN",             "MATERIALES",   0.9),
    "WEGE3.SA":  ("WEG",             "INDUSTRIAL",   1.1),
    "EMBR3.SA":  ("Embraer",         "INDUSTRIAL",   1.0),
    "EQTL3.SA":  ("Equatorial",      "UTILITIES",    0.7),
    "EGIE3.SA":  ("Engie Brasil",    "UTILITIES",    0.7),
    "CPLE6.SA":  ("Copel",           "UTILITIES",    0.7),
    "ABEV3.SA":  ("Ambev",           "CONSUMO",      0.8),
    "LREN3.SA":  ("Lojas Renner",    "CONSUMO",      1.2),
    "MGLU3.SA":  ("Magazine Luiza",  "CONSUMO",      1.3),
    "RDOR3.SA":  ("Rede D'Or",       "SALUD",        0.6),
    "HAPV3.SA":  ("Hapvida",         "SALUD",        0.6),
    "RENT3.SA":  ("Localiza",        "INDUSTRIAL",   1.1),
    "JBSS3.SA":  ("JBS",             "CONSUMO",      0.8),
    "CYRE3.SA":  ("Cyrela",          "INMOBILIARIO", 1.2),
}

SP500 = {
    # Tecnología — alta sensibilidad a tasas y ciclo
    "AAPL":      ("Apple",               "TECNOLOGÍA",   1.2),
    "MSFT":      ("Microsoft",           "TECNOLOGÍA",   1.2),
    "NVDA":      ("NVIDIA",              "TECNOLOGÍA",   1.5),
    "GOOGL":     ("Alphabet (Google)",   "TECNOLOGÍA",   1.2),
    "META":      ("Meta Platforms",      "TECNOLOGÍA",   1.3),
    "AMZN":      ("Amazon",              "CONSUMO",      1.3),
    # Finanzas — cíclico, se beneficia de tasas altas moderadas
    "JPM":       ("JPMorgan Chase",      "FINANCIERO",   1.4),
    "BAC":       ("Bank of America",     "FINANCIERO",   1.4),
    "GS":        ("Goldman Sachs",       "FINANCIERO",   1.3),
    "V":         ("Visa",                "FINANCIERO",   1.1),
    # Energía — ligado a ciclo y commodities
    "XOM":       ("ExxonMobil",          "ENERGÍA",      1.0),
    "CVX":       ("Chevron",             "ENERGÍA",      1.0),
    # Salud — defensivo
    "JNJ":       ("Johnson & Johnson",   "SALUD",        0.6),
    "UNH":       ("UnitedHealth",        "SALUD",        0.6),
    "LLY":       ("Eli Lilly",           "SALUD",        0.7),
    # Consumo básico — muy defensivo
    "WMT":       ("Walmart",             "CONSUMO",      0.6),
    "PG":        ("Procter & Gamble",    "CONSUMO",      0.5),
    "KO":        ("Coca-Cola",           "CONSUMO",      0.5),
    "MCD":       ("McDonald's",          "CONSUMO",      0.6),
    # Industrial — cíclico
    "CAT":       ("Caterpillar",         "INDUSTRIAL",   1.2),
    "BA":        ("Boeing",              "INDUSTRIAL",   1.1),
    "GE":        ("GE Aerospace",        "INDUSTRIAL",   1.1),
    # Alto beta / crecimiento
    "TSLA":      ("Tesla",               "TECNOLOGÍA",   1.6),
}


# ── FÓRMULAS ───────────────────────────────────────────────────────────────────

def normalizar(valor, peor, mejor):
    """Normaliza un valor entre 0 y 100 según rango [peor, mejor]."""
    if mejor == peor:
        return 50
    score = (valor - peor) / (mejor - peor) * 100
    return max(0, min(100, score))


def calcular_score_macro(macro_dict, nombre_pais):
    """Score Macro (0-100) = promedio de variables normalizadas."""
    scores   = []
    detalles = []
    for var, (valor, peor, mejor, impacto, desc) in macro_dict.items():
        s = normalizar(valor, peor, mejor)
        if impacto == -1:
            s = 100 - s
        scores.append(s)
        semaforo = "🟢" if s >= 60 else ("🟡" if s >= 40 else "🔴")
        detalles.append({
            "País":            nombre_pais,
            "Variable":        var,
            "Valor":           valor,
            "Score (0-100)":   round(s, 1),
            "Semáforo":        semaforo,
            "Fuente":          desc,
        })
    return round(np.mean(scores), 1), pd.DataFrame(detalles)


def rsi(serie, periodos=14):
    """Relative Strength Index (RSI)."""
    delta     = serie.diff()
    ganancias = delta.clip(lower=0)
    perdidas  = -delta.clip(upper=0)
    media_gan = ganancias.rolling(periodos).mean()
    media_per = perdidas.rolling(periodos).mean()
    rs = media_gan / media_per.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def score_tecnico(serie):
    """
    Score técnico (0-100):
      RSI 14 días      → 40%
      Momentum 21 días → 35%
      Cruce MA20/MA50  → 25%
    """
    if len(serie) < 55:
        return 50, {}

    r = rsi(serie).iloc[-1]
    if   r < 30: s_rsi = 90
    elif r < 40: s_rsi = 70
    elif r < 55: s_rsi = 50
    elif r < 70: s_rsi = 30
    else:        s_rsi = 10

    mom   = (serie.iloc[-1] / serie.iloc[-22] - 1) * 100 if len(serie) > 22 else 0
    s_mom = normalizar(mom, -20, 20)

    ma20  = serie.rolling(20).mean().iloc[-1]
    ma50  = serie.rolling(50).mean().iloc[-1]
    s_ma  = 70 if ma20 > ma50 else 30

    score = round(0.40 * s_rsi + 0.35 * s_mom + 0.25 * s_ma, 1)
    return score, {
        "RSI(14)":       round(r, 1),
        "Momentum(21d)": round(mom, 2),
        "MA20>MA50":     "Sí" if ma20 > ma50 else "No",
    }


def descargar_y_analizar(tickers_dict, score_macro, nombre_mercado):
    """Descarga datos, calcula scores y genera señales de compra/venta."""
    print(f"\n{'='*65}")
    print(f"  {nombre_mercado} — Score Macro del entorno: {score_macro}/100")
    print(f"{'='*65}")

    resultados = []

    for ticker, (nombre, sector, sens_sectorial) in tickers_dict.items():
        try:
            data = yf.Ticker(ticker).history(
                start=START_DATE, end=END_DATE,
                interval="1d", auto_adjust=True, raise_errors=False
            )
            if data is None or data.empty or "Close" not in data.columns:
                print(f"  --  {ticker:12s}  sin datos")
                continue

            serie = data["Close"].copy()
            serie.index = pd.to_datetime(serie.index).tz_localize(None)
            serie = serie.dropna()

            if len(serie) < 2:
                continue

            precio_hoy = serie.iloc[-1]
            precio_1m  = serie.iloc[-22] if len(serie) > 22 else serie.iloc[0]
            precio_3m  = serie.iloc[-63] if len(serie) > 63 else serie.iloc[0]

            var_1d  = (serie.iloc[-1] / serie.iloc[-2] - 1) * 100 if len(serie) > 1 else 0
            var_5d  = (serie.iloc[-1] / serie.iloc[-6] - 1) * 100 if len(serie) > 5 else 0
            var_1m  = (serie.iloc[-1] / precio_1m - 1) * 100
            var_3m  = (serie.iloc[-1] / precio_3m - 1) * 100

            ytd_ini = serie[serie.index >= pd.Timestamp(f"{serie.index[-1].year}-01-01")]
            var_ytd = (ytd_ini.iloc[-1] / ytd_ini.iloc[0] - 1) * 100 if len(ytd_ini) > 1 else 0

            s_tec, indicadores = score_tecnico(serie)

            # Score sectorial: amplifica o atenúa el macro según sensibilidad
            s_sectorial = min(100, max(0, (score_macro - 50) * sens_sectorial + 50))

            # Score final: Macro 40% + Técnico 40% + Sectorial 20%
            score_final = round(
                0.40 * score_macro +
                0.40 * s_tec       +
                0.20 * s_sectorial,
                1
            )

            if   score_final >= 70: senal = "⭐ COMPRA FUERTE"
            elif score_final >= 60: senal = "🟢 COMPRA"
            elif score_final >= 45: senal = "🟡 NEUTRAL/ESPERAR"
            elif score_final >= 35: senal = "🟠 VENTA PARCIAL"
            else:                   senal = "🔴 VENTA"

            resultados.append({
                "Mercado":          nombre_mercado,
                "Ticker":           ticker,
                "Empresa":          nombre,
                "Sector":           sector,
                "Precio actual":    round(precio_hoy, 2),
                "Var 1d (%)":       round(var_1d, 2),
                "Var 5d (%)":       round(var_5d, 2),
                "Var 1m (%)":       round(var_1m, 2),
                "Var 3m (%)":       round(var_3m, 2),
                "Var YTD (%)":      round(var_ytd, 2),
                "RSI(14)":          indicadores.get("RSI(14)", "-"),
                "Momentum 21d (%)": indicadores.get("Momentum(21d)", "-"),
                "MA20>MA50":        indicadores.get("MA20>MA50", "-"),
                "Score Macro":      score_macro,
                "Score Técnico":    s_tec,
                "Score Sectorial":  round(s_sectorial, 1),
                "SCORE FINAL":      score_final,
                "SEÑAL":            senal,
            })
            print(f"  OK  {ticker:12s}  {nombre:35s}  Score: {score_final:5.1f}  {senal}")

        except Exception as e:
            print(f"  ERR {ticker:12s}  {e}")

    df = pd.DataFrame(resultados)
    if not df.empty:
        df = df.sort_values("SCORE FINAL", ascending=False)
    return df


# ── EJECUCIÓN ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("\n" + "="*65)
    print("  MODELO MACRO→MICRO  |  MERVAL + BOVESPA + S&P 500")
    print(f"  Ejecutado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*65)

    # 1. Scores macro por país
    score_arg, df_macro_arg = calcular_score_macro(MACRO_ARG, "Argentina")
    score_bra, df_macro_bra = calcular_score_macro(MACRO_BRA, "Brasil")
    score_usa, df_macro_usa = calcular_score_macro(MACRO_USA, "EE.UU.")
    df_macro = pd.concat([df_macro_arg, df_macro_bra, df_macro_usa], ignore_index=True)

    print(f"\n  SCORE MACRO ARGENTINA : {score_arg}/100")
    print(f"  SCORE MACRO BRASIL    : {score_bra}/100")
    print(f"  SCORE MACRO EE.UU.    : {score_usa}/100")

    # 2. Análisis por acción
    df_merval  = descargar_y_analizar(MERVAL,  score_arg, "MERVAL")
    df_bovespa = descargar_y_analizar(BOVESPA, score_bra, "BOVESPA")
    df_sp500   = descargar_y_analizar(SP500,   score_usa, "S&P 500")
    df_total   = pd.concat([df_merval, df_bovespa, df_sp500], ignore_index=True)

    # 3. Resumen en consola
    print("\n" + "="*65)
    print("  TOP 10 OPORTUNIDADES DE COMPRA (3 mercados)")
    print("="*65)
    compras = df_total[df_total["SEÑAL"].str.contains("COMPRA")].head(10)
    if not compras.empty:
        print(compras[["Mercado","Ticker","Empresa","SCORE FINAL","SEÑAL"]].to_string(index=False))

    print("\n" + "="*65)
    print("  ALERTAS DE VENTA (3 mercados)")
    print("="*65)
    ventas = df_total[df_total["SEÑAL"].str.contains("VENTA")]
    if not ventas.empty:
        print(ventas[["Mercado","Ticker","Empresa","SCORE FINAL","SEÑAL"]].to_string(index=False))
    else:
        print("  Sin señales de venta en el período analizado.")

    # 4. Exportar Excel con 6 hojas
    output = "modelo_macro_micro_señales.xlsx"
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_macro.to_excel(writer, sheet_name="Macro Variables", index=False)
        if not df_merval.empty:
            df_merval.to_excel(writer,  sheet_name="MERVAL Señales",   index=False)
        if not df_bovespa.empty:
            df_bovespa.to_excel(writer, sheet_name="BOVESPA Señales",  index=False)
        if not df_sp500.empty:
            df_sp500.to_excel(writer,   sheet_name="S&P500 Señales",   index=False)
        if not df_total.empty:
            df_total.sort_values("SCORE FINAL", ascending=False).to_excel(
                writer, sheet_name="Ranking Combinado", index=False)

    print(f"\n  ✅ Excel generado: {output}  (6 hojas)")
    print("\n  METODOLOGÍA RESUMIDA:")
    print("  ┌─ Score Macro (40%)    → 9 vars por país")
    print("  │    ARG: TAMAR, riesgo país, IPC, desempleo, reservas BCRA,")
    print("  │         TCR, brecha cambiaria, balanza comercial, fiscal primario")
    print("  │    BRA: SELIC, riesgo país, IPCA, desempleo, reservas BCB,")
    print("  │         BRL/USD, diferencial tasa real, deuda/PIB, PMI")
    print("  │    USA: Fed Funds, CPI, desempleo, GDP, confianza consumidor,")
    print("  │         PCE Core, spread HY, DXY, ISM Manufacturero")
    print("  ├─ Score Técnico (40%)  → RSI(14) + Momentum(21d) + Cruce MA20/MA50")
    print("  └─ Score Sectorial (20%)→ sensibilidad del sector al entorno macro")
    print("     Score Final ≥ 70 → ⭐ COMPRA FUERTE")
    print("     Score Final 60-70→ 🟢 COMPRA")
    print("     Score Final 45-60→ 🟡 NEUTRAL/ESPERAR")
    print("     Score Final 35-45→ 🟠 VENTA PARCIAL")
    print("     Score Final < 35 → 🔴 VENTA")
    print("\n  ⚠️  Este modelo es orientativo. No constituye asesoramiento financiero.")
