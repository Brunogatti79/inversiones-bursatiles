# ==============================================================================
#  ESTADOS CONTABLES Y FINANCIEROS — MERVAL + BOVESPA + S&P 500
#  Para análisis cuantitativo e informes financieros
#  v3 — incorpora S&P 500 (EE.UU.)
#
#  Instalación:
#      pip install --upgrade yfinance pandas openpyxl numpy
# ==============================================================================

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import time
import warnings
warnings.filterwarnings("ignore")

# ── MERVAL ─────────────────────────────────────────────────────────────────────
MERVAL = {
    "GGAL.BA":  "Grupo Financiero Galicia",
    "BMA.BA":   "Banco Macro",
    "PAMP.BA":  "Pampa Energía",
    "YPF.BA":   "YPF",
    "TXAR.BA":  "Ternium Argentina",
    "ALUA.BA":  "Aluar",
    "CRES.BA":  "Cresud",
    "SUPV.BA":  "Supervielle",
    "CEPU.BA":  "Central Puerto",
    "LOMA.BA":  "Loma Negra",
    "MIRG.BA":  "Mirgor",
    "TECO2.BA": "Telecom Argentina",
    "TGSU2.BA": "Transportadora Gas del Sur",
    "VALO.BA":  "Grupo Supervielle VALO",
    "COME.BA":  "Soc. Comercial del Plata",
    "EDN.BA":   "Edenor",
    "HARG.BA":  "Holcim Argentina",
    "VIST.BA":  "Vista Energy",
    "TRAN.BA":  "Transener",
    "MOLI.BA":  "Molinos Río de la Plata",
    "BYMA.BA":  "BYMA",
    "IRSA.BA":  "IRSA",
}

# ── BOVESPA ────────────────────────────────────────────────────────────────────
BOVESPA = {
    "PETR4.SA":  "Petrobras PN",
    "VALE3.SA":  "Vale",
    "ITUB4.SA":  "Itaú Unibanco",
    "BBDC4.SA":  "Bradesco",
    "ABEV3.SA":  "Ambev",
    "WEGE3.SA":  "WEG",
    "RENT3.SA":  "Localiza",
    "RDOR3.SA":  "Rede D'Or",
    "BBAS3.SA":  "Banco do Brasil",
    "MGLU3.SA":  "Magazine Luiza",
    "SUZB3.SA":  "Suzano",
    "JBSS3.SA":  "JBS",
    "EQTL3.SA":  "Equatorial",
    "RAIZ4.SA":  "Raízen",
    "EMBR3.SA":  "Embraer",
    "HAPV3.SA":  "Hapvida",
    "LREN3.SA":  "Lojas Renner",
    "CSNA3.SA":  "CSN",
    "CYRE3.SA":  "Cyrela",
    "EGIE3.SA":  "Engie Brasil",
    "BPAC11.SA": "BTG Pactual",
    "CPLE6.SA":  "Copel",
}

# ── S&P 500 ────────────────────────────────────────────────────────────────────
SP500 = {
    # Tecnología
    "AAPL":      "Apple",
    "MSFT":      "Microsoft",
    "NVDA":      "NVIDIA",
    "GOOGL":     "Alphabet (Google)",
    "META":      "Meta Platforms",
    "AMZN":      "Amazon",
    # Finanzas
    "JPM":       "JPMorgan Chase",
    "BAC":       "Bank of America",
    "GS":        "Goldman Sachs",
    "V":         "Visa",
    # Energía
    "XOM":       "ExxonMobil",
    "CVX":       "Chevron",
    # Salud
    "JNJ":       "Johnson & Johnson",
    "UNH":       "UnitedHealth",
    "LLY":       "Eli Lilly",
    # Consumo
    "WMT":       "Walmart",
    "PG":        "Procter & Gamble",
    "KO":        "Coca-Cola",
    "MCD":       "McDonald's",
    # Industria
    "CAT":       "Caterpillar",
    "BA":        "Boeing",
    "GE":        "GE Aerospace",
    # Otros
    "TSLA":      "Tesla",
}


# ── UTILIDADES ─────────────────────────────────────────────────────────────────

def strip_tz(df):
    """Elimina timezone de índice y columnas datetime para compatibilidad con Excel."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    elif isinstance(df.index, pd.DatetimeIndex):
        try:
            df.index = df.index.tz_localize(None)
        except Exception:
            pass
    new_cols = []
    for c in df.columns:
        if hasattr(c, 'tz') and c.tz is not None:
            new_cols.append(c.tz_localize(None))
        elif isinstance(c, pd.Timestamp):
            try:
                new_cols.append(c.tz_localize(None))
            except Exception:
                new_cols.append(c)
        else:
            new_cols.append(c)
    df.columns = new_cols
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                try:
                    df[col] = df[col].dt.tz_convert(None)
                except Exception:
                    pass
    return df


def safe_get(ticker_obj, attr):
    try:
        val = getattr(ticker_obj, attr)
        if val is None:
            return pd.DataFrame()
        if isinstance(val, pd.DataFrame):
            return strip_tz(val) if not val.empty else pd.DataFrame()
        if isinstance(val, pd.Series):
            s = val.copy()
            if hasattr(s.index, 'tz') and s.index.tz is not None:
                s.index = s.index.tz_localize(None)
            return s
        return val
    except Exception:
        return pd.DataFrame()


def calcular_ratios(t, nombre, ticker_str):
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    def gi(key, default=np.nan):
        v = info.get(key, default)
        return v if v is not None else default

    pe         = gi("trailingPE")
    pe_fwd     = gi("forwardPE")
    pb         = gi("priceToBook")
    ps         = gi("priceToSalesTrailing12Months")
    ev_ebitda  = gi("enterpriseToEbitda")
    ev_revenue = gi("enterpriseToRevenue")
    peg        = gi("pegRatio")
    roe        = gi("returnOnEquity")
    roa        = gi("returnOnAssets")
    mg_bruta   = gi("grossMargins")
    mg_oper    = gi("operatingMargins")
    mg_neta    = gi("profitMargins")
    ebitda_mg  = gi("ebitdaMargins")
    deuda_eq   = gi("debtToEquity")
    current_r  = gi("currentRatio")
    quick_r    = gi("quickRatio")
    crec_ing   = gi("revenueGrowth")
    crec_gan   = gi("earningsGrowth")
    crec_trim  = gi("revenueQuarterlyGrowth")
    div_yield  = gi("dividendYield")
    div_rate   = gi("dividendRate")
    payout     = gi("payoutRatio")
    cap_merc   = gi("marketCap")
    enterprise = gi("enterpriseValue")
    beta       = gi("beta")
    precio     = gi("currentPrice") or gi("regularMarketPrice")
    max_52     = gi("fiftyTwoWeekHigh")
    min_52     = gi("fiftyTwoWeekLow")
    vol_prom   = gi("averageVolume")
    acciones   = gi("sharesOutstanding")
    sector     = gi("sector", "N/D")
    industria  = gi("industry", "N/D")
    empleados  = gi("fullTimeEmployees")
    pais       = gi("country", "N/D")
    moneda     = gi("currency", "N/D")
    descripcion= (gi("longBusinessSummary", "") or "")[:300]
    eps        = gi("trailingEps")
    bv_share   = gi("bookValue")

    graham = np.nan
    if not np.isnan(float(eps if eps else np.nan)) and not np.isnan(float(bv_share if bv_share else np.nan)) \
            and float(eps) > 0 and float(bv_share) > 0:
        try:
            graham = round(np.sqrt(22.5 * float(eps) * float(bv_share)), 2)
        except Exception:
            pass

    upside_graham = np.nan
    if not np.isnan(graham) and precio and float(precio) > 0:
        upside_graham = round((graham / float(precio) - 1) * 100, 2)

    def pct(v):
        try:
            f = float(v)
            return round(f * 100, 2) if not np.isnan(f) else np.nan
        except Exception:
            return np.nan

    def rnd(v, d=2):
        try:
            f = float(v)
            return round(f, d) if not np.isnan(f) else np.nan
        except Exception:
            return np.nan

    return {
        "Ticker": ticker_str, "Empresa": nombre, "Sector": sector,
        "Industria": industria, "País": pais, "Moneda": moneda,
        "Empleados": empleados, "Descripción": descripcion,
        "Precio actual": precio, "Máximo 52s": max_52, "Mínimo 52s": min_52,
        "Dist. Máx 52s (%)": rnd((float(precio)/float(max_52)-1)*100) if precio and max_52 else np.nan,
        "Dist. Mín 52s (%)": rnd((float(precio)/float(min_52)-1)*100) if precio and min_52 else np.nan,
        "Cap. Mercado": cap_merc, "Enterprise Value": enterprise,
        "Acciones en circ.": acciones, "Beta": beta, "Vol. Prom. 30d": vol_prom,
        "P/E (trailing)": pe, "P/E (forward)": pe_fwd, "P/B": pb, "P/S": ps,
        "EV/EBITDA": ev_ebitda, "EV/Revenue": ev_revenue, "PEG Ratio": peg,
        "Valor Graham": graham, "Upside vs Graham (%)": upside_graham,
        "ROE (%)": pct(roe), "ROA (%)": pct(roa),
        "Margen Bruto (%)": pct(mg_bruta), "Margen Operativo (%)": pct(mg_oper),
        "Margen Neto (%)": pct(mg_neta), "Margen EBITDA (%)": pct(ebitda_mg),
        "Deuda/Equity": deuda_eq, "Current Ratio": current_r, "Quick Ratio": quick_r,
        "Crec. Ingresos YoY (%)": pct(crec_ing), "Crec. Ganancias YoY (%)": pct(crec_gan),
        "Crec. Ingresos QoQ (%)": pct(crec_trim),
        "Div. Yield (%)": pct(div_yield), "Div. Rate": div_rate,
        "Payout Ratio (%)": pct(payout),
    }


# ── DESCARGA Y EXPORTACIÓN ─────────────────────────────────────────────────────

def descargar_estados(tickers_dict, nombre_mercado, output_excel):
    print(f"\n{'='*65}")
    print(f"  {nombre_mercado} — {len(tickers_dict)} empresas")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*65}")

    todos_ratios = []

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        for ticker_str, nombre in tickers_dict.items():
            print(f"\n  [{ticker_str}] {nombre}")
            try:
                t = yf.Ticker(ticker_str)
                time.sleep(0.5)

                bs_anual = safe_get(t, "balance_sheet")
                bs_trim  = safe_get(t, "quarterly_balance_sheet")
                is_anual = safe_get(t, "income_stmt")
                is_trim  = safe_get(t, "quarterly_income_stmt")
                cf_anual = safe_get(t, "cash_flow")
                cf_trim  = safe_get(t, "quarterly_cash_flow")

                divs_raw = safe_get(t, "dividends")
                divs = pd.DataFrame()
                if isinstance(divs_raw, pd.Series) and not divs_raw.empty:
                    divs = divs_raw.reset_index()
                    divs.columns = ["Fecha", "Dividendo"]
                    divs["Fecha"] = pd.to_datetime(divs["Fecha"]).dt.tz_localize(None)

                ratios = calcular_ratios(t, nombre, ticker_str)
                todos_ratios.append(ratios)

                hoja = ticker_str.replace(".", "_").replace("^", "")[:18]

                def escribir(df, sufijo, trp=True):
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        out = strip_tz(df.T.copy() if trp else df.copy())
                        out.columns = [
                            str(c.date()) if isinstance(c, pd.Timestamp) else str(c)
                            for c in out.columns
                        ]
                        out.to_excel(writer, sheet_name=f"{hoja}_{sufijo}"[:31])
                        return True
                    return False

                e = [
                    escribir(bs_anual, "BS_An"),
                    escribir(bs_trim,  "BS_Tr"),
                    escribir(is_anual, "PL_An"),
                    escribir(is_trim,  "PL_Tr"),
                    escribir(cf_anual, "CF_An"),
                    escribir(cf_trim,  "CF_Tr"),
                ]
                if not divs.empty:
                    divs.to_excel(writer, sheet_name=f"{hoja}_Divs"[:31], index=False)
                    e.append(True)
                else:
                    e.append(False)

                r_df = pd.DataFrame(list(ratios.items()), columns=["Variable", "Valor"])
                r_df.to_excel(writer, sheet_name=f"{hoja}_Ratios"[:31], index=False)

                tags = ["BS_An","BS_Tr","PL_An","PL_Tr","CF_An","CF_Tr","Divs"]
                ok   = " | ".join(f"{'✓' if v else '—'} {tags[i]}" for i, v in enumerate(e))
                print(f"      {ok}")

            except Exception as ex:
                print(f"      ✗ Error general: {ex}")
                todos_ratios.append({"Ticker": ticker_str, "Empresa": nombre})

        if todos_ratios:
            df_cons = pd.DataFrame(todos_ratios)
            df_cons.to_excel(writer, sheet_name="RATIOS CONSOLIDADO", index=False)

    print(f"\n  ✅ Guardado: {output_excel}")
    return pd.DataFrame(todos_ratios)


# ── CSV CUANTITATIVO CONSOLIDADO (3 mercados) ──────────────────────────────────

def generar_csv_cuantitativo(df_m, df_b, df_s):
    df = pd.concat([df_m, df_b, df_s], ignore_index=True)
    if df.empty:
        return

    cols = [
        "Ticker","Empresa","Sector","Industria","País","Moneda",
        "Precio actual","Cap. Mercado","Beta",
        "P/E (trailing)","P/E (forward)","P/B","P/S","EV/EBITDA","PEG Ratio",
        "Valor Graham","Upside vs Graham (%)",
        "ROE (%)","ROA (%)","Margen Bruto (%)","Margen Operativo (%)",
        "Margen Neto (%)","Margen EBITDA (%)",
        "Deuda/Equity","Current Ratio","Quick Ratio",
        "Crec. Ingresos YoY (%)","Crec. Ganancias YoY (%)",
        "Div. Yield (%)","Payout Ratio (%)",
        "Máximo 52s","Mínimo 52s","Dist. Máx 52s (%)","Dist. Mín 52s (%)",
    ]
    cols_ok = [c for c in cols if c in df.columns]
    df_out  = df[cols_ok].copy()

    def score(row):
        s = 0
        try:
            v = row.get("ROE (%)", np.nan)
            if v and not np.isnan(float(v)):   s += min(float(v)/30*25, 25)
            v = row.get("P/E (trailing)", np.nan)
            if v and not np.isnan(float(v)) and float(v) > 0: s += max(0, 25 - float(v)/30*25)
            v = row.get("Deuda/Equity", np.nan)
            if v and not np.isnan(float(v)):   s += max(0, 25 - float(v)/200*25)
            v = row.get("Crec. Ingresos YoY (%)", np.nan)
            if v and not np.isnan(float(v)):   s += min(max(float(v)/30*25, 0), 25)
        except Exception:
            pass
        return round(s, 1)

    df_out["Score Cuantitativo"] = df_out.apply(score, axis=1)
    df_out = df_out.sort_values("Score Cuantitativo", ascending=False)
    df_out.to_csv("ratios_consolidado_quant.csv", sep=";", decimal=",",
                  encoding="utf-8-sig", index=False)

    total = len(df_out)
    print(f"\n  ✅ CSV guardado: ratios_consolidado_quant.csv")
    print(f"     {total} empresas x {len(df_out.columns)} variables\n")
    print(f"  {'Ticker':<12} {'Empresa':<35} {'Score':>6} {'ROE%':>7} {'P/E':>7} {'EV/EBITDA':>10}")
    print(f"  {'-'*75}")
    for _, r in df_out.head(10).iterrows():
        def fmt(k):
            v = r.get(k, np.nan)
            try:
                return f"{float(v):.1f}" if v and not np.isnan(float(v)) else "  N/D"
            except:
                return "  N/D"
        print(f"  {r['Ticker']:<12} {str(r['Empresa']):<35} "
              f"{r['Score Cuantitativo']:>6.1f} {fmt('ROE (%)'):>7} "
              f"{fmt('P/E (trailing)'):>7} {fmt('EV/EBITDA'):>10}")


# ── EJECUCIÓN PRINCIPAL ────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*65)
    print("  ESTADOS CONTABLES Y FINANCIEROS — MERVAL + BOVESPA + S&P 500  v3")
    print(f"  Inicio: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*65)
    print("  Duración estimada: 8-15 minutos.\n")

    df_m = descargar_estados(MERVAL,  "MERVAL (Argentina)", "merval_estados_contables.xlsx")
    df_b = descargar_estados(BOVESPA, "BOVESPA (Brasil)",   "bovespa_estados_contables.xlsx")
    df_s = descargar_estados(SP500,   "S&P 500 (EE.UU.)",   "sp500_estados_contables.xlsx")

    generar_csv_cuantitativo(df_m, df_b, df_s)

    print("\n" + "="*65)
    print(f"  FIN: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*65)
    print("\n  ARCHIVOS GENERADOS:")
    print("  ├── merval_estados_contables.xlsx   (hojas: BS / P&L / CF / Divs / Ratios)")
    print("  ├── bovespa_estados_contables.xlsx  (ídem Brasil)")
    print("  ├── sp500_estados_contables.xlsx    (ídem EE.UU.)")
    print("  └── ratios_consolidado_quant.csv    (3 mercados x 34 variables)")
    print("\n  ⚠️  Fuente: Yahoo Finance. Verificar con CNV/CVM/SEC/BYMA/B3/NYSE para uso formal.")
