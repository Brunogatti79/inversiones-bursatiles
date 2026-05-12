""" 
src/generator.py
Genera el dashboard HTML dark y las fichas Excel.
Versión actualizada con:
  1. Variación diaria en cards Panorama + refresh cada 10s
  2. Leyenda en gráfico base-100 + eje temporal con fecha más reciente a la derecha
  3. MERVAL usa gráfico de líneas (igual que BOVESPA y S&P 500)
  4. Sección "Radar de Oportunidades Tempranas" en Conclusiones (ranking por score)
  5. Cards Panorama más grandes con mejor jerarquía visual
  6. Sort cronológico real en gráfico comparativo base-100
  7. [NUEVO] Solapa "Oportunidades de Compra" con análisis técnico completo
"""
 
import os
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional
 
logger = logging.getLogger(__name__)
logger.warning("=== GENERATOR VERSION NUEVA 2026-05-08 ===")
 
 
# ─────────────────────────────────────────────
# Helpers técnicos para oportunidades
# ─────────────────────────────────────────────
 
def _rsi(serie, p=14):
    d = serie.diff().dropna()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    rs = g / l.replace(0, float('nan'))
    r = 100 - 100/(1+rs)
    return float(r.iloc[-1]) if len(r) >= p else 50.0
 
def _momentum(serie, p=21):
    if len(serie) < p+1: return 0.0
    return float((serie.iloc[-1]/serie.iloc[-p]-1)*100)
 
def _find_levels(serie, window=15):
    highs = serie.rolling(window, center=True).max()
    lows  = serie.rolling(window, center=True).min()
    precio = float(serie.iloc[-1])
    res = sorted(set([round(float(r),2) for r in serie[serie==highs].dropna().values if r > precio]))[:3]
    sup = sorted(set([round(float(s),2) for s in serie[serie==lows].dropna().values  if s < precio]), reverse=True)[:3]
    return sup, res
 
def _build_oportunidades(signals, price_data):
    """
    Construye fichas de oportunidades de compra con análisis técnico.
    price_data: dict {'merval': df, 'bovespa': df, 'sp500': df}
    """
    compras = [s for s in signals if 'COMPRA' in s.get('signal','')]
    compras.sort(key=lambda x: x['score_final'], reverse=True)
 
    fichas = []
    for s in compras:
        ticker  = s['ticker']
        empresa = s['empresa']
        market  = s['mercado']
        df_key  = 'merval' if market=='MERVAL' else 'bovespa' if market=='BOVESPA' else 'sp500'
        df      = price_data.get(df_key)
        if df is None or df.empty:
            continue
 
        # Buscar columna
        col = None
        for c in df.columns:
            if any(w in c for w in empresa.split()[:2]):
                col = c; break
        if col is None:
            continue
 
        serie = df[col].dropna()
        if len(serie) < 20:
            continue
 
        precio   = float(serie.iloc[-1])
        max12m   = float(serie.max())
        min12m   = float(serie.min())
        max_dt   = serie.idxmax().strftime('%d/%m/%Y') if hasattr(serie.idxmax(), 'strftime') else ''
        min_dt   = serie.idxmin().strftime('%d/%m/%Y') if hasattr(serie.idxmin(), 'strftime') else ''
        dist_max = round((max12m - precio) / max12m * 100, 1) if max12m > 0 else 0
 
        ma20 = float(serie.rolling(20).mean().iloc[-1]) if len(serie)>=20 else precio
        ma50 = float(serie.rolling(50).mean().iloc[-1]) if len(serie)>=50 else precio
        ma200 = float(serie.rolling(200).mean().iloc[-1]) if len(serie)>=200 else None
 
        sup, res = _find_levels(serie)
 
        # Punto entrada
        entrada = round(min(precio, ma20) * 0.99, 2)
        stop    = round(sup[1]*0.985 if len(sup)>1 else entrada*0.94, 2)
        target  = round(res[0]*0.995 if res else precio*1.12, 2)
        riesgo  = round((entrada-stop)/entrada*100, 1) if entrada > 0 else 0
        reward  = round((target-entrada)/entrada*100, 1) if entrada > 0 else 0
        rr      = round(reward/riesgo, 1) if riesgo > 0 else 0
 
        # Serie de 60 sesiones para gráfico
        tail60 = serie.tail(60)
        closes60 = [round(float(v), 2) for v in tail60.values]
        dates60  = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in tail60.index]
 
        # MA lines
        ma20_line, ma50_line = [], []
        if len(serie) >= 80:
            s20 = serie.rolling(20).mean()
            ma20_line = [round(float(v),2) if not pd.isna(v) else None for v in s20.tail(60).values]
        if len(serie) >= 110:
            s50 = serie.rolling(50).mean()
            ma50_line = [round(float(v),2) if not pd.isna(v) else None for v in s50.tail(60).values]
 
        flag   = '🇦🇷' if market=='MERVAL' else '🇧🇷' if market=='BOVESPA' else '🇺🇸'
        moneda = 'ARS' if market=='MERVAL' else 'BRL' if market=='BOVESPA' else 'USD'
 
        fichas.append({
            'ticker': ticker, 'empresa': empresa, 'market': market,
            'flag': flag, 'moneda': moneda,
            'precio': round(precio,2), 'max12m': round(max12m,2), 'min12m': round(min12m,2),
            'max_dt': max_dt, 'min_dt': min_dt, 'dist_max': dist_max,
            'ma20': round(ma20,2), 'ma50': round(ma50,2),
            'ma200': round(ma200,2) if ma200 else None,
            'ma_cross': bool(serie.rolling(20).mean().iloc[-1] > serie.rolling(50).mean().iloc[-1]) if len(serie)>=50 else False,
            'rsi': round(s.get('rsi',50),1),
            'momentum': round(s.get('momentum_21d', _momentum(serie)),1),
            'ret_anual': round(s.get('ret_anual',0),1),
            'soportes': sup, 'resistencias': res,
            'entrada': entrada, 'stop': stop, 'target': target,
            'riesgo': riesgo, 'reward': reward, 'rr': rr,
            'score_macro': round(s.get('score_macro',0),1),
            'score_tec':   round(s.get('score_tecnico',0),1),
            'score_fund':  round(s.get('score_fundamental',50),1),
            'score_final': round(s.get('score_final',0),1),
            'signal': s.get('signal',''),
            'closes60': closes60, 'dates60': dates60,
            'ma20_line': ma20_line, 'ma50_line': ma50_line,
        })
    return fichas
 
 
# ─────────────────────────────────────────────
# Dashboard HTML
# ─────────────────────────────────────────────
 
def generate_dashboard(
    signals: list[dict],
    index_stats: dict,
    output_path: str,
    run_date: str = "",
    price_data: dict = None,
) -> str:
    """Genera el HTML del dashboard y lo escribe en output_path."""
 
    # Construir fichas de oportunidades
    fichas = []
    if price_data:
        try:
            fichas = _build_oportunidades(signals, price_data)
        except Exception as e:
            logger.warning(f"No se pudieron generar fichas de oportunidades: {e}")
 
    signals_json     = json.dumps(signals,     ensure_ascii=False)
    index_stats_json = json.dumps(index_stats, ensure_ascii=False)
    fichas_json      = json.dumps(fichas,      ensure_ascii=False, default=str)
 
    merval_labels  = index_stats.get("merval",  {}).get("monthly_labels", [])
    merval_values  = index_stats.get("merval",  {}).get("monthly_values", [])
    bovespa_labels = index_stats.get("bovespa", {}).get("monthly_labels", [])
    bovespa_values = index_stats.get("bovespa", {}).get("monthly_values", [])
    sp500_labels   = index_stats.get("sp500",   {}).get("monthly_labels", [])
    sp500_values   = index_stats.get("sp500",   {}).get("monthly_values", [])
 
    m_ret  = index_stats.get("merval",  {}).get("ret_anual",  0)
    b_ret  = index_stats.get("bovespa", {}).get("ret_anual",  0)
    s_ret  = index_stats.get("sp500",   {}).get("ret_anual",  0)
    m_act  = index_stats.get("merval",  {}).get("actual",     0)
    b_act  = index_stats.get("bovespa", {}).get("actual",     0)
    s_act  = index_stats.get("sp500",   {}).get("actual",     0)
    m_vol  = index_stats.get("merval",  {}).get("volatilidad", 0)
    b_vol  = index_stats.get("bovespa", {}).get("volatilidad", 0)
    s_vol  = index_stats.get("sp500",   {}).get("volatilidad", 0)
    m_day  = index_stats.get("merval",  {}).get("ret_dia", None)
    b_day  = index_stats.get("bovespa", {}).get("ret_dia", None)
    s_day  = index_stats.get("sp500",   {}).get("ret_dia", None)
 
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Inversiones Bursátiles — {run_date}</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0d0d0f;color:#e8e8ea;font-size:14px}}
  .header{{background:#111115;border-bottom:1px solid #222230;padding:20px 32px;display:flex;justify-content:space-between;align-items:center}}
  .header h1{{font-size:20px;font-weight:600;color:#fff}}
  .badge{{display:inline-block;background:#1e3a5f;color:#5ba3ff;padding:3px 10px;border-radius:10px;font-size:11px;font-weight:600;margin-bottom:4px}}
  .tabs{{display:flex;background:#111115;border-bottom:1px solid #222230;padding:0 32px;gap:4px;position:sticky;top:0;z-index:100;overflow-x:auto}}
  .tab{{padding:12px 18px;cursor:pointer;font-size:13px;color:#888;border-bottom:2px solid transparent;white-space:nowrap}}
  .tab.on{{color:#5ba3ff;border-bottom-color:#5ba3ff;font-weight:500}}
  .page{{display:none;padding:28px 32px;max-width:1200px}}
  .page.on{{display:block}}
  .grid-3{{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:24px}}
  .grid-4{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}}
  .card{{background:#16161e;border:1px solid #222230;border-radius:10px;padding:16px}}
  .card-title{{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.7px;margin-bottom:6px}}
  .card-value{{font-size:24px;font-weight:700;color:#fff;line-height:1}}
  .card-sub{{font-size:11px;color:#666;margin-top:5px}}
  .pos{{color:#4ade80}}.neg{{color:#f87171}}
  .section-title{{font-size:16px;font-weight:600;color:#fff;margin-bottom:16px;padding-bottom:8px;border-bottom:1px solid #222230}}
  .tbl{{width:100%;border-collapse:collapse;margin-bottom:24px}}
  .tbl th{{text-align:left;padding:9px 12px;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #222230}}
  .tbl td{{padding:10px 12px;border-bottom:1px solid #1a1a22;font-size:13px}}
  .tbl tr:hover td{{background:#1a1a24}}
  .ticker{{font-weight:700;color:#5ba3ff;font-family:monospace;font-size:12px}}
  .chart-wrap{{position:relative;width:100%;height:260px;margin-bottom:24px}}
  .sig-buy{{color:#4ade80}}.sig-neu{{color:#fbbf24}}.sig-sell{{color:#fb923c}}
  .pano-header{{display:flex;flex-direction:column;gap:12px;margin-bottom:24px}}
  .pano-card{{width:100%;background:#16161e;border:1px solid #222230;border-radius:12px;padding:20px 24px;display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:0 20px}}
  .pano-flag{{font-size:36px;grid-row:1/4}}
  .pano-label{{font-size:11px;color:#555;text-transform:uppercase;letter-spacing:1.5px;font-weight:600}}
  .pano-value{{font-size:clamp(28px,7vw,48px);font-weight:900;color:#fff;line-height:1;letter-spacing:-1px}}
  .pano-anual{{font-size:clamp(24px,6vw,40px);font-weight:900;line-height:1}}
  .pano-day-row{{display:flex;align-items:center;gap:6px;grid-column:2}}
  .pano-day-label{{font-size:10px;color:#444;text-transform:uppercase;letter-spacing:.5px}}
  .pano-day-value{{font-size:clamp(16px,4vw,22px);font-weight:700}}
  .pano-day-dot{{width:8px;height:8px;border-radius:50%;animation:pulse 2s infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
  .pano-vol{{font-size:12px;color:#444;grid-column:3;text-align:right;align-self:center}}
  .chart-legend{{display:flex;gap:20px;margin-bottom:10px;flex-wrap:wrap}}
  .legend-item{{display:flex;align-items:center;gap:6px;font-size:12px;color:#aaa}}
  .radar-card{{background:#0d1a0d;border:1px solid #1a3320;border-radius:10px;padding:18px;margin-bottom:14px;display:flex;align-items:center;gap:16px;transition:background .2s}}
  .radar-card:hover{{background:#112211}}
  .radar-rank{{font-size:28px;font-weight:900;color:#1e3a22;min-width:44px;text-align:center;line-height:1}}
  .radar-info{{flex:1}}
  .radar-ticker{{font-size:15px;font-weight:700;color:#5ba3ff;font-family:monospace}}
  .radar-metrics{{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:#aaa}}
  .radar-score-wrap{{text-align:right;min-width:80px}}
  .radar-score{{font-size:26px;font-weight:900;line-height:1}}
  .radar-score-label{{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.5px}}
  .radar-bar-wrap{{width:100%;height:4px;background:#1a2a1a;border-radius:2px;margin-top:8px}}
  .radar-bar{{height:4px;border-radius:2px}}
  .radar-signals{{display:flex;gap:8px;margin-top:6px;flex-wrap:wrap}}
  .radar-tag{{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:600}}
  .tag-green{{background:#0d2b1a;color:#4ade80;border:1px solid #1a4a2a}}
  .tag-yellow{{background:#2b2100;color:#fbbf24;border:1px solid #4a3500}}
  .tag-blue{{background:#0d1e3a;color:#5ba3ff;border:1px solid #1a3560}}
  .tag-orange{{background:#2b1500;color:#fb923c;border:1px solid #4a2800}}
  .radar-criteria{{background:#111115;border:1px solid #1a1a2e;border-radius:8px;padding:12px 16px;margin-bottom:20px;font-size:12px;color:#666;display:flex;gap:20px;flex-wrap:wrap}}
 
  /* ── OPORTUNIDADES DE COMPRA ── */
  .op-tabs{{display:flex;gap:4px;margin-bottom:16px;overflow-x:auto;padding-bottom:4px}}
  .op-tab{{padding:7px 14px;cursor:pointer;font-size:11px;color:#888;border:1px solid #222230;border-radius:6px;white-space:nowrap;background:#16161e;font-family:monospace}}
  .op-tab.on{{color:#5ba3ff;border-color:#5ba3ff;background:#0d1e3a;font-weight:600}}
  .op-rank-row{{background:#16161e;border:1px solid #222230;border-radius:8px;padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px;cursor:pointer;transition:border-color .15s;flex-wrap:wrap}}
  .op-rank-row:hover{{border-color:#5ba3ff}}
  .op-num{{font-size:18px;font-weight:900;color:#2a2a3a;min-width:28px;font-family:monospace}}
  .op-num.gold{{color:#d29922}}
  .op-main{{flex:1;min-width:140px}}
  .op-ticker{{font-size:14px;font-weight:700;color:#5ba3ff;font-family:monospace}}
  .op-emp{{font-size:11px;color:#666;margin-top:1px}}
  .op-sbar{{width:100%;height:3px;background:#1a1a2e;border-radius:2px;margin-top:5px}}
  .op-sbarf{{height:3px;border-radius:2px}}
  .op-mets{{display:flex;gap:14px;flex-wrap:wrap;align-items:center}}
  .op-m{{display:flex;flex-direction:column;align-items:flex-end;min-width:44px}}
  .op-mv{{font-size:12px;font-weight:600;font-family:monospace}}
  .op-ml{{font-size:9px;color:#555;text-transform:uppercase}}
  .op-sig{{font-size:10px;font-weight:700;padding:3px 8px;border-radius:3px;white-space:nowrap}}
  .op-sig-c{{background:#0d2b1a;color:#4ade80;border:1px solid #1a4a2a}}
  .op-sig-f{{background:#2a1f00;color:#d29922;border:1px solid #4a3500}}
  .op-ficha-hdr{{display:flex;align-items:flex-start;gap:14px;margin-bottom:14px;flex-wrap:wrap}}
  .op-ftick{{font-size:26px;font-weight:900;color:#5ba3ff;font-family:monospace}}
  .op-femp{{font-size:13px;color:#888;margin-top:2px}}
  .op-fprice{{font-size:22px;font-weight:700;margin-left:auto;text-align:right}}
  .op-card{{background:#16161e;border:1px solid #222230;border-radius:8px;padding:12px;margin-bottom:12px}}
  .op-card h3{{font-size:10px;color:#666;text-transform:uppercase;letter-spacing:.6px;margin-bottom:8px}}
  .op-chart-wrap{{position:relative;width:100%;height:260px;margin-bottom:12px}}
  .op-rrbox{{background:#111115;border-radius:8px;padding:12px;display:grid;grid-template-columns:repeat(3,1fr);gap:8px;text-align:center;margin-bottom:12px;border:1px solid #1a1a2e}}
  .op-rrval{{font-size:18px;font-weight:900;font-family:monospace}}
  .op-rrlbl{{font-size:9px;color:#666;text-transform:uppercase;letter-spacing:.4px;margin-top:2px}}
  .op-fgrid{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
  .op-lvl{{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #1a1a22;font-family:monospace;font-size:12px}}
  .op-ltag{{font-size:9px;padding:2px 5px;border-radius:3px;font-weight:700;margin-right:4px}}
  .op-lt-r{{background:#1f0d0d;color:#f87171}}
  .op-lt-s{{background:#0d1f0d;color:#4ade80}}
  .op-lt-e{{background:#0d1e3a;color:#5ba3ff}}
  .op-lt-st{{background:#2b1500;color:#fb923c}}
  .op-lt-tg{{background:#1a0d2b;color:#bc8cff}}
  .op-sc-row{{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #1a1a22}}
  .op-sc-name{{font-size:11px;color:#666}}
  .op-sc-val{{font-size:14px;font-weight:700;font-family:monospace}}
  .op-sc-bar{{width:60px;height:3px;background:#1a1a2e;border-radius:2px;margin-top:2px}}
  .op-sc-fill{{height:3px;border-radius:2px}}
  .op-techbox{{background:#111115;border-radius:6px;padding:8px 10px;margin-top:8px;display:flex;gap:12px;flex-wrap:wrap;font-family:monospace;font-size:11px}}
  @media(max-width:768px){{
    .grid-3,.grid-4{{grid-template-columns:repeat(2,1fr)}}
    .page{{padding:16px}}
    .op-fgrid{{grid-template-columns:1fr}}
    .radar-card{{flex-direction:column;align-items:flex-start}}
  }}
</style>
</head>
<body>
<div class="header">
  <div>
    <div class="badge">INVERSIONES BURSÁTILES</div>
    <h1>Informe de Inversiones</h1>
    <div style="font-size:12px;color:#666;margin-top:3px">MERVAL · BOVESPA · S&P 500 · Generado {run_date}</div>
  </div>
  <div style="text-align:right;font-size:12px;color:#666">Pipeline automático<br>Modelo Fase 2 — 35M+35T+10S+20F</div>
</div>
 
<div class="tabs">
  <div class="tab on" onclick="sw('panorama',this)">Panorama</div>
  <div class="tab"    onclick="sw('merval',this)">MERVAL</div>
  <div class="tab"    onclick="sw('bovespa',this)">BOVESPA</div>
  <div class="tab"    onclick="sw('sp500',this)">S&amp;P 500</div>
  <div class="tab"    onclick="sw('oportunidades',this)">🟢 Oportunidades</div>
  <div class="tab"    onclick="sw('conclusiones',this)">Conclusiones</div>
</div>
 
<!-- PANORAMA -->
<div id="panorama" class="page on">
  <div class="pano-header">
    <div class="pano-card">
      <div class="pano-flag">🇦🇷</div>
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="pano-label">MERVAL</div>
        <div class="pano-value" id="pano-m-val">{m_act:,.0f}</div>
        <div class="pano-anual" id="pano-m-anual" style="color:{'#4ade80' if m_ret>=0 else '#f87171'}">{'+ ' if m_ret>=0 else ''}{m_ret:.2f}%</div>
        <div class="pano-day-row">
          <div class="pano-day-dot" id="dot-m" style="background:#4ade80"></div>
          <span class="pano-day-label">HOY</span>
          <span class="pano-day-value" id="pano-m-day">{'—' if m_day is None else ('+' if m_day>=0 else '')+f'{m_day:.2f}%'}</span>
        </div>
      </div>
      <div class="pano-vol">Vol<br>{m_vol:.1f}%</div>
    </div>
    <div class="pano-card">
      <div class="pano-flag">🇧🇷</div>
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="pano-label">BOVESPA</div>
        <div class="pano-value" id="pano-b-val">{b_act:,.0f}</div>
        <div class="pano-anual" id="pano-b-anual" style="color:{'#4ade80' if b_ret>=0 else '#f87171'}">{'+ ' if b_ret>=0 else ''}{b_ret:.2f}%</div>
        <div class="pano-day-row">
          <div class="pano-day-dot" id="dot-b" style="background:#4ade80"></div>
          <span class="pano-day-label">HOY</span>
          <span class="pano-day-value" id="pano-b-day">{'—' if b_day is None else ('+' if b_day>=0 else '')+f'{b_day:.2f}%'}</span>
        </div>
      </div>
      <div class="pano-vol">Vol<br>{b_vol:.1f}%</div>
    </div>
    <div class="pano-card">
      <div class="pano-flag">🇺🇸</div>
      <div style="display:flex;flex-direction:column;gap:4px">
        <div class="pano-label">S&amp;P 500</div>
        <div class="pano-value" id="pano-s-val">{s_act:,.0f}</div>
        <div class="pano-anual" id="pano-s-anual" style="color:{'#4ade80' if s_ret>=0 else '#f87171'}">{'+ ' if s_ret>=0 else ''}{s_ret:.2f}%</div>
        <div class="pano-day-row">
          <div class="pano-day-dot" id="dot-s" style="background:#4ade80"></div>
          <span class="pano-day-label">HOY</span>
          <span class="pano-day-value" id="pano-s-day">{'—' if s_day is None else ('+' if s_day>=0 else '')+f'{s_day:.2f}%'}</span>
        </div>
      </div>
      <div class="pano-vol">Vol<br>{s_vol:.1f}%</div>
    </div>
  </div>
  <div class="section-title">Evolución comparativa — base 100</div>
  <div class="chart-legend">
    <div class="legend-item"><div style="background:#5ba3ff;height:3px;width:24px;border-radius:2px"></div><span style="color:#5ba3ff;font-weight:600">MERVAL 🇦🇷</span></div>
    <div class="legend-item"><div style="background:#4ade80;height:3px;width:24px;border-radius:2px"></div><span style="color:#4ade80;font-weight:600">BOVESPA 🇧🇷</span></div>
    <div class="legend-item"><div style="background:#fbbf24;height:3px;width:24px;border-radius:2px"></div><span style="color:#fbbf24;font-weight:600">S&amp;P 500 🇺🇸</span></div>
  </div>
  <div class="chart-wrap"><canvas id="chartPano"></canvas></div>
  <div class="section-title">Ranking global de oportunidades</div>
  <table class="tbl" id="tbl-global"></table>
</div>
 
<!-- MERVAL -->
<div id="merval" class="page">
  <div class="section-title">MERVAL — Estadísticas 12 meses</div>
  <div class="grid-4" id="merval-stats"></div>
  <div class="chart-wrap"><canvas id="chartMerval"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-merval"></table>
</div>
 
<!-- BOVESPA -->
<div id="bovespa" class="page">
  <div class="section-title">BOVESPA — Estadísticas 12 meses</div>
  <div class="grid-4" id="bovespa-stats"></div>
  <div class="chart-wrap"><canvas id="chartBovespa"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-bovespa"></table>
</div>
 
<!-- SP500 -->
<div id="sp500" class="page">
  <div class="section-title">S&amp;P 500 — Estadísticas 12 meses</div>
  <div class="grid-4" id="sp500-stats"></div>
  <div class="chart-wrap"><canvas id="chartSP500"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-sp500"></table>
</div>
 
<!-- OPORTUNIDADES DE COMPRA -->
<div id="oportunidades" class="page">
  <div class="section-title" style="color:#4ade80">🟢 Oportunidades de Compra — Análisis Técnico Fase 2</div>
  <div style="font-size:12px;color:#666;margin-bottom:16px;background:#111115;border:1px solid #1a1a2e;border-radius:8px;padding:10px 14px;display:flex;gap:20px;flex-wrap:wrap">
    <span>📊 <b style="color:#aaa">35% Macro</b> · condiciones país</span>
    <span>📈 <b style="color:#aaa">35% Técnico</b> · RSI, momentum, MA</span>
    <span>🏭 <b style="color:#aaa">10% Sectorial</b> · ciclo industria</span>
    <span>📋 <b style="color:#aaa">20% Fundamental</b> · ratios financieros</span>
  </div>
  <div id="op-rank-page">
    <div style="font-size:13px;font-weight:600;color:#aaa;margin-bottom:12px;padding-bottom:7px;border-bottom:1px solid #222230">
      Acciones con señal de compra — Score Final descendente
    </div>
    <div id="op-rg"></div>
  </div>
  <div id="op-ficha-page" style="display:none">
    <button onclick="showOpRank()" style="background:#16161e;border:1px solid #222230;color:#5ba3ff;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px;margin-bottom:16px">← Volver al ranking</button>
    <div id="op-fi"></div>
  </div>
</div>
 
<!-- CONCLUSIONES -->
<div id="conclusiones" class="page">
  <div class="section-title" style="color:#86efac">🔭 Radar de Oportunidades Tempranas</div>
  <div class="radar-criteria">
    <span>📊 <b style="color:#aaa">Score técnico</b> — RSI, momentum, cruces MA</span>
    <span>📈 <b style="color:#aaa">Soporte/Resistencia</b> — proximidad a zonas clave</span>
    <span>⚙️ <b style="color:#aaa">Score modelo</b> — macro × técnico × sectorial × fundamental</span>
  </div>
  <div id="radar-block"></div>
  <div class="section-title" style="margin-top:28px">✅ Oportunidades de compra confirmadas</div>
  <div id="compras-block"></div>
  <div class="section-title" style="margin-top:20px">🔴 Señales de reducción</div>
  <div id="ventas-block"></div>
</div>
 
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
var SIGNALS = {signals_json};
var IDX     = {index_stats_json};
var FICHAS  = {fichas_json};
var mL = {json.dumps(merval_labels)};
var mV = {json.dumps(merval_values)};
var bL = {json.dumps(bovespa_labels)};
var bV = {json.dumps(bovespa_values)};
var sL = {json.dumps(sp500_labels)};
var sV = {json.dumps(sp500_values)};
var opChartInst = null;
 
function fn(v,d){{ if(v==null||isNaN(v))return'—'; return Number(v).toFixed(d!=null?d:2); }}
function fp(v){{ return (v>=0?'+':'')+Number(v).toFixed(1)+'%'; }}
function rc(v){{ return v>=0?'#4ade80':'#f87171'; }}
 
// Day refresh
function refreshDay(){{
  [['pano-m-day','dot-m','merval'],['pano-b-day','dot-b','bovespa'],['pano-s-day','dot-s','sp500']].forEach(function(x){{
    var val=IDX[x[2]]&&IDX[x[2]].ret_dia!==undefined?IDX[x[2]].ret_dia:null;
    var e=document.getElementById(x[0]),d=document.getElementById(x[1]);
    var c=val===null?'#888':val>=0?'#4ade80':'#f87171';
    if(e){{e.textContent=val===null?'—':(val>=0?'+':'')+val.toFixed(2)+'%';e.style.color=c;}}
    if(d) d.style.background=c;
  }});
}}
refreshDay(); setInterval(refreshDay,10000);
 
function sw(id,el){{
  document.querySelectorAll('.tab').forEach(function(t){{t.classList.remove('on');}});
  document.querySelectorAll('.page').forEach(function(p){{p.classList.remove('on');}});
  el.classList.add('on'); document.getElementById(id).classList.add('on');
}}
 
function sigColor(s){{
  if(s.indexOf('COMPRA FUERTE')>=0) return '#ffd700';
  if(s.indexOf('COMPRA')>=0)        return '#4ade80';
  if(s.indexOf('NEUTRAL')>=0)       return '#fbbf24';
  if(s.indexOf('VENTA P')>=0)       return '#fb923c';
  return '#f87171';
}}
 
function buildTable(tbId,market){{
  var rows=market?SIGNALS.filter(function(s){{return s.mercado===market;}}):SIGNALS.slice(0,20);
  var tb=document.getElementById(tbId); if(!tb) return;
  tb.innerHTML='<tr><th>Ticker</th><th>Empresa</th><th>Sector</th><th>Precio</th><th>Sem%</th><th>Mes%</th><th>Anual%</th><th>RSI</th><th>Score</th><th>Señal</th></tr>'+
    rows.map(function(s){{return '<tr><td class="ticker">'+s.ticker+'</td><td style="color:#ccc">'+s.empresa.substring(0,22)+'</td><td style="color:#888;font-size:11px">'+s.sector+'</td><td>'+s.precio_actual.toLocaleString('es-AR')+'</td><td style="color:'+rc(s.ret_sem)+';font-weight:600">'+(s.ret_sem>=0?'+':'')+s.ret_sem.toFixed(1)+'%</td><td style="color:'+rc(s.ret_mes)+';font-weight:600">'+(s.ret_mes>=0?'+':'')+s.ret_mes.toFixed(1)+'%</td><td style="color:'+rc(s.ret_anual)+';font-weight:600">'+(s.ret_anual>=0?'+':'')+s.ret_anual.toFixed(1)+'%</td><td>'+s.rsi.toFixed(0)+'</td><td style="color:#fbbf24;font-weight:700">'+s.score_final.toFixed(0)+'</td><td style="color:'+sigColor(s.signal)+';font-weight:600">'+s.signal+'</td></tr>';}}).join('');
}}
 
function buildStats(divId,marketKey){{
  var st=IDX[marketKey]||{{}};
  var d=document.getElementById(divId); if(!d) return;
  d.innerHTML=[['Cierre actual',st.actual?st.actual.toLocaleString('es-AR'):'—',''],
    ['Variación 12m',st.ret_anual!=null?(st.ret_anual>=0?'+':'')+st.ret_anual.toFixed(2)+'%':'—',st.ret_anual>=0?'#4ade80':'#f87171'],
    ['Máximo 12m',st.max_12m?st.max_12m.toLocaleString('es-AR'):'—','#fbbf24'],
    ['Mínimo 12m',st.min_12m?st.min_12m.toLocaleString('es-AR'):'—','#f87171']]
    .map(function(x){{return '<div class="card"><div class="card-title">'+x[0]+'</div><div class="card-value" style="color:'+(x[2]||'#fff')+'">'+x[1]+'</div></div>';}}).join('');
}}
 
function normalize(arr){{ var b=arr[0]||1; return arr.map(function(v){{return +(v/b*100).toFixed(1);}}); }}
var scaleOpts={{x:{{ticks:{{color:'#666',font:{{size:11}},autoSkip:true,maxTicksLimit:12,maxRotation:45}},grid:{{color:'rgba(255,255,255,.05)'}}}},y:{{ticks:{{color:'#666',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,.05)'}}}}}} ;
 
var monthMap={{Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11}};
function labelToDate(l){{
  var p=l.split('-'),yr=parseInt(p[1])+(parseInt(p[1])<50?2000:1900);
  return new Date(yr,monthMap[p[0]]||0,1);
}}
 
if(mL.length&&bL.length&&sL.length){{
  var allLabels=[].concat(mL,bL,sL).filter(function(v,i,a){{return a.indexOf(v)===i;}}).sort(function(a,b){{return labelToDate(a)-labelToDate(b);}});
  function pick(labels,vals,all){{return all.map(function(l){{var i=labels.indexOf(l);return i>=0?vals[i]:null;}});}}
  var mN=normalize(pick(mL,mV,allLabels).filter(function(v){{return v!==null;}}));
  var bN=normalize(pick(bL,bV,allLabels).filter(function(v){{return v!==null;}}));
  var sN=normalize(pick(sL,sV,allLabels).filter(function(v){{return v!==null;}}));
  new Chart(document.getElementById('chartPano'),{{type:'line',data:{{labels:allLabels,datasets:[
    {{label:'MERVAL',data:mN,borderColor:'#5ba3ff',borderWidth:2.5,pointRadius:3,tension:.3,fill:false}},
    {{label:'BOVESPA',data:bN,borderColor:'#4ade80',borderWidth:2,pointRadius:3,tension:.3,fill:false,borderDash:[5,4]}},
    {{label:'S&P 500',data:sN,borderColor:'#fbbf24',borderWidth:2,pointRadius:3,tension:.3,fill:false,borderDash:[2,3]}},
  ]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{mode:'index',intersect:false}}}},scales:scaleOpts}}}});
}}
 
if(mL.length) new Chart(document.getElementById('chartMerval'),{{type:'line',data:{{labels:mL,datasets:[{{data:mV,borderColor:'#5ba3ff',borderWidth:2.5,pointRadius:3,fill:true,backgroundColor:'rgba(91,163,255,.07)',tension:.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:scaleOpts}}}});
if(bL.length) new Chart(document.getElementById('chartBovespa'),{{type:'line',data:{{labels:bL,datasets:[{{data:bV,borderColor:'#4ade80',borderWidth:2,pointRadius:3,fill:true,backgroundColor:'rgba(74,222,128,.07)',tension:.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:scaleOpts}}}});
if(sL.length) new Chart(document.getElementById('chartSP500'),{{type:'line',data:{{labels:sL,datasets:[{{data:sV,borderColor:'#fbbf24',borderWidth:2,pointRadius:3,fill:true,backgroundColor:'rgba(251,191,36,.07)',tension:.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:scaleOpts}}}});
 
buildTable('tbl-global',null); buildTable('tbl-merval','MERVAL'); buildTable('tbl-bovespa','BOVESPA'); buildTable('tbl-sp500','SP500');
buildStats('merval-stats','merval'); buildStats('bovespa-stats','bovespa'); buildStats('sp500-stats','sp500');
 
// ── RADAR ──────────────────────────────────────────────────────────────────
function computeRadarScore(s){{
  var score=0;
  score+=Math.min(s.score_final/100,1)*35;
  var rsi=s.rsi||50;
  if(rsi>=28&&rsi<=45) score+=20; else if(rsi>45&&rsi<=55) score+=12; else if(rsi>55&&rsi<=65) score+=6;
  var rs=s.ret_sem||0,rm=s.ret_mes||0;
  if(rs>0&&rm<5) score+=20; else if(rs>0&&rm>=5&&rm<15) score+=12; else if(rs>0) score+=6;
  if(s.ma_cross) score+=15;
  if(s.precio_actual&&s.max_12m&&s.max_12m>0){{
    var p=(s.max_12m-s.precio_actual)/s.max_12m;
    if(p>0.35) score+=10; else if(p>0.20) score+=7; else if(p>0.10) score+=4;
  }}
  return Math.min(Math.round(score),100);
}}
function flagOf(m){{ return m==='MERVAL'?'🇦🇷':m==='BOVESPA'?'🇧🇷':'🇺🇸'; }}
var ranked=SIGNALS.filter(function(s){{return s.signal.indexOf('VENTA')<0;}})
  .map(function(s){{return Object.assign({{}},s,{{radar_score:computeRadarScore(s)}});}})
  .sort(function(a,b){{return b.radar_score-a.radar_score;}}).slice(0,10);
var radarHtml=ranked.map(function(s,i){{
  var pfm=s.max_12m>0?((s.max_12m-s.precio_actual)/s.max_12m*100).toFixed(1):'—';
  var tags=[];
  if(s.rsi>=28&&s.rsi<=45) tags.push('<span class="radar-tag tag-green">RSI sobreventa</span>');
  if(s.rsi>45&&s.rsi<=55) tags.push('<span class="radar-tag tag-blue">RSI neutro-pos</span>');
  if(s.ret_sem>0&&s.ret_mes<5) tags.push('<span class="radar-tag tag-green">Arranque temprano</span>');
  if(s.ret_sem>0&&s.ret_mes>=5) tags.push('<span class="radar-tag tag-blue">Momentum activo</span>');
  if(s.ma_cross) tags.push('<span class="radar-tag tag-yellow">Cruce MA</span>');
  if(parseFloat(pfm)>30) tags.push('<span class="radar-tag tag-orange">-'+pfm+'% vs máx</span>');
  if(s.signal.indexOf('COMPRA FUERTE')>=0) tags.push('<span class="radar-tag tag-green">⭐ Compra Fuerte</span>');
  else if(s.signal.indexOf('COMPRA')>=0) tags.push('<span class="radar-tag tag-green">🟢 Compra</span>');
  else tags.push('<span class="radar-tag tag-yellow">🟡 Monitorear</span>');
  var sc=s.radar_score>=70?'#22c55e':s.radar_score>=50?'#86efac':s.radar_score>=35?'#fbbf24':'#fb923c';
  return '<div class="radar-card"><div class="radar-rank" style="font-size:'+(i<3?'32px':'24px')+'">#'+(i+1)+'</div>'+
    '<div class="radar-info"><div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">'+
    '<span class="radar-ticker">'+flagOf(s.mercado)+' '+s.ticker+'</span>'+
    '<span style="font-size:12px;color:#666">'+s.empresa.substring(0,28)+'</span></div>'+
    '<div class="radar-metrics"><span>💰 '+s.precio_actual.toLocaleString('es-AR')+'</span>'+
    '<span style="color:'+rc(s.ret_sem)+'">Sem: '+(s.ret_sem>=0?'+':'')+s.ret_sem.toFixed(1)+'%</span>'+
    '<span style="color:'+rc(s.ret_mes)+'">Mes: '+(s.ret_mes>=0?'+':'')+s.ret_mes.toFixed(1)+'%</span>'+
    '<span>RSI: '+s.rsi.toFixed(0)+'</span><span style="color:#fb923c">-'+pfm+'% vs máx</span></div>'+
    '<div class="radar-signals">'+tags.join('')+'</div>'+
    '<div class="radar-bar-wrap"><div class="radar-bar" style="width:'+s.radar_score+'%;background:'+sc+'"></div></div></div>'+
    '<div class="radar-score-wrap"><div class="radar-score" style="color:'+sc+'">'+s.radar_score+'</div>'+
    '<div class="radar-score-label">Score<br>Radar</div></div></div>';
}}).join('');
var rb=document.getElementById('radar-block');
if(rb) rb.innerHTML=radarHtml||'<div style="color:#666;padding:20px">Sin datos.</div>';
 
var compras=SIGNALS.filter(function(s){{return s.signal.indexOf('COMPRA')>=0;}});
var ventas=SIGNALS.filter(function(s){{return s.signal.indexOf('VENTA')>=0;}});
document.getElementById('compras-block').innerHTML=compras.slice(0,8).map(function(s){{
  return '<div style="background:#0d2b1a;border:1px solid #1a3a1a;border-radius:10px;padding:16px;margin-bottom:12px">'+
    '<div style="font-size:16px;font-weight:700;color:#4ade80;margin-bottom:6px">'+flagOf(s.mercado)+' '+s.signal+' — '+s.ticker+' · '+s.empresa+'</div>'+
    '<div style="font-size:13px;color:#aaa;display:flex;gap:20px;flex-wrap:wrap">'+
    '<span>Score: <b style="color:#fff">'+s.score_final.toFixed(0)+'</b></span>'+
    '<span>RSI: <b style="color:#fff">'+s.rsi.toFixed(0)+'</b></span>'+
    '<span>Sem: <b style="color:'+rc(s.ret_sem)+'">'+(s.ret_sem>=0?'+':'')+s.ret_sem.toFixed(1)+'%</b></span>'+
    '<span>Anual: <b style="color:'+rc(s.ret_anual)+'">'+(s.ret_anual>=0?'+':'')+s.ret_anual.toFixed(1)+'%</b></span></div></div>';
}}).join('')||'<div style="color:#666;padding:16px">Sin señales de compra activas.</div>';
document.getElementById('ventas-block').innerHTML=ventas.slice(0,5).map(function(s){{
  return '<div style="background:#1f0d0d;border:1px solid #3a1a1a;border-radius:10px;padding:16px;margin-bottom:12px">'+
    '<div style="font-size:16px;font-weight:700;color:#fb923c;margin-bottom:6px">'+flagOf(s.mercado)+' '+s.signal+' — '+s.ticker+' · '+s.empresa+'</div>'+
    '<div style="font-size:13px;color:#aaa;display:flex;gap:20px;flex-wrap:wrap">'+
    '<span>Score: <b style="color:#fff">'+s.score_final.toFixed(0)+'</b></span>'+
    '<span>Sem: <b style="color:'+rc(s.ret_sem)+'">'+(s.ret_sem>=0?'+':'')+s.ret_sem.toFixed(1)+'%</b></span>'+
    '<span>Anual: <b style="color:'+rc(s.ret_anual)+'">'+(s.ret_anual>=0?'+':'')+s.ret_anual.toFixed(1)+'%</b></span></div></div>';
}}).join('');
 
// ── OPORTUNIDADES DE COMPRA ────────────────────────────────────────────────
function showOpRank(){{
  document.getElementById('op-rank-page').style.display='block';
  document.getElementById('op-ficha-page').style.display='none';
  if(opChartInst){{try{{opChartInst.destroy();}}catch(e){{}} opChartInst=null;}}
}}
 
var opRg=document.getElementById('op-rg');
if(FICHAS.length===0){{
  opRg.innerHTML='<div style="color:#666;padding:24px;text-align:center">Sin señales de compra activas en este período.</div>';
}} else {{
  for(var i=0;i<FICHAS.length;i++){{
    (function(f,idx){{
      var sc2=f.score_final>=65?'#d29922':'#4ade80';
      var row=document.createElement('div');
      row.className='op-rank-row';
      var num=document.createElement('div');
      num.className='op-num'+(idx<3?' gold':'');
      num.textContent='#'+(idx+1);
      row.appendChild(num);
      var main=document.createElement('div');
      main.className='op-main';
      main.innerHTML='<div class="op-ticker">'+f.flag+' '+f.ticker+'</div>'+
        '<div class="op-emp">'+f.empresa+' · '+f.market+'</div>'+
        '<div class="op-sbar"><div class="op-sbarf" style="width:'+f.score_final+'%;background:'+sc2+'"></div></div>';
      row.appendChild(main);
      var mets=document.createElement('div');
      mets.className='op-mets';
      mets.innerHTML=
        '<div class="op-m"><span class="op-mv">'+fn(f.precio)+'</span><span class="op-ml">'+f.moneda+'</span></div>'+
        '<div class="op-m"><span class="op-mv" style="color:'+rc(f.ret_anual)+'">'+fp(f.ret_anual)+'</span><span class="op-ml">12m</span></div>'+
        '<div class="op-m"><span class="op-mv" style="color:'+(f.rsi<40?'#4ade80':f.rsi>65?'#f87171':'#fbbf24')+'">'+fn(f.rsi,1)+'</span><span class="op-ml">RSI</span></div>'+
        '<div class="op-m"><span class="op-mv" style="color:#f87171">-'+fn(f.dist_max,1)+'%</span><span class="op-ml">vs Máx</span></div>'+
        '<div class="op-m"><span class="op-mv" style="color:'+sc2+'">'+fn(f.score_final,1)+'</span><span class="op-ml">Score</span></div>'+
        '<div class="op-m"><span class="op-mv" style="color:#bc8cff">'+fn(f.rr,1)+'x</span><span class="op-ml">R/R</span></div>'+
        '<span class="op-sig '+(f.signal.indexOf('FUERTE')>=0?'op-sig-f':'op-sig-c')+'">'+f.signal+'</span>';
      row.appendChild(mets);
      row.onclick=function(){{showOpFicha(f.ticker);}};
      opRg.appendChild(row);
    }})(FICHAS[i],i);
  }}
}}
 
function showOpFicha(ticker){{
  var f=null;
  for(var i=0;i<FICHAS.length;i++){{if(FICHAS[i].ticker===ticker){{f=FICHAS[i];break;}}}}
  if(!f) return;
  document.getElementById('op-rank-page').style.display='none';
  document.getElementById('op-ficha-page').style.display='block';
  if(opChartInst){{try{{opChartInst.destroy();}}catch(e){{}} opChartInst=null;}}
 
  var sc2=f.score_final>=65?'#d29922':'#4ade80';
  var res=f.resistencias||[], sup=f.soportes||[];
  var lvls='';
  for(var i=res.length-1;i>=0;i--){{
    var d=fn((res[i]-f.precio)/f.precio*100,1);
    lvls+='<div class="op-lvl"><span><span class="op-ltag op-lt-r">R</span>'+fn(res[i])+'</span><span style="color:#666;font-size:10px">+'+d+'%</span></div>';
  }}
  lvls+='<div class="op-lvl" style="background:#111115"><span><span class="op-ltag op-lt-e">ENTRADA</span>'+fn(f.entrada)+'</span><span style="color:#5ba3ff;font-size:10px">actual '+fn(f.precio)+'</span></div>';
  for(var i=0;i<sup.length;i++){{
    var d2=fn((f.precio-sup[i])/f.precio*100,1);
    lvls+='<div class="op-lvl"><span><span class="op-ltag op-lt-s">S</span>'+fn(sup[i])+'</span><span style="color:#666;font-size:10px">-'+d2+'%</span></div>';
  }}
  lvls+='<div class="op-lvl"><span><span class="op-ltag op-lt-st">STOP</span>'+fn(f.stop)+'</span><span style="color:#fb923c;font-size:10px">-'+fn(f.riesgo,1)+'%</span></div>';
  lvls+='<div class="op-lvl"><span><span class="op-ltag op-lt-tg">TARGET</span>'+fn(f.target)+'</span><span style="color:#bc8cff;font-size:10px">+'+fn(f.reward,1)+'%</span></div>';
 
  document.getElementById('op-fi').innerHTML=
    '<div class="op-ficha-hdr">'+
      '<div><div class="op-ftick">'+f.flag+' '+f.ticker+'</div><div class="op-femp">'+f.empresa+' · '+f.market+'</div></div>'+
      '<div class="op-fprice">'+fn(f.precio)+' <span style="font-size:11px;color:#666">'+f.moneda+'</span><br>'+
        '<span style="font-size:10px;color:#666">Máx12m: '+fn(f.max12m)+' ('+f.max_dt+') · Mín: '+fn(f.min12m)+' ('+f.min_dt+')</span></div>'+
    '</div>'+
    '<div class="op-card">'+
      '<h3>📈 Precio + MA20 + MA50 · 60 sesiones · Soportes/Resistencias</h3>'+
      '<div class="op-chart-wrap"><canvas id="opChartF"></canvas></div>'+
    '</div>'+
    '<div class="op-rrbox">'+
      '<div><div class="op-rrval" style="color:#5ba3ff">'+fn(f.entrada)+'</div><div class="op-rrlbl">Entrada</div></div>'+
      '<div><div class="op-rrval" style="color:#bc8cff">'+fn(f.rr,1)+'x</div><div class="op-rrlbl">R/Recompensa</div></div>'+
      '<div><div class="op-rrval" style="color:#4ade80">+'+fn(f.reward,1)+'%</div><div class="op-rrlbl">Upside</div></div>'+
    '</div>'+
    '<div class="op-fgrid">'+
      '<div class="op-card"><h3>📐 Niveles operativos</h3>'+lvls+
        '<div style="margin-top:8px;font-size:10px;color:#555">'+
          (f.ma_cross?'✅ MA20 > MA50':'⚠️ MA20 ≤ MA50')+
          ' · MA20: '+fn(f.ma20)+' · MA50: '+fn(f.ma50)+
          (f.ma200?' · MA200: '+fn(f.ma200):'')+
        '</div></div>'+
      '<div class="op-card"><h3>🧮 Scoring Fase 2 (35M+35T+10S+20F)</h3>'+
        '<div class="op-sc-row"><span class="op-sc-name">Macro (35%)</span>'+
          '<div><span class="op-sc-val" style="color:#fbbf24">'+fn(f.score_macro,1)+'</span>'+
          '<div class="op-sc-bar"><div class="op-sc-fill" style="width:'+f.score_macro+'%;background:#fbbf24"></div></div></div></div>'+
        '<div class="op-sc-row"><span class="op-sc-name">Técnico (35%)</span>'+
          '<div><span class="op-sc-val" style="color:#5ba3ff">'+fn(f.score_tec,1)+'</span>'+
          '<div class="op-sc-bar"><div class="op-sc-fill" style="width:'+f.score_tec+'%;background:#5ba3ff"></div></div></div></div>'+
        '<div class="op-sc-row"><span class="op-sc-name">Fundamental (20%)</span>'+
          '<div><span class="op-sc-val" style="color:#bc8cff">'+fn(f.score_fund,1)+'</span>'+
          '<div class="op-sc-bar"><div class="op-sc-fill" style="width:'+f.score_fund+'%;background:#bc8cff"></div></div></div></div>'+
        '<div class="op-sc-row" style="border-bottom:none">'+
          '<span class="op-sc-name" style="font-weight:700;color:#fff">SCORE FINAL</span>'+
          '<div><span class="op-sc-val" style="font-size:18px;color:'+sc2+'">'+fn(f.score_final,1)+'</span>'+
          '<div class="op-sc-bar"><div class="op-sc-fill" style="width:'+f.score_final+'%;background:'+sc2+'"></div></div></div></div>'+
        '<div class="op-techbox">'+
          '<span>RSI <b style="color:'+(f.rsi<40?'#4ade80':f.rsi>65?'#f87171':'#fbbf24')+'">'+fn(f.rsi,1)+'</b></span>'+
          '<span>Mom <b style="color:'+rc(f.momentum)+'">'+fp(f.momentum)+'</b></span>'+
          '<span style="color:#666">-'+fn(f.dist_max,1)+'% del máx</span>'+
          '<span>12m <b style="color:'+rc(f.ret_anual)+'">'+fp(f.ret_anual)+'</b></span>'+
        '</div>'+
      '</div>'+
    '</div>';
 
  setTimeout(function(){{
    var canvas=document.getElementById('opChartF');
    if(!canvas) return;
    var ctx=canvas.getContext('2d');
    var closes=f.closes60||[], labels=(f.dates60||[]).map(function(d){{return d.slice(5);}});
    var datasets=[{{
      label:f.ticker, data:closes,
      borderColor:'#5ba3ff', borderWidth:2, pointRadius:0, tension:.3,
      fill:true, backgroundColor:'rgba(91,163,255,0.06)',
    }}];
    if(f.ma20_line&&f.ma20_line.length) datasets.push({{
      label:'MA20', data:f.ma20_line,
      borderColor:'#fb923c', borderWidth:1.5, pointRadius:0, tension:.3, fill:false, borderDash:[4,3],
    }});
    if(f.ma50_line&&f.ma50_line.length) datasets.push({{
      label:'MA50', data:f.ma50_line,
      borderColor:'#bc8cff', borderWidth:1.5, pointRadius:0, tension:.3, fill:false, borderDash:[8,4],
    }});
    var lp={{id:'lp',afterDraw:function(ch){{
      var c2=ch.ctx,ya=ch.scales.y;
      var x0=ch.chartArea.left,x1=ch.chartArea.right,top=ch.chartArea.top,bot=ch.chartArea.bottom;
      function dl(val,color,lbl){{
        if(!val||isNaN(val))return;
        var y=ya.getPixelForValue(val);
        if(y<top||y>bot)return;
        c2.save();c2.strokeStyle=color;c2.lineWidth=1;c2.setLineDash([4,4]);
        c2.beginPath();c2.moveTo(x0,y);c2.lineTo(x1,y);c2.stroke();
        c2.fillStyle=color;c2.font='9px monospace';
        c2.fillText(lbl+' '+val.toFixed(2),x1-90,y-2);c2.restore();
      }}
      (f.resistencias||[]).forEach(function(r){{dl(r,'rgba(248,81,73,0.7)','R');}});
      (f.soportes||[]).forEach(function(s){{dl(s,'rgba(74,222,128,0.7)','S');}});
      dl(f.entrada,'rgba(91,163,255,0.9)','ENTRADA');
      dl(f.stop,'rgba(251,146,60,0.8)','STOP');
      dl(f.target,'rgba(188,140,255,0.8)','TARGET');
    }}}};
    opChartInst=new Chart(ctx,{{
      type:'line',
      data:{{labels:labels,datasets:datasets}},
      options:{{
        responsive:true,maintainAspectRatio:false,
        plugins:{{legend:{{display:true,labels:{{color:'#666',font:{{size:10}},boxWidth:16}}}},tooltip:{{mode:'index',intersect:false}}}},
        scales:{{
          x:{{ticks:{{color:'#666',font:{{size:9}},maxTicksLimit:10}},grid:{{color:'rgba(255,255,255,.04)'}}}},
          y:{{ticks:{{color:'#666',font:{{size:9}}}},grid:{{color:'rgba(255,255,255,.04)'}}}},
        }},
      }},
      plugins:[lp],
    }});
  }},80);
}}
</script>
</body>
</html>"""
 
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path
 
 
# ─────────────────────────────────────────────
# Excel de fichas
# ─────────────────────────────────────────────
 
def generate_excel(signals: list[dict], index_stats: dict, output_path: str) -> str:
    try:
        import openpyxl
        from openpyxl.styles import PatternFill, Font, Alignment
    except ImportError:
        logger.warning("openpyxl no disponible, saltando Excel")
        return ""
 
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
 
    HDR_FILL  = PatternFill("solid", fgColor="1e2a3a")
    BUY_FILL  = PatternFill("solid", fgColor="0d2b1a")
    SELL_FILL = PatternFill("solid", fgColor="2b1010")
    NEU_FILL  = PatternFill("solid", fgColor="1a1a2e")
    HDR_FONT  = Font(bold=True, color="5ba3ff", size=11)
    WHITE     = Font(color="e8e8ea", size=10)
    GREEN     = Font(color="4ade80", bold=True, size=10)
    RED       = Font(color="f87171", bold=True, size=10)
    ORANGE    = Font(color="fb923c", bold=True, size=10)
 
    headers = ["Ticker","Empresa","Sector","Precio","Sem%","Mes%","Anual%",
               "RSI","Macro","Técnico","Score","Señal","MA>50","Máx 12m","Mín 12m"]
 
    def _write_sheet(ws_name, market):
        ws = wb.create_sheet(ws_name)
        ws.freeze_panes = "A2"
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.fill = HDR_FILL; cell.font = HDR_FONT
            cell.alignment = Alignment(horizontal="center")
        rows = [s for s in signals if s["mercado"] == market]
        for r, s in enumerate(rows, 2):
            row_data = [s["ticker"],s["empresa"],s["sector"],s["precio_actual"],
                        s["ret_sem"],s["ret_mes"],s["ret_anual"],s["rsi"],
                        s["score_macro"],s["score_tecnico"],s["score_final"],s["signal"],
                        "Sí" if s.get("ma_cross") else "No",s["max_12m"],s["min_12m"]]
            sig  = s["signal"]
            fill = BUY_FILL if "COMPRA" in sig else SELL_FILL if "VENTA" in sig else NEU_FILL
            for col, val in enumerate(row_data, 1):
                cell = ws.cell(row=r, column=col, value=val)
                cell.fill = fill; cell.font = WHITE
                cell.alignment = Alignment(horizontal="center" if col > 3 else "left")
                if col in (5,6,7) and isinstance(val,(int,float)):
                    cell.font = GREEN if val >= 0 else RED
                if col == 12:
                    if "COMPRA" in str(val): cell.font = GREEN
                    elif "VENTA" in str(val): cell.font = ORANGE
        widths = [12,28,14,14,8,8,8,7,8,8,8,22,7,14,14]
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[chr(64+i)].width = w
 
    _write_sheet("MERVAL","MERVAL")
    _write_sheet("BOVESPA","BOVESPA")
    _write_sheet("SP500","SP500")
 
    ws_rank = wb.create_sheet("Ranking Global")
    ws_rank.freeze_panes = "A2"
    headers_ext = ["Mercado"] + headers
    from openpyxl.styles import Font as F2, Alignment as A2
    for col, h in enumerate(headers_ext, 1):
        cell = ws_rank.cell(row=1, column=col, value=h)
        cell.fill = HDR_FILL; cell.font = HDR_FONT
        cell.alignment = A2(horizontal="center")
    top = sorted(signals, key=lambda x: x["score_final"], reverse=True)[:30]
    for r, s in enumerate(top, 2):
        sig  = s["signal"]
        fill = BUY_FILL if "COMPRA" in sig else SELL_FILL if "VENTA" in sig else NEU_FILL
        vals = [s["mercado"],s["ticker"],s["empresa"],s["sector"],
                s["precio_actual"],s["ret_sem"],s["ret_mes"],s["ret_anual"],
                s["rsi"],s["score_macro"],s["score_tecnico"],s["score_final"],
                s["signal"],"Sí" if s.get("ma_cross") else "No",s["max_12m"],s["min_12m"]]
        for col, val in enumerate(vals, 1):
            cell = ws_rank.cell(row=r, column=col, value=val)
            cell.fill = fill; cell.font = WHITE
            cell.alignment = A2(horizontal="center" if col > 4 else "left")
            if col in (6,7,8) and isinstance(val,(int,float)):
                cell.font = GREEN if val >= 0 else RED
 
    wb.save(output_path)
    logger.info(f"Excel guardado: {output_path}")
    return output_path
 
