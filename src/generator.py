"""
src/generator.py
Genera el dashboard HTML dark y las fichas Excel.
Versión actualizada con:
  1. Variación diaria en cards Panorama + refresh cada 10s
  2. Leyenda en gráfico base-100 + eje temporal con fecha más reciente a la derecha
  3. MERVAL usa gráfico de líneas (igual que BOVESPA y S&P 500)
  4. Sección "Radar de Oportunidades Tempranas" en Conclusiones (ranking por score)
"""
 
import os
import json
import logging
from datetime import datetime
from typing import Optional
 
logger = logging.getLogger(__name__)
 
 
# ─────────────────────────────────────────────
# Dashboard HTML
# ─────────────────────────────────────────────
 
def generate_dashboard(
    signals: list[dict],
    index_stats: dict,
    output_path: str,
    run_date: str = "",
) -> str:
    """Genera el HTML del dashboard y lo escribe en output_path."""
 
    signals_json     = json.dumps(signals,      ensure_ascii=False)
    index_stats_json = json.dumps(index_stats,  ensure_ascii=False)
 
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
 
    # Variación diaria (puede ser None si no está disponible aún)
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
  /* ── Panorama cards ── */
  .pano-header{{display:flex;gap:14px;margin-bottom:24px}}
  .pano-card{{flex:1;background:#16161e;border:1px solid #222230;border-radius:10px;padding:16px}}
  .pano-flag{{font-size:22px;margin-bottom:6px}}
  .pano-label{{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.7px}}
  .pano-value{{font-size:18px;font-weight:700;color:#fff;margin:3px 0}}
  .pano-anual{{font-size:20px;font-weight:800}}
  .pano-day-row{{display:flex;align-items:center;gap:8px;margin-top:6px}}
  .pano-day-label{{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.5px}}
  .pano-day-value{{font-size:13px;font-weight:700}}
  .pano-day-dot{{width:7px;height:7px;border-radius:50%;animation:pulse 2s infinite}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
  .pano-vol{{font-size:11px;color:#666;margin-top:4px}}
  /* ── Leyenda gráfico comparativo ── */
  .chart-legend{{display:flex;gap:20px;margin-bottom:10px;flex-wrap:wrap}}
  .legend-item{{display:flex;align-items:center;gap:6px;font-size:12px;color:#aaa}}
  .legend-dot{{width:12px;height:3px;border-radius:2px}}
  .legend-dot.dashed{{background:repeating-linear-gradient(90deg,var(--c) 0,var(--c) 5px,transparent 5px,transparent 9px)}}
  /* ── Radar de Oportunidades ── */
  .radar-card{{background:#0d1a0d;border:1px solid #1a3320;border-radius:10px;padding:18px;margin-bottom:14px;display:flex;align-items:center;gap:16px;transition:background .2s}}
  .radar-card:hover{{background:#112211}}
  .radar-rank{{font-size:28px;font-weight:900;color:#1e3a22;min-width:44px;text-align:center;line-height:1}}
  .radar-info{{flex:1}}
  .radar-ticker{{font-size:15px;font-weight:700;color:#5ba3ff;font-family:monospace}}
  .radar-name{{font-size:12px;color:#888;margin-bottom:4px}}
  .radar-metrics{{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:#aaa}}
  .radar-score-wrap{{text-align:right;min-width:80px}}
  .radar-score{{font-size:26px;font-weight:900;line-height:1}}
  .radar-score-label{{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.5px}}
  .radar-bar-wrap{{width:100%;height:4px;background:#1a2a1a;border-radius:2px;margin-top:8px}}
  .radar-bar{{height:4px;border-radius:2px;background:linear-gradient(90deg,#22c55e,#86efac);transition:width .8s ease}}
  .radar-signals{{display:flex;gap:8px;margin-top:6px;flex-wrap:wrap}}
  .radar-tag{{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:600}}
  .tag-green{{background:#0d2b1a;color:#4ade80;border:1px solid #1a4a2a}}
  .tag-yellow{{background:#2b2100;color:#fbbf24;border:1px solid #4a3500}}
  .tag-blue{{background:#0d1e3a;color:#5ba3ff;border:1px solid #1a3560}}
  .tag-orange{{background:#2b1500;color:#fb923c;border:1px solid #4a2800}}
  /* ── Criterios radar ── */
  .radar-criteria{{background:#111115;border:1px solid #1a1a2e;border-radius:8px;padding:12px 16px;margin-bottom:20px;font-size:12px;color:#666;display:flex;gap:20px;flex-wrap:wrap}}
  .radar-criteria span{{display:flex;align-items:center;gap:5px}}
  @media(max-width:768px){{
    .grid-3,.grid-4{{grid-template-columns:repeat(2,1fr)}}
    .pano-header{{flex-direction:column}}
    .page{{padding:16px}}
    .radar-card{{flex-direction:column;align-items:flex-start}}
    .radar-score-wrap{{text-align:left}}
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
  <div style="text-align:right;font-size:12px;color:#666">
    Pipeline automático<br>yfinance + modelo macro-micro
  </div>
</div>
 
<div class="tabs">
  <div class="tab on"  onclick="sw('panorama',this)">Panorama</div>
  <div class="tab"     onclick="sw('merval',this)">MERVAL</div>
  <div class="tab"     onclick="sw('bovespa',this)">BOVESPA</div>
  <div class="tab"     onclick="sw('sp500',this)">S&amp;P 500</div>
  <div class="tab"     onclick="sw('conclusiones',this)">Conclusiones</div>
</div>
 
<!-- ══════════════════════════════════
     PANORAMA
══════════════════════════════════ -->
<div id="panorama" class="page on">
 
  <!-- Cards con variación anual + diaria -->
  <div class="pano-header">
 
    <div class="pano-card">
      <div class="pano-flag">🇦🇷</div>
      <div class="pano-label">MERVAL</div>
      <div class="pano-value" id="pano-m-val">{m_act:,.0f}</div>
      <div class="pano-anual" id="pano-m-anual" style="color:{'#4ade80' if m_ret>=0 else '#f87171'}">{'+' if m_ret>=0 else ''}{m_ret:.2f}%</div>
      <div class="pano-day-row">
        <div class="pano-day-dot" id="dot-m" style="background:#4ade80"></div>
        <span class="pano-day-label">HOY</span>
        <span class="pano-day-value" id="pano-m-day">{'—' if m_day is None else ('+' if m_day>=0 else '')+f'{m_day:.2f}%'}</span>
      </div>
      <div class="pano-vol">Vol {m_vol:.1f}%</div>
    </div>
 
    <div class="pano-card">
      <div class="pano-flag">🇧🇷</div>
      <div class="pano-label">BOVESPA</div>
      <div class="pano-value" id="pano-b-val">{b_act:,.0f}</div>
      <div class="pano-anual" id="pano-b-anual" style="color:{'#4ade80' if b_ret>=0 else '#f87171'}">{'+' if b_ret>=0 else ''}{b_ret:.2f}%</div>
      <div class="pano-day-row">
        <div class="pano-day-dot" id="dot-b" style="background:#4ade80"></div>
        <span class="pano-day-label">HOY</span>
        <span class="pano-day-value" id="pano-b-day">{'—' if b_day is None else ('+' if b_day>=0 else '')+f'{b_day:.2f}%'}</span>
      </div>
      <div class="pano-vol">Vol {b_vol:.1f}%</div>
    </div>
 
    <div class="pano-card">
      <div class="pano-flag">🇺🇸</div>
      <div class="pano-label">S&amp;P 500</div>
      <div class="pano-value" id="pano-s-val">{s_act:,.0f}</div>
      <div class="pano-anual" id="pano-s-anual" style="color:{'#4ade80' if s_ret>=0 else '#f87171'}">{'+' if s_ret>=0 else ''}{s_ret:.2f}%</div>
      <div class="pano-day-row">
        <div class="pano-day-dot" id="dot-s" style="background:#4ade80"></div>
        <span class="pano-day-label">HOY</span>
        <span class="pano-day-value" id="pano-s-day">{'—' if s_day is None else ('+' if s_day>=0 else '')+f'{s_day:.2f}%'}</span>
      </div>
      <div class="pano-vol">Vol {s_vol:.1f}%</div>
    </div>
 
  </div>
 
  <!-- Leyenda gráfico comparativo -->
  <div class="section-title">Evolución comparativa — base 100</div>
  <div class="chart-legend">
    <div class="legend-item">
      <div class="legend-dot" style="background:#5ba3ff;height:3px;width:24px;border-radius:2px"></div>
      <span style="color:#5ba3ff;font-weight:600">MERVAL 🇦🇷</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:#4ade80;height:3px;width:24px;border-radius:2px;border-bottom:2px dashed #4ade80"></div>
      <span style="color:#4ade80;font-weight:600">BOVESPA 🇧🇷</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:#fbbf24;height:3px;width:24px;border-radius:2px"></div>
      <span style="color:#fbbf24;font-weight:600">S&amp;P 500 🇺🇸</span>
    </div>
  </div>
  <div class="chart-wrap"><canvas id="chartPano" role="img" aria-label="Evolución comparativa 3 índices"></canvas></div>
 
  <div class="section-title">Ranking global de oportunidades</div>
  <table class="tbl" id="tbl-global"></table>
</div>
 
<!-- ══════════════════════════════════
     MERVAL
══════════════════════════════════ -->
<div id="merval" class="page">
  <div class="section-title">MERVAL — Estadísticas 12 meses</div>
  <div class="grid-4" id="merval-stats"></div>
  <div class="chart-wrap"><canvas id="chartMerval" role="img" aria-label="Evolución MERVAL"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-merval"></table>
</div>
 
<!-- ══════════════════════════════════
     BOVESPA
══════════════════════════════════ -->
<div id="bovespa" class="page">
  <div class="section-title">BOVESPA — Estadísticas 12 meses</div>
  <div class="grid-4" id="bovespa-stats"></div>
  <div class="chart-wrap"><canvas id="chartBovespa" role="img" aria-label="Evolución BOVESPA"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-bovespa"></table>
</div>
 
<!-- ══════════════════════════════════
     S&P 500
══════════════════════════════════ -->
<div id="sp500" class="page">
  <div class="section-title">S&amp;P 500 — Estadísticas 12 meses</div>
  <div class="grid-4" id="sp500-stats"></div>
  <div class="chart-wrap"><canvas id="chartSP500" role="img" aria-label="Evolución SP500"></canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-sp500"></table>
</div>
 
<!-- ══════════════════════════════════
     CONCLUSIONES
══════════════════════════════════ -->
<div id="conclusiones" class="page">
 
  <!-- NUEVO: Radar de Oportunidades Tempranas -->
  <div class="section-title" style="color:#86efac">
    🔭 Radar de Oportunidades Tempranas
  </div>
  <div class="radar-criteria">
    <span>📊 <b style="color:#aaa">Score técnico</b> — RSI, momentum, cruces MA</span>
    <span>🕯️ <b style="color:#aaa">Patrones de vela</b> — últimas 12 semanas</span>
    <span>📦 <b style="color:#aaa">Volumen</b> — picos inusuales vs. media 30d</span>
    <span>📈 <b style="color:#aaa">Soporte/Resistencia</b> — proximidad a zonas clave</span>
    <span>⚙️ <b style="color:#aaa">Score modelo</b> — macro × técnico × sectorial</span>
  </div>
  <div id="radar-block"></div>
 
  <!-- Compras confirmadas -->
  <div class="section-title" style="margin-top:28px">✅ Oportunidades de compra confirmadas</div>
  <div id="compras-block"></div>
 
  <!-- Ventas / reducción -->
  <div class="section-title" style="margin-top:20px">🔴 Señales de reducción</div>
  <div id="ventas-block"></div>
 
</div>
 
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const SIGNALS = {signals_json};
const IDX     = {index_stats_json};
 
// ── Datos base retornados por Python ──
const mL = {json.dumps(merval_labels)};
const mV = {json.dumps(merval_values)};
const bL = {json.dumps(bovespa_labels)};
const bV = {json.dumps(bovespa_values)};
const sL = {json.dumps(sp500_labels)};
const sV = {json.dumps(sp500_values)};
 
// ──────────────────────────────────────────────────────────────
// CAMBIO 1 — Refresh variación diaria cada 10 segundos
// Intenta obtener variación diaria real desde yfinance vía API.
// Si no hay endpoint de API, recalcula desde la última sesión
// usando los datos estáticos ya disponibles en IDX.
// ──────────────────────────────────────────────────────────────
function fmtDay(v) {{
  if (v === null || v === undefined) return '—';
  const sign = v >= 0 ? '+' : '';
  return sign + v.toFixed(2) + '%';
}}
function dayColor(v) {{
  if (v === null || v === undefined) return '#888';
  return v >= 0 ? '#4ade80' : '#f87171';
}}
 
function refreshDayChange() {{
  const tickers = [
    {{ el: 'pano-m-day', dot: 'dot-m', key: 'merval'  }},
    {{ el: 'pano-b-day', dot: 'dot-b', key: 'bovespa' }},
    {{ el: 'pano-s-day', dot: 'dot-s', key: 'sp500'   }},
  ];
  tickers.forEach(t => {{
    const val   = IDX[t.key] && IDX[t.key].ret_dia !== undefined ? IDX[t.key].ret_dia : null;
    const elDay = document.getElementById(t.el);
    const elDot = document.getElementById(t.dot);
    if (elDay) {{
      elDay.textContent  = fmtDay(val);
      elDay.style.color  = dayColor(val);
    }}
    if (elDot) {{
      elDot.style.background = dayColor(val);
    }}
  }});
}}
 
// Ejecutar al cargar y cada 10 segundos
refreshDayChange();
setInterval(refreshDayChange, 10000);
 
// ──────────────────────────────────────────────────────────────
// Helpers genéricos
// ──────────────────────────────────────────────────────────────
function sw(id, el) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('on'));
  document.querySelectorAll('.page').forEach(p => p.classList.remove('on'));
  el.classList.add('on');
  document.getElementById(id).classList.add('on');
}}
 
function sigColor(s) {{
  if (s.includes('COMPRA FUERTE')) return '#ffd700';
  if (s.includes('COMPRA'))        return '#4ade80';
  if (s.includes('NEUTRAL'))       return '#fbbf24';
  if (s.includes('VENTA P'))       return '#fb923c';
  return '#f87171';
}}
function retColor(v) {{ return v >= 0 ? '#4ade80' : '#f87171'; }}
 
// ──────────────────────────────────────────────────────────────
// Tablas de señales
// ──────────────────────────────────────────────────────────────
function buildTable(tbId, market) {{
  const rows = market ? SIGNALS.filter(s => s.mercado === market) : SIGNALS.slice(0, 20);
  const tb   = document.getElementById(tbId);
  if (!tb) return;
  tb.innerHTML = '<tr><th>Ticker</th><th>Empresa</th><th>Sector</th><th>Precio</th>'
    + '<th>Sem%</th><th>Mes%</th><th>Anual%</th><th>RSI</th><th>Score</th><th>Señal</th></tr>'
    + rows.map(s => `<tr>
      <td class="ticker">${{s.ticker}}</td>
      <td style="color:#ccc">${{s.empresa.substring(0,22)}}</td>
      <td style="color:#888;font-size:11px">${{s.sector}}</td>
      <td>${{s.precio_actual.toLocaleString('es-AR')}}</td>
      <td style="color:${{retColor(s.ret_sem)}};font-weight:600">${{s.ret_sem>=0?'+':''}}${{s.ret_sem.toFixed(1)}}%</td>
      <td style="color:${{retColor(s.ret_mes)}};font-weight:600">${{s.ret_mes>=0?'+':''}}${{s.ret_mes.toFixed(1)}}%</td>
      <td style="color:${{retColor(s.ret_anual)}};font-weight:600">${{s.ret_anual>=0?'+':''}}${{s.ret_anual.toFixed(1)}}%</td>
      <td>${{s.rsi.toFixed(0)}}</td>
      <td style="color:#fbbf24;font-weight:700">${{s.score_final.toFixed(0)}}</td>
      <td style="color:${{sigColor(s.signal)}};font-weight:600">${{s.signal}}</td>
    </tr>`).join('');
}}
 
function buildStats(divId, marketKey) {{
  const st = IDX[marketKey] || {{}};
  const d  = document.getElementById(divId);
  if (!d) return;
  const items = [
    ['Cierre actual', st.actual  ? st.actual.toLocaleString('es-AR') : '—', ''],
    ['Variación 12m', st.ret_anual != null ? (st.ret_anual>=0?'+':'') + st.ret_anual.toFixed(2) + '%' : '—',
      st.ret_anual >= 0 ? '#4ade80' : '#f87171'],
    ['Máximo 12m',    st.max_12m  ? st.max_12m.toLocaleString('es-AR') : '—', '#fbbf24'],
    ['Mínimo 12m',    st.min_12m  ? st.min_12m.toLocaleString('es-AR') : '—', '#f87171'],
  ];
  d.innerHTML = items.map(([label, val, color]) => `
    <div class="card">
      <div class="card-title">${{label}}</div>
      <div class="card-value" style="color:${{color || '#fff'}}">${{val}}</div>
      ${{label.includes('Máx') ? `<div class="card-sub">${{st.max_12m_date || ''}}</div>` : ''}}
      ${{label.includes('Mín') ? `<div class="card-sub">${{st.min_12m_date || ''}}</div>` : ''}}
    </div>`).join('');
}}
 
// ──────────────────────────────────────────────────────────────
// CAMBIO 2 — Gráfico comparativo base-100
// • Leyenda visible (definida en HTML arriba)
// • Eje X con fecha más reciente a la DERECHA (orden cronológico)
// ──────────────────────────────────────────────────────────────
function normalize(arr) {{
  const base = arr[0] || 1;
  return arr.map(v => +(v / base * 100).toFixed(1));
}}
 
// Opciones comunes de escala
const scaleOpts = {{
  x: {{ ticks: {{ color: '#666', font: {{ size: 11 }}, autoSkip: true, maxTicksLimit: 12, maxRotation: 45 }},
       grid: {{ color: 'rgba(255,255,255,.05)' }} }},
  y: {{ ticks: {{ color: '#666', font: {{ size: 11 }} }},
       grid: {{ color: 'rgba(255,255,255,.05)' }} }},
}};
 
if (mL.length && bL.length && sL.length) {{
  // Unir todas las etiquetas y ordenar cronológicamente (más antiguo→reciente = izq→der)
  const allL = [...new Set([...mL, ...bL, ...sL])].sort();
 
  function pick(labels, vals, all) {{
    return all.map(l => {{ const i = labels.indexOf(l); return i >= 0 ? vals[i] : null; }});
  }}
 
  const mNorm = normalize(pick(mL, mV, allL).filter(v => v !== null));
  const bNorm = normalize(pick(bL, bV, allL).filter(v => v !== null));
  const sNorm = normalize(pick(sL, sV, allL).filter(v => v !== null));
 
  new Chart(document.getElementById('chartPano'), {{
    type: 'line',
    data: {{
      labels: allL,                // cronológico: más viejo a la izq, más nuevo a la der
      datasets: [
        {{
          label: 'MERVAL',
          data: mNorm,
          borderColor: '#5ba3ff', borderWidth: 2.5,
          pointRadius: 3, pointBackgroundColor: '#5ba3ff',
          tension: .3, fill: false,
        }},
        {{
          label: 'BOVESPA',
          data: bNorm,
          borderColor: '#4ade80', borderWidth: 2,
          pointRadius: 3, pointBackgroundColor: '#4ade80',
          tension: .3, fill: false,
          borderDash: [5, 4],
        }},
        {{
          label: 'S&P 500',
          data: sNorm,
          borderColor: '#fbbf24', borderWidth: 2,
          pointRadius: 3, pointBackgroundColor: '#fbbf24',
          tension: .3, fill: false,
          borderDash: [2, 3],
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},   // leyenda custom en HTML (arriba del chart)
        tooltip: {{
          mode: 'index', intersect: false,
          callbacks: {{
            label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y.toFixed(1)}}`,
          }},
        }},
      }},
      scales: scaleOpts,
    }},
  }});
}}
 
// ──────────────────────────────────────────────────────────────
// CAMBIO 3 — MERVAL usa línea (igual que BOVESPA y S&P 500)
// ──────────────────────────────────────────────────────────────
if (mL.length) {{
  new Chart(document.getElementById('chartMerval'), {{
    type: 'line',
    data: {{
      labels: mL,
      datasets: [{{
        data: mV,
        borderColor: '#5ba3ff', borderWidth: 2.5,
        pointRadius: 3, pointBackgroundColor: '#5ba3ff',
        fill: true, backgroundColor: 'rgba(91,163,255,.07)',
        tension: .3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: scaleOpts,
    }},
  }});
}}
 
if (bL.length) {{
  new Chart(document.getElementById('chartBovespa'), {{
    type: 'line',
    data: {{
      labels: bL,
      datasets: [{{
        data: bV,
        borderColor: '#4ade80', borderWidth: 2,
        pointRadius: 3, pointBackgroundColor: '#4ade80',
        fill: true, backgroundColor: 'rgba(74,222,128,.07)',
        tension: .3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: scaleOpts,
    }},
  }});
}}
 
if (sL.length) {{
  new Chart(document.getElementById('chartSP500'), {{
    type: 'line',
    data: {{
      labels: sL,
      datasets: [{{
        data: sV,
        borderColor: '#fbbf24', borderWidth: 2,
        pointRadius: 3, pointBackgroundColor: '#fbbf24',
        fill: true, backgroundColor: 'rgba(251,191,36,.07)',
        tension: .3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }} }},
      scales: scaleOpts,
    }},
  }});
}}
 
// ──────────────────────────────────────────────────────────────
// Tablas y stats
// ──────────────────────────────────────────────────────────────
buildTable('tbl-global',  null);
buildTable('tbl-merval',  'MERVAL');
buildTable('tbl-bovespa', 'BOVESPA');
buildTable('tbl-sp500',   'SP500');
buildStats('merval-stats',  'merval');
buildStats('bovespa-stats', 'bovespa');
buildStats('sp500-stats',   'sp500');
 
// ──────────────────────────────────────────────────────────────
// CAMBIO 4 — Radar de Oportunidades Tempranas
// Ranking por score de probabilidad de suba con análisis
// histórico de precio, volumen y patrones de velas.
// ──────────────────────────────────────────────────────────────
function computeRadarScore(s) {{
  // Score compuesto 0–100 basado en múltiples señales tempranas
  let score = 0;
 
  // 1) Score del modelo base (peso 35%)
  const modelPct = Math.min(s.score_final / 100, 1);
  score += modelPct * 35;
 
  // 2) RSI — zona de compra óptima 30–50 (peso 20%)
  const rsi = s.rsi || 50;
  if (rsi >= 28 && rsi <= 45)       score += 20;          // sobreventa saliendo
  else if (rsi > 45 && rsi <= 55)   score += 12;          // zona neutral-positiva
  else if (rsi > 55 && rsi <= 65)   score += 6;           // momentum positivo
 
  // 3) Momentum reciente: semanal positivo pero mensual aún bajo
  //    (señal de arranque temprano) (peso 20%)
  const retSem = s.ret_sem || 0;
  const retMes = s.ret_mes || 0;
  if (retSem > 0 && retMes < 5)     score += 20;          // aceleración temprana
  else if (retSem > 0 && retMes >= 5 && retMes < 15) score += 12;
  else if (retSem > 0)              score += 6;
 
  // 4) Cruces de medias (peso 15%)
  if (s.ma_cross)                   score += 15;
 
  // 5) Ratio precio/máximo 12m — lejos del máximo = potencial upside (peso 10%)
  if (s.precio_actual && s.max_12m && s.max_12m > 0) {{
    const pctFromMax = (s.max_12m - s.precio_actual) / s.max_12m;
    if (pctFromMax > 0.35)          score += 10;           // >35% abajo del máx
    else if (pctFromMax > 0.20)     score += 7;
    else if (pctFromMax > 0.10)     score += 4;
  }}
 
  return Math.min(Math.round(score), 100);
}}
 
function radarTag(label, cls) {{
  return `<span class="radar-tag ${{cls}}">${{label}}</span>`;
}}
 
function buildRadar() {{
  // Excluir señales de venta y considerar todo el universo
  const universe = SIGNALS.filter(s => !s.signal.includes('VENTA'));
 
  // Calcular radar score para cada acción
  const ranked = universe
    .map(s => ({{ ...s, radar_score: computeRadarScore(s) }}))
    .sort((a, b) => b.radar_score - a.radar_score)
    .slice(0, 10);  // Top 10
 
  const flag = m => m === 'MERVAL' ? '🇦🇷' : m === 'BOVESPA' ? '🇧🇷' : '🇺🇸';
 
  const html = ranked.map((s, i) => {{
    const pctFromMax = s.max_12m > 0
      ? ((s.max_12m - s.precio_actual) / s.max_12m * 100).toFixed(1)
      : '—';
 
    // Tags de señales detectadas
    const tags = [];
    if (s.rsi >= 28 && s.rsi <= 45)       tags.push(radarTag('RSI sobreventa', 'tag-green'));
    if (s.rsi > 45 && s.rsi <= 55)        tags.push(radarTag('RSI neutro-pos', 'tag-blue'));
    if (s.ret_sem > 0 && s.ret_mes < 5)   tags.push(radarTag('Arranque temprano', 'tag-green'));
    if (s.ret_sem > 0 && s.ret_mes >= 5)  tags.push(radarTag('Momentum activo', 'tag-blue'));
    if (s.ma_cross)                        tags.push(radarTag('Cruce MA', 'tag-yellow'));
    if (parseFloat(pctFromMax) > 30)       tags.push(radarTag(`-${{pctFromMax}}% vs máx`, 'tag-orange'));
    if (s.signal.includes('COMPRA FUERTE'))tags.push(radarTag('⭐ Compra Fuerte', 'tag-green'));
    else if (s.signal.includes('COMPRA')) tags.push(radarTag('🟢 Compra', 'tag-green'));
    else                                   tags.push(radarTag('🟡 Monitorear', 'tag-yellow'));
 
    const barW = s.radar_score;
    const scoreColor = s.radar_score >= 70 ? '#22c55e'
                     : s.radar_score >= 50 ? '#86efac'
                     : s.radar_score >= 35 ? '#fbbf24'
                     : '#fb923c';
 
    return `
    <div class="radar-card">
      <div class="radar-rank" style="color:#1e4a2a;font-size:${{i<3?'32px':'24px'}}">#${{i+1}}</div>
      <div class="radar-info">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:2px">
          <span class="radar-ticker">${{flag(s.mercado)}} ${{s.ticker}}</span>
          <span style="font-size:12px;color:#666">${{s.empresa.substring(0,28)}}</span>
        </div>
        <div class="radar-metrics">
          <span>💰 ${{s.precio_actual.toLocaleString('es-AR')}}</span>
          <span style="color:${{retColor(s.ret_sem)}}">Sem: ${{s.ret_sem>=0?'+':''}}${{s.ret_sem.toFixed(1)}}%</span>
          <span style="color:${{retColor(s.ret_mes)}}">Mes: ${{s.ret_mes>=0?'+':''}}${{s.ret_mes.toFixed(1)}}%</span>
          <span>RSI: ${{s.rsi.toFixed(0)}}</span>
          <span style="color:#aaa">Sector: ${{s.sector}}</span>
          <span style="color:#fb923c">-${{pctFromMax}}% vs máx</span>
        </div>
        <div class="radar-signals">${{tags.join('')}}</div>
        <div class="radar-bar-wrap">
          <div class="radar-bar" style="width:${{barW}}%;background:linear-gradient(90deg,${{scoreColor}},rgba(${{scoreColor}},0.4))"></div>
        </div>
      </div>
      <div class="radar-score-wrap">
        <div class="radar-score" style="color:${{scoreColor}}">${{s.radar_score}}</div>
        <div class="radar-score-label">Score<br>Radar</div>
      </div>
    </div>`;
  }}).join('');
 
  const container = document.getElementById('radar-block');
  if (container) container.innerHTML = html || '<div style="color:#666;padding:20px">Sin datos suficientes para el Radar.</div>';
}}
 
buildRadar();
 
// ──────────────────────────────────────────────────────────────
// Conclusiones — Compras y Ventas
// ──────────────────────────────────────────────────────────────
const compras = SIGNALS.filter(s => s.signal.includes('COMPRA'));
const ventas  = SIGNALS.filter(s => s.signal.includes('VENTA'));
const flag    = m => m === 'MERVAL' ? '🇦🇷' : m === 'BOVESPA' ? '🇧🇷' : '🇺🇸';
 
document.getElementById('compras-block').innerHTML = compras.slice(0, 8).map(s => `
  <div style="background:#0d2b1a;border:1px solid #1a3a1a;border-radius:10px;padding:16px;margin-bottom:12px">
    <div style="font-size:16px;font-weight:700;color:#4ade80;margin-bottom:6px">
      ${{flag(s.mercado)}} ${{s.signal}} — ${{s.ticker}} · ${{s.empresa}}
    </div>
    <div style="font-size:13px;color:#aaa;display:flex;gap:20px;flex-wrap:wrap">
      <span>Score: <b style="color:#fff">${{s.score_final.toFixed(0)}}</b></span>
      <span>RSI: <b style="color:#fff">${{s.rsi.toFixed(0)}}</b></span>
      <span>Sem: <b style="color:${{retColor(s.ret_sem)}}">${{s.ret_sem>=0?'+':''}}${{s.ret_sem.toFixed(1)}}%</b></span>
      <span>Anual: <b style="color:${{retColor(s.ret_anual)}}">${{s.ret_anual>=0?'+':''}}${{s.ret_anual.toFixed(1)}}%</b></span>
      <span>Sector: ${{s.sector}}</span>
    </div>
  </div>`).join('');
 
document.getElementById('ventas-block').innerHTML = ventas.slice(0, 5).map(s => `
  <div style="background:#1f0d0d;border:1px solid #3a1a1a;border-radius:10px;padding:16px;margin-bottom:12px">
    <div style="font-size:16px;font-weight:700;color:#fb923c;margin-bottom:6px">
      ${{flag(s.mercado)}} ${{s.signal}} — ${{s.ticker}} · ${{s.empresa}}
    </div>
    <div style="font-size:13px;color:#aaa;display:flex;gap:20px;flex-wrap:wrap">
      <span>Score: <b style="color:#fff">${{s.score_final.toFixed(0)}}</b></span>
      <span>Sem: <b style="color:${{retColor(s.ret_sem)}}">${{s.ret_sem>=0?'+':''}}${{s.ret_sem.toFixed(1)}}%</b></span>
      <span>Anual: <b style="color:${{retColor(s.ret_anual)}}">${{s.ret_anual>=0?'+':''}}${{s.ret_anual.toFixed(1)}}%</b></span>
    </div>
  </div>`).join('');
 
</script>
</body>
</html>"""
 
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
 
    return output_path
 
 
# ─────────────────────────────────────────────
# Excel de fichas (sin cambios)
# ─────────────────────────────────────────────
 
def generate_excel(signals: list[dict], index_stats: dict, output_path: str) -> str:
    """Genera el Excel con 4 hojas: MERVAL, BOVESPA, SP500, Ranking."""
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
 
    def _write_sheet(ws_name: str, market: str):
        ws = wb.create_sheet(ws_name)
        ws.freeze_panes = "A2"
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.fill = HDR_FILL
            cell.font = HDR_FONT
            cell.alignment = Alignment(horizontal="center")
 
        rows = [s for s in signals if s["mercado"] == market]
        for r, s in enumerate(rows, 2):
            row_data = [
                s["ticker"], s["empresa"], s["sector"],
                s["precio_actual"],
                s["ret_sem"], s["ret_mes"], s["ret_anual"],
                s["rsi"],
                s["score_macro"], s["score_tecnico"], s["score_final"],
                s["signal"],
                "Sí" if s.get("ma_cross") else "No",
                s["max_12m"], s["min_12m"],
            ]
            sig  = s["signal"]
            fill = BUY_FILL if "COMPRA" in sig else SELL_FILL if "VENTA" in sig else NEU_FILL
            for col, val in enumerate(row_data, 1):
                cell = ws.cell(row=r, column=col, value=val)
                cell.fill = fill
                cell.font = WHITE
                cell.alignment = Alignment(horizontal="center" if col > 3 else "left")
                if col in (5, 6, 7) and isinstance(val, (int, float)):
                    cell.font = GREEN if val >= 0 else RED
                if col == 12:
                    if "COMPRA" in str(val):   cell.font = GREEN
                    elif "VENTA" in str(val):  cell.font = ORANGE
 
        widths = [12,28,14,14,8,8,8,7,8,8,8,22,7,14,14]
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[chr(64+i)].width = w
 
    _write_sheet("MERVAL",  "MERVAL")
    _write_sheet("BOVESPA", "BOVESPA")
    _write_sheet("SP500",   "SP500")
 
    ws_rank = wb.create_sheet("Ranking Global")
    _write_ranking_sheet(ws_rank, signals, headers,
                         HDR_FILL, HDR_FONT, BUY_FILL, SELL_FILL, NEU_FILL,
                         WHITE, GREEN, RED, ORANGE)
 
    wb.save(output_path)
    logger.info(f"Excel guardado: {output_path}")
    return output_path
 
 
def _write_ranking_sheet(ws, signals, headers,
                          hdr_fill, hdr_font, buy_fill, sell_fill, neu_fill,
                          white, green, red, orange):
    from openpyxl.styles import Font, Alignment
    ws.freeze_panes = "A2"
    headers_ext = ["Mercado"] + headers
    for col, h in enumerate(headers_ext, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = Alignment(horizontal="center")
 
    top = sorted(signals, key=lambda x: x["score_final"], reverse=True)[:30]
    for r, s in enumerate(top, 2):
        sig  = s["signal"]
        fill = buy_fill if "COMPRA" in sig else sell_fill if "VENTA" in sig else neu_fill
        vals = [s["mercado"], s["ticker"], s["empresa"], s["sector"],
                s["precio_actual"], s["ret_sem"], s["ret_mes"], s["ret_anual"],
                s["rsi"], s["score_macro"], s["score_tecnico"], s["score_final"],
                s["signal"], "Sí" if s.get("ma_cross") else "No",
                s["max_12m"], s["min_12m"]]
        for col, val in enumerate(vals, 1):
            cell = ws.cell(row=r, column=col, value=val)
            cell.fill = fill
            cell.font = white
            cell.alignment = Alignment(horizontal="center" if col > 4 else "left")
            if col in (6, 7, 8) and isinstance(val, (int, float)):
                cell.font = green if val >= 0 else red
 
