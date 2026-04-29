"""
src/generator.py
Genera el dashboard HTML dark y las fichas Excel.
El HTML reutiliza la misma estructura visual del informe manual.
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

    # Serializar datos para el JS del dashboard
    signals_json     = json.dumps(signals,      ensure_ascii=False)
    index_stats_json = json.dumps(index_stats,  ensure_ascii=False)

    # Datos para los charts
    merval_labels = index_stats.get("merval", {}).get("monthly_labels", [])
    merval_values = index_stats.get("merval", {}).get("monthly_values", [])
    bovespa_labels = index_stats.get("bovespa", {}).get("monthly_labels", [])
    bovespa_values = index_stats.get("bovespa", {}).get("monthly_values", [])
    sp500_labels   = index_stats.get("sp500",   {}).get("monthly_labels", [])
    sp500_values   = index_stats.get("sp500",   {}).get("monthly_values", [])

    # Stats summary
    m_ret  = index_stats.get("merval",  {}).get("ret_anual",  0)
    b_ret  = index_stats.get("bovespa", {}).get("ret_anual",  0)
    s_ret  = index_stats.get("sp500",   {}).get("ret_anual",  0)
    m_act  = index_stats.get("merval",  {}).get("actual",     0)
    b_act  = index_stats.get("bovespa", {}).get("actual",     0)
    s_act  = index_stats.get("sp500",   {}).get("actual",     0)
    m_vol  = index_stats.get("merval",  {}).get("volatilidad", 0)
    b_vol  = index_stats.get("bovespa", {}).get("volatilidad", 0)
    s_vol  = index_stats.get("sp500",   {}).get("volatilidad", 0)

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
  .pano-card{{flex:1;background:#16161e;border:1px solid #222230;border-radius:10px;padding:16px}}
  .pano-header{{display:flex;gap:14px;margin-bottom:24px}}
  @media(max-width:768px){{.grid-3,.grid-4{{grid-template-columns:repeat(2,1fr)}}.pano-header{{flex-direction:column}}.page{{padding:16px}}}}
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
  <div class="tab on"    onclick="sw('panorama',this)">Panorama</div>
  <div class="tab"       onclick="sw('merval',this)">MERVAL</div>
  <div class="tab"       onclick="sw('bovespa',this)">BOVESPA</div>
  <div class="tab"       onclick="sw('sp500',this)">S&P 500</div>
  <div class="tab"       onclick="sw('conclusiones',this)">Conclusiones</div>
</div>

<!-- PANORAMA -->
<div id="panorama" class="page on">
  <div class="pano-header">
    <div class="pano-card">
      <div style="font-size:22px;margin-bottom:6px">🇦🇷</div>
      <div style="font-size:11px;color:#666;text-transform:uppercase">MERVAL</div>
      <div style="font-size:18px;font-weight:700;color:#fff;margin:3px 0">{m_act:,.0f}</div>
      <div style="font-size:20px;font-weight:800;color:{'#4ade80' if m_ret>=0 else '#f87171'}">{'+' if m_ret>=0 else ''}{m_ret:.2f}%</div>
      <div style="font-size:11px;color:#666;margin-top:5px">Vol {m_vol:.1f}%</div>
    </div>
    <div class="pano-card">
      <div style="font-size:22px;margin-bottom:6px">🇧🇷</div>
      <div style="font-size:11px;color:#666;text-transform:uppercase">BOVESPA</div>
      <div style="font-size:18px;font-weight:700;color:#fff;margin:3px 0">{b_act:,.0f}</div>
      <div style="font-size:20px;font-weight:800;color:{'#4ade80' if b_ret>=0 else '#f87171'}">{'+' if b_ret>=0 else ''}{b_ret:.2f}%</div>
      <div style="font-size:11px;color:#666;margin-top:5px">Vol {b_vol:.1f}%</div>
    </div>
    <div class="pano-card">
      <div style="font-size:22px;margin-bottom:6px">🇺🇸</div>
      <div style="font-size:11px;color:#666;text-transform:uppercase">S&P 500</div>
      <div style="font-size:18px;font-weight:700;color:#fff;margin:3px 0">{s_act:,.0f}</div>
      <div style="font-size:20px;font-weight:800;color:{'#4ade80' if s_ret>=0 else '#f87171'}">{'+' if s_ret>=0 else ''}{s_ret:.2f}%</div>
      <div style="font-size:11px;color:#666;margin-top:5px">Vol {s_vol:.1f}%</div>
    </div>
  </div>

  <div class="section-title">Evolución comparativa — base 100</div>
  <div class="chart-wrap"><canvas id="chartPano" role="img" aria-label="Evolución comparativa 3 índices">Comparativa MERVAL BOVESPA SP500</canvas></div>

  <div class="section-title">Ranking global de oportunidades</div>
  <table class="tbl" id="tbl-global"></table>
</div>

<!-- MERVAL -->
<div id="merval" class="page">
  <div class="section-title">MERVAL — Estadísticas 12 meses</div>
  <div class="grid-4" id="merval-stats"></div>
  <div class="chart-wrap"><canvas id="chartMerval" role="img" aria-label="Evolución MERVAL">MERVAL</canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-merval"></table>
</div>

<!-- BOVESPA -->
<div id="bovespa" class="page">
  <div class="section-title">BOVESPA — Estadísticas 12 meses</div>
  <div class="grid-4" id="bovespa-stats"></div>
  <div class="chart-wrap"><canvas id="chartBovespa" role="img" aria-label="Evolución BOVESPA">BOVESPA</canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-bovespa"></table>
</div>

<!-- SP500 -->
<div id="sp500" class="page">
  <div class="section-title">S&P 500 — Estadísticas 12 meses</div>
  <div class="grid-4" id="sp500-stats"></div>
  <div class="chart-wrap"><canvas id="chartSP500" role="img" aria-label="Evolución SP500">S&P 500</canvas></div>
  <div class="section-title">Señales del modelo</div>
  <table class="tbl" id="tbl-sp500"></table>
</div>

<!-- CONCLUSIONES -->
<div id="conclusiones" class="page">
  <div class="section-title">Oportunidades de compra</div>
  <div id="compras-block"></div>
  <div class="section-title" style="margin-top:20px">Señales de reducción</div>
  <div id="ventas-block"></div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const SIGNALS = {signals_json};
const IDX = {index_stats_json};

function sw(id,el){{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('on'));
  el.classList.add('on');
  document.getElementById(id).classList.add('on');
}}

function sigColor(s){{
  if(s.includes('COMPRA FUERTE')) return '#ffd700';
  if(s.includes('COMPRA'))  return '#4ade80';
  if(s.includes('NEUTRAL')) return '#fbbf24';
  if(s.includes('VENTA P')) return '#fb923c';
  return '#f87171';
}}

function retColor(v){{ return v>=0?'#4ade80':'#f87171'; }}

function buildTable(tbId, market){{
  const rows = market ? SIGNALS.filter(s=>s.mercado===market) : SIGNALS.slice(0,20);
  const tb = document.getElementById(tbId);
  tb.innerHTML = '<tr><th>Ticker</th><th>Empresa</th><th>Sector</th><th>Precio</th><th>Sem%</th><th>Mes%</th><th>Anual%</th><th>RSI</th><th>Score</th><th>Señal</th></tr>'
    + rows.map(s=>`<tr>
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

function buildStats(divId, marketKey){{
  const st = IDX[marketKey]||{{}};
  const d = document.getElementById(divId);
  const items = [
    ['Cierre actual', st.actual?st.actual.toLocaleString('es-AR'):'—', ''],
    ['Variación 12m', st.ret_anual!=null?(st.ret_anual>=0?'+':'')+st.ret_anual.toFixed(2)+'%':'—', st.ret_anual>=0?'#4ade80':'#f87171'],
    ['Máximo 12m',    st.max_12m?st.max_12m.toLocaleString('es-AR'):'—', '#fbbf24'],
    ['Mínimo 12m',    st.min_12m?st.min_12m.toLocaleString('es-AR'):'—', '#f87171'],
  ];
  d.innerHTML = items.map(([label,val,color])=>`
    <div class="card">
      <div class="card-title">${{label}}</div>
      <div class="card-value" style="color:${{color||'#fff'}}">${{val}}</div>
      ${{label.includes('Máx')?`<div class="card-sub">${{st.max_12m_date||''}}</div>`:''}}
      ${{label.includes('Mín')?`<div class="card-sub">${{st.min_12m_date||''}}</div>`:''}}
    </div>`).join('');
}}

function normalize(arr){{
  const base = arr[0]||1;
  return arr.map(v=>+(v/base*100).toFixed(1));
}}

const chartOpts = {{
  responsive:true, maintainAspectRatio:false,
  plugins:{{legend:{{display:false}}}},
  scales:{{
    x:{{ticks:{{color:'#666',font:{{size:11}},autoSkip:false,maxRotation:45}},grid:{{color:'rgba(255,255,255,.05)'}}}},
    y:{{ticks:{{color:'#666',font:{{size:11}}}},grid:{{color:'rgba(255,255,255,.05)'}}}},
  }}
}};

// Charts
const mL = {json.dumps(merval_labels)};
const mV = {json.dumps(merval_values)};
const bL = {json.dumps(bovespa_labels)};
const bV = {json.dumps(bovespa_values)};
const sL = {json.dumps(sp500_labels)};
const sV = {json.dumps(sp500_values)};

if(mL.length&&bL.length&&sL.length){{
  const allL = [...new Set([...mL,...bL,...sL])].sort();
  const pick = (labels,vals,all)=>all.map(l=>{{const i=labels.indexOf(l);return i>=0?vals[i]:null;}});
  new Chart(document.getElementById('chartPano'),{{
    type:'line',
    data:{{labels:allL,datasets:[
      {{label:'MERVAL', data:normalize(pick(mL,mV,allL).filter(v=>v!=null)), borderColor:'#5ba3ff',borderWidth:2,pointRadius:3,pointBackgroundColor:'#5ba3ff',tension:.3}},
      {{label:'BOVESPA',data:normalize(pick(bL,bV,allL).filter(v=>v!=null)), borderColor:'#4ade80',borderWidth:2,pointRadius:3,pointBackgroundColor:'#4ade80',tension:.3,borderDash:[4,4]}},
      {{label:'S&P 500',data:normalize(pick(sL,sV,allL).filter(v=>v!=null)), borderColor:'#fbbf24',borderWidth:2,pointRadius:3,pointBackgroundColor:'#fbbf24',tension:.3,borderDash:[2,2]}},
    ]}},
    options:{{...chartOpts,plugins:{{...chartOpts.plugins,legend:{{display:false}}}}}}
  }});
}}

if(mL.length) new Chart(document.getElementById('chartMerval'),{{type:'bar',data:{{labels:mL,datasets:[{{data:mV,backgroundColor:mV.map((v,i,a)=>i==0?'#444':v>a[i-1]?'rgba(74,222,128,.6)':'rgba(248,113,113,.6)'),borderWidth:1}}]}},options:chartOpts}});
if(bL.length) new Chart(document.getElementById('chartBovespa'),{{type:'line',data:{{labels:bL,datasets:[{{data:bV,borderColor:'#4ade80',borderWidth:2,pointRadius:3,fill:true,backgroundColor:'rgba(74,222,128,.08)',tension:.3}}]}},options:chartOpts}});
if(sL.length) new Chart(document.getElementById('chartSP500'),{{type:'line',data:{{labels:sL,datasets:[{{data:sV,borderColor:'#fbbf24',borderWidth:2,pointRadius:3,fill:true,backgroundColor:'rgba(251,191,36,.08)',tension:.3}}]}},options:chartOpts}});

// Tablas
buildTable('tbl-global', null);
buildTable('tbl-merval',  'MERVAL');
buildTable('tbl-bovespa', 'BOVESPA');
buildTable('tbl-sp500',   'SP500');

// Stats
buildStats('merval-stats',  'merval');
buildStats('bovespa-stats', 'bovespa');
buildStats('sp500-stats',   'sp500');

// Conclusiones
const compras = SIGNALS.filter(s=>s.signal.includes('COMPRA'));
const ventas  = SIGNALS.filter(s=>s.signal.includes('VENTA'));
const flag = m => m==='MERVAL'?'🇦🇷':m==='BOVESPA'?'🇧🇷':'🇺🇸';

document.getElementById('compras-block').innerHTML = compras.slice(0,8).map(s=>`
  <div style="background:#0d2b1a;border:1px solid #1a3a1a;border-radius:10px;padding:16px;margin-bottom:12px">
    <div style="font-size:16px;font-weight:700;color:#4ade80;margin-bottom:6px">
      ${{flag(s.mercado)}} ${{s.signal}} ${{s.ticker}} — ${{s.empresa}}
    </div>
    <div style="font-size:13px;color:#aaa;display:flex;gap:20px;flex-wrap:wrap">
      <span>Score: <b style="color:#fff">${{s.score_final.toFixed(0)}}</b></span>
      <span>RSI: <b style="color:#fff">${{s.rsi.toFixed(0)}}</b></span>
      <span>Sem: <b style="color:${{retColor(s.ret_sem)}}">${{s.ret_sem>=0?'+':''}}${{s.ret_sem.toFixed(1)}}%</b></span>
      <span>Anual: <b style="color:${{retColor(s.ret_anual)}}">${{s.ret_anual>=0?'+':''}}${{s.ret_anual.toFixed(1)}}%</b></span>
      <span>Sector: ${{s.sector}}</span>
    </div>
  </div>`).join('');

document.getElementById('ventas-block').innerHTML = ventas.slice(0,5).map(s=>`
  <div style="background:#1f0d0d;border:1px solid #3a1a1a;border-radius:10px;padding:16px;margin-bottom:12px">
    <div style="font-size:16px;font-weight:700;color:#fb923c;margin-bottom:6px">
      ${{flag(s.mercado)}} ${{s.signal}} ${{s.ticker}} — ${{s.empresa}}
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
# Excel de fichas
# ─────────────────────────────────────────────

def generate_excel(signals: list[dict], index_stats: dict, output_path: str) -> str:
    """Genera el Excel con 4 hojas: MERVAL, BOVESPA, SP500, Ranking."""
    try:
        import openpyxl
        from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    except ImportError:
        logger.warning("openpyxl no disponible, saltando Excel")
        return ""

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    # Colores
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

        # Header
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
            sig = s["signal"]
            fill = BUY_FILL if "COMPRA" in sig else SELL_FILL if "VENTA" in sig else NEU_FILL

            for col, val in enumerate(row_data, 1):
                cell = ws.cell(row=r, column=col, value=val)
                cell.fill = fill
                cell.font = WHITE
                cell.alignment = Alignment(horizontal="center" if col > 3 else "left")

                # Color para % de retorno
                if col in (5, 6, 7) and isinstance(val, (int, float)):
                    cell.font = GREEN if val >= 0 else RED
                if col == 12:  # Señal
                    if "COMPRA" in str(val):
                        cell.font = GREEN
                    elif "VENTA" in str(val):
                        cell.font = ORANGE

        # Anchos de columna
        widths = [12,28,14,14,8,8,8,7,8,8,8,22,7,14,14]
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[chr(64+i)].width = w

    _write_sheet("MERVAL",  "MERVAL")
    _write_sheet("BOVESPA", "BOVESPA")
    _write_sheet("SP500",   "SP500")

    # Ranking combinado
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
    """Escribe la hoja de ranking global."""
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
