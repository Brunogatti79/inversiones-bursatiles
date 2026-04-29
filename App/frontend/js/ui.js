function fmtSize(bytes) {
  if (bytes < 1024)        return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function fmtDate(iso) {
  return new Date(iso).toLocaleDateString('es-AR', { day: '2-digit', month: 'short', year: 'numeric' });
}

function marketBadge(market) {
  const map = { merval: '🇦🇷 MERVAL', bovespa: '🇧🇷 BOVESPA', sp500: '🇺🇸 S&P 500', general: 'General' };
  const cls = { merval: 'mkt-merval', bovespa: 'mkt-bovespa', sp500: 'mkt-sp500', general: 'mkt-general' };
  return `<span class="result-market ${cls[market] || 'mkt-general'}">${map[market] || market || '—'}</span>`;
}

function typeBadge(type) {
  const cls = { pdf: 'badge-pdf', docx: 'badge-docx', doc: 'badge-docx', xlsx: 'badge-xlsx', xls: 'badge-xlsx', txt: 'badge-txt', csv: 'badge-csv' };
  return `<span class="badge-type ${cls[type] || 'badge-txt'}">${type.toUpperCase()}</span>`;
}

function renderDocRows(docs) {
  if (!docs.length) return `<tr><td colspan="7"><div class="empty-state">Sin documentos indexados</div></td></tr>`;
  return docs.map(d => `
    <tr>
      <td><strong>${d.file_name}</strong></td>
      <td>${typeBadge(d.file_type)}</td>
      <td>${marketBadge(d.market)}</td>
      <td><span style="font-family:'DM Mono',monospace">${d.chunk_count}</span></td>
      <td><span style="color:var(--muted);font-family:'DM Mono',monospace">${fmtSize(d.size_bytes)}</span></td>
      <td><span style="color:var(--muted)">${fmtDate(d.indexed_at)}</span></td>
      <td><button class="btn-secondary" style="padding:4px 10px;font-size:11px" onclick="deleteDoc('${d.file_name}')">✕</button></td>
    </tr>`).join('');
}

function renderSearchResults(results) {
  if (!results.length) return `<div class="empty-state">Sin resultados para esta búsqueda</div>`;
  return results.map((r, i) => `
    <div class="result-card">
      <div class="result-meta">
        <span style="color:var(--muted);font-size:11px">#${i + 1}</span>
        <span class="result-file">${r.file_name}</span>
        ${marketBadge(r.market)}
        <span class="result-score">${(r.score * 100).toFixed(1)}% relevancia</span>
      </div>
      <div class="result-text">${escHtml(r.text.slice(0, 420))}${r.text.length > 420 ? '…' : ''}</div>
    </div>`).join('');
}

function renderSources(sources) {
  return sources.map(s => `<span class="source-tag">${escHtml(s)}</span>`).join('');
}

function escHtml(str) {
  return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function setStatus(ok) {
  document.querySelector('.dot').className = 'dot ' + (ok ? 'ok' : 'err');
  document.getElementById('statusLabel').textContent = ok ? 'Conectado' : 'Sin conexión';
}
