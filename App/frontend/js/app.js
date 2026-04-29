// ── Navigation ───────────────────────────────────────────
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => {
    const view = item.dataset.view;
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    item.classList.add('active');
    document.getElementById('view-' + view).classList.add('active');
    if (view === 'documents') loadDocuments();
  });
});

// ── Dashboard ────────────────────────────────────────────
async function loadStatus() {
  try {
    const s = await api.status();
    document.getElementById('kpiDocs').textContent   = s.total_documents;
    document.getElementById('kpiChunks').textContent = s.total_chunks.toLocaleString();
    document.getElementById('kpiLLM').textContent    = s.llm_provider;
    document.getElementById('kpiEmbed').textContent  = s.embedding_model.split('/').pop();
    document.getElementById('kpiFolder').textContent = s.docs_folder;
    setStatus(true);
  } catch {
    setStatus(false);
  }
}

document.getElementById('btnReindex').addEventListener('click', async () => {
  const btn = document.getElementById('btnReindex');
  const log = document.getElementById('reindexLog');
  btn.disabled = true;
  btn.textContent = '↺ Reindexando...';
  log.innerHTML = '<span class="spinner"></span> Procesando...';
  try {
    const r = await api.reindex();
    log.textContent = `✓ ${r.message}`;
    await loadStatus();
  } catch (e) {
    log.textContent = `✗ Error: ${e.message}`;
  } finally {
    btn.disabled = false;
    btn.textContent = '↺ Reindexar';
  }
});

// ── Documents ────────────────────────────────────────────
let allDocs = [];

async function loadDocuments() {
  const tbody = document.getElementById('docsBody');
  tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state"><span class="spinner"></span></div></td></tr>`;
  try {
    const market = document.getElementById('marketFilter').value;
    allDocs = await api.listDocuments(market);
    renderDocs(allDocs);
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty-state">Error: ${e.message}</div></td></tr>`;
  }
}

function renderDocs(docs) {
  const q = document.getElementById('docsSearch').value.toLowerCase();
  const filtered = q ? docs.filter(d => d.file_name.toLowerCase().includes(q)) : docs;
  document.getElementById('docsBody').innerHTML = renderDocRows(filtered);
  document.getElementById('docsFooter').textContent = `${filtered.length} de ${docs.length} documentos`;
}

document.getElementById('marketFilter').addEventListener('change', loadDocuments);
document.getElementById('docsSearch').addEventListener('input', () => renderDocs(allDocs));

async function deleteDoc(name) {
  if (!confirm(`¿Eliminar "${name}" del índice?`)) return;
  try {
    await api.deleteDocument(name);
    await loadDocuments();
  } catch (e) {
    alert('Error: ' + e.message);
  }
}

// ── Search ───────────────────────────────────────────────
document.getElementById('btnSearch').addEventListener('click', async () => {
  const query  = document.getElementById('searchQuery').value.trim();
  const market = document.getElementById('searchMarket').value;
  const n      = document.getElementById('searchN').value;
  const out    = document.getElementById('searchResults');
  const btn    = document.getElementById('btnSearch');
  if (!query) return;

  out.innerHTML = `<div class="empty-state"><span class="spinner"></span> Buscando...</div>`;
  btn.disabled = true;
  try {
    const res = await api.search(query, market, n);
    out.innerHTML = renderSearchResults(res.results);
  } catch (e) {
    out.innerHTML = `<div class="empty-state">Error: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
  }
});

document.getElementById('searchQuery').addEventListener('keydown', e => {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) document.getElementById('btnSearch').click();
});

// ── Agent ────────────────────────────────────────────────
document.getElementById('btnAgent').addEventListener('click', async () => {
  const query      = document.getElementById('agentQuery').value.trim();
  const outputType = document.getElementById('agentOutputType').value;
  const market     = document.getElementById('agentMarket').value;
  const useStream  = document.getElementById('agentStream').checked;
  const btn        = document.getElementById('btnAgent');
  if (!query) return;

  const wrap   = document.getElementById('agentResponse');
  const body   = document.getElementById('responseText');
  const badge  = document.getElementById('responseType');
  const chunks = document.getElementById('responseChunks');
  const srcs   = document.getElementById('responseSources');

  wrap.style.display = 'block';
  body.textContent = '';
  srcs.innerHTML = '';
  badge.textContent = outputType.toUpperCase();
  chunks.textContent = '';
  btn.disabled = true;

  if (useStream) {
    body.classList.add('cursor-blink');
    try {
      const res    = await api.agentStream(query, outputType, market);
      const reader = res.body.getReader();
      const dec    = new TextDecoder();
      let buffer   = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += dec.decode(value, { stream: true });
        const lines = buffer.split('\n\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6);
          if (data === '[DONE]') break;
          if (data.startsWith('sources=')) {
            srcs.innerHTML = renderSources(data.slice(8).split(',').filter(Boolean));
          } else {
            body.textContent += data;
          }
        }
      }
    } catch (e) {
      body.textContent = 'Error de streaming: ' + e.message;
    } finally {
      body.classList.remove('cursor-blink');
      btn.disabled = false;
    }
  } else {
    body.textContent = '⏳ Procesando...';
    try {
      const res = await api.agentQuery(query, outputType, market);
      body.textContent = res.answer;
      chunks.textContent = `${res.chunks_used} fragmentos usados`;
      srcs.innerHTML = renderSources(res.sources);
    } catch (e) {
      body.textContent = 'Error: ' + e.message;
    } finally {
      btn.disabled = false;
    }
  }
});

document.getElementById('agentQuery').addEventListener('keydown', e => {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) document.getElementById('btnAgent').click();
});

// ── Init ─────────────────────────────────────────────────
loadStatus();
setInterval(loadStatus, 30000);
