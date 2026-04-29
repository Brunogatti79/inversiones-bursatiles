const API = 'http://localhost:8000/api';

async function apiFetch(path, options = {}) {
  const res = await fetch(API + path, {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
  return res.json();
}

const api = {
  status:         ()             => apiFetch('/documents/status'),
  listDocuments:  (market)       => apiFetch('/documents/' + (market ? `?market=${market}` : '')),
  reindex:        ()             => apiFetch('/documents/reindex', { method: 'POST' }),
  deleteDocument: (name)         => apiFetch(`/documents/${encodeURIComponent(name)}`, { method: 'DELETE' }),

  search: (query, market, n) => apiFetch('/search/', {
    method: 'POST',
    body: JSON.stringify({ query, market: market || null, n_results: parseInt(n) })
  }),

  agentQuery: (query, outputType, market) => apiFetch('/agent/query', {
    method: 'POST',
    body: JSON.stringify({ query, output_type: outputType, market: market || null })
  }),

  agentStream: (query, outputType, market) => fetch(API + '/agent/stream', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, output_type: outputType, market: market || null, stream: true })
  })
};
