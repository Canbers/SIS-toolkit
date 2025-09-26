// app.js - single-file modules: state/storage, logger, proxy client, console, scripts, runner

// 4.1 State & Storage
const LS_CFG_KEY = 'sis-bulk:cfg';
const LS_TOKEN_KEY = 'sis-bulk:token';

const state = {
  cfg: {
    baseUrl: '',
    tenantId: '',
    persistToken: false,
    concurrency: 4,
    chunkSize: 50,
    dryRun: false,
  },
  token: '',
};

function loadCfg() {
  try {
    const raw = localStorage.getItem(LS_CFG_KEY);
    if (raw) {
      const saved = JSON.parse(raw);
      state.cfg = { ...state.cfg, ...saved };
    }
  } catch {}
  try {
    const t = localStorage.getItem(LS_TOKEN_KEY);
    if (t) state.token = t;
  } catch {}
  // reflect to UI
  byId('cfg_base').value = state.cfg.baseUrl || '';
  byId('cfg_tenant').value = state.cfg.tenantId || '';
  byId('cfg_token').value = state.token || '';
  byId('cfg_persist').checked = !!state.cfg.persistToken;
  byId('cfg_conc').value = String(state.cfg.concurrency || 4);
  byId('cfg_chunk').value = String(state.cfg.chunkSize || 50);
  byId('cfg_dry').checked = !!state.cfg.dryRun;
}

function saveCfg() {
  state.cfg.baseUrl = byId('cfg_base').value.trim();
  state.cfg.tenantId = byId('cfg_tenant').value.trim();
  state.cfg.persistToken = !!byId('cfg_persist').checked;
  state.cfg.concurrency = Math.max(1, parseInt(byId('cfg_conc').value || '4', 10));
  state.cfg.chunkSize = Math.max(1, parseInt(byId('cfg_chunk').value || '50', 10));
  state.cfg.dryRun = !!byId('cfg_dry').checked;
  state.token = byId('cfg_token').value.trim();
  try {
    localStorage.setItem(LS_CFG_KEY, JSON.stringify(state.cfg));
    if (state.cfg.persistToken) localStorage.setItem(LS_TOKEN_KEY, state.token);
    else localStorage.removeItem(LS_TOKEN_KEY);
    log.ok('Config saved');
  } catch (e) {
    log.warn('Failed to persist config: ' + String(e?.message || e));
  }
}

function clearCfg() {
  state.cfg = { baseUrl: '', tenantId: '', persistToken: false, concurrency: 4, chunkSize: 50, dryRun: false };
  state.token = '';
  try {
    localStorage.removeItem(LS_CFG_KEY);
    localStorage.removeItem(LS_TOKEN_KEY);
  } catch {}
  loadCfg();
  log.info('Config cleared');
}

// 4.2 Logger
const logger = {
  lines: [], // {ts, level, msg}
  max: 1000,
  viewEl: null,
};

function redact(str) {
  try {
    if (!str) return str;
    let out = String(str);
    if (state.token) {
      const esc = state.token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      out = out.replace(new RegExp(esc, 'g'), '«REDACTED_TOKEN»');
    }
    return out;
  } catch { return str; }
}

function pushLog(level, msg) {
  const ts = new Date().toISOString();
  const text = typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2);
  const line = { ts, level, msg: redact(text) };
  logger.lines.push(line);
  if (logger.lines.length > logger.max) logger.lines.splice(0, logger.lines.length - logger.max);
  if (logger.viewEl) {
    const div = document.createElement('div');
    div.className = level;
    div.textContent = `[${ts}] ${level.toUpperCase()}: ${line.msg}`;
    logger.viewEl.appendChild(div);
    logger.viewEl.scrollTop = logger.viewEl.scrollHeight;
  }
}

const log = {
  info: (m) => pushLog('info', m),
  ok: (m) => pushLog('ok', m),
  warn: (m) => pushLog('warn', m),
  err: (m) => pushLog('err', m),
};

function downloadLogs() {
  const txt = logger.lines.map(l => `[${l.ts}] ${l.level.toUpperCase()}: ${l.msg}`).join('\n');
  const blob = new Blob([txt], { type: 'text/plain' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `logs-${new Date().toISOString().replace(/[:.]/g,'-')}.txt`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// 4.3 Proxy Client
function resolveUrl(u) {
  if (!u) return '';
  const trimmed = u.trim();
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  const base = (state.cfg.baseUrl || '').replace(/\/+$/,'');
  const path = trimmed.replace(/^\/+/, '');
  return base ? `${base}/${path}` : trimmed; // if no base, allow relative
}

async function proxyRequest({ method = 'GET', url = '', headers = {}, body = undefined, timeout = 30000, signal }) {
  const hdr = { ...headers };
  if (!hdr['Authorization'] && state.token) hdr['Authorization'] = `Bearer ${state.token}`;
  // do not set content-type for pass-through body; content-type applies to /proxy payload
  const payload = { method, url, headers: hdr, body, timeout: Math.floor(timeout/1000) };
  const ctrl = new AbortController();
  const signals = mergeSignals(signal, ctrl.signal);
  const t = setTimeout(() => ctrl.abort(new DOMException('Timeout', 'AbortError')), timeout + 500);
  try {
    const resp = await fetch('/proxy', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: signals,
    });
    const data = await resp.json().catch(() => ({ error: 'Invalid JSON from proxy' }));
    if (!resp.ok) {
      const err = new Error(data?.error || `Proxy HTTP ${resp.status}`);
      err.proxy = true; err.data = data; err.status = resp.status;
      throw err;
    }
    if (data && data.error) {
      const err = new Error(data.error);
      err.proxy = true; err.data = data; err.status = 502;
      throw err;
    }
    return data; // {status, headers, body}
  } finally {
    clearTimeout(t);
  }
}

function mergeSignals(a, b) {
  if (!a) return b;
  if (!b) return a;
  const ctrl = new AbortController();
  const onAbort = () => ctrl.abort();
  if (a.aborted || b.aborted) ctrl.abort();
  a.addEventListener('abort', onAbort);
  b.addEventListener('abort', onAbort);
  return ctrl.signal;
}

async function fetchWithBackoff(doCall, tries = 4, { signal } = {}) {
  let attempt = 0;
  let lastErr;
  while (attempt < tries) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    try {
      const res = await doCall();
      const status = res?.status || 0;
      if (status === 429 || (status >= 500 && status <= 599)) throw new Error(`Transient HTTP ${status}`);
      return res;
    } catch (e) {
      lastErr = e;
      const isTransient = (
        (e && (e.name === 'TypeError' || e.name === 'AbortError')) ||
        (e && e.message && /Transient HTTP|network|ECONN|ETIMEDOUT|fetch/i.test(String(e.message)))
      );
      const status = e?.data?.status || e?.status || 0;
      const proxyTransient = status === 429 || (status >= 500 && status <= 599);
      if (!(isTransient || proxyTransient)) throw e;
      attempt += 1;
      const base = 300 * Math.pow(2, attempt - 1);
      const jitter = Math.floor(Math.random() * 200);
      const delay = Math.min(5000, base + jitter);
      await wait(delay, { signal });
    }
  }
  throw lastErr;
}

function wait(ms, { signal } = {}) {
  return new Promise((res, rej) => {
    const t = setTimeout(() => { cleanup(); res(); }, ms);
    const onAbort = () => { cleanup(); rej(new DOMException('Aborted', 'AbortError')); };
    function cleanup() { clearTimeout(t); signal?.removeEventListener?.('abort', onAbort); }
    if (signal) signal.addEventListener('abort', onAbort);
  });
}

// 4.4 REST Console
function parseHeaders(input) {
  const txt = (input || '').trim();
  if (!txt) return {};
  try {
    const obj = JSON.parse(txt);
    if (obj && typeof obj === 'object') return obj;
  } catch {}
  const out = {};
  txt.split(/\r?\n/).forEach(line => {
    const m = line.split(':');
    if (m.length >= 2) {
      const k = m.shift().trim();
      const v = m.join(':').trim();
      if (k) out[k] = v;
    }
  });
  return out;
}

function parseBody(input) {
  const txt = (input || '').trim();
  if (!txt) return undefined;
  try { return JSON.parse(txt); } catch { return txt; }
}

async function handleSend() {
  const method = byId('c_method').value;
  const urlRaw = byId('c_url').value;
  const url = resolveUrl(urlRaw);
  const headers = parseHeaders(byId('c_headers').value);
  const body = parseBody(byId('c_body').value);
  log.info(`Send ${method} ${url}`);
  try {
    const res = await fetchWithBackoff(() => proxyRequest({ method, url, headers, body, timeout: 30000 }), 4, {});
    byId('c_resp').textContent = JSON.stringify(res, null, 2);
    log.ok(`${method} ${url} -> ${res.status}`);
  } catch (e) {
    const data = e?.data || { error: e?.message || String(e) };
    byId('c_resp').textContent = JSON.stringify(data, null, 2);
    log.err(`${method} ${url} failed: ${data.error || e?.message}`);
  }
}

// 4.5 Scripts System
/** @typedef {{type:'text'|'number'|'checkbox'|'select'|'textarea', name:string, label:string, required?:boolean, default?:any, options?:{label:string,value:string}[], placeholder?:string}} ScriptField */
/** @typedef {{id:string,name:string,fields:ScriptField[],preview:(cfg:any,ctx:any)=>Promise<any>,plan:(cfg:any,ctx:any)=>Promise<any[]>,execute:(item:any,cfg:any,ctx:any)=>Promise<void>}} Script */

const SCRIPT_REGISTRY = [];

function registerScripts(list) {
  for (const s of list) SCRIPT_REGISTRY.push(s);
  renderScriptPicker();
}

function renderScriptPicker() {
  const pick = byId('s_pick');
  pick.innerHTML = '';
  for (const s of SCRIPT_REGISTRY) {
    const opt = document.createElement('option');
    opt.value = s.id; opt.textContent = s.name;
    pick.appendChild(opt);
  }
  renderScriptForm(getCurrentScript());
}

function getCurrentScript() {
  const id = byId('s_pick').value || (SCRIPT_REGISTRY[0]?.id || '');
  return SCRIPT_REGISTRY.find(s => s.id === id) || null;
}

function renderScriptForm(script) {
  const container = byId('s_form');
  container.innerHTML = '';
  if (!script) return;
  const frag = document.createDocumentFragment();
  for (const f of script.fields) {
    const wrap = document.createElement('div');
    wrap.style.marginBottom = '8px';
    const label = document.createElement('label');
    label.textContent = f.label + (f.required ? ' *' : '');
    label.htmlFor = `f_${f.name}`;
    wrap.appendChild(label);
    let el;
    switch (f.type) {
      case 'textarea':
        el = document.createElement('textarea');
        break;
      case 'select':
        el = document.createElement('select');
        for (const opt of f.options || []) {
          const o = document.createElement('option');
          o.value = opt.value; o.textContent = opt.label; el.appendChild(o);
        }
        break;
      case 'checkbox':
        el = document.createElement('input'); el.type = 'checkbox'; break;
      case 'number':
        el = document.createElement('input'); el.type = 'number'; break;
      default:
        el = document.createElement('input'); el.type = 'text';
    }
    el.id = `f_${f.name}`;
    if (f.placeholder) el.placeholder = f.placeholder;
    if (f.type === 'checkbox') el.checked = !!f.default; else if (f.default != null) el.value = String(f.default);
    wrap.appendChild(el);
    container.appendChild(wrap);
  }
}

function readForm(script) {
  const out = {};
  for (const f of script.fields) {
    const el = byId(`f_${f.name}`);
    if (!el) continue;
    let v;
    if (f.type === 'checkbox') v = !!el.checked;
    else if (f.type === 'number') v = Number(el.value);
    else v = el.value;
    if (f.required && (v === '' || v == null || Number.isNaN(v))) throw new Error(`Field required: ${f.label}`);
    out[f.name] = v;
  }
  return out;
}

// Example Scripts
registerScripts([
  {
    id: 'users_tag',
    name: 'Users: add tag to all matching',
    fields: [
      { type: 'text', name: 'listPath', label: 'List users path', default: '/v1/users', required: true },
      { type: 'text', name: 'match', label: 'Email contains', default: '', placeholder: 'example.com' },
      { type: 'text', name: 'tag', label: 'Tag to add', required: true },
      { type: 'text', name: 'updatePath', label: 'Update path', default: '/v1/users/{id}/tags', required: true },
    ],
    async preview(cfg, ctx) {
      const url = resolveUrl(cfg.listPath);
      const res = await ctx.request({ method: 'GET', url });
      const items = Array.isArray(res.body) ? res.body : (res.body?.items || []);
      log.info(`Preview users: showing up to 5 of ${items.length}`);
      log.info(JSON.stringify(items.slice(0, 5), null, 2));
      return items.slice(0, 5);
    },
    async plan(cfg, ctx) {
      const url = resolveUrl(cfg.listPath);
      const res = await ctx.request({ method: 'GET', url });
      const users = Array.isArray(res.body) ? res.body : (res.body?.items || []);
      const match = (cfg.match || '').toLowerCase();
      const filtered = match ? users.filter(u => String(u.email||'').toLowerCase().includes(match)) : users;
      const items = filtered.map(u => ({ id: u.id, email: u.email }));
      log.ok(`Plan ready: ${items.length} users to process`);
      return items;
    },
    async execute(item, cfg, ctx) {
      const path = (cfg.updatePath || '').replace('{id}', encodeURIComponent(item.id));
      const url = resolveUrl(path);
      if (state.cfg.dryRun) {
        log.info(`Would tag user ${item.id} (${item.email}) with "${cfg.tag}"`);
        return;
      }
      const body = { add: [cfg.tag] };
      const res = await fetchWithBackoff(() => ctx.request({ method: 'POST', url, headers: { 'Content-Type': 'application/json' }, body }), 4, { signal: ctx.signal });
      if (res.status >= 200 && res.status < 300) log.ok(`Tagged ${item.id}`); else throw new Error(`HTTP ${res.status}`);
    },
  },
  {
    id: 'csv_post',
    name: 'Custom: POST each row from CSV',
    fields: [
      { type: 'text', name: 'endpoint', label: 'Endpoint', default: '/v1/ingest', required: true },
      { type: 'textarea', name: 'csv', label: 'CSV (with header)', required: true, placeholder: 'id,email\n1,a@example.com' },
    ],
    async preview(cfg, ctx) {
      const rows = parseCSV(cfg.csv || '');
      log.info(`CSV rows: ${rows.length}`);
      log.info(JSON.stringify(rows.slice(0, 3), null, 2));
      return rows.slice(0, 3);
    },
    async plan(cfg, ctx) {
      const rows = parseCSV(cfg.csv || '');
      log.ok(`Plan ready: ${rows.length} rows`);
      return rows;
    },
    async execute(item, cfg, ctx) {
      const url = resolveUrl(cfg.endpoint);
      if (state.cfg.dryRun) { log.info(`Would POST: ${JSON.stringify(item)}`); return; }
      const res = await fetchWithBackoff(() => ctx.request({ method: 'POST', url, headers: { 'Content-Type': 'application/json' }, body: item, signal: ctx.signal }), 4, { signal: ctx.signal });
      if (res.status >= 200 && res.status < 300) log.ok(`Posted`); else throw new Error(`HTTP ${res.status}`);
    },
  },
]);

function parseCSV(text) {
  const lines = (text || '').replace(/\r/g, '').split('\n').filter(l => l.trim() !== '');
  if (lines.length === 0) return [];
  const header = splitCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = splitCSVLine(lines[i]);
    const obj = {};
    for (let j = 0; j < header.length; j++) obj[header[j]] = cols[j] ?? '';
    rows.push(obj);
  }
  return rows;
}

function splitCSVLine(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i+1] === '"') { cur += '"'; i++; }
        else inQ = false;
      } else cur += ch;
    } else {
      if (ch === ',') { out.push(cur); cur = ''; }
      else if (ch === '"') inQ = true;
      else cur += ch;
    }
  }
  out.push(cur);
  return out.map(s => s.trim());
}

// 4.6 Runner
async function runBatches(items, perItemFn, { concurrency = 4, signal, onProgress } = {}) {
  const total = items.length;
  let next = 0, done = 0, ok = 0, fail = 0;
  const update = () => onProgress && onProgress({ total, done, ok, fail });
  update();
  const runOne = async () => {
    while (true) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError');
      const idx = next++;
      if (idx >= total) return;
      try {
        await perItemFn(items[idx], idx);
        ok++;
      } catch (e) {
        fail++;
        log.err(`Item ${idx} failed: ${e?.message || e}`);
      } finally { done++; update(); }
    }
  };
  const workers = Array.from({ length: Math.min(concurrency, total) }, () => runOne());
  await Promise.all(workers);
  return { total, done, ok, fail };
}

// UI wiring for Runner
let currentController = null;

async function onPreview() {
  const script = getCurrentScript(); if (!script) return;
  try {
    const cfg = readForm(script);
    await script.preview(cfg, { request: proxyRequest });
  } catch (e) { log.err(e?.message || String(e)); }
}

async function onRun() {
  const script = getCurrentScript(); if (!script) return;
  const cfg = readForm(script);
  log.info(`Planning…`);
  let items = [];
  try { items = await script.plan(cfg, { request: proxyRequest }); }
  catch (e) { log.err(e?.message || String(e)); return; }
  const conc = Math.max(1, Number(state.cfg.concurrency) || 4);
  const chunkSize = Math.max(1, Number(state.cfg.chunkSize) || items.length);
  log.ok(`Running ${items.length} items with concurrency ${conc} (chunk ${chunkSize})`);
  currentController = new AbortController();
  const progress = { total: items.length, done: 0, ok: 0, fail: 0 };
  const onProgress = (p) => {
    Object.assign(progress, p);
    byId('r_done').textContent = String(p.done);
    byId('r_ok').textContent = String(p.ok);
    byId('r_fail').textContent = String(p.fail);
    byId('r_prog').value = progress.total ? Math.round((p.done / progress.total) * 100) : 0;
  };
  const perItem = (item) => script.execute(item, cfg, { request: proxyRequest, signal: currentController.signal });
  try {
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, i + chunkSize);
      await runBatches(chunk, perItem, { concurrency: conc, signal: currentController.signal, onProgress });
      if (currentController.signal.aborted) throw new DOMException('Aborted', 'AbortError');
    }
    log.ok('Run completed');
  } catch (e) {
    if (e?.name === 'AbortError') log.warn('Run cancelled'); else log.err(e?.message || String(e));
  } finally {
    currentController = null;
  }
}

function onCancel() {
  if (currentController) { currentController.abort(); }
}

// Helpers
function byId(id) { return document.getElementById(id); }

function bindUI() {
  logger.viewEl = byId('log_view');
  byId('cfg_save').addEventListener('click', saveCfg);
  byId('cfg_clear').addEventListener('click', clearCfg);
  byId('log_download').addEventListener('click', downloadLogs);

  byId('c_send').addEventListener('click', handleSend);
  byId('s_pick').addEventListener('change', () => renderScriptForm(getCurrentScript()));
  byId('s_preview').addEventListener('click', onPreview);
  byId('s_run').addEventListener('click', onRun);
  byId('s_cancel').addEventListener('click', onCancel);
}

// Init
window.addEventListener('DOMContentLoaded', () => {
  loadCfg();
  bindUI();
  renderScriptPicker();
  byId('c_method').value = 'GET';
  log.info('Ready');
});

