import registerPermissionReassign from './scripts/permissionReassign.js';

// app.js - single-file modules: state/storage, logger, proxy client, console, scripts, runner

// 4.1 State & Storage
const LS_CFG_KEY = 'sis-bulk:cfg';
const LS_TOKEN_KEY = 'sis-bulk:token';

const state = {
  cfg: {
    baseUrl: '',
    persistToken: false,
    concurrency: 4,
    chunkSize: 50,
  },
  token: '',
};

function loadCfg() {
  try {
    const raw = localStorage.getItem(LS_CFG_KEY);
    if (raw) {
      const saved = JSON.parse(raw);
      const { tenantId, ...rest } = saved || {};
      state.cfg = { ...state.cfg, ...rest };
    }
  } catch {}
  try {
    const t = localStorage.getItem(LS_TOKEN_KEY);
    if (t) state.token = t;
  } catch {}
  // reflect to UI
  const baseEl = byId('cfg_base');
  if (baseEl) baseEl.value = state.cfg.baseUrl || '';
  const tokenEl = byId('cfg_token');
  if (tokenEl) {
    tokenEl.value = state.token || '';
    tokenEl.type = 'password';
  }
  const toggleBtn = byId('cfg_token_toggle');
  if (toggleBtn) {
    toggleBtn.textContent = 'Show';
    toggleBtn.setAttribute('aria-pressed', 'false');
  }
  const persistEl = byId('cfg_persist');
  if (persistEl) persistEl.checked = !!state.cfg.persistToken;
  const concEl = byId('cfg_conc');
  if (concEl) concEl.value = String(state.cfg.concurrency || 4);
  const chunkEl = byId('cfg_chunk');
  if (chunkEl) chunkEl.value = String(state.cfg.chunkSize || 50);
}

function saveCfg() {
  const baseEl = byId('cfg_base');
  state.cfg.baseUrl = baseEl ? baseEl.value.trim() : '';
  const persistEl = byId('cfg_persist');
  state.cfg.persistToken = !!persistEl?.checked;
  const concEl = byId('cfg_conc');
  state.cfg.concurrency = Math.max(1, parseInt(concEl?.value || '4', 10));
  const chunkEl = byId('cfg_chunk');
  state.cfg.chunkSize = Math.max(1, parseInt(chunkEl?.value || '50', 10));
  const tokenEl = byId('cfg_token');
  state.token = tokenEl ? tokenEl.value.trim() : '';
  delete state.cfg.tenantId;
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
  state.cfg = { baseUrl: '', persistToken: false, concurrency: 4, chunkSize: 50 };
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

// Scoped script logger that writes to Run Summary details and optionally mirrors to Activity Log
function createScriptLog({ mirrorLevels = ['warn', 'err'], mirrorFilter } = {}) {
  function writeToRunDetail(level, msg) {
    try {
      const el = byId('r_run_detail');
      if (!el) return;
      const ts = new Date().toISOString();
      const text = typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2);
      const line = `[${ts}] ${level.toUpperCase()}: ${redact(text)}`;
      el.textContent += (el.textContent ? '\n' : '') + line;
      el.scrollTop = el.scrollHeight;
    } catch {}
  }
  return {
    info: (m) => { writeToRunDetail('info', m); if (mirrorLevels.includes('info') && (!mirrorFilter || mirrorFilter('info', m))) log.info(m); },
    ok: (m) => { writeToRunDetail('ok', m); if (mirrorLevels.includes('ok') && (!mirrorFilter || mirrorFilter('ok', m))) log.ok(m); },
    warn: (m) => { writeToRunDetail('warn', m); if (mirrorLevels.includes('warn') && (!mirrorFilter || mirrorFilter('warn', m))) log.warn(m); },
    err: (m) => { writeToRunDetail('err', m); if (mirrorLevels.includes('err') && (!mirrorFilter || mirrorFilter('err', m))) log.err(m); },
  };
}

function clearRunDetail() {
  const el = byId('r_run_detail');
  if (el) el.textContent = '';
}

function setRunSummary(text) {
  const el = byId('r_run_summary');
  if (el) el.textContent = text || '';
}

function appendRunDetail(text) {
  try {
    const el = byId('r_run_detail');
    if (!el) return;
    el.textContent += (el.textContent ? '\n' : '') + String(text ?? '');
    el.scrollTop = el.scrollHeight;
  } catch {}
}

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
// Simple JSON editors (Ace with graceful fallback)
const editors = {};

function initJsonEditor(id) {
  const el = byId(id);
  if (!el) return null;
  try {
    if (window.ace && typeof window.ace.edit === 'function') {
      const editor = window.ace.edit(el);
      editor.session.setMode('ace/mode/json');
      editor.setOptions({
        tabSize: 2,
        useSoftTabs: true,
        wrap: true,
        showPrintMargin: false,
        highlightActiveLine: true,
        behavioursEnabled: true,
      });
      editor.on('focus', () => el.classList.add('focused'));
      editor.on('blur', () => el.classList.remove('focused'));
      // Resize with container
      try {
        const ro = new ResizeObserver(() => editor.resize());
        ro.observe(el);
        el._ro = ro;
      } catch {}
      editors[id] = {
        get: () => editor.getValue(),
        set: (v) => editor.session.setValue(v || '', -1),
        instance: editor,
      };
      return editors[id];
    }
  } catch {}
  // Fallback: contenteditable div
  el.contentEditable = 'true';
  el.setAttribute('role', 'textbox');
  el.setAttribute('aria-multiline', 'true');
  el.addEventListener('focus', () => el.classList.add('focused'));
  el.addEventListener('blur', () => el.classList.remove('focused'));
  editors[id] = {
    get: () => (el.textContent || ''),
    set: (v) => { el.textContent = v || ''; },
  };
  return editors[id];
}

function getEditorValue(id) {
  const ed = editors[id];
  if (!ed) return '';
  try { return ed.get() || ''; } catch { return ''; }
}
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
  const headers = parseHeaders(getEditorValue('c_headers'));
  const body = parseBody(getEditorValue('c_body'));
  log.info(`Send ${method} ${url}`);
  try {
    const statusEl = byId('c_resp_status');
    const hdrEl = byId('c_resp_headers');
    const bodyEl = byId('c_resp_body');
    if (statusEl) statusEl.textContent = 'Sending…';
    if (hdrEl) hdrEl.textContent = '';
    if (bodyEl) bodyEl.textContent = '';

    const res = await fetchWithBackoff(() => proxyRequest({ method, url, headers, body, timeout: 30000 }), 4, {});

    // Format headers
    function formatHeaders(h) {
      try {
        if (!h) return '';
        const entries = Array.isArray(h) ? h : Object.entries(h);
        const lines = entries
          .map(([k, v]) => [String(k), Array.isArray(v) ? v.join(', ') : String(v)])
          .sort((a, b) => a[0].localeCompare(b[0]))
          .map(([k, v]) => `${k}: ${v}`);
        return lines.join('\n');
      } catch { return ''; }
    }
    // Format body, attempting pretty JSON
    function formatBody(b) {
      try {
        if (b == null) return '';
        if (typeof b === 'string') {
          try { return JSON.stringify(JSON.parse(b), null, 2); } catch { return b; }
        }
        if (typeof b === 'object') return JSON.stringify(b, null, 2);
        return String(b);
      } catch { return String(b); }
    }

    if (statusEl) statusEl.textContent = `HTTP ${res?.status ?? ''}`;
    if (hdrEl) hdrEl.textContent = formatHeaders(res?.headers);
    if (bodyEl) bodyEl.textContent = formatBody(res?.body);
    log.ok(`${method} ${url} -> ${res.status}`);
  } catch (e) {
    const statusEl = byId('c_resp_status');
    const hdrEl = byId('c_resp_headers');
    const bodyEl = byId('c_resp_body');
    const data = e?.data || { error: e?.message || String(e) };
    if (statusEl) statusEl.textContent = `Error${data?.status ? ` ${data.status}` : ''}`;
    if (hdrEl) hdrEl.textContent = '';
    if (bodyEl) bodyEl.textContent = JSON.stringify(data, null, 2);
    log.err(`${method} ${url} failed: ${data.error || e?.message}`);
  }
}

// 4.5 Scripts System
/** @typedef {{label:string,value:string,disabled?:boolean}} ScriptFieldOption */
/** @typedef {{
 *   type:'text'|'number'|'checkbox'|'select'|'textarea'|'multiselect'|'button',
 *   name:string,
 *   label:string,
 *   required?:boolean,
 *   default?:any,
 *   options?:ScriptFieldOption[],
 *   placeholder?:string,
 *   // UX niceties for selects
 *   searchable?:boolean,
 *   checkboxes?:boolean,
 *   showPills?:boolean,
 *   loadOptions?:(ctx:{
 *     request:typeof proxyRequest,
 *     resolveUrl:typeof resolveUrl,
 *     fetchWithBackoff:typeof fetchWithBackoff,
 *     getFieldValue:(fieldName:string)=>any,
 *     setFieldValue:(fieldName:string,value:any)=>void,
 *     reloadFieldOptions:(fieldName:string, options?:{preserveSelection?:boolean})=>Promise<void>,
 *     log:typeof log,
 *   })=>Promise<ScriptFieldOption[]>,
 *   reloadOn?:string[],
 *   autoLoad?:boolean,
 *   onClick?:(ctx:{
 *     request:typeof proxyRequest,
 *     resolveUrl:typeof resolveUrl,
 *     fetchWithBackoff:typeof fetchWithBackoff,
 *     getFieldValue:(fieldName:string)=>any,
 *     setFieldValue:(fieldName:string,value:any)=>void,
 *     reloadFieldOptions:(fieldName:string, options?:{preserveSelection?:boolean})=>Promise<void>,
 *     log:typeof log,
 *   })=>Promise<void>|void,
}} ScriptField */
/** @typedef {{id:string,name:string,fields:ScriptField[],preview:(cfg:any,ctx:any)=>Promise<any>,plan:(cfg:any,ctx:any)=>Promise<any[]>,execute:(item:any,cfg:any,ctx:any)=>Promise<void>}} Script */

const SCRIPT_REGISTRY = [];

function registerScripts(list) {
  let inserted = 0;
  for (const s of list) {
    if (!s || !s.id) continue;
    const exists = SCRIPT_REGISTRY.some((existing) => existing.id === s.id);
    if (exists) continue;
    SCRIPT_REGISTRY.push(s);
    inserted += 1;
  }
  if (inserted) renderScriptPicker();
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

async function renderScriptForm(script) {
  const container = byId('s_form');
  container.innerHTML = '';
  if (!script) return;
  // Any time the form is (re)rendered, require a fresh preview
  setRunEnabled(false);
  const frag = document.createDocumentFragment();
  const fieldContexts = new Map();
  const context = {
    request: proxyRequest,
    resolveUrl,
    fetchWithBackoff,
    getFieldValue(name) {
      const el = byId(`f_${name}`);
      if (!el) return undefined;
      if (el.dataset.multiselect === '1') return Array.from(el.selectedOptions).map(o => o.value);
      if (el.type === 'checkbox') return !!el.checked;
      if (el.type === 'number') return Number(el.value);
      return el.value;
    },
    setFieldValue(name, value) {
      const el = byId(`f_${name}`);
      if (!el) return;
      if (el.dataset.multiselect === '1' && Array.isArray(value)) {
        const values = value.map(String);
        for (const opt of el.options) opt.selected = values.includes(opt.value);
        return;
      }
      if (el.type === 'checkbox') {
        el.checked = !!value;
        return;
      }
      el.value = value != null ? String(value) : '';
    },
    async reloadFieldOptions(name, options = {}) {
      const stored = fieldContexts.get(name);
      if (!stored) return;
      const { field, element } = stored;
      const preserve = options.preserveSelection !== false;
      let previous;
      if (preserve) {
        previous = element.dataset.multiselect === '1'
          ? Array.from(element.selectedOptions).map((opt) => opt.value)
          : element.value;
      }
      await populateOptions(field, element, { preserveSelection: preserve });
      if (preserve && previous != null) {
        if (Array.isArray(previous)) {
          for (const opt of element.options) opt.selected = previous.includes(opt.value);
        } else {
          element.value = previous;
        }
      }
    },
    log,
  };
  const pendingReloads = new Set();
  function buildEnhancedFromSelect(selectEl, field) {
    const isMulti = !!selectEl.multiple;
    const wrap = document.createElement('div');
    wrap.className = 'enh-select';
    const control = document.createElement('div');
    control.className = 'enh-control';
    const pills = document.createElement('div');
    pills.className = 'enh-pills';
    const input = document.createElement('input');
    input.type = 'text';
    input.placeholder = field.placeholder || 'Search…';
    input.className = 'enh-input';
    const dropdown = document.createElement('div');
    dropdown.className = 'enh-dropdown';
    const list = document.createElement('ul');
    dropdown.appendChild(list);
    control.appendChild(pills);
    control.appendChild(input);
    wrap.appendChild(control);
    wrap.appendChild(dropdown);
    selectEl.style.display = 'none';
    selectEl.parentNode.insertBefore(wrap, selectEl.nextSibling);

    function getOptions() {
      return Array.from(selectEl.options).map(o => ({ value: o.value, label: o.textContent, disabled: o.disabled, selected: o.selected }));
    }
    function syncFromSelect() {
      const opts = getOptions();
      // pills
      pills.innerHTML = '';
      if (isMulti && field.showPills) {
        for (const o of opts.filter(o => o.selected)) {
          const chip = document.createElement('span');
          chip.className = 'enh-pill';
          chip.textContent = o.label;
          const x = document.createElement('button');
          x.type = 'button'; x.className = 'enh-pill-x'; x.textContent = '×';
          x.addEventListener('click', () => {
            for (const optEl of selectEl.options) if (optEl.value === o.value) optEl.selected = false;
            updateListChecks();
            selectEl.dispatchEvent(new Event('input', { bubbles: true }));
            syncFromSelect();
          });
          chip.appendChild(x);
          pills.appendChild(chip);
        }
      }
      // single selection placeholder
      if (!isMulti && field.searchable) {
        const sel = opts.find(o => o.selected);
        input.value = sel ? sel.label : '';
      }
    }
    function updateListChecks() {
      const opts = getOptions();
      const q = input.value.trim().toLowerCase();
      list.innerHTML = '';
      for (const o of opts) {
        if (q && !o.label.toLowerCase().includes(q)) continue;
        const li = document.createElement('li');
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'enh-item';
        btn.disabled = !!o.disabled;
        if (isMulti && field.checkboxes) {
          const cb = document.createElement('input');
          cb.type = 'checkbox'; cb.checked = !!o.selected; cb.disabled = !!o.disabled;
          cb.addEventListener('click', (e) => { e.preventDefault(); });
          btn.appendChild(cb);
        }
        const span = document.createElement('span');
        span.textContent = o.label;
        btn.appendChild(span);
        btn.addEventListener('click', () => {
          if (isMulti) {
            for (const optEl of selectEl.options) if (optEl.value === o.value) optEl.selected = !optEl.selected;
          } else {
            for (const optEl of selectEl.options) optEl.selected = (optEl.value === o.value);
            dropdown.classList.remove('open');
          }
          selectEl.dispatchEvent(new Event('input', { bubbles: true }));
          updateListChecks();
          syncFromSelect();
        });
        li.appendChild(btn);
        list.appendChild(li);
      }
    }
    function open() { dropdown.classList.add('open'); input.focus(); }
    function close() { dropdown.classList.remove('open'); }
    control.addEventListener('click', (e) => {
      if (!dropdown.classList.contains('open')) open(); else close();
      e.stopPropagation();
    });
    input.addEventListener('input', () => updateListChecks());
    document.addEventListener('click', () => close());
    wrap.addEventListener('click', (e) => e.stopPropagation());

    // expose rebuild hook
    selectEl._enhancedRebuild = () => { updateListChecks(); syncFromSelect(); };
    selectEl.addEventListener('change', () => selectEl._enhancedRebuild());
    selectEl.addEventListener('input', () => selectEl._enhancedRebuild());
    // initial
    updateListChecks();
    syncFromSelect();
  }
  async function populateOptions(field, selectEl, extra = {}) {
    const loader = field.loadOptions;
    if (!loader) return;
    const key = field.name;
    if (pendingReloads.has(key)) return;
    pendingReloads.add(key);
    selectEl.disabled = true;
    selectEl.innerHTML = '';
    const loading = document.createElement('option');
    loading.textContent = 'Loading…';
    loading.value = '';
    selectEl.appendChild(loading);
    try {
      const opts = await loader(context);
      selectEl.innerHTML = '';
      for (const opt of opts || []) {
        const o = document.createElement('option');
        o.value = opt.value;
        o.textContent = opt.label;
        if (opt.disabled) o.disabled = true;
        selectEl.appendChild(o);
      }
      if (extra.preserveSelection === false) {
        selectEl.selectedIndex = -1;
      }
      const def = field.default;
      if (field.type === 'multiselect') {
        const values = Array.isArray(def) ? def.map(String) : [];
        for (const opt of selectEl.options) opt.selected = values.includes(opt.value);
      } else if (def != null && selectEl.value === '') {
        selectEl.value = String(def);
      }
      if (typeof selectEl._enhancedRebuild === 'function') {
        selectEl._enhancedRebuild();
      }
    } catch (e) {
      log.err(`Failed loading options for ${field.label}: ${e?.message || e}`);
      selectEl.innerHTML = '';
      const fail = document.createElement('option');
      fail.textContent = 'Failed to load';
      fail.value = '';
      fail.disabled = true;
      selectEl.appendChild(fail);
    } finally {
      selectEl.disabled = false;
      pendingReloads.delete(key);
    }
  }
  // Special layout for permission reassign script: emphasize Target -> Replacement and hide advanced by default
  if (script.id === 'perm_reassign') {
    const byName = Object.fromEntries(script.fields.map(f => [f.name, f]));
    const fetchBtnField = byName.fetchBundles;
    const targetField = byName.targetBundles;
    const replacementField = byName.replacementBundle;

    // Optional: fetch button stays visible (not an advanced config)
    if (fetchBtnField) {
      const wrap = document.createElement('div');
      wrap.style.marginBottom = '8px';
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.id = `f_${fetchBtnField.name}`;
      btn.textContent = fetchBtnField.label || 'Fetch';
      btn.dataset.buttonField = '1';
      wrap.appendChild(btn);
      frag.appendChild(wrap);
      if (typeof fetchBtnField.onClick === 'function') {
        btn.addEventListener('click', async () => {
          try {
            setRunEnabled(false);
            await fetchBtnField.onClick(context);
          } catch (e) { log.err(e?.message || String(e)); }
        });
      }
    }

    // Explanatory text
    const explain = document.createElement('div');
    explain.className = 'callout';
    explain.innerHTML = '<strong>How this works:</strong> All users who have any of the Target bundles will have those bundle(s) removed and will instead be assigned the Replacement bundle. If a user also has other bundle(s) that are not selected as Target, those non‑target bundles stay as they are.';
    frag.appendChild(explain);

    // Main split layout: Target → Replacement
    const split = document.createElement('div');
    split.style.display = 'grid';
    split.style.gap = '16px 20px';
    split.style.gridTemplateColumns = 'minmax(0,1fr) auto minmax(0,1fr)';
    split.style.alignItems = 'start';

    // Left: Target bundles (multiselect)
    if (targetField) {
      const left = document.createElement('div');
      left.className = 'field';
      const lab = document.createElement('label');
      lab.textContent = targetField.label + (targetField.required ? ' *' : '');
      lab.htmlFor = `f_${targetField.name}`;
      const sel = document.createElement('select');
      sel.id = `f_${targetField.name}`;
      sel.multiple = true;
      sel.dataset.multiselect = '1';
      if (Array.isArray(targetField.options)) {
        for (const opt of targetField.options) {
          const o = document.createElement('option');
          o.value = opt.value; o.textContent = opt.label; if (opt.disabled) o.disabled = true; sel.appendChild(o);
        }
      }
      left.appendChild(lab);
      left.appendChild(sel);
      split.appendChild(left);
      const invalidate = () => setRunEnabled(false);
      sel.addEventListener('input', invalidate);
      fieldContexts.set(targetField.name, { field: targetField, element: sel });
      if (targetField.searchable || targetField.checkboxes || targetField.showPills) {
        queueMicrotask(() => buildEnhancedFromSelect(sel, targetField));
      }
      if (targetField.loadOptions && targetField.autoLoad !== false) {
        populateOptions(targetField, sel, { preserveSelection: true });
      }
    }

    // Middle: Arrow
    const mid = document.createElement('div');
    mid.setAttribute('aria-hidden', 'true');
    mid.style.alignSelf = 'center';
    mid.style.fontSize = '28px';
    mid.style.opacity = '0.6';
    mid.style.textAlign = 'center';
    mid.style.padding = '10px 0';
    mid.textContent = '→';
    split.appendChild(mid);

    // Right: Replacement bundle (single select)
    if (replacementField) {
      const right = document.createElement('div');
      right.className = 'field';
      const lab = document.createElement('label');
      lab.textContent = replacementField.label + (replacementField.required ? ' *' : '');
      lab.htmlFor = `f_${replacementField.name}`;
      const sel = document.createElement('select');
      sel.id = `f_${replacementField.name}`;
      if (Array.isArray(replacementField.options)) {
        for (const opt of replacementField.options) {
          const o = document.createElement('option');
          o.value = opt.value; o.textContent = opt.label; if (opt.disabled) o.disabled = true; sel.appendChild(o);
        }
      }
      right.appendChild(lab);
      right.appendChild(sel);
      split.appendChild(right);
      const invalidate = () => setRunEnabled(false);
      sel.addEventListener('input', invalidate);
      fieldContexts.set(replacementField.name, { field: replacementField, element: sel });
      if (replacementField.searchable || replacementField.checkboxes || replacementField.showPills) {
        queueMicrotask(() => buildEnhancedFromSelect(sel, replacementField));
      }
      if (replacementField.loadOptions && replacementField.autoLoad !== false) {
        populateOptions(replacementField, sel, { preserveSelection: true });
      }
    }

    frag.appendChild(split);

    // Advanced settings collapsed by default
    const details = document.createElement('details');
    const summary = document.createElement('summary');
    summary.textContent = 'Advanced settings';
    details.appendChild(summary);
    const advStack = document.createElement('div');
    advStack.className = 'stack';
    advStack.style.marginTop = '10px';

    for (const f of script.fields) {
      if (f.name === 'fetchBundles' || f.name === 'targetBundles' || f.name === 'replacementBundle') continue;
      const wrap = document.createElement('div');
      wrap.style.marginBottom = '8px';
      const label = document.createElement('label');
      label.textContent = f.label + (f.required ? ' *' : '');
      label.htmlFor = `f_${f.name}`;
      wrap.appendChild(label);
      let el;
      switch (f.type) {
        case 'textarea': el = document.createElement('textarea'); break;
        case 'select':
        case 'multiselect': {
          el = document.createElement('select');
          if (f.type === 'multiselect') { el.multiple = true; el.dataset.multiselect = '1'; }
          for (const opt of f.options || []) { const o = document.createElement('option'); o.value = opt.value; o.textContent = opt.label; if (opt.disabled) o.disabled = true; el.appendChild(o); }
          if (f.searchable || f.checkboxes || f.showPills) { queueMicrotask(() => buildEnhancedFromSelect(el, f)); }
          break;
        }
        case 'checkbox': el = document.createElement('input'); el.type = 'checkbox'; break;
        case 'number': el = document.createElement('input'); el.type = 'number'; break;
        case 'button': {
          el = document.createElement('button');
          el.type = 'button';
          el.textContent = f.label || 'Action';
          el.dataset.buttonField = '1';
          label.textContent = '';
          label.style.display = 'none';
          break;
        }
        default: el = document.createElement('input'); el.type = 'text';
      }
      el.id = `f_${f.name}`;
      if (f.placeholder && el.tagName !== 'BUTTON') el.placeholder = f.placeholder;
      if (f.type === 'checkbox') el.checked = !!f.default;
      else if (Array.isArray(f.default) && el.dataset.multiselect === '1') {
        const defaults = f.default.map(String);
        for (const opt of el.options) opt.selected = defaults.includes(opt.value);
      } else if (f.default != null && f.type !== 'textarea' && el.tagName !== 'BUTTON') el.value = String(f.default);
      wrap.appendChild(el);
      advStack.appendChild(wrap);
      // Invalidate preview on change
      const evt = f.type === 'checkbox' ? 'change' : 'input';
      el.addEventListener(evt, () => setRunEnabled(false));
      if (f.loadOptions) {
        fieldContexts.set(f.name, { field: f, element: el });
        if (f.autoLoad !== false) populateOptions(f, el, { preserveSelection: true });
      }
      if (f.type === 'button' && typeof f.onClick === 'function') {
        el.addEventListener('click', async () => {
          try { setRunEnabled(false); await f.onClick(context); } catch (e) { log.err(e?.message || String(e)); }
        });
      }
    }
    details.appendChild(advStack);
    frag.appendChild(details);

    container.appendChild(frag);
    // Wire up reloadOn dependencies
    for (const f of script.fields) {
      if (!Array.isArray(f.reloadOn) || !f.reloadOn.length) continue;
      const targetEl = byId(`f_${f.name}`);
      if (!targetEl) continue;
      const handler = () => populateOptions(f, targetEl, { preserveSelection: false });
      for (const dep of f.reloadOn) {
        const depEl = byId(`f_${dep}`);
        if (!depEl) continue;
        const evt = depEl.type === 'checkbox' ? 'change' : 'input';
        depEl.addEventListener(evt, handler);
      }
    }
    return;
  }
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
      case 'multiselect': {
        el = document.createElement('select');
        if (f.type === 'multiselect') {
          el.multiple = true;
          el.dataset.multiselect = '1';
        }
        for (const opt of f.options || []) {
          const o = document.createElement('option');
          o.value = opt.value;
          o.textContent = opt.label;
          if (opt.disabled) o.disabled = true;
          el.appendChild(o);
        }
        // Enhance select UI if requested
        if (f.searchable || f.checkboxes || f.showPills) {
          // Build enhanced UI after the element is in the DOM
          queueMicrotask(() => buildEnhancedFromSelect(el, f));
        }
        break;
      }
      case 'checkbox':
        el = document.createElement('input'); el.type = 'checkbox'; break;
      case 'number':
        el = document.createElement('input'); el.type = 'number'; break;
      case 'button':
        el = document.createElement('button');
        el.type = 'button';
        el.textContent = f.label || 'Action';
        el.dataset.buttonField = '1';
        label.textContent = '';
        label.style.display = 'none';
        break;
      default:
        el = document.createElement('input'); el.type = 'text';
    }
    el.id = `f_${f.name}`;
    if (f.placeholder && el.tagName !== 'BUTTON') el.placeholder = f.placeholder;
    if (f.type === 'checkbox') el.checked = !!f.default;
    else if (Array.isArray(f.default) && el.dataset.multiselect === '1') {
      const defaults = f.default.map(String);
      for (const opt of el.options) opt.selected = defaults.includes(opt.value);
    }
    else if (f.default != null && f.type !== 'textarea' && el.tagName !== 'BUTTON') el.value = String(f.default);
    wrap.appendChild(el);
    frag.appendChild(wrap);
    // Any change to a script field invalidates prior preview
    const invalidate = () => setRunEnabled(false);
    const evt = f.type === 'checkbox' ? 'change' : 'input';
    el.addEventListener(evt, invalidate);
    if (f.loadOptions) {
      fieldContexts.set(f.name, { field: f, element: el });
      if (f.autoLoad !== false) {
        populateOptions(f, el, { preserveSelection: true });
      }
    }
    if (f.type === 'button' && typeof f.onClick === 'function') {
      el.addEventListener('click', async () => {
        try {
          setRunEnabled(false);
          await f.onClick(context);
        } catch (e) {
          log.err(e?.message || String(e));
        }
      });
    }
  }
  container.appendChild(frag);
  for (const f of script.fields) {
    if (!Array.isArray(f.reloadOn) || !f.reloadOn.length) continue;
    const targetEl = byId(`f_${f.name}`);
    if (!targetEl) continue;
    const handler = () => populateOptions(f, targetEl, { preserveSelection: false });
    for (const dep of f.reloadOn) {
      const depEl = byId(`f_${dep}`);
      if (!depEl) continue;
      const evt = depEl.type === 'checkbox' ? 'change' : 'input';
      depEl.addEventListener(evt, handler);
    }
  }
}

function readForm(script) {
  const out = {};
  for (const f of script.fields) {
    if (f.type === 'button') continue;
    const el = byId(`f_${f.name}`);
    if (!el) continue;
    let v;
    if (el.dataset.multiselect === '1') v = Array.from(el.selectedOptions || []).map((opt) => opt.value);
    else if (f.type === 'checkbox') v = !!el.checked;
    else if (f.type === 'number') v = Number(el.value);
    else v = el.value;
    if (f.required) {
      const empty = el.dataset.multiselect === '1' ? (Array.isArray(v) && v.length === 0) : (v === '' || v == null || Number.isNaN(v));
      if (empty) throw new Error(`Field required: ${f.label}`);
    }
    out[f.name] = v;
  }
  return out;
}

registerPermissionReassign({
  registerScripts,
  resolveUrl,
  fetchWithBackoff,
  log,
  state,
});

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
    clearRunDetail();
    setRunSummary('Running preview…');
    const sLog = createScriptLog({ mirrorLevels: ['warn', 'err'] });
    const t0 = performance.now();
    const prev = await script.preview(cfg, { request: proxyRequest, resolveUrl, fetchWithBackoff, state, log: sLog });
    const items = Array.isArray(prev) ? prev : (prev?.items || []);
    const scanned = typeof prev?.scanned === 'number' ? prev.scanned : undefined;
    const t1 = performance.now();
    const ms = Math.round(t1 - t0);
    const count = Array.isArray(items) ? items.length : 0;
    const scannedPart = scanned != null ? `${scanned} users scanned. ` : '';
    setRunSummary(`${scannedPart}${count} user${count === 1 ? '' : 's'} have target permission bundle(s) to be updated. (${ms} ms)`);
    // Also print a friendly, user-focused summary list for the first N items
    if (Array.isArray(items) && items.length) {
      const header = 'What will change:';
      const lines = [];
      const sample = items.slice(0, 50);
      for (const item of sample) {
        const who = item.email || item.name || `User ${item.id}`;
        const current = Array.isArray(item.summary?.current) ? item.summary.current.join(', ') : 'none';
        const next = Array.isArray(item.summary?.next) ? item.summary.next.join(', ') : 'none';
        lines.push(`• ${who}\n    Current: ${current}\n    After:   ${next}`);
      }
      if (items.length > sample.length) lines.push(`… and ${items.length - sample.length} more users`);
      appendRunDetail([header, '', ...lines].join('\n'));
    }
    // Preview succeeded; enable run
    setRunEnabled(true);
  } catch (e) { log.err(e?.message || String(e)); }
}

async function onRun() {
  const script = getCurrentScript(); if (!script) return;
  const cfg = readForm(script);
  log.info(`Planning…`);
  let items = [];
  try {
    clearRunDetail();
    setRunSummary('Planning…');
    const sLog = createScriptLog({ mirrorLevels: ['warn', 'err'] });
    const t0 = performance.now();
    const planRes = await script.plan(cfg, { request: proxyRequest, resolveUrl, fetchWithBackoff, state, log: sLog });
    const scanned = typeof planRes?.scanned === 'number' ? planRes.scanned : undefined;
    items = Array.isArray(planRes) ? planRes : (planRes?.items || []);
    const t1 = performance.now();
    const ms = Math.round(t1 - t0);
    const scannedPart = scanned != null ? `${scanned} users scanned. ` : '';
    setRunSummary(`${scannedPart}${items.length} item${items.length === 1 ? '' : 's'} planned in ${ms} ms.`);
  }
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
  const sLogExec = createScriptLog({ mirrorLevels: ['warn', 'err'] });
  const tStart = performance.now();
  const perItem = (item) => script.execute(item, cfg, { request: proxyRequest, resolveUrl, fetchWithBackoff, signal: currentController.signal, state, log: sLogExec });
  try {
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, i + chunkSize);
      await runBatches(chunk, perItem, { concurrency: conc, signal: currentController.signal, onProgress });
      if (currentController.signal.aborted) throw new DOMException('Aborted', 'AbortError');
    }
    const tEnd = performance.now();
    const ms = Math.round(tEnd - tStart);
    log.ok('Run completed');
    setRunSummary(`Run completed in ${ms} ms. Processed ${progress.done}, success ${progress.ok}, failed ${progress.fail}.`);
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
  byId('cfg_save').addEventListener('click', () => { setRunEnabled(false); saveCfg(); });
  byId('cfg_clear').addEventListener('click', () => { setRunEnabled(false); clearCfg(); });
  byId('log_download').addEventListener('click', downloadLogs);
  const tokenEl = byId('cfg_token');
  const toggleBtn = byId('cfg_token_toggle');
  if (tokenEl && toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      const isHidden = tokenEl.type === 'password';
      tokenEl.type = isHidden ? 'text' : 'password';
      toggleBtn.textContent = isHidden ? 'Hide' : 'Show';
      toggleBtn.setAttribute('aria-pressed', isHidden ? 'true' : 'false');
      try {
        tokenEl.focus({ preventScroll: true });
      } catch {
        tokenEl.focus();
      }
      if (tokenEl.type === 'text' && typeof tokenEl.setSelectionRange === 'function') {
        const pos = tokenEl.value.length;
        try {
          tokenEl.setSelectionRange(pos, pos);
        } catch {}
      }
    });
  }

  byId('c_send').addEventListener('click', handleSend);
  byId('s_pick').addEventListener('change', () => { setRunEnabled(false); renderScriptForm(getCurrentScript()); });
  byId('s_preview').addEventListener('click', onPreview);
  byId('s_run').addEventListener('click', onRun);
  byId('s_cancel').addEventListener('click', onCancel);

  // Invalidate run when connection settings change
  const cfgIds = ['cfg_base','cfg_token','cfg_persist','cfg_conc','cfg_chunk'];
  for (const id of cfgIds) {
    const el = byId(id);
    if (!el) continue;
    const ev = el.type === 'checkbox' ? 'change' : 'input';
    el.addEventListener(ev, () => setRunEnabled(false));
  }

  // Tabs
  const tabs = [
    { btn: byId('tab_conn'), panel: byId('panel_conn') },
    { btn: byId('tab_console'), panel: byId('panel_console') },
    { btn: byId('tab_scripts'), panel: byId('panel_scripts') },
  ];
  function activateTab(targetBtn) {
    for (const { btn, panel } of tabs) {
      const active = btn === targetBtn;
      btn.setAttribute('aria-selected', active ? 'true' : 'false');
      if (active) {
        panel.removeAttribute('hidden');
        panel.setAttribute('tabindex', '0');
      } else {
        panel.setAttribute('hidden', '');
        panel.setAttribute('tabindex', '-1');
      }
    }
  }
  for (const { btn } of tabs) {
    if (!btn) continue;
    btn.addEventListener('click', () => activateTab(btn));
    btn.addEventListener('keydown', (e) => {
      // Arrow key navigation between tabs
      if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
      e.preventDefault();
      const idx = tabs.findIndex(t => t.btn === btn);
      if (idx === -1) return;
      const nextIdx = e.key === 'ArrowRight' ? (idx + 1) % tabs.length : (idx - 1 + tabs.length) % tabs.length;
      const nextBtn = tabs[nextIdx].btn;
      nextBtn.focus();
      activateTab(nextBtn);
    });
  }

  // Initialize JSON editors (headers, body)
  initJsonEditor('c_headers');
  initJsonEditor('c_body');
  // Start with Run disabled until preview
  setRunEnabled(false);
}

// Init
window.addEventListener('DOMContentLoaded', () => {
  loadCfg();
  bindUI();
  renderScriptPicker();
  byId('c_method').value = 'GET';
  // Seed placeholders for JSON editors if empty
  const headersEl = byId('c_headers');
  const bodyEl = byId('c_body');
  const headersEd = editors['c_headers'];
  const bodyEd = editors['c_body'];
  try {
    if (headersEl && headersEd && !headersEd.get()) headersEd.set(headersEl.dataset.placeholder || '');
  } catch {}
  try {
    if (bodyEl && bodyEd && !bodyEd.get()) bodyEd.set(bodyEl.dataset.placeholder || '');
  } catch {}
  log.info('Ready');
  // Ensure default tab is active
  const defaultTab = byId('tab_conn');
  if (defaultTab) defaultTab.click();
});

// Enable/disable Run button helper
function setRunEnabled(enabled) {
  try {
    const btn = byId('s_run');
    if (btn) btn.disabled = !enabled;
  } catch {}
}

