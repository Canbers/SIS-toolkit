const scriptCaches = {
  bundles: { key: null, data: [], fetchedAt: 0 },
};

/**
 * Factory returning the permission reassignment script definition.
 * @param {{registerScripts:(scripts:any[])=>void, resolveUrl:Function, fetchWithBackoff:Function, log:any}} api
 */
export function registerPermissionReassign(api) {
  const { registerScripts, resolveUrl, fetchWithBackoff, log } = api;
  if (typeof registerScripts !== 'function') throw new Error('registerScripts required');
  const script = buildPermissionReassignScript({ resolveUrl, fetchWithBackoff, log });
  registerScripts([script]);
}

function buildPermissionReassignScript({ resolveUrl, fetchWithBackoff, log }) {
  const pickLog = (ctx) => (ctx && ctx.log) ? ctx.log : log;
  const loadBundles = async (ctx) => {
    const cfg = parseConfig({
      targetBundles: ctx.getFieldValue('targetBundles'),
      replacementBundle: ctx.getFieldValue('replacementBundle'),
      bundlesPath: ctx.getFieldValue('bundlesPath') || '/bundles',
      limit: ctx.getFieldValue('limit') || 0,
      pageSize: ctx.getFieldValue('pageSize') || 50,
      listPath: ctx.getFieldValue('listPath') || '/users',
      userUpdatePath: ctx.getFieldValue('userUpdatePath') || '/users/{id}',
      stopOnError: ctx.getFieldValue('stopOnError'),
      skipIfHasReplacement: ctx.getFieldValue('skipIfHasReplacement'),
    }, { loose: true });
    const bundles = await ensureBundlesCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
    return bundles
      .map((bundle) => ({ value: String(bundle.id), label: bundleLabelFromBundle(bundle) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  };

  const fetchAndPopulate = async (ctx) => {
    const L = pickLog(ctx);
    try {
      await ctx.reloadFieldOptions('targetBundles', { preserveSelection: true });
      await ctx.reloadFieldOptions('replacementBundle', { preserveSelection: false });
      L.ok?.('Permission bundles refreshed');
    } catch (e) {
      L.err?.(e?.message || String(e));
    }
  };

  return {
    id: 'perm_reassign',
    name: 'Users: reassign permission bundles',
    fields: [
      {
        type: 'button',
        name: 'fetchBundles',
        label: 'Fetch permission bundles',
        onClick: fetchAndPopulate,
      },
      {
        type: 'multiselect',
        name: 'targetBundles',
        label: 'Target bundles',
        required: true,
        placeholder: 'Select permission bundles to replace',
        searchable: true,
        checkboxes: true,
        showPills: true,
        loadOptions: loadBundles,
        autoLoad: false,
      },
      {
        type: 'select',
        name: 'replacementBundle',
        label: 'Replacement bundle',
        required: true,
        placeholder: 'Select replacement bundle',
        searchable: true,
        loadOptions: loadBundles,
        autoLoad: false,
      },
      {
        type: 'number',
        name: 'limit',
        label: 'Process max users (0 = all)',
        default: 0,
      },
      {
        type: 'number',
        name: 'pageSize',
        label: 'Page size',
        default: 50,
      },
      {
        type: 'text',
        name: 'listPath',
        label: 'Users list path',
        default: '/users',
        required: true,
      },
      {
        type: 'text',
        name: 'bundlesPath',
        label: 'Bundles list path',
        default: '/bundles',
        required: true,
      },
      {
        type: 'text',
        name: 'userUpdatePath',
        label: 'User update path',
        default: '/users/{id}',
        required: true,
      },
      {
        type: 'checkbox',
        name: 'stopOnError',
        label: 'Stop on first error',
        default: false,
      },
      {
        type: 'checkbox',
        name: 'skipIfHasReplacement',
        label: 'Skip users already assigned replacement bundle',
        default: true,
      },
    ],
    async preview(cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      if (!parsed.replacementBundle || !parsed.targetBundles.length) {
        throw new Error('Fetch bundles and select target/replacement before previewing.');
      }
      const { items, scanned } = await collectMatchingUsers(parsed, ctx, { fetchWithBackoff, resolveUrl, log: L });
      const limitNote = parsed.limit && scanned > parsed.limit ? ` (limited to ${parsed.limit})` : '';
      L.info?.(`Preview: ${items.length} users queued${limitNote}; scanned ${scanned}`);
      if (!items.length) {
        L.info?.('No users match the selected bundles.');
      }
      return { items, scanned };
    },
    async plan(cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      if (!parsed.replacementBundle || !parsed.targetBundles.length) {
        throw new Error('Fetch bundles and select target/replacement before planning.');
      }
      const { items, scanned } = await collectMatchingUsers(parsed, ctx, { fetchWithBackoff, resolveUrl, log: L });
      L.ok?.(`Plan ready: ${items.length} users queued (${scanned} scanned${parsed.limit && scanned > parsed.limit ? `, limited to ${parsed.limit}` : ''})`);
      return { items, scanned };
    },
    async execute(item, cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      if (!parsed.replacementBundle || !parsed.targetBundles.length) {
        throw new Error('Bundles not configured. Fetch bundles and configure fields before executing.');
      }
      const url = buildUserUrl(parsed.userUpdatePath, item.id, resolveUrl);
      const body = buildUserUpdatePayload(item);
      const res = await fetchWithBackoff(() => ctx.request({
        method: 'PUT',
        url,
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: ctx.signal,
      }), 4, { signal: ctx.signal });
      if (res.status >= 200 && res.status < 300) {
        L.ok?.(`Updated ${item.id}`);
      } else {
        const msg = `HTTP ${res.status}`;
        if (parsed.stopOnError) throw new Error(msg);
        L.err?.(`Update failed for ${item.id}: ${msg}`);
      }
    },
  };
}

function parseConfig(raw, options = {}) {
  const loose = !!options.loose;
  const targetBundles = uniqueNumbers(splitIds(raw.targetBundles));
  const replacementRaw = Array.isArray(raw.replacementBundle) ? raw.replacementBundle[0] : raw.replacementBundle;
  const replacementBundle = Number(replacementRaw);
  if (!targetBundles.length && !loose) throw new Error('Provide at least one target bundle ID');
  if ((!Number.isFinite(replacementBundle) || replacementBundle <= 0) && !loose) throw new Error('Replacement bundle ID required');
  return {
    targetBundles,
    replacementBundle: Number.isFinite(replacementBundle) && replacementBundle > 0 ? replacementBundle : (loose ? 0 : replacementBundle),
    limit: Math.max(0, Number(raw.limit) || 0),
    pageSize: Math.max(1, Number(raw.pageSize) || 50),
    listPath: raw.listPath || '/users',
    bundlesPath: raw.bundlesPath || '/bundles',
    userUpdatePath: raw.userUpdatePath || '/users/{id}',
    stopOnError: !!raw.stopOnError,
    skipIfHasReplacement: raw.skipIfHasReplacement !== false,
  };
}

function splitIds(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  return String(value)
    .split(/[\s,]+/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function uniqueNumbers(list) {
  const out = [];
  const seen = new Set();
  for (const value of list || []) {
    const num = Number(value);
    if (!Number.isFinite(num)) continue;
    if (seen.has(num)) continue;
    seen.add(num);
    out.push(num);
  }
  return out.sort((a, b) => a - b);
}

async function ensureBundlesCached(cfg, ctx, { fetchWithBackoff, resolveUrl }) {
  const path = cfg.bundlesPath || '/bundles';
  const url = resolveUrl(path);
  if (scriptCaches.bundles.key === url && scriptCaches.bundles.data.length) return scriptCaches.bundles.data;
  const bundles = [];
  let offset = 0;
  const guardLimit = 500;
  let guard = 0;
  while (true) {
    const pageUrl = buildUrlWithParams(url, { limit: 100, offset });
    const res = await fetchWithBackoff(() => ctx.request({ method: 'GET', url: pageUrl }), 4, {});
    if (res.status >= 400) throw new Error(`Bundles request failed (${res.status})`);
    const body = res.body || {};
    const page = Array.isArray(body.bundles) ? body.bundles : Array.isArray(body.items) ? body.items : [];
    bundles.push(...page);
    const pagination = body.pagination || {};
    if (!page.length || pagination.next_offset == null) break;
    offset = Number(pagination.next_offset);
    if (!Number.isFinite(offset)) break;
    guard += 1;
    if (guard > guardLimit) throw new Error('Bundle pagination exceeded guard threshold');
  }
  scriptCaches.bundles = { key: url, data: bundles, fetchedAt: Date.now() };
  return bundles;
}

async function fetchUsersPage(cfg, ctx, { fetchWithBackoff, resolveUrl }, { offset }) {
  const baseUrl = resolveUrl(cfg.listPath || '/users');
  const url = buildUrlWithParams(baseUrl, { limit: cfg.pageSize, offset });
  const res = await fetchWithBackoff(() => ctx.request({ method: 'GET', url }), 4, {});
  if (res.status >= 400) throw new Error(`Users request failed (${res.status})`);
  const body = res.body || {};
  const items = Array.isArray(body.users)
    ? body.users
    : Array.isArray(body.items)
      ? body.items
      : [];
  const pagination = body.pagination || {};
  const nextOffsetRaw = pagination.next_offset;
  const nextOffset = nextOffsetRaw == null ? null : Number(nextOffsetRaw);
  const total = pagination.total_records ?? pagination.total ?? body.total ?? body.summarization?.all_users ?? null;
  return {
    items,
    nextOffset: Number.isFinite(nextOffset) ? nextOffset : null,
    total,
  };
}

async function collectMatchingUsers(cfg, ctx, { fetchWithBackoff, resolveUrl, log }) {
  const bundles = await ensureBundlesCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
  const bundleMap = new Map();
  for (const bundle of bundles) bundleMap.set(Number(bundle.id), bundle);
  const targetSet = new Set(cfg.targetBundles || []);
  const items = [];
  let offset = 0;
  let scanned = 0;
  let guard = 0;
  const guardLimit = 500;
  while (true) {
    const page = await fetchUsersPage(cfg, ctx, { fetchWithBackoff, resolveUrl }, { offset });
    const users = page.items || [];
    if (!users.length && guard > 0) break;
    for (const user of users) {
      scanned += 1;
      const currentBundles = extractBundleIds(user);
      const hasTarget = currentBundles.some((id) => targetSet.has(id));
      const hasReplacement = Number.isFinite(cfg.replacementBundle) && cfg.replacementBundle > 0 && currentBundles.includes(cfg.replacementBundle);
      if (cfg.skipIfHasReplacement && hasReplacement && !hasTarget) continue;
      if (!hasTarget) continue;
      if (!cfg.replacementBundle) continue;
      const nextBundleIds = computeNextBundleIds(currentBundles, targetSet, cfg.replacementBundle);
      if (arraysEqual(currentBundles, nextBundleIds)) continue;
      items.push({
        id: user.id,
        email: user.email,
        name: `${user.first_name || ''} ${user.last_name || ''}`.trim(),
        originalUser: user,
        currentBundleIds: currentBundles,
        nextBundleIds,
        summary: {
          current: currentBundles.map((id) => bundleLabelFromMap(bundleMap, id)),
          next: nextBundleIds.map((id) => bundleLabelFromMap(bundleMap, id)),
        },
      });
      if (cfg.limit && items.length >= cfg.limit) return { items, scanned };
    }
    guard += 1;
    // Prefer API-provided next_offset; otherwise compute via total and pageSize
    if (page.nextOffset != null) {
      offset = page.nextOffset;
    } else {
      const total = Number(page.total);
      const size = Math.max(1, Number(cfg.pageSize) || 50);
      const next = offset + size;
      if (Number.isFinite(total)) {
        if (next >= total) break;
        offset = next;
      } else {
        // Fallback: advance by page size until we hit an empty page
        if (!users.length) break;
        offset = next;
      }
    }
    if (guard > guardLimit) {
      log.warn?.('User pagination reached guard threshold; stopping early');
      break;
    }
  }
  return { items, scanned };
}

function buildUserUrl(pathTemplate, userId, resolveUrl) {
  const path = (pathTemplate || '/users/{id}').replace('{id}', encodeURIComponent(userId));
  return resolveUrl(path);
}

function buildUserUpdatePayload(item) {
  const user = item?.originalUser || {};
  const payloadUser = {
    id: user.id ?? item.id,
    permission_bundles: (item.nextBundleIds || []).map(Number),
  };
  if (user.first_name != null) payloadUser.first_name = user.first_name;
  if (user.last_name != null) payloadUser.last_name = user.last_name;
  if (user.email != null) payloadUser.email = user.email;
  if (Array.isArray(user.user_groups)) {
    payloadUser.user_groups = user.user_groups
      .filter((g) => g && g.id != null)
      .map((g) => ({ id: g.id, name: g.name }));
  }
  if (Array.isArray(user.locations)) {
    payloadUser.location_ids = user.locations
      .map((loc) => loc?.id)
      .filter((id) => id != null);
  }
  return { user: payloadUser };
}

function extractBundleIds(user) {
  const ids = [];
  if (user?.permission_bundles) {
    for (const entry of user.permission_bundles) {
      if (!entry && entry !== 0) continue;
      if (typeof entry === 'number' || typeof entry === 'string') ids.push(Number(entry));
      else if (typeof entry?.id !== 'undefined') ids.push(Number(entry.id));
    }
  }
  if (user?.permission_bundle?.id != null) ids.push(Number(user.permission_bundle.id));
  return uniqueNumbers(ids);
}

function computeNextBundleIds(current, targets, replacement) {
  const targetSet = targets instanceof Set ? targets : new Set(targets || []);
  const next = [];
  for (const raw of current || []) {
    const id = Number(raw);
    if (!Number.isFinite(id)) continue;
    if (!targetSet.has(id)) next.push(id);
  }
  const replacementId = Number(replacement);
  if (Number.isFinite(replacementId) && replacementId > 0 && !next.includes(replacementId)) {
    next.push(replacementId);
  }
  return uniqueNumbers(next);
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function buildUrlWithParams(url, params) {
  const u = new URL(url, window.location.origin);
  for (const [key, value] of Object.entries(params || {})) {
    if (value == null) continue;
    u.searchParams.set(key, String(value));
  }
  return u.toString();
}

function bundleLabelFromBundle(bundle) {
  if (!bundle) return 'Unknown bundle';
  const name = bundle.bundle_name || bundle.name || 'Bundle';
  return `${name} (#${bundle.id})`;
}

function bundleLabelFromMap(map, id) {
  return bundleLabelFromBundle(map.get(Number(id))) || `#${id}`;
}

export default registerPermissionReassign;

