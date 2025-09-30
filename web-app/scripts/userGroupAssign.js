const scriptCaches = {
  bundles: { key: null, data: [], fetchedAt: 0 },
  locations: { key: null, data: [], fetchedAt: 0 },
  groups: { key: null, data: [], fetchedAt: 0 },
};

/**
 * Factory returning the user-group assignment script definition.
 * Users can be targeted by permission bundles OR by locations; the selected
 * user group will be added to each targeted user.
 * @param {{registerScripts:(scripts:any[])=>void, resolveUrl:Function, fetchWithBackoff:Function, log:any}} api
 */
export function registerUserGroupAssign(api) {
  const { registerScripts, resolveUrl, fetchWithBackoff, log } = api;
  if (typeof registerScripts !== 'function') throw new Error('registerScripts required');
  const script = buildUserGroupAssignScript({ resolveUrl, fetchWithBackoff, log });
  registerScripts([script]);
}

function buildUserGroupAssignScript({ resolveUrl, fetchWithBackoff, log }) {
  const pickLog = (ctx) => (ctx && ctx.log) ? ctx.log : log;

  const loadBundles = async (ctx) => {
    const cfg = parseConfig({
      bundlesPath: ctx.getFieldValue('bundlesPath') || '/bundles',
      listPath: ctx.getFieldValue('listPath') || '/users',
      pageSize: ctx.getFieldValue('pageSize') || 50,
    }, { loose: true });
    const bundles = await ensureBundlesCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
    return bundles
      .map((bundle) => ({ value: String(bundle.id), label: bundleLabelFromBundle(bundle) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  };

  const loadLocations = async (ctx) => {
    const cfg = parseConfig({
      locationsPath: ctx.getFieldValue('locationsPath') || '/api/v3/locations',
    }, { loose: true });
    const locations = await ensureLocationsCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
    return locations
      .map((loc) => ({ value: String(loc.id), label: locationLabelFromLocation(loc) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  };

  const loadGroups = async (ctx) => {
    const cfg = parseConfig({
      groupsPath: ctx.getFieldValue('groupsPath') || '/user_groups',
    }, { loose: true });
    const groups = await ensureGroupsCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
    return groups
      .map((g) => ({ value: String(g.id), label: groupLabelFromGroup(g) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  };

  const fetchAndPopulate = async (ctx) => {
    const L = pickLog(ctx);
    try {
      await Promise.all([
        ctx.reloadFieldOptions('targetBundles', { preserveSelection: true }),
        ctx.reloadFieldOptions('targetLocations', { preserveSelection: true }),
        ctx.reloadFieldOptions('groupId', { preserveSelection: false }),
      ]);
      L.ok?.('Options refreshed (bundles, locations, groups)');
    } catch (e) {
      L.err?.(e?.message || String(e));
    }
  };

  return {
    id: 'user_group_assign',
    name: 'Users: assign user group (by bundles or locations)',
    fields: [
      {
        type: 'button',
        name: 'refreshOptions',
        label: 'Fetch bundles/locations/groups',
        onClick: fetchAndPopulate,
      },
      {
        type: 'select',
        name: 'mode',
        label: 'Targeting mode',
        required: true,
        default: 'bundles',
        placeholder: 'Choose how to target users',
        options: [
          { value: 'bundles', label: 'By permission bundles' },
          { value: 'locations', label: 'By locations' },
        ],
      },
      {
        type: 'multiselect',
        name: 'targetBundles',
        label: 'Target bundles',
        placeholder: 'Select permission bundles to target',
        searchable: true,
        checkboxes: true,
        showPills: true,
        loadOptions: loadBundles,
        autoLoad: false,
      },
      {
        type: 'multiselect',
        name: 'targetLocations',
        label: 'Target locations',
        placeholder: 'Select locations to target',
        searchable: true,
        checkboxes: true,
        showPills: true,
        loadOptions: loadLocations,
        autoLoad: false,
      },
      {
        type: 'select',
        name: 'groupId',
        label: 'User group to assign',
        required: true,
        placeholder: 'Select a user group',
        searchable: true,
        loadOptions: loadGroups,
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
        name: 'locationsPath',
        label: 'Locations list path',
        default: '/api/v3/locations',
        required: true,
      },
      {
        type: 'text',
        name: 'groupsPath',
        label: 'User groups base path',
        default: '/user_groups',
        required: true,
      },
      {
        type: 'checkbox',
        name: 'stopOnError',
        label: 'Stop on first error',
        default: false,
      },
    ],
    async preview(cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      validateTargets(parsed);
      const { items, scanned } = await collectMatchingUsers(parsed, ctx, { fetchWithBackoff, resolveUrl, log: L });
      const limitNote = parsed.limit && scanned > parsed.limit ? ` (limited to ${parsed.limit})` : '';
      L.info?.(`Preview: ${items.length} users queued${limitNote}; scanned ${scanned}`);
      if (!items.length) L.info?.('No users match the selected criteria.');
      return { items, scanned };
    },
    async plan(cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      validateTargets(parsed);
      const { items, scanned } = await collectMatchingUsers(parsed, ctx, { fetchWithBackoff, resolveUrl, log: L });
      L.ok?.(`Plan ready: ${items.length} users queued (${scanned} scanned${parsed.limit && scanned > parsed.limit ? `, limited to ${parsed.limit}` : ''})`);
      // Return a single batch item instead of individual items
      return { 
        items: items.length > 0 ? [{ userIds: items.map(u => u.id), users: items }] : [],
        scanned 
      };
    },
    async execute(batchItem, cfg, ctx) {
      const L = pickLog(ctx);
      const parsed = parseConfig(cfg);
      validateTargets(parsed);
      const userIds = batchItem.userIds || [];
      if (!userIds.length) {
        L.info?.('No users to add to group.');
        return;
      }
      const url = buildGroupUsersUrl(parsed.groupsPath, parsed.groupId, resolveUrl);
      const body = { members: userIds };
      L.info?.(`Adding ${userIds.length} user${userIds.length === 1 ? '' : 's'} to group ${parsed.groupId}...`);
      const res = await fetchWithBackoff(() => ctx.request({
        method: 'POST',
        url,
        headers: { 'Content-Type': 'application/json' },
        body,
        signal: ctx.signal,
      }), 4, { signal: ctx.signal });
      if (res.status >= 200 && res.status < 300) {
        L.ok?.(`Successfully added ${userIds.length} user${userIds.length === 1 ? '' : 's'} to group`);
      } else {
        const msg = `HTTP ${res.status}`;
        L.err?.(`Failed to add users to group: ${msg}`);
        throw new Error(msg);
      }
    },
  };
}

function parseConfig(raw, options = {}) {
  const loose = !!options.loose;
  const mode = (raw.mode === 'locations' || raw.mode === 'bundles') ? raw.mode : 'bundles';
  const targetBundles = uniqueNumbers(splitIds(raw.targetBundles));
  const targetLocations = uniqueNumbers(splitIds(raw.targetLocations));
  const groupRaw = Array.isArray(raw.groupId) ? raw.groupId[0] : raw.groupId;
  const groupId = Number(groupRaw);
  if ((!Number.isFinite(groupId) || groupId <= 0) && !loose) throw new Error('User group ID required');
  return {
    mode,
    targetBundles,
    targetLocations,
    groupId: Number.isFinite(groupId) && groupId > 0 ? groupId : (loose ? 0 : groupId),
    limit: Math.max(0, Number(raw.limit) || 0),
    pageSize: Math.max(1, Number(raw.pageSize) || 50),
    listPath: raw.listPath || '/users',
    bundlesPath: raw.bundlesPath || '/bundles',
    locationsPath: raw.locationsPath || '/api/v3/locations',
    groupsPath: raw.groupsPath || '/user_groups',
    stopOnError: !!raw.stopOnError,
  };
}

function validateTargets(cfg) {
  if (!cfg.groupId) throw new Error('Select a user group to assign.');
  if (cfg.mode === 'bundles' && (!cfg.targetBundles || !cfg.targetBundles.length)) {
    throw new Error('Select at least one target bundle when targeting by bundles.');
  }
  if (cfg.mode === 'locations' && (!cfg.targetLocations || !cfg.targetLocations.length)) {
    throw new Error('Select at least one target location when targeting by locations.');
  }
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

async function ensureLocationsCached(cfg, ctx, { fetchWithBackoff, resolveUrl }) {
  const path = cfg.locationsPath || '/api/v3/locations';
  const url = resolveUrl(path);
  if (scriptCaches.locations.key === url && scriptCaches.locations.data.length) return scriptCaches.locations.data;
  const out = [];
  let offset = 0;
  const guardLimit = 500;
  let guard = 0;
  while (true) {
    const pageUrl = buildUrlWithParams(url, { limit: 100, offset });
    const res = await fetchWithBackoff(() => ctx.request({ method: 'GET', url: pageUrl }), 4, {});
    if (res.status >= 400) throw new Error(`Locations request failed (${res.status})`);
    const body = res.body || {};
    const page = Array.isArray(body.locations) ? body.locations : Array.isArray(body.items) ? body.items : [];
    out.push(...page);
    const pagination = body.pagination || {};
    if (!page.length || pagination.next_offset == null) break;
    offset = Number(pagination.next_offset);
    if (!Number.isFinite(offset)) break;
    guard += 1;
    if (guard > guardLimit) throw new Error('Location pagination exceeded guard threshold');
  }
  scriptCaches.locations = { key: url, data: out, fetchedAt: Date.now() };
  return out;
}

async function ensureGroupsCached(cfg, ctx, { fetchWithBackoff, resolveUrl }) {
  const path = cfg.groupsPath || '/user_groups';
  const url = resolveUrl(path);
  if (scriptCaches.groups.key === url && scriptCaches.groups.data.length) return scriptCaches.groups.data;
  const out = [];
  let offset = 0;
  const guardLimit = 200;
  let guard = 0;
  while (true) {
    const pageUrl = buildUrlWithParams(url, { limit: 100, offset });
    const res = await fetchWithBackoff(() => ctx.request({ method: 'GET', url: pageUrl }), 4, {});
    if (res.status >= 400) throw new Error(`User groups request failed (${res.status})`);
    const body = res.body;
    const page = Array.isArray(body) ? body : Array.isArray(body?.user_groups) ? body.user_groups : Array.isArray(body?.items) ? body.items : [];
    out.push(...page);
    const pagination = body?.pagination || {};
    if (!page.length || pagination.next_offset == null) break;
    offset = Number(pagination.next_offset);
    if (!Number.isFinite(offset)) break;
    guard += 1;
    if (guard > guardLimit) throw new Error('User group pagination exceeded guard threshold');
  }
  scriptCaches.groups = { key: url, data: out, fetchedAt: Date.now() };
  return out;
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
  const groups = await ensureGroupsCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
  const groupMap = new Map();
  for (const g of groups) groupMap.set(Number(g.id), g);
  const targetBundleSet = new Set(cfg.targetBundles || []);
  const targetLocationSet = new Set(cfg.targetLocations || []);

  // Preload option lists for labels used in summaries
  const bundles = await ensureBundlesCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
  const bundleMap = new Map();
  for (const b of bundles) bundleMap.set(Number(b.id), b);
  const locations = await ensureLocationsCached(cfg, ctx, { fetchWithBackoff, resolveUrl });
  const locationMap = new Map();
  for (const loc of locations) locationMap.set(Number(loc.id), loc);

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
      const userBundleIds = extractBundleIds(user);
      const userLocationIds = extractLocationIds(user);
      const userGroupIds = extractGroupIds(user);

      const matchesByBundle = cfg.mode === 'bundles' && userBundleIds.some((id) => targetBundleSet.has(id));
      const matchesByLocation = cfg.mode === 'locations' && userLocationIds.some((id) => targetLocationSet.has(id));
      const isMatch = matchesByBundle || matchesByLocation;
      if (!isMatch) continue;


      const nextGroupIds = computeNextGroupIds(userGroupIds, cfg.groupId);
      if (arraysEqual(userGroupIds, nextGroupIds)) continue;

      items.push({
        id: user.id,
        email: user.email,
        name: `${user.first_name || ''} ${user.last_name || ''}`.trim(),
        originalUser: user,
        currentGroupIds: userGroupIds,
        nextGroupIds,
        summary: {
          match: cfg.mode === 'bundles'
            ? {
                by: 'bundles',
                current: userBundleIds.map((id) => bundleLabelFromMap(bundleMap, id)),
                target: [...targetBundleSet].map((id) => bundleLabelFromMap(bundleMap, id)),
              }
            : {
                by: 'locations',
                current: userLocationIds.map((id) => locationLabelFromMap(locationMap, id)),
                target: [...targetLocationSet].map((id) => locationLabelFromMap(locationMap, id)),
              },
          group: {
            current: userGroupIds.map((id) => groupLabelFromMap(groupMap, id)),
            next: nextGroupIds.map((id) => groupLabelFromMap(groupMap, id)),
          },
        },
      });
      if (cfg.limit && items.length >= cfg.limit) return { items, scanned };
    }
    guard += 1;
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

function buildGroupUsersUrl(groupsPath, groupId, resolveUrl) {
  const basePath = groupsPath || '/user_groups';
  const path = `${basePath}/${encodeURIComponent(groupId)}/users`;
  return resolveUrl(path);
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

function extractLocationIds(user) {
  const ids = [];
  if (Array.isArray(user?.locations)) {
    for (const loc of user.locations) {
      const id = loc?.id;
      if (id == null) continue;
      const num = Number(id);
      if (Number.isFinite(num)) ids.push(num);
    }
  }
  return uniqueNumbers(ids);
}

function extractGroupIds(user) {
  const ids = [];
  if (Array.isArray(user?.user_groups)) {
    for (const g of user.user_groups) {
      const id = g?.id;
      if (id == null) continue;
      const num = Number(id);
      if (Number.isFinite(num)) ids.push(num);
    }
  }
  return uniqueNumbers(ids);
}

function computeNextGroupIds(current, toAdd) {
  const next = new Set(Array.isArray(current) ? current.map(Number) : []);
  const add = Number(toAdd);
  if (Number.isFinite(add) && add > 0) next.add(add);
  return Array.from(next.values()).sort((a, b) => a - b);
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

function locationLabelFromLocation(loc) {
  if (!loc) return 'Unknown location';
  const name = loc.name || 'Location';
  return `${name} (#${loc.id})`;
}

function locationLabelFromMap(map, id) {
  return locationLabelFromLocation(map.get(Number(id))) || `#${id}`;
}

function groupLabelFromGroup(group) {
  if (!group) return 'Unknown group';
  const name = group.name || 'Group';
  return `${name} (#${group.id})`;
}

function groupLabelFromMap(map, id) {
  return groupLabelFromGroup(map.get(Number(id))) || `#${id}`;
}

export default registerUserGroupAssign;


