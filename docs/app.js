/**
 * Open Library Viewer
 * 
 * A static viewer for URL metadata records crawled from government sources.
 * Reads JSONL data from a GitHub repository's `data` branch.
 */

// ============================================================================
// DOM Elements
// ============================================================================

const els = {
  repoInput: document.getElementById("repoInput"),
  saveRepoBtn: document.getElementById("saveRepoBtn"),
  dateSelect: document.getElementById("dateSelect"),
  sourceFilter: document.getElementById("sourceFilter"),
  searchInput: document.getElementById("searchInput"),
  pageSizeSelect: document.getElementById("pageSizeSelect"),
  status: document.getElementById("status"),
  downloadMenu: document.getElementById("downloadMenu"),
  downloadJsonLink: document.getElementById("downloadJsonLink"),
  runInfo: document.getElementById("runInfo"),
  downloadExcelBtn: document.getElementById("downloadExcelBtn"),
  reloadBtn: document.getElementById("reloadBtn"),
  resetBtn: document.getElementById("resetBtn"),
  resultInfo: document.getElementById("resultInfo"),
  prevBtn: document.getElementById("prevBtn"),
  pageInput: document.getElementById("pageInput"),
  goPageBtn: document.getElementById("goPageBtn"),
  nextBtn: document.getElementById("nextBtn"),
  pageText: document.getElementById("pageText"),
  tbody: document.getElementById("tbody"),
  detailDialog: document.getElementById("detailDialog"),
  detailTitle: document.getElementById("detailTitle"),
  detailSubtitle: document.getElementById("detailSubtitle"),
  detailFields: document.getElementById("detailFields"),
  detailJson: document.getElementById("detailJson"),
  detailOpenUrl: document.getElementById("detailOpenUrl"),
  copyJsonBtn: document.getElementById("copyJsonBtn"),
};

// ============================================================================
// Viewer Configuration
// ============================================================================

const VIEWER = {
  defaultRepo: { owner: "", repo: "" },
  sourceGroups: /** @type {Array<{id:string,label:string}>} */ ([]),
  sourceGroupLabels: /** @type {Record<string, string>} */ ({}),
  sourceGroupByCrawler: /** @type {Record<string, string>} */ ({}),
};

// ============================================================================
// Application State
// ============================================================================

const state = {
  owner: "",
  repo: "",
  branch: "data",
  dataRoot: "data",
  records: /** @type {Array<any>} */ ([]),
  filteredIdx: /** @type {Array<number>} */ ([]),
  sortKey: "publish_date",
  sortDir: "desc",
  page: 1,
  pageSize: 50,
  archiveIndex: /** @type {Array<any>} */ ([]),
  selectedDataPath: "latest/urls.jsonl",
  viewerConfig: null,
  loading: false,
};

// ============================================================================
// Utility Functions
// ============================================================================

function setStatus(text) {
  els.status.textContent = text;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDateUtc(value, opts = {}) {
  const s = (value || "").trim();
  if (!s) return "";

  let d;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    d = new Date(`${s}T00:00:00Z`);
  } else {
    const t = Date.parse(s);
    d = Number.isFinite(t) ? new Date(t) : null;
  }
  if (!d || Number.isNaN(d.getTime())) return s;

  const fmt = new Intl.DateTimeFormat(undefined, {
    timeZone: "UTC",
    year: "numeric",
    month: "short",
    day: "numeric",
    ...opts,
  });
  return fmt.format(d);
}

function humanBytes(n) {
  const x = Number(n) || 0;
  if (!x) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let v = x;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function safeHost(url) {
  try {
    return url ? new URL(url).hostname.toLowerCase() : "";
  } catch {
    return "";
  }
}

function compareValues(a, b) {
  if (a === b) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  return a < b ? -1 : 1;
}

// ============================================================================
// URL Helpers
// ============================================================================

function rawUrl(path) {
  if (!state.owner || !state.repo) return "";
  const clean = String(path || "").replace(/^\/+/, "");
  return `https://raw.githubusercontent.com/${state.owner}/${state.repo}/${state.branch}/${state.dataRoot}/${clean}`;
}

function githubApiUrl(path) {
  return `https://api.github.com/${String(path || "").replace(/^\/+/, "")}`;
}

function repoFileUrl(path, branch = "main") {
  if (!state.owner || !state.repo) return "";
  const clean = String(path || "").replace(/^\/+/, "");
  return `https://raw.githubusercontent.com/${state.owner}/${state.repo}/${branch}/${clean}`;
}

function normalizeDataRootPath(p) {
  const s = String(p || "").trim().replace(/^\/+/, "");
  return s.startsWith("data/") ? s.slice(5) : s;
}

function archiveDateFromPath(path) {
  const s = String(path || "").trim();
  const m = s.match(/archive\/(\d{4})\/(\d{2})\/(\d{2})\/urls\.jsonl$/);
  if (!m) return "";
  return `${m[1]}-${m[2]}-${m[3]}`;
}

function jsonlDownloadFileName() {
  const relPath = String(state.selectedDataPath || "latest/urls.jsonl").trim();
  const archiveDate = archiveDateFromPath(relPath);
  const stamp = archiveDate || new Date().toISOString().slice(0, 10);
  return `open-library-${stamp}.jsonl`;
}

function downloadBlobFile(blob, fileName) {
  const objectUrl = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = objectUrl;
  a.download = fileName;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
}

async function downloadJsonl() {
  if (!state.owner || !state.repo) {
    setStatus("Set repo (owner/repo) to load data");
    return;
  }

  const relPath = String(state.selectedDataPath || "latest/urls.jsonl").trim();
  const url = rawUrl(relPath);
  if (!url) {
    setStatus("Invalid repo configuration");
    return;
  }

  try {
    setStatus("Preparing JSONL download...");
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const blob = new Blob([text], { type: "application/x-ndjson;charset=utf-8" });
    downloadBlobFile(blob, jsonlDownloadFileName());
    setStatus("JSONL downloaded");
  } catch (err) {
    console.error(err);
    setStatus("Failed to download JSONL");
  }
}

let xlsxLoadPromise = null;

function loadScriptOnce(src) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector(`script[data-lib-src="${src}"]`);
    if (existing) {
      if (existing.getAttribute("data-loaded") === "true") {
        resolve();
        return;
      }
      existing.addEventListener("load", () => resolve(), { once: true });
      existing.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
      return;
    }

    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.setAttribute("data-lib-src", src);
    script.addEventListener("load", () => {
      script.setAttribute("data-loaded", "true");
      resolve();
    }, { once: true });
    script.addEventListener("error", () => reject(new Error(`Failed to load ${src}`)), { once: true });
    document.head.appendChild(script);
  });
}

async function ensureXlsxLoaded() {
  if (window.XLSX?.utils?.writeFile) return window.XLSX;

  if (!xlsxLoadPromise) {
    xlsxLoadPromise = (async () => {
      const sources = [
        "./vendor/xlsx.full.min.js",
        "https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js",
        "https://unpkg.com/xlsx@0.18.5/dist/xlsx.full.min.js",
      ];

      let lastError = null;

      for (const src of sources) {
        try {
          await loadScriptOnce(src);
          if (window.XLSX?.utils?.writeFile) return window.XLSX;
        } catch (err) {
          lastError = err;
          // Try next CDN source.
        }
      }

      throw new Error(
        `XLSX library unavailable (${lastError instanceof Error ? lastError.message : "load failed"})`
      );
    })();
  }

  try {
    return await xlsxLoadPromise;
  } catch (err) {
    // Allow retry on next click in case this was a transient network/CSP issue.
    xlsxLoadPromise = null;
    throw err;
  }
}

// ============================================================================
// Source Group Resolution
// ============================================================================

function sourceGroupFromCrawler(source) {
  const s = String(source || "").trim().toLowerCase();
  if (!s) return "";

  // Check crawler-to-source mapping from settings.yaml
  if (VIEWER.sourceGroupByCrawler[s]) {
    return VIEWER.sourceGroupByCrawler[s];
  }

  // Handle dotted names like "emsd.gas_safety_portal"
  const dottedMatch = s.match(/^([a-z_]+)\.([a-z_]+)$/);
  if (dottedMatch) {
    const [, prefix, suffix] = dottedMatch;
    // Check if full dotted name maps
    if (VIEWER.sourceGroupByCrawler[s]) {
      return VIEWER.sourceGroupByCrawler[s];
    }
    // Check if suffix maps to a source
    if (VIEWER.sourceGroupByCrawler[suffix]) {
      return VIEWER.sourceGroupByCrawler[suffix];
    }
    // Check if prefix is a known source
    if (VIEWER.sourceGroupLabels[prefix]) {
      return prefix;
    }
  }

  // Check if source itself is a known source ID
  if (VIEWER.sourceGroupLabels[s]) {
    return s;
  }

  // Fallback: extract prefix from underscore-separated name (e.g., "devb_press_releases" -> "devb")
  const underscoreIdx = s.indexOf("_");
  if (underscoreIdx > 0) {
    const prefix = s.slice(0, underscoreIdx);
    if (VIEWER.sourceGroupLabels[prefix]) {
      return prefix;
    }
  }

  return "";
}

// ============================================================================
// Record Processing
// ============================================================================

function normalizeRecord(r) {
  const url = typeof r?.url === "string" ? r.url : "";
  const source = typeof r?.source === "string" ? r.source : "";

  // Prefer source_id from record, then resolve from crawler name via settings mapping.
  const sourceId = String(r?.source_id || "").trim().toLowerCase();

  const sourceGroup = sourceId
    ? sourceId
    : sourceGroupFromCrawler(source);

  return {
    url,
    name: typeof r?.name === "string" ? r.name : "",
    discovered_at_utc: typeof r?.discovered_at_utc === "string" ? r.discovered_at_utc : "",
    publish_date: typeof r?.publish_date === "string" ? r.publish_date : "",
    source,
    source_group: sourceGroup,
    source_group_label: VIEWER.sourceGroupLabels[sourceGroup] || sourceGroup || source,
    meta: r?.meta ?? null,
    domain: safeHost(url),
  };
}

function recordSortValue(rec, key) {
  switch (key) {
    case "publish_date": {
      const s = (rec.publish_date || "").trim();
      if (!s) return null;
      const t = Date.parse(/^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T00:00:00Z` : s);
      return Number.isFinite(t) ? t : s;
    }
    case "discovered_at_utc": {
      const t = Date.parse(rec.discovered_at_utc || "");
      return Number.isFinite(t) ? t : rec.discovered_at_utc;
    }
    case "name":
      return (rec.name || "").toLowerCase();
    case "url":
      return (rec.url || "").toLowerCase();
    case "domain":
      return (rec.domain || "").toLowerCase();
    case "source":
      return (rec.source_group_label || rec.source_group || rec.source || "").toLowerCase();
    default:
      return rec[key] ?? "";
  }
}

// ============================================================================
// Excel Export
// ============================================================================

function metaValueForExcel(v) {
  if (v == null) return "";
  if (Array.isArray(v)) {
    return v.map((x) => String(x ?? "").trim()).filter(Boolean).join("; ");
  }
  if (typeof v === "object") {
    try { return JSON.stringify(v); } catch { return String(v); }
  }
  return String(v);
}

async function downloadExcel() {
  let XLSX;
  try {
    XLSX = await ensureXlsxLoaded();
  } catch (err) {
    console.error(err);
    const detail = err instanceof Error ? err.message : "unknown error";
    alert(`Excel export library could not be loaded. ${detail}`);
    setStatus("Excel export unavailable (see console)");
    return;
  }

  if (!state.records.length) {
    alert("No records loaded yet.");
    return;
  }

  const idx = state.filteredIdx.length ? state.filteredIdx : state.records.map((_, i) => i);
  if (!idx.length) {
    alert("No matching records to export.");
    return;
  }

  const rows = idx.map((i) => {
    const r = state.records[i];
    const meta = r?.meta && typeof r.meta === "object" ? r.meta : null;

    return {
      name: r?.name ?? "",
      url: r?.url ?? "",
      domain: r?.domain ?? "",
      source: r?.source ?? "",
      discovered_at_utc: r?.discovered_at_utc ?? "",
      species_id: metaValueForExcel(meta?.species_id),
      family_name: metaValueForExcel(meta?.family_name),
      genus_name: metaValueForExcel(meta?.genus_name),
      common_name: metaValueForExcel(meta?.common_name),
      chinese_name: metaValueForExcel(meta?.chinese_name),
      meta_json: meta ? metaValueForExcel(meta) : "",
    };
  });

  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "links");

  const stamp = new Date().toISOString().slice(0, 10);
  XLSX.writeFile(wb, `open-library-${stamp}.xlsx`);
}

// ============================================================================
// Detail Dialog Rendering
// ============================================================================

function metaGet(meta, key) {
  if (!meta || typeof meta !== "object") return null;
  return Object.prototype.hasOwnProperty.call(meta, key) ? meta[key] : null;
}

function departmentLines(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter(Array.isArray)
    .map((p) => p.map((x) => String(x || "").trim()).filter(Boolean).join(" -> "))
    .filter(Boolean);
}

function emailDisplay(v) {
  const s = String(v ?? "").trim();
  return s || "N/A";
}

function renderKvRows(rows) {
  const frag = document.createDocumentFragment();
  for (const row of rows) {
    const div = document.createElement("div");
    div.className = "kv__row";

    const k = document.createElement("div");
    k.className = "kv__k";
    k.textContent = row.label;

    const v = document.createElement("div");
    v.className = "kv__v";

    if (row.kind === "departments") {
      const lines = Array.isArray(row.lines) ? row.lines : [];
      const limit = Number(row.limit) || lines.length;
      const shown = lines.slice(0, limit);
      const hidden = lines.slice(limit);

      const pre = document.createElement("div");
      pre.style.whiteSpace = "pre-line";
      pre.textContent = shown.join("\n");
      v.appendChild(pre);

      if (hidden.length) {
        const more = document.createElement("details");
        more.className = "raw";
        const summary = document.createElement("summary");
        summary.className = "raw__summary";
        summary.textContent = `Show ${hidden.length} more`;
        const rest = document.createElement("div");
        rest.style.whiteSpace = "pre-line";
        rest.style.marginTop = "8px";
        rest.textContent = hidden.join("\n");
        more.appendChild(summary);
        more.appendChild(rest);
        v.appendChild(more);
      }
    } else if (row.href) {
      v.innerHTML = `<a class="link" href="${escapeHtml(row.href)}" target="_blank" rel="noreferrer">${escapeHtml(row.value || row.href)}</a>`;
    } else {
      v.textContent = row.value;
    }

    div.appendChild(k);
    div.appendChild(v);
    frag.appendChild(div);
  }
  els.detailFields.replaceChildren(frag);
}

function openDetail(r) {
  const title = r.name || "(no title)";
  els.detailTitle.textContent = title;
  els.detailSubtitle.textContent = `${r.domain || ""} • ${r.source_group_label || r.source || ""} • ${formatDateUtc(r.publish_date) || "N/A"}`;
  els.detailOpenUrl.href = r.url || "#";

  const payload = {
    url: r.url,
    name: r.name,
    discovered_at_utc: r.discovered_at_utc,
    publish_date: r.publish_date,
    source: r.source,
    meta: r.meta,
  };
  els.detailJson.textContent = JSON.stringify(payload, null, 2);

  const meta = r?.meta && typeof r.meta === "object" ? r.meta : null;
  const rows = [
    { label: "URL", value: r.url || "", href: r.url || "" },
    { label: "Name", value: r.name || "(no title)" },
    { label: "Publish date", value: formatDateUtc(r.publish_date) || "N/A" },
    { label: "Source", value: r.source || "" },
    { label: "Website", value: r.domain || "" },
  ];

  // Add configured meta fields
  const cfg = state.viewerConfig || {};
  const srcCfg = cfg?.sources?.[r.source];
  const fieldDefs = srcCfg?.fields || cfg?.defaults?.fields || [];
  const already = new Set(rows.map((x) => x.label));

  for (const def of fieldDefs) {
    if (!def || def.type !== "meta") continue;
    const label = String(def.label || "").trim();
    if (!label || already.has(label)) continue;

    // Try primary key then fallback keys
    const keys = [def.key, ...(def.fallbackKeys || [])].map((k) => String(k || "").trim()).filter(Boolean);
    let val = null;
    for (const k of keys) {
      const got = metaGet(meta, k);
      if (got != null && !(typeof got === "string" && !got.trim())) {
        val = got;
        break;
      }
    }

    // Apply formatting
    const fmt = String(def.format || "").trim();

    if (fmt === "departments") {
      const lines = departmentLines(val);
      if (!lines.length) continue;
      rows.push({ label, kind: "departments", lines, limit: def.limit || 5 });
      already.add(label);
    } else if (fmt === "email") {
      if (r.source === "tel_directory" || val != null) {
        rows.push({ label, value: emailDisplay(val) });
        already.add(label);
      }
    } else if (fmt === "date" && val != null) {
      rows.push({ label, value: formatDateUtc(String(val)) });
      already.add(label);
    } else if (fmt === "url" && typeof val === "string" && val.trim()) {
      rows.push({ label, value: val.trim(), href: val.trim() });
      already.add(label);
    } else if (val != null && !(typeof val === "string" && !val.trim())) {
      rows.push({ label, value: String(val) });
      already.add(label);
    }
  }

  renderKvRows(rows);

  els.copyJsonBtn.onclick = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload));
      setStatus("Copied JSON to clipboard");
    } catch {
      setStatus("Could not copy (browser blocked clipboard)");
    }
  };

  els.detailDialog.showModal();
}

// ============================================================================
// Filtering & Sorting
// ============================================================================

function rebuildFilters() {
  const sources = new Map();

  for (const r of state.records) {
    if (r.source_group) sources.set(r.source_group, (sources.get(r.source_group) || 0) + 1);
  }

  const sourceSelected = els.sourceFilter.value;

  els.sourceFilter.replaceChildren(new Option("All", ""));
  for (const { id, label } of VIEWER.sourceGroups) {
    const count = sources.get(id) || 0;
    if (!count) continue;
    els.sourceFilter.add(new Option(`${label} (${count})`, id));
  }

  els.sourceFilter.value = sourceSelected;
}

function applyFiltersAndSort() {
  const source = (els.sourceFilter.value || "").trim();
  const q = (els.searchInput.value || "").trim().toLowerCase();

  const idx = [];
  for (let i = 0; i < state.records.length; i++) {
    const r = state.records[i];
    if (source && r.source_group !== source) continue;
    if (q) {
      const hay = `${r.name || ""}\n${r.url || ""}`.toLowerCase();
      if (!hay.includes(q)) continue;
    }
    idx.push(i);
  }

  const dirMul = state.sortDir === "asc" ? 1 : -1;
  idx.sort((ia, ib) => {
    const a = state.records[ia];
    const b = state.records[ib];
    const c = compareValues(recordSortValue(a, state.sortKey), recordSortValue(b, state.sortKey));
    return c !== 0 ? c * dirMul : (a.url || "").localeCompare(b.url || "") * dirMul;
  });

  state.filteredIdx = idx;
  state.page = 1;
}

function totalPages() {
  return Math.max(1, Math.ceil(state.filteredIdx.length / state.pageSize));
}

function clampPage() {
  const tp = totalPages();
  state.page = Math.max(1, Math.min(state.page, tp));
}

// ============================================================================
// Table Rendering
// ============================================================================

function renderTable() {
  clampPage();

  const total = state.filteredIdx.length;
  const tp = totalPages();
  const start = (state.page - 1) * state.pageSize;
  const end = Math.min(total, start + state.pageSize);

  els.pageText.textContent = `Page ${tp === 0 ? 0 : state.page} / ${tp}`;
  if (els.pageInput) {
    els.pageInput.max = String(tp);
    els.pageInput.value = String(state.page);
  }
  els.prevBtn.disabled = state.page <= 1;
  els.nextBtn.disabled = state.page >= tp;

  els.resultInfo.textContent = total
    ? `Showing ${start + 1}-${end} of ${total} records (loaded: ${state.records.length})`
    : `No matching records (loaded: ${state.records.length})`;

  const frag = document.createDocumentFragment();
  for (let i = start; i < end; i++) {
    const r = state.records[state.filteredIdx[i]];
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    tr.addEventListener("click", () => openDetail(r));
    tr.addEventListener("keydown", (e) => e.key === "Enter" && openDetail(r));

    tr.innerHTML = `
      <td class="cellTitle">${escapeHtml(r.name || "(no title)")}</td>
      <td><span class="badge">${escapeHtml(r.domain || "(unknown)")}</span></td>
      <td><span class="badge">${escapeHtml(r.source_group_label || r.source_group || r.source || "(unknown)")}</span></td>
      <td title="${escapeHtml(r.publish_date || "")}">${escapeHtml(formatDateUtc(r.publish_date) || "N/A")}</td>
      <td class="cellUrl"><a class="link" href="${escapeHtml(r.url || "#")}" target="_blank" rel="noreferrer">URL</a></td>
    `;
    frag.appendChild(tr);
  }
  els.tbody.replaceChildren(frag);
}

// ============================================================================
// Data Loading
// ============================================================================

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.json();
}

async function loadViewerConfig() {
  try {
    const res = await fetch("./viewer-config.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.viewerConfig = await res.json();
  } catch {
    state.viewerConfig = { version: 1, viewer: {}, defaults: { fields: [] }, sources: {} };
  }
}

async function loadSourceMapFromSettingsYaml() {
  if (!state.owner || !state.repo) {
    console.warn("[viewer] No repo configured, skipping settings.yaml load");
    return;
  }
  
  const yaml = window.jsyaml;
  if (!yaml?.load) {
    console.warn("[viewer] jsyaml not loaded, skipping settings.yaml parse");
    return;
  }

  const url = repoFileUrl("config/settings.yaml", "main");
  if (!url) {
    console.warn("[viewer] Could not build settings.yaml URL");
    return;
  }

  try {
    console.log("[viewer] Loading settings.yaml from:", url);
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) {
      console.warn("[viewer] Failed to fetch settings.yaml:", res.status);
      return;
    }

    const text = await res.text();
    const parsed = yaml.load(text);
    const crawlers = parsed?.crawlers;
    
    if (!crawlers || typeof crawlers !== "object") {
      console.warn("[viewer] No crawlers section in settings.yaml");
      return;
    }

    const groups = [];
    const byCrawler = {};

    for (const [sourceIdRaw, sourceCfg] of Object.entries(crawlers)) {
      const sourceId = String(sourceIdRaw || "").trim().toLowerCase();
      if (!sourceId || !sourceCfg || typeof sourceCfg !== "object") continue;

      const label = String(sourceCfg.label || sourceId).trim();
      groups.push({ id: sourceId, label });

      const pages = sourceCfg.pages;
      if (!pages || typeof pages !== "object") continue;

      for (const crawlerName of Object.keys(pages)) {
        const key = String(crawlerName || "").trim().toLowerCase();
        if (key) byCrawler[key] = sourceId;
      }
    }

    if (groups.length) {
      VIEWER.sourceGroups = groups;
      VIEWER.sourceGroupLabels = Object.fromEntries(groups.map((x) => [x.id, x.label]));
      console.log("[viewer] Loaded source groups:", groups.map(g => g.id).join(", "));
    }

    VIEWER.sourceGroupByCrawler = { ...VIEWER.sourceGroupByCrawler, ...byCrawler };
    console.log("[viewer] Loaded crawler mappings:", Object.keys(byCrawler).length);
  } catch (err) {
    console.error("[viewer] Error loading settings.yaml:", err);
  }
}

function applyViewerConfig() {
  const viewerCfg = state.viewerConfig?.viewer || {};

  const cfgRepo = viewerCfg.defaultRepo || {};
  VIEWER.defaultRepo = {
    owner: String(cfgRepo.owner || "").trim(),
    repo: String(cfgRepo.repo || "").trim(),
  };

  const groups = Array.isArray(viewerCfg.sourceGroups)
    ? viewerCfg.sourceGroups
        .map((g) => ({
          id: String(g?.id || "").trim().toLowerCase(),
          label: String(g?.label || "").trim(),
        }))
        .filter((g) => g.id && g.label)
    : [];

  VIEWER.sourceGroups = groups;
  VIEWER.sourceGroupLabels = Object.fromEntries(groups.map((x) => [x.id, x.label]));

  const byCrawler = viewerCfg.sourceGroupByCrawler || {};
  VIEWER.sourceGroupByCrawler = Object.fromEntries(
    Object.entries(byCrawler)
      .map(([k, v]) => [String(k || "").trim().toLowerCase(), String(v || "").trim().toLowerCase()])
      .filter(([k, v]) => k && v)
  );
}

async function tryBuildArchiveIndexFromGitTree() {
  if (!state.owner || !state.repo) return [];

  try {
    // Get ref for data branch
    const ref = await fetchJson(githubApiUrl(`repos/${state.owner}/${state.repo}/git/ref/heads/${state.branch}`));
    const commitSha = ref?.object?.sha;
    if (!commitSha) return [];

    // Get commit -> tree sha
    const commit = await fetchJson(githubApiUrl(`repos/${state.owner}/${state.repo}/git/commits/${commitSha}`));
    const treeSha = commit?.tree?.sha;
    if (!treeSha) return [];

    // Get full tree
    const tree = await fetchJson(githubApiUrl(`repos/${state.owner}/${state.repo}/git/trees/${treeSha}?recursive=1`));
    const items = Array.isArray(tree?.tree) ? tree.tree : [];

    const out = [];
    const re = /^data\/archive\/(\d{4})\/(\d{2})\/(\d{2})\/urls\.jsonl$/;

    for (const it of items) {
      if (it?.type !== "blob" || typeof it?.path !== "string") continue;
      const m = it.path.match(re);
      if (!m) continue;
      out.push({
        date: `${m[1]}-${m[2]}-${m[3]}`,
        path: normalizeDataRootPath(it.path),
        bytes: typeof it.size === "number" ? it.size : undefined,
      });
    }

    out.sort((a, b) => String(b.date).localeCompare(String(a.date)));
    return out;
  } catch {
    return [];
  }
}

async function loadArchiveIndex() {
  if (!els.dateSelect) return;

  let entries = [];
  try {
    const url = rawUrl("archive/index.json");
    if (url) {
      const obj = await fetchJson(url);
      const rawEntries = obj?.archives || (Array.isArray(obj) ? obj : []);

      entries = rawEntries
        .map((e) => {
          const path = normalizeDataRootPath((e.path || "").trim());
          let date = (e.date || e.run_date_utc || "").trim();
          if (!date && path) {
            const m = path.match(/archive\/(\d{4})\/(\d{2})\/(\d{2})\//);
            if (m) date = `${m[1]}-${m[2]}-${m[3]}`;
          }
          return { date, path, bytes: typeof e.bytes === "number" ? e.bytes : undefined };
        })
        .filter((x) => x.path);
    }
  } catch {
    setStatus("Loading archives…");
    entries = await tryBuildArchiveIndexFromGitTree();
  }

  entries.sort((a, b) => String(b.date || "").localeCompare(String(a.date || "")));
  state.archiveIndex = entries;

  let latestDate = "";
  try {
    const latestSummary = await fetchJson(rawUrl("latest/summary.json"));
    latestDate = String(latestSummary?.run_date_utc || "").trim();
  } catch { /* ignore */ }

  els.dateSelect.replaceChildren();
  els.dateSelect.add(new Option(latestDate ? `Latest (${latestDate})` : "Latest", "latest/urls.jsonl"));

  for (const e of entries) {
    if (!e.path) continue;
    const size = e.bytes != null ? ` (${humanBytes(e.bytes)})` : "";
    els.dateSelect.add(new Option(`${e.date || e.path}${size}`, e.path));
  }

  els.dateSelect.value = "latest/urls.jsonl";
  state.selectedDataPath = "latest/urls.jsonl";

  setStatus(entries.length ? `Loaded ${entries.length} archive dates` : "No archives found");
}

async function loadRunInfo() {
  if (!els.runInfo) return;
  els.runInfo.textContent = "";

  try {
    const s = await fetchJson(rawUrl("latest/summary.json"));
    const runDate = formatDateUtc(s?.run_date_utc) || "";
    const rows = s?.rows != null ? String(s.rows) : "";
    const crawler = s?.crawler || "";
    els.runInfo.textContent = runDate
      ? `run: ${runDate}${rows ? ` • rows: ${rows}` : ""}${crawler ? ` • scope: ${crawler}` : ""}`
      : "";
  } catch { /* ignore */ }
}

async function loadDataset() {
  if (!state.owner || !state.repo) {
    setStatus("Set repo (owner/repo) to load data");
    return;
  }

  const relPath = state.selectedDataPath || "latest/urls.jsonl";
  const isLatest = relPath === "latest/urls.jsonl";
  const url = rawUrl(relPath);

  if (!url) {
    setStatus("Invalid repo configuration");
    return;
  }

  if (els.downloadJsonLink) {
    els.downloadJsonLink.href = "#";
    els.downloadJsonLink.textContent = "JSONL";
    els.downloadJsonLink.removeAttribute("aria-disabled");
    els.downloadJsonLink.classList.remove("menu__item--disabled");
  }
  if (els.downloadExcelBtn) els.downloadExcelBtn.disabled = true;

  state.loading = true;
  state.records = [];
  state.filteredIdx = [];
  setStatus("Loading…");

  try {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

    const totalBytes = Number(res.headers.get("Content-Length") || "0") || 0;
    const reader = res.body.getReader();
    const decoder = new TextDecoder("utf-8");

    let bytesRead = 0;
    let buf = "";
    let lines = 0;
    let parseErrors = 0;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      bytesRead += value?.byteLength || 0;
      buf += decoder.decode(value, { stream: true });

      let idx;
      while ((idx = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, idx).trim();
        buf = buf.slice(idx + 1);
        if (!line) continue;

        try {
          state.records.push(normalizeRecord(JSON.parse(line)));
        } catch {
          parseErrors++;
        }
        lines++;

        if (lines % 1000 === 0) {
          const pct = totalBytes ? Math.round((100 * bytesRead) / totalBytes) : 0;
          setStatus(totalBytes
            ? `Loading… ${pct}% (${state.records.length.toLocaleString()} records)`
            : `Loading… (${state.records.length.toLocaleString()} records)`);
          await new Promise((r) => setTimeout(r, 0));
        }
      }
    }

    // Process remaining buffer
    const tail = decoder.decode().trim();
    if (tail) {
      try {
        state.records.push(normalizeRecord(JSON.parse(tail)));
      } catch {
        parseErrors++;
      }
    }

    rebuildFilters();
    applyFiltersAndSort();
    renderTable();

    if (els.downloadExcelBtn) els.downloadExcelBtn.disabled = state.filteredIdx.length === 0;
    setStatus(parseErrors ? `Idle (parse errors: ${parseErrors})` : "Idle");

    if (isLatest) {
      await loadRunInfo();
    } else if (els.runInfo) {
      els.runInfo.textContent = "";
    }
  } catch (err) {
    console.error(err);
    setStatus("Failed to load JSONL (check repo/branch/path)");
    els.tbody.replaceChildren();
    els.resultInfo.textContent = "No data loaded";
  } finally {
    state.loading = false;
  }
}

// ============================================================================
// Repository Management
// ============================================================================

function inferGitHubRepoFromPages() {
  const host = window.location.hostname || "";
  if (!host.endsWith(".github.io")) return null;

  const owner = host.split(".")[0] || "";
  const parts = (window.location.pathname || "/").split("/").filter(Boolean);
  const repo = parts[0] || "";

  return (owner && repo) ? { owner, repo } : null;
}

function loadRepoFromQueryOrStorage() {
  const url = new URL(window.location.href);
  const qp = url.searchParams.get("repo") || "";
  const stored = localStorage.getItem("ol_viewer_repo") || "";
  const inferred = inferGitHubRepoFromPages();
  const cfgRepo = VIEWER.defaultRepo.owner && VIEWER.defaultRepo.repo
    ? `${VIEWER.defaultRepo.owner}/${VIEWER.defaultRepo.repo}`
    : "";

  const chosen = (qp || stored || (inferred ? `${inferred.owner}/${inferred.repo}` : "") || cfgRepo).trim();
  const [owner, repo] = chosen.split("/");

  if (owner && repo) {
    state.owner = owner;
    state.repo = repo;
  }

  if (els.repoInput) {
    els.repoInput.value = state.owner && state.repo ? `${state.owner}/${state.repo}` : "";
  }
}

function saveRepo(value) {
  const v = (value || "").trim();
  const [owner, repo] = v.split("/");
  if (!owner || !repo) {
    alert("Repo must be like: owner/repo");
    return;
  }
  state.owner = owner;
  state.repo = repo;
  localStorage.setItem("ol_viewer_repo", `${owner}/${repo}`);
}

// ============================================================================
// Event Handlers
// ============================================================================

function closeDownloadMenu() {
  if (els.downloadMenu?.open) {
    els.downloadMenu.open = false;
  }
}

function resetFilters() {
  els.sourceFilter.value = "";
  els.searchInput.value = "";
  state.sortKey = "publish_date";
  state.sortDir = "desc";
  state.page = 1;
  applyFiltersAndSort();
  renderTable();
}

async function handleRepoChange() {
  saveRepo(els.repoInput.value);
  await loadSourceMapFromSettingsYaml();
  await loadArchiveIndex();
  await loadDataset();
}

function wireEvents() {
  // Repo input
  els.saveRepoBtn?.addEventListener("click", handleRepoChange);
  els.repoInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleRepoChange();
    }
  });

  // Date select
  els.dateSelect?.addEventListener("change", async () => {
    state.selectedDataPath = String(els.dateSelect.value || "latest/urls.jsonl");
    await loadDataset();
  });

  // Filters
  els.sourceFilter.addEventListener("change", () => {
    applyFiltersAndSort();
    renderTable();
  });

  let searchTimer = null;
  els.searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      applyFiltersAndSort();
      renderTable();
    }, 150);
  });

  // Pagination
  els.pageSizeSelect.addEventListener("change", () => {
    state.pageSize = Number(els.pageSizeSelect.value) || 50;
    state.page = 1;
    renderTable();
  });

  els.prevBtn.addEventListener("click", () => {
    state.page--;
    renderTable();
  });

  els.nextBtn.addEventListener("click", () => {
    state.page++;
    renderTable();
  });

  const goToPage = () => {
    const v = Number(els.pageInput?.value || "0") || 0;
    if (v > 0) state.page = v;
    renderTable();
  };

  els.goPageBtn?.addEventListener("click", goToPage);
  els.pageInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      goToPage();
    }
  });

  // Actions
  els.reloadBtn.addEventListener("click", loadDataset);
  els.resetBtn.addEventListener("click", resetFilters);

  els.downloadJsonLink?.addEventListener("click", async (e) => {
    e.preventDefault();
    await downloadJsonl();
    closeDownloadMenu();
  });
  els.downloadExcelBtn?.addEventListener("click", async () => {
    await downloadExcel();
    closeDownloadMenu();
  });

  // Sorting
  document.querySelectorAll("th[data-sort]").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (!key) return;

      if (state.sortKey === key) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDir = key === "publish_date" ? "desc" : "asc";
      }
      applyFiltersAndSort();
      renderTable();
    });
  });

  // Dialog handling
  els.detailDialog.addEventListener("click", (e) => {
    if (e.target === els.detailDialog) {
      els.detailDialog.close();
    }
  });

  // Close download menu on outside click
  document.addEventListener("click", (e) => {
    if (els.downloadMenu?.open && !els.downloadMenu.contains(e.target)) {
      closeDownloadMenu();
    }
  });
}

// ============================================================================
// Main Entry Point
// ============================================================================

async function main() {
  await loadViewerConfig();
  applyViewerConfig();

  loadRepoFromQueryOrStorage();
  await loadSourceMapFromSettingsYaml();

  state.pageSize = Number(els.pageSizeSelect.value) || 50;
  wireEvents();

  if (state.owner && state.repo) {
    await loadArchiveIndex();
    await loadDataset();
  } else {
    setStatus("Set repo (owner/repo) to load data");
  }
}

main();
