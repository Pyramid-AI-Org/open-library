const els = {
  repoInput: document.getElementById("repoInput"),
  saveRepoBtn: document.getElementById("saveRepoBtn"),

  datasetSelect: document.getElementById("datasetSelect"),
  archiveField: document.getElementById("archiveField"),
  archiveSelect: document.getElementById("archiveSelect"),

  sourceFilter: document.getElementById("sourceFilter"),
  domainFilter: document.getElementById("domainFilter"),
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

// Fallback repo for custom domains (where GitHub Pages inference won't work).
const DEFAULT_REPO = { owner: "pyramid-ai-org", repo: "open-library" };

/**
 * State
 */
const state = {
  owner: "",
  repo: "",
  branch: "data",
  dataRoot: "data",

  records: /** @type {Array<any>} */ ([]),
  filteredIdx: /** @type {Array<number>} */ ([]),

  sortKey: "discovered_at_utc",
  sortDir: "desc", // asc | desc

  page: 1,
  pageSize: 50,

  archiveIndex: /** @type {Array<any>} */ ([]),
  selectedArchivePath: "",

  viewerConfig: null,

  loading: false,
};

function metaValueForExcel(v) {
  if (v === null || v === undefined) return "";
  if (Array.isArray(v)) {
    return v
      .map((x) => String(x ?? "").trim())
      .filter(Boolean)
      .join("; ");
  }
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return String(v);
}

function downloadExcel() {
  const XLSX = window.XLSX;
  if (!XLSX || !XLSX.utils || !XLSX.writeFile) {
    alert("Excel export library not loaded. Please refresh the page.");
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
    const meta = r && r.meta && typeof r.meta === "object" ? r.meta : null;

    // A few useful meta columns (works great for herbarium; harmless for others).
    const speciesId = meta ? meta.species_id : null;
    const familyName = meta ? meta.family_name : null;
    const genusName = meta ? meta.genus_name : null;
    const commonName = meta ? meta.common_name : null;
    const chineseName = meta ? meta.chinese_name : null;

    return {
      name: r?.name ?? "",
      url: r?.url ?? "",
      domain: r?.domain ?? "",
      source: r?.source ?? "",
      discovered_at_utc: r?.discovered_at_utc ?? "",

      species_id: metaValueForExcel(speciesId),
      family_name: metaValueForExcel(familyName),
      genus_name: metaValueForExcel(genusName),
      common_name: metaValueForExcel(commonName),
      chinese_name: metaValueForExcel(chineseName),

      meta_json: meta ? metaValueForExcel(meta) : "",
    };
  });

  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "links");

  const d = new Date();
  const stamp = Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "export";
  XLSX.writeFile(wb, `open-library-${stamp}.xlsx`);
}

function closeDownloadMenu() {
  if (els.downloadMenu && els.downloadMenu.open) {
    els.downloadMenu.open = false;
  }
}

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
  // Support YYYY-MM-DD and ISO timestamps.
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
  const digits = i === 0 ? 0 : i === 1 ? 1 : 1;
  return `${v.toFixed(digits)} ${units[i]}`;
}

function normalizeDataRootPath(p) {
  const s = String(p || "").trim().replace(/^\/+/, "");
  // GitHub tree API returns paths like `data/archive/...`; rawUrl expects relative to data root.
  return s.startsWith("data/") ? s.slice("data/".length) : s;
}

function inferGitHubRepoFromPages() {
  // For https://OWNER.github.io/REPO/
  const host = window.location.hostname || "";
  const isPages = host.endsWith(".github.io");
  if (!isPages) return null;

  const owner = host.split(".")[0] || "";
  const parts = (window.location.pathname || "/").split("/").filter(Boolean);
  const repo = parts[0] || "";
  if (!owner || !repo) return null;

  return { owner, repo };
}

function loadRepoFromQueryOrStorage() {
  const url = new URL(window.location.href);
  const qp = url.searchParams.get("repo") || "";
  const stored = localStorage.getItem("ol_viewer_repo") || "";

  const inferred = inferGitHubRepoFromPages();

  const chosen = (
    qp ||
    stored ||
    (inferred ? `${inferred.owner}/${inferred.repo}` : "") ||
    `${DEFAULT_REPO.owner}/${DEFAULT_REPO.repo}`
  ).trim();
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

function rawUrl(path) {
  if (!state.owner || !state.repo) return "";
  const clean = String(path || "").replace(/^\/+/, "");
  return `https://raw.githubusercontent.com/${state.owner}/${state.repo}/${state.branch}/${state.dataRoot}/${clean}`;
}

function githubApiUrl(path) {
  const clean = String(path || "").replace(/^\/+/, "");
  return `https://api.github.com/${clean}`;
}

function safeHost(url) {
  const s = (url || "").trim();
  if (!s) return "";
  try {
    return new URL(s).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function normalizeRecord(r) {
  const url = typeof r?.url === "string" ? r.url : "";
  return {
    url,
    name: typeof r?.name === "string" ? r.name : "",
    discovered_at_utc: typeof r?.discovered_at_utc === "string" ? r.discovered_at_utc : "",
    source: typeof r?.source === "string" ? r.source : "",
    meta: r?.meta ?? null,
    domain: safeHost(url),
  };
}

function metaGet(meta, key) {
  if (!meta || typeof meta !== "object") return null;
  // eslint-disable-next-line no-prototype-builtins
  if (!Object.prototype.hasOwnProperty.call(meta, key)) return null;
  return meta[key];
}

function departmentLines(value) {
  // department_paths: [["Dept", "Division"], ...]
  if (!Array.isArray(value)) return [];
  const lines = [];
  for (const p of value) {
    if (!Array.isArray(p)) continue;
    const segs = p.map((x) => String(x || "").trim()).filter(Boolean);
    if (segs.length) lines.push(segs.join(" -> "));
  }
  return lines;
}

function emailDisplay(v) {
  if (v === null || v === undefined) return "N/A";
  const s = String(v).trim();
  return s ? s : "N/A";
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

function compareValues(a, b) {
  if (a === b) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  return a < b ? -1 : 1;
}

function recordSortValue(rec, key) {
  switch (key) {
    case "discovered_at_utc": {
      const t = Date.parse(rec.discovered_at_utc || "");
      return Number.isFinite(t) ? t : (rec.discovered_at_utc || "");
    }
    case "name":
      return (rec.name || "").toLowerCase();
    case "url":
      return (rec.url || "").toLowerCase();
    case "domain":
      return (rec.domain || "").toLowerCase();
    case "source":
      return (rec.source || "").toLowerCase();
    default:
      return (rec[key] ?? "");
  }
}

function rebuildFilters() {
  const sources = new Map();
  const domains = new Map();

  for (const r of state.records) {
    if (r.source) sources.set(r.source, (sources.get(r.source) || 0) + 1);
    if (r.domain) domains.set(r.domain, (domains.get(r.domain) || 0) + 1);
  }

  const sourceSelected = els.sourceFilter.value;
  const domainSelected = els.domainFilter.value;

  const sourceOptions = Array.from(sources.entries()).sort((a, b) => b[1] - a[1]);
  const domainOptions = Array.from(domains.entries()).sort((a, b) => b[1] - a[1]);

  els.sourceFilter.replaceChildren(new Option("All", ""));
  for (const [src, count] of sourceOptions) {
    els.sourceFilter.add(new Option(`${src} (${count})`, src));
  }

  els.domainFilter.replaceChildren(new Option("All", ""));
  for (const [d, count] of domainOptions) {
    els.domainFilter.add(new Option(`${d} (${count})`, d));
  }

  els.sourceFilter.value = sourceSelected;
  els.domainFilter.value = domainSelected;
}

function applyFiltersAndSort() {
  const source = (els.sourceFilter.value || "").trim();
  const domain = (els.domainFilter.value || "").trim();
  const q = (els.searchInput.value || "").trim().toLowerCase();

  const idx = [];
  for (let i = 0; i < state.records.length; i++) {
    const r = state.records[i];
    if (source && r.source !== source) continue;
    if (domain && r.domain !== domain) continue;

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
    const va = recordSortValue(a, state.sortKey);
    const vb = recordSortValue(b, state.sortKey);
    const c = compareValues(va, vb);
    if (c !== 0) return c * dirMul;
    // stable-ish fallback
    return (a.url || "").localeCompare(b.url || "") * dirMul;
  });

  state.filteredIdx = idx;
  state.page = 1;
}

function totalPages() {
  return Math.max(1, Math.ceil(state.filteredIdx.length / state.pageSize));
}

function clampPage() {
  const tp = totalPages();
  if (state.page < 1) state.page = 1;
  if (state.page > tp) state.page = tp;
}

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
    tr.addEventListener("keydown", (e) => {
      if (e.key === "Enter") openDetail(r);
    });

    const title = r.name || "(no title)";
    tr.innerHTML = `
      <td class="cellTitle">${escapeHtml(title)}</td>
      <td><span class="badge">${escapeHtml(r.domain || "(unknown)")}</span></td>
      <td><span class="badge">${escapeHtml(r.source || "(unknown)")}</span></td>
      <td title="${escapeHtml(r.discovered_at_utc || "")}">${escapeHtml(formatDateUtc(r.discovered_at_utc))}</td>
      <td class="cellUrl"><a class="link" href="${escapeHtml(r.url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(r.url || "")}</a></td>
    `;
    frag.appendChild(tr);
  }
  els.tbody.replaceChildren(frag);
}

function openDetail(r) {
  const title = r.name || "(no title)";
  els.detailTitle.textContent = title;
  els.detailSubtitle.textContent = `${r.domain || ""} • ${r.source || ""} • ${formatDateUtc(r.discovered_at_utc)}`;
  els.detailOpenUrl.href = r.url || "#";

  const payload = {
    url: r.url,
    name: r.name,
    discovered_at_utc: r.discovered_at_utc,
    source: r.source,
    meta: r.meta,
  };

  els.detailJson.textContent = JSON.stringify(payload, null, 2);

  const meta = r.meta && typeof r.meta === "object" ? r.meta : null;

  /** @type {Array<any>} */
  const rows = [];

  rows.push({ label: "URL", value: r.url || "", href: r.url || "" });
  rows.push({ label: "Name", value: r.name || "(no title)" });
  rows.push({ label: "Discovered at", value: formatDateUtc(r.discovered_at_utc) || "" });
  rows.push({ label: "Source", value: r.source || "" });
  rows.push({ label: "Website", value: r.domain || "" });

  const cfg = state.viewerConfig || {};
  const srcCfg = (cfg.sources && r.source && cfg.sources[r.source]) || null;
  const fieldDefs =
    (srcCfg && Array.isArray(srcCfg.fields) ? srcCfg.fields : null) ||
    (cfg.defaults && Array.isArray(cfg.defaults.fields) ? cfg.defaults.fields : []);

  const already = new Set(rows.map((x) => x.label));

  for (const def of fieldDefs) {
    if (!def || def.type !== "meta") continue;
    const label = String(def.label || "").trim();
    if (!label || already.has(label)) continue;

    const keys = [String(def.key || "").trim()].filter(Boolean);
    const fallbacks = Array.isArray(def.fallbackKeys) ? def.fallbackKeys.map(String) : [];
    for (const k of fallbacks) {
      const kk = String(k || "").trim();
      if (kk) keys.push(kk);
    }

    let val = null;
    for (const k of keys) {
      const got = metaGet(meta, k);
      if (got !== null && got !== undefined && !(typeof got === "string" && !got.trim())) {
        val = got;
        break;
      }
    }

    // Special formatting
    const fmt = String(def.format || "").trim();
    if (fmt === "departments") {
      const lines = departmentLines(val);
      if (!lines.length) continue;
      rows.push({ label, kind: "departments", lines, limit: def.limit || 5 });
      already.add(label);
      continue;
    }

    if (fmt === "email") {
      // For tel_directory specifically, show Email even if null.
      if ((r.source || "").toLowerCase() === "tel_directory") {
        rows.push({ label, value: emailDisplay(val) });
        already.add(label);
      } else if (val !== null && val !== undefined) {
        rows.push({ label, value: emailDisplay(val) });
        already.add(label);
      }
      continue;
    }

    if (fmt === "date") {
      if (val === null || val === undefined) continue;
      rows.push({ label, value: formatDateUtc(String(val)) });
      already.add(label);
      continue;
    }

    if (fmt === "url") {
      if (typeof val !== "string" || !val.trim()) continue;
      rows.push({ label, value: val.trim(), href: val.trim() });
      already.add(label);
      continue;
    }

    if (val === null || val === undefined) continue;
    if (typeof val === "string" && !val.trim()) continue;

    rows.push({ label, value: String(val) });
    already.add(label);
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
    state.viewerConfig = { version: 1, defaults: { fields: [] }, sources: {} };
  }
}

async function tryBuildArchiveIndexFromGitTree() {
  if (!state.owner || !state.repo) return [];
  // 1) Get ref for data branch
  const refUrl = githubApiUrl(`repos/${state.owner}/${state.repo}/git/ref/heads/${state.branch}`);
  const ref = await fetchJson(refUrl);
  const commitSha = ref?.object?.sha;
  if (!commitSha) return [];

  // 2) Get commit -> tree sha
  const commitUrl = githubApiUrl(`repos/${state.owner}/${state.repo}/git/commits/${commitSha}`);
  const commit = await fetchJson(commitUrl);
  const treeSha = commit?.tree?.sha;
  if (!treeSha) return [];

  // 3) Get full tree
  const treeUrl = githubApiUrl(
    `repos/${state.owner}/${state.repo}/git/trees/${treeSha}?recursive=1`
  );
  const tree = await fetchJson(treeUrl);
  const items = Array.isArray(tree?.tree) ? tree.tree : [];

  const out = [];
  const re = /^data\/archive\/(\d{4})\/(\d{2})\/(\d{2})\/urls\.jsonl$/;
  for (const it of items) {
    if (!it || it.type !== "blob" || typeof it.path !== "string") continue;
    const m = it.path.match(re);
    if (!m) continue;
    const date = `${m[1]}-${m[2]}-${m[3]}`;
    out.push({
      date,
      path: normalizeDataRootPath(it.path),
      bytes: typeof it.size === "number" ? it.size : undefined,
    });
  }

  out.sort((a, b) => String(b.date).localeCompare(String(a.date)));
  return out;
}

async function loadArchiveIndex() {
  const url = rawUrl("archive/index.json");
  if (!url) return;

  try {
    const obj = await fetchJson(url);
    const entries = Array.isArray(obj?.archives) ? obj.archives : Array.isArray(obj) ? obj : [];
    state.archiveIndex = entries;

    els.archiveSelect.replaceChildren();
    els.archiveSelect.add(new Option("Select an archive…", ""));

    for (const e of entries) {
      const path = normalizeDataRootPath((e.path || "").trim());
      let date = (e.date || e.run_date_utc || "").trim();
      if (!date && path) {
        // Try infer from archive/YYYY/MM/DD/...
        const m = path.match(/archive\/(\d{4})\/(\d{2})\/(\d{2})\//);
        if (m) date = `${m[1]}-${m[2]}-${m[3]}`;
      }
      if (!path) continue;
      const size = e.bytes != null ? ` (${humanBytes(e.bytes)})` : "";
      els.archiveSelect.add(new Option(`${date || path}${size}`, path));
    }

    if (els.archiveSelect.options.length > 1) {
      // Default to most recent archive entry when user switches to Archive.
      els.archiveSelect.selectedIndex = 1;
      state.selectedArchivePath = String(els.archiveSelect.value || "");
    }
  } catch (err) {
    // Fallback: build archive list from the data branch git tree.
    try {
      setStatus("Loading archives…");
      const built = await tryBuildArchiveIndexFromGitTree();
      state.archiveIndex = built;

      els.archiveSelect.replaceChildren();
      els.archiveSelect.add(new Option("Select an archive…", ""));
      for (const e of built) {
        const size = e.bytes != null ? ` (${humanBytes(e.bytes)})` : "";
        els.archiveSelect.add(new Option(`${e.date}${size}`, e.path));
      }

      if (els.archiveSelect.options.length > 1) {
        els.archiveSelect.selectedIndex = 1;
        state.selectedArchivePath = String(els.archiveSelect.value || "");
      }

      setStatus(built.length ? `Loaded ${built.length} archives` : "No archives found");
    } catch {
      state.archiveIndex = [];
      els.archiveSelect.replaceChildren(new Option("(no archives found)", ""));
    }
  }
}

async function loadRunInfo() {
  if (!els.runInfo) return;
  els.runInfo.textContent = "";
  const url = rawUrl("latest/summary.json");
  if (!url) return;
  try {
    const s = await fetchJson(url);
    const runDate = s?.run_date_utc || "";
    const rows = s?.rows != null ? String(s.rows) : "";
    const crawler = s?.crawler || "";
    const prettyRun = runDate ? formatDateUtc(String(runDate)) : "";
    els.runInfo.textContent = prettyRun
      ? `run: ${prettyRun}${rows ? ` • rows: ${rows}` : ""}${crawler ? ` • scope: ${crawler}` : ""}`
      : "";
  } catch {
    // ignore
  }
}

async function loadDataset() {
  if (!state.owner || !state.repo) {
    setStatus("Set repo (owner/repo) to load data");
    return;
  }

  const kind = els.datasetSelect.value;
  const relPath =
    kind === "archive" && state.selectedArchivePath
      ? state.selectedArchivePath
      : "latest/urls.jsonl";

  const url = rawUrl(relPath);
  if (!url) {
    setStatus("Invalid repo configuration");
    return;
  }

  if (els.downloadJsonLink) {
    els.downloadJsonLink.href = url;
    els.downloadJsonLink.textContent = `JSONL (${kind === "archive" ? "archive" : "latest"})`;
    els.downloadJsonLink.removeAttribute("aria-disabled");
    els.downloadJsonLink.classList.remove("menu__item--disabled");
  }
  if (els.downloadExcelBtn) els.downloadExcelBtn.disabled = true;

  state.loading = true;
  state.records = [];
  state.filteredIdx = [];
  setStatus("Loading…");

  const startT = performance.now();

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
          const obj = JSON.parse(line);
          state.records.push(normalizeRecord(obj));
        } catch {
          parseErrors++;
        }
        lines++;

        if (lines % 1000 === 0) {
          const pct = totalBytes ? Math.round((100 * bytesRead) / totalBytes) : 0;
          setStatus(
            totalBytes
              ? `Loading… ${pct}% (${state.records.length.toLocaleString()} records)`
              : `Loading… (${state.records.length.toLocaleString()} records)`
          );
          await new Promise((r) => setTimeout(r, 0));
        }
      }
    }

    buf += decoder.decode();
    const tail = buf.trim();
    if (tail) {
      try {
        const obj = JSON.parse(tail);
        state.records.push(normalizeRecord(obj));
      } catch {
        parseErrors++;
      }
    }

    rebuildFilters();
    applyFiltersAndSort();
    renderTable();

    if (els.downloadExcelBtn) els.downloadExcelBtn.disabled = state.filteredIdx.length === 0;

    // Keep status quiet after load; table/resultInfo already shows counts.
    setStatus(parseErrors ? `Idle (parse errors: ${parseErrors})` : "Idle");

    if (kind !== "archive") {
      await loadRunInfo();
    } else {
      if (els.runInfo) els.runInfo.textContent = "";
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

function resetFilters() {
  els.sourceFilter.value = "";
  els.domainFilter.value = "";
  els.searchInput.value = "";
  state.sortKey = "discovered_at_utc";
  state.sortDir = "desc";
  state.page = 1;
  applyFiltersAndSort();
  renderTable();
}

function wireEvents() {
  if (els.saveRepoBtn && els.repoInput) {
    els.saveRepoBtn.addEventListener("click", async () => {
      saveRepo(els.repoInput.value);
      await loadArchiveIndex();
      await loadDataset();
    });

    els.repoInput.addEventListener("keydown", async (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        saveRepo(els.repoInput.value);
        await loadArchiveIndex();
        await loadDataset();
      }
    });
  }

  els.datasetSelect.addEventListener("change", async () => {
    const kind = els.datasetSelect.value;
    els.archiveField.hidden = kind !== "archive";
    if (kind === "archive" && state.archiveIndex.length === 0) {
      await loadArchiveIndex();
    }
    state.selectedArchivePath = String(els.archiveSelect.value || "");
    await loadDataset();
  });

  els.archiveSelect.addEventListener("change", async () => {
    state.selectedArchivePath = String(els.archiveSelect.value || "");
    await loadDataset();
  });

  els.sourceFilter.addEventListener("change", () => {
    applyFiltersAndSort();
    renderTable();
  });

  els.domainFilter.addEventListener("change", () => {
    applyFiltersAndSort();
    renderTable();
  });

  let searchTimer = null;
  els.searchInput.addEventListener("input", () => {
    if (searchTimer) window.clearTimeout(searchTimer);
    searchTimer = window.setTimeout(() => {
      applyFiltersAndSort();
      renderTable();
    }, 150);
  });

  els.pageSizeSelect.addEventListener("change", () => {
    state.pageSize = Number(els.pageSizeSelect.value) || 50;
    state.page = 1;
    renderTable();
  });

  els.prevBtn.addEventListener("click", () => {
    state.page -= 1;
    renderTable();
  });

  const goToPage = () => {
    const v = Number(els.pageInput?.value || "0") || 0;
    if (v > 0) state.page = v;
    renderTable();
  };

  els.goPageBtn?.addEventListener("click", () => goToPage());
  els.pageInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      goToPage();
    }
  });

  els.nextBtn.addEventListener("click", () => {
    state.page += 1;
    renderTable();
  });

  els.reloadBtn.addEventListener("click", async () => {
    await loadDataset();
  });

  els.downloadJsonLink?.addEventListener("click", () => {
    closeDownloadMenu();
  });

  els.downloadExcelBtn?.addEventListener("click", () => {
    downloadExcel();
    closeDownloadMenu();
  });

  els.resetBtn.addEventListener("click", () => resetFilters());

  document.querySelectorAll("th[data-sort]").forEach((th) => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (!key) return;
      if (state.sortKey === key) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDir = key === "discovered_at_utc" ? "desc" : "asc";
      }
      applyFiltersAndSort();
      renderTable();
    });
  });

  // Close the record dialog when clicking the backdrop.
  els.detailDialog.addEventListener("click", (e) => {
    if (e.target === els.detailDialog) {
      els.detailDialog.close();
    }
  });

  // Close the download menu when clicking outside of it.
  document.addEventListener("click", (e) => {
    if (!els.downloadMenu || !els.downloadMenu.open) return;
    const t = /** @type {any} */ (e.target);
    if (t && typeof els.downloadMenu.contains === "function" && els.downloadMenu.contains(t)) return;
    closeDownloadMenu();
  });
}

async function main() {
  await loadViewerConfig();
  loadRepoFromQueryOrStorage();
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
