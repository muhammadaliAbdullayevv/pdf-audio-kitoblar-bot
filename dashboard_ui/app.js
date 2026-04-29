// Demo-only fallback data for local UI development.
// It is intentionally generic and should not be treated as live production analytics.
const fallbackTodayIso = new Date().toISOString().slice(0, 10);

const FALLBACK_DATA = {
  generated_at: new Date().toISOString(),
  range: {
    key: "today",
    label: "Today",
    start_date: fallbackTodayIso,
    end_date: fallbackTodayIso,
    days: 1,
  },
  services: {
    bot_api: { status: "ok", label: "Bot API Online", detail: "sample local endpoint" },
    db: { status: "ok", label: "PostgreSQL Healthy", detail: "sample dataset" },
    es: { status: "ok", label: "ES Green", detail: "sample index stats" },
  },
  bot_runtime: {
    last_restart_epoch: Math.floor(Date.now() / 1000) - 5400,
    last_restart_iso: "",
    uptime_sec: 5400,
    status: "active",
    pid: 12345,
  },
  kpis: {
    users_current: { value: 120, change: "sample only", scope: "lifetime" },
    users_new: { value: 8, change: "+12.0% vs previous range", scope: "range" },
    users_left: { value: 2, change: "-20.0% vs previous range", scope: "range" },
    books_total: { value: 540, change: "12 not indexed", scope: "lifetime" },
    books_new: { value: 18, change: "+8.0% vs previous range", scope: "range" },
    book_searches: { value: 96, change: "+4.0% vs previous range", scope: "range" },
    book_downloads: { value: 41, change: "+3.0% vs previous range", scope: "range" },
    searches: { value: 110, change: "+5.0% vs previous range", scope: "range" },
    downloads_total: { value: 49, change: "+2.0% vs previous range", scope: "range" },
    audios_total: { value: 75, change: "640 audio parts", scope: "lifetime" },
    audios_new: { value: 6, change: "+6.0% vs previous range", scope: "range" },
  },
  catalog_growth: {
    labels: ["Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6", "Day 7"],
    books_new: [5, 8, 6, 4, 9, 7, 3],
    audio_new: [2, 3, 1, 2, 4, 2, 1],
    unindexed_new: [0, 1, 0, 0, 1, 0, 0],
  },
  retention: {
    dau: 14,
    wau: 52,
    mau: 120,
    active_in_range: 52,
    active_prev_in_range: 48,
    stickiness_pct: 26.9,
    dau_change: "+4.0%",
    wau_change: "+6.0%",
    mau_change: "+8.0%",
    active_change: "+8.3%",
    window_days: { range: 7, wau: 7, mau: 7 },
  },
  funnel: {
    new_users: 8,
    active_users: 52,
    search_users: 41,
    download_users: 29,
    steps: [
      { label: "New users", value: 8, pct: 100 },
      { label: "Active users", value: 52, pct: 100 },
      { label: "Search users", value: 41, pct: 78.8 },
      { label: "Download users", value: 29, pct: 70.7 },
    ],
    new_to_active_pct: 100,
    active_to_search_pct: 78.8,
    search_to_download_pct: 70.7,
  },
  search_quality: {
    searches_total: 110,
    downloads_total: 49,
    book_searches_total: 96,
    book_downloads_total: 41,
    conversion_pct: 42.7,
    request_queries_total: 11,
    zero_result_total: 4,
    zero_result_rate_pct: 9.1,
    top_queries: [
      { query: "Atomic Habits", count: 4 },
      { query: "Shaytanat", count: 3 },
      { query: "Rich Dad Poor Dad", count: 2 },
    ],
    zero_result_queries: [
      { query: "Sample missing title", count: 2 },
      { query: "Another sample query", count: 1 },
    ],
  },
  queue_sla: {
    pending_upload_count: 1,
    oldest_pending_age_sec: 900,
    avg_resolve_sec: 600,
    avg_accept_sec: 420,
    avg_reject_sec: 240,
    avg_book_request_resolve_sec: 1800,
  },
  catalog: {
    books_total: 540,
    books_indexed: 528,
    books_unindexed: 12,
    books_index_ratio: 97.8,
    books_downloads_total: 3200,
    books_searches_total: 4700,
    audio_books_total: 75,
    audio_books_with_source_books: 58,
    audio_parts_total: 640,
    audio_duration_seconds: 540000,
  },
  requests: {
    book: {
      total: 16,
      segments: [
        { key: "open", label: "Open", value: 3, pct: 18.8, color: "#3f88c5" },
        { key: "seen", label: "Seen", value: 2, pct: 12.5, color: "#9b8de5" },
        { key: "done", label: "Done", value: 9, pct: 56.2, color: "#2ca58d" },
        { key: "no", label: "No", value: 2, pct: 12.5, color: "#ef6f6c" },
      ],
    },
    upload: {
      total: 9,
      pending: 1,
      segments: [
        { key: "open", label: "Open", value: 1, pct: 11.1, color: "#3f88c5" },
        { key: "accept", label: "Accepted", value: 6, pct: 66.7, color: "#2ca58d" },
        { key: "reject", label: "Rejected", value: 2, pct: 22.2, color: "#ef6f6c" },
      ],
    },
    queue_pending: 1,
  },
  storage: {
    total_files: 1180,
    total_size: 7340032000,
    book_count: 540,
    total_book_size: 5242880000,
    audio_count: 640,
    total_audio_size: 2097152000,
    avg_book_size: 9718296,
    avg_audio_size: 3276800,
    book_size_ratio: 71.4,
  },
  engagement: {
    favorites_total: 210,
    favorites_added_total: 560,
    reaction_total_current: 320,
    reaction_total_lifetime: 840,
    favorites_per_active_user: 4.0,
    reactions_per_active_user: 6.2,
    book_reactions_current: { like: 140, dislike: 28, berry: 36, whale: 18 },
    book_reactions_lifetime: { like: 360, dislike: 72, berry: 94, whale: 44 },
  },
  downloader: {
    success_total: 42,
    fail_total: 3,
    attempts_total: 45,
    success_rate: 93.3,
    avg_processing_sec: 18.6,
    platform: [
      { platform: "youtube", success: 22, fail: 2, total: 24, success_rate: 91.7 },
      { platform: "instagram", success: 12, fail: 1, total: 13, success_rate: 92.3 },
      { platform: "generic", success: 8, fail: 0, total: 8, success_rate: 100.0 },
    ],
    fail_reasons: [
      { reason: "timeout", count: 1 },
      { reason: "network", count: 1 },
      { reason: "invalid", count: 1 },
    ],
    recent_video_errors: 1,
    recent_video_warns: 2,
    recent_issues: [
      // Sample-only fallback
    ],
  },
  audience: {
    languages: [
      { language: "uz", label: "Uzbek", total_users: 72, active_users: 31, share_pct: 60.0, active_pct: 43.1 },
      { language: "en", label: "English", total_users: 30, active_users: 13, share_pct: 25.0, active_pct: 43.3 },
      { language: "ru", label: "Russian", total_users: 18, active_users: 8, share_pct: 15.0, active_pct: 44.4 },
    ],
    geo_available: false,
    geo_note: "Sample-only locale split. Geo analytics are not enabled.",
  },
  infra: {
    cpu: { cores: 4, load1: 0.42, load5: 0.38, load15: 0.31, load_pct: 10.5 },
    memory: { total: 8589934592, available: 4294967296, used: 4294967296, used_pct: 50.0 },
    disk: { total: 268435456000, used: 107374182400, free: 161061273600, used_pct: 40.0 },
    network: { rx_rate_bps: 24576.0, tx_rate_bps: 16384.0 },
    process_uptime_sec: 5400,
    system_uptime_sec: 86400,
    services: [
      { unit: "pdf_audio_kitoblar_bot.service", status: "active", substatus: "running", restarts: 0, uptime_sec: 7200, pid: 2101 },
      { unit: "pdf_audio_kitoblar_bot-bot.service", status: "active", substatus: "running", restarts: 1, uptime_sec: 5400, pid: 2202 },
      { unit: "pdf_audio_kitoblar_bot-dashboard.service", status: "active", substatus: "running", restarts: 0, uptime_sec: 5000, pid: 2303 },
    ],
  },
  reliability: {
    log_errors: 1,
    log_warns: 3,
    video_errors: 0,
    video_warns: 1,
  },
  events: [
    { time: "10:14:12", service: "search_flow", message: "sample cache hit: 12ms", status: "ok" },
    { time: "10:10:05", service: "dashboard_server", message: "sample payload refresh complete", status: "ok" },
    { time: "10:08:32", service: "elasticsearch", message: "sample warning event", status: "warn" },
  ],
  commands: [
    { name: "/start", count: 120 },
    { name: "Search Books", count: 96 },
    { name: "Favorites", count: 34 },
    { name: "Other Functions", count: 28 },
  ],
};

let dashboardData = structuredClone(FALLBACK_DATA);
let isLiveData = false;
let refreshTimer = null;
let currentRange = "today";
let currentView = "overview";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(Number(value || 0));
}

function formatPercent(value, digits = 1) {
  return `${Number(value || 0).toFixed(digits)}%`;
}

function formatBytes(bytes) {
  const b = Number(bytes || 0);
  if (!Number.isFinite(b) || b <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = b;
  let idx = 0;
  while (n >= 1024 && idx < units.length - 1) {
    n /= 1024;
    idx += 1;
  }
  const digits = n >= 100 || idx === 0 ? 0 : n >= 10 ? 1 : 2;
  return `${n.toFixed(digits)} ${units[idx]}`;
}

function formatDuration(seconds) {
  const total = Math.max(0, Number(seconds || 0));
  const hrs = Math.floor(total / 3600);
  const mins = Math.floor((total % 3600) / 60);
  const secs = Math.floor(total % 60);
  if (hrs > 0) return `${hrs}h ${mins}m`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

function statusClass(status) {
  const raw = String(status || "").toLowerCase();
  if (raw === "ok") return "status-chip--ok";
  if (raw === "warn") return "status-chip--warn";
  return "status-chip--err";
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (!node) return;
  node.textContent = String(value ?? "");
}

function setServiceChip(id, svcData) {
  const node = document.getElementById(id);
  if (!node) return;
  node.className = `status-chip ${statusClass(svcData?.status)}`;
  const label = escapeHtml(svcData?.label || "Unknown");
  const detail = svcData?.detail ? ` (${escapeHtml(svcData.detail)})` : "";
  node.innerHTML = `<span class="dot"></span>${label}${detail}`;
}

function setServices() {
  const services = dashboardData.services || {};
  setServiceChip("svc-bot-api", services.bot_api);
  setServiceChip("svc-db", services.db);
  setServiceChip("svc-es", services.es);
}

function setRangeMeta() {
  const range = dashboardData.range || {};
  setText("range-label", `Range: ${range.label || "Today"}`);
  setText(
    "range-subtitle",
    `${range.start_date || "-"} -> ${range.end_date || "-"} (${formatNumber(range.days || 0)} days)`
  );
}

function setRangeContextLabels() {
  const range = dashboardData.range || {};
  const label = String(range.label || "Today");

  document.querySelectorAll("[data-range-badge]").forEach((node) => {
    node.textContent = label;
  });

  document.querySelectorAll("[data-range-aware]").forEach((node) => {
    const el = node;
    const base = el.dataset.baseText || String(el.textContent || "").replace(/\s+\([^)]+\)\s*$/, "").trim();
    el.dataset.baseText = base;
    el.textContent = `${base} (${label})`;
  });
}

function setKpis() {
  const kpis = dashboardData.kpis || {};
  if (!kpis.book_searches && kpis.searches) {
    kpis.book_searches = kpis.searches;
  }
  if (!kpis.book_downloads && kpis.downloads_total) {
    kpis.book_downloads = kpis.downloads_total;
  }
  const rangeLabel = String((dashboardData.range || {}).label || "selected range");
  const mappings = [
    ["users_current", "kpi-users-current", "kpi-users-current-meta"],
    ["users_new", "kpi-users-new", "kpi-users-new-meta"],
    ["users_left", "kpi-users-left", "kpi-users-left-meta"],
    ["books_total", "kpi-books-total", "kpi-books-total-meta"],
    ["books_new", "kpi-books-new", "kpi-books-new-meta"],
    ["book_searches", "kpi-book-searches", "kpi-book-searches-meta"],
    ["book_downloads", "kpi-book-downloads", "kpi-book-downloads-meta"],
    ["audios_total", "kpi-audios-total", "kpi-audios-total-meta"],
    ["audios_new", "kpi-audios-new", "kpi-audios-new-meta"],
  ];

  mappings.forEach(([key, valueId, metaId]) => {
    const data = kpis[key] || { value: 0, change: "" };
    const rawScope = String(data.scope || "").toLowerCase();
    const scopeLabel = rawScope === "lifetime"
      ? "Lifetime"
      : rawScope === "range"
        ? rangeLabel
        : rawScope === "mixed"
          ? `Mixed (${rangeLabel})`
          : "";
    const metaText = scopeLabel
      ? `${data.change || ""}${data.change ? " | " : ""}${scopeLabel}`
      : (data.change || "");
    setText(valueId, formatNumber(data.value || 0));
    setText(metaId, metaText);
  });
}

function setCatalog() {
  const catalog = dashboardData.catalog || FALLBACK_DATA.catalog;
  const booksRatio = Number(catalog.books_index_ratio || 0);
  const audioRatio = Number(catalog.books_total || 0) > 0
    ? ((Number(catalog.audio_books_with_source_books || 0) / Number(catalog.books_total || 1)) * 100)
    : 0;

  setText("catalog-books-ratio", formatPercent(booksRatio));
  setText("catalog-audio-ratio", formatPercent(audioRatio));

  const booksMeter = document.getElementById("catalog-books-meter");
  const audioMeter = document.getElementById("catalog-audio-meter");
  if (booksMeter) booksMeter.style.width = `${Math.max(0, Math.min(100, booksRatio))}%`;
  if (audioMeter) audioMeter.style.width = `${Math.max(0, Math.min(100, audioRatio))}%`;

  setText("catalog-books-meta", `${formatNumber(catalog.books_indexed || 0)} indexed / ${formatNumber(catalog.books_total || 0)} total`);
  setText("catalog-audio-meta", `${formatNumber(catalog.audio_books_with_source_books || 0)} linked audiobook books`);

  setText("catalog-books-unindexed", formatNumber(catalog.books_unindexed || 0));
  setText("catalog-books-downloads", formatNumber(catalog.books_downloads_total || 0));
  setText("catalog-books-searches", formatNumber(catalog.books_searches_total || 0));
  setText("catalog-audio-parts", formatNumber(catalog.audio_parts_total || 0));
  setText("catalog-audio-duration", formatDuration(catalog.audio_duration_seconds || 0));
}

function renderStackedBar(barId, legendId, segments) {
  const bar = document.getElementById(barId);
  const legend = document.getElementById(legendId);
  if (!bar || !legend) return;

  const list = Array.isArray(segments) && segments.length ? segments : [];
  const total = list.reduce((acc, seg) => acc + Number(seg.value || 0), 0);

  bar.innerHTML = "";
  legend.innerHTML = "";

  if (total <= 0) {
    const empty = document.createElement("span");
    empty.className = "stack-empty";
    empty.textContent = "No data";
    bar.appendChild(empty);
  }

  list.forEach((segment) => {
    const value = Number(segment.value || 0);
    const pct = total > 0 && value > 0 ? (value / total) * 100 : 0;

    if (total > 0) {
      const part = document.createElement("span");
      part.className = "stack-segment";
      part.style.width = `${pct}%`;
      part.style.background = String(segment.color || "#7f8c9f");
      part.title = `${segment.label}: ${formatNumber(value)} (${pct.toFixed(1)}%)`;
      bar.appendChild(part);
    }

    const li = document.createElement("li");
    li.innerHTML = `
      <span class="legend-name">
        <span class="legend-dot" style="background:${escapeHtml(segment.color || "#7f8c9f")}"></span>
        ${escapeHtml(segment.label || "Unknown")}
      </span>
      <strong>${formatNumber(value)} (${pct.toFixed(1)}%)</strong>
    `;
    legend.appendChild(li);
  });
}

function setRequests() {
  const requests = dashboardData.requests || FALLBACK_DATA.requests;
  const book = requests.book || FALLBACK_DATA.requests.book;
  const upload = requests.upload || FALLBACK_DATA.requests.upload;

  setText("request-book-total", `${formatNumber(book.total || 0)} total`);
  setText("request-upload-total", `${formatNumber(upload.total || 0)} total`);
  setText("request-queue-pending", `Queue pending: ${formatNumber(requests.queue_pending || upload.pending || 0)}`);

  renderStackedBar("request-book-bar", "request-book-legend", book.segments || []);
  renderStackedBar("request-upload-bar", "request-upload-legend", upload.segments || []);
}

function setStorage() {
  const storage = dashboardData.storage || FALLBACK_DATA.storage;

  setText("storage-total-size", formatBytes(storage.total_size || 0));
  setText("storage-total-files", formatNumber(storage.total_files || 0));
  setText("storage-book-size", formatBytes(storage.total_book_size || 0));
  setText("storage-audio-size", formatBytes(storage.total_audio_size || 0));
  setText("storage-avg-book-size", formatBytes(storage.avg_book_size || 0));
  setText("storage-avg-audio-size", formatBytes(storage.avg_audio_size || 0));
  setText("storage-book-share", formatPercent(storage.book_size_ratio || 0));

  const meter = document.getElementById("storage-book-share-meter");
  if (meter) {
    meter.style.width = `${Math.max(0, Math.min(100, Number(storage.book_size_ratio || 0)))}%`;
  }
}

function renderListRows(listId, rows, formatter, emptyText = "No data") {
  const node = document.getElementById(listId);
  if (!node) return;
  node.innerHTML = "";

  if (!Array.isArray(rows) || !rows.length) {
    const li = document.createElement("li");
    li.textContent = emptyText;
    node.appendChild(li);
    return;
  }

  rows.forEach((row) => {
    const li = document.createElement("li");
    li.innerHTML = formatter(row);
    node.appendChild(li);
  });
}

function setEngagement() {
  const engagement = dashboardData.engagement || FALLBACK_DATA.engagement;
  const downloader = dashboardData.downloader || FALLBACK_DATA.downloader;

  setText("eng-favorites-current", formatNumber(engagement.favorites_total || 0));
  setText("eng-favorites-added", formatNumber(engagement.favorites_added_total || 0));
  setText("eng-reactions-current", formatNumber(engagement.reaction_total_current || 0));
  setText("eng-reactions-lifetime", formatNumber(engagement.reaction_total_lifetime || 0));
  setText("downloader-health", `${formatPercent(downloader.success_rate || 0)} (${formatNumber(downloader.success_total || 0)}/${formatNumber(downloader.attempts_total || 0)})`);

  const reactionRows = [
    { label: "Book reactions now", value: Object.values(engagement.book_reactions_current || {}).reduce((acc, x) => acc + Number(x || 0), 0) },
    { label: "Favorites per active user", value: Number(engagement.favorites_per_active_user || 0).toFixed(2) },
    { label: "Reactions per active user", value: Number(engagement.reactions_per_active_user || 0).toFixed(2) },
  ];
  renderListRows("reaction-mix-list", reactionRows, (row) => `<span>${escapeHtml(row.label)}</span><strong>${escapeHtml(row.value)}</strong>`);

  const issues = Array.isArray(downloader.recent_issues) ? downloader.recent_issues : [];
  renderListRows(
    "downloader-issues-list",
    issues,
    (issue) => {
      const state = String(issue.status || "warn").toUpperCase();
      return `<span>[${escapeHtml(state)}] ${escapeHtml(issue.time || "--:--")}</span><strong>${escapeHtml(issue.message || "")}</strong>`;
    },
    "No recent downloader issues"
  );
}

function setRetention() {
  const r = dashboardData.retention || FALLBACK_DATA.retention;
  const windows = r.window_days || {};
  const rangeDays = Number(windows.range || (dashboardData.range || {}).days || 1);
  const wauDays = Number(windows.wau || Math.min(7, rangeDays || 7));
  const mauDays = Number(windows.mau || Math.min(30, rangeDays || 30));
  setText("ret-dau", formatNumber(r.dau || 0));
  setText("ret-wau", formatNumber(r.wau || 0));
  setText("ret-mau", formatNumber(r.mau || 0));
  setText("ret-active-range", formatNumber(r.active_in_range || 0));
  setText("ret-stickiness", formatPercent(r.stickiness_pct || 0));
  setText("ret-dau-change", `${r.dau_change || "0.0%"} vs prev day`);
  setText("ret-wau-change", `${r.wau_change || "0.0%"} vs prev ${formatNumber(wauDays)}d`);
  setText("ret-mau-change", `${r.mau_change || "0.0%"} vs prev ${formatNumber(mauDays)}d`);
  setText("ret-active-range-change", `${r.active_change || "0.0%"} vs prev ${formatNumber(rangeDays)}d`);
}

function setFunnel() {
  const f = dashboardData.funnel || FALLBACK_DATA.funnel;
  setText("funnel-new", formatNumber(f.new_users || 0));
  setText("funnel-active", formatNumber(f.active_users || 0));
  setText("funnel-search", formatNumber(f.search_users || 0));
  setText("funnel-download", formatNumber(f.download_users || 0));
  setText(
    "funnel-rates",
    `new->active ${formatPercent(f.new_to_active_pct || 0)} | active->search ${formatPercent(f.active_to_search_pct || 0)} | search->download ${formatPercent(f.search_to_download_pct || 0)}`
  );

  renderListRows(
    "funnel-steps",
    Array.isArray(f.steps) ? f.steps : [],
    (step) => `<span>${escapeHtml(step.label || "step")}</span><strong>${formatNumber(step.value || 0)} (${formatPercent(step.pct || 0)})</strong>`
  );
}

function setSearchQuality() {
  const q = dashboardData.search_quality || FALLBACK_DATA.search_quality;
  const bookSearches = Number(q.book_searches_total != null ? q.book_searches_total : q.searches_total || 0);
  const bookDownloads = Number(q.book_downloads_total != null ? q.book_downloads_total : q.downloads_total || 0);
  const conversionPct = Number.isFinite(Number(q.conversion_pct))
    ? Number(q.conversion_pct)
    : (bookSearches > 0 ? (bookDownloads / bookSearches) * 100 : 0);

  setText("search-book-total", formatNumber(bookSearches));
  setText("search-book-download-total", formatNumber(bookDownloads));
  setText("search-conversion", formatPercent(conversionPct));
  setText("search-request-queries", formatNumber(q.request_queries_total || 0));
  setText("search-zero-rate", `${formatPercent(q.zero_result_rate_pct || 0)} (${formatNumber(q.zero_result_total || 0)})`);

  renderListRows(
    "top-queries-list",
    Array.isArray(q.top_queries) ? q.top_queries : [],
    (row) => `<span>${escapeHtml(row.query || "")}</span><strong>${formatNumber(row.count || 0)}</strong>`
  );
  renderListRows(
    "zero-queries-list",
    Array.isArray(q.zero_result_queries) ? q.zero_result_queries : [],
    (row) => `<span>${escapeHtml(row.query || "")}</span><strong>${formatNumber(row.count || 0)}</strong>`
  );
}

function setDownloaderQuality() {
  const d = dashboardData.downloader || FALLBACK_DATA.downloader;
  setText("dl-success-total", formatNumber(d.success_total || 0));
  setText("dl-fail-total", formatNumber(d.fail_total || 0));
  setText("dl-success-rate", formatPercent(d.success_rate || 0));
  setText("dl-avg-sec", `${Number(d.avg_processing_sec || 0).toFixed(2)}s`);

  const pbody = document.getElementById("dl-platform-body");
  if (pbody) {
    pbody.innerHTML = "";
    const rows = Array.isArray(d.platform) ? d.platform : [];
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(String(row.platform || "generic"))}</td>
        <td>${formatNumber(row.success || 0)}</td>
        <td>${formatNumber(row.fail || 0)}</td>
        <td>${formatPercent(row.success_rate || 0)}</td>
      `;
      pbody.appendChild(tr);
    });
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td colspan=\"4\">No data</td>";
      pbody.appendChild(tr);
    }
  }

  renderListRows(
    "dl-reasons-list",
    Array.isArray(d.fail_reasons) ? d.fail_reasons : [],
    (row) => `<span>${escapeHtml(String(row.reason || "other"))}</span><strong>${formatNumber(row.count || 0)}</strong>`
  );
}

function setQueueSla() {
  const s = dashboardData.queue_sla || FALLBACK_DATA.queue_sla;
  setText("sla-pending", formatNumber(s.pending_upload_count || 0));
  setText("sla-oldest", formatDuration(s.oldest_pending_age_sec || 0));
  setText("sla-avg-resolve", formatDuration(s.avg_resolve_sec || 0));
  setText("sla-avg-accept", formatDuration(s.avg_accept_sec || 0));
  setText("sla-avg-reject", formatDuration(s.avg_reject_sec || 0));
  setText("sla-book-resolve", formatDuration(s.avg_book_request_resolve_sec || 0));
}

function setCatalogGrowth() {
  const g = dashboardData.catalog_growth || FALLBACK_DATA.catalog_growth;
  const labels = Array.isArray(g.labels) ? g.labels : [];
  const books = Array.isArray(g.books_new) ? g.books_new : [];
  const audio = Array.isArray(g.audio_new) ? g.audio_new : [];
  const unindexed = Array.isArray(g.unindexed_new) ? g.unindexed_new : [];

  const tbody = document.getElementById("growth-table-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  const start = Math.max(0, labels.length - 10);
  for (let i = start; i < labels.length; i += 1) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(labels[i] || "-")}</td>
      <td>${formatNumber(books[i] || 0)}</td>
      <td>${formatNumber(audio[i] || 0)}</td>
      <td>${formatNumber(unindexed[i] || 0)}</td>
    `;
    tbody.appendChild(tr);
  }
  if (!labels.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = "<td colspan=\"4\">No growth data in this range</td>";
    tbody.appendChild(tr);
  }
}

function setAudience() {
  const a = dashboardData.audience || FALLBACK_DATA.audience;
  const languages = Array.isArray(a.languages) ? a.languages : [];
  renderListRows(
    "audience-lang-list",
    languages,
    (row) => `<span>${escapeHtml(row.label || row.language || "unknown")}</span><strong>${formatNumber(row.total_users || 0)} users (${formatPercent(row.share_pct || 0)}), active ${formatNumber(row.active_users || 0)} (${formatPercent(row.active_share_pct || row.active_pct || 0)})</strong>`
  );

  setText("audience-geo-note", a.geo_note || "");
}

function setInfra() {
  const infra = dashboardData.infra || FALLBACK_DATA.infra;
  const cpu = infra.cpu || {};
  const mem = infra.memory || {};
  const disk = infra.disk || {};
  const net = infra.network || {};

  setText("infra-cpu", `${formatPercent(cpu.load_pct || 0)} (L1 ${Number(cpu.load1 || 0).toFixed(2)} / ${formatNumber(cpu.cores || 0)} cores)`);
  setText("infra-mem", `${formatPercent(mem.used_pct || 0)} (${formatBytes(mem.used || 0)} / ${formatBytes(mem.total || 0)})`);
  setText("infra-disk", `${formatPercent(disk.used_pct || 0)} (${formatBytes(disk.used || 0)} / ${formatBytes(disk.total || 0)})`);
  setText("infra-net", `${formatBytes(net.rx_rate_bps || 0)}/s down | ${formatBytes(net.tx_rate_bps || 0)}/s up`);
  setText("infra-proc-up", formatDuration(infra.process_uptime_sec || 0));
  setText("infra-sys-up", formatDuration(infra.system_uptime_sec || 0));

  const tbody = document.getElementById("infra-service-body");
  if (tbody) {
    tbody.innerHTML = "";
    const rows = Array.isArray(infra.services) ? infra.services : [];
    rows.forEach((svc) => {
      const active = String(svc.status || "").toLowerCase() === "active";
      const badge = active ? "badge--ok" : "badge--err";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${escapeHtml(String(svc.unit || "service"))}</td>
        <td><span class="badge ${badge}">${escapeHtml(String(svc.status || "unknown"))}</span></td>
        <td>${formatNumber(svc.restarts || 0)}</td>
        <td>${formatDuration(svc.uptime_sec || 0)}</td>
        <td>${formatNumber(svc.pid || 0)}</td>
      `;
      tbody.appendChild(tr);
    });
    if (!rows.length) {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td colspan=\"5\">No service runtime data</td>";
      tbody.appendChild(tr);
    }
  }
}

function renderEvents() {
  const tbody = document.getElementById("events-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  const events = Array.isArray(dashboardData.events) && dashboardData.events.length
    ? dashboardData.events
    : FALLBACK_DATA.events;

  events.forEach((event) => {
    const tr = document.createElement("tr");
    const rawStatus = String(event.status || "").toLowerCase();
    const badge = rawStatus === "ok" ? "badge--ok" : rawStatus === "warn" ? "badge--warn" : "badge--err";
    const statusLabel = rawStatus === "ok" ? "OK" : rawStatus === "warn" ? "WARN" : "ERR";

    tr.innerHTML = `
      <td>${escapeHtml(event.time || "")}</td>
      <td>${escapeHtml(event.service || "")}</td>
      <td>${escapeHtml(event.message || "")}</td>
      <td><span class="badge ${badge}">${statusLabel}</span></td>
    `;
    tbody.appendChild(tr);
  });
}

function renderCommandBars() {
  const list = document.getElementById("cmd-bars");
  if (!list) return;
  list.innerHTML = "";

  const commands = Array.isArray(dashboardData.commands) && dashboardData.commands.length
    ? dashboardData.commands
    : FALLBACK_DATA.commands;
  const allZero = !commands.length || commands.every((cmd) => Number(cmd.count || 0) <= 0);
  if (allZero) {
    const li = document.createElement("li");
    const rangeLabel = String((dashboardData.range || {}).label || "selected range");
    li.innerHTML = `
      <div class="cmd-label">
        <span>No feature activity in ${escapeHtml(rangeLabel)}</span>
        <strong>0</strong>
      </div>
      <div class="bar-track"><div class="bar-fill" style="width:0%"></div></div>
    `;
    list.appendChild(li);
    return;
  }
  const max = Math.max(...commands.map((cmd) => Number(cmd.count || 0)), 1);

  commands.forEach((cmd) => {
    const count = Number(cmd.count || 0);
    const pct = Number(cmd.pct || 0);
    const width = (count / max) * 100;
    const li = document.createElement("li");
    li.innerHTML = `
      <div class="cmd-label">
        <span>${escapeHtml(cmd.name || "Unknown")}</span>
        <strong>${formatNumber(count)}${pct > 0 ? ` (${formatPercent(pct, 1)})` : ""}</strong>
      </div>
      <div class="bar-track">
        <div class="bar-fill" style="width:${width.toFixed(1)}%"></div>
      </div>
    `;
    list.appendChild(li);
  });
}

function setTimestamp() {
  const source = dashboardData.generated_at ? new Date(dashboardData.generated_at) : new Date();
  const dtf = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  setText("last-updated", `Updated ${dtf.format(source)}`);
  setText("data-mode", isLiveData ? "Data source: live backend" : "Data source: fallback demo");

  const reliability = dashboardData.reliability || {};
  setText(
    "reliability-note",
    `Log signals: ERR ${formatNumber(reliability.log_errors || 0)} | WARN ${formatNumber(reliability.log_warns || 0)} | Video ERR ${formatNumber(reliability.video_errors || 0)}`
  );

  const runtime = dashboardData.bot_runtime || {};
  const runtimeUptime = formatDuration(runtime.uptime_sec || 0);
  let restartDate = null;
  if (runtime.last_restart_epoch) {
    restartDate = new Date(Number(runtime.last_restart_epoch) * 1000);
  } else if (runtime.last_restart_iso) {
    restartDate = new Date(String(runtime.last_restart_iso));
  }

  const restartNode = document.getElementById("bot-restart-note");
  if (restartNode) {
    if (restartDate && !Number.isNaN(restartDate.getTime())) {
      restartNode.textContent = `Bot restart: ${dtf.format(restartDate)} | Uptime: ${runtimeUptime}`;
    } else if ((runtime.uptime_sec || 0) > 0) {
      restartNode.textContent = `Bot uptime: ${runtimeUptime}`;
    } else {
      restartNode.textContent = "Bot runtime: unavailable";
    }
  }
}

function setRangeButtonsState() {
  document.querySelectorAll(".range-btn[data-range]").forEach((btn) => {
    btn.classList.toggle("range-btn--active", btn.dataset.range === currentRange);
  });
}

function applyView(view, scroll = false) {
  currentView = view;

  const buttons = document.querySelectorAll(".nav-btn[data-view]");
  buttons.forEach((btn) => {
    btn.classList.toggle("nav-btn--active", btn.dataset.view === view);
  });

  const blocks = document.querySelectorAll("[data-views]");
  const matchingBlocks = [];
  blocks.forEach((block) => {
    const views = String(block.getAttribute("data-views") || "overview")
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);
    const match = view === "overview" ? true : views.includes(view);
    block.classList.toggle("panel-muted", !match);
    block.classList.toggle("panel-hidden", !match);
    if (match) matchingBlocks.push(block);
  });

  if (scroll) {
    const activeBtn = Array.from(buttons).find((btn) => btn.dataset.view === view);
    const targetId = activeBtn?.dataset.target;
    const preferredTarget = targetId ? document.getElementById(targetId) : null;
    const target = preferredTarget && !preferredTarget.classList.contains("panel-hidden")
      ? preferredTarget
      : matchingBlocks[0] || null;
    if (target) {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }
}

function initQuickViews() {
  const buttons = document.querySelectorAll(".nav-btn[data-view]");
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      applyView(String(btn.dataset.view || "overview"), true);
    });
  });
  applyView(currentView, false);
}

function initRangeControls() {
  const buttons = document.querySelectorAll(".range-btn[data-range]");
  buttons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const next = String(btn.dataset.range || "today");
      if (next === currentRange) return;
      currentRange = next;
      setRangeButtonsState();
      await refreshLoop();
    });
  });
  setRangeButtonsState();
}

async function fetchDashboardData() {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 7000);
  try {
    const res = await fetch(`/api/dashboard?range=${encodeURIComponent(currentRange)}`, {
      method: "GET",
      cache: "no-store",
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }

    const payload = await res.json();
    if (!payload || typeof payload !== "object") {
      throw new Error("invalid payload");
    }

    dashboardData = payload;
    isLiveData = true;
  } catch (err) {
    console.warn("dashboard fetch failed, fallback mode:", err);
    dashboardData = structuredClone(FALLBACK_DATA);
    dashboardData.generated_at = new Date().toISOString();
    const now = new Date();
    const iso = (d) => d.toISOString().slice(0, 10);
    const start = new Date(now);
    let days = 1;
    let fallbackLabel = "Today";
    if (currentRange === "today") fallbackLabel = "Today";
    else if (currentRange === "week") {
      fallbackLabel = "This week";
      days = 7;
      start.setDate(now.getDate() - 6);
    } else if (currentRange === "month") {
      fallbackLabel = "This month";
      days = 30;
      start.setDate(now.getDate() - 29);
    } else if (currentRange === "all") {
      fallbackLabel = "All time";
      days = Math.max(1, Number(FALLBACK_DATA.range?.days || 365));
      start.setDate(now.getDate() - (days - 1));
    } else if (/^\\d+d$/.test(currentRange)) {
      days = Math.max(1, Number(currentRange.replace("d", "")));
      fallbackLabel = `Last ${days} days`;
      start.setDate(now.getDate() - (days - 1));
    }
    dashboardData.range = {
      ...(dashboardData.range || {}),
      key: currentRange,
      label: fallbackLabel,
      start_date: iso(start),
      end_date: iso(now),
      days,
    };

    if (currentRange === "all") {
      const kpis = dashboardData.kpis || {};
      if (kpis.books_total) kpis.books_new = { ...kpis.books_new, value: Number(kpis.books_total.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.audios_total) kpis.audios_new = { ...kpis.audios_new, value: Number(kpis.audios_total.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.users_current) kpis.users_new = { ...kpis.users_new, value: Number(kpis.users_current.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.users_left) kpis.users_left = { ...kpis.users_left, change: "lifetime total", scope: "range" };
      if (kpis.book_searches) kpis.book_searches = { ...kpis.book_searches, change: "lifetime total", scope: "range" };
      if (kpis.book_downloads) kpis.book_downloads = { ...kpis.book_downloads, change: "lifetime total", scope: "range" };
    }
    isLiveData = false;
  } finally {
    window.clearTimeout(timer);
  }
}

function renderAll() {
  setRangeMeta();
  setRangeContextLabels();
  setServices();
  setKpis();
  setCatalog();
  setRequests();
  setStorage();
  setEngagement();
  setRetention();
  setFunnel();
  setSearchQuality();
  setDownloaderQuality();
  setQueueSla();
  setCatalogGrowth();
  setAudience();
  setInfra();
  renderEvents();
  renderCommandBars();
  setTimestamp();
  applyView(currentView, false);
}

async function refreshLoop() {
  await fetchDashboardData();
  renderAll();
}

async function boot() {
  initQuickViews();
  initRangeControls();
  await refreshLoop();
  if (refreshTimer) {
    window.clearInterval(refreshTimer);
  }
  refreshTimer = window.setInterval(refreshLoop, 15000);
}

window.addEventListener("DOMContentLoaded", boot);
