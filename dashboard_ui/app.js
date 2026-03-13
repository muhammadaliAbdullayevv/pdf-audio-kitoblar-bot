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
    bot_api: { status: "ok", label: "Bot API Online", detail: "127.0.0.1:8081" },
    db: { status: "ok", label: "PostgreSQL Healthy", detail: "users=37 books=11975" },
    es: { status: "ok", label: "ES Green", detail: "books indexed=12.0k" },
  },
  bot_runtime: {
    last_restart_epoch: Math.floor(Date.now() / 1000) - 12480,
    last_restart_iso: "",
    uptime_sec: 12480,
    status: "active",
    pid: 56550,
  },
  kpis: {
    users_current: { value: 37, change: "34 allowed, 3 blocked", scope: "lifetime" },
    users_new: { value: 5, change: "+25.0% vs previous 7d", scope: "range" },
    users_left: { value: 1, change: "-50.0% vs previous 7d", scope: "range" },
    books_total: { value: 11975, change: "268 not indexed", scope: "lifetime" },
    books_new: { value: 140, change: "+9.4% vs previous 7d", scope: "range" },
    movies_total: { value: 865, change: "810 indexed (93.6%)", scope: "lifetime" },
    movies_new: { value: 21, change: "+16.7% vs previous 7d", scope: "range" },
    book_searches: { value: 206, change: "+6.2% vs previous 7d", scope: "range" },
    movie_searches: { value: 38, change: "+1.8% vs previous 7d", scope: "range" },
    book_downloads: { value: 75, change: "-2.3% vs previous 7d", scope: "range" },
    movie_downloads: { value: 16, change: "-6.5% vs previous 7d", scope: "range" },
    searches: { value: 244, change: "+5.2% vs previous 7d", scope: "range" },
    downloads_total: { value: 91, change: "-3.1% vs previous 7d", scope: "range" },
    audios_total: { value: 902, change: "12140 audio parts", scope: "lifetime" },
    audios_new: { value: 57, change: "+4.1% vs previous 7d", scope: "range" },
  },
  catalog_growth: {
    labels: ["Mar 05", "Mar 06", "Mar 07", "Mar 08", "Mar 09", "Mar 10", "Mar 11"],
    books_new: [22, 18, 30, 14, 26, 19, 11],
    movies_new: [3, 2, 6, 1, 3, 4, 2],
    audio_new: [9, 8, 11, 7, 8, 9, 5],
    unindexed_new: [2, 1, 3, 1, 2, 1, 0],
  },
  retention: {
    dau: 10,
    wau: 28,
    mau: 37,
    active_in_range: 28,
    active_prev_in_range: 26,
    stickiness_pct: 27.0,
    dau_change: "-9.1%",
    wau_change: "+3.7%",
    mau_change: "+2.1%",
    active_change: "+7.7%",
    window_days: { range: 7, wau: 7, mau: 7 },
  },
  funnel: {
    new_users: 5,
    active_users: 28,
    search_users: 24,
    download_users: 19,
    steps: [
      { label: "New users", value: 5, pct: 100 },
      { label: "Active users", value: 28, pct: 560 },
      { label: "Search users", value: 24, pct: 85.7 },
      { label: "Download users", value: 19, pct: 79.2 },
    ],
    new_to_active_pct: 560,
    active_to_search_pct: 85.7,
    search_to_download_pct: 79.2,
  },
  search_quality: {
    searches_total: 244,
    downloads_total: 91,
    book_searches_total: 206,
    movie_searches_total: 38,
    book_downloads_total: 75,
    movie_downloads_total: 16,
    conversion_pct: 37.3,
    request_queries_total: 43,
    zero_result_total: 9,
    zero_result_rate_pct: 20.9,
    top_queries: [
      { query: "Atomic Habits", count: 6 },
      { query: "IELTS", count: 5 },
      { query: "Quran", count: 4 },
    ],
    zero_result_queries: [
      { query: "Medical genetics", count: 3 },
      { query: "AI product management", count: 2 },
    ],
  },
  queue_sla: {
    pending_upload_count: 2,
    oldest_pending_age_sec: 3900,
    avg_resolve_sec: 1200,
    avg_accept_sec: 970,
    avg_reject_sec: 650,
    avg_book_request_resolve_sec: 4300,
  },
  catalog: {
    books_total: 11975,
    books_indexed: 11707,
    books_unindexed: 268,
    books_index_ratio: 97.8,
    books_downloads_total: 48392,
    books_searches_total: 63190,
    movies_total: 865,
    movies_indexed: 810,
    movies_index_ratio: 93.6,
    audio_books_total: 902,
    audio_books_with_source_books: 741,
    audio_parts_total: 12140,
    audio_duration_seconds: 11500600,
  },
  requests: {
    book: {
      total: 101,
      segments: [
        { key: "open", label: "Open", value: 21, pct: 20.8, color: "#3f88c5" },
        { key: "seen", label: "Seen", value: 11, pct: 10.9, color: "#9b8de5" },
        { key: "done", label: "Done", value: 58, pct: 57.4, color: "#2ca58d" },
        { key: "no", label: "No", value: 8, pct: 7.9, color: "#ef6f6c" },
      ],
    },
    upload: {
      total: 74,
      pending: 2,
      segments: [
        { key: "open", label: "Open", value: 2, pct: 2.7, color: "#3f88c5" },
        { key: "accept", label: "Accepted", value: 56, pct: 75.7, color: "#2ca58d" },
        { key: "reject", label: "Rejected", value: 12, pct: 16.2, color: "#ef6f6c" },
      ],
    },
    queue_pending: 2,
  },
  storage: {
    total_files: 24115,
    total_size: 146232391222,
    book_count: 11975,
    total_book_size: 103912391222,
    audio_count: 12140,
    total_audio_size: 42320000000,
    avg_book_size: 8677439,
    avg_audio_size: 3485996,
    book_size_ratio: 71.1,
  },
  engagement: {
    favorites_total: 1271,
    favorites_added_total: 5942,
    reaction_total_current: 3840,
    reaction_total_lifetime: 9142,
    ai_total: 412,
    favorites_per_active_user: 45.4,
    reactions_per_active_user: 137.1,
    ai_breakdown: {
      chat: 200,
      translator: 71,
      grammar: 42,
      email: 29,
      quiz: 33,
      music: 14,
      pdf: 23,
    },
    book_reactions_current: { like: 1689, dislike: 411, berry: 502, whale: 334 },
    movie_reactions_current: { like: 611, dislike: 93, berry: 107, whale: 93 },
    book_reactions_lifetime: { like: 3924, dislike: 998, berry: 1069, whale: 833 },
    movie_reactions_lifetime: { like: 1231, dislike: 341, berry: 412, whale: 334 },
  },
  downloader: {
    success_total: 356,
    fail_total: 13,
    attempts_total: 369,
    success_rate: 96.5,
    avg_processing_sec: 24.3,
    platform: [
      { platform: "youtube", success: 240, fail: 8, total: 248, success_rate: 96.8 },
      { platform: "instagram", success: 77, fail: 3, total: 80, success_rate: 96.2 },
      { platform: "generic", success: 39, fail: 2, total: 41, success_rate: 95.1 },
    ],
    fail_reasons: [
      { reason: "timeout", count: 5 },
      { reason: "network", count: 4 },
      { reason: "invalid", count: 3 },
    ],
    recent_video_errors: 4,
    recent_video_warns: 9,
    recent_issues: [
      { time: "19:16:08", service: "video_downloader", message: "upload timeout from local uplink", status: "warn" },
      { time: "19:12:42", service: "video_downloader", message: "source stream interrupted", status: "err" },
    ],
  },
  audience: {
    languages: [
      { language: "uz", label: "Uzbek", total_users: 23, active_users: 18, share_pct: 62.1, active_pct: 78.3 },
      { language: "en", label: "English", total_users: 9, active_users: 6, share_pct: 24.3, active_pct: 66.7 },
      { language: "ru", label: "Russian", total_users: 5, active_users: 4, share_pct: 13.5, active_pct: 80.0 },
    ],
    geo_available: false,
    geo_note: "Geo analytics are not tracked yet. Language split is available.",
  },
  infra: {
    cpu: { cores: 8, load1: 1.02, load5: 0.94, load15: 0.86, load_pct: 12.8 },
    memory: { total: 16995553280, available: 9183801344, used: 7811751936, used_pct: 46.0 },
    disk: { total: 511507677184, used: 265214046208, free: 246293630976, used_pct: 51.9 },
    network: { rx_rate_bps: 124221.7, tx_rate_bps: 88112.5 },
    process_uptime_sec: 9420,
    system_uptime_sec: 56200,
    services: [
      { unit: "SmartAIToolsBot.service", status: "active", substatus: "running", restarts: 0, uptime_sec: 17100, pid: 8773 },
      { unit: "SmartAIToolsBot-bot.service", status: "active", substatus: "running", restarts: 0, uptime_sec: 12480, pid: 56550 },
      { unit: "SmartAIToolsBot-dashboard.service", status: "active", substatus: "running", restarts: 0, uptime_sec: 3800, pid: 146366 },
    ],
  },
  reliability: {
    log_errors: 6,
    log_warns: 19,
    video_errors: 4,
    video_warns: 9,
  },
  events: [
    { time: "19:16:08", service: "video_downloader", message: "upload timeout from local uplink", status: "warn" },
    { time: "19:14:12", service: "search_flow", message: "query cache hit: 14ms", status: "ok" },
    { time: "19:12:42", service: "video_downloader", message: "source stream interrupted", status: "err" },
    { time: "19:10:05", service: "dashboard_server", message: "API payload refresh complete", status: "ok" },
    { time: "19:08:32", service: "elasticsearch", message: "health yellow (replicas pending)", status: "warn" },
  ],
  commands: [
    { name: "/start", count: 981 },
    { name: "Search Books", count: 736 },
    { name: "Video Downloader", count: 515 },
    { name: "AI Tools", count: 472 },
    { name: "Search Movies", count: 301 },
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
  if (!kpis.movie_searches) {
    kpis.movie_searches = { value: 0, change: "0.0% vs previous window", scope: "range" };
  }
  if (!kpis.movie_downloads) {
    kpis.movie_downloads = { value: 0, change: "0.0% vs previous window", scope: "range" };
  }
  const rangeLabel = String((dashboardData.range || {}).label || "selected range");
  const mappings = [
    ["users_current", "kpi-users-current", "kpi-users-current-meta"],
    ["users_new", "kpi-users-new", "kpi-users-new-meta"],
    ["users_left", "kpi-users-left", "kpi-users-left-meta"],
    ["books_total", "kpi-books-total", "kpi-books-total-meta"],
    ["books_new", "kpi-books-new", "kpi-books-new-meta"],
    ["movies_total", "kpi-movies-total", "kpi-movies-total-meta"],
    ["movies_new", "kpi-movies-new", "kpi-movies-new-meta"],
    ["book_searches", "kpi-book-searches", "kpi-book-searches-meta"],
    ["movie_searches", "kpi-movie-searches", "kpi-movie-searches-meta"],
    ["book_downloads", "kpi-book-downloads", "kpi-book-downloads-meta"],
    ["movie_downloads", "kpi-movie-downloads", "kpi-movie-downloads-meta"],
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
  const moviesRatio = Number(catalog.movies_index_ratio || 0);
  const audioRatio = Number(catalog.books_total || 0) > 0
    ? ((Number(catalog.audio_books_with_source_books || 0) / Number(catalog.books_total || 1)) * 100)
    : 0;

  setText("catalog-books-ratio", formatPercent(booksRatio));
  setText("catalog-movies-ratio", formatPercent(moviesRatio));
  setText("catalog-audio-ratio", formatPercent(audioRatio));

  const booksMeter = document.getElementById("catalog-books-meter");
  const moviesMeter = document.getElementById("catalog-movies-meter");
  const audioMeter = document.getElementById("catalog-audio-meter");
  if (booksMeter) booksMeter.style.width = `${Math.max(0, Math.min(100, booksRatio))}%`;
  if (moviesMeter) moviesMeter.style.width = `${Math.max(0, Math.min(100, moviesRatio))}%`;
  if (audioMeter) audioMeter.style.width = `${Math.max(0, Math.min(100, audioRatio))}%`;

  setText("catalog-books-meta", `${formatNumber(catalog.books_indexed || 0)} indexed / ${formatNumber(catalog.books_total || 0)} total`);
  setText("catalog-movies-meta", `${formatNumber(catalog.movies_indexed || 0)} indexed / ${formatNumber(catalog.movies_total || 0)} total`);
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
  setText("eng-ai-total", formatNumber(engagement.ai_total || 0));
  setText("downloader-health", `${formatPercent(downloader.success_rate || 0)} (${formatNumber(downloader.success_total || 0)}/${formatNumber(downloader.attempts_total || 0)})`);

  const reactionRows = [
    { label: "Book reactions now", value: Object.values(engagement.book_reactions_current || {}).reduce((acc, x) => acc + Number(x || 0), 0) },
    { label: "Movie reactions now", value: Object.values(engagement.movie_reactions_current || {}).reduce((acc, x) => acc + Number(x || 0), 0) },
    { label: "Favorites per active user", value: Number(engagement.favorites_per_active_user || 0).toFixed(2) },
    { label: "Reactions per active user", value: Number(engagement.reactions_per_active_user || 0).toFixed(2) },
  ];
  renderListRows("reaction-mix-list", reactionRows, (row) => `<span>${escapeHtml(row.label)}</span><strong>${escapeHtml(row.value)}</strong>`);

  const ai = engagement.ai_breakdown || {};
  const aiRows = Object.keys(ai)
    .map((key) => ({ label: key.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase()), value: Number(ai[key] || 0) }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 7);
  renderListRows("ai-breakdown-list", aiRows, (row) => `<span>AI ${escapeHtml(row.label)}</span><strong>${formatNumber(row.value)}</strong>`);

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
  const totalSearches = Number(q.searches_total || 0);
  const totalDownloads = Number(q.downloads_total || 0);
  const movieSearches = Number(q.movie_searches_total || 0);
  const movieDownloads = Number(q.movie_downloads_total || 0);
  const bookSearches = Number(
    q.book_searches_total != null ? q.book_searches_total : Math.max(0, totalSearches - movieSearches)
  );
  const bookDownloads = Number(
    q.book_downloads_total != null ? q.book_downloads_total : Math.max(0, totalDownloads - movieDownloads)
  );
  const conversionPct = Number.isFinite(Number(q.conversion_pct))
    ? Number(q.conversion_pct)
    : (bookSearches + movieSearches > 0 ? ((bookDownloads + movieDownloads) / (bookSearches + movieSearches)) * 100 : 0);

  setText("search-book-total", formatNumber(bookSearches));
  setText("search-movie-total", formatNumber(movieSearches));
  setText("search-book-download-total", formatNumber(bookDownloads));
  setText("search-movie-download-total", formatNumber(movieDownloads));
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
  const movies = Array.isArray(g.movies_new) ? g.movies_new : [];
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
      <td>${formatNumber(movies[i] || 0)}</td>
      <td>${formatNumber(audio[i] || 0)}</td>
      <td>${formatNumber(unindexed[i] || 0)}</td>
    `;
    tbody.appendChild(tr);
  }
  if (!labels.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = "<td colspan=\"5\">No growth data in this range</td>";
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
      if (kpis.movies_total) kpis.movies_new = { ...kpis.movies_new, value: Number(kpis.movies_total.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.audios_total) kpis.audios_new = { ...kpis.audios_new, value: Number(kpis.audios_total.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.users_current) kpis.users_new = { ...kpis.users_new, value: Number(kpis.users_current.value || 0), change: "lifetime total", scope: "range" };
      if (kpis.users_left) kpis.users_left = { ...kpis.users_left, change: "lifetime total", scope: "range" };
      if (kpis.book_searches) kpis.book_searches = { ...kpis.book_searches, change: "lifetime total", scope: "range" };
      if (kpis.movie_searches) kpis.movie_searches = { ...kpis.movie_searches, change: "lifetime total", scope: "range" };
      if (kpis.book_downloads) kpis.book_downloads = { ...kpis.book_downloads, change: "lifetime total", scope: "range" };
      if (kpis.movie_downloads) kpis.movie_downloads = { ...kpis.movie_downloads, change: "lifetime total", scope: "range" };
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
