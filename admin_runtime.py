from __future__ import annotations

import logging
import textwrap
from typing import Any

logger = logging.getLogger(__name__)

_CONFIG_REQUIRED_KEYS = (
    "MESSAGES",
    "ensure_user_language",
    "_is_admin_user",
    "spam_check_message",
    "run_blocking",
    "safe_answer",
    "_edit_progress_message",
    "_send_chat_message",
    "db_list_admin_task_runs",
    "db_insert_admin_task_run",
    "db_update_admin_task_run",
)


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v
    missing = [key for key in _CONFIG_REQUIRED_KEYS if key not in globals()]
    if missing:
        raise RuntimeError(f"admin_runtime missing configured dependencies: {', '.join(missing)}")


def _safe_asyncio_current_task():
    asyncio_mod = globals().get("asyncio")
    if not asyncio_mod:
        return None
    try:
        return asyncio_mod.current_task()
    except Exception:
        return None


def _list_running_background_tasks(app) -> list[dict]:
    items: list[dict] = []
    bot_data = getattr(app, "bot_data", {}) or {}

    def add_item(key: str, label: str, details: str = ""):
        task = bot_data.get(key)
        if not task or task.done():
            return
        items.append({"key": key, "label": label, "details": details, "task": task})

    upload_local_status = bot_data.get("upload_local_status") or {}
    if bot_data.get("upload_local_task") and not bot_data["upload_local_task"].done():
        done = int(upload_local_status.get("done", 0) or 0)
        total = int(upload_local_status.get("total", 0) or 0)
        mode = str(upload_local_status.get("mode", "all"))
        errs = int(upload_local_status.get("errors", 0) or 0)
        add_item("upload_local_task", "Upload local books", f"mode={mode} progress={done}/{total} errors={errs}")

    upload_fanout_task = bot_data.get("upload_fanout_task")
    if upload_fanout_task and not upload_fanout_task.done():
        q = bot_data.get("upload_fanout_queue")
        qsize = q.qsize() if q else 0
        add_item("upload_fanout_task", "Upload fanout queue", f"queued={qsize}")

    for kind, label in [("db", "DB dupes cleanup"), ("es", "ES dupes cleanup")]:
        task_key = _dupes_task_key(kind)
        task = bot_data.get(task_key)
        if task and not task.done():
            st = _get_dupes_status(app, kind)
            stage = st.get("stage", "running")
            processed = int(st.get("processed", 0) or 0)
            total = int(st.get("total", 0) or 0)
            details = f"stage={stage} progress={processed}/{total}"
            add_item(task_key, label, details)

    return items


def _background_tasks_keyboard(items: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for item in items:
        rows.append([InlineKeyboardButton(f"Cancel: {item['label']}", callback_data=f"bgtask:cancel:{item['key']}")])
    rows.append([
        InlineKeyboardButton("Refresh", callback_data="bgtask:refresh"),
        InlineKeyboardButton("Close", callback_data="bgtask:close"),
    ])
    return InlineKeyboardMarkup(rows)


def _format_background_tasks_text(app, notice: str | None = None) -> str:
    items = _list_running_background_tasks(app)
    lines = ["Background tasks", "──────────"]
    if notice:
        lines.append(notice)
        lines.append("──────────")
    if not items:
        lines.append("No background tasks are currently running.")
        return "\n".join(lines)
    for i, item in enumerate(items, start=1):
        details = f" ({item['details']})" if item.get("details") else ""
        lines.append(f"{i}. {item['label']}{details}")
    lines.append("──────────")
    lines.append("Choose a task below to cancel it.")
    return "\n".join(lines)


def _fmt_task_ts(value) -> str:
    if not value:
        return "-"
    try:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def _format_task_history_text(rows: list[dict], limit: int = 6) -> str:
    if not rows:
        return "Recent task runs\n──────────\nNo persisted task runs yet."
    lines = ["Recent task runs", "──────────"]
    for row in rows[: max(1, int(limit))]:
        kind = str(row.get("task_kind") or row.get("task_key") or "task")
        status = str(row.get("status") or "unknown")
        started = _fmt_task_ts(row.get("started_at"))
        finished = _fmt_task_ts(row.get("finished_at"))
        summary = str(row.get("summary") or "").strip()
        line = f"- {kind} | {status} | start={started} | end={finished}"
        if summary:
            line += f" | {summary[:80]}"
        lines.append(line)
    return "\n".join(lines)


async def _build_background_tasks_with_history_text(
    context: ContextTypes.DEFAULT_TYPE,
    notice: str | None = None,
    history_limit: int = 6,
) -> str:
    text = _format_background_tasks_text(context.application, notice=notice)
    try:
        history = await run_blocking(db_list_admin_task_runs, max(8, int(history_limit or 6)))
    except Exception as e:
        logger.warning("Failed to load admin task history: %s", e)
        history = []
    if history:
        text = text + "\n\n" + _format_task_history_text(history, limit=history_limit)
    return text


def _now_dt():
    try:
        return datetime.now()
    except Exception:
        return None


def _admin_panel_snapshot_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    app = context.application
    paused = "ON" if is_bot_paused(context) else "OFF"
    tasks = _list_running_background_tasks(app)
    dupes_db = _get_dupes_status(app, "db") if "dupes_status" in (app.bot_data or {}) else {}
    dupes_es = _get_dupes_status(app, "es") if "dupes_status" in (app.bot_data or {}) else {}
    upload_status = app.bot_data.get("upload_local_status") or {}
    upload_line = "idle"
    if upload_status:
        done = int(upload_status.get("done", 0) or 0)
        total = int(upload_status.get("total", 0) or 0)
        mode = str(upload_status.get("mode", "all"))
        upload_line = f"{mode} {done}/{total}"
    db_stage = dupes_db.get("stage") or "idle"
    es_stage = dupes_es.get("stage") or "idle"
    return (
        "Admin Control Panel\n"
        "──────────\n"
        f"Bot paused: {paused}\n"
        f"Background tasks: {len(tasks)}\n"
        f"DB dupes: {db_stage}\n"
        f"ES dupes: {es_stage}\n"
        f"Upload local: {upload_line}\n"
        "──────────\n"
        "Choose a section."
    )


def _admin_panel_keyboard(section: str = "main") -> InlineKeyboardMarkup:
    if section == "system":
        rows = [
            [InlineKeyboardButton("⏸ Pause bot", callback_data="adminp:act:pause"),
             InlineKeyboardButton("▶ Resume bot", callback_data="adminp:act:resume")],
            [InlineKeyboardButton("🧾 Audit report", callback_data="adminp:act:audit")],
            [InlineKeyboardButton("🔄 Refresh panel", callback_data="adminp:nav:main"),
             InlineKeyboardButton("⬅ Back", callback_data="adminp:nav:main")],
        ]
    elif section == "uploads":
        rows = [
            [InlineKeyboardButton("📊 Status", callback_data="adminp:act:upload_status")],
            [InlineKeyboardButton("⬆ All", callback_data="adminp:act:upload_all"),
             InlineKeyboardButton("🩹 Missing", callback_data="adminp:act:upload_missing")],
            [InlineKeyboardButton("🆔 Unique", callback_data="adminp:act:upload_unique"),
             InlineKeyboardButton("📦 Large", callback_data="adminp:act:upload_large")],
            [InlineKeyboardButton("⬅ Back", callback_data="adminp:nav:main")],
        ]
    elif section == "dupes":
        rows = [
            [InlineKeyboardButton("📊 Dupes status", callback_data="adminp:act:dupes_status")],
            [InlineKeyboardButton("🧼 DB dupes preview", callback_data="adminp:act:db_dupes"),
             InlineKeyboardButton("🧼 ES dupes preview", callback_data="adminp:act:es_dupes")],
            [InlineKeyboardButton("⬅ Back", callback_data="adminp:nav:main")],
        ]
    elif section == "tasks":
        rows = [
            [InlineKeyboardButton("🧵 Show tasks", callback_data="adminp:act:tasks_show")],
            [InlineKeyboardButton("⬅ Back", callback_data="adminp:nav:main")],
        ]
    elif section == "maint":
        rows = [
            [InlineKeyboardButton("🧹 Prune blocked users", callback_data="adminp:act:prune")],
            [InlineKeyboardButton("⚠ Missing files preview", callback_data="adminp:act:missing_preview")],
            [InlineKeyboardButton("⬅ Back", callback_data="adminp:nav:main")],
        ]
    else:
        rows = [
            [InlineKeyboardButton("🖥 System", callback_data="adminp:nav:system"),
             InlineKeyboardButton("⬆ Uploads", callback_data="adminp:nav:uploads")],
            [InlineKeyboardButton("🧼 Duplicates", callback_data="adminp:nav:dupes"),
             InlineKeyboardButton("🧵 Tasks", callback_data="adminp:nav:tasks")],
            [InlineKeyboardButton("🛠 Maintenance", callback_data="adminp:nav:maint"),
             InlineKeyboardButton("🔄 Refresh", callback_data="adminp:nav:main")],
            [InlineKeyboardButton("❌ Close", callback_data="adminp:act:close")],
        ]
    return InlineKeyboardMarkup(rows)


async def _admin_panel_send_or_edit(update: Update, context: ContextTypes.DEFAULT_TYPE, section: str = "main"):
    text = _admin_panel_snapshot_text(context)
    if section != "main":
        titles = {
            "system": "System Controls",
            "uploads": "Upload Local Books",
            "dupes": "Duplicate Cleanup",
            "tasks": "Background Tasks",
            "maint": "Maintenance",
        }
        text = f"{text}\n\nSection: {titles.get(section, section)}"
    kb = _admin_panel_keyboard(section)
    query = update.callback_query
    if query and query.message:
        try:
            await query.edit_message_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    target_message = update.message or (query.message if query else None)
    if target_message:
        await target_message.reply_text(text, reply_markup=kb)


async def _admin_panel_send_upload_local_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    lang = ensure_user_language(update, context)
    status = context.application.bot_data.get("upload_local_status")
    if not status:
        await target_message.reply_text(MESSAGES[lang]["upload_local_status_empty"])
        return
    done = int(status.get("done", 0) or 0)
    total = int(status.get("total", 0) or 0)
    uploaded = int(status.get("uploaded", 0) or 0)
    updated = int(status.get("updated", 0) or 0)
    skipped = int(status.get("skipped", 0) or 0)
    skipped_large = int(status.get("skipped_large", 0) or 0)
    missing = int(status.get("missing", 0) or 0)
    errors = int(status.get("errors", 0) or 0)
    start_ts = float(status.get("start_ts", time.time()))
    elapsed = max(1, int(time.time() - start_ts))
    rate = round(done / elapsed, 2)
    per_channel = status.get("per_channel", {}) or {}
    if per_channel:
        for cid, info in per_channel.items():
            if info.get("title") and info.get("title") != str(cid):
                continue
            try:
                chat = await context.bot.get_chat(int(cid))
                info["title"] = chat.title or chat.username or str(cid)
            except Exception:
                info["title"] = str(cid)
        status["per_channel"] = per_channel
        context.application.bot_data["upload_local_status"] = status
    lines = []
    for cid, info in per_channel.items():
        sent = info.get("sent", 0)
        err = info.get("errors", 0)
        title = info.get("title") or str(cid)
        lines.append(MESSAGES[lang]["upload_local_status_channel"].format(channel=title, sent=sent, errors=err))
    per_channel_text = "\n".join(lines) if lines else MESSAGES[lang]["upload_local_status_channel_empty"]
    text = MESSAGES[lang]["upload_local_status"].format(
        done=done, total=total, uploaded=uploaded, updated=updated, skipped=skipped,
        skipped_large=skipped_large, missing=missing, errors=errors, elapsed=elapsed,
        rate=rate, per_channel=per_channel_text,
    )
    await target_message.reply_text(text)


async def _admin_panel_send_missing_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    lang = ensure_user_language(update, context)
    items = await run_blocking(get_missing_file_info, 20)
    if not items:
        await target_message.reply_text(MESSAGES[lang]["missing_none"])
        return
    def reason_label(reason_code: str) -> str:
        if reason_code == "local_missing":
            return MESSAGES[lang]["missing_reason_local_missing"]
        return MESSAGES[lang]["missing_reason_no_file_id"]
    lines = [MESSAGES[lang]["missing_title"]]
    for item in items:
        lines.append(MESSAGES[lang]["missing_item"].format(title=item.get("title") or "—", reason=reason_label(item.get("reason", ""))))
    if len(items) >= 20:
        lines.append("…")
    lines.append("To delete all missing entries, use /missing confirm")
    await target_message.reply_text("\n".join(lines))


async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    await update_user_info(update, context)
    if update.message:
        await _send_main_menu(update, context, lang, "admin")
        return
    await _admin_panel_send_or_edit(update, context, "main")


async def smoke_check_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return

    es_ok = bool(es_available())
    pdf_ok = bool(globals().get("canvas") and globals().get("A4"))
    tts_ok = False
    try:
        tts_ok = bool(_tts_tools_available())
    except Exception:
        tts_ok = False
    ollama_url = str(os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")).rstrip("/")

    def mark(ok: bool) -> str:
        return "✅" if ok else "⚠️"

    text = "\n".join([
        "🧪 Smoke Test Checklist (Admin)",
        "──────────",
        "Runtime snapshot",
        f"{mark(es_ok)} Elasticsearch: {'available' if es_ok else 'unavailable'}",
        f"{mark(pdf_ok)} PDF Maker deps (reportlab): {'ready' if pdf_ok else 'missing'}",
        f"{mark(tts_ok)} TTS deps (edge-tts + ffmpeg): {'ready' if tts_ok else 'missing'}",
        f"🤖 Ollama URL: {ollama_url}",
        "──────────",
        "Manual checks",
        "1. `/start` -> greeting + main menu shows once",
        "2. `🔎 Search Books` -> search results + paging + book download",
        "3. `🎙️ Text to Voice` -> wizard -> generate voice/audio",
        "4. `📄 PDF Maker` -> name/style/paper/orientation -> PDF send",
        "5. `🤖 AI Tools` -> AI Chat / Translator / Grammar / Email",
        "6. `🛠️ Other Functions` -> Help / Top / Requests / Favorites / Profile",
        "7. `🛠 Admin Control` -> User search / audit / admin actions",
        "8. Request callbacks: request -> admin status -> user notification",
        "9. Favorite / reaction buttons update caption counts correctly",
        "──────────",
        "Tip: test one feature per menu group after every refactor.",
    ])
    await target_message.reply_text(text)


async def handle_admin_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id if query.from_user else 0):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = str(query.data or "")
    if not data.startswith("adminp:"):
        await safe_answer(query)
        return
    parts = data.split(":", 2)
    kind = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""
    await safe_answer(query)

    if kind == "nav":
        await _admin_panel_send_or_edit(update, context, value or "main")
        return

    if kind != "act":
        return

    if value == "close":
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if value == "pause":
        await pause_bot_command(update, context)
        await _admin_panel_send_or_edit(update, context, "system")
        return
    if value == "resume":
        await resume_bot_command(update, context)
        await _admin_panel_send_or_edit(update, context, "system")
        return
    if value == "audit":
        context.user_data["_skip_spam_check_once"] = True
        await audit_command(update, context)
        await _admin_panel_send_or_edit(update, context, "system")
        return
    if value == "prune":
        await prune_command(update, context)
        await _admin_panel_send_or_edit(update, context, "maint")
        return
    if value == "missing_preview":
        await _admin_panel_send_missing_preview(update, context)
        await _admin_panel_send_or_edit(update, context, "maint")
        return
    if value == "tasks_show":
        context.user_data["_skip_spam_check_once"] = True
        await cancel_task_command(update, context)
        await _admin_panel_send_or_edit(update, context, "tasks")
        return
    if value == "dupes_status":
        context.user_data["_skip_spam_check_once"] = True
        await dupes_status_command(update, context)
        await _admin_panel_send_or_edit(update, context, "dupes")
        return
    if value == "db_dupes":
        context.user_data["_skip_spam_check_once"] = True
        await db_dupes_command(update, context)
        await _admin_panel_send_or_edit(update, context, "dupes")
        return
    if value == "es_dupes":
        context.user_data["_skip_spam_check_once"] = True
        await es_dupes_command(update, context)
        await _admin_panel_send_or_edit(update, context, "dupes")
        return
    if value == "upload_status":
        await _admin_panel_send_upload_local_status(update, context)
        await _admin_panel_send_or_edit(update, context, "uploads")
        return
    if value in {"upload_all", "upload_missing", "upload_unique", "upload_large"}:
        context.user_data["_skip_spam_check_once"] = True
        mode = value.replace("upload_", "", 1)
        await _start_upload_local_books(update, context, mode)
        await _admin_panel_send_or_edit(update, context, "uploads")
        return

    try:
        await query.answer(MESSAGES[lang]["error"], show_alert=True)
    except Exception:
        pass


async def cancel_task_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    items = _list_running_background_tasks(context.application)
    text = await _build_background_tasks_with_history_text(context, history_limit=6)
    reply_markup = _background_tasks_keyboard(items) if items else None
    await target_message.reply_text(text, reply_markup=reply_markup)


async def handle_background_task_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id if query.from_user else 0):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = str(query.data or "")
    if not data.startswith("bgtask:"):
        await safe_answer(query)
        return
    parts = data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    key = parts[2] if len(parts) > 2 else ""

    if action == "close":
        await safe_answer(query)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if action == "refresh":
        await safe_answer(query)
        items = _list_running_background_tasks(context.application)
        text = await _build_background_tasks_with_history_text(context, history_limit=6)
        markup = _background_tasks_keyboard(items) if items else None
        await _edit_progress_message(query.message, text, reply_markup=markup)
        return

    if action != "cancel" or not key:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    task = context.application.bot_data.get(key)
    if not task or task.done():
        await safe_answer(query, "Task is already finished.", show_alert=True)
        items = _list_running_background_tasks(context.application)
        text = await _build_background_tasks_with_history_text(
            context,
            notice=f"{key}: already finished",
            history_limit=6,
        )
        markup = _background_tasks_keyboard(items) if items else None
        await _edit_progress_message(query.message, text, reply_markup=markup)
        return

    task.cancel()
    run_id = None
    if key == "upload_local_task":
        run_id = context.application.bot_data.get("upload_local_task_run_id")
    elif key == _dupes_task_key("db"):
        st = _get_dupes_status(context.application, "db")
        run_id = st.get("task_run_id") if isinstance(st, dict) else None
    elif key == _dupes_task_key("es"):
        st = _get_dupes_status(context.application, "es")
        run_id = st.get("task_run_id") if isinstance(st, dict) else None
    if run_id:
        try:
            await run_blocking(
                db_update_admin_task_run,
                str(run_id),
                status="cancelling",
                summary="Cancellation requested by operator",
            )
        except Exception as e:
            logger.warning("Failed to mark task run as cancelling (%s): %s", run_id, e)
    if key == "upload_local_task":
        context.application.bot_data.pop("upload_local_status", None)
    elif key == "upload_fanout_task":
        context.application.bot_data["upload_fanout_queue"] = asyncio.Queue()
    elif key == _dupes_task_key("db"):
        _update_dupes_status(context.application, "db", stage="cancelled", running=False, final_message_sent=False, finished_at=time.time())
    elif key == _dupes_task_key("es"):
        _update_dupes_status(context.application, "es", stage="cancelled", running=False, final_message_sent=False, finished_at=time.time())

    await safe_answer(query, "Cancel signal sent")
    await asyncio.sleep(0)
    items = _list_running_background_tasks(context.application)
    text = await _build_background_tasks_with_history_text(
        context,
        notice=f"Cancel requested: {key}",
        history_limit=6,
    )
    markup = _background_tasks_keyboard(items) if items else None
    await _edit_progress_message(query.message, text, reply_markup=markup)


_DUPES_PDF_FONT_READY = False
_DUPES_PDF_FONT_NAME = "Helvetica"


def _ensure_dupes_pdf_font():
    global _DUPES_PDF_FONT_READY, _DUPES_PDF_FONT_NAME
    if _DUPES_PDF_FONT_READY:
        return _DUPES_PDF_FONT_NAME
    _DUPES_PDF_FONT_READY = True
    if not (pdfmetrics and TTFont):
        return _DUPES_PDF_FONT_NAME
    candidates = [
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("DejaVuSans", path))
                _DUPES_PDF_FONT_NAME = "DejaVuSans"
                break
            except Exception:
                continue
    return _DUPES_PDF_FONT_NAME


def _build_dupes_preview_pdf(kind: str, stats: dict, preview_pairs: list[dict] | None = None) -> bytes | None:
    if not canvas or not A4:
        return None
    text = _format_dupes_preview(kind, stats, preview_pairs)
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    page_w, page_h = A4
    left = 36
    top = page_h - 36
    bottom = 36
    line_h = 12
    font_name = _ensure_dupes_pdf_font()
    font_size = 9

    def new_text_obj():
        t = c.beginText(left, top)
        t.setFont(font_name, font_size)
        return t

    text_obj = new_text_obj()
    y = top
    wrap_width = 95 if font_name == "Helvetica" else 100
    for raw_line in text.splitlines():
        wrapped = textwrap.wrap(raw_line, width=wrap_width, break_long_words=True, replace_whitespace=False) or [""]
        for line in wrapped:
            if y <= bottom:
                c.drawText(text_obj)
                c.showPage()
                text_obj = new_text_obj()
                y = top
            text_obj.textLine(line)
            y -= line_h
    c.drawText(text_obj)
    c.save()
    return buf.getvalue()


def _dupes_confirm_keyboard(kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Confirm cleanup", callback_data=f"dupesop:{kind}:confirm"),
            InlineKeyboardButton("Cancel", callback_data=f"dupesop:{kind}:cancel"),
        ]]
    )


def _format_dupes_preview(kind: str, stats: dict, preview_pairs: list[dict] | None = None) -> str:
    title = "DB duplicate cleanup confirmation" if kind == "db" else "ES duplicate cleanup confirmation"
    head = [
        title,
        "──────────",
        "This will remove duplicates and keep 1 item per duplicate group.",
        f"Before: {stats.get('total_before', 0)}",
        f"Will delete: {stats.get('total_delete', 0)}",
        f"After (planned): {stats.get('total_after', 0)}",
        "──────────",
        f"file_unique_id groups: {stats.get('file_unique_groups', 0)} | delete: {stats.get('file_unique_deleted', 0)}",
        f"path groups: {stats.get('path_groups', 0)} | delete: {stats.get('path_deleted', 0)}",
        f"name (normalized) groups: {stats.get('name_groups', 0)} | delete: {stats.get('name_deleted', 0)}",
        "──────────",
        "Duplicate preview (max 50):",
    ]
    tail = [
        "──────────",
        "Errors (before run): 0",
        "Press Confirm cleanup to start.",
    ]
    preview_lines = _build_dupe_preview_lines(preview_pairs or [], limit=50)
    if not preview_lines:
        return "\n".join(head + ["No duplicate items found."] + tail)

    # Keep preview message under Telegram text limit.
    max_chars = 3900
    chosen = []
    for line in preview_lines:
        candidate = "\n".join(head + chosen + [line] + tail)
        if len(candidate) > max_chars:
            break
        chosen.append(line)
    if not chosen:
        chosen = preview_lines[:1]
    return "\n".join(head + chosen + tail)


def _format_dupes_preview_caption(kind: str, stats: dict) -> str:
    title = "DB dupes preview" if kind == "db" else "ES dupes preview"
    return (
        f"{title}\n"
        f"Will delete: {int(stats.get('total_delete', 0) or 0)}\n"
        f"Groups: fuid={int(stats.get('file_unique_groups', 0) or 0)}, "
        f"path={int(stats.get('path_groups', 0) or 0)}, "
        f"name={int(stats.get('name_groups', 0) or 0)}\n"
        "PDF contains duplicate list (up to 50).\n"
        "Confirm to start cleanup."
    )


def _format_db_dupes_summary(stats: dict, deleted_db: int, deleted_es: int, es_failed: int, processed: int | None = None, total: int | None = None, running: bool = False) -> str:
    lines = [
        "DB duplicate cleanup",
        "──────────",
        f"Before: {stats['total_before']}",
        f"Planned delete: {stats['total_delete']}",
        f"After (planned): {stats['total_after']}",
        "──────────",
        f"file_unique_id groups: {stats['file_unique_groups']} | deleted: {stats['file_unique_deleted']}",
        f"path groups: {stats['path_groups']} | deleted: {stats['path_deleted']}",
        f"name (normalized) groups: {stats['name_groups']} | deleted: {stats['name_deleted']}",
        "──────────",
    ]
    if running and total is not None and processed is not None:
        lines.append(f"Progress: {processed}/{total}")
    lines.append(f"Deleted from DB: {deleted_db}")
    lines.append(f"Deleted matching docs from ES: {deleted_es}")
    if es_failed:
        lines.append(f"ES delete errors: {es_failed}")
    if stats.get("total_delete", 0) == 0:
        lines.append("No duplicates found.")
    elif running:
        lines.append("Status: running...")
    else:
        lines.append("Status: done")
    return "\n".join(lines)


def _format_es_dupes_summary(stats: dict, deleted_es: int, es_failed: int, processed: int | None = None, total: int | None = None, running: bool = False) -> str:
    lines = [
        "ES duplicate cleanup",
        "──────────",
        f"Before: {stats['total_before']}",
        f"Planned delete: {stats['total_delete']}",
        f"After (planned): {stats['total_after']}",
        "──────────",
        f"file_unique_id groups: {stats['file_unique_groups']} | deleted: {stats['file_unique_deleted']}",
        f"path groups: {stats['path_groups']} | deleted: {stats['path_deleted']}",
        f"name (normalized) groups: {stats['name_groups']} | deleted: {stats['name_deleted']}",
        "──────────",
    ]
    if running and total is not None and processed is not None:
        lines.append(f"Progress: {processed}/{total}")
    lines.append(f"Deleted from ES: {deleted_es}")
    if es_failed:
        lines.append(f"ES delete errors: {es_failed}")
    if stats.get("total_delete", 0) == 0:
        lines.append("No duplicates found.")
    elif running:
        lines.append("Status: running...")
    else:
        lines.append("Status: done")
    return "\n".join(lines)


async def _run_db_dupes_cleanup_job(
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
    status_msg,
    admin_chat_id: int | None,
    task_run_id: str | None = None,
):
    _update_dupes_status(context.application, "db", stage="scanning", running=True)
    stats, victims, _preview_pairs = await run_blocking(_compute_db_duplicate_cleanup_plan)
    deleted_db = 0
    deleted_es = 0
    es_failed = 0
    total = len(victims)
    _update_dupes_status(
        context.application,
        "db",
        stage="cleaning",
        running=True,
        total=total,
        processed=0,
        planned_delete=stats.get("total_delete", 0),
        deleted_db=0,
        deleted_es=0,
        es_failed=0,
        final_message_sent=None,
        started_at=time.time(),
    )

    es = get_es() if victims else None
    for idx, row in enumerate(victims, start=1):
        book_id = str(row.get("id") or "")
        if not book_id:
            continue
        try:
            deleted_db += int(await run_blocking(delete_book_and_related, book_id) or 0)
        except Exception as e:
            logger.error(f"db_dupes failed deleting DB row {book_id}: {e}", exc_info=True)
            continue
        if es:
            try:
                await run_blocking(lambda: es.delete(index=ES_INDEX, id=book_id))
                deleted_es += 1
            except NotFoundError:
                pass
            except Exception as e:
                es_failed += 1
                logger.error(f"db_dupes failed deleting ES doc {book_id}: {e}", exc_info=True)
        if idx == total or idx % 25 == 0:
            _update_dupes_status(
                context.application,
                "db",
                stage="cleaning",
                running=True,
                total=total,
                processed=idx,
                deleted_db=deleted_db,
                deleted_es=deleted_es,
                es_failed=es_failed,
            )

    final_text = _format_db_dupes_summary(stats, deleted_db, deleted_es, es_failed, processed=total, total=total, running=False)
    sent = await _send_chat_message(context, admin_chat_id, final_text)
    _update_dupes_status(
        context.application,
        "db",
        stage="done",
        running=False,
        total=total,
        processed=total,
        planned_delete=stats.get("total_delete", 0),
        deleted_db=deleted_db,
        deleted_es=deleted_es,
        es_failed=es_failed,
        final_message_sent=bool(sent is not None),
        finished_at=time.time(),
    )
    if task_run_id:
        try:
            await run_blocking(
                db_update_admin_task_run,
                str(task_run_id),
                status="done",
                summary=final_text,
                metadata={
                    "kind": "db",
                    "total": total,
                    "planned_delete": int(stats.get("total_delete", 0) or 0),
                    "deleted_db": deleted_db,
                    "deleted_es": deleted_es,
                    "es_failed": es_failed,
                },
                finished_at=_now_dt(),
            )
        except Exception as e:
            logger.warning("Failed to persist db dupes task completion: %s", e)


async def _run_es_dupes_cleanup_job(
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
    status_msg,
    admin_chat_id: int | None,
    task_run_id: str | None = None,
):
    _update_dupes_status(context.application, "es", stage="scanning", running=True)
    stats, victims, _preview_pairs = await run_blocking(_compute_es_duplicate_cleanup_plan)
    deleted_es = 0
    es_failed = 0
    total = len(victims)
    _update_dupes_status(
        context.application,
        "es",
        stage="cleaning",
        running=True,
        total=total,
        processed=0,
        planned_delete=stats.get("total_delete", 0),
        deleted_es=0,
        es_failed=0,
        final_message_sent=None,
        started_at=time.time(),
    )
    es = get_es()
    if es and victims:
        for idx, row in enumerate(victims, start=1):
            es_id = str(row.get("_es_id") or row.get("id") or "")
            if not es_id:
                continue
            try:
                await run_blocking(lambda: es.delete(index=ES_INDEX, id=es_id))
                deleted_es += 1
            except NotFoundError:
                pass
            except Exception as e:
                es_failed += 1
                logger.error(f"es_dupes failed deleting ES doc {es_id}: {e}", exc_info=True)
            if idx == total or idx % 50 == 0:
                _update_dupes_status(
                    context.application,
                    "es",
                    stage="cleaning",
                    running=True,
                    total=total,
                    processed=idx,
                    deleted_es=deleted_es,
                    es_failed=es_failed,
                )
        try:
            await run_blocking(lambda: es.indices.refresh(index=ES_INDEX))
        except Exception:
            pass

    final_text = _format_es_dupes_summary(stats, deleted_es, es_failed, processed=total, total=total, running=False)
    sent = await _send_chat_message(context, admin_chat_id, final_text)
    _update_dupes_status(
        context.application,
        "es",
        stage="done",
        running=False,
        total=total,
        processed=total,
        planned_delete=stats.get("total_delete", 0),
        deleted_es=deleted_es,
        es_failed=es_failed,
        final_message_sent=bool(sent is not None),
        finished_at=time.time(),
    )
    if task_run_id:
        try:
            await run_blocking(
                db_update_admin_task_run,
                str(task_run_id),
                status="done",
                summary=final_text,
                metadata={
                    "kind": "es",
                    "total": total,
                    "planned_delete": int(stats.get("total_delete", 0) or 0),
                    "deleted_es": deleted_es,
                    "es_failed": es_failed,
                },
                finished_at=_now_dt(),
            )
        except Exception as e:
            logger.warning("Failed to persist es dupes task completion: %s", e)


def _dupes_task_key(kind: str) -> str:
    return f"{kind}_dupes_task"


def _dupes_is_running(app, kind: str) -> bool:
    task = app.bot_data.get(_dupes_task_key(kind))
    return bool(task and not task.done())


def _start_dupes_cleanup_task(
    context: ContextTypes.DEFAULT_TYPE,
    kind: str,
    lang: str,
    status_msg,
    admin_chat_id: int | None,
    started_by: int | None = None,
):
    app = context.application
    key = _dupes_task_key(kind)
    if _dupes_is_running(app, kind):
        return False

    async def _runner():
        task_run_id = None
        try:
            try:
                task_run_id = await run_blocking(
                    db_insert_admin_task_run,
                    key,
                    f"{kind}_dupes",
                    started_by,
                    "running",
                    {"kind": kind, "admin_chat_id": admin_chat_id},
                )
                _update_dupes_status(app, kind, task_run_id=task_run_id)
            except Exception as e:
                logger.warning("Failed to persist %s dupes task start: %s", kind, e)
            if kind == "db":
                await _run_db_dupes_cleanup_job(context, lang, status_msg, admin_chat_id, task_run_id=task_run_id)
            else:
                await _run_es_dupes_cleanup_job(context, lang, status_msg, admin_chat_id, task_run_id=task_run_id)
        except asyncio.CancelledError:
            if task_run_id:
                try:
                    await run_blocking(
                        db_update_admin_task_run,
                        str(task_run_id),
                        status="cancelled",
                        summary="Task cancelled",
                        finished_at=_now_dt(),
                    )
                except Exception as e:
                    logger.warning("Failed to persist %s dupes task cancellation: %s", kind, e)
            raise
        except Exception as e:
            _update_dupes_status(
                app,
                kind,
                stage="failed",
                running=False,
                final_message_sent=False,
                last_error=str(e)[:1000],
                finished_at=time.time(),
            )
            logger.error("%s_dupes background task failed: %s", kind, e, exc_info=True)
            if task_run_id:
                try:
                    await run_blocking(
                        db_update_admin_task_run,
                        str(task_run_id),
                        status="failed",
                        error=str(e),
                        summary=f"{kind.upper()} duplicate cleanup failed",
                        finished_at=_now_dt(),
                    )
                except Exception as db_e:
                    logger.warning("Failed to persist %s dupes task failure: %s", kind, db_e)
            try:
                await _send_chat_message(context, admin_chat_id, f"{kind.upper()} duplicate cleanup failed: {e}")
            except Exception:
                pass
        finally:
            current_task = _safe_asyncio_current_task()
            if current_task is not None and app.bot_data.get(key) is current_task:
                app.bot_data.pop(key, None)

    task = app.create_task(_runner())
    app.bot_data[key] = task
    _update_dupes_status(app, kind, stage="queued", running=True, final_message_sent=None)
    return True


async def db_dupes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    if _dupes_is_running(context.application, "db"):
        await target_message.reply_text("DB duplicate cleanup is already running.")
        return
    stats, _victims, preview_pairs = await run_blocking(_compute_db_duplicate_cleanup_plan)
    _update_dupes_status(
        context.application,
        "db",
        stage="preview_ready",
        running=False,
        total=int(stats.get("total_delete", 0) or 0),
        processed=0,
        planned_delete=int(stats.get("total_delete", 0) or 0),
        final_message_sent=None,
        preview_generated_at=time.time(),
    )
    caption = _format_dupes_preview_caption("db", stats)
    pdf_bytes = _build_dupes_preview_pdf("db", stats, preview_pairs)
    if pdf_bytes:
        sent = await _send_preview_pdf(update, pdf_bytes, "db_dupes_preview.pdf", caption, reply_markup=_dupes_confirm_keyboard("db"))
        if sent is not None:
            return
    preview_text = _format_dupes_preview("db", stats, preview_pairs)
    await _send_progress_message(update, preview_text, reply_markup=_dupes_confirm_keyboard("db"))


async def es_dupes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    if not es_available():
        await target_message.reply_text("Elasticsearch is not available.")
        return
    if _dupes_is_running(context.application, "es"):
        await target_message.reply_text("ES duplicate cleanup is already running.")
        return

    stats, _victims, preview_pairs = await run_blocking(_compute_es_duplicate_cleanup_plan)
    _update_dupes_status(
        context.application,
        "es",
        stage="preview_ready",
        running=False,
        total=int(stats.get("total_delete", 0) or 0),
        processed=0,
        planned_delete=int(stats.get("total_delete", 0) or 0),
        final_message_sent=None,
        preview_generated_at=time.time(),
    )
    caption = _format_dupes_preview_caption("es", stats)
    pdf_bytes = _build_dupes_preview_pdf("es", stats, preview_pairs)
    if pdf_bytes:
        sent = await _send_preview_pdf(update, pdf_bytes, "es_dupes_preview.pdf", caption, reply_markup=_dupes_confirm_keyboard("es"))
        if sent is not None:
            return
    preview_text = _format_dupes_preview("es", stats, preview_pairs)
    await _send_progress_message(update, preview_text, reply_markup=_dupes_confirm_keyboard("es"))


async def dupes_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await target_message.reply_text(_format_dupes_status_text(context.application))


async def handle_dupes_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    user_id = query.from_user.id if query.from_user else None
    if not _is_admin_user(user_id or 0):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = str(query.data or "")
    parts = data.split(":")
    if len(parts) != 3:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    _, kind, action = parts
    if kind not in {"db", "es"}:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    if action == "cancel":
        await safe_answer(query, "Cancelled")
        try:
            await _send_chat_message(
                context,
                query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None),
                f"{kind.upper()} duplicate cleanup cancelled.",
            )
        except Exception:
            pass
        return
    if action != "confirm":
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    if kind == "es" and not es_available():
        await safe_answer(query, "Elasticsearch is not available.", show_alert=True)
        return
    if _dupes_is_running(context.application, kind):
        await safe_answer(query, f"{kind.upper()} dupes cleanup is already running.", show_alert=True)
        return
    await safe_answer(query, "Started")
    await _send_chat_message(
        context,
        query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None),
        f"{kind.upper()} duplicate cleanup started. Working in background...",
    )
    started = _start_dupes_cleanup_task(
        context,
        kind,
        lang,
        None,
        query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None),
        started_by=user_id,
    )
    if not started:
        await _send_chat_message(
            context,
            query.message.chat_id if query.message else (update.effective_chat.id if update.effective_chat else None),
            f"{kind.upper()} duplicate cleanup is already running.",
        )


async def user_search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    query = " ".join(context.args or []).strip()
    query = " ".join(query.split())
    if not query:
        await update.message.reply_text(MESSAGES[lang]["user_search_usage"])
        return

    all_users: list[dict] = []
    db_search_fn = globals().get("db_search_users_by_name")
    db_limit = max(int(USER_SEARCH_LIMIT or 30) * 8, 200)
    if callable(db_search_fn):
        try:
            all_users = await run_blocking(db_search_fn, query, db_limit)
        except Exception as e:
            logger.error("User search DB query failed: %s", e)
            all_users = []
    else:
        try:
            all_users = await run_blocking(list_users)
        except Exception as e:
            logger.error("User search failed to load users: %s", e)
            all_users = []

    q_norm = normalize(query).lower()
    q_lat = latinize_text(query)
    q_digits = re.sub(r"\D+", "", query or "")
    q_digits_only = re.fullmatch(r"\d+", (query or "").strip()) is not None
    q_tokens = [t for t in q_norm.split() if t]

    scored_users: list[tuple[float, dict]] = []
    for u in all_users:
        uid_raw = u.get("id")
        if uid_raw is None:
            continue
        uid_str = str(uid_raw).strip()
        if not uid_str:
            continue

        first_name = (u.get("first_name") or "").strip()
        last_name = (u.get("last_name") or "").strip()
        name = " ".join([p for p in [first_name, last_name] if p]).strip()
        username = str(u.get("username") or "").strip()
        username_no_at = username[1:] if username.startswith("@") else username

        name_norm = normalize(name).lower()
        user_norm = normalize(username_no_at).lower()
        hay_norm = " ".join([p for p in [name_norm, user_norm] if p]).strip()
        hay_lat = latinize_text(f"{name} {username_no_at}")

        score = 0.0

        # ID matching (supports partial IDs and ranks exact/prefix higher).
        if q_digits:
            if uid_str == q_digits:
                score += 1000.0
            elif uid_str.startswith(q_digits):
                score += 800.0 + min(len(q_digits), 12)
            elif q_digits in uid_str:
                score += 620.0 + min(len(q_digits), 12)

        # Direct normalized text matching.
        if q_norm:
            if q_norm == user_norm:
                score += 520.0
            if q_norm == name_norm:
                score += 480.0
            if q_norm and user_norm.startswith(q_norm):
                score += 420.0
            if q_norm and name_norm.startswith(q_norm):
                score += 380.0
            if q_norm and q_norm in user_norm:
                score += 320.0
            if q_norm and q_norm in name_norm:
                score += 300.0
            if q_norm and q_norm in hay_norm:
                score += 240.0

        # Transliteration-based containment (useful for Cyrillic/Latin mismatch).
        if q_lat and q_lat != q_norm:
            if q_lat in hay_lat:
                score += 220.0

        # Token overlap boosts.
        if q_tokens and hay_norm:
            hay_tokens = set(hay_norm.split())
            overlap = sum(1 for t in q_tokens if t in hay_tokens)
            if overlap:
                score += overlap * 90.0

        # Fuzzy ranking for imperfect input, only if there is text part.
        if q_norm and hay_norm:
            try:
                wr = float(fuzz.WRatio(q_norm, hay_norm))
                pr = float(fuzz.partial_ratio(q_norm, hay_norm))
                if wr >= 55:
                    score += wr * 1.5
                if pr >= 60:
                    score += pr * 1.0
            except Exception:
                pass

        # If query is purely numeric and no ID hit, avoid unrelated fuzzy name matches.
        if q_digits_only:
            if q_digits not in uid_str and not uid_str.startswith(q_digits):
                if score < 600:
                    score = 0.0

        if score > 0:
            scored_users.append((score, u))

    scored_users.sort(
        key=lambda item: (
            -float(item[0]),
            str(item[1].get("first_name") or "").lower(),
            str(item[1].get("username") or "").lower(),
            str(item[1].get("id") or ""),
        )
    )
    users = [u for _score, u in scored_users[:USER_SEARCH_LIMIT]]
    if not users:
        await update.message.reply_text(MESSAGES[lang]["user_search_empty"])
        return

    entries = []
    for u in users:
        name = " ".join([p for p in [u.get("first_name"), u.get("last_name")] if p]).strip() or "—"
        username = f"@{u.get('username')}" if u.get("username") else "—"
        title = f"{name} ({username})"
        entries.append({"id": str(u.get("id")), "title": title})

    query_id = cache_user_results(context, query, entries)
    result_text, page_entries, pages = build_user_results_text(query, entries, 0, lang)
    reply_markup = build_user_results_keyboard(page_entries, 0, pages, query_id)
    await update.message.reply_text(result_text, reply_markup=reply_markup)


async def _start_upload_local_books(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    if not UPLOAD_CHANNEL_IDS:
        await target_message.reply_text(MESSAGES[lang]["upload_local_no_channel"])
        return
    running = context.application.bot_data.get("upload_local_task")
    if running and not running.done():
        await target_message.reply_text(MESSAGES[lang]["upload_local_running"])
        return

    admin_chat_id = update.effective_chat.id if update.effective_chat else update.effective_user.id
    await target_message.reply_text(MESSAGES[lang]["upload_local_started"])
    task_run_id = None
    try:
        task_run_id = await run_blocking(
            db_insert_admin_task_run,
            "upload_local_task",
            "upload_local",
            update.effective_user.id if update.effective_user else None,
            "running",
            {"mode": mode, "admin_chat_id": int(admin_chat_id)},
        )
        context.application.bot_data["upload_local_task_run_id"] = task_run_id
    except Exception as e:
        logger.warning("Failed to persist upload_local task run start: %s", e)

    async def _run():
        uploaded = 0
        updated = 0
        skipped = 0
        skipped_large = 0
        missing = 0
        errors = 0
        lock = asyncio.Lock()
        sem = asyncio.Semaphore(UPLOAD_LOCAL_WORKERS)
        large_sem = asyncio.Semaphore(UPLOAD_LOCAL_LARGE_CONCURRENCY)
        channel_ids = list(UPLOAD_CHANNEL_IDS)
        chan_lock = asyncio.Lock()
        chan_index = 0

        async def next_channel_id() -> int:
            nonlocal chan_index
            async with chan_lock:
                cid = channel_ids[chan_index % len(channel_ids)]
                chan_index += 1
                return cid

        books = await run_blocking(db_list_books)
        to_process: list[dict] = []
        for b in books:
            file_id = b.get("file_id")
            file_unique_id = b.get("file_unique_id")
            local_path = b.get("path")
            if mode == "missing":
                if file_id:
                    skipped += 1
                    continue
                if not local_path or not os.path.exists(local_path):
                    missing += 1
                    continue
                try:
                    size_mb = os.path.getsize(local_path) / (1024 * 1024)
                except Exception:
                    size_mb = None
                if size_mb is not None and size_mb > UPLOAD_LOCAL_MAX_MB:
                    skipped_large += 1
                    continue
                to_process.append(b)
                continue
            if mode == "unique":
                if not file_id or file_unique_id:
                    skipped += 1
                    continue
                to_process.append(b)
                continue
            if mode == "large":
                if file_id:
                    skipped += 1
                    continue
                if not local_path or not os.path.exists(local_path):
                    missing += 1
                    continue
                try:
                    size_mb = os.path.getsize(local_path) / (1024 * 1024)
                except Exception:
                    size_mb = None
                if size_mb is None or size_mb <= UPLOAD_LOCAL_MAX_MB:
                    skipped += 1
                    continue
                to_process.append(b)
                continue
            # mode == "all"
            if file_id and file_unique_id:
                skipped += 1
                continue
            if not file_id and (not local_path or not os.path.exists(local_path)):
                missing += 1
                continue
            if not file_id and local_path:
                try:
                    size_mb = os.path.getsize(local_path) / (1024 * 1024)
                except Exception:
                    size_mb = None
                if size_mb is not None and size_mb > UPLOAD_LOCAL_MAX_MB:
                    skipped_large += 1
                    continue
            to_process.append(b)

        async def send_once(b: dict, target_channel: int):
            file_id = b.get("file_id")
            local_path = b.get("path")
            if file_id:
                sent = await context.bot.send_document(
                    chat_id=target_channel,
                    document=file_id,
                    connect_timeout=UPLOAD_LOCAL_CONNECT_TIMEOUT,
                    read_timeout=UPLOAD_LOCAL_READ_TIMEOUT,
                    write_timeout=UPLOAD_LOCAL_WRITE_TIMEOUT,
                    pool_timeout=UPLOAD_LOCAL_POOL_TIMEOUT,
                )
                return target_channel, sent
            with open(local_path, "rb") as f:
                sent = await context.bot.send_document(
                    chat_id=target_channel,
                    document=InputFile(f, filename=_book_filename(b)),
                    connect_timeout=UPLOAD_LOCAL_CONNECT_TIMEOUT,
                    read_timeout=UPLOAD_LOCAL_READ_TIMEOUT,
                    write_timeout=UPLOAD_LOCAL_WRITE_TIMEOUT,
                    pool_timeout=UPLOAD_LOCAL_POOL_TIMEOUT,
                )
                return target_channel, sent

        total = len(to_process)
        start_ts = time.time()
        per_channel = {cid: {"sent": 0, "errors": 0, "title": str(cid)} for cid in channel_ids}
        context.application.bot_data["upload_local_status"] = {
            "mode": mode,
            "total": total,
            "done": 0,
            "uploaded": 0,
            "updated": 0,
            "skipped": skipped,
            "skipped_large": skipped_large,
            "missing": missing,
            "errors": 0,
            "start_ts": start_ts,
            "per_channel": per_channel,
        }

        async def process_book(b: dict):
            nonlocal uploaded, updated, errors
            async with sem:
                book_id = str(b.get("id") or "")
                file_id = b.get("file_id")
                local_path = b.get("path")
                size_mb = None
                if local_path and os.path.exists(local_path):
                    try:
                        size_mb = round(os.path.getsize(local_path) / (1024 * 1024), 1)
                    except Exception:
                        size_mb = None
                sent = None
                target_channel = await next_channel_id()

                async def _send_with_retries():
                    nonlocal sent, target_channel
                    attempts = 4
                    delay = 2.0
                    for attempt in range(attempts):
                        try:
                            target_channel, sent = await send_once(b, target_channel)
                            return
                        except RetryAfter as e:
                            await asyncio.sleep(getattr(e, "retry_after", 1) + 0.5)
                            continue
                        except (TimedOut, NetworkError):
                            if attempt == attempts - 1:
                                raise
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, 30.0)
                            continue

                try:
                    if size_mb is not None and size_mb >= UPLOAD_LOCAL_LARGE_MB:
                        async with large_sem:
                            await _send_with_retries()
                    else:
                        await _send_with_retries()
                    if sent is None:
                        raise TimedOut("send_document timed out")
                except Exception as e:
                    logger.error(
                        "upload_local failed book_id=%s channel=%s file_id=%s path=%s size_mb=%s err=%s",
                        book_id,
                        target_channel,
                        file_id,
                        local_path,
                        size_mb,
                        e,
                        exc_info=True,
                    )
                    async with lock:
                        errors += 1
                        status = context.application.bot_data.get("upload_local_status", {})
                        per_channel = status.get("per_channel", {})
                        if target_channel in per_channel:
                            per_channel[target_channel]["errors"] += 1
                            status["per_channel"] = per_channel
                        status["errors"] = errors
                        status["done"] = uploaded + updated + errors
                        context.application.bot_data["upload_local_status"] = status
                    return

                if sent and sent.document:
                    new_file_id = sent.document.file_id
                    new_unique_id = getattr(sent.document, "file_unique_id", None)
                    await run_blocking(update_book_file_id, book_id, new_file_id, True, new_unique_id)
                    async with lock:
                        if file_id:
                            updated += 1
                        else:
                            uploaded += 1
                        status = context.application.bot_data.get("upload_local_status", {})
                        per_channel = status.get("per_channel", {})
                        if target_channel in per_channel:
                            per_channel[target_channel]["sent"] += 1
                            status["per_channel"] = per_channel
                        status["uploaded"] = uploaded
                        status["updated"] = updated
                        status["done"] = uploaded + updated + errors
                        context.application.bot_data["upload_local_status"] = status
                else:
                    logger.error(
                        "upload_local empty send book_id=%s channel=%s file_id=%s path=%s size_mb=%s",
                        book_id,
                        target_channel,
                        file_id,
                        local_path,
                        size_mb,
                    )
                    async with lock:
                        errors += 1
                        status = context.application.bot_data.get("upload_local_status", {})
                        per_channel = status.get("per_channel", {})
                        if target_channel in per_channel:
                            per_channel[target_channel]["errors"] += 1
                            status["per_channel"] = per_channel
                        status["errors"] = errors
                        status["done"] = uploaded + updated + errors
                        context.application.bot_data["upload_local_status"] = status

        try:
            tasks = [asyncio.create_task(process_book(b)) for b in to_process]
            if tasks:
                await asyncio.gather(*tasks)

            summary = MESSAGES[lang]["upload_local_done"].format(
                uploaded=uploaded,
                updated=updated,
                skipped=skipped,
                skipped_large=skipped_large,
                missing=missing,
                errors=errors,
            )
            try:
                await context.bot.send_message(chat_id=admin_chat_id, text=summary)
            except Exception:
                pass
            if task_run_id:
                try:
                    await run_blocking(
                        db_update_admin_task_run,
                        str(task_run_id),
                        status="done",
                        summary=summary,
                        metadata={
                            "mode": mode,
                            "uploaded": uploaded,
                            "updated": updated,
                            "skipped": skipped,
                            "skipped_large": skipped_large,
                            "missing": missing,
                            "errors": errors,
                            "total": total,
                        },
                        finished_at=_now_dt(),
                    )
                except Exception as e:
                    logger.warning("Failed to persist upload_local task completion: %s", e)
        except asyncio.CancelledError:
            if task_run_id:
                try:
                    await run_blocking(
                        db_update_admin_task_run,
                        str(task_run_id),
                        status="cancelled",
                        summary="Task cancelled",
                        finished_at=_now_dt(),
                    )
                except Exception as e:
                    logger.warning("Failed to persist upload_local task cancellation: %s", e)
            raise
        except Exception as e:
            logger.error("upload_local task failed: %s", e, exc_info=True)
            if task_run_id:
                try:
                    await run_blocking(
                        db_update_admin_task_run,
                        str(task_run_id),
                        status="failed",
                        error=str(e),
                        summary="Upload local task failed",
                        finished_at=_now_dt(),
                    )
                except Exception as db_e:
                    logger.warning("Failed to persist upload_local task failure: %s", db_e)
            try:
                await context.bot.send_message(chat_id=admin_chat_id, text=MESSAGES[lang]["error"])
            except Exception:
                pass
            raise
        finally:
            try:
                context.application.bot_data.pop("upload_local_status", None)
            except Exception:
                pass

    task = asyncio.create_task(_run())
    context.application.bot_data["upload_local_task"] = task
    def _cleanup(_t):
        try:
            context.application.bot_data.pop("upload_local_task", None)
            context.application.bot_data.pop("upload_local_task_run_id", None)
        except Exception:
            pass
    task.add_done_callback(_cleanup)


async def upload_local_books_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    mode = context.args[0].strip().lower() if context.args else "all"
    if mode == "status":
        lang = ensure_user_language(update, context)
        if not _is_admin_user(update.effective_user.id):
            await target_message.reply_text(MESSAGES[lang]["admin_only"])
            return
        status = context.application.bot_data.get("upload_local_status")
        if not status:
            await target_message.reply_text(MESSAGES[lang]["upload_local_status_empty"])
            return
        done = int(status.get("done", 0) or 0)
        total = int(status.get("total", 0) or 0)
        uploaded = int(status.get("uploaded", 0) or 0)
        updated = int(status.get("updated", 0) or 0)
        skipped = int(status.get("skipped", 0) or 0)
        skipped_large = int(status.get("skipped_large", 0) or 0)
        missing = int(status.get("missing", 0) or 0)
        errors = int(status.get("errors", 0) or 0)
        start_ts = float(status.get("start_ts", time.time()))
        elapsed = max(1, int(time.time() - start_ts))
        rate = round(done / elapsed, 2)
        per_channel = status.get("per_channel", {}) or {}
        if per_channel:
            for cid, info in per_channel.items():
                if info.get("title") and info.get("title") != str(cid):
                    continue
                try:
                    chat = await context.bot.get_chat(int(cid))
                    title = chat.title or chat.username or str(cid)
                    info["title"] = title
                except Exception:
                    info["title"] = str(cid)
            status["per_channel"] = per_channel
            context.application.bot_data["upload_local_status"] = status
        lines = []
        for cid, info in per_channel.items():
            sent = info.get("sent", 0)
            err = info.get("errors", 0)
            title = info.get("title") or str(cid)
            lines.append(MESSAGES[lang]["upload_local_status_channel"].format(channel=title, sent=sent, errors=err))
        per_channel_text = "\n".join(lines) if lines else MESSAGES[lang]["upload_local_status_channel_empty"]

        text = MESSAGES[lang]["upload_local_status"].format(
            done=done,
            total=total,
            uploaded=uploaded,
            updated=updated,
            skipped=skipped,
            skipped_large=skipped_large,
            missing=missing,
            errors=errors,
            elapsed=elapsed,
            rate=rate,
            per_channel=per_channel_text,
        )
        await target_message.reply_text(text)
        return
    if mode not in {"all", "missing", "unique", "large"}:
        lang = ensure_user_language(update, context)
        await target_message.reply_text(MESSAGES[lang]["upload_local_usage"])
        return
    await _start_upload_local_books(update, context, mode)


# --- Main ---
