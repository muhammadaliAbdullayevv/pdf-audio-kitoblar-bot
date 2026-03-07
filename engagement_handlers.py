from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
from typing import Any

import safe_subprocess

from telegram import Update
from telegram.ext import ContextTypes

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v


async def _can_show_delete_button(update: Update, user_id: int | None) -> bool:
    if not user_id:
        return False
    try:
        allowed = await can_delete_books(user_id)
    except Exception:
        allowed = False
    if not allowed:
        return False

    chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
    if not chat_type:
        cb = getattr(update, "callback_query", None)
        msg = getattr(cb, "message", None) or getattr(update, "effective_message", None)
        chat_type = str(getattr(getattr(msg, "chat", None), "type", "") or "").lower()
    if chat_type in {"group", "supergroup"}:
        owner_fn = globals().get("_is_owner_user")
        if callable(owner_fn):
            try:
                return bool(owner_fn(user_id))
            except Exception:
                return False
        return False
    return True


def _invalidate_top_caches(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        context.application.bot_data.pop("top_entries_cache", None)
    except Exception:
        pass
    try:
        context.user_data.pop("top_cache", None)
    except Exception:
        pass


async def _edit_progress_or_reply(progress_message, target_message, text: str, reply_markup=None) -> None:
    if progress_message:
        try:
            await progress_message.edit_text(text, reply_markup=reply_markup)
            return
        except Exception:
            pass
    await target_message.reply_text(text, reply_markup=reply_markup)


async def handle_user_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    if not _is_admin_user(query.from_user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    data = query.data or ""
    try:
        parts = data.split(":")
        action = parts[1]
        user_id_str = parts[2]
        user_id = int(user_id_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    user = await run_blocking(get_user, user_id)
    if not user:
        await safe_answer(query, MESSAGES[lang]["user_not_found"], show_alert=True)
        return

    prev_stopped = bool(user.get("stopped"))
    prev_upload = bool(user.get("allowed"))
    prev_delete = bool(user.get("delete_allowed"))
    if action == "block":
        await run_blocking(set_user_blocked, user_id, not bool(user.get("blocked")))
    elif action == "upload":
        await run_blocking(set_user_allowed, user_id, not bool(user.get("allowed")))
    elif action == "del":
        await run_blocking(set_user_delete_allowed, user_id, not bool(user.get("delete_allowed")))
    elif action == "stop":
        await run_blocking(set_user_stopped, user_id, not bool(user.get("stopped")))
    elif action in {"bonus_add", "bonus_del"}:
        context.user_data["awaiting_user_bonus"] = {
            "user_id": user_id,
            "mode": "add" if action == "bonus_add" else "del",
            "expires_at": time.time() + 120,
        }
        name = format_user_name(user)
        if action == "bonus_add":
            await safe_answer(query)
            await query.message.reply_text(MESSAGES[lang]["user_bonus_prompt_add"].format(name=name))
        else:
            await safe_answer(query)
            await query.message.reply_text(MESSAGES[lang]["user_bonus_prompt_del"].format(name=name))
        return
    else:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    user = await run_blocking(get_user, user_id)
    if not user:
        await safe_answer(query, MESSAGES[lang]["user_not_found"], show_alert=True)
        return

    text = build_user_info_text(user)
    keyboard = build_user_admin_keyboard(user)
    try:
        await query.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        pass

    if action == "upload" and not prev_upload and bool(user.get("allowed")):
        notice_lang = user.get("language") or "en"
        try:
            await context.bot.send_message(chat_id=user_id, text=MESSAGES[notice_lang]["upload_allowed_notice"])
        except Exception:
            pass
    if action == "del" and not prev_delete and bool(user.get("delete_allowed")):
        notice_lang = user.get("language") or "en"
        try:
            await context.bot.send_message(chat_id=user_id, text=MESSAGES[notice_lang]["delete_allowed_notice"])
        except Exception:
            pass

    await safe_answer(query)


async def top_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    progress_message = None
    try:
        progress_message = await target_message.reply_text(
            MESSAGES[lang].get("processing_general", "⏳ Processing your request...")
        )
    except Exception:
        progress_message = None
    try:
        entries = await run_blocking(
            db_get_top_users,
            TOP_USERS_LIMIT * 2,
            COIN_SEARCH,
            COIN_DOWNLOAD,
            COIN_REACTION,
            COIN_FAVORITE,
            COIN_REFERRAL,
        )
        if not entries:
            await _edit_progress_or_reply(progress_message, target_message, MESSAGES[lang]["top_users_empty"])
            return
        text = build_top_users_text(entries, TOP_USERS_LIMIT, lang, len(entries))
        keyboard = build_top_users_keyboard(len(entries), TOP_USERS_LIMIT, lang)
        await _edit_progress_or_reply(progress_message, target_message, text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"/top_users failed: {e}", exc_info=True)
        await _edit_progress_or_reply(progress_message, target_message, MESSAGES[lang]["error"])


async def handle_top_users_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    action = (query.data or "").split(":", 1)[1] if ":" in (query.data or "") else ""
    limit = TOP_USERS_LIMIT * 2 if action == "more" else TOP_USERS_LIMIT
    try:
        entries = await run_blocking(
            db_get_top_users,
            TOP_USERS_LIMIT * 2,
            COIN_SEARCH,
            COIN_DOWNLOAD,
            COIN_REACTION,
            COIN_FAVORITE,
            COIN_REFERRAL,
        )
        if not entries:
            await safe_answer(query, MESSAGES[lang]["top_users_empty"], show_alert=True)
            return
        text = build_top_users_text(entries, limit, lang, len(entries))
        keyboard = build_top_users_keyboard(len(entries), limit, lang)
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            pass
        await safe_answer(query)
    except Exception as e:
        logger.error(f"/top_users toggle failed: {e}", exc_info=True)
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    progress_message = None
    try:
        progress_message = await target_message.reply_text(
            MESSAGES[lang].get("processing_general", "⏳ Processing your request...")
        )
    except Exception:
        progress_message = None
    try:
        entries = get_cached_top_entries(context)
        if entries is None:
            books = await run_blocking(db_get_top_books, 20, 0)
            if not books:
                await _edit_progress_or_reply(progress_message, target_message, MESSAGES[lang]["top_empty"])
                return

            entries = []
            for b in books:
                title = get_result_title(b)
                entries.append(
                    {
                        "id": str(b.get("id")),
                        "title": title,
                        "downloads": int(b.get("downloads") or 0),
                        "searches": int(b.get("searches") or 0),
                        "fav_count": int(b.get("fav_count") or 0),
                        "like": int(b.get("like_count") or 0),
                        "dislike": int(b.get("dislike_count") or 0),
                        "berry": int(b.get("berry_count") or 0),
                        "whale": int(b.get("whale_count") or 0),
                    }
                )
            set_cached_top_entries(context, entries)

        if not entries:
            await _edit_progress_or_reply(progress_message, target_message, MESSAGES[lang]["top_empty"])
            return

        query_id = cache_top_results(context, entries)
        text, page_entries, pages = build_top_text(entries, 0, lang)
        reply_markup = build_top_keyboard(page_entries, 0, pages, query_id)
        await _edit_progress_or_reply(progress_message, target_message, text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"/top failed: {e}", exc_info=True)
        await _edit_progress_or_reply(progress_message, target_message, MESSAGES[lang]["error"])


async def handle_top_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    try:
        _, query_id, page_str = query.data.split(":")
        page = int(page_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    cache = get_top_cache(context, query_id)
    if not cache:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    entries = cache.get("results", [])
    text, page_entries, pages = build_top_text(entries, page, lang)
    reply_markup = build_top_keyboard(page_entries, page, pages, query_id)

    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception:
        pass
    await safe_answer(query)


async def handle_favorite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    try:
        _, action, book_id = query.data.split(":", 2)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    book = await run_blocking(db_get_book_by_id, book_id)
    title = get_result_title(book) if book else book_id

    if action == "toggle":
        is_fav = await run_blocking(is_favorited, query.from_user.id, book_id)
        if is_fav:
            await run_blocking(remove_favorite, query.from_user.id, book_id)
            await run_blocking(db_increment_counter, "favorite_removed", 1)
            msg = MESSAGES[lang]["unfavorited"]
        else:
            await run_blocking(add_favorite, query.from_user.id, book_id, title)
            await run_blocking(db_increment_counter, "favorite_added", 1)
            await run_blocking(db_award_favorite_action, query.from_user.id, book_id)
            msg = MESSAGES[lang]["favorited"]
    elif action == "add":
        await run_blocking(add_favorite, query.from_user.id, book_id, title)
        await run_blocking(db_increment_counter, "favorite_added", 1)
        await run_blocking(db_award_favorite_action, query.from_user.id, book_id)
        msg = MESSAGES[lang]["favorited"]
    elif action == "remove":
        await run_blocking(remove_favorite, query.from_user.id, book_id)
        await run_blocking(db_increment_counter, "favorite_removed", 1)
        msg = MESSAGES[lang]["unfavorited"]
    else:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    _invalidate_top_caches(context)

    try:
        if book and query.message:
            stats = await run_blocking(db_get_book_stats, book_id)
            downloads = stats.get("downloads", 0)
            fav_count = stats.get("fav_count", 0)
            counts = {
                "like": stats.get("like", 0),
                "dislike": stats.get("dislike", 0),
                "berry": stats.get("berry", 0),
                "whale": stats.get("whale", 0),
            }
            user_reaction = await run_blocking(db_get_user_reaction, book_id, query.from_user.id)
            is_fav_now = await run_blocking(is_favorited, query.from_user.id, book_id)
            can_delete = await _can_show_delete_button(update, query.from_user.id)
            # Check if book has audiobook
            audio_book = await run_blocking(get_audio_book_for_book, book_id)
            has_ab = bool(audio_book)
            can_add_ab = bool(_is_admin_user(query.from_user.id)) if callable(globals().get("_is_admin_user")) else False
            await query.message.edit_caption(
                caption=build_book_caption(book, downloads, fav_count, counts),
                reply_markup=build_book_keyboard(book_id, counts, is_fav_now, user_reaction, can_delete, lang, has_ab, can_add_ab),
            )
    except Exception:
        pass
    await safe_answer(query, msg)


async def handle_reaction_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    try:
        _, book_id, reaction = query.data.split(":", 2)
    except Exception:
        await safe_answer(query, "Error", show_alert=True)
        return

    if reaction not in REACTION_EMOJI:
        await safe_answer(query, "Error", show_alert=True)
        return

    try:
        old_reaction = await run_blocking(db_get_user_reaction, book_id, query.from_user.id)
        if old_reaction == reaction:
            await safe_answer(query)
            return

        await run_blocking(db_set_book_reaction, query.from_user.id, book_id, reaction)
        await run_blocking(db_increment_counter, f"reaction_{reaction}", 1)
        await run_blocking(db_award_reaction_action, query.from_user.id, book_id)
        _invalidate_top_caches(context)
        stats = await run_blocking(db_get_book_stats, book_id)
        downloads = stats.get("downloads", 0)
        fav_count = stats.get("fav_count", 0)
        counts = {
            "like": stats.get("like", 0),
            "dislike": stats.get("dislike", 0),
            "berry": stats.get("berry", 0),
            "whale": stats.get("whale", 0),
        }
        book = await run_blocking(db_get_book_by_id, book_id)
        if book and query.message:
            user_reaction = await run_blocking(db_get_user_reaction, book_id, query.from_user.id)
            is_fav_now = await run_blocking(is_favorited, query.from_user.id, book_id)
            can_delete = await _can_show_delete_button(update, query.from_user.id)
            # Check if book has audiobook
            audio_book = await run_blocking(get_audio_book_for_book, book_id)
            has_ab = bool(audio_book)
            can_add_ab = bool(_is_admin_user(query.from_user.id)) if callable(globals().get("_is_admin_user")) else False
            await query.message.edit_caption(
                caption=build_book_caption(book, downloads, fav_count, counts),
                reply_markup=build_book_keyboard(book_id, counts, is_fav_now, user_reaction, can_delete, lang, has_ab, can_add_ab),
            )
        await safe_answer(query)
    except Exception as e:
        logger.error(f"Reaction update failed: {e}", exc_info=True)
        await safe_answer(query, "Error", show_alert=True)


SUMMARY_MODES = ("short", "detailed", "points")


def _summary_mode_label(lang: str, mode: str) -> str:
    return MESSAGES.get(lang, MESSAGES["en"]).get(f"summary_mode_label_{mode}", mode)


def _summary_mode_keyboard(book_id: str, lang: str) -> InlineKeyboardMarkup:
    m = MESSAGES.get(lang, MESSAGES["en"])
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(m["summary_mode_short"], callback_data=f"summary:{book_id}:short"),
                InlineKeyboardButton(m["summary_mode_detailed"], callback_data=f"summary:{book_id}:detailed"),
            ],
            [
                InlineKeyboardButton(m["summary_mode_points"], callback_data=f"summary:{book_id}:points"),
                InlineKeyboardButton(m["summary_mode_cancel"], callback_data="summary:cancel"),
            ],
        ]
    )


def _summary_lang_name(lang: str) -> str:
    return {"uz": "Uzbek (Latin script)", "ru": "Russian", "en": "English"}.get(lang, "English")


def _summary_stage_text(lang: str, stage_key: str) -> str:
    m = MESSAGES.get(lang, MESSAGES["en"])
    return m.get(f"summary_stage_{stage_key}", stage_key)


def _summary_progress_render(lang: str, state: dict) -> str:
    m = MESSAGES.get(lang, MESSAGES["en"])
    spinner = state.get("spinner", "⏳")
    stage = _summary_stage_text(lang, str(state.get("stage") or "start"))
    detail = str(state.get("detail") or "").strip()
    elapsed_s = int(max(0, time.time() - float(state.get("started_at") or time.time())))
    line = f"{spinner} {m.get('summary_progress_prefix', '🧠 Preparing summary')} — {stage}"
    if detail:
        line += f"\n{detail}"
    line += f"\n{m.get('summary_progress_elapsed', '⏱️ {seconds}s').format(seconds=elapsed_s)}"
    return line


def _summary_progress_set(state: dict | None, stage: str, detail: str | None = None):
    if state is None:
        return
    state["stage"] = stage
    if detail is not None:
        state["detail"] = detail
    state["updated_at"] = time.time()


def _summary_telegram_split(text: str, max_len: int = 3900) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_len:
        cut = remaining.rfind("\n\n", 0, max_len)
        if cut < max_len * 0.45:
            cut = remaining.rfind("\n", 0, max_len)
        if cut < max_len * 0.45:
            cut = remaining.rfind(" ", 0, max_len)
        if cut < max_len * 0.45:
            cut = max_len
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _summary_chunk_text(text: str, target_chars: int = 7000, overlap_chars: int = 500) -> list[str]:
    normalized = "\n".join(line.rstrip() for line in (text or "").splitlines())
    paras = [p.strip() for p in re.split(r"\n\s*\n", normalized) if p.strip()]
    if not paras:
        stripped = normalized.strip()
        return [stripped] if stripped else []
    chunks: list[str] = []
    buf = ""
    for para in paras:
        candidate = f"{buf}\n\n{para}".strip() if buf else para
        if len(candidate) <= target_chars:
            buf = candidate
            continue
        if buf:
            chunks.append(buf)
            tail = buf[-overlap_chars:].strip()
            buf = (tail + "\n\n" + para).strip() if tail else para
        else:
            # Split very long paragraph on sentence-ish boundaries.
            tmp = para
            while len(tmp) > target_chars:
                cut = tmp.rfind(". ", 0, target_chars)
                if cut < target_chars * 0.5:
                    cut = tmp.rfind(" ", 0, target_chars)
                if cut < target_chars * 0.5:
                    cut = target_chars
                chunks.append(tmp[:cut].strip())
                tmp = tmp[cut:].strip()
            buf = tmp
    if buf:
        chunks.append(buf)
    return [c for c in chunks if c.strip()]


def _summary_tesseract_lang_candidates(lang: str) -> list[str]:
    base = {"uz": "uzb+eng", "ru": "rus+eng", "en": "eng"}.get(lang, "eng")
    candidates = [base]
    if base != "eng":
        candidates.append("eng")
    return candidates


def _summary_ocr_pdf_text_blocking(file_path: str, lang: str, max_chars: int, progress: dict | None = None) -> str:
    if not shutil.which("pdftoppm") or not shutil.which("tesseract"):
        raise RuntimeError("ocr-tools-missing")
    dpi = os.getenv("BOOK_SUMMARY_OCR_DPI", "150")
    max_pages = int(os.getenv("BOOK_SUMMARY_OCR_MAX_PAGES", "20"))
    render_timeout_s = float(os.getenv("BOOK_SUMMARY_OCR_RENDER_TIMEOUT_S", "30") or "30")
    tesseract_timeout_s = float(os.getenv("BOOK_SUMMARY_OCR_TESS_TIMEOUT_S", "30") or "30")
    chars_per_page_cap = max(800, max_chars // max(1, max_pages))
    out: list[str] = []
    total = 0
    with tempfile.TemporaryDirectory(prefix="bookocr_") as td:
        for page_num in range(1, max_pages + 1):
            if total >= max_chars:
                break
            _summary_progress_set(progress, "ocr", f"📄 OCR page {page_num}/{max_pages}")
            img_prefix = os.path.join(td, f"p{page_num}")
            render = safe_subprocess.run(
                [
                    "pdftoppm",
                    "-f",
                    str(page_num),
                    "-l",
                    str(page_num),
                    "-r",
                    str(dpi),
                    "-png",
                    "-singlefile",
                    file_path,
                    img_prefix,
                ],
                timeout_s=render_timeout_s,
                max_output_chars=8000,
                text=True,
            )
            img_path = f"{img_prefix}.png"
            if render.returncode != 0 or not os.path.exists(img_path):
                # No more pages or render failure
                if page_num == 1:
                    raise RuntimeError(f"ocr-render-failed: {str(render.stderr or '').strip()}")
                break
            page_text = ""
            for lang_code in _summary_tesseract_lang_candidates(lang):
                ocr = safe_subprocess.run(
                    ["tesseract", img_path, "stdout", "-l", lang_code, "--psm", "6"],
                    timeout_s=tesseract_timeout_s,
                    max_output_chars=20000,
                    text=True,
                )
                if ocr.returncode == 0:
                    page_text = (ocr.stdout or "").strip()
                    if page_text:
                        break
            if not page_text:
                continue
            page_text = re.sub(r"[ \t]+\n", "\n", page_text)
            page_text = re.sub(r"\n{3,}", "\n\n", page_text).strip()
            if not page_text:
                continue
            page_text = page_text[: min(chars_per_page_cap, max_chars - total)]
            if total:
                out.append("\n\n")
                total += 2
            out.append(page_text)
            total += len(page_text)
    return "".join(out).strip()


def _summary_extract_text_blocking(file_path: str, lang: str = "en", progress: dict | None = None) -> str:
    ext = os.path.splitext(file_path or "")[1].lower()
    # Keep a generous floor so summaries can analyze much more of the book by default.
    # If the env is set lower, we still use this floor to avoid aggressive truncation.
    max_chars = max(int(os.getenv("BOOK_SUMMARY_MAX_CHARS_EXTRACT", "180000")), 450000)
    if ext == ".pdf":
        if PdfReader is None:
            raise RuntimeError("pypdf not installed")
        _summary_progress_set(progress, "extract", "📄 PDF text extraction")
        reader = PdfReader(file_path)
        out: list[str] = []
        total = 0
        max_pages = int(os.getenv("BOOK_SUMMARY_MAX_PAGES", "120"))
        for idx, page in enumerate(reader.pages):
            if idx >= max_pages or total >= max_chars:
                break
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            txt = txt.strip()
            if not txt:
                continue
            if total:
                out.append("\n\n")
                total += 2
            txt = txt[: max_chars - total]
            out.append(txt)
            total += len(txt)
        extracted = "".join(out).strip()
        min_text_chars = int(os.getenv("BOOK_SUMMARY_PDF_MIN_TEXT_CHARS", "120"))
        # OCR fallback for scanned/image PDFs when extractable text is too small.
        if len(extracted) < min_text_chars:
            try:
                _summary_progress_set(progress, "ocr", "🔎 OCR fallback")
                ocr_text = _summary_ocr_pdf_text_blocking(file_path, lang, max_chars=max_chars, progress=progress)
                if len(ocr_text) > len(extracted):
                    logger.info("summary OCR fallback used for PDF: %s", os.path.basename(file_path))
                    return ocr_text
            except Exception as e:
                logger.info("summary OCR fallback failed for %s: %s", os.path.basename(file_path), e)
        return extracted
    if ext in {".txt", ".md", ".markdown", ".csv", ".json", ".log", ".xml", ".html", ".htm", ".fb2"}:
        _summary_progress_set(progress, "extract", "📝 Reading text file")
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read(max_chars).strip()
    raise RuntimeError(f"unsupported file type: {ext or 'unknown'}")


def _summary_text_hash(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8", errors="ignore")).hexdigest()


def _summary_ollama_generate_blocking(
    prompt: str,
    num_predict: int = 450,
    timeout_s: float | None = None,
    *,
    temperature: float = 0.1,
) -> tuple[str, str]:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv("BOOK_SUMMARY_OLLAMA_MODEL", os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b"))
    timeout = float(timeout_s or os.getenv("BOOK_SUMMARY_OLLAMA_TIMEOUT", "70"))
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": "10m",
        "options": {"temperature": temperature, "num_predict": num_predict},
    }
    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except (TimeoutError, socket.timeout):
        # Cold model loads and long summaries can exceed first timeout.
        with urllib.request.urlopen(req, timeout=max(timeout, 180.0)) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    text = str((data or {}).get("response") or "").strip()
    if not text:
        raise RuntimeError("empty ollama response")
    return text, model


def _summary_prompt_for_mode(lang: str, mode: str, title: str, content: str) -> str:
    lang_name = _summary_lang_name(lang)
    mode_spec = {
        "short": "Return a concise summary in 5-8 lines.",
        "detailed": "Return a detailed summary with clear paragraphs covering main ideas, important details, and structure.",
        "points": "Return 8-15 key points as bullet points.",
    }.get(mode, "Return a concise summary.")
    return (
        "You are summarizing a book text extracted from a file.\n"
        f"Write ONLY in {lang_name}.\n"
        "Do not translate quoted names unless needed.\n"
        "Do not invent facts. If the text looks partial/incomplete, mention that briefly.\n"
        "Do not add random commentary, greetings, or meta explanations.\n"
        "Do not mention that you are an AI.\n"
        f"{mode_spec}\n\n"
        f"Book title: {title}\n\n"
        "Text:\n"
        f"{content}"
    )


def _summary_chunk_prompt(lang: str, title: str, chunk_index: int, total_chunks: int, chunk_text: str) -> str:
    lang_name = _summary_lang_name(lang)
    return (
        "Summarize this chunk of a book into compact notes.\n"
        f"Write ONLY in {lang_name}.\n"
        "Do not invent facts.\n"
        "No intro sentence. No conclusion. Only chunk notes.\n"
        "Return 6-10 bullet points covering characters/topics/events/arguments present in this chunk.\n\n"
        f"Book title: {title}\n"
        f"Chunk: {chunk_index}/{total_chunks}\n\n"
        "Chunk text:\n"
        f"{chunk_text}"
    )


def _summary_cleanup_output(text: str, mode: str) -> str:
    s = str(text or "").strip()
    if not s:
        return s
    s = re.sub(r"^```[a-zA-Z]*\n?", "", s)
    s = re.sub(r"\n?```$", "", s).strip()
    lines = [ln.rstrip() for ln in s.splitlines()]
    cleaned: list[str] = []
    seen_consecutive = None
    for ln in lines:
        t = ln.strip()
        if not t:
            if cleaned and cleaned[-1] == "":
                continue
            cleaned.append("")
            continue
        # normalize bullet markers
        if mode == "points":
            t = re.sub(r"^\s*(?:[-*•]+|\d+[.)])\s*", "• ", t)
        if t.lower().startswith(("as an ai", "i am an ai", "men sun'iy intellekt", "я искусственный интеллект")):
            continue
        key = re.sub(r"\s+", " ", t).strip().lower()
        if key == seen_consecutive:
            continue
        cleaned.append(t)
        seen_consecutive = key
    out = "\n".join(cleaned).strip()
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out


def _summary_output_looks_invalid(text: str, lang: str) -> bool:
    s = (text or "").strip()
    if len(s) < 40:
        return True
    norm_lines = [re.sub(r"\s+", " ", ln).strip().lower() for ln in s.splitlines() if ln.strip()]
    if len(norm_lines) >= 4:
        counts = {}
        for ln in norm_lines:
            counts[ln] = counts.get(ln, 0) + 1
        if max(counts.values()) >= 3:
            return True
    if lang == "ru":
        cyr = len(re.findall(r"[А-Яа-яЁё]", s))
        lat = len(re.findall(r"[A-Za-z]", s))
        if cyr < 20 and lat > cyr * 2:
            return True
    return False


def _summary_summarize_text_blocking(text: str, lang: str, mode: str, title: str, progress: dict | None = None) -> tuple[str, str]:
    cleaned = re.sub(r"\n{3,}", "\n\n", (text or "")).strip()
    if not cleaned:
        raise ValueError("empty text")
    # Summarize from a much larger portion of the book (chunked + merged),
    # so the result reflects the whole book better.
    max_input = max(int(os.getenv("BOOK_SUMMARY_MAX_INPUT_CHARS", "120000")), 350000)
    cleaned = cleaned[:max_input]
    chunks = _summary_chunk_text(cleaned)
    if not chunks:
        raise ValueError("empty chunks")

    if len(chunks) == 1:
        _summary_progress_set(progress, "ai", "🧠 AI analyzing text")
        out, model = _summary_ollama_generate_blocking(_summary_prompt_for_mode(lang, mode, title, chunks[0]), num_predict=700)
        out = _summary_cleanup_output(out, mode)
        if _summary_output_looks_invalid(out, lang):
            out, model = _summary_ollama_generate_blocking(
                _summary_prompt_for_mode(lang, mode, title, chunks[0]) + "\n\nReturn only the final summary in the requested language.",
                num_predict=700,
                temperature=0.0,
            )
            out = _summary_cleanup_output(out, mode)
        return out, model

    partials: list[str] = []
    model_name = ""
    for idx, chunk in enumerate(chunks, start=1):
        _summary_progress_set(progress, "ai", "🧠 AI analyzing book")
        partial, model_name = _summary_ollama_generate_blocking(
            _summary_chunk_prompt(lang, title, idx, len(chunks), chunk),
            num_predict=350,
            timeout_s=max(float(os.getenv("BOOK_SUMMARY_OLLAMA_TIMEOUT", "70")), 70.0),
            temperature=0.0,
        )
        partials.append(_summary_cleanup_output(partial, "points").strip())

    combined_notes = "\n\n".join(f"Chunk {i+1} notes:\n{p}" for i, p in enumerate(partials))
    _summary_progress_set(progress, "finalize", "🧩 Merging chunk summaries")
    final_prompt = (
        _summary_prompt_for_mode(lang, mode, title, combined_notes)
        + "\n\nUse the chunk notes above as source material and produce one final coherent answer."
    )
    final_text, final_model = _summary_ollama_generate_blocking(final_prompt, num_predict=900, temperature=0.0)
    final_text = _summary_cleanup_output(final_text, mode)
    if _summary_output_looks_invalid(final_text, lang):
        _summary_progress_set(progress, "finalize", "🔁 Retrying final summary")
        retry_prompt = final_prompt + (
            "\n\nSTRICT REQUIREMENTS:\n"
            "- Use only the requested language.\n"
            "- No meta text.\n"
            "- No random sentences.\n"
            "- Output only the summary.\n"
        )
        final_text, final_model = _summary_ollama_generate_blocking(retry_prompt, num_predict=900, temperature=0.0)
        final_text = _summary_cleanup_output(final_text, mode)
    return final_text, (final_model or model_name)


async def _summary_send_text(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_to_message_id: int | None = None):
    parts = _summary_telegram_split(text)
    for idx, part in enumerate(parts):
        kwargs = {"chat_id": chat_id, "text": part}
        if idx == 0 and reply_to_message_id:
            kwargs["reply_to_message_id"] = reply_to_message_id
        await _send_with_retry(lambda kwargs=kwargs: context.bot.send_message(**kwargs))


async def _summary_edit_progress_message(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    text: str,
):
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
        return True
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return True
    except RetryAfter as e:
        logger.warning("summary progress edit flood wait (%ss)", getattr(e, "retry_after", "?"))
    except Exception:
        pass
    return False


async def _summary_progress_loop(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    lang: str,
    state: dict,
):
    frames = ["⏳", "⌛", "🔄", "🧠"]
    i = 0
    last_text = None
    while not state.get("done"):
        state["spinner"] = frames[i % len(frames)]
        i += 1
        text = _summary_progress_render(lang, state)
        if text != last_text:
            await _summary_edit_progress_message(context, chat_id, message_id, text)
            last_text = text
        await asyncio.sleep(1.8)
    state["spinner"] = "✅" if not state.get("error") else "❌"
    final_text = _summary_progress_render(lang, state)
    if state.get("error"):
        err_line = str(state.get("error_text") or "").strip()
        if err_line:
            final_text += f"\n{err_line}"
    await _summary_edit_progress_message(context, chat_id, message_id, final_text)


async def _summary_prepare_text_for_book(
    context: ContextTypes.DEFAULT_TYPE,
    book: dict,
    lang: str,
    progress: dict | None = None,
) -> tuple[str, str]:
    path = (book.get("path") or "").strip()
    _summary_progress_set(progress, "source", "📦 Checking file source")
    if path and os.path.exists(path):
        _summary_progress_set(progress, "extract", "📄 Reading local file")
        text = await run_blocking(_summary_extract_text_blocking, path, lang, progress)
        return text, _summary_text_hash(text)

    file_id = (book.get("file_id") or "").strip()
    if not file_id:
        raise FileNotFoundError("no file source")

    guessed_name = get_display_name(book)
    ext = ""
    if path:
        ext = os.path.splitext(path)[1]
    if not ext:
        ext = os.path.splitext(guessed_name)[1]
    try:
        _summary_progress_set(progress, "download", "⬇️ Requesting file from Telegram")
        tg_file = await context.bot.get_file(file_id)
    except Exception as e:
        raise RuntimeError(f"download-get-file-failed: {e}") from e
    if not ext:
        ext = os.path.splitext((getattr(tg_file, "file_path", "") or ""))[1]
    if not ext:
        ext = ".bin"

    with tempfile.TemporaryDirectory(prefix="booksum_") as td:
        temp_path = os.path.join(td, f"{book.get('id') or 'book'}{ext}")
        try:
            _summary_progress_set(progress, "download", "⬇️ Downloading temp file")
            await tg_file.download_to_drive(custom_path=temp_path)
        except Exception as e:
            raise RuntimeError(f"download-to-temp-failed: {e}") from e
        _summary_progress_set(progress, "extract", "📄 Extracting text from downloaded file")
        text = await run_blocking(_summary_extract_text_blocking, temp_path, lang, progress)
        return text, _summary_text_hash(text)


async def _run_book_summary_job(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    reply_to_message_id: int | None,
    book_id: str,
    lang: str,
    mode: str,
    user_id: int | None,
    progress: dict | None = None,
):
    jobs = context.application.bot_data.setdefault("summary_jobs", {})
    job_key = f"{user_id or 0}:{book_id}:{lang}:{mode}"
    try:
        m = MESSAGES.get(lang, MESSAGES["en"])
        _summary_progress_set(progress, "cache")
        book = await run_blocking(db_get_book_by_id, book_id)
        if not book:
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_book_missing"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_book_missing"], reply_to_message_id)
            return

        cached = await run_blocking(db_get_book_summary, book_id, lang, mode)
        if cached and cached.get("summary_text"):
            title = get_result_title(book)
            mode_label = _summary_mode_label(lang, mode)
            header = m["summary_ready_title"].format(mode=mode_label)
            body = f"{header}\n📘 {title}\n{m['summary_cached_note']}\n\n{cached['summary_text']}"
            _summary_progress_set(progress, "send", "⚡ Cached")
            await _summary_send_text(context, chat_id, body, reply_to_message_id)
            _summary_progress_set(progress, "done")
            return

        try:
            extracted_text, source_hash = await _summary_prepare_text_for_book(context, book, lang, progress)
        except FileNotFoundError:
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_no_source"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_no_source"], reply_to_message_id)
            return
        except RuntimeError as e:
            msg = str(e)
            if "too big" in msg.lower():
                if progress is not None:
                    progress["error"] = True
                    progress["error_text"] = m.get("summary_download_too_big", m["summary_download_failed"])
                await _summary_send_text(context, chat_id, m.get("summary_download_too_big", m["summary_download_failed"]), reply_to_message_id)
            elif msg.startswith("download-"):
                if progress is not None:
                    progress["error"] = True
                    progress["error_text"] = m["summary_download_failed"]
                await _summary_send_text(context, chat_id, m["summary_download_failed"], reply_to_message_id)
            else:
                if progress is not None:
                    progress["error"] = True
                    progress["error_text"] = m["summary_extract_failed"]
                await _summary_send_text(context, chat_id, m["summary_extract_failed"], reply_to_message_id)
            _summary_progress_set(progress, "done")
            logger.warning("summary source preparation failed for %s: %s", book_id, e)
            return
        except Exception as e:
            logger.warning("summary source preparation error for %s: %s", book_id, e, exc_info=True)
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_error"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_error"], reply_to_message_id)
            return

        if not extracted_text or len(extracted_text.strip()) < 80:
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_empty_text"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_empty_text"], reply_to_message_id)
            return

        title = get_result_title(book)
        try:
            _summary_progress_set(progress, "ai", "🧠 AI analyzing")
            summary_text, model_name = await run_blocking(
                _summary_summarize_text_blocking,
                extracted_text,
                lang,
                mode,
                title,
                progress,
            )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, socket.timeout) as e:
            logger.info("book summary ollama unavailable: %s", e)
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_ai_unavailable"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_ai_unavailable"], reply_to_message_id)
            return
        except Exception as e:
            logger.warning("book summary generation failed for %s: %s", book_id, e, exc_info=True)
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_error"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_error"], reply_to_message_id)
            return

        summary_text = summary_text.strip()
        if not summary_text:
            if progress is not None:
                progress["error"] = True
                progress["error_text"] = m["summary_ai_unavailable"]
            _summary_progress_set(progress, "done")
            await _summary_send_text(context, chat_id, m["summary_ai_unavailable"], reply_to_message_id)
            return

        _summary_progress_set(progress, "save", "💾 Saving summary")
        await run_blocking(
            db_upsert_book_summary,
            book_id,
            lang,
            mode,
            summary_text,
            model_name,
            source_hash,
        )

        mode_label = _summary_mode_label(lang, mode)
        header = m["summary_ready_title"].format(mode=mode_label)
        body = f"{header}\n📘 {title}\n\n{summary_text}"
        _summary_progress_set(progress, "send", "📤 Sending summary")
        await _summary_send_text(context, chat_id, body, reply_to_message_id)
        if progress is not None:
            progress["error"] = False
        _summary_progress_set(progress, "done")
    finally:
        if progress is not None:
            progress["done"] = True
        jobs.pop(job_key, None)


async def handle_summary_placeholder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    parts = data.split(":")
    if len(parts) >= 2 and parts[1] == "cancel":
        try:
            if query.message:
                await query.message.delete()
        except Exception:
            pass
        await safe_answer(query)
        return

    if len(parts) == 2:
        book_id = parts[1]
        if not book_id:
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        if not await run_blocking(db_get_book_by_id, book_id):
            await safe_answer(query, MESSAGES[lang]["summary_book_missing"], show_alert=True)
            return
        if query.message:
            await _send_with_retry(
                lambda: query.message.reply_text(
                    MESSAGES[lang]["summary_choose_mode"],
                    reply_markup=_summary_mode_keyboard(book_id, lang),
                )
            )
        await safe_answer(query)
        return

    if len(parts) != 3:
        await safe_answer(query)
        return

    _, book_id, mode = parts
    mode = mode.strip().lower()
    if mode not in SUMMARY_MODES:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    user_id = query.from_user.id if query.from_user else None
    job_key = f"{user_id or 0}:{book_id}:{lang}:{mode}"
    jobs = context.application.bot_data.setdefault("summary_jobs", {})
    existing = jobs.get(job_key)
    if existing and not existing.done():
        await safe_answer(query, MESSAGES[lang]["summary_already_running"], show_alert=True)
        return

    working_msg = None
    if query.message:
        working_msg = await _send_with_retry(lambda: query.message.reply_text(MESSAGES[lang]["summary_working"]))
    if query.message:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
    await safe_answer(query)

    chat_id = query.message.chat_id if query.message else (query.from_user.id if query.from_user else None)
    if not chat_id:
        return

    progress = None
    if working_msg:
        progress = {
            "stage": "start",
            "detail": "",
            "started_at": time.time(),
            "done": False,
            "error": False,
        }
        asyncio.create_task(
            _summary_progress_loop(
                context,
                working_msg.chat_id,
                working_msg.message_id,
                lang,
                progress,
            )
        )

    task = asyncio.create_task(
        _run_book_summary_job(
            context,
            chat_id=chat_id,
            reply_to_message_id=None,
            book_id=book_id,
            lang=lang,
            mode=mode,
            user_id=user_id,
            progress=progress,
        )
    )
    jobs[job_key] = task


async def handle_delete_book_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    user_id = query.from_user.id if query.from_user else None
    if not user_id or not await can_delete_books(user_id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    data = query.data or ""
    book_id = data.split(":", 1)[1] if ":" in data else ""
    if not book_id:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    now = time.time()
    pending = context.user_data.get("pending_delete")
    if not pending or pending.get("book_id") != book_id or now > pending.get("expires_at", 0):
        context.user_data["pending_delete"] = {"book_id": book_id, "expires_at": now + 15}
        await safe_answer(query, MESSAGES[lang]["delete_confirm"], show_alert=True)
        return

    context.user_data.pop("pending_delete", None)

    book = await run_blocking(db_get_book_by_id, book_id)
    title = get_result_title(book) if book else book_id

    local_deleted = False
    local_missing = False
    if book and book.get("path"):
        path = book.get("path")
        try:
            if path and os.path.exists(path):
                os.remove(path)
                local_deleted = True
            else:
                local_missing = True
        except Exception as e:
            logger.error(f"Failed to delete local file {path}: {e}")

    deleted_db = await run_blocking(delete_book_and_related, book_id)
    # cascade delete audiobooks associated with this book
    await run_blocking(delete_audio_books_by_book_id, book_id)

    deleted_es = 0
    failed_es = 0
    if es_available():
        es = get_es()
        if es:
            try:
                await run_blocking(lambda: es.delete(index=ES_INDEX, id=book_id))
                deleted_es = 1
            except NotFoundError:
                deleted_es = 0
            except Exception:
                failed_es = 1

    _invalidate_top_caches(context)

    try:
        if query.message:
            await query.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    local_note = ""
    if local_deleted:
        local_note = MESSAGES[lang]["delete_local_deleted"]
    elif local_missing:
        local_note = MESSAGES[lang]["delete_local_missing"]

    delete_msg = MESSAGES[lang]["delete_done"].format(
        title=title,
        db=deleted_db,
        es=deleted_es,
        es_failed=failed_es,
        local=local_note,
    )
    try:
        if query.message:
            await query.message.reply_text(delete_msg)
        else:
            await context.bot.send_message(chat_id=user_id, text=delete_msg)
    except Exception:
        pass
    await safe_answer(query)
