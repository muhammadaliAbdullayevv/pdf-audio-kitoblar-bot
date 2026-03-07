from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
import uuid
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
from telegram.ext import ContextTypes

try:
    from telegram import ReactionTypeEmoji
except Exception:
    ReactionTypeEmoji = None  # type: ignore

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)

# Import Redis cache for performance
try:
    from cache import cache_get, cache_set, cache_delete
    REDIS_CACHE_AVAILABLE = True
except ImportError:
    REDIS_CACHE_AVAILABLE = False
    cache_get = lambda k: None
    cache_set = lambda k, v, ttl=300: False
    cache_delete = lambda k: False


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v


_THANKS_REPLY_PATTERNS: list[tuple[str, str]] = [
    ("uz", r"\b(rahm+a+t|raxm+a+t|rakhmat|tashakkur|tashakkurlar|minnatdor(m\w*)?)\b"),
    ("en", r"\b(thanks?|thank you|thankyou|thank u|thx|tnx|appreciate it|much appreciated)\b"),
    ("ru", r"\b(спасибо|благодарю|благодарствую|spasibo)\b"),
]


def _detect_thanks_reply_lang(text: str) -> str | None:
    cleaned = re.sub(r"[^\w\s]+", " ", str(text or "").lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None
    for lang_key, pattern in _THANKS_REPLY_PATTERNS:
        if re.search(pattern, cleaned):
            return lang_key
    return None


async def _send_reaction_for_message(update: Update, context: ContextTypes.DEFAULT_TYPE, emoji: str) -> None:
    msg = getattr(update, "message", None)
    chat = getattr(update, "effective_chat", None)
    bot = getattr(context, "bot", None)
    if not msg or not chat or not bot or not emoji:
        return

    # PTB >= 21 has native set_message_reaction; PTB 20.x can still call raw API via _post.
    if hasattr(bot, "set_message_reaction") and ReactionTypeEmoji is not None:
        try:
            await bot.set_message_reaction(
                chat_id=chat.id,
                message_id=msg.message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
                is_big=False,
            )
            return
        except Exception as e:
            logger.debug("message reaction (native) failed: %s", e)

    try:
        reaction_payload = json.dumps([{"type": "emoji", "emoji": emoji}], ensure_ascii=False)
        await bot._post(
            "setMessageReaction",
            data={
                "chat_id": chat.id,
                "message_id": msg.message_id,
                "reaction": reaction_payload,
                "is_big": False,
            },
        )
    except Exception as e:
        # Reactions are optional and may be unsupported in some chats / Bot API versions.
        logger.debug("message reaction (raw) failed: %s", e)
        return


async def _send_heart_reaction_for_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_reaction_for_message(update, context, "❤️")


async def _send_salute_reaction_for_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_reaction_for_message(update, context, "🫡")


def _get_audio_channel_send_guard(context: ContextTypes.DEFAULT_TYPE, channel_id: int):
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", None) if app else None
    if not isinstance(bot_data, dict):
        return None, None

    locks = bot_data.setdefault("_audio_channel_send_locks", {})
    if not isinstance(locks, dict):
        locks = {}
        bot_data["_audio_channel_send_locks"] = locks
    lock = locks.get(channel_id)
    if lock is None:
        lock = asyncio.Lock()
        locks[channel_id] = lock

    states = bot_data.setdefault("_audio_channel_send_state", {})
    if not isinstance(states, dict):
        states = {}
        bot_data["_audio_channel_send_state"] = states
    state = states.get(channel_id)
    if not isinstance(state, dict):
        state = {"next_allowed_at": 0.0}
        states[channel_id] = state
    state.setdefault("next_allowed_at", 0.0)
    return lock, state


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


def prune_search_cache(cache: dict, max_items: int = 5, max_age_sec: int = 1800):
    now = time.time()
    # Remove stale entries
    stale_keys = [k for k, v in cache.items() if now - v.get("ts", 0) > max_age_sec]
    for k in stale_keys:
        cache.pop(k, None)
    # Keep only most recent
    if len(cache) > max_items:
        sorted_items = sorted(cache.items(), key=lambda kv: kv[1].get("ts", 0), reverse=True)
        for k, _ in sorted_items[max_items:]:
            cache.pop(k, None)


def cache_search_results(context: ContextTypes.DEFAULT_TYPE, query: str, results: list):
    cache = context.user_data.setdefault("search_cache", {})
    prune_search_cache(cache)
    query_id = uuid.uuid4().hex[:8]
    cache[query_id] = {"query": query, "results": results, "ts": time.time()}
    context.user_data["last_search_id"] = query_id
    return query_id


def get_search_cache(context: ContextTypes.DEFAULT_TYPE, query_id: str):
    cache = context.user_data.get("search_cache", {})
    return cache.get(query_id)


def cache_movie_search_results(context: ContextTypes.DEFAULT_TYPE, query: str, results: list):
    cache = context.user_data.setdefault("movie_search_cache", {})
    prune_search_cache(cache)
    query_id = uuid.uuid4().hex[:8]
    cache[query_id] = {"query": query, "results": results, "ts": time.time()}
    context.user_data["last_movie_search_id"] = query_id
    return query_id


def get_movie_search_cache(context: ContextTypes.DEFAULT_TYPE, query_id: str):
    cache = context.user_data.get("movie_search_cache", {})
    return cache.get(query_id)


def cache_user_results(context: ContextTypes.DEFAULT_TYPE, query: str, results: list):
    cache = context.user_data.setdefault("user_search_cache", {})
    prune_search_cache(cache)
    query_id = uuid.uuid4().hex[:8]
    cache[query_id] = {"query": query, "results": results, "ts": time.time()}
    context.user_data["last_user_search_id"] = query_id
    return query_id


def get_user_search_cache(context: ContextTypes.DEFAULT_TYPE, query_id: str):
    cache = context.user_data.get("user_search_cache", {})
    return cache.get(query_id)


def cache_top_results(context: ContextTypes.DEFAULT_TYPE, results: list):
    if REDIS_CACHE_AVAILABLE:
        query_id = uuid.uuid4().hex[:8]
        cache_key = f"top_results:{query_id}"
        cache_set(cache_key, {"results": results, "ts": time.time()}, ttl=300)
        return query_id
    else:
        # Fallback to memory cache
        cache = context.user_data.setdefault("top_cache", {})
        prune_search_cache(cache)
        query_id = uuid.uuid4().hex[:8]
        cache[query_id] = {"results": results, "ts": time.time()}
        return query_id


def get_top_cache(context: ContextTypes.DEFAULT_TYPE, query_id: str):
    if REDIS_CACHE_AVAILABLE:
        cache_key = f"top_results:{query_id}"
        return cache_get(cache_key)
    else:
        # Fallback to memory cache
        cache = context.user_data.get("top_cache", {})
        return cache.get(query_id)


def get_cached_top_entries(context: ContextTypes.DEFAULT_TYPE):
    cache = context.application.bot_data.get("top_entries_cache")
    if not cache:
        return None
    if time.time() - cache.get("ts", 0) > TOP_CACHE_TTL:
        return None
    return cache.get("entries")


def set_cached_top_entries(context: ContextTypes.DEFAULT_TYPE, entries: list):
    context.application.bot_data["top_entries_cache"] = {
        "entries": entries,
        "ts": time.time(),
    }


def invalidate_top_caches(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        context.application.bot_data.pop("top_entries_cache", None)
    except Exception:
        pass
    try:
        context.user_data.pop("top_cache", None)
    except Exception:
        pass


def get_cached_audit_report(context: ContextTypes.DEFAULT_TYPE, lang: str):
    cache = context.application.bot_data.get("audit_cache", {})
    entry = cache.get(lang)
    if not entry:
        return None
    if time.time() - entry.get("ts", 0) > AUDIT_CACHE_TTL:
        return None
    return entry.get("text")


def set_cached_audit_report(context: ContextTypes.DEFAULT_TYPE, lang: str, text: str):
    cache = context.application.bot_data.setdefault("audit_cache", {})
    cache[lang] = {"text": text, "ts": time.time()}


def build_results_text(query: str, entries: list, page: int, lang: str):
    total = len(entries)
    pages = max(1, int(math.ceil(total / PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_entries = entries[start:end]

    header = MESSAGES[lang]["results_header"].format(
        query=query,
        page=page + 1,
        pages=pages,
        total=total
    ) + "\n\n"
    lines = [f"{i}. {e['title']}" for i, e in enumerate(page_entries, start=start + 1)]
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return header + body + footer, page_entries, pages


def build_results_keyboard(entries: list, page: int, pages: int, query_id: str):
    keyboard = []
    row = []
    start_idx = page * PAGE_SIZE
    for idx, entry in enumerate(entries, start=start_idx + 1):
        row.append(
            InlineKeyboardButton(
                str(idx),
                callback_data=f"book:{entry['id']}"
            )
        )
        if idx % 5 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"page:{query_id}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"page:{query_id}:{page + 1}"))
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


def build_user_results_text(query: str, entries: list, page: int, lang: str):
    total = len(entries)
    pages = max(1, int(math.ceil(total / PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_entries = entries[start:end]

    header = MESSAGES[lang]["user_results_header"].format(
        query=query,
        page=page + 1,
        pages=pages,
        total=total
    ) + "\n\n"
    lines = [f"{i}. {e['title']}" for i, e in enumerate(page_entries, start=start + 1)]
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return header + body + footer, page_entries, pages


def build_movie_results_text(query: str, entries: list, page: int, lang: str):
    total = len(entries)
    pages = max(1, int(math.ceil(total / PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_entries = entries[start:end]

    header = MESSAGES[lang]["movie_results_header"].format(
        query=query,
        page=page + 1,
        pages=pages,
        total=total
    ) + "\n\n"
    lines = [f"{i}. {e['title']}" for i, e in enumerate(page_entries, start=start + 1)]
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return header + body + footer, page_entries, pages


def build_user_results_keyboard(entries: list, page: int, pages: int, query_id: str):
    keyboard = []
    row = []
    start_idx = page * PAGE_SIZE
    for idx, entry in enumerate(entries, start=start_idx + 1):
        row.append(
            InlineKeyboardButton(
                str(idx),
                callback_data=f"user:{entry['id']}"
            )
        )
        if idx % 5 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"userpage:{query_id}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"userpage:{query_id}:{page + 1}"))
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


def build_movie_results_keyboard(entries: list, page: int, pages: int, query_id: str):
    keyboard = []
    row = []
    start_idx = page * PAGE_SIZE
    for idx, entry in enumerate(entries, start=start_idx + 1):
        row.append(
            InlineKeyboardButton(
                str(idx),
                callback_data=f"movie:{entry['id']}"
            )
        )
        if idx % 5 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"mpage:{query_id}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"mpage:{query_id}:{page + 1}"))
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


def build_user_info_text(user: dict) -> str:
    name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip() or "—"
    username = f"@{user.get('username')}" if user.get("username") else "—"
    language = user.get("language") or "—"
    joined = user.get("joined_date") or "—"
    return "\n".join([
        f"👤 Name: {name}",
        f"🔤 Username: {username}",
        f"🆔 User ID: {user.get('id')}",
        f"🌐 Language: {language}",
        f"📅 Joined: {joined}",
    ])


def build_user_admin_keyboard(user: dict) -> InlineKeyboardMarkup:
    user_id = user.get("id")
    blocked = bool(user.get("blocked"))
    upload_allowed = bool(user.get("allowed"))
    delete_allowed = bool(user.get("delete_allowed"))
    stopped = bool(user.get("stopped"))

    def mark(flag: bool) -> str:
        return "✅" if flag else "❌"

    keyboard = [
        [
            InlineKeyboardButton(f"🚫 Block {mark(blocked)}", callback_data=f"uact:block:{user_id}"),
            InlineKeyboardButton(f"⬆️ Upload {mark(upload_allowed)}", callback_data=f"uact:upload:{user_id}"),
        ],
        [
            InlineKeyboardButton(f"🗑️ Delete {mark(delete_allowed)}", callback_data=f"uact:del:{user_id}"),
            InlineKeyboardButton(f"🔇 Stop {mark(stopped)}", callback_data=f"uact:stop:{user_id}"),
        ],
        [
            InlineKeyboardButton("➕ Bonus", callback_data=f"uact:bonus_add:{user_id}"),
            InlineKeyboardButton("➖ Bonus", callback_data=f"uact:bonus_del:{user_id}"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)



def transliterate_to_latin(text: str) -> str:
    mapping = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
        "ё": "yo", "ж": "j", "з": "z", "и": "i", "й": "y", "к": "k",
        "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
        "с": "s", "т": "t", "у": "u", "ф": "f", "х": "x", "ц": "ts",
        "ч": "ch", "ш": "sh", "щ": "sh", "ъ": "", "ы": "i", "ь": "",
        "э": "e", "ю": "yu", "я": "ya"
    }
    result = []
    for ch in text:
        lower = ch.lower()
        result.append(mapping.get(lower, ch))
    return "".join(result)


def _mark_and_check_duplicate_text_update(context: ContextTypes.DEFAULT_TYPE, update: Update) -> bool:
    """Return True if this exact text update was already processed for this user context."""
    msg = getattr(update, "message", None)
    if not msg:
        return False
    dedupe_key = f"{getattr(update, 'update_id', 0)}:{getattr(msg, 'chat_id', 0)}:{getattr(msg, 'message_id', 0)}"
    if context.user_data.get("_last_text_update_key") == dedupe_key:
        return True
    context.user_data["_last_text_update_key"] = dedupe_key
    return False


async def _edit_progress_or_reply(
    progress_message,
    fallback_message,
    text: str,
    reply_markup=None,
):
    if progress_message:
        for attempt in range(2):
            try:
                await progress_message.edit_text(text, reply_markup=reply_markup)
                return
            except Exception as e:
                retry_after = getattr(e, "retry_after", None)
                if retry_after is not None and attempt == 0:
                    await asyncio.sleep(float(retry_after or 1) + 0.5)
                    continue
                break

    for attempt in range(2):
        try:
            await fallback_message.reply_text(text, reply_markup=reply_markup)
            return
        except Exception as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None and attempt == 0:
                await asyncio.sleep(float(retry_after or 1) + 0.5)
                continue
            logger.warning("Failed to send progress/reply text: %s", e)
            return


def _db_retry_params() -> tuple[int, float]:
    retries = int(globals().get("DB_RETRY_ATTEMPTS", 2) or 2)
    base_delay = float(globals().get("DB_RETRY_BASE_DELAY_SEC", 0.20) or 0.20)
    return max(0, retries), max(0.05, base_delay)


async def _run_db_retry(func, *args, **kwargs):
    retry_runner = globals().get("run_blocking_db_retry")
    if callable(retry_runner):
        retries, base_delay = _db_retry_params()
        return await retry_runner(func, *args, retries=retries, base_delay=base_delay, **kwargs)
    return await run_blocking(func, *args, **kwargs)


def _schedule_bg_task(context: ContextTypes.DEFAULT_TYPE, coro) -> None:
    try:
        scheduler = globals().get("_schedule_application_task")
        if callable(scheduler):
            scheduler(context.application, coro)
        else:
            context.application.create_task(coro)
    except Exception:
        try:
            coro.close()
        except Exception:
            pass


_AB_PAGE_SIZE = 10
_AB_PAGINATION_THRESHOLD = 10


def build_audiobook_parts_keyboard(
    audio_book_id: str, parts: list[dict], lang: str, page: int = 0
) -> InlineKeyboardMarkup:
    """Create an inline keyboard listing all parts of an audiobook (3 per row).
    When there are more than _AB_PAGINATION_THRESHOLD parts, shows _AB_PAGE_SIZE
    per page with Prev/Next navigation."""
    msgs = MESSAGES.get(lang, MESSAGES.get("en", {}))
    use_pagination = len(parts) > _AB_PAGINATION_THRESHOLD
    if use_pagination:
        total_pages = max(1, (len(parts) + _AB_PAGE_SIZE - 1) // _AB_PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        page_parts = parts[page * _AB_PAGE_SIZE: (page + 1) * _AB_PAGE_SIZE]
    else:
        page_parts = parts
        total_pages = 1

    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for part in page_parts:
        idx = part.get("part_index")
        if idx is None:
            continue
        row.append(InlineKeyboardButton(str(idx), callback_data=f"abpart:{audio_book_id}:{idx}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if use_pagination:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(
                msgs.get("audiobook_prev_page", "◀ Prev"),
                callback_data=f"abpage:{audio_book_id}:{page - 1}",
            ))
        nav.append(InlineKeyboardButton(
            msgs.get("audiobook_page_indicator", f"{page + 1}/{total_pages}").format(
                page=page + 1, total=total_pages
            ),
            callback_data="noop",
        ))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(
                msgs.get("audiobook_next_page", "Next ▶"),
                callback_data=f"abpage:{audio_book_id}:{page + 1}",
            ))
        rows.append(nav)

    return InlineKeyboardMarkup(rows)


async def handle_audiobook_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Prev/Next page navigation for audiobook parts keyboard."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    # expected format: abpage:<audio_book_id>:<page>
    parts = data.split(":")
    if len(parts) != 3:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    _, audio_book_id, page_str = parts
    try:
        page = int(page_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    all_parts = await run_blocking(list_audio_book_parts, audio_book_id)
    if not all_parts:
        await safe_answer(query, MESSAGES[lang]["audiobook_no_parts"], show_alert=True)
        return
    kb = build_audiobook_parts_keyboard(audio_book_id, all_parts, lang, page=page)
    try:
        await query.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass
    await safe_answer(query)


async def handle_audiobook_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback invoked when user presses "Listen Audiobook" button."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abook:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    audio_book = await run_blocking(get_audio_book_for_book, book_id)
    if not audio_book:
        await safe_answer(query, MESSAGES[lang]["audiobook_no_parts"], show_alert=True)
        return
    # count this view as a search for the audiobook
    _schedule_bg_task(context, _run_db_retry(increment_audio_book_searches, [audio_book.get("id")]))
    parts = await run_blocking(list_audio_book_parts, audio_book.get("id"))
    if not parts:
        await safe_answer(query, MESSAGES[lang]["audiobook_no_parts"])
        return
    # if only one part, send immediately
    if len(parts) == 1:
        part = parts[0]
        file_id = part.get("file_id")
        caption = audio_book.get("display_title") or audio_book.get("title") or ""
        try:
            await query.message.reply_audio(audio=file_id, caption=caption)
        except Exception:
            try:
                await query.message.reply_document(document=file_id, caption=caption)
            except Exception:
                pass
        await safe_answer(query)
        return
    # otherwise show selection keyboard
    kb = build_audiobook_parts_keyboard(audio_book.get("id"), parts, lang)
    try:
        await query.message.reply_text(MESSAGES[lang]["audiobook_choose_part"], reply_markup=kb)
    except Exception:
        pass
    await safe_answer(query)


async def handle_audiobook_part_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a specific audiobook part when the corresponding button is clicked."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    # expected format abpart:<audio_book_id>:<part_index>
    parts = data.split(":")
    if len(parts) != 3:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    _, audio_book_id, part_str = parts
    try:
        part_index = int(part_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    all_parts = await run_blocking(list_audio_book_parts, audio_book_id)
    part = next((p for p in all_parts if p.get("part_index") == part_index), None)
    if not part:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    file_id = part.get("file_id")
    part_id = part.get("id")
    caption = f"{part_index}/{len(all_parts)}"
    # Build keyboard with delete button (admin only)
    kb = None
    if _is_admin_user(query.from_user.id):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete Part", callback_data=f"apdel:{part_id}")]])
    try:
        await query.message.reply_audio(audio=file_id, caption=caption, reply_markup=kb)
    except Exception:
        try:
            await query.message.reply_document(document=file_id, caption=caption, reply_markup=kb)
        except Exception:
            pass
    # increment audiobook download counter in background
    _schedule_bg_task(context, _run_db_retry(increment_audio_book_download, audio_book_id))
    await safe_answer(query)


_AUDIO_REQUEST_BOOK_ID_RE = re.compile(r"\[book_id:\s*([^\]]+)\]", re.IGNORECASE)


def _extract_requested_book_id(query_text: str) -> str | None:
    if not query_text:
        return None
    match = _AUDIO_REQUEST_BOOK_ID_RE.search(str(query_text))
    if not match:
        return None
    value = str(match.group(1) or "").strip()
    return value or None


async def _notify_waiting_users_audiobook_ready(
    context: ContextTypes.DEFAULT_TYPE,
    book_id: str,
    book_title: str,
) -> int:
    list_requests_fn = globals().get("load_requests") or globals().get("db_list_requests")
    if not callable(list_requests_fn):
        return 0

    try:
        requests = await run_blocking(list_requests_fn)
    except Exception as e:
        logger.warning("Failed to load requests for audiobook-ready notify (book_id=%s): %s", book_id, e)
        return 0

    if not requests:
        return 0

    mark_done_fn = globals().get("mark_request_fulfilled")
    update_status_fn = globals().get("update_request_status")
    notified = 0

    for req in requests:
        if str(req.get("status") or "") not in {"open", "seen"}:
            continue
        req_book_id = _extract_requested_book_id(str(req.get("query") or ""))
        if not req_book_id or str(req_book_id).strip() != str(book_id).strip():
            continue

        user_id = req.get("user_id")
        if not user_id:
            continue

        req_lang = req.get("language") or "en"
        msgs = MESSAGES.get(req_lang, MESSAGES.get("en", {}))
        text = msgs.get(
            "audiobook_ready_notify",
            "🎧 Audiobook is ready for: {title}\n👇 Tap the button below to open the book.",
        ).format(title=book_title)
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton(book_title, callback_data=f"book:{book_id}")]]
        )

        try:
            await context.bot.send_message(chat_id=user_id, text=text, reply_markup=keyboard)
        except Exception as e:
            logger.warning(
                "Failed to send audiobook-ready message to user=%s for book_id=%s: %s",
                user_id,
                book_id,
                e,
            )
            continue

        try:
            req_id = req.get("id")
            if req_id:
                if callable(mark_done_fn):
                    await run_blocking(mark_done_fn, req_id, book_id)
                elif callable(update_status_fn):
                    await run_blocking(update_status_fn, req_id, "done", None, "Audiobook added automatically")
        except Exception as e:
            logger.warning("Failed to mark audiobook request as done (request_id=%s): %s", req.get("id"), e)

        notified += 1

    return notified


async def handle_abook_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capture audio/voice/document while an audiobook add flow is pending."""
    pending = context.user_data.get("pending_abook")
    logger.debug("handle_abook_audio called (pending=%s)", pending is not None)
    if not pending:
        # not in audiobook flow, let other handlers run
        logger.debug("handle_abook_audio: no pending flow, returning")
        return
    # identify file object
    msg = update.message
    if not msg:
        logger.debug("handle_abook_audio: no message, returning")
        return
    file = None
    if getattr(msg, "audio", None):
        file = msg.audio
    elif getattr(msg, "voice", None):
        file = msg.voice
    elif getattr(msg, "document", None) and getattr(msg.document, "mime_type", "").startswith("audio"):
        file = msg.document
    if not file:
        # not an audio attachment
        logger.debug("handle_abook_audio: not an audio attachment, returning")
        return
    # insert part record
    lang = ensure_user_language(update, context)
    audio_book_id = pending.get("audio_book_id")
    part_index = pending.get("next_part_index", 1)
    is_insert_mode = "insert_max" in pending and not pending.get("awaiting_insert_index", False)
    title = getattr(file, "file_name", None)
    file_id = file.file_id
    file_unique = getattr(file, "file_unique_id", None)
    duration = getattr(file, "duration", None)
    stored_channel_id = None
    stored_channel_message_id = None

    # Check if this audio part already exists in THIS audiobook (duplicate prevention within same audiobook)
    duplicate_part = None
    if file_unique:
        try:
            duplicate_part = await run_blocking(
                get_audio_book_part_by_file_unique_id_and_audio_book,
                file_unique,
                audio_book_id,
            )
        except Exception as e:
            logger.debug(
                "Duplicate check by file_unique_id failed for audiobook=%s, file_unique_id=%s: %s",
                audio_book_id,
                file_unique,
                e,
            )
    elif file_id:
        # Fallback for rare payloads without file_unique_id.
        try:
            existing_parts = await run_blocking(list_audio_book_parts, audio_book_id)
            duplicate_part = next((p for p in (existing_parts or []) if p.get("file_id") == file_id), None)
        except Exception as e:
            logger.debug(
                "Fallback duplicate check by file_id failed for audiobook=%s, file_id=%s: %s",
                audio_book_id,
                file_id,
                e,
            )

    if duplicate_part:
        try:
            await msg.reply_text(
                MESSAGES[lang].get(
                    "audiobook_duplicate_part",
                    "❌ This audio is already added to this book. Delete it first if you want to re-add it.",
                )
            )
        except Exception:
            pass
        raise ApplicationHandlerStop()

    # Optional: persist audiobook media in a dedicated channel and use that message media file_id.
    # Fall back to env lookup in case globals were not yet refreshed.
    audio_channel_id = int(
        globals().get("AUDIO_UPLOAD_CHANNEL_ID")
        or os.getenv("AUDIO_UPLOAD_CHANNEL_ID", "0")
        or "0"
    )
    if audio_channel_id:
        logger.info(
            "Audiobook channel storage enabled: channel=%s audiobook=%s part_index=%s",
            audio_channel_id,
            audio_book_id,
            part_index,
        )
        sent = None
        last_err = None
        send_retry_max = 5
        try:
            send_min_interval = max(0.0, float(os.getenv("AUDIO_UPLOAD_SEND_DELAY_SEC", "0.90") or "0.90"))
        except Exception:
            send_min_interval = 0.90

        channel_lock, channel_state = _get_audio_channel_send_guard(context, int(audio_channel_id))

        async def _send_once():
            if getattr(msg, "audio", None):
                return await context.bot.send_audio(chat_id=audio_channel_id, audio=file_id)
            if getattr(msg, "voice", None):
                return await context.bot.send_voice(chat_id=audio_channel_id, voice=file_id)
            return await context.bot.send_document(chat_id=audio_channel_id, document=file_id)

        for attempt in range(1, send_retry_max + 1):
            try:
                if channel_lock is not None:
                    async with channel_lock:
                        if isinstance(channel_state, dict):
                            now_ts = asyncio.get_running_loop().time()
                            next_allowed_at = float(channel_state.get("next_allowed_at", 0.0) or 0.0)
                            if next_allowed_at > now_ts:
                                await asyncio.sleep(next_allowed_at - now_ts)
                        sent = await _send_once()
                        if isinstance(channel_state, dict) and send_min_interval > 0:
                            channel_state["next_allowed_at"] = asyncio.get_running_loop().time() + send_min_interval
                else:
                    sent = await _send_once()
                last_err = None
                break
            except Exception as e:
                last_err = e
                retry_after = getattr(e, "retry_after", None)
                if retry_after is not None and attempt < send_retry_max:
                    wait_s = float(retry_after or 1) + 0.5
                    if isinstance(channel_state, dict):
                        channel_state["next_allowed_at"] = max(
                            float(channel_state.get("next_allowed_at", 0.0) or 0.0),
                            asyncio.get_running_loop().time() + wait_s,
                        )
                    logger.warning(
                        "Audio channel flood control for channel %s, waiting %.2fs (attempt %s/%s)",
                        audio_channel_id,
                        wait_s,
                        attempt,
                        send_retry_max,
                    )
                    await asyncio.sleep(wait_s)
                    continue
                msg_text = str(e).lower()
                transient = any(
                    marker in msg_text
                    for marker in ("timed out", "timeout", "network", "connection reset", "temporary failure")
                )
                if transient and attempt < send_retry_max:
                    backoff = min(10.0, 0.5 * (2 ** (attempt - 1)))
                    if isinstance(channel_state, dict):
                        channel_state["next_allowed_at"] = max(
                            float(channel_state.get("next_allowed_at", 0.0) or 0.0),
                            asyncio.get_running_loop().time() + backoff,
                        )
                    logger.warning(
                        "Audio channel transient error for channel %s: %s (attempt %s/%s, wait %.2fs)",
                        audio_channel_id,
                        e,
                        attempt,
                        send_retry_max,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error("Failed to send audiobook media to audio channel %s: %s", audio_channel_id, e)
                break

        if not sent:
            err_text = (str(last_err).strip() if last_err else "unknown error")
            try:
                await msg.reply_text(
                    f"❌ Failed to store this audio in the audio channel.\n{err_text[:180]}"
                )
            except Exception:
                pass
            if last_err is not None:
                logger.error("Audiobook channel storage failed after retries: %s", last_err)
            raise ApplicationHandlerStop()

        stored_channel_id = int(audio_channel_id)
        stored_channel_message_id = int(getattr(sent, "message_id", 0) or 0) or None
        sent_media = getattr(sent, "audio", None) or getattr(sent, "voice", None) or getattr(sent, "document", None)
        if sent_media is not None:
            new_file_id = getattr(sent_media, "file_id", None)
            new_file_unique = getattr(sent_media, "file_unique_id", None)
            if new_file_id:
                file_id = new_file_id
            if new_file_unique:
                file_unique = new_file_unique
            if duration is None:
                duration = getattr(sent_media, "duration", None)
            if not title:
                title = getattr(sent_media, "file_name", None)
        logger.info(
            "Audiobook media stored in channel=%s message_id=%s audiobook=%s part_index=%s",
            stored_channel_id,
            stored_channel_message_id,
            audio_book_id,
            part_index,
        )
    else:
        logger.info(
            "Audiobook channel storage disabled (AUDIO_UPLOAD_CHANNEL_ID is 0). audiobook=%s part_index=%s",
            audio_book_id,
            part_index,
        )

    # In insert mode: shift existing parts >= part_index up by 1 to make room
    if is_insert_mode:
        try:
            await run_blocking(shift_audio_book_parts_from, audio_book_id, part_index)
        except Exception as e:
            logger.error(f"Failed to shift audiobook parts: {e}")
            try:
                await msg.reply_text("❌ Couldn't prepare insert position. Please try again.")
            except Exception:
                pass
            raise ApplicationHandlerStop()

    try:
        await run_blocking(
            insert_audio_book_part,
            audio_book_id=audio_book_id,
            part_index=part_index,
            title=title,
            file_id=file_id,
            file_unique_id=file_unique,
            path=None,
            duration_seconds=duration,
            channel_id=stored_channel_id,
            channel_message_id=stored_channel_message_id,
        )
    except Exception as e:
        # With the global unique index removed, "duplicate/unique" here typically means
        # a part_index collision (or other constraint). Don't block reuse across books.
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            try:
                await msg.reply_text("❌ Couldn't save this audio part. Please try again.")
            except Exception:
                pass
            logger.warning(f"Audio part insertion failed (constraint): {e}")
            raise ApplicationHandlerStop()
        raise

    if is_insert_mode:
        # Single-file insert: clear the flow after insertion
        context.user_data.pop("pending_abook", None)
    else:
        pending["next_part_index"] = part_index + 1
    try:
        await msg.reply_text(MESSAGES[lang]["audiobook_part_saved"].format(index=part_index))
    except Exception:
        pass

    # Auto-notify users who requested audiobook for this specific book.
    try:
        notify_book_id = str(pending.get("book_id") or "").strip()
        if not notify_book_id:
            get_abook_by_id = globals().get("get_audio_book_by_id")
            if callable(get_abook_by_id):
                abook_row = await run_blocking(get_abook_by_id, audio_book_id)
                notify_book_id = str((abook_row or {}).get("book_id") or "").strip()

        if notify_book_id:
            title = notify_book_id
            find_book_fn = globals().get("find_book_by_id") or globals().get("get_book_by_id")
            if callable(find_book_fn):
                book_row = await run_blocking(find_book_fn, notify_book_id)
                if book_row:
                    get_title = globals().get("get_result_title")
                    if callable(get_title):
                        t = get_title(book_row)
                        if t:
                            title = str(t)
                    if title == notify_book_id:
                        title = str(
                            book_row.get("book_name")
                            or book_row.get("display_name")
                            or title
                        )

            notified_count = await _notify_waiting_users_audiobook_ready(context, notify_book_id, title)
            if notified_count:
                logger.info(
                    "Audiobook-ready auto notifications sent: book_id=%s users=%s",
                    notify_book_id,
                    notified_count,
                )
    except Exception as e:
        logger.warning("Audiobook-ready auto notify failed: %s", e)

    # stop further handlers (like upload_flow.handle_file)
    raise ApplicationHandlerStop()


async def _handle_missing_audiobook_request(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query,
    lang: str,
    book_id: str,
) -> None:
    msgs = MESSAGES.get(lang, MESSAGES.get("en", {}))
    book_title = str(book_id)

    try:
        lookup = globals().get("find_book_by_id") or globals().get("get_book_by_id")
        if callable(lookup):
            book = await run_blocking(lookup, book_id)
            if book:
                get_title = globals().get("get_result_title")
                if callable(get_title):
                    title = get_title(book)
                    if title:
                        book_title = str(title)
                if book_title == str(book_id):
                    book_title = str(
                        book.get("display_name")
                        or book.get("book_name")
                        or book.get("name")
                        or book_title
                    )
    except Exception as e:
        logger.debug("Failed to resolve title for audiobook request (book_id=%s): %s", book_id, e)

    request_query = msgs.get(
        "audiobook_missing_request_query",
        "🎧 Audiobook request: {title} [book_id: {book_id}]",
    ).format(title=book_title, book_id=book_id)

    try:
        send_request = globals().get("send_request_to_admin")
        if callable(send_request) and update.effective_user:
            await send_request(context, update.effective_user, request_query, lang)
    except Exception as e:
        logger.warning("Failed to send audiobook request to admin group (book_id=%s): %s", book_id, e)

    try:
        await query.message.reply_text(
            msgs.get(
                "audiobook_missing_reply",
                "🎧 This book has no audiobook yet.\n⏳ We will add it within 1 hour.\n🔔 Wait for our message.",
            )
        )
    except Exception:
        pass

    await safe_answer(query)


async def handle_audiobook_listen_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user request to listen to an audiobook (show audio parts)."""
    logger.debug("handle_audiobook_listen_callback called")
    query = update.callback_query
    if not query:
        logger.debug("handle_audiobook_listen_callback: no query")
        return
    lang = ensure_user_language(update, context)
    data = query.data or ""
    logger.debug("audiobook callback data=%s", data)
    if not data.startswith("abook:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]

    # Get the audiobook for this book
    audio_book = await run_blocking(get_audio_book_for_book, book_id)
    if not audio_book:
        await _handle_missing_audiobook_request(update, context, query, lang, book_id)
        return

    # Get all audio parts
    all_parts = await run_blocking(list_audio_book_parts, audio_book.get("id"))
    if not all_parts:
        await _handle_missing_audiobook_request(update, context, query, lang, book_id)
        return

    # Pagination: show only first 10 parts initially
    parts_per_page = 10
    parts = all_parts[:parts_per_page]

    # Build keyboard with audio parts (3 per row)
    keyboard = []
    row = []
    for i, part in enumerate(parts):
        part_index = part.get("part_index", 0)

        # Multilingual button text based on user language
        if lang == "uz":
            button_text = f"🎵 {part_index}-qism"
        elif lang == "ru":
            button_text = f"🎵 Часть {part_index}"
        else:  # English default
            button_text = f"🎵 Part {part_index}"

        callback_data = f"abplay:{part.get('id')}"
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))

        # Add row when we have 3 buttons or it's the last part
        if len(row) == 3 or i == len(parts) - 1:
            keyboard.append(row)
            row = []

    # Add pagination if more than 10 parts
    if len(all_parts) > 10:
        # Navigation row with prev/next buttons and page info
        total_pages = (len(all_parts) + parts_per_page - 1) // parts_per_page
        nav_row = [
            InlineKeyboardButton("⬅️ Prev", callback_data=f"abpage:{book_id}:prev"),
            InlineKeyboardButton(f"1/{total_pages}", callback_data="noop"),
            InlineKeyboardButton("➡️ Next", callback_data=f"abpage:{book_id}:next")
        ]
        keyboard.append(nav_row)

    text = MESSAGES[lang].get("audiobook_listen_title", f"🎧 Audiobook: {len(all_parts)} parts")

    try:
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.debug("Failed to send audiobook parts message: %s", e)
        await safe_answer(query)
        return

    await safe_answer(query)


async def handle_audiobook_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle pagination for audiobook parts."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    data = query.data or ""
    if not data.startswith("abpage:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    
    try:
        _, book_id, direction = data.split(":")
    except ValueError:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    
    # Get current page from user data or default to 0
    current_page = context.user_data.get(f"abook_page_{book_id}", 0)
    
    # Calculate new page
    if direction == "next":
        current_page += 1
    elif direction == "prev":
        current_page = max(0, current_page - 1)
    
    # Store new page
    context.user_data[f"abook_page_{book_id}"] = current_page
    
    # Get audiobook and parts
    audio_book = await run_blocking(get_audio_book_for_book, book_id)
    if not audio_book:
        await safe_answer(query, MESSAGES[lang].get("audiobook_not_found", "Audiobook not found"), show_alert=True)
        return
    
    all_parts = await run_blocking(list_audio_book_parts, audio_book.get("id"))
    if not all_parts:
        await safe_answer(query, MESSAGES[lang].get("audiobook_no_parts", "No audio parts found"), show_alert=True)
        return
    
    # Pagination: 10 parts per page
    parts_per_page = 10
    start_idx = current_page * parts_per_page
    end_idx = start_idx + parts_per_page
    parts = all_parts[start_idx:end_idx]
    
    # Build keyboard with pagination
    keyboard = []
    row = []
    for i, part in enumerate(parts):
        part_index = part.get("part_index", 0)
        
        # Multilingual button text based on user language
        if lang == "uz":
            button_text = f"🎵 {part_index}-qism"
        elif lang == "ru":
            button_text = f"🎵 Часть {part_index}"
        else:  # English default
            button_text = f"🎵 Part {part_index}"
            
        callback_data = f"abplay:{part.get('id')}"
        row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        # Add row when we have 3 buttons or it's the last part on this page
        if len(row) == 3 or i == len(parts) - 1:
            keyboard.append(row)
            row = []
    
    # Add pagination navigation
    total_pages = (len(all_parts) + parts_per_page - 1) // parts_per_page
    nav_row = []
    
    # Prev button (disabled if on first page)
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"abpage:{book_id}:prev"))
    else:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data="noop"))  # Disabled
    
    # Page info
    page_info = f"{current_page + 1}/{total_pages}"
    nav_row.append(InlineKeyboardButton(page_info, callback_data="noop"))
    
    # Next button (disabled if on last page)
    if current_page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"abpage:{book_id}:next"))
    else:
        nav_row.append(InlineKeyboardButton("➡️ Next", callback_data="noop"))  # Disabled
    
    keyboard.append(nav_row)
    
    text = MESSAGES[lang].get("audiobook_listen_title", f"🎧 Audiobook: {len(all_parts)} parts (Page {current_page + 1})")
    
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    await safe_answer(query)


async def handle_audiobook_part_play_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle playing an individual audiobook part."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    data = query.data or ""
    if not data.startswith("abplay:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    part_id = data.split(":", 1)[1]
    
    # Get the audio part
    part = await run_blocking(get_audio_book_part, part_id)
    if not part:
        await safe_answer(query, MESSAGES[lang].get("audio_part_not_found", "Audio part not found"), show_alert=True)
        return
    
    # Send the audio file with delete button for admins
    try:
        # Create keyboard with delete button for admins
        keyboard = []
        if _is_admin_user(query.from_user.id):
            delete_button = InlineKeyboardButton("🗑️ Delete Audio", callback_data=f"apdel:{part_id}")
            keyboard.append([delete_button])
        
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        
        await context.bot.send_audio(
            chat_id=query.message.chat_id,
            audio=part.get("file_id"),
            title=part.get("title", f"Part {part.get('part_index', 0)}"),
            duration=part.get("duration_seconds"),
            reply_markup=reply_markup,
        )
        await safe_answer(query)
    except Exception as e:
        logger.error(f"Failed to send audiobook part {part_id}: {e}")
        await safe_answer(query, MESSAGES[lang].get("audio_send_failed", "Failed to send audio"), show_alert=True)


async def handle_audiobook_part_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a single audiobook part (admin only)."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("apdel:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    part_id = data.split(":", 1)[1]
    part = await run_blocking(get_audio_book_part, part_id)
    if not part:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    # delete the part
    await run_blocking(delete_audio_book_part, part_id)
    await safe_answer(query, MESSAGES[lang].get("audiobook_part_deleted", "✅ Part deleted."))
    try:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_part_deleted", "✅ Part deleted."))
    except Exception:
        pass


async def handle_audiobook_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete entire audiobook (admin only)."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abdel:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    audio_book_id = data.split(":", 1)[1]
    # delete the audiobook (cascades to parts)
    await run_blocking(delete_audio_book, audio_book_id)
    await safe_answer(query, MESSAGES[lang].get("audiobook_deleted", "✅ Audiobook deleted."))
    try:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_deleted", "✅ Audiobook deleted."))
    except Exception:
        pass


async def handle_audiobook_delete_by_book_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete all audiobooks for a specific book (admin only)."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abdelbook:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    deleted = await run_blocking(delete_audio_books_by_book_id, book_id)
    if deleted > 0:
        text = MESSAGES[lang].get(
            "audiobook_delete_all_done",
            "✅ All audios for this book were deleted.",
        )
        await safe_answer(query, text)
        try:
            await query.message.reply_text(text)
        except Exception:
            pass
        return
    text = MESSAGES[lang].get(
        "audiobook_delete_all_none",
        "ℹ️ This book has no audios to delete.",
    )
    await safe_answer(query, text, show_alert=True)
    try:
        await query.message.reply_text(text)
    except Exception:
        pass


async def handle_audiobook_add_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start audiobook creation flow (admin only)."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not _is_admin_user(query.from_user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abadd:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    
    try:
        # If the book already has audio parts, require choosing an insert index.
        # Otherwise start regular sequential upload mode.
        existing_ab = await run_blocking(get_audio_book_for_book, book_id)
        if existing_ab and existing_ab.get("id"):
            audio_book_id = existing_ab["id"]
            parts = await run_blocking(list_audio_book_parts, audio_book_id)
            max_idx = 0
            try:
                max_idx = max(int(p.get("part_index") or 0) for p in (parts or []))
            except Exception:
                max_idx = 0
            if max_idx > 0:
                insert_max = max_idx + 1
                context.user_data["pending_abook"] = {
                    "audio_book_id": audio_book_id,
                    "book_id": book_id,
                    "next_part_index": insert_max,
                    "awaiting_insert_index": True,
                    "insert_max": insert_max,
                }
                await safe_answer(query)
                try:
                    await query.message.reply_text(
                        MESSAGES[lang]["audiobook_exists_info"].format(
                            count=len(parts or []),
                            max=insert_max,
                        )
                    )
                except Exception:
                    pass
                return
            next_part_index = 1
        else:
            audio_book_id = await run_blocking(create_audio_book_for_book, book_id=book_id)
            next_part_index = 1
        context.user_data["pending_abook"] = {
            "audio_book_id": audio_book_id,
            "book_id": book_id,
            "next_part_index": next_part_index,
        }
        await safe_answer(query)
        try:
            await query.message.reply_text(MESSAGES[lang]["audiobook_add_intro"])
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Failed to create audiobook for book {book_id}: {e}")
        await safe_answer(query)
        try:
            await query.message.reply_text("❌ Failed to create audiobook. Please try again.")
        except Exception:
            pass


async def search_books(update: Update, context: ContextTypes.DEFAULT_TYPE):
    progress_message = None
    try:
        lang = ensure_user_language(update, context)

        if not update.message or not update.message.text:
            return
        if _mark_and_check_duplicate_text_update(context, update):
            return

        user_id = update.effective_user.id
        if is_blocked(user_id):
            await update.message.reply_text(MESSAGES[lang]["blocked"])
            return
        if await is_stopped_user(user_id):
            return
        group_read_activity_fn = globals().get("_group_read_handle_message_activity")
        if callable(group_read_activity_fn):
            try:
                if await group_read_activity_fn(update, context, lang):
                    return
            except Exception:
                pass

        menu_action = _main_menu_text_action(update.message.text.strip())
        if menu_action:
            await _cancel_menu_conflicting_flows(update, context, lang)
            handled = await _handle_main_menu_action(update, context, lang, menu_action)
            if handled:
                return

        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            return

        # User activity persistence should not delay visible UX paths.
        try:
            scheduler = globals().get("_schedule_application_task")
            if callable(scheduler):
                scheduler(context.application, update_user_info(update, context))
            else:
                context.application.create_task(update_user_info(update, context))
        except Exception:
            pass

        if await _tts_handle_text_input(update, context, lang):
            return

        if await _pdf_maker_handle_text_input(update, context, lang):
            return

        pending_bonus = context.user_data.get("awaiting_user_bonus")
        if pending_bonus and update.effective_user.id in {ADMIN_ID, OWNER_ID}:
            if time.time() > pending_bonus.get("expires_at", 0):
                context.user_data.pop("awaiting_user_bonus", None)
            else:
                admin_text = update.message.text.strip()
                if admin_text.lower() in {"cancel", "stop"}:
                    context.user_data.pop("awaiting_user_bonus", None)
                    await update.message.reply_text(MESSAGES[lang]["user_bonus_cancelled"])
                    return
                if not admin_text.lstrip("+-").isdigit():
                    await update.message.reply_text(MESSAGES[lang]["user_bonus_invalid"])
                    return
                amount = int(admin_text)
                if amount <= 0:
                    await update.message.reply_text(MESSAGES[lang]["user_bonus_invalid"])
                    return
                target_id = int(pending_bonus.get("user_id"))
                mode = pending_bonus.get("mode")
                delta = amount if mode == "add" else -amount
                new_bonus = await run_blocking(db_add_user_coin_adjustment, target_id, delta)
                context.user_data.pop("awaiting_user_bonus", None)

                target = await run_blocking(get_user, target_id)
                target_name = format_user_name(target) if target else f"User {target_id}"
                if mode == "add":
                    await update.message.reply_text(
                        MESSAGES[lang]["user_bonus_added"].format(name=target_name, amount=amount, bonus=new_bonus)
                    )
                    try:
                        admin_tag = format_user_tag(update.effective_user)
                        user_lang = (target or {}).get("language") or "en"
                        notice = MESSAGES[user_lang]["bonus_received_notice"].format(
                            amount=amount,
                            admin=admin_tag,
                        )
                        await context.bot.send_message(chat_id=target_id, text=notice)
                    except Exception as e:
                        logger.error(f"Failed to notify user {target_id} bonus: {e}")
                else:
                    await update.message.reply_text(
                        MESSAGES[lang]["user_bonus_removed"].format(name=target_name, amount=amount, bonus=new_bonus)
                    )
                return

        # Admin reply to request status
        pending = context.user_data.get("pending_request_reply")
        if pending and update.effective_user.id in {ADMIN_ID, OWNER_ID}:
            if time.time() > pending.get("expires_at", 0):
                context.user_data.pop("pending_request_reply", None)
            else:
                admin_text = update.message.text.strip()
                if admin_text.lower() in {"cancel", "stop"}:
                    context.user_data.pop("pending_request_reply", None)
                    await update.message.reply_text(MESSAGES[lang]["request_admin_cancelled"])
                    return

                record = await run_blocking(
                    update_request_status,
                    pending["request_id"],
                    pending["status"],
                    update.effective_user,
                    admin_text,
                )
                if not record:
                    await update.message.reply_text(MESSAGES[lang]["page_expired"])
                    context.user_data.pop("pending_request_reply", None)
                    return

                # Notify requester in their language
                req_lang = record.get("language", "en")
                base = MESSAGES[req_lang].get(f"request_reply_{record.get('status')}", "")
                msg = base.format(query=record.get("query"))
                if admin_text:
                    msg += "\n\n" + MESSAGES[req_lang]["request_reply_note"].format(note=admin_text)
                await context.bot.send_message(chat_id=record["user_id"], text=msg)

                # Update admin message if possible
                try:
                    keyboard = build_request_admin_keyboard(record.get("status", "open"), record.get("id"))
                    await context.bot.edit_message_text(
                        chat_id=pending["admin_chat_id"],
                        message_id=pending["admin_message_id"],
                        text=format_request_admin_text(record),
                        reply_markup=keyboard
                    )
                except Exception:
                    pass

                context.user_data.pop("pending_request_reply", None)
                await update.message.reply_text(MESSAGES[lang]["request_status_updated_admin"].format(status=record.get("status")))
                return

        # Admin reply to upload access request
        pending_upload = context.user_data.get("pending_upload_reply")
        if pending_upload and update.effective_user.id in {ADMIN_ID, OWNER_ID}:
            if time.time() > pending_upload.get("expires_at", 0):
                context.user_data.pop("pending_upload_reply", None)
            else:
                admin_text = update.message.text.strip()
                if admin_text.lower() in {"cancel", "stop"}:
                    context.user_data.pop("pending_upload_reply", None)
                    await update.message.reply_text(MESSAGES[lang]["upload_admin_cancelled"])
                    return

                record = await run_blocking(
                    update_upload_request_status,
                    pending_upload["request_id"],
                    pending_upload["status"],
                    update.effective_user,
                    admin_text,
                )
                if not record:
                    await update.message.reply_text(MESSAGES[lang]["page_expired"])
                    context.user_data.pop("pending_upload_reply", None)
                    return

                # If accepted, allow user to upload
                if record.get("status") == "accept":
                    if record.get("user_id"):
                        await run_blocking(set_user_allowed, record.get("user_id"), True)

                # Notify user
                req_lang = record.get("language", "en")
                base = MESSAGES[req_lang].get(f"upload_reply_{record.get('status')}", "")
                msg = base
                if admin_text:
                    msg += "\n\n" + MESSAGES[req_lang]["upload_reply_note"].format(note=admin_text)
                await context.bot.send_message(chat_id=record["user_id"], text=msg)

                # Update admin message
                try:
                    keyboard = build_upload_admin_keyboard(record.get("status", "open"), record.get("id"))
                    await context.bot.edit_message_text(
                        chat_id=pending_upload["admin_chat_id"],
                        message_id=pending_upload["admin_message_id"],
                        text=format_upload_request_admin_text(record),
                        reply_markup=keyboard
                    )
                except Exception:
                    pass

                context.user_data.pop("pending_upload_reply", None)
                await update.message.reply_text(MESSAGES[lang]["upload_status_updated_admin"].format(status=record.get("status")))
                return

        # Admin adding audiobook parts
        pending_abook = context.user_data.get("pending_abook")
        if pending_abook and update.effective_user.id in {ADMIN_ID, OWNER_ID}:
            # allow cancelling/finishing via text
            if update.message.text:
                t = update.message.text.strip().lower()
                if t in {"cancel", "stop", "/cancel"}:
                    context.user_data.pop("pending_abook", None)
                    await update.message.reply_text(MESSAGES[lang]["audiobook_add_cancel"])
                    return
                if t in {"done", "finish", "✅"}:
                    # complete flow
                    audio_id = pending_abook.get("audio_book_id")
                    parts = await run_blocking(list_audio_book_parts, audio_id)
                    await update.message.reply_text(MESSAGES[lang]["audiobook_add_done"].format(count=len(parts)))
                    context.user_data.pop("pending_abook", None)
                    return
                # Handle insert position selection
                if pending_abook.get("awaiting_insert_index"):
                    insert_max = pending_abook.get("insert_max", 1)
                    raw = update.message.text.strip()
                    if raw.isdigit():
                        pos = int(raw)
                        if 1 <= pos <= insert_max:
                            pending_abook["next_part_index"] = pos
                            pending_abook["awaiting_insert_index"] = False
                            context.user_data["pending_abook"] = pending_abook
                            await update.message.reply_text(
                                MESSAGES[lang]["audiobook_insert_position_set"].format(index=pos)
                            )
                            return
                    await update.message.reply_text(
                        MESSAGES[lang]["audiobook_insert_invalid"].format(max=insert_max)
                    )
                    return
            # If in audiobook flow and not a command, don't process as search
            # This prevents interfering with normal search when audiobook mode is active
            return


        if await _admin_tools_handle_admin_menu_prompt_input(
            update=update,
            context=context,
            lang=lang,
            messages=MESSAGES,
            is_admin_user_fn=_is_admin_user,
            main_menu_keyboard_fn=_main_menu_keyboard,
            broadcast_fn=broadcast,
            user_search_command_fn=user_search_command,
        ):
            return

        if await _ai_tool_mode_handle_text_input(update, context, lang):
            return

        if await _ai_image_handle_text_input(update, context, lang):
            return

        if await _video_dl_handle_text_input(update, context, lang):
            return

        if await _ai_chat_handle_text_input(update, context, lang):
            return

        # Simple thanks replies
        thanks_lang = _detect_thanks_reply_lang(update.message.text)
        if thanks_lang:
            await _send_heart_reaction_for_message(update, context)
            reply_lang = thanks_lang if thanks_lang in MESSAGES else lang
            await safe_reply(update, MESSAGES[reply_lang]["thanks_reply"])
            return

        if context.user_data.get("awaiting_request"):
            expires_at = context.user_data.get("awaiting_request_until", 0)
            if expires_at and time.time() > expires_at:
                context.user_data["awaiting_request"] = False
                context.user_data.pop("awaiting_request_until", None)
            else:
                request_text = update.message.text.strip()
                if not request_text:
                    await update.message.reply_text(MESSAGES[lang]["request_prompt"])
                    return
                menu_action = _main_menu_text_action(request_text)
                if menu_action:
                    context.user_data["awaiting_request"] = False
                    context.user_data.pop("awaiting_request_until", None)
                    handled = await _handle_main_menu_action(update, context, lang, menu_action)
                    if handled:
                        return
                await send_request_to_admin(context, update.effective_user, request_text, lang)
                context.user_data["awaiting_request"] = False
                context.user_data.pop("awaiting_request_until", None)
                await update.message.reply_text(MESSAGES[lang]["request_sent"])
                return

        if context.user_data.get("awaiting_movie_search"):
            movie_query = (update.message.text or "").strip()
            if not movie_query:
                await update.message.reply_text(
                    MESSAGES[lang].get("menu_movie_search_prompt", "Send a movie name to search.")
                )
                return
            menu_action = _main_menu_text_action(movie_query)
            if menu_action:
                context.user_data["awaiting_movie_search"] = False
                handled = await _handle_main_menu_action(update, context, lang, menu_action)
                if handled:
                    return

            await _send_salute_reaction_for_message(update, context)
            try:
                progress_message = await update.message.reply_text(
                    MESSAGES[lang].get("processing_movie_search", "🎬 Searching movies... please wait.")
                )
            except Exception:
                progress_message = None

            async def _record_movie_search_analytics() -> None:
                try:
                    await _run_db_retry(increment_analytics, "searches", 1)
                    await _run_db_retry(increment_user_analytics, user_id, "searches", 1)
                    await _run_db_retry(db_increment_counter, "search_total", 1)
                    await _run_db_retry(db_increment_counter, "movie_search_total", 1)
                except Exception as e:
                    logger.warning("movie search analytics update failed: %s", e)

            _schedule_bg_task(context, _record_movie_search_analytics())

            entries: list[dict[str, str]] = []

            cleaned_movie_query = normalize(movie_query).lower()
            translit_movie_query = transliterate_to_latin(cleaned_movie_query) if cleaned_movie_query else ""

            # Prefer ES movie index for better search quality; fallback to DB when needed.
            if cleaned_movie_query and es_available():
                es_results = await run_blocking(search_movies_es, cleaned_movie_query, MAX_SEARCH_RESULTS)
                if translit_movie_query and translit_movie_query != cleaned_movie_query:
                    es_results += await run_blocking(search_movies_es, translit_movie_query, MAX_SEARCH_RESULTS)

                unique_matches: dict[str, dict[str, Any]] = {}
                for movie, score, es_id in es_results:
                    row = movie or {}
                    mid = str(row.get("id") or es_id or "").strip()
                    if not mid:
                        continue
                    title_base = str(row.get("display_name") or row.get("movie_name") or f"Movie {len(unique_matches) + 1}")
                    year_val = row.get("release_year")
                    title = f"{title_base} ({year_val})" if year_val and str(year_val) not in title_base else title_base
                    prev = unique_matches.get(mid)
                    try:
                        current_score = float(score or 0.0)
                    except Exception:
                        current_score = 0.0
                    if not prev or current_score > float(prev.get("score", 0.0) or 0.0):
                        unique_matches[mid] = {"id": mid, "title": title, "score": current_score}
                entries = [
                    {"id": v["id"], "title": v["title"]}
                    for v in sorted(unique_matches.values(), key=lambda e: e.get("score", 0.0), reverse=True)
                ]

            if not entries:
                movies = await run_blocking(db_search_movies, movie_query, MAX_SEARCH_RESULTS)
                for idx, movie in enumerate(movies, start=1):
                    mid = str(movie.get("id") or "").strip()
                    if mid:
                        title_base = str(movie.get("display_name") or movie.get("movie_name") or f"Movie {idx}")
                        year_val = movie.get("release_year")
                        title = f"{title_base} ({year_val})" if year_val and str(year_val) not in title_base else title_base
                        entries.append({
                            "id": mid,
                            "title": title,
                        })
            entries = entries[:MAX_SEARCH_RESULTS]

            if not entries:
                await _edit_progress_or_reply(
                    progress_message,
                    update.message,
                    MESSAGES[lang].get("movie_not_found", MESSAGES[lang].get("not_found", "Not found.")),
                )
                return

            query_id = cache_movie_search_results(context, movie_query, entries)
            result_text, page_entries, pages = build_movie_results_text(movie_query, entries, 0, lang)
            reply_markup = build_movie_results_keyboard(page_entries, 0, pages, query_id)
            await _edit_progress_or_reply(
                progress_message,
                update.message,
                result_text,
                reply_markup=reply_markup,
            )
            return

        chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
        is_group_chat = chat_type in {"group", "supergroup"}
        reply_msg = getattr(update.message, "reply_to_message", None)
        bot_id = getattr(context.bot, "id", None)
        is_reply_to_bot = bool(
            is_group_chat
            and reply_msg
            and getattr(reply_msg, "from_user", None)
            and bot_id
            and int(getattr(reply_msg.from_user, "id", 0) or 0) == int(bot_id)
            and (update.message.text or "").strip()
        )
        # In groups, answer book searches only when user replies directly to the bot.
        if is_group_chat and not is_reply_to_bot:
            return

        is_reply_search_in_group = is_reply_to_bot
        active_menu_section = str(context.user_data.get("main_menu_section") or "").strip().lower()
        has_active_menu_context = active_menu_section in {"main", "other", "ai_tools", "admin"}
        # Allow direct search on main menu (for old users used to typing book names)
        # and when no menu context is active (e.g. before/without opening menus).
        # Keep explicit Search Books requirement in submenus/admin sections to avoid confusion.
        require_search_button = active_menu_section in {"other", "ai_tools", "admin"}

        if require_search_button and not bool(context.user_data.get("awaiting_book_search")) and not is_reply_search_in_group:
            if is_group_chat:
                # In groups, avoid noisy prompts on normal messages; search is allowed on replies.
                return
            await _reply_search_menu_click_hint(update, context, lang)
            return

        limited, wait_s = rate_limited(context, "last_search_ts", SEARCH_COOLDOWN_SEC)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["rate_limited"].format(seconds=wait_s))
            return
        query = update.message.text.strip()
        if not query:
            await update.message.reply_text(MESSAGES[lang]["enter_specific"])
            return
        await _send_salute_reaction_for_message(update, context)
        try:
            progress_message = await update.message.reply_text(
                MESSAGES[lang].get("processing_search", "🔎 Searching... Please wait.")
            )
        except Exception:
            progress_message = None

        async def _record_search_analytics() -> None:
            try:
                await _run_db_retry(increment_analytics, "searches", 1)
                await _run_db_retry(increment_user_analytics, user_id, "searches", 1)
                await _run_db_retry(db_increment_counter, "search_total", 1)
            except Exception as e:
                logger.warning("search analytics update failed: %s", e)

        _schedule_bg_task(context, _record_search_analytics())

        cleaned_query = normalize(query).lower()
        if not cleaned_query:
            await update.message.reply_text(MESSAGES[lang]["enter_specific"])
            return
        translit_query = transliterate_to_latin(cleaned_query)

        results = []

        # --- Search in ES if available ---
        if es_available():
            results += await run_blocking(search_es, cleaned_query)  # raw Cyrillic or Latin
            if translit_query != cleaned_query:
                results += await run_blocking(search_es, translit_query)  # transliterated Latin
        else:
            books = await run_blocking(load_books)
            # --- Fallback: local substring search ---
            results += [(b, 1.0, b.get("id")) for b in books if cleaned_query in b["book_name"].lower()]
            if translit_query != cleaned_query:
                results += [(b, 1.0, b.get("id")) for b in books if translit_query in b["book_name"].lower()]

        # ✅ Deduplicate by UUID and build entries
        unique_matches = {}
        for book, score, es_id in results:
            book_id = str(book.get("id") or es_id).strip() if book else None
            if not book_id:
                continue
            title = get_result_title(book)
            if book_id not in unique_matches or score > unique_matches[book_id]["score"]:
                unique_matches[book_id] = {"id": book_id, "title": title, "score": score}

        entries = sorted(unique_matches.values(), key=lambda e: e["score"], reverse=True)
        entries = entries[:MAX_SEARCH_RESULTS]

        if not entries:
            if es_available():
                books = await run_blocking(load_books)
            suggestions = suggest_books(books, cleaned_query, limit=5)
            if suggestions:
                suggestion_lines = [f"{i + 1}. {s['title']}" for i, s in enumerate(suggestions)]
                text = MESSAGES[lang]["suggestions"] + "\n\n" + "\n".join(suggestion_lines)
                keyboard = []
                row = []
                for idx, s in enumerate(suggestions, start=1):
                    row.append(InlineKeyboardButton(str(idx), callback_data=f"book:{s['id']}"))
                    if idx % 5 == 0:
                        keyboard.append(row)
                        row = []
                if row:
                    keyboard.append(row)
                req_id = cache_request(context, query, update.effective_user)
                keyboard.append([InlineKeyboardButton(MESSAGES[lang]["request_button"], callback_data=f"request:{req_id}")])
                await _edit_progress_or_reply(
                    progress_message,
                    update.message,
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            else:
                req_id = cache_request(context, query, update.effective_user)
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(MESSAGES[lang]["request_button"], callback_data=f"request:{req_id}")]])
                await _edit_progress_or_reply(
                    progress_message,
                    update.message,
                    MESSAGES[lang]["not_found"],
                    reply_markup=keyboard,
                )
            return

        # Cache results for paging
        query_id = cache_search_results(context, query, entries)

        # Build page 0
        result_text, page_entries, pages = build_results_text(query, entries, 0, lang)
        reply_markup = build_results_keyboard(page_entries, 0, pages, query_id)

        # Count searches for books shown on this page
        page_ids = [e.get("id") for e in page_entries if e.get("id")]
        if page_ids:
            _schedule_bg_task(context, _run_db_retry(db_increment_book_searches, page_ids))
            # also bump audiobook search counters if any
            try:
                audio_ids = await run_blocking(
                    lambda: [a.get("id") for a in [get_audio_book_for_book(bid) for bid in page_ids] if a and a.get("id")]
                )
                if audio_ids:
                    _schedule_bg_task(context, _run_db_retry(increment_audio_book_searches, audio_ids))
            except Exception:
                pass

        await _edit_progress_or_reply(
            progress_message,
            update.message,
            result_text,
            reply_markup=reply_markup,
        )

    except Exception as e:
        logger.error(f"search_books failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        if update.message:
            await _edit_progress_or_reply(progress_message, update.message, MESSAGES[lang]["error"])

        raise



# --- Callback handler for numeric selection ---
from telegram.ext import ContextTypes


import os, uuid
from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from db import (
    get_audio_book_for_book,
    create_audio_book_for_book,
    insert_audio_book_part,
    list_audio_book_parts,
    get_audio_book_part,
    get_audio_book_part_by_file_unique_id_and_audio_book,
    delete_audio_book_part,
    delete_audio_book,
    delete_audio_books_by_book_id,
    increment_audio_book_download,
    increment_audio_book_searches,
    shift_audio_book_parts_from,
)

async def handle_book_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        if update.effective_user and await is_stopped_user(update.effective_user.id):
            if query:
                await safe_answer(query)
            return
        _schedule_bg_task(context, update_user_info(update, context))
        lang = ensure_user_language(update, context)
        limited, wait_s = spam_check_callback(update, context)
        if limited:
            await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
            return
        data = str(query.data).strip()
        if data.startswith("book:"):
            book_id = data.split(":", 1)[1].strip()
        else:
            book_id = data  # backward compatibility for old buttons

        # --- Lookup book by UUID in DB ---
        book = await run_blocking(db_get_book_by_id, book_id)

        # --- Fallback: fetch from ES if not found locally ---
        if not book and es_available():
            try:
                es = get_es()
                if not es:
                    raise RuntimeError("Elasticsearch client not available")
                res = await run_blocking(lambda: es.get(index="books", id=book_id))
                if res and res.get("_source"):
                    book = res["_source"]
                    book["id"] = book_id
                    await run_blocking(db_insert_book, book)
                    logger.debug(f"Fetched and restored book {book_id} from ES fallback")
            except Exception as e:
                logger.error(f"ES lookup failed for {book_id}: {e}")

        # --- If still not found ---
        if not book:
            logger.debug(f"Book not found for UUID {book_id}")
            await safe_answer(query, MESSAGES[lang]["book_not_found"])
            return

        local_path = book.get("path")
        file_id = book.get("file_id")

        status_msg = None
        try:
            await context.bot.send_chat_action(chat_id=query.message.chat_id, action="upload_document")
            status_msg = await query.message.reply_text(MESSAGES[lang]["sending"])
        except Exception:
            pass

        # --- Helper: update file_id and reindex ---
        async def update_file_id(new_file_id: str, new_file_unique_id: str | None):
            book["file_id"] = new_file_id
            book["indexed"] = True
            await _run_db_retry(update_book_file_id, book_id, new_file_id, True, new_file_unique_id)
            if es_available():
                await run_blocking(
                    index_book,
                    book["book_name"],
                    new_file_id,
                    local_path,
                    book_id,
                    get_display_name(book),
                    new_file_unique_id,
                )

        error_key = None
        sent_ok = False
        sent = None
        stats = await run_blocking(db_get_book_stats, book_id)
        downloads = stats.get("downloads", 0)
        fav_count = stats.get("fav_count", 0)
        counts = {
            "like": stats.get("like", 0),
            "dislike": stats.get("dislike", 0),
            "berry": stats.get("berry", 0),
            "whale": stats.get("whale", 0),
        }
        caption = build_book_caption(book, downloads, fav_count, counts)
        is_fav_now = await run_blocking(is_favorited, query.from_user.id, book_id)
        user_reaction = await run_blocking(db_get_user_reaction, book_id, query.from_user.id)
        can_delete = await _can_show_delete_button(update, query.from_user.id)
        # Audiobook flags: show listen if audiobook exists; allow add for admins
        audio_book = await run_blocking(get_audio_book_for_book, book_id)
        has_ab = bool(audio_book)
        can_add_ab = bool(_is_admin_user(query.from_user.id)) if callable(globals().get("_is_admin_user")) else False
        is_owner_user = bool(_is_owner_user(query.from_user.id)) if callable(globals().get("_is_owner_user")) else False
        show_listen_btn = has_ab if is_owner_user else True
        ab_request_count = 0
        if can_add_ab and is_owner_user and callable(globals().get("count_pending_audiobook_requests")):
            try:
                ab_request_count = await run_blocking(count_pending_audiobook_requests, book_id)
            except Exception:
                ab_request_count = 0
        reactions_kb = build_book_keyboard(
            book_id,
            counts,
            is_fav_now,
            user_reaction,
            can_delete,
            lang,
            has_audiobook=has_ab,
            can_add_audiobook=can_add_ab,
            show_listen_button=show_listen_btn,
            audiobook_request_count=ab_request_count,
        )

        # --- Case 1: File ID available (prefer cache) ---
        if file_id:
            try:
                sent = await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_id,
                    caption=caption,
                    reply_markup=reactions_kb
                )
                if sent and sent.document:
                    await update_file_id(sent.document.file_id, getattr(sent.document, "file_unique_id", None))
                    sent_ok = True
            except Exception as e:
                logger.error(f"Failed to send by file_id {file_id}: {e}")
                # Fallback: try local path if available
                if local_path and os.path.exists(local_path):
                    try:
                        with open(local_path, "rb") as f:
                            sent = await context.bot.send_document(
                                chat_id=query.message.chat_id,
                                document=InputFile(f, filename=_book_filename(book)),
                                caption=caption,
                                reply_markup=reactions_kb
                            )
                        if sent and sent.document:
                            await update_file_id(sent.document.file_id, getattr(sent.document, "file_unique_id", None))
                            sent_ok = True
                    except Exception as e:
                        logger.error(f"Fallback local send failed for {local_path}: {e}")
                        error_key = "book_unavailable_send_failed"
                else:
                    error_key = "book_unavailable_send_failed"

        # --- Case 2: Local path available (no file_id) ---
        elif local_path and os.path.exists(local_path):
            for attempt in range(1, LOCAL_SEND_RETRIES + 1):
                try:
                    with open(local_path, "rb") as f:
                        sent = await context.bot.send_document(
                            chat_id=query.message.chat_id,
                            document=InputFile(f, filename=_book_filename(book)),
                            caption=caption,
                            reply_markup=reactions_kb
                        )
                    if sent and sent.document:
                        await update_file_id(sent.document.file_id, getattr(sent.document, "file_unique_id", None))
                        sent_ok = True
                        break
                except Exception as e:
                    logger.error(f"Failed to send local file {local_path} (attempt {attempt}): {e}")
                    if attempt < LOCAL_SEND_RETRIES:
                        await asyncio.sleep(LOCAL_SEND_BACKOFF_SEC * attempt)
                    else:
                        error_key = "book_unavailable_send_failed"

        # --- Case 3: No usable path or file_id ---
        else:
            if local_path and not os.path.exists(local_path):
                error_key = "book_unavailable_file_missing"
            else:
                error_key = "book_unavailable_no_file"

        if not sent_ok and error_key:
            message = MESSAGES[lang].get(error_key, MESSAGES[lang]["book_unavailable"])
            if status_msg:
                try:
                    await status_msg.edit_text(message)
                except Exception:
                    await query.message.reply_text(message)
            else:
                await query.message.reply_text(message)

        if sent_ok:
            # ✅ Increment statistics counter for button downloads
            async def _record_download_analytics() -> None:
                try:
                    await _run_db_retry(increment_analytics, "buttons", 1)
                    await _run_db_retry(increment_user_analytics, query.from_user.id, "buttons", 1)
                    await _run_db_retry(db_increment_counter, "download_total", 1)
                except Exception as e:
                    logger.warning("download analytics update failed: %s", e)

            _schedule_bg_task(context, _record_download_analytics())
            logger.debug(f"Button download logged for {get_display_name(book)} on {_today_str()}")

            _schedule_bg_task(context, _run_db_retry(add_recent_download, query.from_user.id, book_id, get_result_title(book)))

            # Update downloads count + refresh caption/keyboard
            try:
                await _run_db_retry(db_increment_book_download, book_id)
                invalidate_top_caches(context)
                stats = await run_blocking(db_get_book_stats, book_id)
                new_downloads = stats.get("downloads", 0)
                fav_count = stats.get("fav_count", 0)
                counts = {
                    "like": stats.get("like", 0),
                    "dislike": stats.get("dislike", 0),
                    "berry": stats.get("berry", 0),
                    "whale": stats.get("whale", 0),
                }
                if sent:
                    is_fav_now = await run_blocking(is_favorited, query.from_user.id, book_id)
                    user_reaction = await run_blocking(db_get_user_reaction, book_id, query.from_user.id)
                    # Recompute audiobook flags for refreshed keyboard
                    audio_book2 = await run_blocking(get_audio_book_for_book, book_id)
                    has_ab2 = bool(audio_book2)
                    can_add_ab2 = bool(_is_admin_user(query.from_user.id)) if callable(globals().get("_is_admin_user")) else False
                    is_owner_user2 = bool(_is_owner_user(query.from_user.id)) if callable(globals().get("_is_owner_user")) else False
                    show_listen_btn2 = has_ab2 if is_owner_user2 else True
                    ab_request_count2 = 0
                    if can_add_ab2 and is_owner_user2 and callable(globals().get("count_pending_audiobook_requests")):
                        try:
                            ab_request_count2 = await run_blocking(count_pending_audiobook_requests, book_id)
                        except Exception:
                            ab_request_count2 = 0
                    await sent.edit_caption(
                        caption=build_book_caption(book, new_downloads, fav_count, counts),
                        reply_markup=build_book_keyboard(
                            book_id,
                            counts,
                            is_fav_now,
                            user_reaction,
                            can_delete,
                            lang,
                            has_audiobook=has_ab2,
                            can_add_audiobook=can_add_ab2,
                            show_listen_button=show_listen_btn2,
                            audiobook_request_count=ab_request_count2,
                        ),
                    )
            except Exception as e:
                logger.error(f"Failed to update book stats caption: {e}", exc_info=True)

        if status_msg:
            try:
                await status_msg.edit_text(MESSAGES[lang]["sent"])
            except Exception:
                pass

        await safe_answer(query)

    except Exception as e:
        logger.error(f"handle_book_selection failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        await update.callback_query.message.reply_text(MESSAGES[lang]["error"])
        raise


async def handle_movie_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
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
        data = str(query.data or "").strip()
        if not data.startswith("movie:"):
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        movie_id = data.split(":", 1)[1].strip()
        movie = await run_blocking(db_get_movie_by_id, movie_id)
        if not movie:
            await safe_answer(query, MESSAGES[lang].get("movie_not_found", MESSAGES[lang]["book_not_found"]))
            return

        status_msg = None
        try:
            await context.bot.send_chat_action(chat_id=query.message.chat_id, action="upload_video")
            status_msg = await query.message.reply_text(MESSAGES[lang].get("sending", "⏳ Sending..."))
        except Exception:
            pass

        file_id = str(movie.get("file_id") or "").strip()
        local_path = movie.get("path")
        title = str(movie.get("display_name") or movie.get("movie_name") or "Movie")
        sent_ok = False
        if file_id:
            try:
                await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_id,
                    caption=title[:1024],
                )
                sent_ok = True
            except Exception:
                try:
                    await context.bot.send_video(
                        chat_id=query.message.chat_id,
                        video=file_id,
                        caption=title[:1024],
                    )
                    sent_ok = True
                except Exception:
                    sent_ok = False

        if (not sent_ok) and local_path and os.path.exists(local_path):
            try:
                with open(local_path, "rb") as f:
                    await context.bot.send_document(
                        chat_id=query.message.chat_id,
                        document=InputFile(f, filename=os.path.basename(local_path)),
                        caption=title[:1024],
                    )
                sent_ok = True
            except Exception:
                sent_ok = False

        if not sent_ok:
            message = MESSAGES[lang].get("movie_unavailable", MESSAGES[lang]["book_unavailable"])
            if status_msg:
                try:
                    await status_msg.edit_text(message)
                except Exception:
                    await query.message.reply_text(message)
            else:
                await query.message.reply_text(message)
        else:
            async def _record_movie_download_analytics() -> None:
                try:
                    await _run_db_retry(increment_analytics, "buttons", 1)
                    await _run_db_retry(increment_user_analytics, query.from_user.id, "buttons", 1)
                    await _run_db_retry(db_increment_counter, "download_total", 1)
                    await _run_db_retry(db_increment_counter, "movie_download_total", 1)
                except Exception as e:
                    logger.warning("movie download analytics update failed: %s", e)

            _schedule_bg_task(context, _record_movie_download_analytics())
            if status_msg:
                try:
                    await status_msg.edit_text(MESSAGES[lang].get("sent", "✅ Sent."))
                except Exception:
                    pass

        await safe_answer(query)
    except Exception as e:
        logger.error(f"handle_movie_selection failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        await update.callback_query.message.reply_text(MESSAGES[lang]["error"])
        raise


async def handle_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    cache = get_search_cache(context, query_id)
    if not cache:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    entries = cache.get("results", [])
    result_text, page_entries, pages = build_results_text(cache.get("query", ""), entries, page, lang)
    reply_markup = build_results_keyboard(page_entries, page, pages, query_id)

    try:
        await query.edit_message_text(result_text, reply_markup=reply_markup)
    except Exception:
        pass
    await safe_answer(query)


async def handle_movie_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    cache = get_movie_search_cache(context, query_id)
    if not cache:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    entries = cache.get("results", [])
    result_text, page_entries, pages = build_movie_results_text(cache.get("query", ""), entries, page, lang)
    reply_markup = build_movie_results_keyboard(page_entries, page, pages, query_id)

    try:
        await query.edit_message_text(result_text, reply_markup=reply_markup)
    except Exception:
        pass
    await safe_answer(query)


async def handle_user_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
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

    cache = get_user_search_cache(context, query_id)
    if not cache:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    entries = cache.get("results", [])
    result_text, page_entries, pages = build_user_results_text(cache.get("query", ""), entries, page, lang)
    reply_markup = build_user_results_keyboard(page_entries, page, pages, query_id)

    try:
        await query.edit_message_text(result_text, reply_markup=reply_markup)
    except Exception:
        pass
    await safe_answer(query)


async def handle_user_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    user_id_str = data.split(":", 1)[1] if ":" in data else ""
    if not user_id_str.isdigit():
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    user_id = int(user_id_str)
    user = await run_blocking(get_user, user_id)
    if not user:
        await safe_answer(query, MESSAGES[lang]["user_not_found"], show_alert=True)
        return

    text = build_user_info_text(user)
    keyboard = build_user_admin_keyboard(user)
    try:
        await query.message.reply_text(text, reply_markup=keyboard)
    except Exception:
        await context.bot.send_message(chat_id=query.from_user.id, text=text, reply_markup=keyboard)
    await safe_answer(query)
