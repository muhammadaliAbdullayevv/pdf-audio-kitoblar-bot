import logging
import warnings
import os
import json
import base64
import re
import hashlib
import traceback
import tempfile
import io
import time
import math
import asyncio
import textwrap
import socket
import subprocess
import shutil
import urllib.request
import urllib.error
import fcntl
import atexit
from functools import partial
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime
from rapidfuzz import process, fuzz

from telegram import InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import InlineQueryHandler
import uuid
from telegram import InlineQueryResultCachedDocument
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update, InputFile
from telegram.ext import CallbackQueryHandler, ContextTypes

from urllib3.exceptions import InsecureRequestWarning
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    PollAnswerHandler,
    CallbackQueryHandler,
    ContextTypes,
    ApplicationHandlerStop,
    filters,
)
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4, LETTER, landscape as rl_landscape
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
except Exception:
    canvas = None
    A4 = None
    LETTER = None
    rl_landscape = None
    pdfmetrics = None
    TTFont = None
try:
    import edge_tts
except Exception:
    edge_tts = None
try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

from config import (
    TOKEN,
    OWNER_ID,
    ADMIN_ID,
    REQUEST_CHAT_ID,
    COIN_SEARCH,
    COIN_DOWNLOAD,
    COIN_REACTION,
    COIN_FAVORITE,
    COIN_REFERRAL,
    TOP_USERS_LIMIT,
    UPLOAD_CHANNEL_IDS,
    AUDIO_UPLOAD_CHANNEL_ID,
    VIDEO_UPLOAD_CHANNEL_ID,
)

from db import (
    init_db,
    get_user,
    list_users,
    upsert_user,
    update_user_language,
    update_user_left_date,
    set_user_allowed,
    set_user_delete_allowed,
    set_user_stopped,
    set_user_blocked,
    add_user_coin_adjustment as db_add_user_coin_adjustment,
    get_user_coin_adjustment as db_get_user_coin_adjustment,
    delete_users_by_ids,
    insert_removed_users,
    add_favorite as db_add_favorite,
    remove_favorite as db_remove_favorite,
    is_favorited as db_is_favorited,
    list_favorites as db_list_favorites,
    award_favorite_action as db_award_favorite_action,
    get_user_favorite_awards_count as db_get_user_favorite_awards_count,
    add_recent as db_add_recent,
    increment_analytics as db_increment_analytics,
    increment_user_analytics as db_increment_user_analytics,
    increment_counter as db_increment_counter,
    get_counters as db_get_counters,
    backfill_counters_if_empty as db_backfill_counters_if_empty,
    backfill_user_awards_if_empty as db_backfill_user_awards_if_empty,
    get_analytics_map,
    get_db_stats,
    get_book_totals as db_get_book_totals,
    get_favorites_total as db_get_favorites_total,
    get_user_favorites_count as db_get_user_favorites_count,
    get_user_usage_stats as db_get_user_usage_stats,
    get_user_reaction_count as db_get_user_reaction_count,
    get_user_referrals_count as db_get_user_referrals_count,
    get_request_status_counts as db_get_request_status_counts,
    get_upload_request_status_counts as db_get_upload_request_status_counts,
    get_user_status_counts as db_get_user_status_counts,
    get_reaction_totals as db_get_reaction_totals,
    get_user_reaction as db_get_user_reaction,
    award_reaction_action as db_award_reaction_action,
    get_user_reaction_awards_count as db_get_user_reaction_awards_count,
    get_daily_analytics as db_get_daily_analytics,
    get_user_daily_counts as db_get_user_daily_counts,
    list_books as db_list_books,
    get_book_by_id as db_get_book_by_id,
    get_book_summary as db_get_book_summary,
    get_book_by_path as db_get_book_by_path,
    get_book_by_name as db_get_book_by_name,
    get_book_by_file_unique_id as db_get_book_by_file_unique_id,
    list_movies as db_list_movies,
    list_unindexed_movies as db_list_unindexed_movies,
    get_movie_by_id as db_get_movie_by_id,
    get_movie_by_file_unique_id as db_get_movie_by_file_unique_id,
    search_movies as db_search_movies,
    find_duplicate_book as db_find_duplicate_book,
    find_duplicate_movie as db_find_duplicate_movie,
    get_duplicate_counts_file_unique_id as db_get_duplicate_counts_file_unique_id,
    get_duplicate_counts_path as db_get_duplicate_counts_path,
    get_duplicate_counts_name as db_get_duplicate_counts_name,
    get_book_storage_counts as db_get_book_storage_counts,
    get_audio_book_stats as db_get_audio_book_stats,
    get_storage_stats as db_get_storage_stats,
    increment_book_download as db_increment_book_download,
    increment_book_searches as db_increment_book_searches,
    set_book_reaction as db_set_book_reaction,
    get_book_stats as db_get_book_stats,
    get_top_books as db_get_top_books,
    get_top_users as db_get_top_users,
    insert_book as db_insert_book,
    insert_movie as db_insert_movie,
    bulk_upsert_books,
    update_book_file_id,
    update_book_indexed,
    update_movie_indexed,
    update_book_by_path,
    update_book_storage_meta as db_update_book_storage_meta,
    get_audio_book_for_book,
    get_audio_book_by_id,
    list_audio_book_parts,
    get_audio_book_part,
    get_audio_book_part_by_file_unique_id,
    get_audio_book_part_by_file_unique_id_and_audio_book,
    create_audio_book_for_book,
    insert_audio_book_part,
    delete_audio_book_part,
    delete_audio_book,
    delete_audio_books_by_book_id,
    increment_audio_book_download,
    increment_audio_book_searches,
    delete_books_by_ids,
    delete_book_and_related,
    list_requests as db_list_requests,
    list_requests_for_user as db_list_requests_for_user,
    get_request_by_id as db_get_request_by_id,
    insert_request as db_insert_request,
    update_request as db_update_request,
    delete_request as db_delete_request,
    set_request_status as db_set_request_status,
    list_upload_requests as db_list_upload_requests,
    get_upload_request_by_id as db_get_upload_request_by_id,
    insert_upload_request as db_insert_upload_request,
    update_upload_request as db_update_upload_request,
    set_upload_request_status as db_set_upload_request_status,
    insert_upload_receipt as db_insert_upload_receipt,
    update_upload_receipt as db_update_upload_receipt,
    update_book_upload_meta as db_update_book_upload_meta,
    upsert_book_summary as db_upsert_book_summary,
    save_user_quiz as db_save_user_quiz,
    get_user_quiz as db_get_user_quiz,
    list_user_quizzes as db_list_user_quizzes,
    count_user_quizzes as db_count_user_quizzes,
    delete_user_quiz as db_delete_user_quiz,
    mark_user_quiz_started as db_mark_user_quiz_started,
    increment_user_quiz_share_count as db_increment_user_quiz_share_count,
    search_users_by_name as db_search_users_by_name,
    is_user_delete_allowed as db_is_user_delete_allowed,
    is_user_stopped as db_is_user_stopped,
    set_user_referrer as db_set_user_referrer,
)
from elasticsearch import Elasticsearch, NotFoundError

from language import get_language_keyboard, MESSAGES
from menu_ui import (
    ADMIN_MENU_LABELS as _ADMIN_MENU_LABELS,
    admin_control_guide_text as _menu_ui_admin_control_guide_text,
    build_help_text as _menu_ui_build_help_text,
    main_menu_text_action as _menu_ui_main_menu_text_action,
)
from menus import (
    build_main_menu_keyboard as _menus_build_main_menu_keyboard,
    build_main_menu_message_text as _menus_build_main_menu_message_text,
    build_main_menu_chat_text as _menus_build_main_menu_chat_text,
)
from admin_tools import (
    handle_admin_menu_action as _admin_tools_handle_admin_menu_action,
    handle_admin_menu_prompt_input as _admin_tools_handle_admin_menu_prompt_input,
)

import ai_tools as _ai_tools
import video_downloader as _video_downloader
import search_flow as _search_flow
import tts_tools as _tts_tools

logger = logging.getLogger(__name__)

# Import cache for performance
try:
    from cache import cache_result, cache_get, cache_set, cache_delete, get_redis_client
except ImportError:
    logger.warning("Cache module not available")
    cache_result = lambda *args, **kwargs: lambda f: f  # No-op decorator
    cache_get = lambda k: None
    cache_set = lambda k, v, ttl=300: False
    cache_delete = lambda k: False
    get_redis_client = lambda: None
import pdf_maker as _pdf_maker_mod
import engagement_handlers as _engagement_handlers
import admin_runtime as _admin_runtime
import user_interactions as _user_interactions
import upload_flow as _upload_flow
import command_sync as _command_sync


async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


# Admin-only command wrappers
async def admin_only_command(update: Update, context: ContextTypes.DEFAULT_TYPE, command_func):
    """Wrapper to ensure only admins can execute admin commands."""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id or not _is_admin_user(user_id):
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang]["admin_only"])
        return
    await command_func(update, context)


async def upload_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_only_command(update, context, upload_command)


async def broadcast_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_only_command(update, context, broadcast)


async def requests_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await requests_command(update, context)


async def smoke_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_only_command(update, context, smoke_check_command)




_HEAVY_EXECUTOR: ThreadPoolExecutor | None = None


async def run_blocking_heavy(func, *args, **kwargs):
    """Run CPU/LLM-heavy operations on a dedicated executor."""
    loop = asyncio.get_running_loop()
    if _HEAVY_EXECUTOR is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    return await loop.run_in_executor(_HEAVY_EXECUTOR, partial(func, *args, **kwargs))


def _is_transient_db_error(exc: Exception) -> bool:
    text = str(exc).lower()
    transient_markers = (
        "connection reset",
        "connection refused",
        "connection already closed",
        "server closed the connection",
        "could not connect",
        "could not obtain connection",
        "connection pool exhausted",
        "could not translate host name",
        "timeout",
        "timed out",
        "deadlock detected",
        "too many clients",
    )
    return any(marker in text for marker in transient_markers)


async def run_blocking_db_retry(func, *args, retries: int = 2, base_delay: float = 0.20, **kwargs):
    """Retry transient DB operations to reduce random write/read failures."""
    attempt = 0
    while True:
        try:
            return await run_blocking(func, *args, **kwargs)
        except Exception as e:
            if attempt >= retries or not _is_transient_db_error(e):
                raise
            wait_s = base_delay * (2 ** attempt)
            logger.warning(
                "Transient DB error in %s (attempt %s/%s): %s; retrying in %.2fs",
                getattr(func, "__name__", "db_op"),
                attempt + 1,
                retries + 1,
                e,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            attempt += 1


_BOT_INSTANCE_LOCK_FH = None


def _release_single_instance_lock() -> None:
    global _BOT_INSTANCE_LOCK_FH
    try:
        if _BOT_INSTANCE_LOCK_FH:
            fcntl.flock(_BOT_INSTANCE_LOCK_FH.fileno(), fcntl.LOCK_UN)
            _BOT_INSTANCE_LOCK_FH.close()
    except Exception:
        pass
    _BOT_INSTANCE_LOCK_FH = None


def _shutdown_heavy_executor() -> None:
    global _HEAVY_EXECUTOR
    try:
        if _HEAVY_EXECUTOR is not None:
            _HEAVY_EXECUTOR.shutdown(wait=False, cancel_futures=False)
    except Exception:
        pass
    _HEAVY_EXECUTOR = None


atexit.register(_shutdown_heavy_executor)


def _acquire_single_instance_lock() -> bool:
    """Ensure only one bot process handles updates at a time."""
    global _BOT_INSTANCE_LOCK_FH
    lock_path = os.getenv("BOT_INSTANCE_LOCK_FILE", "/tmp/smartaitoolsbot.instance.lock")
    try:
        fh = open(lock_path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()))
        fh.flush()
        _BOT_INSTANCE_LOCK_FH = fh

        atexit.register(_release_single_instance_lock)
        return True
    except BlockingIOError:
        logger.error(f"Another bot instance is already running (lock: {lock_path}). Exiting.")
        return False
    except Exception as e:
        logger.error(f"Failed to acquire bot instance lock: {e}")
        return False


def _schedule_application_task(application, coro):
    """Create app task only when app is running; otherwise close coroutine to avoid warnings."""
    if not application or not getattr(application, "running", False):
        try:
            coro.close()
        except Exception:
            pass
        return None
    try:
        return application.create_task(coro)
    except Exception:
        try:
            coro.close()
        except Exception:
            pass
        return None


def _safe_filename(name: str, default: str = "book") -> str:
    if not name:
        return default
    name = name.strip()
    if not name:
        return default
    # Replace characters that are unsafe in filenames
    name = re.sub(r'[\\/:*?"<>|]+', ' ', name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120] if name else default


def parse_referral_payload(payload: str | None) -> int | None:
    if not payload:
        return None
    token = str(payload).strip()
    if not token:
        return None
    if token.startswith("ref_"):
        token = token[4:]
    elif token.startswith("ref"):
        token = token[3:]
    token = token.strip()
    if token.isdigit():
        return int(token)
    return None


def format_user_name(user: dict) -> str:
    first = (user.get("first_name") or "").strip()
    last = (user.get("last_name") or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    username = (user.get("username") or "").strip()
    if username:
        return f"@{username}"
    return f"User {user.get('id')}"


def format_user_display(user_obj) -> str:
    if not user_obj:
        return "User"
    first = (getattr(user_obj, "first_name", "") or "").strip()
    last = (getattr(user_obj, "last_name", "") or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    username = (getattr(user_obj, "username", "") or "").strip()
    if username:
        return f"@{username}"
    uid = getattr(user_obj, "id", None)
    return f"User {uid}" if uid else "User"


def format_user_tag(user_obj) -> str:
    if not user_obj:
        return "the owner"
    username = (getattr(user_obj, "username", "") or "").strip()
    if username:
        return f"@{username}"
    return format_user_display(user_obj)


def compute_coin_breakdown(searches: int, downloads: int, reactions: int, favorites: int, referrals: int, bonus: int = 0) -> dict:
    coins_searches = int(searches or 0) * COIN_SEARCH
    coins_downloads = int(downloads or 0) * COIN_DOWNLOAD
    coins_reactions = int(reactions or 0) * COIN_REACTION
    coins_favorites = int(favorites or 0) * COIN_FAVORITE
    coins_referrals = int(referrals or 0) * COIN_REFERRAL
    bonus = int(bonus or 0)
    total = coins_searches + coins_downloads + coins_reactions + coins_favorites + coins_referrals + bonus
    return {
        "searches": coins_searches,
        "downloads": coins_downloads,
        "reactions": coins_reactions,
        "favorites": coins_favorites,
        "referrals": coins_referrals,
        "bonus": bonus,
        "total": total,
    }


async def build_referral_link(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str:
    username = getattr(context.bot, "username", None)
    if not username:
        try:
            me = await context.bot.get_me()
            username = getattr(me, "username", None)
        except Exception:
            username = None
    if not username:
        return f"ref_{user_id}"
    return f"https://t.me/{username}?start=ref_{user_id}"


def rank_icon(rank: int) -> str:
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    return "🏅"


COIN_EMOJI = "🪙"


def build_top_users_text(entries: list, limit: int, lang: str, total: int) -> str:
    lines = []
    for idx, entry in enumerate(entries[:limit], start=1):
        name = format_user_name(entry)
        coins = compute_coin_breakdown(
            entry.get("searches", 0),
            entry.get("downloads", 0),
            entry.get("reactions", 0),
            entry.get("favorites", 0),
            entry.get("referrals", 0),
            entry.get("coin_adjustment", 0),
        )
        lines.append(f"{rank_icon(idx)} {name} — {coins['total']} {COIN_EMOJI}")
    hint = ""
    if limit <= TOP_USERS_LIMIT and total > TOP_USERS_LIMIT:
        hint = "\n\n" + MESSAGES[lang]["top_users_hint_more"]
    footer = MESSAGES[lang]["top_users_footer"]
    return MESSAGES[lang]["top_users_title"] + "\n\n" + "\n".join(lines) + hint + "\n\n──────────\n" + footer


def build_top_users_keyboard(total: int, limit: int, lang: str) -> InlineKeyboardMarkup:
    if limit > TOP_USERS_LIMIT:
        label = MESSAGES[lang]["top_users_less"]
        data = "topusers:less"
    else:
        label = MESSAGES[lang]["top_users_more"]
        data = "topusers:more"
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=data)]])


def _book_filename(book) -> str:
    display = _safe_filename(get_display_name(book))
    ext = ""
    path = book.get("path") or ""
    if path:
        ext = os.path.splitext(path)[1]
    if not ext:
        ext = ".epub"
    return f"{display}{ext}"


REACTION_EMOJI = {
    "like": "👍",
    "dislike": "👎",
    "berry": "🍓",
    "whale": "🐳",
}


def build_book_keyboard(
    book_id: str,
    counts: dict,
    is_fav: bool,
    user_reaction: str | None = None,
    can_delete: bool = False,
    lang: str = "en",
    has_audiobook: bool = False,
    can_add_audiobook: bool = False,
    show_listen_button: bool = True,
    audiobook_request_count: int = 0,
) -> InlineKeyboardMarkup:
    like = counts.get("like", 0)
    dislike = counts.get("dislike", 0)
    berry = counts.get("berry", 0)
    whale = counts.get("whale", 0)
    fav_label = "❌ Remove" if is_fav else "⭐ Favorite"
    m = MESSAGES.get(lang, MESSAGES["en"])
    ## summary_label = m.get("summary_button", "🧠 Summarize")

    def label(key: str, emoji: str, count: int) -> str:
        prefix = "★ " if user_reaction == key else ""
        return f"{prefix}{emoji} {count}"

    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(label("whale", REACTION_EMOJI["whale"], whale), callback_data=f"react:{book_id}:whale"),
            InlineKeyboardButton(label("berry", REACTION_EMOJI["berry"], berry), callback_data=f"react:{book_id}:berry"),
            InlineKeyboardButton(label("like", REACTION_EMOJI["like"], like), callback_data=f"react:{book_id}:like"),
            InlineKeyboardButton(label("dislike", REACTION_EMOJI["dislike"], dislike), callback_data=f"react:{book_id}:dislike"),
        ],
        ## [InlineKeyboardButton(summary_label, callback_data=f"summary:{book_id}")],
    ]

    if show_listen_button:
        listen_label = m.get("audiobook_listen_button", "🎧 Listen Audiobook")
        rows.append([InlineKeyboardButton(listen_label, callback_data=f"abook:{book_id}")])

    rows.append([InlineKeyboardButton(fav_label, callback_data=f"fav:toggle:{book_id}")])

    if can_add_audiobook:
        add_label = m.get("audiobook_add_button", "➕ Add Audiobook")
        try:
            pending_count = int(audiobook_request_count or 0)
        except Exception:
            pending_count = 0
        if pending_count > 0:
            template = m.get("audiobook_add_button_with_requests", "{label} ({count})")
            add_label = template.format(label=add_label, count=pending_count)
        rows.append([InlineKeyboardButton(add_label, callback_data=f"abadd:{book_id}")])

    if has_audiobook and can_add_audiobook:
        del_audio_label = m.get("audiobook_delete_all_button", "🗑️ Delete Audios")
        rows.append([InlineKeyboardButton(del_audio_label, callback_data=f"abdelbook:{book_id}")])

    if can_delete:
        rows.append([InlineKeyboardButton("🗑️ Delete book", callback_data=f"delbook:{book_id}")])

    return InlineKeyboardMarkup(rows)



def build_book_caption(book, downloads: int, fav_count: int, counts: dict) -> str:
    return (
        f"⬇️ {downloads} | ⭐ {fav_count}"
    )


async def send_book(bot, chat_id, book):
    """
    Send a book to the user:
    - Prefer local file if it exists
    - Capture and save file_id after upload
    - Fall back to Telegram cache if local file is missing
    - Always index/update in ES using stable UUID
    """

    book_path = book.get("path")
    book_id = str(book.get("id") or "")
    stats = db_get_book_stats(book_id) if book_id else {"downloads": 0, "fav_count": 0, "like": 0, "dislike": 0, "berry": 0, "whale": 0}
    downloads = stats.get("downloads", 0)
    fav_count = stats.get("fav_count", 0)
    counts = {
        "like": stats.get("like", 0),
        "dislike": stats.get("dislike", 0),
        "berry": stats.get("berry", 0),
        "whale": stats.get("whale", 0),
    }
    caption = build_book_caption(book, downloads, fav_count, counts)
    user_id = chat_id if isinstance(chat_id, int) else None
    is_fav = is_favorited(user_id, book_id) if (user_id and book_id) else False
    user_reaction = db_get_user_reaction(book_id, user_id) if (book_id and user_id) else None
    can_delete = await can_delete_books(user_id) if user_id else False
    audio_book = get_audio_book_for_book(book_id) if book_id else None
    has_ab = bool(audio_book)
    can_add_ab = bool(_is_admin_user(user_id)) if user_id else False
    is_owner_user = bool(_is_owner_user(user_id)) if user_id and callable(globals().get("_is_owner_user")) else False
    show_listen_btn = has_ab if is_owner_user else True
    ab_request_count = count_pending_audiobook_requests(book_id) if (book_id and can_add_ab and is_owner_user) else 0
    reactions_kb = (
        build_book_keyboard(
            book_id,
            counts,
            is_fav,
            user_reaction,
            can_delete,
            "en",
            has_audiobook=has_ab,
            can_add_audiobook=can_add_ab,
            show_listen_button=show_listen_btn,
            audiobook_request_count=ab_request_count,
        )
        if book_id
        else None
    )

    if book.get("file_id"):
        # Prefer Telegram cache
        await bot.send_document(
            chat_id=chat_id,
            document=book["file_id"],
            caption=caption,
            reply_markup=reactions_kb,
        )

    elif book_path and os.path.exists(book_path):
        # Fallback to local file
        with open(book_path, "rb") as f:
            sent_message = await bot.send_document(
                chat_id=chat_id,
                document=InputFile(f, filename=_book_filename(book)),
                caption=caption,
                reply_markup=reactions_kb,
            )

        # Capture file_id for future use
        if sent_message and sent_message.document:
            new_file_id = sent_message.document.file_id
            book["file_id"] = new_file_id

            # Save updated file_id in DB
            try:
                if book.get("id"):
                    update_book_file_id(str(book.get("id")), new_file_id, indexed=True)
                elif book_path:
                    update_book_by_path(book_path, file_id=new_file_id, indexed=True)
                logger.debug(f"Updated file_id + indexed flag for {book.get('book_name')} in DB")
            except Exception as e:
                logger.error(f"⚠️ Failed to update book file_id in DB: {e}")

            # ✅ Index in Elasticsearch with stable UUID
            if es_available():
                index_book(
                    book["book_name"],
                    file_id=new_file_id,
                    path=book_path,
                    book_id=book.get("id"),   # <-- permanent UUID
                    display_name=get_display_name(book)
                )
    else:
        await bot.send_message(chat_id=chat_id, text=MESSAGES["en"]["book_unavailable"])

# --- Logging setup ---
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("__main__")
# Reduce noisy logs that include request URLs (and tokens)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("telegram").setLevel(logging.ERROR)
logging.getLogger("telegram.ext").setLevel(logging.ERROR)
logging.getLogger("elastic_transport").setLevel(logging.ERROR)
logging.getLogger("elastic_transport.transport").setLevel(logging.ERROR)
logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("apscheduler").setLevel(logging.ERROR)
logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _normalize_bot_api_base_url(raw: str) -> str:
    value = str(raw or "").strip().rstrip("/")
    if not value:
        return ""
    return value if value.endswith("/bot") else f"{value}/bot"


def _normalize_bot_api_base_file_url(raw: str, fallback_base_url: str) -> str:
    value = str(raw or "").strip().rstrip("/")
    if value:
        return value if value.endswith("/file/bot") else f"{value}/file/bot"
    base = str(fallback_base_url or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/bot"):
        base = base[:-4]
    if not base:
        return ""
    return f"{base}/file/bot"


# Persist errors to file for debugging without cluttering terminal
try:
    os.makedirs("logs", exist_ok=True)
    error_handler = logging.FileHandler("logs/errors.log", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logging.getLogger().addHandler(error_handler)
except Exception:
    pass


# Suppress only the InsecureRequestWarning
warnings.simplefilter("ignore", InsecureRequestWarning)

BOOKS_FILE = "books.json"
USER_FILE = "users.json"
upload_mode = False
movie_upload_mode = False

# File locks (process-level) for simple concurrency safety
BOOKS_LOCK = Lock()
USERS_LOCK = Lock()
BLOCKED_LOCK = Lock()
REMOVED_LOCK = Lock()
ANALYTICS_LOCK = Lock()
REQUESTS_LOCK = Lock()
UPLOAD_REQUESTS_LOCK = Lock()

# ✅ Define the index name once
ES_INDEX = "books"
MOVIES_ES_INDEX = os.getenv("MOVIES_ES_INDEX", "movies").strip() or "movies"

# Elasticsearch config via environment variables
ES_URL = os.getenv("ES_URL", "")
ES_CA_CERT = os.getenv("ES_CA_CERT", "")
ES_USER = os.getenv("ES_USER", "")
ES_PASS = os.getenv("ES_PASS", "")
_ES_CLIENT = None
BOOK_LOVERS_GROUP_URL = (os.getenv("BOOK_LOVERS_GROUP_URL", "https://t.me/book_lovers_clubb") or "").strip()
_BOOK_LOVERS_GROUP_HANDLE_RAW = (os.getenv("BOOK_LOVERS_GROUP_HANDLE", "") or "").strip()
BOT_OWNER_USERNAME = (os.getenv("BOT_OWNER_USERNAME", "@MuhammadaliAbdullayev") or "@MuhammadaliAbdullayev").strip()

ANALYTICS_FILE = "analytics.json"
REQUESTS_FILE = "requests.json"
UPLOAD_REQUESTS_FILE = "upload_requests.json"
PAGE_SIZE = 10
SEARCH_COOLDOWN_SEC = 2
DOWNLOAD_COOLDOWN_SEC = 5
MAX_RECENTS = 5
MAX_FAVORITES = 50
MAX_SEARCH_RESULTS = 20
REQUESTS_PAGE_SIZE = 10
USER_SEARCH_LIMIT = 30
LOCAL_SEND_RETRIES = 3
LOCAL_SEND_BACKOFF_SEC = 3
UPLOAD_LOCAL_WORKERS = 1
UPLOAD_LOCAL_WRITE_TIMEOUT = 3600
UPLOAD_LOCAL_READ_TIMEOUT = 120
UPLOAD_LOCAL_CONNECT_TIMEOUT = 60
UPLOAD_LOCAL_POOL_TIMEOUT = 60
UPLOAD_LOCAL_LARGE_MB = 50
UPLOAD_LOCAL_LARGE_CONCURRENCY = 1
UPLOAD_LOCAL_MAX_MB = 20
SPAM_MSG_LIMIT = 8
SPAM_MSG_WINDOW = 10
SPAM_MSG_BLOCK = 15
SPAM_CB_LIMIT = 12
SPAM_CB_WINDOW = 10
SPAM_CB_BLOCK = 10
TOP_CACHE_TTL = 60
AUDIT_CACHE_TTL = 30
try:
    THREAD_POOL_WORKERS = max(4, int(os.getenv("THREAD_POOL_WORKERS", "50")))
except Exception:
    THREAD_POOL_WORKERS = 50
try:
    HEAVY_THREAD_POOL_WORKERS = max(1, int(os.getenv("HEAVY_THREAD_POOL_WORKERS", "10")))
except Exception:
    HEAVY_THREAD_POOL_WORKERS = 10
try:
    DB_RETRY_ATTEMPTS = max(0, int(os.getenv("DB_RETRY_ATTEMPTS", "2")))
except Exception:
    DB_RETRY_ATTEMPTS = 2
try:
    DB_RETRY_BASE_DELAY_SEC = max(0.05, float(os.getenv("DB_RETRY_BASE_DELAY_SEC", "0.20")))
except Exception:
    DB_RETRY_BASE_DELAY_SEC = 0.20
try:
    ES_HEALTH_CACHE_TTL_SEC = max(3, int(os.getenv("ES_HEALTH_CACHE_TTL_SEC", "15")))
except Exception:
    ES_HEALTH_CACHE_TTL_SEC = 15
_ES_HEALTH_CACHE = {"ok": None, "checked_at": 0.0, "error": None}


def _derive_group_handle(url: str) -> str:
    raw = (url or "").strip().rstrip("/")
    if not raw:
        return "@book_lovers_clubb"
    if raw.startswith("@"):
        return raw
    if "t.me/" in raw:
        slug = raw.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
        if slug:
            return f"@{slug}"
    return raw if raw.startswith("@") else f"@{raw}"


BOOK_LOVERS_GROUP_HANDLE = _BOOK_LOVERS_GROUP_HANDLE_RAW or _derive_group_handle(BOOK_LOVERS_GROUP_URL)


def _read_json_locked(path: str, default, lock: Lock):
    with lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return default
        except Exception as e:
            logger.error(f"Failed to read {path}: {e}")
            return default


def _write_json_atomic(path: str, data, lock: Lock, indent: int = 2):
    dir_name = os.path.dirname(path) or "."
    with lock:
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=dir_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)
            os.replace(tmp_path, path)
        except Exception as e:
            logger.error(f"Failed to write {path}: {e}")
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _today_str():
    return str(datetime.now().date())


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def load_analytics():
    return get_analytics_map()


def increment_analytics(key: str, amount: int = 1):
    return db_increment_analytics(key, amount)


def increment_user_analytics(user_id: int, key: str, amount: int = 1):
    return db_increment_user_analytics(user_id, key, amount)


def load_requests():
    return db_list_requests()


def save_requests(data):
    for r in data:
        if not r.get("id"):
            continue
        existing = db_get_request_by_id(r.get("id"))
        if existing:
            db_update_request(r)
        else:
            db_insert_request(r)


def add_request_record(user, query: str, lang: str):
    request_id = uuid.uuid4().hex[:10]
    record = {
        "id": request_id,
        "user_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "query": query,
        "query_norm": normalize(query),
        "language": lang,
        "status": "open",
        "created_at": _now_iso(),
        "created_ts": time.time(),
        "updated_at": None,
        "status_by": None,
        "status_by_name": None,
        "admin_chat_id": None,
        "admin_message_id": None,
        "admin_note": None,
        "book_id": None
    }
    db_insert_request(record)
    db_increment_counter("request_created", 1)
    return record


def load_upload_requests():
    return db_list_upload_requests()


def save_upload_requests(data):
    for r in data:
        if not r.get("id"):
            continue
        existing = db_get_upload_request_by_id(r.get("id"))
        if existing:
            db_update_upload_request(r)
        else:
            db_insert_upload_request(r)


def add_upload_request_record(user, lang: str):
    request_id = uuid.uuid4().hex[:10]
    record = {
        "id": request_id,
        "user_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language": lang,
        "status": "open",
        "created_at": _now_iso(),
        "created_ts": time.time(),
        "updated_at": None,
        "status_by": None,
        "status_by_name": None,
        "admin_chat_id": None,
        "admin_message_id": None,
        "admin_note": None
    }
    db_insert_upload_request(record)
    return record


def get_upload_request_by_id(request_id: str):
    return db_get_upload_request_by_id(request_id)


def update_upload_request_status(request_id: str, status: str, admin_user=None, admin_note: str | None = None):
    r = db_get_upload_request_by_id(request_id)
    if not r:
        return None
    old_status = r.get("status")
    r["status"] = status
    r["updated_at"] = _now_iso()
    if admin_user:
        r["status_by"] = admin_user.id
        name = f"{admin_user.first_name} {admin_user.last_name or ''}".strip()
        if admin_user.username:
            name += f" (@{admin_user.username})"
        r["status_by_name"] = name
    if admin_note is not None:
        r["admin_note"] = admin_note
    db_update_upload_request(r)
    if status != old_status:
        if status == "accept":
            db_increment_counter("upload_accept", 1)
        elif status == "reject":
            db_increment_counter("upload_reject", 1)
    return r


def set_upload_request_admin_message(request_id: str, chat_id: int, message_id: int):
    r = db_get_upload_request_by_id(request_id)
    if not r:
        return None
    r["admin_chat_id"] = chat_id
    r["admin_message_id"] = message_id
    db_update_upload_request(r)
    return r


def update_request_status(request_id: str, status: str, admin_user=None, admin_note: str | None = None):
    r = db_get_request_by_id(request_id)
    if not r:
        return None
    old_status = r.get("status")
    r["status"] = status
    r["updated_at"] = _now_iso()
    if admin_user:
        r["status_by"] = admin_user.id
        name = f"{admin_user.first_name} {admin_user.last_name or ''}".strip()
        if admin_user.username:
            name += f" (@{admin_user.username})"
        r["status_by_name"] = name
    if admin_note is not None:
        r["admin_note"] = admin_note
    db_update_request(r)
    if status != old_status:
        if status == "seen":
            db_increment_counter("request_seen", 1)
        elif status == "done":
            db_increment_counter("request_done", 1)
        elif status == "no":
            db_increment_counter("request_no", 1)
    return r


def mark_request_fulfilled(request_id: str, book_id: str):
    r = db_get_request_by_id(request_id)
    if not r:
        return None
    old_status = r.get("status")
    r["status"] = "done"
    r["updated_at"] = _now_iso()
    r["book_id"] = book_id
    db_update_request(r)
    if old_status != "done":
        db_increment_counter("request_done", 1)
    return r


def set_request_admin_message(request_id: str, chat_id: int, message_id: int):
    r = db_get_request_by_id(request_id)
    if not r:
        return None
    r["admin_chat_id"] = chat_id
    r["admin_message_id"] = message_id
    db_update_request(r)
    return r


def get_request_by_id(request_id: str):
    return db_get_request_by_id(request_id)


_AUDIOBOOK_REQUEST_BOOK_ID_RE = re.compile(r"\[book_id:\s*([^\]]+)\]", re.IGNORECASE)


def _extract_audiobook_request_book_id(query_text: str | None) -> str | None:
    if not query_text:
        return None
    match = _AUDIOBOOK_REQUEST_BOOK_ID_RE.search(str(query_text))
    if not match:
        return None
    value = str(match.group(1) or "").strip()
    return value or None


def count_pending_audiobook_requests(book_id: str) -> int:
    target = str(book_id or "").strip()
    if not target:
        return 0
    try:
        requests = load_requests()
    except Exception:
        return 0

    pending = 0
    for r in requests or []:
        if (r.get("status") or "") not in {"open", "seen"}:
            continue
        req_book_id = _extract_audiobook_request_book_id(r.get("query"))
        if req_book_id and req_book_id.strip() == target:
            pending += 1
    return pending


def find_open_requests_for_book(book: dict):
    requests = load_requests()
    matches = []
    book_norm = normalize(get_display_name(book))
    book_search = (book.get("book_name") or "").lower()
    for r in requests:
        if r.get("status") not in {"open", "seen"}:
            continue
        q = (r.get("query_norm") or "").strip()
        if not q:
            continue
        if q in book_norm or q in book_search:
            matches.append(r)
    return matches


async def notify_request_matches(bot, book: dict):
    matches = find_open_requests_for_book(book)
    if not matches:
        return 0
    count = 0
    title = get_result_title(book)
    for r in matches:
        lang = r.get("language") or "en"
        try:
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton(MESSAGES[lang]["get_book_button"], callback_data=f"book:{book['id']}")]]
            )
            await bot.send_message(
                chat_id=r["user_id"],
                text=MESSAGES[lang]["request_fulfilled"].format(title=title),
                reply_markup=keyboard
            )
            mark_request_fulfilled(r["id"], book["id"])
            count += 1
        except Exception as e:
            logger.error(f"Failed to notify user {r.get('user_id')} about request: {e}")
    return count


def get_display_name(book: dict) -> str:
    return (book.get("display_name") or book.get("book_name") or "Untitled").strip()

def get_result_title(book: dict) -> str:
    return (book.get("book_name") or book.get("display_name") or "Untitled").strip()


def rate_limited(context: ContextTypes.DEFAULT_TYPE, key: str, cooldown: int):
    now = time.time()
    last = context.user_data.get(key, 0.0)
    delta = now - last
    if delta < cooldown:
        remaining = int(math.ceil(cooldown - delta))
        return True, remaining
    context.user_data[key] = now
    return False, 0


async def safe_answer(query, text: str | None = None, show_alert: bool = False):
    try:
        await query.answer(text=text, show_alert=show_alert)
    except BadRequest as e:
        msg = str(e)
        low = msg.lower()
        if "query is too old" in low or "query id is invalid" in low:
            return
        if "message is not modified" in low:
            return
        raise


async def _send_with_retry(send_fn, retries: int = 3):
    for attempt in range(retries):
        try:
            return await send_fn()
        except RetryAfter as e:
            await asyncio.sleep(getattr(e, "retry_after", 1) + 0.5)
    return None


async def safe_reply(update: Update, text: str, **kwargs) -> bool:
    if not update.message:
        return False
    try:
        sent = await _send_with_retry(lambda: update.message.reply_text(text, **kwargs))
        return sent is not None
    except Forbidden:
        user_id = update.effective_user.id if update.effective_user else None
        if user_id:
            try:
                await run_blocking(set_user_blocked, user_id, True)
                await run_blocking(update_user_left_date, user_id, datetime.now().date())
            except Exception:
                pass
        logger.info("User %s blocked the bot; skipping reply.", user_id)
        return False


def _spam_guard(context: ContextTypes.DEFAULT_TYPE, key: str, limit: int, window: int, block: int):
    now = time.time()
    block_until = context.user_data.get(f"{key}_block_until", 0)
    if now < block_until:
        return True, int(math.ceil(block_until - now))
    history = context.user_data.get(f"{key}_history", [])
    history = [t for t in history if now - t < window]
    history.append(now)
    context.user_data[f"{key}_history"] = history
    if len(history) > limit:
        context.user_data[f"{key}_block_until"] = now + block
        return True, block
    return False, 0


def _is_admin_user(user_id: int) -> bool:
    try:
        return user_id in {ADMIN_ID, OWNER_ID}
    except Exception:
        return user_id == OWNER_ID


def _is_owner_user(user_id: int) -> bool:
    try:
        if OWNER_ID:
            return user_id == OWNER_ID
        return user_id == ADMIN_ID
    except Exception:
        return False


GROUP_READING_SESSIONS_KEY = "group_reading_sessions"


def _is_group_chat(update: Update) -> bool:
    chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
    return chat_type in {"group", "supergroup"}


async def _is_group_admin_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user or not update.effective_chat:
        return False
    user_id = update.effective_user.id
    if _is_admin_user(user_id):
        return True
    try:
        member = await context.bot.get_chat_member(update.effective_chat.id, user_id)
        status = str(getattr(member, "status", "") or "").lower()
        return status in {"administrator", "creator"}
    except Exception:
        return False


def _group_read_sessions(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.application.bot_data.setdefault(GROUP_READING_SESSIONS_KEY, {})


def _group_read_get(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> dict | None:
    session = _group_read_sessions(context).get(chat_id)
    return session if isinstance(session, dict) else None


def _group_read_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "group_only": "⚠️ Bu buyruq faqat guruhlarda ishlaydi.",
            "admin_only": "🚫 Bu buyruq faqat guruh adminlari uchun.",
            "usage": "📘 Foydalanish: /group_read_start <kitob nomi>\nMasalan: /group_read_start Atomic Habits",
            "already_active": "⚠️ Guruhda allaqachon o‘qish sessiyasi faol.\n📊 Holat: /group_read_status\n✅ Yakunlash: /group_read_end",
            "started": "✅ Guruh o‘qish sessiyasi boshlandi.",
            "no_active": "ℹ️ Hozir guruhda faol o‘qish sessiyasi yo‘q.",
            "ended": "✅ Guruh o‘qish sessiyasi yakunlandi.",
            "joined": "📖 Siz o‘quvchilarga qo‘shildingiz.",
            "done": "✅ Siz ‘yakunladim’ deb belgiladingiz.",
            "ask": "❓ Savol belgilandi. Endi shu xabar ostida yozing.",
            "discuss": "💬 Muhokamani shu post ostida javob/reply bilan davom ettiring.",
            "refreshed": "🔄 Holat yangilandi.",
            "panel_title": "📘 Guruh O‘qish Rejimi",
            "book": "Kitob",
            "started_by": "Boshlagan",
            "started_at": "Boshlangan",
            "duration": "Davomiylik",
            "readers": "O‘quvchilar",
            "done_users": "Yakunlaganlar",
            "contributors": "Muhokama qatnashchilari",
            "messages": "Muhokama xabarlari",
            "questions": "Savollar",
            "howto": "💬 Fikr yozish uchun shu xabarga reply qiling.\n❓ Savol bo‘lsa xabarda `?` ishlating.",
            "summary_title": "📘 Guruh O‘qish Yakuni",
        }
    if lang == "ru":
        return {
            "group_only": "⚠️ Эта команда работает только в группах.",
            "admin_only": "🚫 Эта команда только для админов группы.",
            "usage": "📘 Использование: /group_read_start <название книги>\nПример: /group_read_start Atomic Habits",
            "already_active": "⚠️ В группе уже активна сессия чтения.\n📊 Статус: /group_read_status\n✅ Завершить: /group_read_end",
            "started": "✅ Сессия группового чтения запущена.",
            "no_active": "ℹ️ В группе сейчас нет активной сессии чтения.",
            "ended": "✅ Сессия группового чтения завершена.",
            "joined": "📖 Вы добавлены в список читающих.",
            "done": "✅ Отмечено как «завершил».",
            "ask": "❓ Вопрос отмечен. Теперь напишите его под этим сообщением.",
            "discuss": "💬 Продолжайте обсуждение ответом/reply под этим сообщением.",
            "refreshed": "🔄 Статус обновлён.",
            "panel_title": "📘 Режим Группового Чтения",
            "book": "Книга",
            "started_by": "Запустил",
            "started_at": "Старт",
            "duration": "Длительность",
            "readers": "Читают",
            "done_users": "Завершили",
            "contributors": "Участники обсуждения",
            "messages": "Сообщения обсуждения",
            "questions": "Вопросы",
            "howto": "💬 Чтобы обсудить, отвечайте reply на это сообщение.\n❓ Для вопроса используйте `?` в тексте.",
            "summary_title": "📘 Итоги Группового Чтения",
        }
    return {
        "group_only": "⚠️ This command works only in groups.",
        "admin_only": "🚫 This command is only for group admins.",
        "usage": "📘 Usage: /group_read_start <book title>\nExample: /group_read_start Atomic Habits",
        "already_active": "⚠️ A group reading session is already active.\n📊 Status: /group_read_status\n✅ End: /group_read_end",
        "started": "✅ Group reading session started.",
        "no_active": "ℹ️ No active group reading session in this group.",
        "ended": "✅ Group reading session ended.",
        "joined": "📖 You joined the readers list.",
        "done": "✅ Marked as done.",
        "ask": "❓ Question noted. Now ask it under this message.",
        "discuss": "💬 Continue discussion by replying under this message.",
        "refreshed": "🔄 Status refreshed.",
        "panel_title": "📘 Group Reading Mode",
        "book": "Book",
        "started_by": "Started by",
        "started_at": "Started at",
        "duration": "Duration",
        "readers": "Readers",
        "done_users": "Completed",
        "contributors": "Discussing users",
        "messages": "Discussion messages",
        "questions": "Questions",
        "howto": "💬 Reply to this message to discuss.\n❓ Use `?` in your message for questions.",
        "summary_title": "📘 Group Reading Summary",
    }


def _group_read_duration_label(seconds: int) -> str:
    minutes = max(0, int(seconds // 60))
    hours = int(minutes // 60)
    minutes = int(minutes % 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _group_read_panel_text(lang: str, session: dict) -> str:
    t = _group_read_texts(lang)
    started_at = datetime.fromtimestamp(float(session.get("started_at") or time.time())).strftime("%Y-%m-%d %H:%M")
    duration = _group_read_duration_label(int(time.time() - float(session.get("started_at") or time.time())))
    readers = len(session.get("readers") or set())
    done_users = len(session.get("done_users") or set())
    contributors = len(session.get("contributors") or set())
    message_count = int(session.get("message_count") or 0)
    questions = int(session.get("questions") or 0)
    book_title = str(session.get("book_title") or "—")
    started_by = str(session.get("started_by") or "—")
    return "\n".join(
        [
            t["panel_title"],
            "──────────",
            f"📚 {t['book']}: {book_title}",
            f"👤 {t['started_by']}: {started_by}",
            f"🕒 {t['started_at']}: {started_at}",
            f"⏱️ {t['duration']}: {duration}",
            "──────────",
            f"📖 {t['readers']}: {readers}",
            f"✅ {t['done_users']}: {done_users}",
            f"💬 {t['contributors']}: {contributors}",
            f"📝 {t['messages']}: {message_count}",
            f"❓ {t['questions']}: {questions}",
            "",
            t["howto"],
        ]
    )


def _group_read_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    cid = str(chat_id)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📖 Reading", callback_data=f"gread:read:{cid}"),
                InlineKeyboardButton("✅ Done", callback_data=f"gread:done:{cid}"),
            ],
            [
                InlineKeyboardButton("💬 Discuss", callback_data=f"gread:discuss:{cid}"),
                InlineKeyboardButton("❓ Ask", callback_data=f"gread:ask:{cid}"),
            ],
            [InlineKeyboardButton("📊 Refresh", callback_data=f"gread:status:{cid}")],
        ]
    )


async def group_read_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_group_chat(update):
        await safe_reply(update, _group_read_texts(lang)["group_only"])
        return
    if not await _is_group_admin_user(update, context):
        await safe_reply(update, _group_read_texts(lang)["admin_only"])
        return
    book_title = " ".join(context.args or []).strip()
    texts = _group_read_texts(lang)
    if not book_title:
        await safe_reply(update, texts["usage"])
        return
    chat_id = update.effective_chat.id
    sessions = _group_read_sessions(context)
    if sessions.get(chat_id):
        await safe_reply(update, texts["already_active"])
        return

    starter_name = format_user_display(update.effective_user)
    session = {
        "chat_id": chat_id,
        "book_title": book_title,
        "started_at": time.time(),
        "started_by": starter_name,
        "started_by_id": update.effective_user.id if update.effective_user else None,
        "readers": set(),
        "done_users": set(),
        "contributors": set(),
        "message_count": 0,
        "questions": 0,
        "thread_id": getattr(update.effective_message, "message_thread_id", None),
        "lang": lang,
    }
    panel_text = _group_read_panel_text(lang, session)
    sent = await _send_with_retry(
        lambda: update.message.reply_text(panel_text, reply_markup=_group_read_keyboard(chat_id))
    )
    if not sent:
        await safe_reply(update, MESSAGES[lang]["error"])
        return
    session["message_id"] = sent.message_id
    sessions[chat_id] = session
    try:
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id, disable_notification=True)
    except Exception:
        pass
    await safe_reply(update, texts["started"])


async def group_read_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_group_chat(update):
        await safe_reply(update, _group_read_texts(lang)["group_only"])
        return
    chat_id = update.effective_chat.id
    session = _group_read_get(context, chat_id)
    texts = _group_read_texts(lang)
    if not session:
        await safe_reply(update, texts["no_active"])
        return
    await safe_reply(update, _group_read_panel_text(lang, session), reply_markup=_group_read_keyboard(chat_id))


async def group_read_end_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_group_chat(update):
        await safe_reply(update, _group_read_texts(lang)["group_only"])
        return
    if not await _is_group_admin_user(update, context):
        await safe_reply(update, _group_read_texts(lang)["admin_only"])
        return
    chat_id = update.effective_chat.id
    sessions = _group_read_sessions(context)
    session = sessions.pop(chat_id, None)
    texts = _group_read_texts(lang)
    if not session:
        await safe_reply(update, texts["no_active"])
        return
    summary = "\n".join(
        [
            texts["summary_title"],
            "──────────",
            _group_read_panel_text(lang, session),
        ]
    )
    try:
        message_id = session.get("message_id")
        if message_id:
            await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
            await context.bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    await safe_reply(update, texts["ended"])
    await safe_reply(update, summary)


async def handle_group_read_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    texts = _group_read_texts(lang)
    parts = str(query.data or "").split(":", 2)
    if len(parts) < 3:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    _, action, chat_id_raw = parts
    try:
        chat_id = int(chat_id_raw)
    except Exception:
        chat_id = getattr(getattr(query, "message", None), "chat_id", None)
    session = _group_read_get(context, chat_id) if chat_id is not None else None
    if not session:
        await safe_answer(query, texts["no_active"], show_alert=True)
        return

    user_id = query.from_user.id if query.from_user else None
    if user_id:
        if action == "read":
            session.setdefault("readers", set()).add(user_id)
            answer = texts["joined"]
        elif action == "done":
            session.setdefault("readers", set()).add(user_id)
            session.setdefault("done_users", set()).add(user_id)
            answer = texts["done"]
        elif action == "ask":
            session.setdefault("contributors", set()).add(user_id)
            session["questions"] = int(session.get("questions") or 0) + 1
            answer = texts["ask"]
        elif action == "discuss":
            session.setdefault("contributors", set()).add(user_id)
            answer = texts["discuss"]
        else:
            answer = texts["refreshed"]
    else:
        answer = texts["refreshed"]

    panel_text = _group_read_panel_text(lang, session)
    try:
        message_id = int(session.get("message_id") or 0)
        if message_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=panel_text,
                reply_markup=_group_read_keyboard(chat_id),
            )
    except Exception:
        pass
    await safe_answer(query, answer, show_alert=False)


async def _group_read_handle_message_activity(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    del lang
    if not _is_group_chat(update):
        return False
    msg = update.message
    if not msg or not update.effective_chat:
        return False
    session = _group_read_get(context, update.effective_chat.id)
    if not session:
        return False

    related = False
    reply_to = getattr(msg, "reply_to_message", None)
    if reply_to and int(getattr(reply_to, "message_id", 0) or 0) == int(session.get("message_id") or 0):
        related = True
    thread_id = session.get("thread_id")
    if not related and thread_id is not None and getattr(msg, "message_thread_id", None) == thread_id:
        related = True
    if not related:
        return False

    user = update.effective_user
    if user:
        session.setdefault("contributors", set()).add(user.id)
    session["message_count"] = int(session.get("message_count") or 0) + 1
    text = (msg.text or "").strip()
    if "?" in text:
        session["questions"] = int(session.get("questions") or 0) + 1

    # Light periodic refresh of the panel (to avoid message spam and excessive edits).
    if session["message_count"] % 12 == 0:
        chat_id = int(session.get("chat_id") or update.effective_chat.id)
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(session.get("message_id") or 0),
                text=_group_read_panel_text(str(session.get("lang") or "en"), session),
                reply_markup=_group_read_keyboard(chat_id),
            )
        except Exception:
            pass
    return True


def spam_check_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context and getattr(context, "user_data", None):
        if context.user_data.pop("_skip_spam_check_once", False):
            return False, 0
    if update.effective_user and _is_admin_user(update.effective_user.id):
        return False, 0
    return _spam_guard(context, "spam_msg", SPAM_MSG_LIMIT, SPAM_MSG_WINDOW, SPAM_MSG_BLOCK)


def spam_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and _is_admin_user(update.effective_user.id):
        return False, 0
    return _spam_guard(context, "spam_cb", SPAM_CB_LIMIT, SPAM_CB_WINDOW, SPAM_CB_BLOCK)


# Search flow helpers extracted module bridge (phase 1)
prune_search_cache = _search_flow.prune_search_cache
cache_search_results = _search_flow.cache_search_results
get_search_cache = _search_flow.get_search_cache
cache_user_results = _search_flow.cache_user_results
get_user_search_cache = _search_flow.get_user_search_cache
cache_top_results = _search_flow.cache_top_results
get_top_cache = _search_flow.get_top_cache
get_cached_top_entries = _search_flow.get_cached_top_entries
set_cached_top_entries = _search_flow.set_cached_top_entries
get_cached_audit_report = _search_flow.get_cached_audit_report
set_cached_audit_report = _search_flow.set_cached_audit_report
build_results_text = _search_flow.build_results_text
build_results_keyboard = _search_flow.build_results_keyboard
build_user_results_text = _search_flow.build_user_results_text
build_user_results_keyboard = _search_flow.build_user_results_keyboard
build_user_info_text = _search_flow.build_user_info_text
build_user_admin_keyboard = _search_flow.build_user_admin_keyboard


def build_top_text(entries: list, page: int, lang: str):
    total = len(entries)
    pages = max(1, int(math.ceil(total / PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_entries = entries[start:end]

    header = MESSAGES[lang]["top_header"].format(
        page=page + 1,
        pages=pages,
        total=total
    ) + "\n\n"
    lines = []
    for i, e in enumerate(page_entries, start=start + 1):
        lines.append(f"{i}. {e['title']}")
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return header + body + footer, page_entries, pages


def build_top_keyboard(entries: list, page: int, pages: int, query_id: str):
    keyboard = []
    row = []
    start = page * PAGE_SIZE
    for idx, entry in enumerate(entries, start=1):
        label = str(start + idx)
        row.append(
            InlineKeyboardButton(
                label,
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
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"top:{query_id}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"top:{query_id}:{page + 1}"))
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


def build_simple_book_keyboard(items: list):
    keyboard = []
    row = []
    for idx, item in enumerate(items, start=1):
        row.append(InlineKeyboardButton(str(idx), callback_data=f"book:{item['id']}"))
        if idx % 5 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)


def build_requests_keyboard(items: list, user_id: int, page: int, pages: int):
    keyboard = []
    row = []
    for idx, item in enumerate(items, start=1):
        row.append(
            InlineKeyboardButton(
                str(idx),
                callback_data=f"reqview:{user_id}:{item['id']}"
            )
        )
        if idx % 5 == 0:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"reqpage:{user_id}:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"reqpage:{user_id}:{page + 1}"))
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


async def refresh_requests_list(context: ContextTypes.DEFAULT_TYPE, user_id: int, lang: str):
    msg_id = context.user_data.get("requests_list_message_id")
    chat_id = context.user_data.get("requests_list_chat_id", user_id)
    if not msg_id:
        return
    requests = await run_blocking(db_list_requests_for_user, user_id)
    if not requests:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=MESSAGES[lang]["requests_empty"]
            )
        except Exception:
            pass
        return
    requests.sort(key=lambda r: r.get("created_ts") or 0, reverse=True)
    total = len(requests)
    pages = max(1, int(math.ceil(total / REQUESTS_PAGE_SIZE)))
    page = context.user_data.get("requests_page", 0)
    page = max(0, min(page, pages - 1))
    start = page * REQUESTS_PAGE_SIZE
    end = start + REQUESTS_PAGE_SIZE
    page_items = requests[start:end]

    def status_label(status: str) -> str:
        return MESSAGES[lang].get(f"request_status_{status}", status)

    lines = [
        f"{i + 1}. {item.get('query')} — {status_label(item.get('status', 'open'))}"
        for i, item in enumerate(page_items)
    ]
    text = MESSAGES[lang]["requests_title"].format(page=page + 1, pages=pages, total=total) + "\n\n" + "\n".join(lines)
    reply_markup = build_requests_keyboard(page_items, user_id, page, pages)
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text,
            reply_markup=reply_markup
        )
    except Exception:
        pass


def format_request_admin_text(record: dict):
    user_line = f"{record.get('first_name','')} {record.get('last_name','')}".strip()
    if record.get("username"):
        user_line += f" (@{record.get('username')})"
    status = record.get("status", "open")
    text = (
        f"{MESSAGES['en']['request_admin_title']}\n"
        f"{MESSAGES['en']['request_admin_user']}: {user_line}\n"
        f"{MESSAGES['en']['request_admin_user_id']}: {record.get('user_id')}\n"
        f"{MESSAGES['en']['request_admin_query']}: {record.get('query')}\n"
        f"{MESSAGES['en']['request_admin_created']}: {record.get('created_at')}\n"
        f"{MESSAGES['en']['request_admin_status']}: {status}"
    )
    if record.get("admin_note"):
        text += f"\n{MESSAGES['en']['request_admin_note']}: {record.get('admin_note')}"
    if record.get("updated_at"):
        text += f"\n{MESSAGES['en']['request_admin_updated']}: {record.get('updated_at')}"
    return text


def format_upload_request_admin_text(record: dict):
    user_line = f"{record.get('first_name','')} {record.get('last_name','')}".strip()
    if record.get("username"):
        user_line += f" (@{record.get('username')})"
    status = record.get("status", "open")
    text = (
        f"{MESSAGES['en']['upload_admin_title']}\n"
        f"{MESSAGES['en']['upload_admin_user']}: {user_line}\n"
        f"{MESSAGES['en']['upload_admin_user_id']}: {record.get('user_id')}\n"
        f"{MESSAGES['en']['upload_admin_created']}: {record.get('created_at')}\n"
        f"{MESSAGES['en']['upload_admin_status']}: {status}"
    )
    if record.get("admin_note"):
        text += f"\n{MESSAGES['en']['upload_admin_note']}: {record.get('admin_note')}"
    if record.get("updated_at"):
        text += f"\n{MESSAGES['en']['upload_admin_updated']}: {record.get('updated_at')}"
    return text


def build_upload_admin_keyboard(status: str, request_id: str):
    if status == "open":
        return InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(MESSAGES["en"]["upload_action_accept"], callback_data=f"uploadreqstatus:accept:{request_id}"),
                InlineKeyboardButton(MESSAGES["en"]["upload_action_reject"], callback_data=f"uploadreqstatus:reject:{request_id}")
            ]]
        )
    return None


def build_request_admin_keyboard(status: str, request_id: str):
    if status in {"done", "no"}:
        return None
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(MESSAGES["en"]["request_action_seen"], callback_data=f"reqstatus:seen:{request_id}"),
            InlineKeyboardButton(MESSAGES["en"]["request_action_done"], callback_data=f"reqstatus:done:{request_id}"),
            InlineKeyboardButton(MESSAGES["en"]["request_action_no"], callback_data=f"reqstatus:no:{request_id}")
        ]]
    )


def add_recent_download(user_id: int, book_id: str, title: str):
    db_add_recent(user_id, book_id, title, MAX_RECENTS)


def add_favorite(user_id: int, book_id: str, title: str):
    return db_add_favorite(user_id, book_id, title, MAX_FAVORITES)


def remove_favorite(user_id: int, book_id: str):
    return db_remove_favorite(user_id, book_id)


def is_favorited(user_id: int, book_id: str):
    return db_is_favorited(user_id, book_id)


def suggest_books(books: list, query: str, limit: int = 5):
    choices = []
    for b in books:
        title = get_result_title(b)
        if not title:
            continue
        choices.append((b.get("id"), title))
    if not choices:
        return []
    titles = [c[1] for c in choices]
    results = process.extract(query, titles, scorer=fuzz.WRatio, limit=limit)
    suggestions = []
    for _, score, idx in results:
        book_id, title = choices[idx]
        if book_id:
            suggestions.append({"id": str(book_id), "title": title, "score": score})
    return suggestions


def cache_request(context: ContextTypes.DEFAULT_TYPE, query: str, user):
    req_id = uuid.uuid4().hex[:8]
    requests = context.user_data.setdefault("requests", {})
    requests[req_id] = {
        "query": query,
        "user_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language": context.user_data.get("language", "en"),
        "ts": time.time()
    }
    return req_id


async def send_request_to_admin(context: ContextTypes.DEFAULT_TYPE, user, query: str, lang: str):
    record = add_request_record(user, query, lang)
    target_id = get_request_target_id()
    if target_id:
        keyboard = build_request_admin_keyboard(record["status"], record["id"])
        sent = await context.bot.send_message(
            chat_id=target_id,
            text=format_request_admin_text(record),
            reply_markup=keyboard
        )
        if sent:
            set_request_admin_message(record["id"], sent.chat_id, sent.message_id)
    return record


async def send_upload_request_to_admin(context: ContextTypes.DEFAULT_TYPE, user, lang: str):
    record = add_upload_request_record(user, lang)
    target_id = get_request_target_id()
    if target_id:
        keyboard = build_upload_admin_keyboard(record["status"], record["id"])
        sent = await context.bot.send_message(
            chat_id=target_id,
            text=format_upload_request_admin_text(record),
            reply_markup=keyboard
        )
        if sent:
            set_upload_request_admin_message(record["id"], sent.chat_id, sent.message_id)
    return record


def get_user_record(user_id: int):
    return get_user(user_id)


def find_book_by_id(book_id: str):
    return db_get_book_by_id(str(book_id))


def get_request_target_id():
    if REQUEST_CHAT_ID:
        return REQUEST_CHAT_ID
    if OWNER_ID:
        return OWNER_ID
    return ADMIN_ID


def get_admin_id() -> int | None:
    if OWNER_ID:
        return OWNER_ID
    if ADMIN_ID:
        return ADMIN_ID
    return None


def get_missing_file_info(limit: int | None = 200):
    books = load_books()
    missing = []
    for b in books:
        path = b.get("path")
        file_id = b.get("file_id")
        if file_id:
            continue
        if path and os.path.exists(path):
            continue
        reason = "no_file_id"
        if path and not os.path.exists(path):
            reason = "local_missing"
        missing.append({
            "id": b.get("id"),
            "title": get_result_title(b),
            "reason": reason
        })
        if limit is not None and len(missing) >= limit:
            break
    return missing


async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post = update.channel_post
    if not post or not post.chat:
        return
    admin_id = get_admin_id()
    if not admin_id:
        return
    channel_id = post.chat.id
    reported = context.application.bot_data.setdefault("reported_channels", set())
    if channel_id in reported:
        return
    reported.add(channel_id)
    title = post.chat.title or "—"
    username = f"@{post.chat.username}" if post.chat.username else "—"
    text = "\n".join(
        [
            "📢 Channel detected",
            f"🆔 ID: {channel_id}",
            f"📛 Title: {title}",
            f"🔗 Username: {username}",
        ]
    )
    try:
        await context.bot.send_message(chat_id=admin_id, text=text)
    except Exception:
        pass


def get_public_commands(lang: str = "en"):
    return _command_sync.get_public_commands(lang)


def get_public_commands_for_menu(lang: str = "en", user_id: int | None = None):
    del user_id
    return _command_sync.get_public_commands_for_menu(lang)


def get_group_commands(lang: str = "en"):
    return _command_sync.get_group_commands(lang)


def get_admin_commands(lang: str = "en", user_id: int | None = None):
    return _command_sync.get_admin_commands(lang, owner_user=bool(user_id and _is_owner_user(user_id)))


def _build_help_text(lang: str, user_id: int | None = None) -> str:
    return _menu_ui_build_help_text(lang, MESSAGES, _is_admin_user, user_id=user_id)


async def set_bot_commands(application):
    await _command_sync.set_bot_commands(
        application,
        owner_id=OWNER_ID,
        admin_id=ADMIN_ID,
        logger=logger,
    )


BOT_PROFILE_TEXTS = {
    # Default fallback when Telegram language is not matched
    "default": {
        "description": (
            "📚 SmartAIToolsBot — books, audiobooks, and AI tools in one place.\n"
            "🔎 Fast search, ⬇️ easy downloads, 🎧 quick listening.\n"
            "👤 Owned and managed by @MuhammadaliAbdullayev"
        ),
        "about": "📚 Books + AI tools | 👤 @MuhammadaliAbdullayev",
    },
    "en": {
        "description": (
            "📚 SmartAIToolsBot — books, audiobooks, and AI tools in one place.\n"
            "🔎 Fast search, ⬇️ easy downloads, 🎧 quick listening.\n"
            "👤 Owned and managed by @MuhammadaliAbdullayev"
        ),
        "about": "📚 Books + AI tools | 👤 @MuhammadaliAbdullayev",
    },
    "uz": {
        "description": (
            "📚 SmartAIToolsBot — kitoblar, audiokitoblar va AI vositalar bir joyda.\n"
            "🔎 Tez qidiruv, ⬇️ qulay yuklab olish, 🎧 tinglash.\n"
            "👤 Owned and managed by @MuhammadaliAbdullayev"
        ),
        "about": "📚 Kitob + AI vositalar | 👤 @MuhammadaliAbdullayev",
    },
    "ru": {
        "description": (
            "📚 SmartAIToolsBot — книги, аудиокниги и AI-инструменты в одном месте.\n"
            "🔎 Быстрый поиск, ⬇️ удобные загрузки, 🎧 прослушивание.\n"
            "👤 Owned and managed by @MuhammadaliAbdullayev"
        ),
        "about": "📚 Книги + AI | 👤 @MuhammadaliAbdullayev",
    },
}


def _clip_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    return text[:limit] if len(text) > limit else text


async def set_bot_profile_texts(application):
    bot = application.bot
    default_payload = BOT_PROFILE_TEXTS.get("default", BOT_PROFILE_TEXTS.get("en", {}))
    default_desc = _clip_text(default_payload.get("description", ""), 512)
    default_about = _clip_text(default_payload.get("about", ""), 120)

    try:
        if default_desc:
            await bot.set_my_description(description=default_desc)
        if default_about:
            await bot.set_my_short_description(short_description=default_about)
    except Exception as e:
        logger.warning("Failed to set default bot profile texts: %s", e)

    for lang_code, payload in BOT_PROFILE_TEXTS.items():
        if lang_code == "default":
            continue
        desc = _clip_text(payload.get("description", ""), 512)
        about = _clip_text(payload.get("about", ""), 120)
        try:
            if desc:
                await bot.set_my_description(description=desc, language_code=lang_code)
            if about:
                await bot.set_my_short_description(short_description=about, language_code=lang_code)
        except Exception as e:
            logger.warning("Failed to set bot profile texts for lang=%s: %s", lang_code, e)


async def _sync_user_commands_if_needed(context: ContextTypes.DEFAULT_TYPE, user_id: int | None, lang: str):
    await _command_sync.sync_user_commands_if_needed(
        context,
        user_id=user_id,
        lang=lang,
        owner_id=OWNER_ID,
        admin_id=ADMIN_ID,
        logger=logger,
    )


async def post_init(application):
    global _HEAVY_EXECUTOR
    try:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=THREAD_POOL_WORKERS))
        logger.debug(f"Thread pool set: max_workers={THREAD_POOL_WORKERS}")
        if _HEAVY_EXECUTOR is None:
            _HEAVY_EXECUTOR = ThreadPoolExecutor(max_workers=HEAVY_THREAD_POOL_WORKERS)
            logger.debug(f"Heavy thread pool set: max_workers={HEAVY_THREAD_POOL_WORKERS}")
    except Exception as e:
        logger.error(f"Failed to set thread pool: {e}")
    async def _bg_set_commands():
        try:
            await set_bot_commands(application)
        except Exception as e:
            logger.error(f"Background command sync failed: {e}")

    async def _bg_set_profile_texts():
        try:
            await set_bot_profile_texts(application)
        except Exception as e:
            logger.error(f"Background profile text sync failed: {e}")

    async def _bg_backfill_awards():
        try:
            await run_blocking_db_retry(
                db_backfill_user_awards_if_empty,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.error(f"Failed to backfill user awards: {e}")

    asyncio.create_task(_bg_set_commands())
    asyncio.create_task(_bg_set_profile_texts())
    try:
        application.job_queue.run_repeating(prune_blocked_users, interval=3 * 60 * 60, first=60)
        logger.debug("Scheduled prune_blocked_users every 3 hours.")
    except Exception as e:
        logger.error(f"Failed to schedule prune_blocked_users: {e}")
    asyncio.create_task(_bg_backfill_awards())


def get_es():
    global _ES_CLIENT
    if _ES_CLIENT is not None:
        return _ES_CLIENT
    if not ES_URL:
        logger.debug("ES_URL not set; Elasticsearch disabled.")
        return None
    kwargs = {}
    if ES_CA_CERT:
        kwargs["ca_certs"] = ES_CA_CERT
    if ES_USER and ES_PASS:
        kwargs["basic_auth"] = (ES_USER, ES_PASS)
    try:
        _ES_CLIENT = Elasticsearch(ES_URL, **kwargs)
        return _ES_CLIENT
    except Exception as e:
        logger.error(f"Failed to create Elasticsearch client: {e}")
        return None


def es_available(force_refresh: bool = False):
    """Return cached ES availability to avoid per-request blocking health checks."""
    try:
        now = time.monotonic()
        checked_at = float(_ES_HEALTH_CACHE.get("checked_at", 0.0) or 0.0)
        cached_ok = _ES_HEALTH_CACHE.get("ok", None)
        if not force_refresh and cached_ok is not None and (now - checked_at) < ES_HEALTH_CACHE_TTL_SEC:
            return bool(cached_ok)

        es = get_es()
        if not es:
            _ES_HEALTH_CACHE.update({"ok": False, "checked_at": now, "error": "client_unavailable"})
            return False

        ok = False
        err = None
        try:
            ok = bool(es.ping())
            if not ok:
                # Fallback for clusters where ping may be disabled.
                es.info()
                ok = True
        except Exception as e:
            err = str(e)
            ok = False

        _ES_HEALTH_CACHE.update({"ok": ok, "checked_at": now, "error": err})
        if not ok and err:
            logger.error(f"ES health check failed: {err}")
        return ok
    except Exception as e:
        logger.error(f"ES availability check failed: {e}")
        return False


def ensure_index():
    try:
        es = get_es()
        if not es:
            return
        if not es.indices.exists(index=ES_INDEX):
            es.indices.create(index=ES_INDEX)
            logger.debug(f"Created ES index: {ES_INDEX}")
        else:
            logger.debug(f"ES index exists: {ES_INDEX}")
    except Exception as e:
        logger.error(f"Failed to ensure index: {e}")


def ensure_movies_index():
    try:
        es = get_es()
        if not es:
            return
        if not es.indices.exists(index=MOVIES_ES_INDEX):
            es.indices.create(index=MOVIES_ES_INDEX)
            logger.debug(f"Created ES index: {MOVIES_ES_INDEX}")
        else:
            logger.debug(f"ES index exists: {MOVIES_ES_INDEX}")
    except Exception as e:
        logger.error(f"Failed to ensure movies index: {e}")


def index_book(book_name, file_id=None, path=None, book_id=None, display_name=None, file_unique_id=None, refresh: str | bool | None = "wait_for"):
    try:
        es = get_es()
        if not es:
            return None
        # Always require a stable UUID
        if not book_id:
            book_id = str(uuid.uuid4())

        doc = {
            "id": book_id,            # ✅ include UUID inside the document
            "book_name": book_name,
            "display_name": display_name or book_name,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "path": path,
            "indexed": True           # ✅ mark as indexed when saving
        }

        # Use stable UUID as ES document ID
        es.index(
            index=ES_INDEX,
            id=book_id,
            document=doc,
            refresh=refresh
        )
        logger.debug(f"Indexed/updated in ES: {book_name} (id={book_id})")
        return book_id
    except Exception as e:
        logger.error(f"Failed to index in ES: {e}")
        return None


def index_movie(
    movie_name,
    file_id=None,
    path=None,
    movie_id=None,
    display_name=None,
    file_unique_id=None,
    mime_type=None,
    duration_seconds=None,
    file_size=None,
    channel_id=None,
    channel_message_id=None,
    release_year=None,
    genre=None,
    movie_lang=None,
    country=None,
    rating=None,
    caption_text=None,
    search_text=None,
    indexed: bool = True,
    refresh: str | bool | None = "wait_for",
):
    try:
        es = get_es()
        if not es:
            return None
        ensure_movies_index()
        if not movie_id:
            movie_id = str(uuid.uuid4())

        doc = {
            "id": movie_id,
            "movie_name": movie_name,
            "display_name": display_name or movie_name,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "path": path,
            "mime_type": mime_type,
            "duration_seconds": duration_seconds,
            "file_size": file_size,
            "channel_id": channel_id,
            "channel_message_id": channel_message_id,
            "release_year": release_year,
            "release_year_text": str(release_year) if release_year else None,
            "genre": genre,
            "movie_lang": movie_lang,
            "country": country,
            "rating": rating,
            "caption_text": caption_text,
            "search_text": search_text,
            "indexed": bool(indexed),
        }
        es.index(
            index=MOVIES_ES_INDEX,
            id=movie_id,
            document=doc,
            refresh=refresh,
        )
        logger.debug("Indexed/updated movie in ES: %s (id=%s)", movie_name, movie_id)
        return movie_id
    except Exception as e:
        logger.error("Failed to index movie in ES: %s", e)
        return None


def bulk_index_books(docs: list[dict], refresh: str | bool | None = "false") -> dict[str, dict]:
    """
    Bulk index docs into Elasticsearch.
    Returns mapping: {book_id: {"ok": bool, "error": str|None}}
    """
    out: dict[str, dict] = {}
    if not docs:
        return out
    try:
        es = get_es()
        if not es:
            for d in docs:
                bid = str((d or {}).get("id") or "")
                if bid:
                    out[bid] = {"ok": False, "error": "ES unavailable"}
            return out

        ensure_index()
        operations = []
        for d in docs:
            if not isinstance(d, dict):
                continue
            bid = str(d.get("id") or "")
            if not bid:
                continue
            operations.append({"index": {"_index": ES_INDEX, "_id": bid}})
            operations.append(
                {
                    "id": bid,
                    "book_name": d.get("book_name"),
                    "display_name": d.get("display_name") or d.get("book_name"),
                    "file_id": d.get("file_id"),
                    "file_unique_id": d.get("file_unique_id"),
                    "path": d.get("path"),
                    "indexed": True,
                }
            )
            out[bid] = {"ok": False, "error": "unknown"}

        if not operations:
            return out

        resp = es.bulk(operations=operations, refresh=refresh)
        items = resp.get("items") or []
        for item in items:
            idx = (item or {}).get("index") or {}
            bid = str(idx.get("_id") or "")
            if not bid:
                continue
            err = idx.get("error")
            status = int(idx.get("status") or 0)
            ok = err is None and 200 <= status < 300
            out[bid] = {"ok": ok, "error": None if ok else str(err or f"status={status}")}
        return out
    except Exception as e:
        logger.error("Bulk indexing failed: %s", e, exc_info=True)
        for d in docs:
            bid = str((d or {}).get("id") or "")
            if bid:
                out[bid] = {"ok": False, "error": str(e)}
        return out


def search_es(query, size: int = MAX_SEARCH_RESULTS):
    try:
        es = get_es()
        if not es:
            return []
        res = es.search(
            index=ES_INDEX,
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["book_name^2", "display_name^2"],
                    "fuzziness": "AUTO"
                }
            },
            size=size,
            track_total_hits=False,
            source_includes=["id", "book_name", "display_name"]
        )
        hits = [(hit["_source"], hit["_score"], hit["_id"]) for hit in res["hits"]["hits"]]
        logger.debug(f"ES search '{query}' -> {len(hits)} hits")
        return hits
    except Exception as e:
        logger.error(f"ES search failed: {e}")
        return []


def search_movies_es(query, size: int = MAX_SEARCH_RESULTS):
    try:
        es = get_es()
        if not es:
            return []
        res = es.search(
            index=MOVIES_ES_INDEX,
            query={
                "multi_match": {
                    "query": query,
                    "fields": [
                        "movie_name^3",
                        "display_name^3",
                        "search_text^4",
                        "genre^2",
                        "movie_lang^2",
                        "country^2",
                        "rating",
                        "release_year_text^2",
                        "caption_text",
                    ],
                    "fuzziness": "AUTO",
                }
            },
            size=size,
            track_total_hits=False,
            source_includes=["id", "movie_name", "display_name", "release_year", "genre", "movie_lang"],
        )
        hits = [(hit["_source"], hit["_score"], hit["_id"]) for hit in res["hits"]["hits"]]
        logger.debug("ES movie search '%s' -> %s hits", query, len(hits))
        return hits
    except Exception as e:
        logger.error("ES movie search failed: %s", e)
        return []


def get_es_health_summary(es):
    status = "unknown"
    count = 0
    try:
        health = es.cluster.health(index=ES_INDEX)
        status = health.get("status", "unknown")
    except Exception as e:
        logger.error(f"ES health failed: {e}")
    try:
        count = es.count(index=ES_INDEX).get("count", 0)
    except Exception as e:
        logger.error(f"ES count failed: {e}")
    return status, count

BOOKS_FILE = "books.json"

# --- Stopwords and token helpers ---
STOPWORDS = {
    "the", "a", "an", "for", "to", "of", "and", "in", "on", "at", "with",
    "pdf", "zip", "rar", "doc", "docx", "z", "lib", "org"
}

def normalize(text: str) -> str:
    text = text.lower()
    # Normalize Uzbek apostrophes to nothing (so o‘tkan == otkan)
    text = text.replace("'", "").replace("’", "").replace("ʻ", "").replace("ʼ", "")
    text = re.sub(r'@[\w]+', ' ', text)                 # remove @usernames
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)  # remove links
    text = text.replace("_", " ")
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)  # remove punctuation/symbols
    text = re.sub(r'\s+', ' ', text).strip()
    return text


_CYRILLIC_TO_LATIN = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
    "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
    "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}

_ARABIC_TO_LATIN = {
    "ا": "a", "أ": "a", "إ": "i", "آ": "a", "ب": "b", "ت": "t",
    "ث": "th", "ج": "j", "ح": "h", "خ": "kh", "د": "d", "ذ": "dh",
    "ر": "r", "ز": "z", "س": "s", "ش": "sh", "ص": "s", "ض": "d",
    "ط": "t", "ظ": "z", "ع": "a", "غ": "gh", "ف": "f", "ق": "q",
    "ك": "k", "ل": "l", "م": "m", "ن": "n", "ه": "h", "و": "w",
    "ؤ": "w", "ي": "y", "ئ": "y", "ى": "a", "ة": "h",
    "پ": "p", "چ": "ch", "ژ": "zh", "گ": "g", "ک": "k", "ی": "y",
}


def latinize_text(text: str) -> str:
    if not text:
        return ""
    s = text.lower()
    out = []
    for ch in s:
        if ch in _CYRILLIC_TO_LATIN:
            out.append(_CYRILLIC_TO_LATIN[ch])
        elif ch in _ARABIC_TO_LATIN:
            out.append(_ARABIC_TO_LATIN[ch])
        else:
            out.append(ch)
    return normalize("".join(out))

def tokenize(text: str):
    return [t for t in normalize(text).split() if t not in STOPWORDS]

# --- Helpers for books ---
def load_books():
    try:
        books = db_list_books()
        logger.debug(f"Loaded {len(books)} books from DB")
        return books
    except Exception as e:
        logger.error(f"Failed to load books from DB: {e}")
        return []

def save_books(books):
    try:
        count = bulk_upsert_books(books)
        logger.debug(f"Saved {count} books to DB")
    except Exception as e:
        logger.error(f"Failed to save books to DB: {e}")


def load_movies(limit: int = 50000):
    try:
        movies = db_list_movies(limit=limit)
        logger.debug(f"Loaded {len(movies)} movies from DB")
        return movies
    except Exception as e:
        logger.error(f"Failed to load movies from DB: {e}")
        return []

def clean_query(text: str) -> str:
    return normalize(text)

def reindex_books():
    if not es_available():
        logger.error("Elasticsearch not available, skipping reindex.")
        return
    try:
        ensure_index()
        books = load_books()
        count = 0
        skipped = 0
        seen = set()  # track already indexed book IDs

        for book in books:
            raw_name = book.get("display_name") or book.get("book_name")
            file_id = book.get("file_id")
            path = book.get("path")
            book_id = book.get("id")   # permanent UUID stored in DB

            if not raw_name:
                skipped += 1
                continue

            # Clean the book_name before indexing
            clean_name = normalize(raw_name)
            book["book_name"] = clean_name  # normalized for search
            if not book.get("display_name"):
                book["display_name"] = raw_name

            # if no UUID yet, assign one
            if not book_id:
                book_id = str(uuid.uuid4())
                book["id"] = book_id

            # avoid duplicates by ID
            if book_id in seen:
                logger.debug(f"Skipping duplicate ID: {clean_name}")
                skipped += 1
                continue

            # Index with cleaned name
            index_book(
                clean_name,
                file_id=file_id,
                path=path,
                book_id=book_id,
                display_name=book.get("display_name") or raw_name,
                file_unique_id=book.get("file_unique_id"),
            )
            seen.add(book_id)
            count += 1

        save_books(books)  # persist cleaned names and any new UUIDs
        logger.debug(
            f"Reindexed {count} unique books into Elasticsearch. Skipped {skipped} duplicates/invalid entries."
        )
    except Exception as e:
        logger.error(f"Reindexing failed: {e}", exc_info=True)


# --- Helpers for users ---
def load_users():
    try:
        users = list_users()
        logger.debug(f"Loaded {len(users)} users from DB")
        return users
    except Exception as e:
        logger.error(f"Failed to load users from DB: {e}")
        return []


def save_users(users):
    try:
        for u in users:
            upsert_user(
                user_id=u.get("id"),
                username=u.get("username"),
                first_name=u.get("first_name"),
                last_name=u.get("last_name"),
                blocked=bool(u.get("blocked", False)),
                allowed=bool(u.get("allowed", False)),
                joined_date=u.get("joined_date"),
                left_date=u.get("left_date"),
                language=u.get("language"),
                delete_allowed=bool(u.get("delete_allowed", False)),
                stopped=bool(u.get("stopped", False)),
                language_selected=u.get("language_selected"),
            )
        logger.debug(f"Saved {len(users)} users to DB")
    except Exception as e:
        logger.error(f"Failed to save users to DB: {e}")


def detect_language_code(code: str | None) -> str:
    if not code:
        return "en"
    code = code.lower()
    if code.startswith("uz"):
        return "uz"
    if code.startswith("ru"):
        return "ru"
    return "en"


def set_user_language(user_id: int, lang: str):
    user = get_user(user_id)
    if not user:
        upsert_user(
            user_id=user_id,
            username=None,
            first_name=None,
            last_name=None,
            blocked=False,
            allowed=False,
            joined_date=datetime.now().date(),
            left_date=None,
            language=lang,
            language_selected=True,
        )
    else:
        update_user_language(user_id, lang)


def ensure_user_language(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    lang = context.user_data.get("language")
    if lang:
        return lang
    user = get_user_record(update.effective_user.id)
    if user and user.get("language") and bool(user.get("language_selected")) is True:
        context.user_data["language"] = user["language"]
        return user["language"]
    # Do not auto-apply Telegram locale. Force explicit language selection.
    context.user_data["language"] = "en"
    return "en"


async def update_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE | None = None):
    try:
        user = update.effective_user
        if not user:
            return
        if context is not None:
            now_ts = time.time()
            last_ts = float(context.user_data.get("_user_info_touch_ts", 0) or 0)
            # Avoid duplicate DB writes when global guards and feature handlers both call this.
            if now_ts - last_ts < 2.0:
                return
            context.user_data["_user_info_touch_ts"] = now_ts
        today = datetime.now().date()
        existing = await run_blocking_db_retry(
            get_user,
            user.id,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        ) or {}
        if existing:
            lang_selected = existing.get("language_selected")
            if lang_selected is None:
                # If user never explicitly chose a language, keep showing language picker on /start.
                lang_selected = False
        else:
            lang_selected = False
        effective_lang = (existing.get("language") if lang_selected else None) or "en"
        await run_blocking_db_retry(
            upsert_user,
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            blocked=bool(existing.get("blocked", False)),
            allowed=bool(existing.get("allowed", False)),
            joined_date=existing.get("joined_date") or today,
            left_date=existing.get("left_date"),
            language=effective_lang,
            delete_allowed=bool(existing.get("delete_allowed", False)),
            stopped=bool(existing.get("stopped", False)),
            language_selected=lang_selected,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
        if context:
            _schedule_application_task(
                context.application,
                _sync_user_commands_if_needed(context, user.id, effective_lang),
            )
    except Exception as e:
        logger.error(f"Failed to update user info: {e}")


async def _touch_user_activity_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.effective_user:
            return
        if update.message and update.message.chat and getattr(update.message.chat, "type", None) == "channel":
            return
        _schedule_application_task(context.application, update_user_info(update, context))
    except Exception:
        pass


async def _touch_user_activity_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.effective_user:
            return
        _schedule_application_task(context.application, update_user_info(update, context))
    except Exception:
        pass


def is_blocked(user_id: int) -> bool:
    user = get_user(user_id)
    return bool(user.get("blocked")) if user else False


def is_allowed(user_id: int) -> bool:
    user = get_user(user_id)
    return bool(user.get("allowed")) if user else False


def is_bot_paused(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.application.bot_data.get("bot_paused"))


def format_bot_paused_message(lang: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    msg = MESSAGES[lang]["bot_paused"]
    if "{time}" in msg:
        dt = context.application.bot_data.get("bot_paused_at")
        if not dt:
            dt = datetime.now().strftime("%Y-%m-%d %H:%M")
            context.application.bot_data["bot_paused_at"] = dt
        try:
            return msg.format(time=dt)
        except Exception:
            return msg
    return msg


def format_bot_paused_on(lang: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    msg = MESSAGES[lang]["bot_paused_on"]
    if "{time}" in msg:
        dt = context.application.bot_data.get("bot_paused_at")
        if not dt:
            dt = datetime.now().strftime("%Y-%m-%d %H:%M")
            context.application.bot_data["bot_paused_at"] = dt
        try:
            return msg.format(time=dt)
        except Exception:
            return msg
    return msg


async def pick_upload_channel(app) -> int | None:
    if not UPLOAD_CHANNEL_IDS:
        return None
    data = app.bot_data
    lock = data.get("upload_channel_lock")
    if lock is None:
        lock = asyncio.Lock()
        data["upload_channel_lock"] = lock
    async with lock:
        idx = int(data.get("upload_channel_index", 0) or 0)
        cid = UPLOAD_CHANNEL_IDS[idx % len(UPLOAD_CHANNEL_IDS)]
        data["upload_channel_index"] = idx + 1
        return cid


async def enqueue_upload_fanout(app, file_id: str, book_id: str | None = None):
    if not UPLOAD_CHANNEL_IDS:
        return
    data = app.bot_data
    q = data.get("upload_fanout_queue")
    if q is None:
        q = asyncio.Queue()
        data["upload_fanout_queue"] = q
    await q.put({"file_id": file_id, "book_id": (str(book_id) if book_id else None)})
    task = data.get("upload_fanout_task")
    if not task or task.done():
        data["upload_fanout_task"] = _schedule_application_task(app, upload_fanout_worker(app))


async def upload_fanout_worker(app):
    data = app.bot_data
    q: asyncio.Queue | None = data.get("upload_fanout_queue")
    if q is None:
        return
    try:
        while True:
            try:
                first = await asyncio.wait_for(q.get(), timeout=5)
            except asyncio.TimeoutError:
                if q.empty():
                    break
                continue

            batch = [first]
            limit = max(1, len(UPLOAD_CHANNEL_IDS))
            while len(batch) < limit:
                try:
                    batch.append(q.get_nowait())
                except asyncio.QueueEmpty:
                    break

            task_meta: list[dict] = []
            for item in batch:
                if isinstance(item, dict):
                    fid = str(item.get("file_id") or "").strip()
                    bid = str(item.get("book_id") or "").strip() or None
                else:
                    fid = str(item or "").strip()
                    bid = None
                if not fid:
                    continue
                cid = await pick_upload_channel(app)
                if not cid:
                    continue
                task_meta.append({"channel_id": cid, "book_id": bid, "file_id": fid})
            fanout_retry_max = max(1, int(os.getenv("UPLOAD_FANOUT_RETRY_MAX", "8") or "8"))
            fanout_send_delay = max(0.0, float(os.getenv("UPLOAD_FANOUT_SEND_DELAY_SEC", "0.15") or "0.15"))
            fanout_retry_jitter = max(0.0, float(os.getenv("UPLOAD_FANOUT_RETRY_JITTER_SEC", "0.5") or "0.5"))

            for meta in task_meta:
                cid = int(meta.get("channel_id") or 0)
                book_id = meta.get("book_id")
                fid = str(meta.get("file_id") or "").strip()
                if not cid or not fid:
                    continue

                sent = None
                last_err = None
                for attempt in range(1, fanout_retry_max + 1):
                    try:
                        sent = await app.bot.send_document(chat_id=cid, document=fid)
                        last_err = None
                        break
                    except RetryAfter as e:
                        last_err = e
                        wait_s = float(getattr(e, "retry_after", 1) or 1) + fanout_retry_jitter
                        logger.warning(
                            "Fanout flood control for channel %s, waiting %.2fs (attempt %s/%s)",
                            cid,
                            wait_s,
                            attempt,
                            fanout_retry_max,
                        )
                        await asyncio.sleep(wait_s)
                    except (TimedOut, NetworkError) as e:
                        last_err = e
                        backoff = min(10.0, 0.5 * (2 ** (attempt - 1)))
                        logger.warning(
                            "Fanout transient error for channel %s: %s (attempt %s/%s, wait %.2fs)",
                            cid,
                            e,
                            attempt,
                            fanout_retry_max,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                    except Exception as e:
                        last_err = e
                        logger.error("Failed to send uploaded file to channel %s: %s", cid, e)
                        break

                if not sent:
                    if last_err is not None:
                        logger.error(
                            "Fanout failed after retries for channel %s (book_id=%s): %s",
                            cid,
                            book_id,
                            last_err,
                        )
                    continue

                if book_id and getattr(sent, "message_id", None):
                    try:
                        doc = getattr(sent, "document", None)
                        await run_blocking(
                            db_update_book_storage_meta,
                            book_id,
                            cid,
                            int(sent.message_id),
                            getattr(doc, "file_id", None),
                            getattr(doc, "file_unique_id", None),
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to persist storage metadata for book_id=%s channel=%s: %s",
                            book_id,
                            cid,
                            e,
                        )

                if fanout_send_delay > 0:
                    await asyncio.sleep(fanout_send_delay)

            for _ in batch:
                q.task_done()

            if q.empty():
                break
    finally:
        if data.get("upload_fanout_task") is asyncio.current_task():
            data.pop("upload_fanout_task", None)


async def can_delete_books(user_id: int) -> bool:
    if _is_owner_user(user_id):
        return True
    try:
        return await run_blocking_db_retry(
            db_is_user_delete_allowed,
            user_id,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
    except Exception:
        return False


async def is_stopped_user(user_id: int) -> bool:
    try:
        return await run_blocking_db_retry(
            db_is_user_stopped,
            user_id,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
    except Exception:
        return False
# --- Error handler ---
async def handle_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception in handler:", exc_info=context.error)
    tb = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
    logger.error(f"Traceback:\n{tb}")
    try:
        if isinstance(update, Update) and update.effective_chat:
            lang = ensure_user_language(update, context)
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=MESSAGES[lang]["error_notified"]
            )
    except Exception as e:
        logger.error(f"Failed to send error message: {e}")


async def paused_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_bot_paused(context):
        return
    if not update.effective_user:
        return
    if _is_admin_user(update.effective_user.id):
        return
    try:
        paused_updates = context.application.bot_data.setdefault("paused_updates", {})
        paused_updates[update.effective_user.id] = update
    except Exception:
        pass
    lang = ensure_user_language(update, context)
    await safe_reply(update, format_bot_paused_message(lang, context))
    raise ApplicationHandlerStop


async def paused_callback_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_bot_paused(context):
        return
    if not update.effective_user:
        return
    if _is_admin_user(update.effective_user.id):
        return
    try:
        paused_updates = context.application.bot_data.setdefault("paused_updates", {})
        paused_updates[update.effective_user.id] = update
    except Exception:
        pass
    lang = ensure_user_language(update, context)
    try:
        await update.callback_query.answer(format_bot_paused_message(lang, context), show_alert=True)
    except Exception:
        pass
    raise ApplicationHandlerStop


async def process_paused_updates(app):
    updates = app.bot_data.pop("paused_updates", {}) or {}
    for upd in updates.values():
        try:
            await app.process_update(upd)
        except Exception as e:
            logger.error(f"Failed to process paused update: {e}", exc_info=True)


async def pause_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    context.application.bot_data["bot_paused"] = True
    context.application.bot_data["bot_paused_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    await target_message.reply_text(format_bot_paused_on(lang, context))


async def resume_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    context.application.bot_data["bot_paused"] = False
    context.application.bot_data.pop("bot_paused_at", None)
    await target_message.reply_text(MESSAGES[lang]["bot_paused_off"])
    try:
        _schedule_application_task(context.application, process_paused_updates(context.application))
    except Exception as e:
        logger.error(f"Failed to resume paused updates: {e}")


async def _post_start_background_sync(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    referrer_id: int | None,
) -> None:
    try:
        await update_user_info(update, context)
    except Exception as e:
        logger.error(f"Background start user sync failed: {e}")
    if not referrer_id:
        return
    try:
        linked = await run_blocking_db_retry(
            db_set_user_referrer,
            update.effective_user.id,
            referrer_id,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
        if not linked:
            return
        ref_user = await run_blocking_db_retry(
            get_user,
            referrer_id,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
        ref_lang = (ref_user or {}).get("language") or "en"
        joined_name = format_user_display(update.effective_user)
        joined_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        notice = MESSAGES[ref_lang]["referral_joined_notice"].format(
            name=joined_name,
            dt=joined_time,
        )
        await context.bot.send_message(chat_id=referrer_id, text=notice)
    except Exception as e:
        logger.error(f"Failed to notify referrer {referrer_id}: {e}")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if is_blocked(update.effective_user.id):
            lang = ensure_user_language(update, context)
            await safe_reply(update, MESSAGES[lang]["blocked"])
            return
        if update.effective_user and await is_stopped_user(update.effective_user.id):
            return

        limited, wait_s = spam_check_message(update, context)
        if limited:
            lang = ensure_user_language(update, context)
            await safe_reply(update, MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            return

        # Reset menu/search state until the user explicitly chooses a language.
        context.user_data.pop("main_menu_section", None)
        context.user_data["awaiting_book_search"] = False
        referrer_id = parse_referral_payload(context.args[0] if context.args else None)
        lang = ensure_user_language(update, context)
        # Always ask user to choose language on /start (do not auto-use Telegram locale).
        await safe_reply(
            update,
            MESSAGES[lang]["choose_language"],
            reply_markup=get_language_keyboard()
        )
        _schedule_application_task(
            context.application,
            _post_start_background_sync(update, context, referrer_id),
        )

    except Exception as e:
        logger.error(f"/start failed: {e}")
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang]["error"])


async def language_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and is_blocked(update.effective_user.id):
        lang = ensure_user_language(update, context)
        await update.message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        lang = ensure_user_language(update, context)
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    lang = ensure_user_language(update, context)
    # Keep menu hidden/reset until language is explicitly chosen.
    context.user_data.pop("main_menu_section", None)
    context.user_data["awaiting_book_search"] = False
    await update.message.reply_text(
        MESSAGES[lang]["choose_language"],
        reply_markup=get_language_keyboard()
    )


def _build_start_greeting_text(lang: str, tg_user) -> str:
    first_name = (getattr(tg_user, "first_name", None) or "").strip() or "Friend"
    salutation = {
        "uz": f"Assalomu alaykum, {first_name} 👋",
        "ru": f"Здравствуйте, {first_name} 👋",
        "en": f"Welcome, {first_name} 👋",
    }.get(lang, f"Welcome, {first_name} 👋")
    base = MESSAGES.get(lang, MESSAGES["en"]).get("greeting") or MESSAGES["en"]["greeting"]
    return f"{salutation}\n\n{base}"


def _build_start_greeting_intro(lang: str, tg_user) -> str:
    first_name = (getattr(tg_user, "first_name", None) or "").strip() or "Friend"
    return {
        "uz": f"Assalomu alaykum, {first_name} 👋\n✨ Bot tayyorlanmoqda...",
        "ru": f"Здравствуйте, {first_name} 👋\n✨ Готовлю бота...",
        "en": f"Welcome, {first_name} 👋\n✨ Getting things ready...",
    }.get(lang, f"Welcome, {first_name} 👋\n✨ Getting things ready...")

def _admin_control_guide_text() -> str:
    return _menu_ui_admin_control_guide_text(_ADMIN_MENU_LABELS)


def _main_menu_keyboard(lang: str, section: str = "main", user_id: int | None = None) -> ReplyKeyboardMarkup:
    return _menus_build_main_menu_keyboard(
        lang=lang,
        section=section,
        user_id=user_id,
        messages=MESSAGES,
        is_admin_user_fn=_is_admin_user,
        admin_labels=_ADMIN_MENU_LABELS,
    )


def _main_menu_text_action(text: str) -> str | None:
    return _menu_ui_main_menu_text_action(text, MESSAGES, _ADMIN_MENU_LABELS)


async def _send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, section: str = "main"):
    if not update.message:
        return False
    first_name = (getattr(update.effective_user, "first_name", None) or "").strip() or "Friend"
    text = _menus_build_main_menu_message_text(
        lang=lang,
        section=section,
        first_name=first_name,
        messages=MESSAGES,
        admin_labels=_ADMIN_MENU_LABELS,
        admin_guide_text_fn=_admin_control_guide_text,
    )
    uid = update.effective_user.id if update.effective_user else None
    await _send_with_retry(lambda: update.message.reply_text(text, reply_markup=_main_menu_keyboard(lang, section, uid)))
    context.user_data["main_menu_section"] = section
    context.user_data["awaiting_book_search"] = False
    return True


async def _send_main_menu_to_chat_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, lang: str, section: str = "main", user_id: int | None = None):
    text = _menus_build_main_menu_chat_text(lang=lang, section=section, messages=MESSAGES)
    try:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=_main_menu_keyboard(lang, section, user_id or chat_id))
        context.user_data["main_menu_section"] = section
        context.user_data["awaiting_book_search"] = False
        return True
    except Exception:
        return False


async def _reply_search_menu_click_hint(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not update.message:
        return
    m = MESSAGES.get(lang, MESSAGES["en"])
    uid = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(
        m.get(
            "menu_search_click_first_hint",
            "📚 First tap 🔎 Search Books, then send the book name as text.",
        ),
        reply_markup=_main_menu_keyboard(lang, "main", uid),
    )
    context.user_data["main_menu_section"] = "main"


async def _reply_search_image_hint(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not update.message:
        return
    m = MESSAGES.get(lang, MESSAGES["en"])
    uid = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(
        m.get(
            "menu_search_image_hint",
            "🖼️ I can’t read book names from images yet. Please send the book name as text.",
        ),
        reply_markup=_main_menu_keyboard(lang, "main", uid),
    )
    context.user_data["main_menu_section"] = "main"


async def _cancel_menu_conflicting_flows(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    user_id = update.effective_user.id if update.effective_user else None
    cancel_text = MESSAGES.get(lang, MESSAGES["en"]).get("menu_flow_cancelled", "❌ Previous process was cancelled.")
    cancelled = False

    async def _edit_prompt_if_any(session: dict | None):
        if not isinstance(session, dict):
            return
        chat_id = session.get("prompt_chat_id")
        msg_id = session.get("prompt_message_id")
        if not chat_id or not msg_id:
            return
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=cancel_text,
                reply_markup=None,
            )
        except BadRequest as e:
            # If text cannot be edited (e.g. unchanged), still try removing stale keyboard.
            if "message is not modified" in str(e).lower():
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
            else:
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
        except Exception:
            try:
                await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
            except Exception:
                pass

    tts_session = _tts_get_session(context)
    if tts_session and (not user_id or tts_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(tts_session)
        _tts_clear_session(context)
        cancelled = True

    pdf_session = _pdf_maker_get_session(context)
    if pdf_session and (not user_id or pdf_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(pdf_session)
        _pdf_maker_clear_session(context)
        cancelled = True

    ai_tool_mode_session = _ai_tool_mode_get_session(context)
    if ai_tool_mode_session and (not user_id or ai_tool_mode_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(ai_tool_mode_session)
        _ai_tool_mode_clear_session(context)
        cancelled = True

    ai_image_session = _ai_image_get_session(context)
    if ai_image_session and (not user_id or ai_image_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(ai_image_session)
        _ai_image_clear_session(context)
        cancelled = True

    # Cancel audiobook adding flow if active
    pending_abook = context.user_data.get("pending_abook")
    if pending_abook and (not user_id or True):  # Audiobook flow is user-specific
        context.user_data.pop("pending_abook", None)
        cancelled = True

    video_dl_session = _video_dl_get_session(context)
    if video_dl_session and (not user_id or video_dl_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(video_dl_session)
        _video_dl_clear_session(context)
        cancelled = True

    if context.user_data.get("awaiting_request"):
        context.user_data["awaiting_request"] = False
        context.user_data.pop("awaiting_request_until", None)
        cancelled = True
    if context.user_data.get("admin_menu_prompt"):
        context.user_data.pop("admin_menu_prompt", None)
        cancelled = True
    if _ai_chat_get_session(context):
        _ai_chat_clear_session(context)
        cancelled = True

    return cancelled


async def _handle_main_menu_action(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, action: str) -> bool:
    if not update.message:
        return False
    m = MESSAGES.get(lang, MESSAGES["en"])
    user_id = update.effective_user.id if update.effective_user else None
    current_section = str(context.user_data.get("main_menu_section") or "main")

    if action == "search":
        context.user_data["awaiting_book_search"] = True
        context.user_data["awaiting_movie_search"] = False
        await update.message.reply_text(
            m.get("menu_search_prompt", "Send a book name to search."),
            reply_markup=_main_menu_keyboard(lang, "main", user_id),
        )
        context.user_data["main_menu_section"] = "main"
        return True
    context.user_data["awaiting_book_search"] = False
    if action == "search_movies":
        context.user_data["awaiting_movie_search"] = True
        await update.message.reply_text(
            m.get("menu_movie_search_prompt", "Send a movie name to search."),
            reply_markup=_main_menu_keyboard(lang, "main", user_id),
        )
        context.user_data["main_menu_section"] = "main"
        return True
    context.user_data["awaiting_movie_search"] = False
    if action == "tts":
        context.user_data["main_menu_section"] = "main"
        await _tts_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "pdf":
        context.user_data["main_menu_section"] = "main"
        await _pdf_maker_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "request":
        keep_section = "other" if current_section == "other" else "main"
        context.user_data["awaiting_request"] = True
        context.user_data["awaiting_request_until"] = time.time() + 30
        context.user_data["main_menu_section"] = keep_section
        await update.message.reply_text(
            m.get("menu_request_prompt", m.get("request_prompt", "Send book name.")),
            reply_markup=_main_menu_keyboard(lang, keep_section, user_id),
        )
        return True
    if action == "favorites":
        context.user_data["main_menu_section"] = "other" if current_section == "other" else "main"
        context.user_data["_skip_spam_check_once"] = True
        await favorites_command(update, context)
        return True
    if action == "other":
        await _send_main_menu(update, context, lang, "other")
        return True
    if action == "ai_tools":
        await _send_main_menu(update, context, lang, "ai_tools")
        return True
    if action == "ai_chat":
        context.user_data["main_menu_section"] = "ai_tools"
        await _ai_chat_start_session_from_message(update.message, update, context, lang)
        return True
    if action in {"ai_translator", "ai_grammar", "ai_email_writer", "ai_quiz", "ai_music"}:
        context.user_data["main_menu_section"] = "ai_tools"
        mode_map = {
            "ai_translator": "translator",
            "ai_grammar": "grammar",
            "ai_email_writer": "email",
            "ai_quiz": "quiz",
            "ai_music": "music",
        }
        await _ai_tool_mode_start_session_from_message(update.message, update, context, lang, mode_map[action])
        return True
    if action == "ai_image":
        context.user_data["main_menu_section"] = "ai_tools"
        await update.message.reply_text(
            m.get(
                "menu_ai_image_coming_soon",
                "🖼️ AI Image Generator will be added soon. Thanks for your patience.",
            ),
            reply_markup=_main_menu_keyboard(lang, "ai_tools", user_id),
        )
        return True
    if action == "back":
        prev = str(context.user_data.get("main_menu_section") or "main")
        if prev in {"admin_maintenance", "admin_duplicates", "admin_tasks", "admin_uploads"}:
            await _send_main_menu(update, context, lang, "admin")
        elif prev == "admin":
            await _send_main_menu(update, context, lang, "main")
        else:
            await _send_main_menu(update, context, lang, "main")
        return True
    if action == "myprofile":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await myprofile_command(update, context)
        return True
    if action == "top_books":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await top_command(update, context)
        return True
    if action == "top_users":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await top_users_command(update, context)
        return True
    if action == "ramazon":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await ramazon_command(update, context)
        return True
    if action == "help":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await help_command(update, context)
        return True
    if action == "upload":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await upload_command(update, context)
        return True
    if action == "movie_upload":
        context.user_data["main_menu_section"] = "other"
        context.user_data["_skip_spam_check_once"] = True
        await movie_upload_command(update, context)
        return True
    if action == "video_downloader":
        context.user_data["main_menu_section"] = "other"
        await _video_dl_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "contact_admin":
        context.user_data["main_menu_section"] = "other"
        info_tpl = m.get(
            "contact_admin_info",
            "📞 Contact Admin\n👤 Bot owner/developer: {owner}\n🌍 Group: {group}",
        )
        try:
            info_text = info_tpl.format(owner=BOT_OWNER_USERNAME, group=BOOK_LOVERS_GROUP_HANDLE)
        except Exception:
            info_text = info_tpl
        await update.message.reply_text(
            info_text,
            reply_markup=_main_menu_keyboard(lang, "other", user_id),
        )
        return True
    handled_admin = await _admin_tools_handle_admin_menu_action(
        update=update,
        context=context,
        lang=lang,
        action=action,
        user_id=user_id,
        messages=MESSAGES,
        is_admin_user_fn=_is_admin_user,
        main_menu_keyboard_fn=_main_menu_keyboard,
        send_main_menu_fn=_send_main_menu,
        upload_command_fn=upload_command,
        pause_bot_command_fn=pause_bot_command,
        resume_bot_command_fn=resume_bot_command,
        audit_command_fn=audit_command,
        prune_command_fn=prune_command,
        missing_command_fn=missing_command,
        db_dupes_command_fn=db_dupes_command,
        es_dupes_command_fn=es_dupes_command,
        dupes_status_command_fn=dupes_status_command,
        cancel_task_command_fn=cancel_task_command,
        admin_panel_send_upload_local_status_fn=_admin_panel_send_upload_local_status,
        start_upload_local_books_fn=_start_upload_local_books,
    )
    if handled_admin:
        return True
    return False


async def _send_animated_start_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not update.message:
        return False
    final_text = _build_start_greeting_text(lang, update.effective_user)
    uid = update.effective_user.id if update.effective_user else None
    sent = await _send_with_retry(lambda: update.message.reply_text(final_text, reply_markup=_main_menu_keyboard(lang, "main", uid)))
    if not sent:
        return False
    context.user_data["main_menu_section"] = "main"
    return True


async def _edit_or_send_animated_start_greeting(query, context: ContextTypes.DEFAULT_TYPE, lang: str):
    final_text = _build_start_greeting_text(lang, getattr(query, "from_user", None))
    uid = getattr(getattr(query, "from_user", None), "id", None)
    # Send main menu immediately; cleanup of old picker message is done in background.
    try:
        sent = await context.bot.send_message(
            chat_id=query.from_user.id,
            text=final_text,
            reply_markup=_main_menu_keyboard(lang, "main", uid),
        )
        context.user_data["main_menu_section"] = "main"
        context.user_data["awaiting_book_search"] = False
        if getattr(query, "message", None):
            async def _cleanup_old_picker():
                try:
                    await query.message.delete()
                except Exception:
                    try:
                        await query.edit_message_reply_markup(reply_markup=None)
                    except Exception:
                        pass

            _schedule_application_task(context.application, _cleanup_old_picker())
        return sent
    except Exception:
        pass


async def _send_ramazon_message_to_target(target_message, context: ContextTypes.DEFAULT_TYPE, lang: str):
    text = MESSAGES.get(lang, {}).get("ramazon_text") or MESSAGES["uz"]["ramazon_text"]
    share_text = text
    share_label = MESSAGES.get(lang, {}).get("ramazon_share_button", "🔗 Share")
    share_url = None
    username = getattr(context.bot, "username", None)
    if not username:
        try:
            me = await context.bot.get_me()
            username = getattr(me, "username", None)
        except Exception:
            username = None
    if share_text:
        share_text = share_text.replace("<blockquote>", "«").replace("</blockquote>", "»")
    if username:
        bot_mention = f"@{username}"
        text = f"{text}\n\n{bot_mention}"
        if share_text:
            share_text = f"{share_text}\n\n{bot_mention}"
            share_url = f"https://t.me/share/url?text={quote_plus(share_text)}"
    reply_markup = None
    if share_url:
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(share_label, url=share_url)]])
    await target_message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")


async def ramazon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if update.effective_user and is_blocked(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await _send_ramazon_message_to_target(update.message, context, lang)


async def handle_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    lang = query.data.split("_")[1]  # uz / en / ru
    context.user_data["language"] = lang
    await safe_answer(query)
    async def _persist_language():
        try:
            await run_blocking_db_retry(
                set_user_language,
                query.from_user.id,
                lang,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.error(f"Failed to persist user language {query.from_user.id}: {e}")

    _schedule_application_task(context.application, _persist_language())
    await _edit_or_send_animated_start_greeting(query, context, lang)
    try:
        uid = query.from_user.id
        _schedule_application_task(
            context.application,
            _sync_user_commands_if_needed(context, uid, lang),
        )
    except Exception as e:
        logger.error(f"Failed to update user commands language: {e}")
    try:
        context.user_data["main_menu_section"] = "main"
    except Exception:
        pass


# Upload flow extracted module bridge
_upload_flow.configure(globals())

upload_command = _upload_flow.upload_command
movie_upload_command = _upload_flow.movie_upload_command
_process_upload = _upload_flow._process_upload
handle_file = _upload_flow.handle_file
handle_movie_video = _upload_flow.handle_movie_video
handle_photo_message = _upload_flow.handle_photo_message
sync_unindexed_books = _upload_flow.sync_unindexed_books
sync_unindexed_movies = _upload_flow.sync_unindexed_movies


def _format_bytes(bytes_count: int) -> str:
    """Format bytes into human readable format."""
    if bytes_count == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    
    while bytes_count >= 1024 and unit_index < len(units) - 1:
        bytes_count /= 1024.0
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(bytes_count)} {units[unit_index]}"
    else:
        return f"{bytes_count:.1f} {units[unit_index]}"


async def audit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user.id != ADMIN_ID:
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    cached = get_cached_audit_report(context, lang)
    if cached:
        await target_message.reply_text(cached)
        return

    try:
        # --- System / ES ---
        es_count = 0
        es_health = "-"
        es_status = MESSAGES[lang]["audit_status_down"]
        if es_available():
            es = get_es()
            if es:
                es_count = await run_blocking(lambda: es.count(index=ES_INDEX).get("count", 0))
                try:
                    health = await run_blocking(lambda: es.cluster.health(index=ES_INDEX))
                    es_health = health.get("status", "-")
                    es_status = MESSAGES[lang]["audit_status_up"]
                except Exception:
                    es_health = "-"
                    es_status = MESSAGES[lang]["audit_status_up"]

        # --- DB status ---
        db_stats = await run_blocking(get_db_stats)
        db_status = MESSAGES[lang]["audit_status_up"] if db_stats.get("ok") else MESSAGES[lang]["audit_status_down"]
        db_error = db_stats.get("error")
        db_counts = db_stats.get("counts", {})
        recents_count = db_counts.get("user_recents", 0)
        removed_count = db_counts.get("removed_users", 0)

        today = datetime.now().date()
        daily_users = await run_blocking(db_get_user_daily_counts, today)
        daily_analytics = await run_blocking(db_get_daily_analytics, today)

        # --- Current totals ---
        books_totals = await run_blocking(db_get_book_totals)
        user_status = await run_blocking(db_get_user_status_counts)
        fav_total = await run_blocking(db_get_favorites_total)
        req_status = await run_blocking(db_get_request_status_counts)
        upload_status = await run_blocking(db_get_upload_request_status_counts)
        reaction_current = await run_blocking(db_get_reaction_totals)
        
        # --- New statistics ---
        try:
            audio_stats = await run_blocking(db_get_audio_book_stats)
        except Exception as e:
            logger.error(f"Audio book stats failed: {e}")
            audio_stats = {'total_audiobooks': 0, 'books_with_audiobooks': 0, 'total_parts': 0, 'total_downloads': 0, 'total_searches': 0, 'total_duration_seconds': 0}
        
        try:
            storage_stats = await run_blocking(db_get_storage_stats)
        except Exception as e:
            logger.error(f"Storage stats failed: {e}")
            storage_stats = {'total_files': 0, 'total_size': 0, 'book_count': 0, 'total_book_size': 0, 'audio_count': 0, 'total_audio_size': 0, 'avg_book_size': 0, 'avg_audio_size': 0}

        # --- Lifetime analytics counters ---
        counter_keys = [
            "search_total",
            "download_total",
            "favorite_added",
            "favorite_removed",
            "request_created",
            "request_cancelled",
            "request_seen",
            "request_done",
            "request_no",
            "upload_accept",
            "upload_reject",
            "reaction_like",
            "reaction_dislike",
            "reaction_berry",
            "reaction_whale",
            # AI Tools counters
            "ai_chat_sessions",
            "ai_translator_uses",
            "ai_grammar_fixes",
            "ai_email_writes",
            "ai_quiz_generated",
            "ai_music_generated",
            "ai_pdf_created",
            "ai_image_generated",
        ]
        counters = await run_blocking(db_get_counters, counter_keys)

        # --- Build report ---
        lines = [MESSAGES[lang]["audit_title"]]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_system"],
            f"- {MESSAGES[lang]['audit_db_status']}: {db_status}",
            f"- {MESSAGES[lang]['audit_es_status']}: {es_status}",
            f"- {MESSAGES[lang]['audit_es_health']}: {es_health}",
            f"- {MESSAGES[lang]['audit_books_es']}: {es_count}",
        ]
        if db_error:
            lines.append(f"- {MESSAGES[lang]['audit_db_error']}: {db_error}")

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_today"],
            f"- {MESSAGES[lang]['audit_today_joined']}: {daily_users.get('joined', 0)}",
            f"- {MESSAGES[lang]['audit_today_left']}: {daily_users.get('left', 0)}",
            f"- {MESSAGES[lang]['audit_today_searches']}: {daily_analytics.get('searches', 0)}",
            f"- {MESSAGES[lang]['audit_today_downloads']}: {daily_analytics.get('downloads', 0)}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_books"],
            f"- {MESSAGES[lang]['audit_books_total']}: {books_totals.get('total', 0)}",
            f"- {MESSAGES[lang]['audit_books_indexed']}: {books_totals.get('indexed', 0)}",
            f"- {MESSAGES[lang]['audit_unindexed']}: {max(0, books_totals.get('total', 0) - books_totals.get('indexed', 0))}",
            f"- {MESSAGES[lang]['audit_books_downloads_total']}: {books_totals.get('downloads', 0)}",
            f"- {MESSAGES[lang]['audit_books_searches_total']}: {books_totals.get('searches', 0)}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_users"],
            f"- {MESSAGES[lang]['audit_total_users']}: {user_status.get('total', 0)}",
            f"- {MESSAGES[lang]['audit_users_blocked']}: {user_status.get('blocked', 0)}",
            f"- {MESSAGES[lang]['audit_users_allowed']}: {user_status.get('allowed', 0)}",
            f"- {MESSAGES[lang]['audit_recents_count']}: {recents_count}",
            f"- {MESSAGES[lang]['audit_removed_users_count']}: {removed_count}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_requests"],
            f"- {MESSAGES[lang]['audit_requests_open']}: {req_status.get('open', 0)}",
            f"- {MESSAGES[lang]['audit_requests_seen']}: {req_status.get('seen', 0)}",
            f"- {MESSAGES[lang]['audit_requests_done']}: {req_status.get('done', 0)}",
            f"- {MESSAGES[lang]['audit_requests_no']}: {req_status.get('no', 0)}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_uploads"],
            f"- {MESSAGES[lang]['audit_upload_open']}: {upload_status.get('open', 0)}",
            f"- {MESSAGES[lang]['audit_upload_accept']}: {upload_status.get('accept', 0)}",
            f"- {MESSAGES[lang]['audit_upload_reject']}: {upload_status.get('reject', 0)}",
        ]

        lines += [
            "──────────",
            "🎧 Audio Books",
            f"- Total audiobooks: {audio_stats.get('total_audiobooks', 0)}",
            f"- Books with audiobooks: {audio_stats.get('books_with_audiobooks', 0)}",
            f"- Total audio parts: {audio_stats.get('total_parts', 0)}",
            f"- Audiobook downloads: {audio_stats.get('total_downloads', 0)}",
            f"- Audiobook searches: {audio_stats.get('total_searches', 0)}",
            f"- Total duration: {audio_stats.get('total_duration_seconds', 0) // 3600}h {audio_stats.get('total_duration_seconds', 0) % 3600 // 60}m",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_favorites"],
            f"- {MESSAGES[lang]['audit_favorites_total']}: {fav_total}",
            f"- {MESSAGES[lang]['audit_favorites_added']}: {counters.get('favorite_added', 0)}",
            f"- {MESSAGES[lang]['audit_favorites_removed']}: {counters.get('favorite_removed', 0)}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_events"],
            f"- {MESSAGES[lang]['audit_search_total']}: {counters.get('search_total', 0)}",
            f"- {MESSAGES[lang]['audit_download_total']}: {counters.get('download_total', 0)}",
            f"- {MESSAGES[lang]['audit_requests_created']}: {counters.get('request_created', 0)}",
            f"- {MESSAGES[lang]['audit_requests_cancelled']}: {counters.get('request_cancelled', 0)}",
            f"- {MESSAGES[lang]['audit_requests_seen_total']}: {counters.get('request_seen', 0)}",
            f"- {MESSAGES[lang]['audit_requests_done_total']}: {counters.get('request_done', 0)}",
            f"- {MESSAGES[lang]['audit_requests_no_total']}: {counters.get('request_no', 0)}",
            f"- {MESSAGES[lang]['audit_upload_accept_total']}: {counters.get('upload_accept', 0)}",
            f"- {MESSAGES[lang]['audit_upload_reject_total']}: {counters.get('upload_reject', 0)}",
        ]

        lines += [
            "──────────",
            "🤖 AI Tools",
            f"- AI Chat sessions: {counters.get('ai_chat_sessions', 0)}",
            f"- Translator uses: {counters.get('ai_translator_uses', 0)}",
            f"- Grammar fixes: {counters.get('ai_grammar_fixes', 0)}",
            f"- Email writes: {counters.get('ai_email_writes', 0)}",
            f"- Quiz generated: {counters.get('ai_quiz_generated', 0)}",
            f"- Music generated: {counters.get('ai_music_generated', 0)}",
            f"- PDF created: {counters.get('ai_pdf_created', 0)}",
            f"- Image generated: {counters.get('ai_image_generated', 0)}",
        ]

        lines += [
            "──────────",
            "💾 Storage",
            f"- Total files: {storage_stats.get('total_files', 0)}",
            f"- Total size: {_format_bytes(storage_stats.get('total_size', 0))}",
            f"- Books: {storage_stats.get('book_count', 0)} files ({_format_bytes(storage_stats.get('total_book_size', 0))})",
            f"- Audio: {storage_stats.get('audio_count', 0)} files ({_format_bytes(storage_stats.get('total_audio_size', 0))})",
            f"- Avg book size: {_format_bytes(storage_stats.get('avg_book_size', 0))}",
            f"- Avg audio size: {_format_bytes(storage_stats.get('avg_audio_size', 0))}",
        ]

        lines += [
            "──────────",
            MESSAGES[lang]["audit_section_reactions"],
            f"- {MESSAGES[lang]['audit_reaction_like']}: {counters.get('reaction_like', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_dislike']}: {counters.get('reaction_dislike', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_berry']}: {counters.get('reaction_berry', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_whale']}: {counters.get('reaction_whale', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_current_like']}: {reaction_current.get('like', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_current_dislike']}: {reaction_current.get('dislike', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_current_berry']}: {reaction_current.get('berry', 0)}",
            f"- {MESSAGES[lang]['audit_reaction_current_whale']}: {reaction_current.get('whale', 0)}",
        ]

        report = "\n".join(lines)
        set_cached_audit_report(context, lang, report)
        await target_message.reply_text(report)

    except Exception as e:
        logger.error(f"Audit command failed: {e}", exc_info=True)
        await target_message.reply_text(MESSAGES[lang]["audit_failed"])

# Search flow handlers extracted module bridge (phase 2)
_search_flow.configure(globals())

transliterate_to_latin = _search_flow.transliterate_to_latin
search_books = _search_flow.search_books
handle_book_selection = _search_flow.handle_book_selection
handle_movie_selection = _search_flow.handle_movie_selection
handle_audiobook_callback = _search_flow.handle_audiobook_callback
handle_audiobook_part_callback = _search_flow.handle_audiobook_part_callback
handle_audiobook_part_delete_callback = _search_flow.handle_audiobook_part_delete_callback
handle_audiobook_delete_callback = _search_flow.handle_audiobook_delete_callback
handle_audiobook_delete_by_book_callback = _search_flow.handle_audiobook_delete_by_book_callback
handle_audiobook_listen_callback = _search_flow.handle_audiobook_listen_callback
handle_audiobook_page_callback = _search_flow.handle_audiobook_page_callback
handle_audiobook_part_play_callback = _search_flow.handle_audiobook_part_play_callback
handle_audiobook_add_callback = _search_flow.handle_audiobook_add_callback
handle_abook_audio = _search_flow.handle_abook_audio
handle_page_callback = _search_flow.handle_page_callback
handle_movie_page_callback = _search_flow.handle_movie_page_callback
handle_user_page_callback = _search_flow.handle_user_page_callback
handle_user_select_callback = _search_flow.handle_user_select_callback


# Engagement handlers extracted module bridge
_engagement_handlers.configure(globals())

handle_user_action_callback = _engagement_handlers.handle_user_action_callback
top_users_command = _engagement_handlers.top_users_command
handle_top_users_toggle_callback = _engagement_handlers.handle_top_users_toggle_callback
top_command = _engagement_handlers.top_command
handle_top_page_callback = _engagement_handlers.handle_top_page_callback
handle_favorite_callback = _engagement_handlers.handle_favorite_callback
handle_reaction_callback = _engagement_handlers.handle_reaction_callback
SUMMARY_MODES = _engagement_handlers.SUMMARY_MODES
_summary_mode_label = _engagement_handlers._summary_mode_label
_summary_mode_keyboard = _engagement_handlers._summary_mode_keyboard
_summary_lang_name = _engagement_handlers._summary_lang_name
_summary_stage_text = _engagement_handlers._summary_stage_text
_summary_progress_render = _engagement_handlers._summary_progress_render
_summary_progress_set = _engagement_handlers._summary_progress_set
_summary_telegram_split = _engagement_handlers._summary_telegram_split
_summary_chunk_text = _engagement_handlers._summary_chunk_text
_summary_tesseract_lang_candidates = _engagement_handlers._summary_tesseract_lang_candidates
_summary_ocr_pdf_text_blocking = _engagement_handlers._summary_ocr_pdf_text_blocking
_summary_extract_text_blocking = _engagement_handlers._summary_extract_text_blocking
_summary_text_hash = _engagement_handlers._summary_text_hash
_summary_ollama_generate_blocking = _engagement_handlers._summary_ollama_generate_blocking
_summary_prompt_for_mode = _engagement_handlers._summary_prompt_for_mode
_summary_chunk_prompt = _engagement_handlers._summary_chunk_prompt
_summary_cleanup_output = _engagement_handlers._summary_cleanup_output
_summary_output_looks_invalid = _engagement_handlers._summary_output_looks_invalid
_summary_summarize_text_blocking = _engagement_handlers._summary_summarize_text_blocking
_summary_send_text = _engagement_handlers._summary_send_text
_summary_edit_progress_message = _engagement_handlers._summary_edit_progress_message
_summary_progress_loop = _engagement_handlers._summary_progress_loop
_summary_prepare_text_for_book = _engagement_handlers._summary_prepare_text_for_book
_run_book_summary_job = _engagement_handlers._run_book_summary_job
handle_summary_placeholder_callback = _engagement_handlers.handle_summary_placeholder_callback
handle_delete_book_callback = _engagement_handlers.handle_delete_book_callback


# User interactions extracted module bridge
_user_interactions.configure(globals())

handle_request_callback = _user_interactions.handle_request_callback
handle_request_status_callback = _user_interactions.handle_request_status_callback
handle_requests_page_callback = _user_interactions.handle_requests_page_callback
handle_requests_view_callback = _user_interactions.handle_requests_view_callback
handle_request_cancel_callback = _user_interactions.handle_request_cancel_callback
handle_upload_request_status_callback = _user_interactions.handle_upload_request_status_callback
handle_upload_help_callback = _user_interactions.handle_upload_help_callback
favorites_command = _user_interactions.favorites_command
help_command = _user_interactions.help_command
request_command = _user_interactions.request_command
requests_command = _user_interactions.requests_command
myprofile_command = _user_interactions.myprofile_command
mystats_command = _user_interactions.mystats_command


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(MESSAGES[lang]["admin_only"])
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return

    if not context.args:
        await update.message.reply_text(MESSAGES[lang]["broadcast_usage"])
        return

    message = " ".join(context.args)

    users = await run_blocking(load_users)  # list of dicts from DB
    sent_count = 0
    blocked_users = []
    today = str(datetime.now().date())

    for user in users:
        user_id = user.get("id")
        if not user_id:
            continue
        try:
            await context.bot.send_message(chat_id=user_id, text=message)
            sent_count += 1
        except Exception as e:
            logger.debug(f"Failed to send to {user_id}: {e}")
            # ✅ If sending fails, assume user blocked the bot
            await run_blocking(update_user_left_date, user_id, datetime.now().date())
            blocked_users.append(user)

    if blocked_users:
        await run_blocking(insert_removed_users, blocked_users)

    await update.message.reply_text(
        MESSAGES[lang]["broadcast_done"].format(sent=sent_count, blocked=len(blocked_users))
    )

    

async def inlinequery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and is_blocked(update.effective_user.id):
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    query = update.inline_query.query.strip()
    results = []

    if not query:
        await update.inline_query.answer([], cache_time=0)
        return

    try:
        es = get_es()
        if not es:
            await update.inline_query.answer([], cache_time=0)
            return
        response = await run_blocking(
            lambda: es.search(
                index="books",
                size=10,
                query={
                    "multi_match": {
                        "query": query,
                        "fields": ["book_name^2", "path", "file_id"],
                        "fuzziness": "AUTO"
                    }
                }
            )
        )

        for hit in response["hits"]["hits"]:
            book = hit["_source"]
            book_id = hit["_id"]
            title = get_result_title(book)
            path = book.get("path")
            file_id = book.get("file_id")

            if file_id:
                result = InlineQueryResultCachedDocument(
                    id=book_id,
                    title=title,
                    document_file_id=file_id,
                    caption=f"📖 {title}"
                )
            elif path and os.path.exists(path):
                message = f"📖 *{title}*\n📂 Path:\n`{path}`"
                result = InlineQueryResultArticle(
                    id=book_id,
                    title=title,
                    input_message_content=InputTextMessageContent(
                        message, parse_mode="Markdown"
                    ),
                    description="Tap to upload book"
                )
            else:
                result = InlineQueryResultArticle(
                    id=book_id,
                    title=title,
                    input_message_content=InputTextMessageContent(
                        f"📖 *{title}*",
                        parse_mode="Markdown"
                    ),
                    description="Book info only"
                )

            results.append(result)

    except Exception as e:
        logger.error(f"⚠️ Elasticsearch error: {e}")
        await update.inline_query.answer([], cache_time=0)
        return

    await update.inline_query.answer(results, cache_time=0)



async def prune_blocked_users(context):
    users = await run_blocking(load_users)
    removed_users = []
    today = datetime.now().date()

    def _definitive_user_unreachable_error(exc: Exception) -> bool:
        # Only treat hard user-level errors as removable.
        if isinstance(exc, Forbidden):
            return True
        if isinstance(exc, BadRequest):
            msg = str(exc).lower()
            return ("chat not found" in msg) or ("user not found" in msg)
        return False

    for user in users:
        user_id = user.get("id")
        if not user_id:
            continue
        try:
            # test with a harmless ping message
            await context.bot.send_chat_action(chat_id=user_id, action="typing")
        except Exception as e:
            if _definitive_user_unreachable_error(e):
                logger.info(f"Prune: removing unreachable user {user_id}: {e}")
                await run_blocking(update_user_left_date, user_id, today)
                removed_users.append(user)
            elif isinstance(e, (NetworkError, TimedOut, RetryAfter)):
                # Transient transport/rate errors must never be treated as blocked users.
                logger.warning(f"Prune: transient error for user {user_id}, skipping: {e}")
            else:
                logger.warning(f"Prune: non-definitive error for user {user_id}, skipping: {e}")

    if removed_users:
        await run_blocking(delete_users_by_ids, [u.get("id") for u in removed_users if u.get("id")])
        await run_blocking(insert_removed_users, removed_users)

    logger.debug(f"✅ Pruned {len(removed_users)} blocked users.")
    return len(removed_users), max(0, len(users) - len(removed_users))


async def _prune_and_notify(context: ContextTypes.DEFAULT_TYPE, chat_id: int, lang: str):
    try:
        removed, active = await prune_blocked_users(context)
        await context.bot.send_message(
            chat_id=chat_id,
            text=MESSAGES[lang]["prune_done"].format(removed=removed, active=active),
        )
    except Exception as e:
        logger.error(f"Prune background task failed: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=chat_id, text=MESSAGES[lang]["error"])
        except Exception:
            pass


async def prune_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user.id != ADMIN_ID:
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return

    await target_message.reply_text(MESSAGES[lang]["prune_started"])
    chat_id = update.effective_chat.id if update.effective_chat else getattr(target_message, "chat_id", None)
    _schedule_application_task(context.application, _prune_and_notify(context, chat_id, lang))


async def missing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(MESSAGES[lang]["admin_only"])
        return
    args = context.args or []
    confirm = len(args) > 0 and args[0].lower() in {"confirm", "delete"}
    if not confirm:
        raw = (update.message.text or "").strip()
        if raw:
            tail = raw.split(maxsplit=1)[1:]  # everything after command
            if tail:
                first = tail[0].strip().split()[0].lower()
                if first in {"confirm", "delete"}:
                    confirm = True

    def reason_label(reason_code: str) -> str:
        if reason_code == "local_missing":
            return MESSAGES[lang]["missing_reason_local_missing"]
        return MESSAGES[lang]["missing_reason_no_file_id"]

    if confirm:
        expires_at = context.user_data.get("missing_confirm_until", 0)
        if not expires_at or time.time() > expires_at:
            await update.message.reply_text(MESSAGES[lang]["missing_confirm_expired"])
            return

        items = await run_blocking(get_missing_file_info, None)
        if not items:
            await update.message.reply_text(MESSAGES[lang]["missing_none"])
            return

        # Delete from DB
        delete_ids = {str(i["id"]) for i in items if i.get("id")}
        deleted_json = await run_blocking(delete_books_by_ids, list(delete_ids))

        # Delete from ES
        deleted_es = 0
        failed_es = 0
        if es_available():
            es = get_es()
            for book_id in delete_ids:
                try:
                    await run_blocking(lambda: es.delete(index=ES_INDEX, id=book_id))
                    deleted_es += 1
                except NotFoundError:
                    # already missing in ES
                    continue
                except Exception:
                    failed_es += 1

        context.user_data.pop("missing_confirm_until", None)
        await update.message.reply_text(
            MESSAGES[lang]["missing_deleted"].format(
                json=deleted_json, es=deleted_es, es_failed=failed_es
            )
        )
        return

    items = await run_blocking(get_missing_file_info, None)
    if not items:
        await update.message.reply_text(MESSAGES[lang]["missing_none"])
        return

    lines = [MESSAGES[lang]["missing_item"].format(title=i["title"], reason=reason_label(i["reason"])) for i in items]
    text = MESSAGES[lang]["missing_title"] + "\n" + "\n".join(lines)

    context.user_data["missing_confirm_until"] = time.time() + 60

    if len(text) <= 3900:
        await update.message.reply_text(text)
        await update.message.reply_text(MESSAGES[lang]["missing_confirm_prompt"])
    else:
        bio = io.BytesIO(text.encode("utf-8"))
        bio.name = "missing_books.txt"
        await update.message.reply_document(
            document=bio,
            caption=MESSAGES[lang]["missing_title"]
        )
        await update.message.reply_text(MESSAGES[lang]["missing_confirm_prompt"])


def _dup_key(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _dup_sort_key(item: dict):
    created = item.get("created_at")
    if isinstance(created, datetime):
        created_key = created.isoformat()
    else:
        created_key = str(created or "")
    return (
        1 if item.get("file_id") else 0,
        1 if item.get("file_unique_id") else 0,
        1 if item.get("path") else 0,
        1 if item.get("indexed") else 0,
        int(item.get("downloads") or 0) + int(item.get("searches") or 0),
        created_key,
        str(item.get("id") or ""),
    )


def _collect_duplicate_victims(items: list[dict], key_field: str):
    groups = {}
    for item in items:
        key = _dup_key(item.get(key_field))
        if not key:
            continue
        groups.setdefault(key, []).append(item)
    victims = []
    dup_groups = 0
    for rows in groups.values():
        if len(rows) < 2:
            continue
        dup_groups += 1
        keeper = max(rows, key=_dup_sort_key)
        keeper_id = str(keeper.get("id"))
        for row in rows:
            row_id = str(row.get("id"))
            if row_id != keeper_id:
                victims.append(row)
    return dup_groups, victims


def _collect_duplicate_groups(items: list[dict], key_field: str):
    groups = {}
    for item in items:
        key = _dup_key(item.get(key_field))
        if not key:
            continue
        groups.setdefault(key, []).append(item)
    dup_groups = 0
    grouped = []
    for rows in groups.values():
        if len(rows) < 2:
            continue
        dup_groups += 1
        keeper = max(rows, key=_dup_sort_key)
        keeper_id = str(keeper.get("id"))
        victims = [row for row in rows if str(row.get("id")) != keeper_id]
        if victims:
            grouped.append({"keeper": keeper, "victims": victims})
    return dup_groups, grouped


def _short_dupe_title(item: dict, max_len: int = 42) -> str:
    title = (
        str(item.get("display_name") or "").strip()
        or str(item.get("book_name") or "").strip()
        or "Untitled"
    )
    title = re.sub(r"\s+", " ", title)
    if len(title) > max_len:
        return title[: max_len - 1] + "…"
    return title


def _build_dupe_preview_lines(preview_pairs: list[dict], limit: int = 50) -> list[str]:
    lines = []
    shown = 0
    for pair in preview_pairs[: max(0, limit)]:
        shown += 1
        reason = pair.get("reason", "unknown")
        keeper = pair.get("keeper") or {}
        victim = pair.get("victim") or {}
        lines.append(
            f"{shown}. [{reason}] KEEP: {_short_dupe_title(keeper)} | DEL: {_short_dupe_title(victim)}"
        )
    remaining = max(0, len(preview_pairs) - shown)
    if remaining:
        lines.append(f"... and {remaining} more duplicate items")
    return lines


def _compute_db_duplicate_cleanup_plan():
    books = list(db_list_books() or [])
    kept = {str(b.get("id")): b for b in books if b.get("id")}
    delete_map: dict[str, dict] = {}
    preview_pairs: list[dict] = []
    stats = {
        "file_unique_groups": 0,
        "file_unique_deleted": 0,
        "path_groups": 0,
        "path_deleted": 0,
        "name_groups": 0,
        "name_deleted": 0,
        "total_before": len(kept),
    }

    for key_field, reason_label, grp_key, del_key in [
        ("file_unique_id", "file_unique_id", "file_unique_groups", "file_unique_deleted"),
        ("path", "path", "path_groups", "path_deleted"),
        ("book_name", "name", "name_groups", "name_deleted"),
    ]:
        current = list(kept.values())
        groups_count, groups = _collect_duplicate_groups(current, key_field)
        stats[grp_key] = groups_count
        for group in groups:
            keeper = group["keeper"]
            for victim in group["victims"]:
                vid = str(victim.get("id"))
                if vid in kept and vid not in delete_map:
                    delete_map[vid] = victim
                    preview_pairs.append({"reason": reason_label, "keeper": keeper, "victim": victim})
                    kept.pop(vid, None)
                    stats[del_key] += 1

    stats["total_delete"] = len(delete_map)
    stats["total_after"] = len(kept)
    return stats, list(delete_map.values()), preview_pairs


def _es_scan_docs_for_dupes():
    es = get_es()
    if not es:
        return []
    docs = []
    scroll_id = None
    try:
        res = es.search(
            index=ES_INDEX,
            query={"match_all": {}},
            size=1000,
            scroll="2m",
            sort=["_doc"],
            source_includes=["id", "book_name", "display_name", "file_id", "file_unique_id", "path", "indexed"],
        )
        scroll_id = res.get("_scroll_id")
        while True:
            hits = (res.get("hits") or {}).get("hits") or []
            if not hits:
                break
            for hit in hits:
                src = hit.get("_source") or {}
                src = dict(src)
                src.setdefault("id", hit.get("_id"))
                src["_es_id"] = hit.get("_id")
                docs.append(src)
            if not scroll_id:
                break
            res = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = res.get("_scroll_id") or scroll_id
        return docs
    finally:
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                pass


def _compute_es_duplicate_cleanup_plan():
    docs = _es_scan_docs_for_dupes()
    kept = {str(d.get("_es_id") or d.get("id")): d for d in docs if (d.get("_es_id") or d.get("id"))}
    delete_map: dict[str, dict] = {}
    preview_pairs: list[dict] = []
    stats = {
        "file_unique_groups": 0,
        "file_unique_deleted": 0,
        "path_groups": 0,
        "path_deleted": 0,
        "name_groups": 0,
        "name_deleted": 0,
        "total_before": len(kept),
    }

    for key_field, reason_label, grp_key, del_key in [
        ("file_unique_id", "file_unique_id", "file_unique_groups", "file_unique_deleted"),
        ("path", "path", "path_groups", "path_deleted"),
        ("book_name", "name", "name_groups", "name_deleted"),
    ]:
        current = []
        for item in kept.values():
            row = dict(item)
            row["id"] = str(item.get("_es_id") or item.get("id"))
            current.append(row)
        groups_count, groups = _collect_duplicate_groups(current, key_field)
        stats[grp_key] = groups_count
        for group in groups:
            keeper = group["keeper"]
            for victim in group["victims"]:
                vid = str(victim.get("id"))
                if vid in kept and vid not in delete_map:
                    delete_map[vid] = kept[vid]
                    preview_pairs.append({"reason": reason_label, "keeper": keeper, "victim": victim})
                    kept.pop(vid, None)
                    stats[del_key] += 1

    stats["total_delete"] = len(delete_map)
    stats["total_after"] = len(kept)
    return stats, list(delete_map.values()), preview_pairs


async def _send_progress_message(update: Update, text: str, **kwargs):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    try:
        return await target_message.reply_text(text, **kwargs)
    except RetryAfter as e:
        logger.warning("Progress message send hit flood wait (%ss): %s", getattr(e, "retry_after", 0), e)
        return None
    except Exception as e:
        logger.warning("Progress message send failed: %s", e)
        return None


async def _edit_progress_message(msg, text: str, **kwargs):
    if not msg:
        return False
    try:
        await msg.edit_text(text, **kwargs)
        return True
    except RetryAfter as e:
        logger.warning("Progress message edit hit flood wait (%ss): %s", getattr(e, "retry_after", 0), e)
        return False
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return False
        logger.warning("Progress message edit bad request: %s", e)
        return False
    except Exception as e:
        logger.warning("Progress message edit failed: %s", e)
        return False


async def _send_chat_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int | None, text: str):
    if not chat_id:
        return None
    try:
        return await context.bot.send_message(chat_id=chat_id, text=text)
    except RetryAfter as e:
        logger.warning("Chat message send hit flood wait (%ss): %s", getattr(e, "retry_after", 0), e)
        return None
    except Exception as e:
        logger.warning("Chat message send failed: %s", e)
        return None


async def _send_preview_pdf(update: Update, pdf_bytes: bytes, filename: str, caption: str, reply_markup=None):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    try:
        bio = io.BytesIO(pdf_bytes)
        bio.name = filename
        return await target_message.reply_document(document=bio, caption=caption, reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning("Preview PDF send hit flood wait (%ss): %s", getattr(e, "retry_after", 0), e)
        return None
    except Exception as e:
        logger.warning("Preview PDF send failed: %s", e)
        return None


def _dupes_status_store(app):
    return app.bot_data.setdefault("dupes_status", {"db": {}, "es": {}})


def _update_dupes_status(app, kind: str, **fields):
    store = _dupes_status_store(app)
    state = dict(store.get(kind) or {})
    state.update(fields)
    state["updated_at"] = time.time()
    store[kind] = state
    app.bot_data["dupes_status"] = store
    return state


def _get_dupes_status(app, kind: str) -> dict:
    store = _dupes_status_store(app)
    return dict(store.get(kind) or {})


def _format_dupes_status_line(kind: str, app) -> str:
    state = _get_dupes_status(app, kind)
    task = app.bot_data.get(_dupes_task_key(kind))
    running = bool(task and not task.done())
    if not state and not running:
        return f"{kind.upper()}: no recent activity"
    stage = state.get("stage", "idle")
    processed = int(state.get("processed", 0) or 0)
    total = int(state.get("total", 0) or 0)
    planned = int(state.get("planned_delete", 0) or 0)
    sent = state.get("final_message_sent")
    notify = "sent" if sent else ("pending/failed" if sent is False else "n/a")
    prefix = "running" if running else "idle"
    line = f"{kind.upper()}: {prefix} | stage={stage} | progress={processed}/{total} | planned={planned} | notify={notify}"
    if state.get("last_error"):
        line += f" | err={str(state.get('last_error'))[:80]}"
    return line


def _format_dupes_status_text(app) -> str:
    lines = [
        "Dupes status",
        "──────────",
        _format_dupes_status_line("db", app),
        _format_dupes_status_line("es", app),
    ]
    return "\n".join(lines)


# PDF Maker extracted module bridge
_pdf_maker_mod.configure(globals())

_pdf_maker_texts = _pdf_maker_mod._pdf_maker_texts
_PDF_MAKER_SESSION_KEY = _pdf_maker_mod._PDF_MAKER_SESSION_KEY
_PDF_MAKER_STYLE_KEYS = _pdf_maker_mod._PDF_MAKER_STYLE_KEYS
_PDF_MAKER_PAPER_KEYS = _pdf_maker_mod._PDF_MAKER_PAPER_KEYS
_PDF_MAKER_ORIENTATION_KEYS = _pdf_maker_mod._PDF_MAKER_ORIENTATION_KEYS
_pdf_maker_style_label = _pdf_maker_mod._pdf_maker_style_label
_pdf_maker_style_keyboard = _pdf_maker_mod._pdf_maker_style_keyboard
_pdf_maker_paper_label = _pdf_maker_mod._pdf_maker_paper_label
_pdf_maker_orientation_label = _pdf_maker_mod._pdf_maker_orientation_label
_pdf_maker_paper_keyboard = _pdf_maker_mod._pdf_maker_paper_keyboard
_pdf_maker_orientation_keyboard = _pdf_maker_mod._pdf_maker_orientation_keyboard
_pdf_maker_generate_confirm_keyboard = _pdf_maker_mod._pdf_maker_generate_confirm_keyboard
_pdf_maker_options_keyboard = _pdf_maker_mod._pdf_maker_options_keyboard
_pdf_maker_default_theme = _pdf_maker_mod._pdf_maker_default_theme
_pdf_maker_theme_from_ai = _pdf_maker_mod._pdf_maker_theme_from_ai
_pdf_maker_build_blocks = _pdf_maker_mod._pdf_maker_build_blocks
_pdf_wrap_by_width = _pdf_maker_mod._pdf_wrap_by_width
_build_modern_text_pdf_bytes = _pdf_maker_mod._build_modern_text_pdf_bytes
_build_text_only_pdf_bytes = _pdf_maker_mod._build_text_only_pdf_bytes
_pdf_maker_clear_session = _pdf_maker_mod._pdf_maker_clear_session
_pdf_maker_get_session = _pdf_maker_mod._pdf_maker_get_session
_pdf_maker_save_session = _pdf_maker_mod._pdf_maker_save_session
_pdf_maker_sanitize_name = _pdf_maker_mod._pdf_maker_sanitize_name
_pdf_maker_session_labels = _pdf_maker_mod._pdf_maker_session_labels
_pdf_maker_text_buffer_stats = _pdf_maker_mod._pdf_maker_text_buffer_stats
_pdf_maker_send_options_panel = _pdf_maker_mod._pdf_maker_send_options_panel
_pdf_maker_edit_or_send_prompt = _pdf_maker_mod._pdf_maker_edit_or_send_prompt
_pdf_maker_heuristic_auto_meta = _pdf_maker_mod._pdf_maker_heuristic_auto_meta
_pdf_maker_call_ollama_auto_meta = _pdf_maker_mod._pdf_maker_call_ollama_auto_meta
_pdf_maker_extract_subtitle = _pdf_maker_mod._pdf_maker_extract_subtitle
_pdf_maker_heuristic_body_font_size = _pdf_maker_mod._pdf_maker_heuristic_body_font_size
_pdf_maker_call_ollama_font_size = _pdf_maker_mod._pdf_maker_call_ollama_font_size
_pdf_maker_resolve_body_font_size = _pdf_maker_mod._pdf_maker_resolve_body_font_size
_pdf_maker_resolve_auto_theme = _pdf_maker_mod._pdf_maker_resolve_auto_theme
_pdf_maker_theme_for_selected_style = _pdf_maker_mod._pdf_maker_theme_for_selected_style
_reply_pdf_document = _pdf_maker_mod._reply_pdf_document
_pdf_maker_send_text_as_pdf = _pdf_maker_mod._pdf_maker_send_text_as_pdf
_pdf_maker_handle_text_input = _pdf_maker_mod._pdf_maker_handle_text_input
pdf_maker_command = _pdf_maker_mod.pdf_maker_command
_pdf_maker_start_session_from_message = _pdf_maker_mod._pdf_maker_start_session_from_message
handle_pdf_maker_callback = _pdf_maker_mod.handle_pdf_maker_callback


# TTS extracted module bridge
_tts_tools.configure(globals())

_TTS_SESSION_KEY = _tts_tools._TTS_SESSION_KEY
_TTS_LANG_KEYS = _tts_tools._TTS_LANG_KEYS
_TTS_SEX_KEYS = _tts_tools._TTS_SEX_KEYS
_TTS_TONE_BASE_KEYS = _tts_tools._TTS_TONE_BASE_KEYS
_TTS_SPEED_KEYS = _tts_tools._TTS_SPEED_KEYS
_TTS_OUTPUT_KEYS = _tts_tools._TTS_OUTPUT_KEYS
_tts_texts = _tts_tools._tts_texts
_tts_clear_session = _tts_tools._tts_clear_session
_tts_get_session = _tts_tools._tts_get_session
_tts_save_session = _tts_tools._tts_save_session
_tts_guess_lang_key = _tts_tools._tts_guess_lang_key
_tts_label = _tts_tools._tts_label
_tts_session_labels = _tts_tools._tts_session_labels
_tts_allowed_tones = _tts_tools._tts_allowed_tones
_tts_tools_available = _tts_tools._tts_tools_available
_tts_options_keyboard = _tts_tools._tts_options_keyboard
_TTS_WIZARD_STEPS = _tts_tools._TTS_WIZARD_STEPS
_tts_wizard_prev_phase = _tts_tools._tts_wizard_prev_phase
_tts_wizard_prompt_text = _tts_tools._tts_wizard_prompt_text
_tts_wizard_keyboard = _tts_tools._tts_wizard_keyboard
_tts_send_wizard_step = _tts_tools._tts_send_wizard_step
_tts_confirm_keyboard = _tts_tools._tts_confirm_keyboard
_tts_edit_or_send_prompt = _tts_tools._tts_edit_or_send_prompt
_tts_send_options_panel = _tts_tools._tts_send_options_panel
_tts_text_stats = _tts_tools._tts_text_stats
_tts_edge_voice_name = _tts_tools._tts_edge_voice_name
_tts_edge_rate = _tts_tools._tts_edge_rate
_tts_edge_pitch = _tts_tools._tts_edge_pitch
_tts_edge_volume = _tts_tools._tts_edge_volume
_tts_edge_save_mp3_async = _tts_tools._tts_edge_save_mp3_async
_tts_ollama_polish_text = _tts_tools._tts_ollama_polish_text
_tts_build_audio_bytes_blocking = _tts_tools._tts_build_audio_bytes_blocking
_tts_send_result = _tts_tools._tts_send_result
_tts_generate_and_send = _tts_tools._tts_generate_and_send
_tts_handle_text_input = _tts_tools._tts_handle_text_input
text_to_voice_command = _tts_tools.text_to_voice_command
_tts_start_session_from_message = _tts_tools._tts_start_session_from_message
handle_tts_callback = _tts_tools.handle_tts_callback


# AI tools extracted module bridge
_ai_tools.configure(
    messages=MESSAGES,
    logger_obj=logger,
    run_blocking_fn=run_blocking,
    run_blocking_heavy_fn=run_blocking_heavy,
    send_with_retry_fn=_send_with_retry,
    main_menu_keyboard_fn=_main_menu_keyboard,
    db_save_user_quiz_fn=db_save_user_quiz,
    db_get_user_quiz_fn=db_get_user_quiz,
    db_list_user_quizzes_fn=db_list_user_quizzes,
    db_count_user_quizzes_fn=db_count_user_quizzes,
    db_delete_user_quiz_fn=db_delete_user_quiz,
    db_mark_user_quiz_started_fn=db_mark_user_quiz_started,
    db_increment_user_quiz_share_count_fn=db_increment_user_quiz_share_count,
)

_AI_CHAT_SESSION_KEY = _ai_tools._AI_CHAT_SESSION_KEY
_ai_chat_texts = _ai_tools._ai_chat_texts
_ai_chat_clear_session = _ai_tools._ai_chat_clear_session
_ai_chat_get_session = _ai_tools._ai_chat_get_session
_ai_chat_save_session = _ai_tools._ai_chat_save_session
_ai_chat_trim_history = _ai_tools._ai_chat_trim_history
_ai_chat_build_prompt = _ai_tools._ai_chat_build_prompt
_ai_chat_user_asked_for_links = _ai_tools._ai_chat_user_asked_for_links
_ai_chat_postprocess_reply = _ai_tools._ai_chat_postprocess_reply
_ai_chat_guess_reply_lang = _ai_tools._ai_chat_guess_reply_lang
_ai_chat_needs_caution_notice = _ai_tools._ai_chat_needs_caution_notice
_ai_chat_add_caution_notice = _ai_tools._ai_chat_add_caution_notice
_AI_TOOL_MODE_SESSION_KEY = _ai_tools._AI_TOOL_MODE_SESSION_KEY
_AI_TOOL_MODE_KEYS = _ai_tools._AI_TOOL_MODE_KEYS
_ai_tool_mode_clear_session = _ai_tools._ai_tool_mode_clear_session
_ai_tool_mode_get_session = _ai_tools._ai_tool_mode_get_session
_ai_tool_mode_save_session = _ai_tools._ai_tool_mode_save_session
_ai_tool_mode_texts = _ai_tools._ai_tool_mode_texts
_ai_tool_mode_title = _ai_tools._ai_tool_mode_title
_ai_tool_mode_prompt = _ai_tools._ai_tool_mode_prompt
_ai_tool_lang_label = _ai_tools._ai_tool_lang_label
_ai_tool_parse_target_lang = _ai_tools._ai_tool_parse_target_lang
_ai_tool_parse_translation_langs = _ai_tools._ai_tool_parse_translation_langs
_ai_tool_guess_translation_source_lang = _ai_tools._ai_tool_guess_translation_source_lang
_ai_tool_translation_output_is_suspicious = _ai_tools._ai_tool_translation_output_is_suspicious
_ai_tools_ollama_generate_blocking = _ai_tools._ai_tools_ollama_generate_blocking
_ai_translator_backend = _ai_tools._ai_translator_backend
_ai_translator_nllb_lang_code = _ai_tools._ai_translator_nllb_lang_code
_ai_translator_get_nllb_bundle = _ai_tools._ai_translator_get_nllb_bundle
_ai_tool_translate_nllb_blocking = _ai_tools._ai_tool_translate_nllb_blocking
_ai_tool_translation_output_looks_bad = _ai_tools._ai_tool_translation_output_looks_bad
_ai_tool_translate_ollama_blocking = _ai_tools._ai_tool_translate_ollama_blocking
_ai_tool_translate_blocking = _ai_tools._ai_tool_translate_blocking
_ai_tool_translate_with_source_retry_blocking = _ai_tools._ai_tool_translate_with_source_retry_blocking
_ai_tool_grammar_fix_blocking = _ai_tools._ai_tool_grammar_fix_blocking
_ai_tool_email_writer_blocking = _ai_tools._ai_tool_email_writer_blocking
_ai_tool_mode_start_session_from_message = _ai_tools._ai_tool_mode_start_session_from_message
_ai_tool_mode_handle_text_input = _ai_tools._ai_tool_mode_handle_text_input
handle_ai_tools_callback = _ai_tools.handle_ai_tools_callback
handle_ai_quiz_poll_answer = _ai_tools.handle_ai_quiz_poll_answer
my_quiz_command = _ai_tools.my_quiz_command
handle_my_quiz_callback = _ai_tools.handle_my_quiz_callback
_ai_chat_owner_identity_reply = _ai_tools._ai_chat_owner_identity_reply
_ai_chat_capabilities_reply = _ai_tools._ai_chat_capabilities_reply
_ai_chat_feature_help_reply = _ai_tools._ai_chat_feature_help_reply
_ai_chat_admin_contact_reply = _ai_tools._ai_chat_admin_contact_reply
_ai_chat_builtin_reply = _ai_tools._ai_chat_builtin_reply
_ai_chat_ollama_reply_blocking = _ai_tools._ai_chat_ollama_reply_blocking
_ai_chat_start_session_from_message = _ai_tools._ai_chat_start_session_from_message
_ai_chat_handle_text_input = _ai_tools._ai_chat_handle_text_input
_AI_IMAGE_SESSION_KEY = _ai_tools._AI_IMAGE_SESSION_KEY
_ai_image_texts = _ai_tools._ai_image_texts
_ai_image_clear_session = _ai_tools._ai_image_clear_session
_ai_image_get_session = _ai_tools._ai_image_get_session
_ai_image_save_session = _ai_tools._ai_image_save_session
_ai_image_generate_local_sd_blocking = _ai_tools._ai_image_generate_local_sd_blocking
_ai_image_send_results = _ai_tools._ai_image_send_results
_ai_image_start_session_from_message = _ai_tools._ai_image_start_session_from_message
_ai_image_handle_text_input = _ai_tools._ai_image_handle_text_input


# Video downloader extracted module bridge
_video_downloader.configure(globals())

_video_dl_clear_session = _video_downloader._video_dl_clear_session
_video_dl_get_session = _video_downloader._video_dl_get_session
_video_dl_start_session_from_message = _video_downloader._video_dl_start_session_from_message
_video_dl_handle_text_input = _video_downloader._video_dl_handle_text_input
handle_video_downloader_callback = _video_downloader.handle_video_downloader_callback

# Refresh search_flow dependencies after late-bound feature bridges.
_search_flow.configure(globals())


# Admin runtime extracted module bridge
_admin_runtime.configure(globals())

_list_running_background_tasks = _admin_runtime._list_running_background_tasks
_background_tasks_keyboard = _admin_runtime._background_tasks_keyboard
_format_background_tasks_text = _admin_runtime._format_background_tasks_text
_admin_panel_snapshot_text = _admin_runtime._admin_panel_snapshot_text
_admin_panel_keyboard = _admin_runtime._admin_panel_keyboard
_admin_panel_send_or_edit = _admin_runtime._admin_panel_send_or_edit
_admin_panel_send_upload_local_status = _admin_runtime._admin_panel_send_upload_local_status
_admin_panel_send_missing_preview = _admin_runtime._admin_panel_send_missing_preview
admin_panel_command = _admin_runtime.admin_panel_command
handle_admin_panel_callback = _admin_runtime.handle_admin_panel_callback
smoke_check_command = _admin_runtime.smoke_check_command
cancel_task_command = _admin_runtime.cancel_task_command
handle_background_task_callback = _admin_runtime.handle_background_task_callback
_ensure_dupes_pdf_font = _admin_runtime._ensure_dupes_pdf_font
_build_dupes_preview_pdf = _admin_runtime._build_dupes_preview_pdf
_dupes_confirm_keyboard = _admin_runtime._dupes_confirm_keyboard
_format_dupes_preview = _admin_runtime._format_dupes_preview
_format_dupes_preview_caption = _admin_runtime._format_dupes_preview_caption
_format_db_dupes_summary = _admin_runtime._format_db_dupes_summary
_format_es_dupes_summary = _admin_runtime._format_es_dupes_summary
_run_db_dupes_cleanup_job = _admin_runtime._run_db_dupes_cleanup_job
_run_es_dupes_cleanup_job = _admin_runtime._run_es_dupes_cleanup_job
_dupes_task_key = _admin_runtime._dupes_task_key
_dupes_is_running = _admin_runtime._dupes_is_running
_start_dupes_cleanup_task = _admin_runtime._start_dupes_cleanup_task
db_dupes_command = _admin_runtime.db_dupes_command
es_dupes_command = _admin_runtime.es_dupes_command
dupes_status_command = _admin_runtime.dupes_status_command
handle_dupes_confirm_callback = _admin_runtime.handle_dupes_confirm_callback
user_search_command = _admin_runtime.user_search_command
_start_upload_local_books = _admin_runtime._start_upload_local_books
upload_local_books_command = _admin_runtime.upload_local_books_command

# Refresh search_flow again after admin runtime aliases (e.g. user_search_command).
_search_flow.configure(globals())


def main():
    if not _acquire_single_instance_lock():
        return
    try:
        if not TOKEN or not isinstance(TOKEN, str) or len(TOKEN) < 10:
            logger.error("Invalid or missing Telegram TOKEN in config.py")
        else:
            logger.debug("TOKEN loaded")

        # Initialize DB
        init_db()
        logger.debug("DB connected")
        try:
            db_backfill_counters_if_empty()
        except Exception as e:
            logger.error(f"Counter backfill failed: {e}", exc_info=True)

        # Startup status summary (minimal INFO)
        db_stats = get_db_stats()
        if db_stats.get("ok"):
            counts = db_stats.get("counts", {})
            users = counts.get("users", 0)
            books = counts.get("books", 0)
            indexed = counts.get("books_indexed", 0)
            logger.info(f"DB: up | users={users} books={books} indexed={indexed}")
        else:
            logger.error(f"DB: down | error={db_stats.get('error')}")
        logger.info(
            "Upload channels: books=%s audio=%s video=%s",
            ",".join(str(x) for x in (UPLOAD_CHANNEL_IDS or [])) or "none",
            AUDIO_UPLOAD_CHANNEL_ID,
            VIDEO_UPLOAD_CHANNEL_ID,
        )

        es_status = "down"
        es_health = "down"
        es_count = 0
        es = get_es()
        if es:
            try:
                es.info()
                ensure_index()
                ensure_movies_index()
                es_status = "up"
                es_health, es_count = get_es_health_summary(es)
            except Exception as e:
                logger.error(f"ES info failed: {e}")
                logger.warning("Bot will continue with limited search functionality (database search only)")
        else:
            logger.warning("Elasticsearch not available - bot will run with database search only")
        logger.info(f"ES: {es_status} | health={es_health} indexed={es_count}")

        builder = (
            ApplicationBuilder()
            .token(TOKEN)
            .post_init(post_init)
            .connect_timeout(20)
            .read_timeout(60)
            .write_timeout(1200)
            .pool_timeout(60)
        )

        bot_api_base_url = _normalize_bot_api_base_url(os.getenv("TELEGRAM_BOT_API_BASE_URL", ""))
        bot_api_base_file_url = _normalize_bot_api_base_file_url(
            os.getenv("TELEGRAM_BOT_API_BASE_FILE_URL", ""),
            bot_api_base_url,
        )
        bot_api_local_mode = _env_bool("TELEGRAM_BOT_API_LOCAL_MODE", False)

        if bot_api_base_url:
            builder = builder.base_url(bot_api_base_url)
        if bot_api_base_file_url:
            builder = builder.base_file_url(bot_api_base_file_url)
        if bot_api_local_mode:
            builder = builder.local_mode(True)

        if bot_api_base_url or bot_api_local_mode:
            logger.info(
                "Telegram Bot API: custom base=%s local_mode=%s",
                bot_api_base_url or "default",
                bot_api_local_mode,
            )

        app = builder.build()

        # Register handlers
        app.add_handler(MessageHandler(filters.ALL, paused_guard), group=-1)
        app.add_handler(CallbackQueryHandler(paused_callback_guard), group=-1)
        app.add_handler(MessageHandler(filters.ALL, _touch_user_activity_message), group=-1)
        app.add_handler(CallbackQueryHandler(_touch_user_activity_callback), group=-1)
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("upload", upload_command_wrapper))
        app.add_handler(CommandHandler("movie_upload", movie_upload_command))
        app.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_channel_post))
        app.add_handler(MessageHandler(filters.PHOTO, handle_photo_message))
        # audiobook audio parts (when admin is uploading)
        app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO | (filters.Document.ALL & filters.Document.MimeType("audio/")), handle_abook_audio))
        app.add_handler(MessageHandler(filters.VIDEO | (filters.Document.ALL & filters.Document.MimeType("video/")), handle_movie_video))
        app.add_handler(MessageHandler(filters.Document.ALL, handle_file))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_books))
        app.add_handler(CommandHandler("language", language_command_handler))
        app.add_handler(CommandHandler("ramazon", ramazon_command))
        app.add_handler(CommandHandler("pdf_maker", pdf_maker_command))
        app.add_handler(CommandHandler("text_to_voice", text_to_voice_command))
        app.add_handler(CallbackQueryHandler(handle_pdf_maker_callback, pattern="^pdfmk:"))
        app.add_handler(CallbackQueryHandler(handle_tts_callback, pattern="^tts:"))
        app.add_handler(CallbackQueryHandler(handle_video_downloader_callback, pattern="^vdl:"))
        app.add_handler(CallbackQueryHandler(handle_ai_tools_callback, pattern="^aitool:"))
        app.add_handler(CallbackQueryHandler(handle_my_quiz_callback, pattern="^myquiz:"))
        app.add_handler(PollAnswerHandler(handle_ai_quiz_poll_answer))
        app.add_handler(CallbackQueryHandler(handle_language_callback, pattern="^lang_"))
        app.add_handler(CallbackQueryHandler(handle_page_callback, pattern="^page:"))
        app.add_handler(CallbackQueryHandler(handle_movie_page_callback, pattern="^mpage:"))
        app.add_handler(CallbackQueryHandler(handle_user_page_callback, pattern="^userpage:"))
        app.add_handler(CallbackQueryHandler(handle_top_page_callback, pattern="^top:"))
        app.add_handler(CallbackQueryHandler(handle_top_users_toggle_callback, pattern="^topusers:"))
        app.add_handler(CallbackQueryHandler(handle_favorite_callback, pattern="^fav:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_listen_callback, pattern="^abook:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_page_callback, pattern="^abpage:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_part_play_callback, pattern="^abplay:"))
        app.add_handler(CallbackQueryHandler(handle_reaction_callback, pattern="^react:"))
        app.add_handler(CallbackQueryHandler(handle_summary_placeholder_callback, pattern="^summary:"))
        app.add_handler(CallbackQueryHandler(handle_admin_panel_callback, pattern="^adminp:"))
        app.add_handler(CallbackQueryHandler(handle_background_task_callback, pattern="^bgtask:"))
        app.add_handler(CallbackQueryHandler(handle_dupes_confirm_callback, pattern="^dupesop:"))
        app.add_handler(CallbackQueryHandler(handle_user_select_callback, pattern="^user:"))
        app.add_handler(CallbackQueryHandler(handle_user_action_callback, pattern="^uact:"))
        app.add_handler(CallbackQueryHandler(handle_request_callback, pattern="^request:"))
        app.add_handler(CallbackQueryHandler(handle_group_read_callback, pattern="^gread:"))
        app.add_handler(CommandHandler("broadcast", broadcast_command_wrapper))
        app.add_handler(CommandHandler("admin", admin_panel_command))
        app.add_handler(CommandHandler("smoke", smoke_command_wrapper))
        app.add_handler(CommandHandler("favorite", favorites_command))
        app.add_handler(CommandHandler("top", top_command))
        app.add_handler(CommandHandler("top_users", top_users_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("request", request_command))
        app.add_handler(CommandHandler("requests", requests_command_wrapper))
        app.add_handler(CommandHandler("my_quiz", my_quiz_command))
        app.add_handler(CommandHandler("mystats", mystats_command))
        app.add_handler(CommandHandler("myprofile", myprofile_command))
        app.add_handler(CommandHandler("group_read_start", group_read_start_command))
        app.add_handler(CommandHandler("group_read_status", group_read_status_command))
        app.add_handler(CommandHandler("group_read_end", group_read_end_command))
        app.add_handler(InlineQueryHandler(inlinequery))
        app.add_handler(CommandHandler("audit", audit_command))
        app.add_handler(CommandHandler("prune", prune_command))
        app.add_handler(CommandHandler("missing", missing_command))
        app.add_handler(CommandHandler("db_dupes", db_dupes_command))
        app.add_handler(CommandHandler("es_dupes", es_dupes_command))
        app.add_handler(CommandHandler("dupes_status", dupes_status_command))
        app.add_handler(CommandHandler("cancel_task", cancel_task_command))
        app.add_handler(CommandHandler("user", user_search_command))
        app.add_handler(CommandHandler("pause_bot", pause_bot_command))
        app.add_handler(CommandHandler("resume_bot", resume_bot_command))
        app.add_handler(CommandHandler("upload_local_books", upload_local_books_command))

        app.add_handler(CallbackQueryHandler(handle_request_status_callback, pattern="^reqstatus:"))
        app.add_handler(CallbackQueryHandler(handle_requests_page_callback, pattern="^reqpage:"))
        app.add_handler(CallbackQueryHandler(handle_requests_view_callback, pattern="^reqview:"))
        app.add_handler(CallbackQueryHandler(handle_request_cancel_callback, pattern="^reqcancel:"))
        app.add_handler(CallbackQueryHandler(handle_upload_help_callback, pattern="^upload_help_"))
        app.add_handler(CallbackQueryHandler(handle_upload_request_status_callback, pattern="^uploadreqstatus:"))
        app.add_handler(CallbackQueryHandler(handle_delete_book_callback, pattern="^delbook:"))

        # audiobook admin callbacks (user-facing audiobook handlers are registered above)
        app.add_handler(CallbackQueryHandler(handle_audiobook_part_delete_callback, pattern=r"^apdel:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_delete_by_book_callback, pattern=r"^abdelbook:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_delete_callback, pattern=r"^abdel:"))
        app.add_handler(CallbackQueryHandler(handle_audiobook_add_callback, pattern=r"^abadd:"))
        app.add_handler(CallbackQueryHandler(handle_movie_selection, pattern=r"^movie:[0-9a-fA-F-]{32,36}$"))
        # ✅ Book selection (only for book callbacks)
        app.add_handler(CallbackQueryHandler(handle_book_selection, pattern=r"^(book:)?[0-9a-fA-F-]{32,36}$"))

        # Error handler
        app.add_error_handler(handle_error)

        sync_unindexed_books()
        sync_unindexed_movies()

        logger.debug("Handlers registered. Starting polling...")
        print("Bot is running...")
        startup_retry_max = max(0, int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0") or "0"))
        bootstrap_retries = -1 if startup_retry_max == 0 else startup_retry_max
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            bootstrap_retries=bootstrap_retries,
        )
    except Exception as e:
        logger.error(f"App failed to start: {e}")
        raise
    finally:
        _release_single_instance_lock()


# ✅ Correct placement
if __name__ == "__main__":
    main()
