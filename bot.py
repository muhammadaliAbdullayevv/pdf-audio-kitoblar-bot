import logging
import warnings
import os
import json
import re
import traceback
import tempfile
import io
import shutil
import time
import math
import asyncio
import socket
import fcntl
import atexit
import html
from pathlib import Path
from urllib.parse import quote_plus, urlparse
from logging.handlers import RotatingFileHandler
from typing import Any
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime
from rapidfuzz import process, fuzz

from telegram import InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import InlineQueryHandler
import uuid
from telegram import InlineQueryResultCachedDocument
try:
    from telegram import InlineQueryResultCachedVideo
except Exception:
    InlineQueryResultCachedVideo = None  # type: ignore
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update, InputFile, Message
from book_thumbnail import get_book_thumbnail_input

from urllib3.exceptions import InsecureRequestWarning
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application,
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
    from pypdf import PdfReader
except Exception:
    PdfReader = None

from config import (
    TOKEN,
    OWNER_ID,
    REQUEST_CHAT_ID,
    BOOK_NEGATIVE_ALERT_THRESHOLD,
    BOOK_STORAGE_CHANNEL_ID,
    CONNECTED_BOT_TOKEN_ENCRYPTION_KEY,
    COIN_SEARCH,
    COIN_DOWNLOAD,
    COIN_REACTION,
    COIN_FAVORITE,
    COIN_REFERRAL,
    ENABLE_WHITE_LABEL,
    TOP_USERS_LIMIT,
    AUDIO_UPLOAD_CHANNEL_IDS,
    AUDIO_UPLOAD_CHANNEL_ID,
    validate_runtime_config,
    validate_white_label_config,
    WHITE_LABEL_CACHE_WAIT_SECONDS,
)


_PRIVATE_LIVE_STATUS_DRAFT_ID = 9001
_SEND_MESSAGE_DRAFT_SUPPORTED: bool | None = None
_NEGATIVE_REACTION_ALERT_THRESHOLD_SETTING_KEY = "book_negative_reaction_alert_threshold"
_PRIVATE_DRAFT_TIME_SETTING_KEYS = {
    "draft_searching": "private_draft_searching_min_seconds",
    "draft_results_ready": "private_draft_results_ready_min_seconds",
    "draft_preparing_book": "private_draft_preparing_book_min_seconds",
    "draft_sending_book": "private_draft_sending_book_min_seconds",
    "draft_search_total": "private_draft_search_total_min_seconds",
}
_PRIVATE_DRAFT_TIME_STAGE_ALIASES = {
    "search": "draft_searching",
    "searching": "draft_searching",
    "results": "draft_results_ready",
    "result": "draft_results_ready",
    "ready": "draft_results_ready",
    "prepare": "draft_preparing_book",
    "preparing": "draft_preparing_book",
    "book": "draft_preparing_book",
    "send": "draft_sending_book",
    "sending": "draft_sending_book",
    "total": "draft_search_total",
    "overall": "draft_search_total",
}
_PRIVATE_DRAFT_TIME_STAGE_LABELS = {
    "draft_searching": "search",
    "draft_results_ready": "results",
    "draft_preparing_book": "prepare",
    "draft_sending_book": "send",
    "draft_search_total": "total",
}
_PRIVATE_DRAFT_TIME_DEFAULTS = {
    "draft_searching": ("PRIVATE_DRAFT_SEARCHING_MIN_SECONDS", 0.75),
    "draft_results_ready": ("PRIVATE_DRAFT_RESULTS_READY_MIN_SECONDS", 0.45),
    "draft_preparing_book": ("PRIVATE_DRAFT_PREPARING_BOOK_MIN_SECONDS", 0.55),
    "draft_sending_book": ("PRIVATE_DRAFT_SENDING_BOOK_MIN_SECONDS", 0.45),
    "draft_search_total": ("PRIVATE_DRAFT_SEARCH_TOTAL_MIN_SECONDS", 1.6),
}
_PRIVATE_LIVE_STATUS_TIMINGS: dict[str, float] = {}
_PRIVATE_TRANSIENT_DRAFT_MIN_SECONDS = {
    "greeting": 1.25,
    "search_guide": 1.15,
}


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
    except Exception:
        return float(default)


def _clamp_private_draft_seconds(value: float) -> float:
    try:
        return round(min(5.0, max(0.0, float(value))), 2)
    except Exception:
        return 0.0


def _private_live_status_default_seconds(status_key: str) -> float:
    env_name, default_value = _PRIVATE_DRAFT_TIME_DEFAULTS.get(status_key, ("", 0.0))
    if not env_name:
        return 0.0
    return _clamp_private_draft_seconds(_env_float(env_name, default_value))


def _private_live_status_min_seconds(status_key: str) -> float:
    if status_key in _PRIVATE_LIVE_STATUS_TIMINGS:
        return _clamp_private_draft_seconds(_PRIVATE_LIVE_STATUS_TIMINGS.get(status_key, 0.0))
    return _private_live_status_default_seconds(status_key)


def _format_private_draft_seconds(value: float) -> str:
    text = f"{_clamp_private_draft_seconds(value):.2f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return f"{text}s"


def _load_private_live_status_settings() -> None:
    overrides: dict[str, float] = {}
    for status_key, setting_key in _PRIVATE_DRAFT_TIME_SETTING_KEYS.items():
        try:
            raw = db_get_bot_setting(setting_key)
        except Exception as e:
            logger.warning("Failed to load draft timing setting %s: %s", setting_key, e)
            continue
        if raw is None:
            continue
        try:
            overrides[status_key] = _clamp_private_draft_seconds(float(str(raw).strip()))
        except Exception:
            continue
    _PRIVATE_LIVE_STATUS_TIMINGS.clear()
    _PRIVATE_LIVE_STATUS_TIMINGS.update(overrides)


def _resolve_private_draft_stage(raw: str | None) -> str | None:
    key = str(raw or "").strip().lower()
    return _PRIVATE_DRAFT_TIME_STAGE_ALIASES.get(key)


def _set_private_live_status_timing(status_key: str, seconds: float) -> float:
    clamped = _clamp_private_draft_seconds(seconds)
    _PRIVATE_LIVE_STATUS_TIMINGS[status_key] = clamped
    db_set_bot_setting(_PRIVATE_DRAFT_TIME_SETTING_KEYS[status_key], str(clamped))
    return clamped


def _reset_private_live_status_timing(status_key: str) -> float:
    _PRIVATE_LIVE_STATUS_TIMINGS.pop(status_key, None)
    db_delete_bot_setting(_PRIVATE_DRAFT_TIME_SETTING_KEYS[status_key])
    return _private_live_status_default_seconds(status_key)


def _build_drafttime_report_text(lang: str) -> str:
    msgs = MESSAGES.get(lang, MESSAGES["en"])
    lines = [msgs.get("drafttime_title", "⏱ Draft timing"), msgs.get("drafttime_inactive", "Only the greeting message uses a fixed draft animation now. No adjustable draft timings are active.")]
    return "\n".join(lines).strip()


def _clamp_negative_reaction_alert_threshold(value: int) -> int:
    try:
        return max(1, min(1000, int(value)))
    except Exception:
        return max(1, int(BOOK_NEGATIVE_ALERT_THRESHOLD or 2))


def _default_negative_reaction_alert_threshold() -> int:
    return _clamp_negative_reaction_alert_threshold(int(BOOK_NEGATIVE_ALERT_THRESHOLD or 2))


def _get_negative_reaction_alert_threshold() -> int:
    try:
        raw = db_get_bot_setting(_NEGATIVE_REACTION_ALERT_THRESHOLD_SETTING_KEY)
    except Exception as e:
        logger.warning("Failed to load negative reaction alert threshold override: %s", e)
        raw = None
    if raw is None:
        return _default_negative_reaction_alert_threshold()
    try:
        return _clamp_negative_reaction_alert_threshold(int(str(raw).strip()))
    except Exception:
        return _default_negative_reaction_alert_threshold()


def _set_negative_reaction_alert_threshold(value: int) -> int:
    clamped = _clamp_negative_reaction_alert_threshold(value)
    db_set_bot_setting(_NEGATIVE_REACTION_ALERT_THRESHOLD_SETTING_KEY, str(clamped))
    return clamped


def _reset_negative_reaction_alert_threshold() -> int:
    db_delete_bot_setting(_NEGATIVE_REACTION_ALERT_THRESHOLD_SETTING_KEY)
    return _default_negative_reaction_alert_threshold()


def _build_negalert_report_text(lang: str) -> str:
    msgs = MESSAGES.get(lang, MESSAGES["en"])
    current = _get_negative_reaction_alert_threshold()
    fallback = _default_negative_reaction_alert_threshold()
    return msgs.get(
        "negalert_report",
        "👎 Negative reaction alert threshold\nCurrent: {current}\nDefault (.env): {default}\n\nUsage:\n/negalert\n/negalert 10\n/negalert reset",
    ).format(current=current, default=fallback)


def _private_live_status_text(lang: str, key: str) -> str:
    messages = MESSAGES.get(lang, MESSAGES["en"])
    fallback_keys = {
        "draft_searching": "processing_search",
        "draft_results_ready": "processing_search",
        "draft_preparing_book": "sending",
        "draft_sending_book": "sending",
    }
    fallback_key = fallback_keys.get(key, key)
    return str(
        messages.get(key)
        or messages.get(fallback_key)
        or MESSAGES["en"].get(key)
        or MESSAGES["en"].get(fallback_key)
        or ""
    ).strip()


def _private_book_status_text(lang: str) -> str:
    messages = MESSAGES.get(lang, MESSAGES["en"])
    return str(
        messages.get("private_book_sending_status")
        or messages.get("draft_sending_book")
        or messages.get("sending")
        or MESSAGES["en"].get("private_book_sending_status")
        or MESSAGES["en"].get("draft_sending_book")
        or MESSAGES["en"].get("sending")
        or ""
    ).strip()


def _plain_draft_text(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", str(text or ""))
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


async def push_private_live_status(
    bot,
    chat_id,
    lang: str,
    status_key: str,
    *,
    message_thread_id: int | None = None,
) -> bool:
    global _SEND_MESSAGE_DRAFT_SUPPORTED
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        return False
    if safe_chat_id <= 0:
        return False
    if _SEND_MESSAGE_DRAFT_SUPPORTED is False:
        return False
    text = _private_live_status_text(lang, status_key)
    if not text:
        return False
    payload: dict[str, Any] = {
        "chat_id": safe_chat_id,
        "draft_id": _PRIVATE_LIVE_STATUS_DRAFT_ID,
        "text": text,
    }
    try:
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)
    except Exception:
        pass
    try:
        await bot._post("sendMessageDraft", payload)
        _SEND_MESSAGE_DRAFT_SUPPORTED = True
        return True
    except BadRequest as e:
        error_text = str(e or "").lower()
        if "method not found" in error_text or "unknown method" in error_text:
            _SEND_MESSAGE_DRAFT_SUPPORTED = False
        logger.debug("sendMessageDraft rejected for chat %s: %s", safe_chat_id, e)
        return False
    except Exception as e:
        error_text = str(e or "").lower()
        if "method not found" in error_text or "unknown method" in error_text:
            _SEND_MESSAGE_DRAFT_SUPPORTED = False
        logger.debug("sendMessageDraft failed for chat %s: %s", safe_chat_id, e)
        return False


async def push_private_text_draft(
    bot,
    chat_id,
    text: str,
    *,
    parse_mode: str | None = None,
    message_thread_id: int | None = None,
) -> bool:
    global _SEND_MESSAGE_DRAFT_SUPPORTED
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        return False
    if safe_chat_id <= 0:
        return False
    if _SEND_MESSAGE_DRAFT_SUPPORTED is False:
        return False
    safe_text = str(text or "").strip()
    if not safe_text:
        return False
    payload: dict[str, Any] = {
        "chat_id": safe_chat_id,
        "draft_id": _PRIVATE_LIVE_STATUS_DRAFT_ID,
        "text": safe_text,
    }
    if parse_mode:
        payload["parse_mode"] = str(parse_mode)
    try:
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)
    except Exception:
        pass
    try:
        await bot._post("sendMessageDraft", payload)
        _SEND_MESSAGE_DRAFT_SUPPORTED = True
        return True
    except BadRequest as e:
        error_text = str(e or "").lower()
        if "method not found" in error_text or "unknown method" in error_text:
            _SEND_MESSAGE_DRAFT_SUPPORTED = False
        logger.debug("sendMessageDraft rejected for chat %s: %s", safe_chat_id, e)
        return False
    except Exception as e:
        error_text = str(e or "").lower()
        if "method not found" in error_text or "unknown method" in error_text:
            _SEND_MESSAGE_DRAFT_SUPPORTED = False
        logger.debug("sendMessageDraft failed for chat %s: %s", safe_chat_id, e)
        return False


async def hold_private_live_status(status_key: str, started_at: float | None) -> None:
    if started_at is None:
        return
    minimum_seconds = _private_live_status_min_seconds(status_key)
    if minimum_seconds <= 0:
        return
    elapsed = time.monotonic() - float(started_at)
    remaining = minimum_seconds - elapsed
    if remaining > 0:
        await asyncio.sleep(remaining)


async def hold_private_transient_draft(kind: str, started_at: float | None) -> None:
    if started_at is None:
        return
    minimum_seconds = float(_PRIVATE_TRANSIENT_DRAFT_MIN_SECONDS.get(str(kind or "").strip().lower(), 0.0) or 0.0)
    if minimum_seconds <= 0:
        return
    elapsed = time.monotonic() - float(started_at)
    remaining = minimum_seconds - elapsed
    if remaining > 0:
        await asyncio.sleep(remaining)


def _private_search_presentation_total_min_seconds() -> float:
    configured_floor = _private_live_status_min_seconds("draft_search_total")
    stage_total = (
        _private_live_status_min_seconds("draft_searching")
        + _private_live_status_min_seconds("draft_results_ready")
    )
    return max(configured_floor, stage_total)


async def hold_private_search_presentation(started_at: float | None) -> None:
    if started_at is None:
        return
    minimum_seconds = _private_search_presentation_total_min_seconds()
    if minimum_seconds <= 0:
        return
    elapsed = time.monotonic() - float(started_at)
    remaining = minimum_seconds - elapsed
    if remaining > 0:
        await asyncio.sleep(remaining)


async def send_private_book_status_message(
    bot,
    chat_id,
    lang: str,
    *,
    reply_to_message_id: int | None = None,
    message_thread_id: int | None = None,
):
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        return None
    if safe_chat_id <= 0:
        return None
    text = _private_book_status_text(lang)
    if not text:
        return None
    payload: dict[str, Any] = {
        "chat_id": safe_chat_id,
        "text": text,
    }
    try:
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = int(reply_to_message_id)
    except Exception:
        pass
    try:
        if message_thread_id is not None:
            payload["message_thread_id"] = int(message_thread_id)
    except Exception:
        pass
    try:
        return await bot.send_message(**payload)
    except Exception:
        return None


async def hold_private_book_status_message(started_at: float | None) -> None:
    if started_at is None:
        return
    minimum_seconds = max(1.0, _private_live_status_min_seconds("draft_sending_book"))
    elapsed = time.monotonic() - float(started_at)
    remaining = minimum_seconds - elapsed
    if remaining > 0:
        await asyncio.sleep(remaining)

from db import (
    init_db,
    get_user,
    list_users,
    upsert_user,
    update_user_language,
    update_user_group_language,
    update_user_left_date,
    set_user_allowed,
    set_user_audio_allowed,
    set_user_delete_allowed,
    set_user_rename_allowed,
    set_user_stopped,
    set_user_blocked,
    add_user_coin_adjustment as db_add_user_coin_adjustment,
    get_user_coin_adjustment as db_get_user_coin_adjustment,
    delete_users_by_ids,
    insert_removed_users,
    insert_admin_task_run as db_insert_admin_task_run,
    update_admin_task_run as db_update_admin_task_run,
    list_admin_task_runs as db_list_admin_task_runs,
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
    ping_db as db_ping,
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
    upsert_guest_group as db_upsert_guest_group,
    get_guest_group_audit_stats as db_get_guest_group_audit_stats,
    get_guest_group_delivery_capability as db_get_guest_group_delivery_capability,
    mark_guest_group_delivery_forbidden as db_mark_guest_group_delivery_forbidden,
    mark_guest_group_delivery_success as db_mark_guest_group_delivery_success,
    record_guest_user_activity as db_record_guest_user_activity,
    get_guest_user_audit_stats as db_get_guest_user_audit_stats,
    record_inline_search_activity as db_record_inline_search_activity,
    record_inline_chosen_activity as db_record_inline_chosen_activity,
    get_inline_audit_stats as db_get_inline_audit_stats,
    list_books as db_list_books,
    get_random_books as db_get_random_books,
    get_random_book as db_get_random_book,
    get_book_by_id as db_get_book_by_id,
    get_book_summary as db_get_book_summary,
    get_book_by_path as db_get_book_by_path,
    get_book_by_name as db_get_book_by_name,
    get_book_by_file_unique_id as db_get_book_by_file_unique_id,
    find_duplicate_book as db_find_duplicate_book,
    get_duplicate_counts_file_unique_id as db_get_duplicate_counts_file_unique_id,
    get_duplicate_counts_path as db_get_duplicate_counts_path,
    get_duplicate_counts_name as db_get_duplicate_counts_name,
    get_book_storage_counts as db_get_book_storage_counts,
    get_audio_book_stats as db_get_audio_book_stats,
    get_storage_stats as db_get_storage_stats,
    get_book_local_download_job_status_counts as db_get_book_local_download_job_status_counts,
    get_audio_book_part_local_download_job_status_counts as db_get_audio_book_part_local_download_job_status_counts,
    increment_book_download as db_increment_book_download,
    increment_book_searches as db_increment_book_searches,
    set_book_reaction as db_set_book_reaction,
    set_book_reaction_display_counts as db_set_book_reaction_display_counts,
    seed_all_book_display_stats_randomly as db_seed_all_book_display_stats_randomly,
    clear_all_book_display_adjustments as db_clear_all_book_display_adjustments,
    get_book_reaction_counts as db_get_book_reaction_counts,
    get_book_negative_reaction_alert_state as db_get_book_negative_reaction_alert_state,
    mark_book_negative_reaction_alert_sent as db_mark_book_negative_reaction_alert_sent,
    clear_book_negative_reaction_alert as db_clear_book_negative_reaction_alert,
    get_book_delivery_snapshot as db_get_book_delivery_snapshot,
    get_book_stats as db_get_book_stats,
    get_top_books as db_get_top_books,
    get_top_users as db_get_top_users,
    insert_book as db_insert_book,
    bulk_upsert_books,
    update_book_file_id,
    update_book_indexed,
    update_book_rename_meta as db_update_book_rename_meta,
    update_book_path as db_update_book_path,
    update_book_by_path,
    enqueue_book_local_download_job as db_enqueue_book_local_download_job,
    claim_book_local_download_job as db_claim_book_local_download_job,
    complete_book_local_download_job as db_complete_book_local_download_job,
    retry_book_local_download_job as db_retry_book_local_download_job,
    fail_book_local_download_job as db_fail_book_local_download_job,
    get_audio_book_for_book,
    list_audio_books_by_book_id,
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
    get_upload_receipt_by_id as db_get_upload_receipt_by_id,
    update_upload_receipt as db_update_upload_receipt,
    update_book_upload_meta as db_update_book_upload_meta,
    set_bot_setting as db_set_bot_setting,
    get_bot_setting as db_get_bot_setting,
    delete_bot_setting as db_delete_bot_setting,
    upsert_forbidden_books as db_upsert_forbidden_books,
    get_forbidden_book_title as db_get_forbidden_book_title,
    list_forbidden_book_titles as db_list_forbidden_book_titles,
    list_forbidden_books as db_list_forbidden_books,
    remove_forbidden_books as db_remove_forbidden_books,
    get_book_reaction_policy as db_get_book_reaction_policy,
    set_book_reaction_policy as db_set_book_reaction_policy,
    get_book_comment_count as db_get_book_comment_count,
    get_book_comment_thread_count as db_get_book_comment_thread_count,
    get_user_book_comment_summary as db_get_user_book_comment_summary,
    list_user_book_comments as db_list_user_book_comments,
    update_book_comment_text as db_update_book_comment_text,
    get_book_comment_by_id as db_get_book_comment_by_id,
    add_book_comment as db_add_book_comment,
    list_book_comment_threads as db_list_book_comment_threads,
    list_book_comment_replies as db_list_book_comment_replies,
    list_book_comment_thread_messages as db_list_book_comment_thread_messages,
    viewer_can_see_book_comment_identity as db_viewer_can_see_book_comment_identity,
    create_book_comment_identity_request as db_create_book_comment_identity_request,
    resolve_book_comment_identity_request as db_resolve_book_comment_identity_request,
    create_book_comment_report as db_create_book_comment_report,
    delete_book_comment as db_delete_book_comment,
    is_book_comment_banned as db_is_book_comment_banned,
    set_book_comment_ban as db_set_book_comment_ban,
    clear_book_comment_ban as db_clear_book_comment_ban,
    is_book_comment_peer_blocked as db_is_book_comment_peer_blocked,
    set_book_comment_peer_block as db_set_book_comment_peer_block,
    clear_book_comment_peer_block as db_clear_book_comment_peer_block,
    get_or_create_book_comment_relay_conversation as db_get_or_create_book_comment_relay_conversation,
    get_book_comment_relay_conversation as db_get_book_comment_relay_conversation,
    close_book_comment_relay_conversation as db_close_book_comment_relay_conversation,
    acknowledge_book_comment_relay_closure as db_acknowledge_book_comment_relay_closure,
    get_book_comment_relay_participant_state as db_get_book_comment_relay_participant_state,
    touch_book_comment_relay_last_seen as db_touch_book_comment_relay_last_seen,
    set_book_comment_relay_muted as db_set_book_comment_relay_muted,
    get_book_comment_relay_unread_summary as db_get_book_comment_relay_unread_summary,
    list_book_comment_relay_conversations_for_user as db_list_book_comment_relay_conversations_for_user,
    count_book_comment_relay_conversations_for_user as db_count_book_comment_relay_conversations_for_user,
    list_book_comment_relay_messages_for_user as db_list_book_comment_relay_messages_for_user,
    create_book_comment_relay_message as db_create_book_comment_relay_message,
    get_book_comment_relay_message as db_get_book_comment_relay_message,
    enqueue_background_job as db_enqueue_background_job,
    create_background_job as db_create_background_job,
    deserialize_background_job_payload as db_deserialize_background_job_payload,
    claim_background_job as db_claim_background_job,
    complete_background_job as db_complete_background_job,
    update_background_job_progress as db_update_background_job_progress,
    retry_background_job as db_retry_background_job,
    fail_background_job as db_fail_background_job,
    recover_stale_background_jobs as db_recover_stale_background_jobs,
    get_background_job_status_counts as db_get_background_job_status_counts,
    get_background_job_status_counts_by_type as db_get_background_job_status_counts_by_type,
    get_background_job_admin_summary as db_get_background_job_admin_summary,
    upsert_book_summary as db_upsert_book_summary,
    search_users_by_name as db_search_users_by_name,
    is_user_delete_allowed as db_is_user_delete_allowed,
    is_user_audio_allowed as db_is_user_audio_allowed,
    is_user_rename_allowed as db_is_user_rename_allowed,
    is_user_stopped as db_is_user_stopped,
    set_user_referrer as db_set_user_referrer,
    upsert_group_private_start_prompt as db_upsert_group_private_start_prompt,
    get_group_private_start_prompt_by_token as db_get_group_private_start_prompt_by_token,
    get_latest_pending_group_private_start_prompt as db_get_latest_pending_group_private_start_prompt,
    set_group_private_start_prompt_status as db_set_group_private_start_prompt_status,
    create_guest_private_handoff as db_create_guest_private_handoff,
    get_guest_private_handoff_by_token as db_get_guest_private_handoff_by_token,
    touch_guest_private_handoff as db_touch_guest_private_handoff,
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

import search_flow as _search_flow
from jobs import (
    job_dir as _job_workspace_dir,
    job_temp_environment as _job_temp_environment,
    mark_job_failed as _mark_job_failed_workspace,
    clear_job_failed_marker as _clear_job_failed_workspace,
    prune_job_dirs as _prune_job_dirs,
)
from workers.handlers import process_background_job as _process_background_job_payload

logger = logging.getLogger(__name__)

# Explicit bridge deps: DB symbols are consumed indirectly by extracted modules
# via module configure(...) dependency injection. This keeps IDE static analysis
# from dimming them as unused.
_BRIDGE_DB_SYMBOLS = (
    init_db,
    get_user,
    list_users,
    upsert_user,
    update_user_language,
    update_user_left_date,
    set_user_allowed,
    set_user_delete_allowed,
    set_user_rename_allowed,
    set_user_stopped,
    set_user_blocked,
    db_add_user_coin_adjustment,
    db_get_user_coin_adjustment,
    delete_users_by_ids,
    insert_removed_users,
    db_insert_admin_task_run,
    db_update_admin_task_run,
    db_list_admin_task_runs,
    db_add_favorite,
    db_remove_favorite,
    db_is_favorited,
    db_list_favorites,
    db_award_favorite_action,
    db_get_user_favorite_awards_count,
    db_add_recent,
    db_increment_analytics,
    db_increment_user_analytics,
    db_increment_counter,
    db_get_counters,
    db_backfill_counters_if_empty,
    db_backfill_user_awards_if_empty,
    get_analytics_map,
    get_db_stats,
    db_get_book_totals,
    db_get_favorites_total,
    db_get_user_favorites_count,
    db_get_user_usage_stats,
    db_get_user_reaction_count,
    db_get_user_referrals_count,
    db_get_request_status_counts,
    db_get_upload_request_status_counts,
    db_get_user_status_counts,
    db_get_reaction_totals,
    db_get_user_reaction,
    db_award_reaction_action,
    db_get_user_reaction_awards_count,
    db_get_daily_analytics,
    db_get_user_daily_counts,
    db_list_books,
    db_get_book_by_id,
    db_get_book_summary,
    db_get_book_by_path,
    db_get_book_by_name,
    db_get_book_by_file_unique_id,
    db_find_duplicate_book,
    db_get_duplicate_counts_file_unique_id,
    db_get_duplicate_counts_path,
    db_get_duplicate_counts_name,
    db_get_book_storage_counts,
    db_get_audio_book_stats,
    db_get_storage_stats,
    db_increment_book_download,
    db_increment_book_searches,
    db_set_book_reaction,
    db_get_book_delivery_snapshot,
    db_get_book_stats,
    db_get_top_books,
    db_get_top_users,
    db_insert_book,
    bulk_upsert_books,
    update_book_file_id,
    update_book_indexed,
    db_update_book_path,
    db_update_book_rename_meta,
    update_book_by_path,
    db_enqueue_book_local_download_job,
    db_claim_book_local_download_job,
    db_complete_book_local_download_job,
    db_retry_book_local_download_job,
    db_fail_book_local_download_job,
    db_get_book_local_download_job_status_counts,
    db_get_audio_book_part_local_download_job_status_counts,
    get_audio_book_for_book,
    list_audio_books_by_book_id,
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
    db_list_requests,
    db_list_requests_for_user,
    db_get_request_by_id,
    db_insert_request,
    db_update_request,
    db_delete_request,
    db_set_request_status,
    db_list_upload_requests,
    db_get_upload_request_by_id,
    db_insert_upload_request,
    db_update_upload_request,
    db_set_upload_request_status,
    db_insert_upload_receipt,
    db_update_upload_receipt,
    db_update_book_upload_meta,
    db_enqueue_background_job,
    db_claim_background_job,
    db_complete_background_job,
    db_retry_background_job,
    db_fail_background_job,
    db_get_background_job_status_counts,
    db_get_background_job_status_counts_by_type,
    db_upsert_book_summary,
    db_search_users_by_name,
    db_is_user_delete_allowed,
    db_is_user_audio_allowed,
    db_is_user_stopped,
    db_set_user_referrer,
)

_SEARCH_FLOW_DEP_KEYS = (
    "MESSAGES",
    "AUDIT_CACHE_TTL",
    "BOOK_STORAGE_CHANNEL_ID",
    "AUDIO_UPLOAD_CHANNEL_IDS",
    "AUDIO_UPLOAD_CHANNEL_ID",
    "ApplicationHandlerStop",
    "DB_RETRY_ATTEMPTS",
    "DB_RETRY_BASE_DELAY_SEC",
    "LOCAL_SEND_BACKOFF_SEC",
    "LOCAL_SEND_RETRIES",
    "MAX_SEARCH_RESULTS",
    "PAGE_SIZE",
    "SEARCH_CACHE_NS",
    "SEARCH_COOLDOWN_SEC",
    "TOP_CACHE_TTL",
    "_admin_tools_handle_admin_menu_prompt_input",
    "_book_filename",
    "_cancel_menu_conflicting_flows",
    "_handle_main_menu_action",
    "_is_admin_user",
    "_is_owner_user",
    "_main_menu_keyboard",
    "_main_menu_text_action",
    "_parse_seedbookstats_ranges",
    "_reply_search_menu_click_hint",
    "_today_str",
    "process_pending_white_label_owner_input",
    "add_recent_download",
    "broadcast",
    "build_book_caption",
    "build_book_keyboard",
    "_create_guest_private_handoff_start",
    "build_guest_private_handoff_reply_markup",
    "build_upload_admin_keyboard",
    "can_delete_books",
    "apply_book_rename",
    "count_pending_audiobook_requests",
    "is_allowed",
    "can_rename_books",
    "db_add_user_coin_adjustment",
    "db_add_book_comment",
    "db_get_book_by_id",
    "db_get_book_comment_by_id",
    "db_get_book_comment_count",
    "db_get_book_comment_thread_count",
    "db_get_user_book_comment_summary",
    "db_update_book_comment_text",
    "db_get_book_delivery_snapshot",
    "db_get_book_reaction_policy",
    "db_get_bot_setting",
    "db_delete_bot_setting",
    "db_get_forbidden_book_title",
    "db_list_forbidden_book_titles",
    "db_list_forbidden_books",
    "db_get_book_stats",
    "db_get_guest_group_delivery_capability",
    "db_get_guest_private_handoff_by_token",
    "db_get_user_reaction",
    "db_is_book_comment_banned",
    "db_set_book_comment_ban",
    "db_clear_book_comment_ban",
    "db_is_book_comment_peer_blocked",
    "db_set_book_comment_peer_block",
    "db_clear_book_comment_peer_block",
    "db_get_book_comment_relay_participant_state",
    "db_touch_book_comment_relay_last_seen",
    "db_set_book_comment_relay_muted",
    "db_get_book_comment_relay_unread_summary",
    "db_list_book_comment_relay_conversations_for_user",
    "db_count_book_comment_relay_conversations_for_user",
    "db_list_book_comment_relay_messages_for_user",
    "db_increment_book_download",
    "db_increment_book_searches",
    "db_increment_counter",
    "db_insert_book",
    "db_list_book_comment_replies",
    "db_list_book_comment_thread_messages",
    "db_list_book_comment_threads",
    "db_list_user_book_comments",
    "db_list_requests",
    "db_seed_all_book_display_stats_randomly",
    "db_create_book_comment_identity_request",
    "db_create_book_comment_report",
    "db_create_book_comment_relay_message",
    "db_delete_book_comment",
    "db_close_book_comment_relay_conversation",
    "db_acknowledge_book_comment_relay_closure",
    "db_get_book_comment_relay_conversation",
    "db_get_book_comment_relay_message",
    "db_get_or_create_book_comment_relay_conversation",
    "db_resolve_book_comment_identity_request",
    "db_set_book_reaction_policy",
    "db_set_book_reaction_display_counts",
    "db_remove_forbidden_books",
    "db_set_bot_setting",
    "db_upsert_forbidden_books",
    "db_viewer_can_see_book_comment_identity",
    "db_enqueue_book_local_download_job",
    "db_claim_book_local_download_job",
    "db_complete_book_local_download_job",
    "db_retry_book_local_download_job",
    "db_fail_book_local_download_job",
    "db_get_book_local_download_job_status_counts",
    "db_mark_guest_group_delivery_forbidden",
    "db_mark_guest_group_delivery_success",
    "ensure_user_language",
    "es_available",
    "format_upload_request_admin_text",
    "format_user_name",
    "format_user_tag",
    "find_book_by_id",
    "get_admin_id",
    "get_request_target_id",
    "get_display_name",
    "get_audio_book_by_id",
    "list_audio_books_by_book_id",
    "get_es",
    "get_result_title",
    "get_user",
    "increment_analytics",
    "increment_user_analytics",
    "index_book",
    "is_blocked",
    "is_favorited",
    "is_audio_allowed",
    "is_allowed",
    "is_user_rename_allowed",
    "is_stopped_user",
    "load_books",
    "load_requests",
    "mark_request_fulfilled",
    "normalize",
    "rate_limited",
    "run_blocking",
    "run_blocking_db_retry",
    "safe_answer",
    "safe_reply",
    "hold_private_live_status",
    "hold_private_book_status_message",
    "hold_private_search_presentation",
    "push_private_live_status",
    "push_private_text_draft",
    "send_private_book_status_message",
    "_schedule_application_task",
    "search_es",
    "send_request_to_admin",
    "set_user_allowed",
    "set_user_audio_allowed",
    "spam_check_callback",
    "spam_check_message",
    "suggest_books",
    "update_request_status",
    "update_book_file_id",
    "update_upload_request_status",
    "update_user_info",
    "user_search_command",
)

_USER_INTERACTIONS_DEP_KEYS = (
    "COIN_REFERRAL",
    "InlineKeyboardButton",
    "InlineKeyboardMarkup",
    "MESSAGES",
    "_build_help_text",
    "_is_admin_user",
    "build_results_keyboard",
    "build_results_text",
    "cache_search_results",
    "build_referral_link",
    "build_simple_book_keyboard",
    "compute_coin_breakdown",
    "db_increment_book_searches",
    "db_get_random_books",
    "db_get_random_book",
    "db_get_user_coin_adjustment",
    "db_get_user_favorite_awards_count",
    "db_get_user_favorites_count",
    "db_get_user_reaction_awards_count",
    "db_get_user_reaction_count",
    "db_get_user_referrals_count",
    "db_get_user_usage_stats",
    "db_increment_counter",
    "db_list_favorites",
    "db_list_requests_for_user",
    "db_get_request_by_id",
    "db_delete_request",
    "ensure_user_language",
    "get_result_title",
    "get_request_by_id",
    "get_upload_request_by_id",
    "is_blocked",
    "is_stopped_user",
    "math",
    "quote_plus",
    "REQUESTS_PAGE_SIZE",
    "run_blocking",
    "safe_answer",
    "send_request_to_admin",
    "build_request_admin_keyboard",
    "format_request_admin_text",
    "build_requests_keyboard",
    "refresh_requests_list",
    "send_upload_request_to_admin",
    "spam_check_callback",
    "spam_check_message",
    "time",
    "update_request_status",
    "update_user_info",
)


def _build_search_flow_deps() -> dict[str, object]:
    # search_flow is configured multiple times as late-bound aliases become available.
    deps: dict[str, object] = {}
    for key in _SEARCH_FLOW_DEP_KEYS:
        if key in globals():
            deps[key] = globals()[key]
    return deps


def _build_user_interactions_deps() -> dict[str, object]:
    deps: dict[str, object] = {}
    missing: list[str] = []
    for key in _USER_INTERACTIONS_DEP_KEYS:
        if key not in globals():
            missing.append(key)
            continue
        deps[key] = globals()[key]
    if missing:
        raise RuntimeError(f"Missing user_interactions dependencies: {', '.join(missing)}")
    return deps


def _build_bridge_deps(
    required_keys: tuple[str, ...],
    optional_keys: tuple[str, ...],
    label: str,
) -> dict[str, object]:
    deps: dict[str, object] = {}
    missing: list[str] = []
    for key in required_keys:
        if key not in globals():
            missing.append(key)
            continue
        deps[key] = globals()[key]
    for key in optional_keys:
        if key in globals():
            deps[key] = globals()[key]
    if missing:
        raise RuntimeError(f"Missing {label} dependencies: {', '.join(missing)}")
    return deps


_ENGAGEMENT_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "run_blocking",
    "ensure_user_language",
    "spam_check_callback",
    "safe_answer",
)

_ENGAGEMENT_OPTIONAL_DEP_KEYS = (
    "BadRequest",
    "COIN_DOWNLOAD",
    "COIN_FAVORITE",
    "COIN_REACTION",
    "COIN_REFERRAL",
    "COIN_SEARCH",
    "ES_INDEX",
    "InlineKeyboardButton",
    "InlineKeyboardMarkup",
    "NotFoundError",
    "PdfReader",
    "REACTION_EMOJI",
    "RetryAfter",
    "SEARCH_CACHE_NS",
    "TOP_USERS_CACHE_TTL",
    "TOP_USERS_LIMIT",
    "_is_admin_user",
    "_is_owner_user",
    "_create_guest_private_handoff_start",
    "_get_negative_reaction_alert_threshold",
    "_send_with_retry",
    "add_favorite",
    "asyncio",
    "build_book_caption",
    "build_book_keyboard",
    "BOOK_NEGATIVE_ALERT_THRESHOLD",
    "build_top_keyboard",
    "build_top_text",
    "build_top_users_keyboard",
    "build_top_users_text",
    "build_user_admin_keyboard",
    "build_user_info_text",
    "cache_clear_pattern",
    "cache_delete",
    "cache_get",
    "cache_set",
    "cache_top_results",
    "can_delete_books",
    "can_rename_books",
    "count_pending_audiobook_requests",
    "db_award_favorite_action",
    "db_award_reaction_action",
    "db_get_book_by_id",
    "db_get_book_negative_reaction_alert_state",
    "db_get_book_reaction_counts",
    "db_get_book_stats",
    "db_get_book_reaction_policy",
    "db_get_book_summary",
    "db_get_top_books",
    "db_get_top_users",
    "db_get_user_reaction",
    "db_increment_counter",
    "db_mark_book_negative_reaction_alert_sent",
    "db_set_book_reaction",
    "db_set_book_reaction_policy",
    "db_upsert_book_summary",
    "db_clear_book_negative_reaction_alert",
    "delete_audio_books_by_book_id",
    "delete_book_and_related",
    "es_available",
    "format_user_name",
    "get_audio_book_for_book",
    "get_cached_top_entries",
    "get_display_name",
    "get_es",
    "get_result_title",
    "get_request_target_id",
    "get_top_cache",
    "get_user",
    "hashlib",
    "is_blocked",
    "is_favorited",
    "is_audio_allowed",
    "is_stopped_user",
    "list_audio_book_parts",
    "list_audio_books_by_book_id",
    "remove_favorite",
    "set_cached_top_entries",
    "set_user_allowed",
    "set_user_audio_allowed",
    "set_user_blocked",
    "set_user_delete_allowed",
    "set_user_rename_allowed",
    "set_user_stopped",
    "socket",
    "spam_check_message",
    "push_private_live_status",
    "push_private_text_draft",
    "hold_private_live_status",
    "hold_private_search_presentation",
    "update_user_info",
    "urllib",
)

_ADMIN_RUNTIME_OPTIONAL_DEP_KEYS = (
    "A4",
    "ContextTypes",
    "ES_INDEX",
    "InlineKeyboardButton",
    "InlineKeyboardMarkup",
    "InputFile",
    "MESSAGES",
    "NetworkError",
    "NotFoundError",
    "RetryAfter",
    "TTFont",
    "TimedOut",
    "UPLOAD_LOCAL_CONNECT_TIMEOUT",
    "UPLOAD_LOCAL_LARGE_CONCURRENCY",
    "UPLOAD_LOCAL_LARGE_MB",
    "UPLOAD_LOCAL_MAX_MB",
    "UPLOAD_LOCAL_POOL_TIMEOUT",
    "UPLOAD_LOCAL_READ_TIMEOUT",
    "UPLOAD_LOCAL_WORKERS",
    "UPLOAD_LOCAL_WRITE_TIMEOUT",
    "USER_SEARCH_LIMIT",
    "Update",
    "_book_filename",
    "_build_dupe_preview_lines",
    "_compute_db_duplicate_cleanup_plan",
    "_compute_es_duplicate_cleanup_plan",
    "_edit_progress_message",
    "_format_dupes_status_text",
    "_get_dupes_status",
    "_is_admin_user",
    "_send_chat_message",
    "_send_main_menu",
    "_send_preview_pdf",
    "_send_progress_message",
    "_update_dupes_status",
    "asyncio",
    "audit_command",
    "guest_audit_command",
    "inline_audit_command",
    "build_user_results_keyboard",
    "build_user_results_text",
    "cache_user_results",
    "canvas",
    "datetime",
    "db_insert_admin_task_run",
    "db_list_admin_task_runs",
    "db_list_books",
    "db_get_audio_book_part_local_download_job_status_counts",
    "db_get_background_job_admin_summary",
    "db_get_background_job_status_counts",
    "db_get_background_job_status_counts_by_type",
    "db_get_book_local_download_job_status_counts",
    "db_search_users_by_name",
    "db_update_book_path",
    "db_update_admin_task_run",
    "delete_book_and_related",
    "ensure_user_language",
    "es_available",
    "fuzz",
    "get_es",
    "get_missing_file_info",
    "io",
    "is_bot_paused",
    "latinize_text",
    "list_users",
    "normalize",
    "os",
    "pause_bot_command",
    "pdfmetrics",
    "prune_command",
    "re",
    "resume_bot_command",
    "run_blocking",
    "safe_answer",
    "spam_check_message",
    "time",
    "update_book_file_id",
    "update_user_info",
)


def _build_engagement_handlers_deps() -> dict[str, object]:
    return _build_bridge_deps(
        _ENGAGEMENT_REQUIRED_DEP_KEYS,
        _ENGAGEMENT_OPTIONAL_DEP_KEYS,
        "engagement_handlers",
    )


def _build_admin_runtime_deps() -> dict[str, object]:
    required = tuple(getattr(_admin_runtime, "_CONFIG_REQUIRED_KEYS", ()) or ())
    return _build_bridge_deps(required, _ADMIN_RUNTIME_OPTIONAL_DEP_KEYS, "admin_runtime")

# Tiny runtime use to keep static analyzers from flagging this tuple itself as dead.
_BRIDGE_DB_SYMBOLS_COUNT = len(_BRIDGE_DB_SYMBOLS)

# Import cache for performance
try:
    from cache import cache_result, cache_get, cache_set, cache_delete, cache_clear_pattern, get_redis_client
except ImportError:
    logger.warning("Cache module not available")
    cache_result = lambda *args, **kwargs: lambda f: f  # No-op decorator
    cache_get = lambda k: None
    cache_set = lambda k, v, ttl=300: False
    cache_delete = lambda k: False
    cache_clear_pattern = lambda p: 0
    get_redis_client = lambda: None
import engagement_handlers as _engagement_handlers
import admin_runtime as _admin_runtime
import user_interactions as _user_interactions
import upload_flow as _upload_flow
import command_sync as _command_sync
import handler_registry as _handler_registry
from white_label import (
    WL_PLAN_TRIAL,
    WL_REQUEST_STATUS_PENDING,
    WL_STATUS_ACTIVE,
    WL_STATUS_ERROR,
    WL_STATUS_SUSPENDED,
)
from white_label.cache_seeding import seed_connected_bot_cache
from white_label.commands import (
    build_connected_bot_list_text,
    describe_token_for_owner,
    format_connected_bot_reference,
    normalize_connected_bot_identifier,
)
from white_label.crypto import (
    decrypt_bot_token,
    encrypt_bot_token,
    fingerprint_bot_token,
    is_crypto_available as wl_crypto_available,
    redact_token_like_strings as wl_redact_token_like_strings,
)
from white_label.db_helpers import (
    accept_connected_bot_request as db_accept_connected_bot_request,
    count_connected_bot_requests as db_count_connected_bot_requests,
    count_connected_bots as db_count_connected_bots,
    create_connected_bot_cache_seed_job as db_create_connected_bot_cache_seed_job,
    create_connected_bot_request as db_create_connected_bot_request,
    delete_connected_bot as db_delete_connected_bot,
    find_existing_connected_bot_request_or_bot as db_find_existing_connected_bot_request_or_bot,
    get_connected_bot_by_id as db_get_connected_bot_by_id,
    get_connected_bot_by_identifier as db_get_connected_bot_by_identifier,
    get_connected_bot_cache_seed_job_by_token as db_get_connected_bot_cache_seed_job_by_token,
    get_connected_bot_file_cache as db_get_connected_bot_file_cache,
    get_connected_bot_request_by_id as db_get_connected_bot_request_by_id,
    get_connected_bot_usage as db_get_connected_bot_usage,
    list_connected_bot_requests as db_list_connected_bot_requests,
    list_connected_bots as db_list_connected_bots,
    list_connected_bots_page as db_list_connected_bots_page,
    record_connected_bot_verification as db_record_connected_bot_verification,
    reject_connected_bot_request as db_reject_connected_bot_request,
    update_connected_bot_cache_channel as db_update_connected_bot_cache_channel,
    update_connected_bot_status as db_update_connected_bot_status,
    upsert_connected_bot as db_upsert_connected_bot,
)
from white_label.runtime_utils import build_bot_client as wl_build_bot_client
from white_label.runtime_control import (
    format_runtime_status as wl_format_runtime_status,
    get_connected_bot_runtime_status as wl_get_runtime_status,
    start_connected_bot_runtime as wl_start_runtime,
    stop_connected_bot_runtime as wl_stop_runtime,
)


async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


# Admin-only command wrappers
async def admin_only_command(update: Update, context: ContextTypes.DEFAULT_TYPE, command_func):
    """Wrapper to ensure only owner-level commands can execute."""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id or not _is_admin_user(user_id):
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang]["admin_only"])
        return
    await command_func(update, context)


async def owner_only_command(update: Update, context: ContextTypes.DEFAULT_TYPE, command_func):
    """Wrapper for owner-only commands."""
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id or not _is_owner_user(user_id):
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang].get("owner_only", MESSAGES[lang]["admin_only"]))
        return
    await command_func(update, context)


async def upload_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Upload permission is controlled by DB "allowed" and owner check inside upload flow.
    lang = ensure_user_language(update, context)
    chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
    if chat_type in {"group", "supergroup"}:
        await safe_reply(update, MESSAGES[lang].get("group_upload_not_needed", MESSAGES["en"]["group_upload_not_needed"]))
        return
    await upload_command(update, context)


async def broadcast_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_only_command(update, context, broadcast)


async def requests_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await requests_command(update, context)


async def smoke_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_only_command(update, context, smoke_check_command)


async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not update.effective_chat or not update.effective_user:
        return
    if not _is_admin_user(update.effective_user.id):
        await safe_reply(update, MESSAGES[lang]["admin_only"])
        return
    chat_id = update.effective_chat.id
    chat_type = getattr(update.effective_chat, "type", "-")
    chat_title = getattr(update.effective_chat, "title", None) or "-"
    user_id = update.effective_user.id
    text = MESSAGES[lang]["chat_id_info"].format(
        chat_id=chat_id,
        chat_type=chat_type,
        user_id=user_id,
    )
    text = f"{text}\n📌 Title: {chat_title}"
    admin_id = get_admin_id()
    if admin_id and admin_id != chat_id:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text)
            if update.effective_chat.type in {"group", "supergroup", "channel"}:
                return
        except Exception:
            pass
    await safe_reply(update, text)


def _build_contact_admin_reply_markup(lang: str, user_id: int | None = None):
    m = MESSAGES[lang]
    owner_username = str(BOT_OWNER_USERNAME or "").strip()
    owner_handle = owner_username[1:] if owner_username.startswith("@") else owner_username
    if owner_handle:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton(m.get("contact_admin_button", "👤 Message Admin"), url=f"https://t.me/{owner_handle}")]]
        )
    return _main_menu_keyboard(lang, "main", user_id)


async def contact_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    m = MESSAGES[lang]
    info_text = m.get(
        "contact_admin_info",
        "📞 Contact Admin\n\nIf you have a question, suggestion, or problem, you can contact the bot owner directly.",
    )
    user_id = update.effective_user.id if update.effective_user else None
    reply_markup = _build_contact_admin_reply_markup(lang, user_id)
    await safe_reply(update, info_text, reply_markup=reply_markup)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await language_command_handler(update, context)


def _my_comments_excerpt(text: str, limit: int = 180) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "").strip())
    if len(cleaned) > limit:
        cleaned = cleaned[: max(1, limit - 3)].rstrip() + "..."
    return html.escape(cleaned)


async def my_comments_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _search_flow.send_my_comments_panel(update, context)


async def my_chats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _search_flow.send_my_chats_panel(update, context)


def _bot_public_handle() -> str:
    handle = str(BOT_PUBLIC_USERNAME or "").strip()
    if not handle:
        return "@pdf_audio_kitoblar_bot"
    return handle if handle.startswith("@") else f"@{handle}"


def _bot_public_url() -> str:
    handle = _bot_public_handle().lstrip("@")
    return f"https://t.me/{handle}" if handle else "https://t.me/pdf_audio_kitoblar_bot"


def _parse_guest_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[Message | None, dict[str, Any] | None]:
    raw = getattr(update, "api_kwargs", {}).get("guest_message")
    if not isinstance(raw, dict):
        return None, None
    try:
        return Message.de_json(raw, context.bot), raw
    except Exception:
        logger.warning("Failed to parse guest_message update", exc_info=True)
        return None, raw


def _guest_message_language(guest_message: Message | None, raw: dict[str, Any] | None) -> str:
    lang_code = None
    try:
        lang_code = getattr(getattr(guest_message, "from_user", None), "language_code", None)
    except Exception:
        lang_code = None
    if not lang_code and isinstance(raw, dict):
        caller = raw.get("guest_bot_caller_user")
        if isinstance(caller, dict):
            lang_code = caller.get("language_code")
    return detect_language_code(lang_code)


def _guest_chat_info(guest_message: Message | None, raw: dict[str, Any] | None) -> dict[str, Any]:
    chat = getattr(guest_message, "chat", None)
    chat_id = getattr(chat, "id", None) if chat else None
    chat_type = getattr(chat, "type", None) if chat else None
    title = getattr(chat, "title", None) if chat else None
    username = getattr(chat, "username", None) if chat else None

    if isinstance(raw, dict):
        raw_chat = raw.get("chat")
        if isinstance(raw_chat, dict):
            if chat_id is None:
                chat_id = raw_chat.get("id")
            if chat_type is None:
                chat_type = raw_chat.get("type")
            if title is None:
                title = raw_chat.get("title")
            if username is None:
                username = raw_chat.get("username")

    safe_username = str(username or "").strip().lstrip("@")
    public_link = f"https://t.me/{safe_username}" if safe_username else ""
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        safe_chat_id = 0
    return {
        "chat_id": safe_chat_id,
        "chat_type": str(chat_type or "").strip().lower(),
        "title": str(title or "").strip(),
        "username": safe_username,
        "public_link": public_link,
    }


def _invalidate_audit_report_cache(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        clear_pattern = globals().get("cache_clear_pattern")
        if callable(clear_pattern):
            clear_pattern("audit:report:*")
    except Exception:
        logger.debug("Failed to clear audit report cache", exc_info=True)
    try:
        context.application.bot_data.pop("audit_cache", None)
    except Exception:
        pass


def _extract_guest_query_text(text: str, bot_username: str) -> tuple[str | None, str]:
    raw = str(text or "").strip()
    if not raw:
        return None, ""
    first_token = raw.split(None, 1)[0]
    command_match = re.match(r"^/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?$", first_token)
    if command_match:
        cmd = str(command_match.group(1) or "").lower()
        target = str(command_match.group(2) or "").lower()
        if not target or target == bot_username.lower():
            rest = raw[len(first_token):].strip()
            return cmd, rest
    mention_pattern = re.compile(rf"@{re.escape(bot_username)}\b", re.IGNORECASE)
    stripped = mention_pattern.sub(" ", raw)
    stripped = re.sub(r"\s+", " ", stripped).strip(" ,:-")
    return None, stripped


def _search_guest_books(query: str, limit: int = 5) -> list[dict[str, str]]:
    cleaned_query = normalize(query).lower()
    if not cleaned_query:
        return []
    variants: list[str] = []
    for candidate in (cleaned_query, cleaned_query.replace("ʻ", "")):
        candidate = str(candidate or "").strip()
        if candidate and candidate not in variants:
            variants.append(candidate)

    results: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for variant in variants:
        for source, _score, book_id in search_es(variant, size=max(limit, MAX_SEARCH_RESULTS)):
            bid = str(book_id or source.get("id") or "").strip()
            if not bid or bid in seen_ids:
                continue
            title = (
                str(source.get("display_name") or "").strip()
                or str(source.get("book_name") or "").strip()
                or bid
            )
            results.append({"id": bid, "title": title})
            seen_ids.add(bid)
            if len(results) >= limit:
                return results

    books = load_books()
    suggestions = suggest_books(books, variants[0], limit=limit)
    if not suggestions and len(variants) > 1:
        suggestions = suggest_books(books, variants[1], limit=limit)
    for item in suggestions:
        bid = str(item.get("id") or "").strip()
        if not bid or bid in seen_ids:
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        results.append({"id": bid, "title": title})
        seen_ids.add(bid)
        if len(results) >= limit:
            break
    return results


async def _answer_guest_query(
    context: ContextTypes.DEFAULT_TYPE,
    guest_query_id: str,
    text: str,
    *,
    title: str | None = None,
    description: str | None = None,
    reply_markup: dict[str, Any] | None = None,
) -> None:
    message_text = str(text or "").strip() or "Open the bot in a private chat."
    result_title = str(title or message_text.splitlines()[0].strip() or "Message from bot").strip()
    result_description = str(description or re.sub(r"\s+", " ", message_text).strip()).strip()
    result = {
        "type": "article",
        "id": uuid.uuid4().hex[:32],
        "title": result_title[:64],
        "description": result_description[:128],
        "input_message_content": {
            "message_text": message_text[:4096],
        },
    }
    if reply_markup:
        result["reply_markup"] = reply_markup
    payload = {
        "guest_query_id": guest_query_id,
        "result": json.dumps(result, ensure_ascii=False),
    }
    await context.bot._post("answerGuestQuery", payload)


def _guest_open_button_text(lang: str) -> str:
    _ = lang
    return "📥 Botni ochish"


def _guest_open_bot_inline_keyboard(lang: str, bot_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(_guest_open_button_text(lang), url=bot_url)]]
    )


def _guest_open_bot_markup(lang: str, bot_url: str) -> dict[str, Any]:
    return _guest_open_bot_inline_keyboard(lang, bot_url).to_dict()


def _guest_results_inline_keyboard(
    entries: list[dict[str, str]],
    lang: str,
    bot_url: str,
    *,
    query_token: str | None = None,
    open_url: str | None = None,
) -> InlineKeyboardMarkup:
    number_row: list[dict[str, str]] = []
    for idx, entry in enumerate(entries, start=1):
        book_id = str(entry.get("id") or "").strip()
        if not book_id:
            continue
        callback_data = f"gbook:{book_id}:{query_token}" if query_token else f"book:{book_id}"
        number_row.append({"text": str(idx), "callback_data": callback_data})
    rows: list[list[InlineKeyboardButton]] = []
    if number_row:
        rows.append([InlineKeyboardButton(button["text"], callback_data=button["callback_data"]) for button in number_row])
    rows.append([InlineKeyboardButton(_guest_open_button_text(lang), url=open_url or bot_url)])
    return InlineKeyboardMarkup(rows)


def _guest_results_markup(
    entries: list[dict[str, str]],
    lang: str,
    bot_url: str,
    *,
    query_token: str | None = None,
    open_url: str | None = None,
) -> dict[str, Any]:
    return _guest_results_inline_keyboard(
        entries,
        lang,
        bot_url,
        query_token=query_token,
        open_url=open_url,
    ).to_dict()


def parse_guest_private_handoff_payload(payload: str | None) -> str | None:
    if not payload:
        return None
    token = str(payload or "").strip()
    if not token.startswith("gh_"):
        return None
    handoff_token = token[len("gh_") :].strip()
    return handoff_token or None


async def _create_guest_private_handoff_start(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    handoff_type: str,
    creator_user_id: int | None = None,
    query_text: str | None = None,
    book_id: str | None = None,
    source_chat_id: int | None = None,
    source_chat_type: str | None = None,
    source_chat_title: str | None = None,
    source_chat_username: str | None = None,
    source_public_link: str | None = None,
) -> tuple[str | None, str | None]:
    try:
        record = await run_blocking_db_retry(
            db_create_guest_private_handoff,
            handoff_type,
            creator_user_id,
            query_text,
            book_id,
            source_chat_id,
            source_chat_type,
            source_chat_title,
            source_chat_username,
            source_public_link,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
    except Exception as e:
        logger.warning("Failed to create guest private handoff: %s", e, exc_info=True)
        return None, None
    token = str((record or {}).get("token") or "").strip()
    if not token:
        return None, None
    start_url = await _build_private_start_url(context, payload=f"gh_{token}")
    return start_url, token


def _guest_private_handoff_context_text(lang: str, handoff_type: str) -> str:
    key = "guest_private_handoff_book_context" if str(handoff_type or "").strip().lower() == "book" else "guest_private_handoff_query_context"
    return MESSAGES.get(lang, MESSAGES["en"]).get(
        key,
        MESSAGES["en"].get(key, "🔓 Opened from guest search."),
    )


async def build_guest_private_handoff_reply_markup(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    handoff_token: str,
    selected_book_id: str,
    actor_user_id: int | None,
    lang: str,
) -> InlineKeyboardMarkup | None:
    try:
        handoff = await run_blocking_db_retry(
            db_get_guest_private_handoff_by_token,
            handoff_token,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
    except Exception as e:
        logger.warning("Failed to load guest private handoff %s: %s", handoff_token, e, exc_info=True)
        return None
    query_text = str((handoff or {}).get("query_text") or "").strip()
    if not query_text:
        return None
    try:
        entries = await run_blocking(_search_guest_books, query_text, 5)
    except Exception as e:
        logger.warning("Failed to rebuild guest search results for handoff %s: %s", handoff_token, e, exc_info=True)
        return None
    rows: list[list[InlineKeyboardButton]] = []
    number_row: list[InlineKeyboardButton] = []
    query_open_url = await _build_private_start_url(context, payload=f"gh_{handoff_token}")
    fallback_query_url = query_open_url or _bot_public_url()
    for idx, entry in enumerate(entries, start=1):
        entry_book_id = str(entry.get("id") or "").strip()
        if not entry_book_id:
            continue
        entry_open_url, _ = await _create_guest_private_handoff_start(
            context,
            handoff_type="book",
            creator_user_id=actor_user_id,
            query_text=query_text,
            book_id=entry_book_id,
            source_chat_id=(handoff or {}).get("source_chat_id"),
            source_chat_type=(handoff or {}).get("source_chat_type"),
            source_chat_title=(handoff or {}).get("source_chat_title"),
            source_chat_username=(handoff or {}).get("source_chat_username"),
            source_public_link=(handoff or {}).get("source_public_link"),
        )
        number_row.append(
            InlineKeyboardButton(
                str(idx),
                url=entry_open_url or fallback_query_url,
            )
        )
    if number_row:
        rows.append(number_row)
    rows.append([InlineKeyboardButton(_guest_open_button_text(lang), url=fallback_query_url)])
    return InlineKeyboardMarkup(rows)


async def handle_guest_message_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    guest_message, raw = _parse_guest_message(update, context)
    if not guest_message:
        return

    guest_query_id = str(
        (raw or {}).get("guest_query_id")
        or getattr(guest_message, "api_kwargs", {}).get("guest_query_id")
        or ""
    ).strip()
    if not guest_query_id:
        return

    lang = _guest_message_language(guest_message, raw)
    guest_chat = _guest_chat_info(guest_message, raw)
    text = str(getattr(guest_message, "text", None) or getattr(guest_message, "caption", None) or "").strip()
    bot_handle = _bot_public_handle()
    bot_url = _bot_public_url()
    bot_username = bot_handle.lstrip("@")

    if guest_chat.get("chat_id") and guest_chat.get("chat_type") in {"group", "supergroup"}:
        try:
            await run_blocking(
                db_upsert_guest_group,
                int(guest_chat["chat_id"]),
                guest_chat.get("chat_type"),
                guest_chat.get("title"),
                guest_chat.get("username"),
                guest_chat.get("public_link"),
                None,
                False,
            )
            _invalidate_audit_report_cache(context)
        except Exception as e:
            logger.warning("Failed to record guest group presence: %s", e)

    try:
        command_name, query_text = _extract_guest_query_text(text, bot_username)

        if command_name in {"start", "help"} or (not command_name and not query_text):
            body = MESSAGES[lang].get("guest_help", MESSAGES["en"]["guest_help"]).format(bot=bot_handle)
            await _answer_guest_query(
                context,
                guest_query_id,
                body,
                title=MESSAGES[lang].get("guest_help_title", MESSAGES["en"].get("guest_help_title", "Guest mode")),
                description=MESSAGES[lang].get("guest_help_description", MESSAGES["en"].get("guest_help_description", "Open the bot for full search and downloads")),
                reply_markup=_guest_open_bot_markup(lang, bot_url),
            )
            return

        if command_name in {"settings", "language"}:
            body = MESSAGES[lang].get("guest_settings", MESSAGES["en"]["guest_settings"]).format(bot=bot_handle)
            await _answer_guest_query(
                context,
                guest_query_id,
                body,
                title=MESSAGES[lang].get("guest_settings_title", MESSAGES["en"].get("guest_settings_title", "Settings")),
                description=MESSAGES[lang].get("guest_settings_description", MESSAGES["en"].get("guest_settings_description", "Open the bot in private chat")),
                reply_markup=_guest_open_bot_markup(lang, bot_url),
            )
            return

        if command_name == "request":
            body = (
                MESSAGES[lang].get("guest_open_private_hint", MESSAGES["en"]["guest_open_private_hint"]).format(bot=bot_handle)
            )
            await _answer_guest_query(
                context,
                guest_query_id,
                body,
                title=MESSAGES[lang].get("guest_request_title", MESSAGES["en"].get("guest_request_title", "Open the bot")),
                description=MESSAGES[lang].get("guest_request_description", MESSAGES["en"].get("guest_request_description", "Requests work in private chat")),
                reply_markup=_guest_open_bot_markup(lang, bot_url),
            )
            return

        effective_query = query_text if query_text else text
        effective_query = str(effective_query or "").strip()
        if not effective_query:
            body = MESSAGES[lang].get("guest_help", MESSAGES["en"]["guest_help"]).format(bot=bot_handle)
            await _answer_guest_query(
                context,
                guest_query_id,
                body,
                title=MESSAGES[lang].get("guest_help_title", MESSAGES["en"].get("guest_help_title", "Guest mode")),
                description=MESSAGES[lang].get("guest_help_description", MESSAGES["en"].get("guest_help_description", "Open the bot for full search and downloads")),
                reply_markup=_guest_open_bot_markup(lang, bot_url),
            )
            return

        try:
            await run_blocking(db_increment_counter, "guest_search_total", 1)
            if guest_chat.get("chat_id") and guest_chat.get("chat_type") in {"group", "supergroup"}:
                await run_blocking(
                    db_upsert_guest_group,
                    int(guest_chat["chat_id"]),
                    guest_chat.get("chat_type"),
                    guest_chat.get("title"),
                    guest_chat.get("username"),
                    guest_chat.get("public_link"),
                    effective_query,
                    True,
                )
            await run_blocking(
                db_record_guest_user_activity,
                getattr(guest_message.from_user, "id", None),
                getattr(guest_message.from_user, "username", None),
                getattr(guest_message.from_user, "first_name", None),
                getattr(guest_message.from_user, "last_name", None),
                guest_chat.get("chat_id"),
                guest_chat.get("chat_type"),
                guest_chat.get("title"),
                guest_chat.get("username"),
                guest_chat.get("public_link"),
                effective_query,
            )
            _invalidate_audit_report_cache(context)
        except Exception as e:
            logger.warning("Failed to record guest search stats: %s", e)

        entries = await run_blocking(_search_guest_books, effective_query, 5)
        if not entries:
            not_found_url, _ = await _create_guest_private_handoff_start(
                context,
                handoff_type="query",
                creator_user_id=getattr(guest_message.from_user, "id", None),
                query_text=effective_query,
                source_chat_id=guest_chat.get("chat_id"),
                source_chat_type=guest_chat.get("chat_type"),
                source_chat_title=guest_chat.get("title"),
                source_chat_username=guest_chat.get("username"),
                source_public_link=guest_chat.get("public_link"),
            )
            body = MESSAGES[lang].get("guest_not_found", MESSAGES["en"]["guest_not_found"]).format(
                query=effective_query,
                bot=bot_handle,
            )
            await _answer_guest_query(
                context,
                guest_query_id,
                body,
                title=MESSAGES[lang].get("guest_not_found_title", MESSAGES["en"].get("guest_not_found_title", "No results")),
                description=effective_query,
                reply_markup=_guest_open_bot_markup(lang, not_found_url or bot_url),
            )
            return

        open_url, query_token = await _create_guest_private_handoff_start(
            context,
            handoff_type="query",
            creator_user_id=getattr(guest_message.from_user, "id", None),
            query_text=effective_query,
            source_chat_id=guest_chat.get("chat_id"),
            source_chat_type=guest_chat.get("chat_type"),
            source_chat_title=guest_chat.get("title"),
            source_chat_username=guest_chat.get("username"),
            source_public_link=guest_chat.get("public_link"),
        )
        reply_markup_obj: InlineKeyboardMarkup | None = None
        source_guest_chat_id = guest_chat.get("chat_id")
        try:
            source_guest_chat_id = int(source_guest_chat_id or 0) or None
        except Exception:
            source_guest_chat_id = None
        if source_guest_chat_id and query_token:
            try:
                delivery_capability = await run_blocking(
                    db_get_guest_group_delivery_capability,
                    source_guest_chat_id,
                )
            except Exception as e:
                logger.warning(
                    "Failed to load guest delivery capability while building guest results for group %s: %s",
                    source_guest_chat_id,
                    e,
                    exc_info=True,
                )
                delivery_capability = {}
            if bool((delivery_capability or {}).get("skip_same_chat_delivery")):
                try:
                    reply_markup_obj = await build_guest_private_handoff_reply_markup(
                        context,
                        handoff_token=query_token,
                        selected_book_id="",
                        actor_user_id=getattr(guest_message.from_user, "id", None),
                        lang=lang,
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to build remembered guest private handoff markup for group %s: %s",
                        source_guest_chat_id,
                        e,
                        exc_info=True,
                    )
                    reply_markup_obj = None
        lines = [
            MESSAGES[lang].get("guest_results_header", MESSAGES["en"]["guest_results_header"]).format(query=effective_query),
            "",
        ]
        for idx, entry in enumerate(entries, start=1):
            lines.append(f"{idx}. {entry['title']}")
        lines.extend(
            [
                "",
                MESSAGES[lang].get("guest_choose_result", MESSAGES["en"]["guest_choose_result"]),
            ]
        )
        await _answer_guest_query(
            context,
            guest_query_id,
            "\n".join(lines).strip(),
            title=MESSAGES[lang].get("guest_results_title", MESSAGES["en"].get("guest_results_title", "Search results")),
            description=MESSAGES[lang].get("guest_results_description", MESSAGES["en"].get("guest_results_description", "{count} matches")).format(count=len(entries)),
            reply_markup=(
                reply_markup_obj.to_dict()
                if reply_markup_obj is not None
                else _guest_results_markup(
                    entries,
                    lang,
                    bot_url,
                    query_token=query_token,
                    open_url=open_url or bot_url,
                )
            ),
        )
    except Exception as e:
        logger.error("Guest message handling failed: %s", e, exc_info=True)
        fallback = MESSAGES[lang].get("guest_open_private_hint", MESSAGES["en"]["guest_open_private_hint"]).format(
            bot=bot_handle,
        )
        try:
            await _answer_guest_query(
                context,
                guest_query_id,
                fallback,
                title=MESSAGES[lang].get("guest_error_title", MESSAGES["en"].get("guest_error_title", "Open the bot")),
                description=MESSAGES[lang].get("guest_error_description", MESSAGES["en"].get("guest_error_description", "Continue in private chat")),
                reply_markup=_guest_open_bot_markup(lang, bot_url),
            )
        except Exception:
            pass




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
            _HEAVY_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass
    _HEAVY_EXECUTOR = None


atexit.register(_shutdown_heavy_executor)


def _acquire_single_instance_lock() -> bool:
    """Ensure only one bot process handles updates at a time."""
    global _BOT_INSTANCE_LOCK_FH
    lock_path = os.getenv("BOT_INSTANCE_LOCK_FILE", "/tmp/pdf_audio_kitoblar_bot.instance.lock")
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


_MANAGED_BACKGROUND_TASKS_KEY = "_managed_background_tasks"
_SHUTDOWN_IN_PROGRESS_KEY = "_shutdown_in_progress"


def _register_managed_task(application, task):
    if not application or task is None:
        return task
    try:
        tasks = application.bot_data.setdefault(_MANAGED_BACKGROUND_TASKS_KEY, set())
        if isinstance(tasks, set):
            tasks.add(task)

            def _discard(done_task):
                try:
                    live = application.bot_data.get(_MANAGED_BACKGROUND_TASKS_KEY)
                    if isinstance(live, set):
                        live.discard(done_task)
                except Exception:
                    pass

            task.add_done_callback(_discard)
    except Exception:
        pass
    return task


def _spawn_managed_background_task(application, coro, *, prefer_application_task: bool = False):
    if not application:
        try:
            coro.close()
        except Exception:
            pass
        return None
    try:
        if prefer_application_task and getattr(application, "running", False):
            task = application.create_task(coro)
        else:
            task = asyncio.create_task(coro)
        return _register_managed_task(application, task)
    except Exception:
        try:
            coro.close()
        except Exception:
            pass
        return None


def _schedule_application_task(application, coro):
    """Schedule coroutine and track it so shutdown can cancel it promptly."""
    return _spawn_managed_background_task(application, coro, prefer_application_task=False)


async def _cancel_managed_background_tasks(application, timeout: float = 4.0) -> None:
    if not application:
        return
    application.bot_data[_SHUTDOWN_IN_PROGRESS_KEY] = True
    current = _safe_asyncio_current_task()
    gathered: list[asyncio.Task] = []

    managed = application.bot_data.get(_MANAGED_BACKGROUND_TASKS_KEY)
    if isinstance(managed, set):
        gathered.extend(task for task in managed if task and not task.done())

    for key in (
        _BACKGROUND_JOB_WORKERS_KEY,
        "upload_local_backup_workers",
        "audiobook_local_backup_workers",
        "upload_bulk_index_worker",
    ):
        value = application.bot_data.get(key)
        if isinstance(value, list):
            gathered.extend(task for task in value if task and not task.done())
        elif value and not getattr(value, "done", lambda: True)():
            gathered.append(value)

    unique_tasks = []
    seen = set()
    for task in gathered:
        if task is None or task is current:
            continue
        ident = id(task)
        if ident in seen:
            continue
        seen.add(ident)
        unique_tasks.append(task)

    if not unique_tasks:
        return

    for task in unique_tasks:
        try:
            task.cancel()
        except Exception:
            pass

    done, pending = await asyncio.wait(unique_tasks, timeout=max(0.5, float(timeout)))
    if pending:
        logger.warning("Shutdown timed out waiting for %s managed background task(s)", len(pending))


async def post_stop(application):
    try:
        await _cancel_managed_background_tasks(application)
    finally:
        _shutdown_heavy_executor()


def _safe_asyncio_current_task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


_BACKGROUND_JOB_ACTIVITY_KEY = "background_job_activity"


def _set_background_job_activity(app, worker_id: str, **fields: Any) -> None:
    try:
        bot_data = getattr(app, "bot_data", None)
        if not isinstance(bot_data, dict):
            return
        state = bot_data.setdefault(_BACKGROUND_JOB_ACTIVITY_KEY, {})
        if not isinstance(state, dict):
            state = {}
            bot_data[_BACKGROUND_JOB_ACTIVITY_KEY] = state
        payload = dict(fields)
        payload["updated_at"] = time.time()
        state[str(worker_id or "worker")] = payload
    except Exception:
        pass


def _clear_background_job_activity(app, worker_id: str) -> None:
    try:
        bot_data = getattr(app, "bot_data", None)
        state = bot_data.get(_BACKGROUND_JOB_ACTIVITY_KEY) if isinstance(bot_data, dict) else None
        if isinstance(state, dict):
            state.pop(str(worker_id or "worker"), None)
            if not state:
                bot_data.pop(_BACKGROUND_JOB_ACTIVITY_KEY, None)
    except Exception:
        pass


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


def _telegram_safe_filename_title(name: str) -> str:
    """
    Keep human-readable apostrophes in Telegram filenames.
    Telegram may strip U+02BB (ʻ) and some curly variants in filenames.
    Map all common apostrophe variants to plain ASCII apostrophe.
    """
    text = str(name or "")
    return re.sub(r"[ʻʼ’'`´‘]", "'", text)


def _normalize_title_apostrophes(name: str) -> str:
    """
    Preserve Uzbek spelling in owner-set titles by converting common
    apostrophe variants to the Uzbek apostrophe character.
    """
    text = str(name or "").strip()
    if not text:
        return ""
    return re.sub(r"[ʼ’'`´‘]", "ʻ", text)


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


def parse_group_private_start_payload(payload: str | None) -> tuple[int | None, str | None]:
    if not payload:
        return None, None
    token = str(payload).strip()
    if not token:
        return None, None
    if token == "group_start":
        return None, None
    if token.startswith("gstart_"):
        body = token[len("gstart_") :].strip()
        if not body:
            return None, None
        parts = body.split("_", 1)
        if len(parts) != 2:
            return None, None
        try:
            user_id = int(parts[0])
        except Exception:
            return None, None
        prompt_token = str(parts[1]).strip()
        if not user_id or not prompt_token:
            return None, None
        return user_id, prompt_token
    return None, None


async def _load_book_for_private_handoff(book_id: str) -> dict[str, Any] | None:
    safe_book_id = str(book_id or "").strip()
    if not safe_book_id:
        return None
    book = await run_blocking(db_get_book_by_id, safe_book_id)
    if book:
        return dict(book)
    if not es_available():
        return None
    try:
        es = get_es()
        if not es:
            return None
        res = await run_blocking(lambda: es.get(index="books", id=safe_book_id))
        source = (res or {}).get("_source") or {}
        if not source:
            return None
        restored = dict(source)
        restored["id"] = safe_book_id
        await run_blocking(db_insert_book, restored)
        return restored
    except Exception as e:
        logger.warning("Private handoff ES lookup failed for %s: %s", safe_book_id, e)
        return None


async def _send_private_handoff_results(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    lang: str,
    query_text: str,
) -> bool:
    effective_query = str(query_text or "").strip()
    if not effective_query:
        return False
    target_message = update.message or update.effective_message
    entries = await run_blocking(_search_guest_books, effective_query, 5)
    if not entries:
        await safe_reply(update, MESSAGES[lang]["not_found"])
        return True
    query_id = cache_search_results(context, effective_query, entries)
    result_text, page_entries, pages = build_results_text(effective_query, entries, 0, lang)
    reply_markup = build_results_keyboard(page_entries, 0, pages, query_id)
    if target_message:
        await _send_with_retry(lambda: target_message.reply_text(result_text, reply_markup=reply_markup))
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=result_text,
            reply_markup=reply_markup,
        )
    return True


async def _handle_guest_private_start_payload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    payload_token: str,
    user_record: dict[str, Any],
) -> bool:
    try:
        handoff = await run_blocking_db_retry(
            db_get_guest_private_handoff_by_token,
            payload_token,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
        )
    except Exception as e:
        logger.warning("Failed to load guest handoff payload %s: %s", payload_token, e, exc_info=True)
        return False
    if not handoff:
        return False

    selected_lang = str(user_record.get("language") or "").strip()
    language_selected = bool(user_record.get("language_selected")) and bool(selected_lang)
    source_chat_type = str((handoff or {}).get("source_chat_type") or "").strip().lower()
    force_uz_for_group_handoff = source_chat_type in {"group", "supergroup"}
    lang = "uz" if force_uz_for_group_handoff else (detect_language_code(selected_lang) if language_selected else "uz")

    if force_uz_for_group_handoff or not language_selected:
        context.user_data["language"] = lang
        try:
            await run_blocking_db_retry(
                set_user_language,
                update.effective_user.id,
                lang,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.warning("Failed to auto-set guest handoff language for %s: %s", update.effective_user.id, e, exc_info=True)

    handled = False
    context_line_sent = False
    handoff_type = str(handoff.get("handoff_type") or "query").strip().lower() or "query"
    query_text = str(handoff.get("query_text") or "").strip()
    book_id = str(handoff.get("book_id") or "").strip()
    context_line = _guest_private_handoff_context_text(lang, handoff_type)

    if language_selected and handoff_type == "book" and book_id:
        book = await _load_book_for_private_handoff(book_id)
        if book:
            if context_line:
                await safe_reply(update, context_line)
                context_line_sent = True
            sent = await send_book(
                context.bot,
                update.effective_chat.id,
                book,
                lang=lang,
                user_id=update.effective_user.id,
            )
            handled = bool(sent)
            if handled:
                try:
                    await run_blocking_db_retry(db_increment_book_download, book_id, retries=DB_RETRY_ATTEMPTS, base_delay=DB_RETRY_BASE_DELAY_SEC)
                    await run_blocking_db_retry(add_recent_download, update.effective_user.id, book_id, get_result_title(book), retries=DB_RETRY_ATTEMPTS, base_delay=DB_RETRY_BASE_DELAY_SEC)
                    await run_blocking_db_retry(db_increment_counter, "download_total", 1, retries=DB_RETRY_ATTEMPTS, base_delay=DB_RETRY_BASE_DELAY_SEC)
                    await run_blocking_db_retry(increment_analytics, "buttons", 1, retries=DB_RETRY_ATTEMPTS, base_delay=DB_RETRY_BASE_DELAY_SEC)
                    await run_blocking_db_retry(increment_user_analytics, update.effective_user.id, "buttons", 1, retries=DB_RETRY_ATTEMPTS, base_delay=DB_RETRY_BASE_DELAY_SEC)
                except Exception as e:
                    logger.warning("Failed to record guest private handoff book analytics for %s: %s", book_id, e, exc_info=True)

    if not handled and query_text:
        if context_line and not context_line_sent:
            await safe_reply(update, context_line)
            context_line_sent = True
        handled = await _send_private_handoff_results(update, context, lang=lang, query_text=query_text)

    if not handled and book_id:
        book = await _load_book_for_private_handoff(book_id)
        if book:
            if context_line and not context_line_sent:
                await safe_reply(update, context_line)
                context_line_sent = True
            handled = bool(
                await send_book(
                    context.bot,
                    update.effective_chat.id,
                    book,
                    lang=lang,
                    user_id=update.effective_user.id,
                )
            )

    if handled:
        try:
            await run_blocking_db_retry(
                db_touch_guest_private_handoff,
                payload_token,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.debug("Failed to touch guest private handoff %s: %s", payload_token, e, exc_info=True)
        _set_main_menu_ready_state(context)
        try:
            _schedule_application_task(
                context.application,
                _sync_user_commands_if_needed(context, update.effective_user.id, lang, force=True),
            )
        except Exception as e:
            logger.debug("Failed to sync commands after guest handoff for %s: %s", update.effective_user.id, e, exc_info=True)
    return handled


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
    display = str(get_display_name(book) or "").strip() or "book"
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
    can_rename_book: bool = False,
    can_edit_reactions: bool = False,
    lang: str = "en",
    has_audiobook: bool = False,
    can_add_audiobook: bool = False,
    show_listen_button: bool = True,
    audiobook_request_count: int = 0,
    show_personal_state: bool = True,
    show_favorite_button: bool = True,
    show_comments_button: bool = False,
    more_books_url: str | None = None,
    more_books_label: str | None = None,
    open_private_url: str | None = None,
    open_private_label: str | None = None,
    reactions_locked: bool = False,
    dislikes_disabled: bool = False,
) -> InlineKeyboardMarkup:
    like = counts.get("like", 0)
    dislike = counts.get("dislike", 0)
    berry = counts.get("berry", 0)
    whale = counts.get("whale", 0)
    m = MESSAGES.get(lang, MESSAGES["en"])
    fav_label = (
        m.get("book_action_remove_favorite", "❌ Remove")
        if show_personal_state and is_fav
        else m.get("book_action_favorite", "⭐ Favorite")
    )

    def label(key: str, emoji: str, count: int) -> str:
        prefix = "★ " if show_personal_state and user_reaction == key else ""
        return f"{prefix}{emoji} {count}"

    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(label("whale", REACTION_EMOJI["whale"], whale), callback_data=f"react:{book_id}:whale"),
            InlineKeyboardButton(label("berry", REACTION_EMOJI["berry"], berry), callback_data=f"react:{book_id}:berry"),
            InlineKeyboardButton(label("like", REACTION_EMOJI["like"], like), callback_data=f"react:{book_id}:like"),
            InlineKeyboardButton(label("dislike", REACTION_EMOJI["dislike"], dislike), callback_data=f"react:{book_id}:dislike"),
        ],
    ]

    if show_listen_button:
        listen_label = m.get("audiobook_listen_button", "🎧 Listen Audiobook")
        rows.append([InlineKeyboardButton(listen_label, callback_data=f"abook:{book_id}")])

    if show_favorite_button:
        rows.append([InlineKeyboardButton(fav_label, callback_data=f"fav:toggle:{book_id}")])

    if show_comments_button:
        rows.append([InlineKeyboardButton(m.get("book_action_comments", "💬 Comments"), callback_data=f"bookcomments:{book_id}:0")])

    if can_rename_book:
        rows.append([InlineKeyboardButton(m.get("book_action_rename", "✏️ Edit name"), callback_data=f"bookrename:{book_id}")])

    if can_edit_reactions:
        rows.append([InlineKeyboardButton(m.get("book_action_edit_reactions", "🎛 Edit reactions"), callback_data=f"bookreactedit:{book_id}")])
        rows.append(
            [
                InlineKeyboardButton(
                    m.get(
                        "book_action_unlock_reactions" if reactions_locked else "book_action_lock_reactions",
                        "🔓 Unlock reactions" if reactions_locked else "🔒 Lock reactions",
                    ),
                    callback_data=f"bookreactpolicy:{book_id}:lock",
                ),
                InlineKeyboardButton(
                    m.get(
                        "book_action_enable_dislikes" if dislikes_disabled else "book_action_disable_dislikes",
                        "✅ Enable dislikes" if dislikes_disabled else "🚫 Disable dislikes",
                    ),
                    callback_data=f"bookreactpolicy:{book_id}:dislikes",
                ),
            ]
        )

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
        rows.append([InlineKeyboardButton(m.get("book_action_delete", "🗑️ Delete book"), callback_data=f"delbook:{book_id}")])

    if more_books_url:
        rows.append(
            [
                InlineKeyboardButton(
                    more_books_label or m.get("inline_more_books_button", "📚 More books"),
                    url=more_books_url,
                )
            ]
        )

    if open_private_url:
        rows.append(
            [
                InlineKeyboardButton(
                    open_private_label or m.get("group_open_private_results", "Open in bot"),
                    url=open_private_url,
                )
            ]
        )

    return InlineKeyboardMarkup(rows)



def build_book_caption(book, downloads: int, fav_count: int, counts: dict) -> str:
    title = str(get_display_name(book) or "").strip() or "Book"
    if len(title) > 96:
        title = title[:93].rstrip() + "..."
    lines = [
        f"📖 {title}",
        f"⬇️ {downloads}   ⭐ {fav_count}",
    ]
    return "\n".join(lines)


async def send_book(bot, chat_id, book, *, lang: str = "en", user_id: int | None = None):
    """
    Send a book to the user:
    - Prefer Telegram cache if available
    - Fall back to local file if it exists
    - Capture and save file_id after local upload
    - Always index/update in ES using stable UUID
    """

    book_path = book.get("path")
    book_id = str(book.get("id") or "")
    stats = {"downloads": 0, "fav_count": 0, "like": 0, "dislike": 0, "berry": 0, "whale": 0}
    if book_id:
        try:
            stats = await run_blocking(db_get_book_stats, book_id) or stats
        except Exception as e:
            logger.warning("Failed to load book stats for %s: %s", book_id, e)
    downloads = stats.get("downloads", 0)
    fav_count = stats.get("fav_count", 0)
    counts = {
        "like": stats.get("like", 0),
        "dislike": stats.get("dislike", 0),
        "berry": stats.get("berry", 0),
        "whale": stats.get("whale", 0),
    }
    caption = build_book_caption(book, downloads, fav_count, counts)
    if user_id is None:
        user_id = chat_id if isinstance(chat_id, int) else None
    is_fav = False
    user_reaction = None
    if user_id and book_id:
        try:
            is_fav = bool(await run_blocking(db_is_favorited, user_id, book_id))
        except Exception as e:
            logger.warning("Failed to load favorite state for user=%s book=%s: %s", user_id, book_id, e)
        try:
            user_reaction = await run_blocking(db_get_user_reaction, book_id, user_id)
        except Exception as e:
            logger.warning("Failed to load reaction state for user=%s book=%s: %s", user_id, book_id, e)
    can_delete = await can_delete_books(user_id) if user_id else False
    audio_book = None
    if book_id:
        try:
            audio_book = await run_blocking(get_audio_book_for_book, book_id)
        except Exception as e:
            logger.warning("Failed to load audiobook metadata for %s: %s", book_id, e)
    has_ab = bool(audio_book)
    can_add_ab = bool(is_audio_allowed(user_id)) if user_id else False
    try:
        chat_id_int = int(chat_id)
    except Exception:
        chat_id_int = 0
    can_rename_book = bool(can_rename_books(user_id)) if user_id and chat_id_int > 0 else False
    is_owner_user = bool(_is_owner_user(user_id)) if user_id and callable(globals().get("_is_owner_user")) else False
    can_edit_reactions = bool(is_owner_user and chat_id_int > 0 and book_id)
    reaction_policy = {"reactions_locked": False, "dislikes_disabled": False}
    if can_edit_reactions and book_id:
        try:
            reaction_policy = await run_blocking(db_get_book_reaction_policy, book_id) or reaction_policy
        except Exception as e:
            logger.warning("Failed to load reaction policy for %s: %s", book_id, e)
    show_listen_btn = has_ab if is_owner_user else True
    ab_request_count = 0
    if book_id and can_add_ab and is_owner_user:
        try:
            ab_request_count = int(await run_blocking(count_pending_audiobook_requests, book_id) or 0)
        except Exception as e:
            logger.warning("Failed to load pending audiobook request count for %s: %s", book_id, e)
    reactions_kb = (
        build_book_keyboard(
            book_id,
            counts,
            is_fav,
            user_reaction,
            can_delete,
            can_rename_book,
            can_edit_reactions,
            lang,
            has_audiobook=has_ab,
            can_add_audiobook=can_add_ab,
            show_listen_button=show_listen_btn,
            audiobook_request_count=ab_request_count,
            show_comments_button=bool(chat_id_int > 0),
            reactions_locked=bool(reaction_policy.get("reactions_locked")),
            dislikes_disabled=bool(reaction_policy.get("dislikes_disabled")),
        )
        if book_id
        else None
    )

    if chat_id_int > 0:
        try:
            await bot.send_chat_action(chat_id=chat_id_int, action="upload_document")
        except Exception:
            pass

    try:
        if book.get("file_id"):
            # Prefer Telegram cache
            try:
                sent_message = await bot.send_document(
                    chat_id=chat_id,
                    document=book["file_id"],
                    caption=caption,
                    reply_markup=reactions_kb,
                )
                return sent_message
            except Exception as e:
                logger.error("send_book failed by file_id for %s: %s", book_id, e)

        if book_path and os.path.exists(book_path):
            # Fallback to local file
            thumbnail = get_book_thumbnail_input()
            with open(book_path, "rb") as f:
                sent_message = await bot.send_document(
                    chat_id=chat_id,
                    document=InputFile(f, filename=_book_filename(book)),
                    caption=caption,
                    reply_markup=reactions_kb,
                    thumbnail=thumbnail,
                )

            # Capture file_id for future use
            if sent_message and sent_message.document:
                new_file_id = sent_message.document.file_id
                new_file_unique_id = getattr(sent_message.document, "file_unique_id", None)
                book["file_id"] = new_file_id
                if new_file_unique_id:
                    book["file_unique_id"] = new_file_unique_id

                # Save updated file_id in DB
                try:
                    if book.get("id"):
                        await run_blocking(
                            update_book_file_id,
                            str(book.get("id")),
                            new_file_id,
                            True,
                            new_file_unique_id,
                        )
                    elif book_path:
                        await run_blocking(update_book_by_path, book_path, file_id=new_file_id, indexed=True)
                    logger.debug(f"Updated file_id + indexed flag for {book.get('book_name')} in DB")
                except Exception as e:
                    logger.error(f"⚠️ Failed to update book file_id in DB: {e}")

                # ✅ Index in Elasticsearch with stable UUID
                if es_available():
                    try:
                        await run_blocking(
                            index_book,
                            book["book_name"],
                            new_file_id,
                            book_path,
                            book.get("id"),
                            get_display_name(book),
                            new_file_unique_id,
                        )
                    except Exception as e:
                        logger.warning("Failed to index book in ES for %s: %s", book.get("id"), e)
                return sent_message
        else:
            await bot.send_message(chat_id=chat_id, text=MESSAGES[lang]["book_unavailable"])
        return None
    except Exception:
        raise

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


def _parse_background_job_type_limits(raw: str | None) -> dict[str, int]:
    limits: dict[str, int] = {}
    text = str(raw or "").strip()
    if not text:
        return limits
    for chunk in text.split(","):
        piece = str(chunk or "").strip()
        if not piece or ":" not in piece:
            continue
        job_type, value = piece.split(":", 1)
        key = str(job_type or "").strip()
        if not key:
            continue
        try:
            parsed = max(0, int(str(value or "").strip()))
        except Exception:
            continue
        limits[key] = parsed
    return limits


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
    try:
        _error_log_max_mb = max(5, int(str(os.getenv("ERROR_LOG_MAX_MB", "20")).strip()))
    except Exception:
        _error_log_max_mb = 20
    try:
        _error_log_backup_count = max(1, int(str(os.getenv("ERROR_LOG_BACKUP_COUNT", "5")).strip()))
    except Exception:
        _error_log_backup_count = 5
    error_handler = RotatingFileHandler(
        "logs/errors.log",
        maxBytes=_error_log_max_mb * 1024 * 1024,
        backupCount=_error_log_backup_count,
        encoding="utf-8",
    )
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

# Elasticsearch config via environment variables
ES_URL = os.getenv("ES_URL", "")
ES_CA_CERT = os.getenv("ES_CA_CERT", "")
ES_USER = os.getenv("ES_USER", "")
ES_PASS = os.getenv("ES_PASS", "")
ENABLE_ELASTICSEARCH = _env_bool("ENABLE_ELASTICSEARCH", True)
try:
    ES_TIMEOUT_SECONDS = max(1, int(os.getenv("ES_TIMEOUT_SECONDS", "3") or "3"))
except Exception:
    ES_TIMEOUT_SECONDS = 3
_ES_CLIENT = None
BOOK_LOVERS_GROUP_URL = (os.getenv("BOOK_LOVERS_GROUP_URL", "") or "").strip()
_BOOK_LOVERS_GROUP_HANDLE_RAW = (os.getenv("BOOK_LOVERS_GROUP_HANDLE", "") or "").strip()
BOT_OWNER_USERNAME = (os.getenv("BOT_OWNER_USERNAME", "@MuhammadaliAbdullayev") or "@MuhammadaliAbdullayev").strip()
BOT_DISPLAY_NAME = (os.getenv("BOT_DISPLAY_NAME", "Pdf va audio kitoblar") or "Pdf va audio kitoblar").strip()
BOT_PUBLIC_USERNAME = (os.getenv("BOT_PUBLIC_USERNAME", "@pdf_audio_kitoblar_bot") or "@pdf_audio_kitoblar_bot").strip()

ANALYTICS_FILE = "analytics.json"
REQUESTS_FILE = "requests.json"
UPLOAD_REQUESTS_FILE = "upload_requests.json"
PAGE_SIZE = 10
try:
    SEARCH_COOLDOWN_SEC = max(0.0, float(os.getenv("SEARCH_COOLDOWN_SEC", "1") or "1"))
except Exception:
    SEARCH_COOLDOWN_SEC = 1.0
try:
    CALLBACK_COOLDOWN_SEC = max(0.0, float(os.getenv("CALLBACK_COOLDOWN_SEC", "0.5") or "0.5"))
except Exception:
    CALLBACK_COOLDOWN_SEC = 0.5
try:
    INLINE_SEARCH_COOLDOWN_SEC = max(0.0, float(os.getenv("INLINE_SEARCH_COOLDOWN_SEC", "1") or "1"))
except Exception:
    INLINE_SEARCH_COOLDOWN_SEC = 1.0
try:
    MAX_SEARCHES_PER_MINUTE = max(1, int(os.getenv("MAX_SEARCHES_PER_MINUTE", "20") or "20"))
except Exception:
    MAX_SEARCHES_PER_MINUTE = 20
DOWNLOAD_COOLDOWN_SEC = 0
MAX_RECENTS = 5
MAX_FAVORITES = 50
MAX_SEARCH_RESULTS = 10
REQUESTS_PAGE_SIZE = 10
USER_SEARCH_LIMIT = 30
LOCAL_SEND_RETRIES = 3
LOCAL_SEND_BACKOFF_SEC = 3
try:
    UPLOAD_LOCAL_WORKERS = max(1, int(os.getenv("UPLOAD_LOCAL_WORKERS", "5")))
except Exception:
    UPLOAD_LOCAL_WORKERS = 5
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
BOOK_SEARCH_RESULT_CACHE_TTL = max(10, int(os.getenv("BOOK_SEARCH_RESULT_CACHE_TTL", "120") or "120"))
TOP_USERS_CACHE_TTL = max(5, int(os.getenv("TOP_USERS_CACHE_TTL", "45") or "45"))
SEARCH_CACHE_NS = (os.getenv("SEARCH_CACHE_NS", "v1") or "v1").strip()
try:
    THREAD_POOL_WORKERS = max(4, int(os.getenv("THREAD_POOL_WORKERS", "100")))
except Exception:
    THREAD_POOL_WORKERS = 100
try:
    HEAVY_THREAD_POOL_WORKERS = max(1, int(os.getenv("HEAVY_THREAD_POOL_WORKERS", "20")))
except Exception:
    HEAVY_THREAD_POOL_WORKERS = 20
try:
    BACKGROUND_JOB_WORKER_COUNT = max(1, int(os.getenv("BACKGROUND_JOB_WORKER_COUNT", "3")))
except Exception:
    BACKGROUND_JOB_WORKER_COUNT = 3
try:
    BACKGROUND_JOB_STALE_AFTER_SECONDS = max(60, int(os.getenv("BACKGROUND_JOB_STALE_AFTER_SECONDS", "1800")))
except Exception:
    BACKGROUND_JOB_STALE_AFTER_SECONDS = 1800
try:
    JOB_LOCK_TIMEOUT_MINUTES = max(1, int(os.getenv("JOB_LOCK_TIMEOUT_MINUTES", "30") or "30"))
except Exception:
    JOB_LOCK_TIMEOUT_MINUTES = 30
try:
    BACKGROUND_JOB_IDLE_SECONDS = max(0.5, float(os.getenv("BACKGROUND_JOB_IDLE_SECONDS", "3.0")))
except Exception:
    BACKGROUND_JOB_IDLE_SECONDS = 3.0
try:
    BOT_CONCURRENT_UPDATES = max(1, int(os.getenv("BOT_CONCURRENT_UPDATES", "16") or "16"))
except Exception:
    BOT_CONCURRENT_UPDATES = 16
try:
    BOT_CONNECTION_POOL_SIZE = max(4, int(os.getenv("BOT_CONNECTION_POOL_SIZE", "32") or "32"))
except Exception:
    BOT_CONNECTION_POOL_SIZE = 32
try:
    BOT_POOL_TIMEOUT = max(5, int(os.getenv("BOT_POOL_TIMEOUT", "30") or "30"))
except Exception:
    BOT_POOL_TIMEOUT = 30
try:
    TEMP_JOB_TTL_HOURS = max(1, int(os.getenv("TEMP_JOB_TTL_HOURS", "24") or "24"))
except Exception:
    TEMP_JOB_TTL_HOURS = 24
try:
    FAILED_JOB_TTL_HOURS = max(1, int(os.getenv("FAILED_JOB_TTL_HOURS", "48") or "48"))
except Exception:
    FAILED_JOB_TTL_HOURS = 48
BACKGROUND_JOB_TYPE_LIMITS = _parse_background_job_type_limits(
    os.getenv(
        "BACKGROUND_JOB_TYPE_LIMITS",
        "",
    )
)
BACKGROUND_JOB_TYPE_LIMITS.pop("book_summary", None)
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
try:
    STARTUP_DEPENDENCY_TIMEOUT_SEC = max(5.0, float(os.getenv("STARTUP_DEPENDENCY_TIMEOUT_SEC", "90") or "90"))
except Exception:
    STARTUP_DEPENDENCY_TIMEOUT_SEC = 90.0
try:
    STARTUP_DEPENDENCY_RETRY_SEC = max(0.5, float(os.getenv("STARTUP_DEPENDENCY_RETRY_SEC", "2") or "2"))
except Exception:
    STARTUP_DEPENDENCY_RETRY_SEC = 2.0
_ES_HEALTH_CACHE = {"ok": None, "checked_at": 0.0, "error": None}


def _check_db_startup_ready() -> tuple[bool, str | None]:
    try:
        stats = db_ping()
    except Exception as e:
        return False, str(e)
    if stats.get("ok"):
        return True, None
    return False, str(stats.get("error") or "database unavailable")


def _check_es_startup_ready() -> tuple[bool, str | None]:
    if not ENABLE_ELASTICSEARCH:
        return True, None
    try:
        es = get_es()
        if es is None:
            return False, "elasticsearch client unavailable"
        es.info()
        ensure_index()
        return True, None
    except Exception as e:
        return False, str(e)


def _check_bot_api_startup_ready(bot_api_base_url: str, *, local_mode: bool = False) -> tuple[bool, str | None]:
    base = str(bot_api_base_url or "").strip()
    if not base and not local_mode:
        return True, None
    target = base or "http://127.0.0.1:8081"
    try:
        parsed = urlparse(target if "://" in target else f"http://{target}")
        host = parsed.hostname or "127.0.0.1"
        if ":" in host and not parsed.hostname:
            host = host.strip("[]")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=2.0):
            return True, None
    except Exception as e:
        return False, str(e)


def wait_for_runtime_dependencies(
    service_name: str,
    *,
    require_db: bool = True,
    require_es: bool = False,
    bot_api_base_url: str = "",
    require_bot_api: bool = False,
    bot_api_local_mode: bool = False,
) -> None:
    checks: list[tuple[str, Any]] = []
    if require_db:
        checks.append(("PostgreSQL", _check_db_startup_ready))
    if require_es:
        checks.append(("Elasticsearch", _check_es_startup_ready))
    if require_bot_api:
        checks.append(("Telegram Bot API", lambda: _check_bot_api_startup_ready(bot_api_base_url, local_mode=bot_api_local_mode)))
    if not checks:
        return

    deadline = time.time() + STARTUP_DEPENDENCY_TIMEOUT_SEC
    pending = {name for name, _ in checks}
    last_errors: dict[str, str] = {}
    logger.info(
        "%s waiting for dependencies: %s",
        service_name,
        ", ".join(name for name, _ in checks),
    )
    while pending:
        ready_now: list[str] = []
        for name, checker in checks:
            if name not in pending:
                continue
            ok, err = checker()
            if ok:
                ready_now.append(name)
            elif err:
                last_errors[name] = str(err)
        for name in ready_now:
            pending.discard(name)
            logger.info("%s dependency ready: %s", service_name, name)
        if not pending:
            return
        if time.time() >= deadline:
            detail = "; ".join(f"{name}={last_errors.get(name, 'not ready')}" for name in pending)
            raise RuntimeError(f"{service_name} dependency wait timed out: {detail}")
        time.sleep(STARTUP_DEPENDENCY_RETRY_SEC)


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


def add_request_record(user, query: str, lang: str, *, book_id: str | None = None):
    request_id = uuid.uuid4().hex[:10]
    resolved_book_id = str(book_id or "").strip() or None
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
        "book_id": resolved_book_id,
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
    db_increment_counter("upload_request_created", 1)
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


def _resolve_request_book_id(record: dict | None) -> str | None:
    if not isinstance(record, dict):
        return None
    direct = str(record.get("book_id") or "").strip()
    if direct:
        return direct
    return _extract_audiobook_request_book_id(record.get("query"))


def _find_open_audiobook_request_for_user_book(user_id: int, book_id: str) -> dict | None:
    target_book_id = str(book_id or "").strip()
    if not user_id or not target_book_id:
        return None
    try:
        requests = load_requests()
    except Exception:
        return None

    for r in requests or []:
        if str(r.get("status") or "") not in {"open", "seen"}:
            continue
        try:
            req_user_id = int(r.get("user_id") or 0)
        except Exception:
            req_user_id = 0
        if req_user_id != int(user_id):
            continue
        req_book_id = _resolve_request_book_id(r)
        if req_book_id and req_book_id.strip() == target_book_id:
            return r
    return None


def count_pending_audiobook_requests(book_id: str) -> int:
    target = str(book_id or "").strip()
    if not target:
        return 0
    try:
        requests = load_requests()
    except Exception:
        return 0

    pending_users: set[int] = set()
    for r in requests or []:
        if (r.get("status") or "") not in {"open", "seen"}:
            continue
        req_book_id = _resolve_request_book_id(r)
        if not req_book_id or req_book_id.strip() != target:
            continue
        try:
            uid = int(r.get("user_id") or 0)
        except Exception:
            uid = 0
        if uid > 0:
            pending_users.add(uid)
    return len(pending_users)


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
    return (book.get("display_name") or book.get("book_name") or "Untitled").strip()


def _rolling_window_limited(
    context: ContextTypes.DEFAULT_TYPE,
    key: str,
    limit: int,
    window_seconds: float,
):
    if limit <= 0 or not context or not getattr(context, "user_data", None):
        return False, 0
    now = time.time()
    history_key = f"{key}_history"
    history = context.user_data.get(history_key, [])
    history = [t for t in history if now - t < window_seconds]
    if len(history) >= limit:
        remaining = int(math.ceil(max(0.0, window_seconds - (now - history[0]))))
        context.user_data[history_key] = history
        return True, max(1, remaining)
    history.append(now)
    context.user_data[history_key] = history
    return False, 0


def rate_limited(context: ContextTypes.DEFAULT_TYPE, key: str, cooldown: float):
    now = time.time()
    last = context.user_data.get(key, 0.0)
    delta = now - last
    if delta < cooldown:
        remaining = int(math.ceil(cooldown - delta))
        return True, remaining
    if "search" in str(key).lower():
        limited, remaining = _rolling_window_limited(
            context,
            key,
            MAX_SEARCHES_PER_MINUTE,
            60.0,
        )
        if limited:
            return True, remaining
    context.user_data[key] = now
    return False, 0


async def safe_answer(query, text: str | None = None, show_alert: bool = False):
    if getattr(query, "_codex_answered", False):
        return
    try:
        await query.answer(text=text, show_alert=show_alert)
        try:
            setattr(query, "_codex_answered", True)
        except Exception:
            pass
    except BadRequest as e:
        msg = str(e)
        low = msg.lower()
        if "query is too old" in low or "query id is invalid" in low:
            try:
                setattr(query, "_codex_answered", True)
            except Exception:
                pass
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
    if not context or not getattr(context, "user_data", None):
        return False, 0
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
    # Legacy naming: "admin" now maps to owner-only authority.
    return _is_owner_user(user_id)


def _is_owner_user(user_id: int) -> bool:
    try:
        return bool(OWNER_ID) and user_id == OWNER_ID
    except Exception:
        return False

def spam_check_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context and getattr(context, "user_data", None):
        if context.user_data.pop("_skip_spam_check_once", False):
            return False, 0
        if str(context.user_data.get("upload_mode_state") or "").strip().lower() == "book":
            return False, 0
    if update.effective_user and _is_admin_user(update.effective_user.id):
        return False, 0
    return _spam_guard(context, "spam_msg", SPAM_MSG_LIMIT, SPAM_MSG_WINDOW, SPAM_MSG_BLOCK)


def spam_check_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and _is_admin_user(update.effective_user.id):
        return False, 0
    limited, wait_s = _spam_guard(context, "spam_cb", SPAM_CB_LIMIT, SPAM_CB_WINDOW, SPAM_CB_BLOCK)
    if limited:
        return True, wait_s
    if CALLBACK_COOLDOWN_SEC > 0:
        return rate_limited(context, "last_callback_ts", CALLBACK_COOLDOWN_SEC)
    return False, 0


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



def add_recent_download(user_id: int, book_id: str, title: str):
    db_add_recent(user_id, book_id, title, MAX_RECENTS)


def add_favorite(user_id: int, book_id: str, title: str):
    return db_add_favorite(user_id, book_id, title, MAX_FAVORITES)


def remove_favorite(user_id: int, book_id: str):
    return db_remove_favorite(user_id, book_id)


def is_favorited(user_id: int, book_id: str):
    return db_is_favorited(user_id, book_id)

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


async def send_request_to_admin(
    context: ContextTypes.DEFAULT_TYPE,
    user,
    query: str,
    lang: str,
    *,
    book_id: str | None = None,
):
    resolved_book_id = str(book_id or "").strip() or None
    if resolved_book_id and user and getattr(user, "id", None):
        existing = _find_open_audiobook_request_for_user_book(int(user.id), resolved_book_id)
        if existing:
            return existing

    record = add_request_record(user, query, lang, book_id=resolved_book_id)
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
    return OWNER_ID or None


def get_admin_id() -> int | None:
    return OWNER_ID or None


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


def _parse_forbidden_book_titles(raw_text: str) -> list[tuple[str, str]]:
    clean_text = str(raw_text or "").strip()
    if not clean_text:
        return []
    entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for part in re.split(r"[\n,]+", clean_text):
        title = str(part or "").strip()
        if not title:
            continue
        normalized_title = normalize(title).lower().strip()
        if not normalized_title or normalized_title in seen:
            continue
        seen.add(normalized_title)
        entries.append((normalized_title, title))
    return entries


FORBIDDEN_BOOKS_WARNING_SETTING_KEY = "forbidden_books_warning_text"


async def _save_forbidden_book_titles(raw_text: str, owner_user_id: int | None) -> int:
    entries = _parse_forbidden_book_titles(raw_text)
    if not entries:
        return 0
    return int(await run_blocking(db_upsert_forbidden_books, entries, owner_user_id))


async def _remove_forbidden_book_titles(raw_text: str) -> int:
    entries = _parse_forbidden_book_titles(raw_text)
    if not entries:
        return 0
    normalized_titles = [normalized_title for normalized_title, _title in entries]
    return int(await run_blocking(db_remove_forbidden_books, normalized_titles))


async def _build_forbidden_books_list_text(lang: str, *, limit: int = 100) -> str:
    items = await run_blocking(db_list_forbidden_books)
    if not items:
        return MESSAGES[lang]["forbidden_books_list_empty"]
    max_items = max(1, int(limit))
    lines = [MESSAGES[lang]["forbidden_books_list_header"].format(count=len(items))]
    visible_count = 0
    for item in items[:max_items]:
        title = str((item or {}).get("title") or "").strip()
        if title:
            candidate = f"{visible_count + 1}. {title}"
            if len("\n".join(lines + [candidate])) > 3500:
                break
            lines.append(candidate)
            visible_count += 1
    hidden_count = max(0, len(items) - visible_count)
    if hidden_count:
        lines.append(MESSAGES[lang]["forbidden_books_list_more"].format(count=hidden_count))
    return "\n".join(lines)


async def _get_forbidden_warning_text(lang: str) -> str:
    custom_text = await run_blocking(db_get_bot_setting, FORBIDDEN_BOOKS_WARNING_SETTING_KEY)
    custom_text = str(custom_text or "").strip()
    return custom_text or MESSAGES[lang]["forbidden_books_blocked"]


async def _set_forbidden_warning_text(text: str) -> str:
    clean_text = str(text or "").strip()
    if not clean_text:
        return ""
    await run_blocking(db_set_bot_setting, FORBIDDEN_BOOKS_WARNING_SETTING_KEY, clean_text)
    return clean_text


async def _reset_forbidden_warning_text() -> None:
    await run_blocking(db_delete_bot_setting, FORBIDDEN_BOOKS_WARNING_SETTING_KEY)


def _set_pending_forbidden_books_action(context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    context.user_data["pending_forbidden_books"] = {
        "mode": str(mode or "add").strip().lower(),
        "expires_at": time.time() + 300,
    }


async def _forbidden_books_command_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return

    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    raw_text = " ".join(args).strip()
    if not raw_text:
        _set_pending_forbidden_books_action(context, "add")
        await target_message.reply_text(MESSAGES[lang]["forbidden_books_prompt"])
        return

    action = args[0].lower()
    rest_text = " ".join(args[1:]).strip() if len(args) > 1 else ""

    if action in {"list", "ls"}:
        context.user_data.pop("pending_forbidden_books", None)
        context.user_data.pop("awaiting_forbidden_books_input_until", None)
        await target_message.reply_text(await _build_forbidden_books_list_text(lang))
        return

    if action in {"remove", "rm", "delete", "del"}:
        if not rest_text:
            _set_pending_forbidden_books_action(context, "remove")
            await target_message.reply_text(MESSAGES[lang]["forbidden_books_remove_prompt"])
            return
        removed_count = await _remove_forbidden_book_titles(rest_text)
        context.user_data.pop("pending_forbidden_books", None)
        context.user_data.pop("awaiting_forbidden_books_input_until", None)
        if removed_count <= 0:
            await target_message.reply_text(MESSAGES[lang]["forbidden_books_remove_none"])
            return
        await target_message.reply_text(MESSAGES[lang]["forbidden_books_removed"].format(count=removed_count))
        return

    if action in {"warning", "warn", "text"}:
        if rest_text.lower() in {"reset", "default"}:
            await _reset_forbidden_warning_text()
            context.user_data.pop("pending_forbidden_books", None)
            context.user_data.pop("awaiting_forbidden_books_input_until", None)
            await target_message.reply_text(MESSAGES[lang]["forbidden_books_warning_reset"])
            return
        if rest_text:
            await _set_forbidden_warning_text(rest_text)
            context.user_data.pop("pending_forbidden_books", None)
            context.user_data.pop("awaiting_forbidden_books_input_until", None)
            await target_message.reply_text(MESSAGES[lang]["forbidden_books_warning_saved"])
            return
        current_text = await _get_forbidden_warning_text(lang)
        _set_pending_forbidden_books_action(context, "warning")
        await target_message.reply_text(
            MESSAGES[lang]["forbidden_books_warning_prompt"].format(text=html.escape(current_text)),
            parse_mode="HTML",
        )
        return

    count = await _save_forbidden_book_titles(raw_text, update.effective_user.id if update.effective_user else None)
    context.user_data.pop("pending_forbidden_books", None)
    context.user_data.pop("awaiting_forbidden_books_input_until", None)
    if count <= 0:
        await target_message.reply_text(MESSAGES[lang]["forbidden_books_invalid"])
        return
    await target_message.reply_text(MESSAGES[lang]["forbidden_books_saved"].format(count=count))


async def forbidden_books_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _forbidden_books_command_impl)


async def drafttime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await safe_reply(update, MESSAGES[lang].get("owner_only", MESSAGES[lang]["admin_only"]))
        return
    await target_message.reply_text(_build_drafttime_report_text(lang))


async def negalert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await safe_reply(update, MESSAGES[lang].get("owner_only", MESSAGES[lang]["admin_only"]))
        return

    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text(_build_negalert_report_text(lang))
        return

    first = args[0].lower()
    if first in {"reset", "default"}:
        value = _reset_negative_reaction_alert_threshold()
        await target_message.reply_text(
            MESSAGES[lang].get(
                "negalert_reset_done",
                "✅ Negative reaction alert threshold reset to default: {value}",
            ).format(value=value)
        )
        return

    try:
        value = int(first)
    except Exception:
        await target_message.reply_text(
            MESSAGES[lang].get(
                "negalert_invalid",
                "⚠️ Send a whole number or `reset`.\nExample:\n/negalert 10",
            )
        )
        return

    if value < 1:
        await target_message.reply_text(
            MESSAGES[lang].get(
                "negalert_invalid",
                "⚠️ Send a whole number or `reset`.\nExample:\n/negalert 10",
            )
        )
        return

    applied = _set_negative_reaction_alert_threshold(value)
    await target_message.reply_text(
        MESSAGES[lang].get(
            "negalert_set_done",
            "✅ Negative reaction alert threshold set to: {value}",
        ).format(value=applied)
    )


def _parse_stat_range_token(raw: str) -> tuple[int, int] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    match = re.fullmatch(r"(-?\d+)\s*[-:]\s*(-?\d+)", text)
    if not match:
        return None
    left = int(match.group(1))
    right = int(match.group(2))
    low, high = sorted((left, right))
    if high < 0:
        return None
    return max(0, low), max(0, high)


def _parse_seedbookstats_ranges(args: list[str]) -> dict[str, tuple[int, int]] | None:
    defaults = {
        "downloads": (20, 50),
        "favorites": (10, 60),
        "positive": (10, 60),
        "negative": (0, 10),
    }
    if not args:
        return defaults

    if any("=" in token for token in args):
        parsed = dict(defaults)
        aliases = {
            "downloads": "downloads",
            "download": "downloads",
            "d": "downloads",
            "favorites": "favorites",
            "favorite": "favorites",
            "fav": "favorites",
            "f": "favorites",
            "positive": "positive",
            "positives": "positive",
            "positive_reactions": "positive",
            "reactions": "positive",
            "pos": "positive",
            "p": "positive",
            "negative": "negative",
            "negatives": "negative",
            "negative_reactions": "negative",
            "dislike": "negative",
            "dislikes": "negative",
            "neg": "negative",
            "n": "negative",
        }
        for token in args:
            if "=" not in token:
                return None
            key_raw, value_raw = token.split("=", 1)
            key = aliases.get(str(key_raw or "").strip().lower())
            if not key:
                return None
            value = _parse_stat_range_token(value_raw)
            if value is None:
                return None
            parsed[key] = value
        return parsed

    ranges = [_parse_stat_range_token(token) for token in args]
    if any(item is None for item in ranges):
        return None
    resolved = [item for item in ranges if item is not None]
    if len(resolved) == 2:
        downloads, positive = resolved
        return {
            "downloads": downloads,
            "favorites": positive,
            "positive": positive,
            "negative": defaults["negative"],
        }
    if len(resolved) == 3:
        downloads, positive, negative = resolved
        return {
            "downloads": downloads,
            "favorites": positive,
            "positive": positive,
            "negative": negative,
        }
    if len(resolved) == 4:
        downloads, favorites, positive, negative = resolved
        return {
            "downloads": downloads,
            "favorites": favorites,
            "positive": positive,
            "negative": negative,
        }
    return None


async def seedbookstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await safe_reply(update, MESSAGES[lang].get("owner_only", MESSAGES[lang]["admin_only"]))
        return

    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if args and args[0].lower() in {"reset", "clear"}:
        cleared = await run_blocking(db_clear_all_book_display_adjustments)
        await target_message.reply_text(
            MESSAGES[lang].get(
                "seedbookstats_reset_done",
                "✅ Randomized display stats cleared.\nCounter rows removed: {counter_rows}\nReaction rows removed: {reaction_rows}",
            ).format(
                counter_rows=int((cleared or {}).get("counter_rows", 0) or 0),
                reaction_rows=int((cleared or {}).get("reaction_rows", 0) or 0),
            )
        )
        return
    context.user_data["pending_seedbookstats"] = {
        "expires_at": time.time() + 300,
    }
    await target_message.reply_text(
        MESSAGES[lang].get(
            "seedbookstats_prompt",
            "🎲 Endi stat diapazonlarini yuboring.\n\nMisollar:\n<blockquote>20-50 100-3000</blockquote>\n<blockquote>20-50 100-3000 0-10</blockquote>\n<blockquote>20-50 50-200 100-3000 0-10</blockquote>\n<blockquote>downloads=20-50 favorites=50-200 positive=100-3000 negative=0-10</blockquote>\n\nBekor qilish: cancel",
        ),
        parse_mode="HTML",
    )


def _white_label_feature_error() -> str | None:
    if not ENABLE_WHITE_LABEL:
        return "⚠️ White-label connected bots are disabled. Set ENABLE_WHITE_LABEL=true first."
    config_errors = validate_white_label_config()
    if config_errors:
        return f"⚠️ White-label config error: {config_errors[0]}"
    if not wl_crypto_available():
        return "⚠️ cryptography is not installed. Add the dependency before using white-label bot tokens."
    return None


_WL_REQUEST_PAGE_SIZE = 10
_WL_CONNECTED_BOT_PAGE_SIZE = 10
_WL_PUBLIC_TOKEN_TTL_SECONDS = 10 * 60
_WL_OWNER_FLOW_TTL_SECONDS = 10 * 60
_WL_TRIAL_DAYS = 3
_WL_TRIAL_DAILY_SEARCH_LIMIT = 100
_WL_TRIAL_DAILY_SEND_LIMIT = 20
_WL_TRIAL_PER_MINUTE_SEND_LIMIT = 10


def _wl_text(lang: str, key: str, default: str) -> str:
    return MESSAGES.get(lang, MESSAGES["en"]).get(key, default)


def _wl_user_line(user: Any) -> str:
    if not user:
        return "-"
    first_name = str(getattr(user, "first_name", "") or "").strip()
    username = str(getattr(user, "username", "") or "").strip()
    user_id = int(getattr(user, "id", 0) or 0)
    label = first_name or "User"
    if username:
        label = f"{label} (@{username})"
    if user_id:
        label = f"{label} / ID {user_id}"
    return label


def _wl_requester_line(row: dict | None) -> str:
    if not row:
        return "-"
    first_name = str((row or {}).get("requesting_first_name") or (row or {}).get("requested_by_first_name") or "").strip() or "User"
    username = str((row or {}).get("requesting_username") or (row or {}).get("requested_by_username") or "").strip()
    user_id = int((row or {}).get("requesting_user_id") or (row or {}).get("requested_by_user_id") or 0)
    label = first_name
    if username:
        label = f"{label} (@{username})"
    if user_id:
        label = f"{label} / ID {user_id}"
    return label


def _wl_bot_reference(row: dict | None) -> str:
    username = str((row or {}).get("bot_username") or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    return str((row or {}).get("id") or "-")


def _wl_format_dt(value: Any) -> str:
    if not value:
        return "-"
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return str(value)
    return str(value)[:16]


def _wl_escape(value: Any) -> str:
    return html.escape(str(value or "").strip())


def _wl_token_request_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_wl_text(lang, "wlreq_send_token_button", "🔐 Send token"), callback_data="wlreq:sendtoken")],
            [InlineKeyboardButton(_wl_text(lang, "wlreq_cancel_button", "❌ Cancel"), callback_data="wlreq:cancel")],
        ]
    )


def _wl_owner_menu_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_wl_text(lang, "wlreq_pending_title", "🤖 Connection requests"), callback_data="wlreqpage:0")],
            [InlineKeyboardButton(_wl_text(lang, "wlreq_connected_title", "🤖 Connected bots"), callback_data="wlbotpage:0")],
        ]
    )


def _wl_number_rows(items: list[dict], prefix: str, id_key: str = "id") -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, item in enumerate(items, start=1):
        item_id = str((item or {}).get(id_key) or "").strip()
        if not item_id:
            continue
        row.append(InlineKeyboardButton(str(idx), callback_data=f"{prefix}:{item_id}"))
        if len(row) >= 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


async def _reply_or_edit_wl(update: Update, text: str, *, reply_markup: InlineKeyboardMarkup | None = None, parse_mode: str | None = "HTML") -> None:
    query = update.callback_query
    if query and query.message:
        try:
            await query.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise
            try:
                await query.message.edit_reply_markup(reply_markup=reply_markup)
                return
            except Exception:
                pass
    message = update.message or (query.message if query else None)
    if message:
        await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)


async def _start_white_label_public_request_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> None:
    feature_error = _white_label_feature_error()
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    context.user_data["pending_wl_connect_request"] = {"expires_at": time.time() + _WL_PUBLIC_TOKEN_TTL_SECONDS}
    await target_message.reply_text(
        _wl_text(lang, "wlreq_guide", "You can connect your own Telegram book searcher bot."),
        reply_markup=_wl_token_request_keyboard(lang),
    )


def _looks_like_bot_token(raw: str) -> bool:
    return bool(re.match(r"^\d{6,12}:[A-Za-z0-9_-]{20,}$", str(raw or "").strip()))


async def _notify_owner_connected_bot_request(context: ContextTypes.DEFAULT_TYPE, request_row: dict, lang: str) -> None:
    if not OWNER_ID:
        return
    text = _wl_text(
        lang,
        "wlreq_owner_notify",
        "🤖 New white-label bot connection request.\n\nUser: {user}\nBot: @{bot_username}\nBot name: {bot_name}\n\nOpen: /botconnectreq",
    ).format(
        user=_wl_escape(_wl_requester_line(request_row)),
        bot_username=_wl_escape(str(request_row.get("bot_username") or "").lstrip("@")),
        bot_name=_wl_escape(str(request_row.get("bot_first_name") or "-")),
    )
    try:
        await context.bot.send_message(
            chat_id=int(OWNER_ID),
            text=text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton(_wl_text(lang, "wlreq_owner_view_button", "👁 View"), callback_data=f"wlreqview:{request_row.get('id')}")]]
            ),
        )
    except Exception:
        logger.debug("Failed to notify owner about connected bot request", exc_info=True)


async def _perform_white_label_public_request_token(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    raw_token: str,
    lang: str,
) -> None:
    if not update.message or not update.effective_user:
        return
    token = str(raw_token or "").strip()
    if not _looks_like_bot_token(token):
        await update.message.reply_text(_wl_text(lang, "wlreq_invalid_token", "⚠️ Invalid bot token."))
        return
    try:
        verified = await _verify_white_label_bot_token(token)
        if int(verified.get("bot_telegram_id") or 0) == int(getattr(context.bot, "id", 0) or 0):
            await update.message.reply_text(_wl_text(lang, "wlreq_invalid_token", "⚠️ Invalid bot token."))
            return
        encrypted = encrypt_bot_token(token, CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        fingerprint = fingerprint_bot_token(token, CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        existing = await run_blocking(
            db_find_existing_connected_bot_request_or_bot,
            token_fingerprint=fingerprint,
            bot_telegram_id=int(verified.get("bot_telegram_id") or 0),
        )
        if existing:
            await update.message.reply_text(_wl_text(lang, "wlreq_duplicate", "⚠️ A request for this bot already exists."))
            return
        request_row = await run_blocking(
            db_create_connected_bot_request,
            requesting_user_id=int(update.effective_user.id),
            requesting_username=str(update.effective_user.username or "").strip() or None,
            requesting_first_name=str(update.effective_user.first_name or "").strip() or None,
            bot_telegram_id=int(verified.get("bot_telegram_id") or 0),
            bot_username=str(verified.get("bot_username") or "").strip(),
            bot_first_name=str(verified.get("bot_first_name") or "").strip() or None,
            bot_token_encrypted=encrypted,
            bot_token_fingerprint=fingerprint,
            token_masked=describe_token_for_owner(token),
        )
        context.user_data.pop("pending_wl_connect_request", None)
        await update.message.reply_text(_wl_text(lang, "wlreq_sent", "✅ Your request has been sent to the owner."))
        await _notify_owner_connected_bot_request(context, dict(request_row or {}), lang)
    except Exception as exc:
        logger.warning("Public white-label bot request failed: %s", wl_redact_token_like_strings(str(exc)), exc_info=True)
        await update.message.reply_text(_wl_text(lang, "wlreq_invalid_token", "⚠️ Invalid bot token."))


async def _show_white_label_owner_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> None:
    feature_error = _white_label_feature_error()
    if feature_error:
        await _reply_or_edit_wl(update, feature_error, parse_mode=None)
        return
    pending_count = await run_blocking(db_count_connected_bot_requests, WL_REQUEST_STATUS_PENDING)
    connected_count = await run_blocking(db_count_connected_bots)
    text = "\n".join(
        [
            "🤖 <b>White Label</b>",
            "",
            f"Pending requests: <b>{int(pending_count or 0)}</b>",
            f"Connected bots: <b>{int(connected_count or 0)}</b>",
            "",
            "Choose a section below.",
        ]
    )
    await _reply_or_edit_wl(update, text, reply_markup=_wl_owner_menu_keyboard(lang))


async def _build_wl_requests_list_view(lang: str, page: int) -> tuple[str, InlineKeyboardMarkup]:
    page = max(0, int(page or 0))
    total = int(await run_blocking(db_count_connected_bot_requests, WL_REQUEST_STATUS_PENDING) or 0)
    pages = max(1, math.ceil(total / _WL_REQUEST_PAGE_SIZE))
    if page >= pages:
        page = pages - 1
    rows = await run_blocking(
        db_list_connected_bot_requests,
        status=WL_REQUEST_STATUS_PENDING,
        limit=_WL_REQUEST_PAGE_SIZE,
        offset=page * _WL_REQUEST_PAGE_SIZE,
    )
    lines = [
        f"<b>{_wl_escape(_wl_text(lang, 'wlreq_pending_title', '🤖 Connection requests'))}</b>",
        f"Jami: {total} • Sahifa {page + 1}/{pages}",
        "",
    ]
    if not rows:
        lines.append("📭 Pending requests are empty.")
    else:
        for idx, row in enumerate(rows, start=1):
            lines.extend(
                [
                    f"{idx}. {_wl_escape(_wl_bot_reference(row))} — {_wl_escape(row.get('bot_first_name') or '-')}",
                    f"   User: {_wl_escape(_wl_requester_line(row))}",
                    f"   Date: {_wl_escape(_wl_format_dt(row.get('created_at')))}",
                    f"   Status: {_wl_escape(row.get('status') or '-')}",
                ]
            )
    button_rows = _wl_number_rows(list(rows or []), "wlreqview")
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(_wl_text(lang, "wlreq_prev_button", "⬅️ Prev"), callback_data=f"wlreqpage:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(_wl_text(lang, "wlreq_next_button", "Next ➡️"), callback_data=f"wlreqpage:{page + 1}"))
    if nav:
        button_rows.append(nav)
    button_rows.append(
        [
            InlineKeyboardButton(_wl_text(lang, "wlreq_refresh_button", "🔄 Refresh"), callback_data=f"wlreqpage:{page}"),
            InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlmenu"),
        ]
    )
    return "\n".join(lines), InlineKeyboardMarkup(button_rows)


async def _show_wl_requests_page(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, page: int = 0) -> None:
    del context
    text, markup = await _build_wl_requests_list_view(lang, page)
    await _reply_or_edit_wl(update, text, reply_markup=markup)


def _build_wl_request_detail_text(row: dict) -> str:
    return "\n".join(
        [
            "🤖 <b>Connection request</b>",
            "",
            f"Request ID: <code>{_wl_escape(row.get('id'))}</code>",
            f"Requester: {_wl_escape(_wl_requester_line(row))}",
            f"Requester ID: <code>{int(row.get('requesting_user_id') or 0)}</code>",
            "",
            f"Bot ID: <code>{int(row.get('bot_telegram_id') or 0)}</code>",
            f"Bot: {_wl_escape(_wl_bot_reference(row))}",
            f"Bot name: {_wl_escape(row.get('bot_first_name') or '-')}",
            f"Status: <b>{_wl_escape(row.get('status') or '-')}</b>",
            f"Requested: {_wl_escape(_wl_format_dt(row.get('created_at')))}",
            f"Token: <code>{_wl_escape(row.get('token_masked') or 'masked')}</code>",
        ]
    )


async def _show_wl_request_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, request_id: str) -> None:
    del context
    row = await run_blocking(db_get_connected_bot_request_by_id, str(request_id or "").strip())
    if not row:
        await _reply_or_edit_wl(update, "⚠️ Request not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlreqpage:0")]]))
        return
    buttons: list[list[InlineKeyboardButton]] = []
    if str(row.get("status") or "").upper() == WL_REQUEST_STATUS_PENDING:
        buttons.append(
            [
                InlineKeyboardButton(_wl_text(lang, "wlreq_accept_button", "✅ Accept"), callback_data=f"wlreqaccept:{row.get('id')}"),
                InlineKeyboardButton(_wl_text(lang, "wlreq_reject_button", "❌ Reject"), callback_data=f"wlreqreject:{row.get('id')}"),
            ]
        )
    buttons.append([InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlreqpage:0")])
    await _reply_or_edit_wl(update, _build_wl_request_detail_text(dict(row)), reply_markup=InlineKeyboardMarkup(buttons))


async def _validate_wl_cache_channel_for_request(
    context: ContextTypes.DEFAULT_TYPE,
    request_row: dict,
    cache_channel_id: int,
) -> tuple[bool, str | None, str | None]:
    decrypted_token = decrypt_bot_token(str(request_row.get("bot_token_encrypted") or ""), CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
    connected_bot_client = wl_build_bot_client(decrypted_token)
    try:
        main_member = await context.bot.get_chat_member(cache_channel_id, context.bot.id)
        connected_member = await connected_bot_client.get_chat_member(cache_channel_id, int(request_row.get("bot_telegram_id") or 0))
        chat_info = await context.bot.get_chat(cache_channel_id)
        main_status = str(getattr(main_member, "status", "") or "").lower()
        connected_status = str(getattr(connected_member, "status", "") or "").lower()
        if main_status not in {"administrator", "creator"} or connected_status not in {"administrator", "creator"}:
            return False, None, "Both the main bot and the connected bot must be admins in the cache channel."
        return True, str(getattr(chat_info, "username", "") or "").strip() or None, None
    finally:
        try:
            await connected_bot_client.shutdown()
        except Exception:
            pass


async def _perform_wl_accept_cache_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    cache_channel_raw: str,
    lang: str,
) -> bool:
    pending = context.user_data.get("pending_wl_accept_cache_channel")
    if not pending:
        return False
    if time.time() > float((pending or {}).get("expires_at", 0) or 0):
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        return False
    if not update.message:
        return True
    text = str(cache_channel_raw or "").strip()
    if text.lower() in {"cancel", "stop", "/cancel"}:
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        await update.message.reply_text(_wl_text(lang, "menu_flow_cancelled", "❌ Cancelled."))
        return True
    try:
        cache_channel_id = int(text)
    except Exception:
        await update.message.reply_text("⚠️ Send a valid cache channel ID, for example -1001234567890.")
        return True
    request_id = str((pending or {}).get("request_id") or "").strip()
    request_row = await run_blocking(db_get_connected_bot_request_by_id, request_id)
    if not request_row:
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        await update.message.reply_text("⚠️ Request not found.")
        return True
    try:
        ok, cache_username, error_text = await _validate_wl_cache_channel_for_request(context, dict(request_row), cache_channel_id)
        if not ok:
            await update.message.reply_text(f"⚠️ {error_text or 'Cache channel could not be verified.'}")
            return True
        connected_bot = await run_blocking(
            db_accept_connected_bot_request,
            request_id,
            accepted_by_owner_id=int(update.effective_user.id or 0),
            cache_channel_id=cache_channel_id,
            cache_channel_username=cache_username,
            trial_days=_WL_TRIAL_DAYS,
            daily_search_limit=_WL_TRIAL_DAILY_SEARCH_LIMIT,
            daily_send_limit=_WL_TRIAL_DAILY_SEND_LIMIT,
            per_minute_send_limit=_WL_TRIAL_PER_MINUTE_SEND_LIMIT,
        )
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        if not connected_bot:
            await update.message.reply_text("⚠️ Request could not be accepted. It may already be processed.")
            return True
        try:
            await context.bot.send_message(
                chat_id=int(request_row.get("requesting_user_id") or 0),
                text=_wl_text(lang, "wlreq_approved_user", "✅ Your bot was approved. It will be started soon."),
            )
        except Exception:
            logger.debug("Failed to notify connected bot requester about approval", exc_info=True)
        await update.message.reply_text(
            "\n".join(
                [
                    _wl_text(lang, "wlreq_saved", "✅ Bot saved and added on TRIAL plan."),
                    _wl_text(lang, "wlreq_trial_started", "🎁 Trial started: {days} days.").format(days=_WL_TRIAL_DAYS),
                    f"Bot: {format_connected_bot_reference(connected_bot)}",
                    f"Cache channel: {cache_channel_id}",
                    "Status: STOPPED. Press Start from /connected_bots.",
                ]
            )
        )
        return True
    except Exception as exc:
        logger.error("White-label request accept failed: %s", wl_redact_token_like_strings(str(exc)), exc_info=True)
        await update.message.reply_text(f"⚠️ Could not accept request.\n{wl_redact_token_like_strings(str(exc))}")
        return True


async def _perform_wl_reject_reason(update: Update, context: ContextTypes.DEFAULT_TYPE, *, reason_raw: str, lang: str) -> bool:
    pending = context.user_data.get("pending_wl_reject_request")
    if not pending:
        return False
    if time.time() > float((pending or {}).get("expires_at", 0) or 0):
        context.user_data.pop("pending_wl_reject_request", None)
        return False
    if not update.message:
        return True
    text = str(reason_raw or "").strip()
    if text.lower() in {"cancel", "stop", "/cancel"}:
        context.user_data.pop("pending_wl_reject_request", None)
        await update.message.reply_text(_wl_text(lang, "menu_flow_cancelled", "❌ Cancelled."))
        return True
    reason = "" if text.lower() == "/skip" else text
    request_id = str((pending or {}).get("request_id") or "").strip()
    request_row = await run_blocking(db_get_connected_bot_request_by_id, request_id)
    rejected = await run_blocking(db_reject_connected_bot_request, request_id, rejected_by_owner_id=int(update.effective_user.id or 0), reason=reason)
    context.user_data.pop("pending_wl_reject_request", None)
    if not rejected:
        await update.message.reply_text("⚠️ Request could not be rejected. It may already be processed.")
        return True
    try:
        notify_text = (
            _wl_text(lang, "wlreq_rejected_user_reason", "❌ Your bot connection request was rejected.\nReason: {reason}").format(reason=reason)
            if reason
            else _wl_text(lang, "wlreq_rejected_user", "❌ Your bot connection request was rejected.")
        )
        await context.bot.send_message(chat_id=int((request_row or rejected).get("requesting_user_id") or 0), text=notify_text)
    except Exception:
        logger.debug("Failed to notify connected bot requester about rejection", exc_info=True)
    await update.message.reply_text("✅ Request rejected.")
    return True


async def _build_wl_connected_bots_view(context: ContextTypes.DEFAULT_TYPE, lang: str, page: int) -> tuple[str, InlineKeyboardMarkup]:
    page = max(0, int(page or 0))
    total = int(await run_blocking(db_count_connected_bots) or 0)
    pages = max(1, math.ceil(total / _WL_CONNECTED_BOT_PAGE_SIZE))
    if page >= pages:
        page = pages - 1
    rows = await run_blocking(db_list_connected_bots_page, limit=_WL_CONNECTED_BOT_PAGE_SIZE, offset=page * _WL_CONNECTED_BOT_PAGE_SIZE)
    lines = [
        f"<b>{_wl_escape(_wl_text(lang, 'wlreq_connected_title', '🤖 Connected bots'))}</b>",
        f"Jami: {total} • Sahifa {page + 1}/{pages}",
        "",
    ]
    if not rows:
        lines.append("📭 Connected bots are empty.")
    else:
        for idx, row in enumerate(rows, start=1):
            usage = await run_blocking(db_get_connected_bot_usage, str(row.get("id") or ""))
            runtime_status = await wl_get_runtime_status(context.application.bot_data, str(row.get("id") or ""))
            is_running = bool(runtime_status.get("running"))
            status_text = "RUNNING" if is_running else ("STOPPED" if str(row.get("status") or "").upper() == WL_STATUS_SUSPENDED else str(row.get("status") or "-"))
            lines.extend(
                [
                    f"{idx}. {_wl_escape(_wl_bot_reference(row))} — {_wl_escape(row.get('bot_first_name') or '-')}",
                    f"   Status: {status_text} • Plan: {_wl_escape(row.get('plan') or '-')}",
                    f"   Trial end: {_wl_escape(_wl_format_dt(row.get('trial_ends_at')))}",
                    f"   Today: searches {int((usage or {}).get('searches') or 0)} / sends {int((usage or {}).get('sends') or 0)}",
                ]
            )
    button_rows = _wl_number_rows(list(rows or []), "wlbotview")
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(_wl_text(lang, "wlreq_prev_button", "⬅️ Prev"), callback_data=f"wlbotpage:{page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(_wl_text(lang, "wlreq_next_button", "Next ➡️"), callback_data=f"wlbotpage:{page + 1}"))
    if nav:
        button_rows.append(nav)
    button_rows.append(
        [
            InlineKeyboardButton(_wl_text(lang, "wlreq_refresh_button", "🔄 Refresh"), callback_data=f"wlbotpage:{page}"),
            InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlmenu"),
        ]
    )
    return "\n".join(lines), InlineKeyboardMarkup(button_rows)


async def _show_wl_connected_bots_page(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, page: int = 0) -> None:
    text, markup = await _build_wl_connected_bots_view(context, lang, page)
    await _reply_or_edit_wl(update, text, reply_markup=markup)


async def _build_wl_connected_bot_detail(context: ContextTypes.DEFAULT_TYPE, row: dict, lang: str) -> tuple[str, InlineKeyboardMarkup]:
    usage = await run_blocking(db_get_connected_bot_usage, str(row.get("id") or ""))
    runtime_status = await wl_get_runtime_status(context.application.bot_data, str(row.get("id") or ""))
    running = bool(runtime_status.get("running"))
    status_text = "RUNNING" if running else ("STOPPED" if str(row.get("status") or "").upper() == WL_STATUS_SUSPENDED else str(row.get("status") or "-"))
    text = "\n".join(
        [
            "🤖 <b>Connected bot</b>",
            "",
            f"ID: <code>{_wl_escape(row.get('id'))}</code>",
            f"Bot: {_wl_escape(_wl_bot_reference(row))}",
            f"Bot name: {_wl_escape(row.get('bot_first_name') or '-')}",
            f"Telegram ID: <code>{int(row.get('bot_telegram_id') or 0)}</code>",
            f"Requester: {_wl_escape(_wl_requester_line(row))}",
            f"Status: <b>{_wl_escape(status_text)}</b>",
            f"Plan: <b>{_wl_escape(row.get('plan') or '-')}</b>",
            f"Subscription: {_wl_escape(row.get('subscription_status') or '-')}",
            f"Trial end: {_wl_escape(_wl_format_dt(row.get('trial_ends_at')))}",
            f"Cache channel: <code>{_wl_escape(row.get('cache_channel_id') or '-')}</code>",
            f"Today: searches {int((usage or {}).get('searches') or 0)} / sends {int((usage or {}).get('sends') or 0)}",
            f"Cache: hits {int((usage or {}).get('cache_hits') or 0)} / misses {int((usage or {}).get('cache_misses') or 0)}",
            f"Last error: {_wl_escape(row.get('last_error') or '-')}",
            f"Token: <code>masked</code>",
        ]
    )
    bot_id = str(row.get("id") or "")
    buttons = [
        [
            InlineKeyboardButton(_wl_text(lang, "wlbot_start_button", "▶️ Start"), callback_data=f"wlbotstart:{bot_id}"),
            InlineKeyboardButton(_wl_text(lang, "wlbot_stop_button", "⏸ Stop"), callback_data=f"wlbotstop:{bot_id}"),
        ],
        [
            InlineKeyboardButton(_wl_text(lang, "wlbot_restart_button", "🔄 Restart"), callback_data=f"wlbotrestart:{bot_id}"),
            InlineKeyboardButton(_wl_text(lang, "wlbot_test_cache_button", "🧪 Test cache"), callback_data=f"wlbottest:{bot_id}"),
        ],
        [
            InlineKeyboardButton(_wl_text(lang, "wlbot_suspend_button", "⛔ Suspend"), callback_data=f"wlbotsuspend:{bot_id}"),
            InlineKeyboardButton(_wl_text(lang, "wlbot_resume_button", "✅ Resume"), callback_data=f"wlbotresume:{bot_id}"),
        ],
        [InlineKeyboardButton(_wl_text(lang, "wlbot_delete_button", "🗑 Delete"), callback_data=f"wlbotdelete:{bot_id}")],
        [InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlbotpage:0")],
    ]
    return text, InlineKeyboardMarkup(buttons)


async def _show_wl_connected_bot_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, connected_bot_id: str) -> None:
    row = await run_blocking(db_get_connected_bot_by_id, str(connected_bot_id or "").strip())
    if not row:
        await _reply_or_edit_wl(update, "⚠️ Connected bot not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data="wlbotpage:0")]]))
        return
    text, markup = await _build_wl_connected_bot_detail(context, dict(row), lang)
    await _reply_or_edit_wl(update, text, reply_markup=markup)


async def _start_wl_connected_bot_from_row(context: ContextTypes.DEFAULT_TYPE, row: dict) -> dict:
    if not int(row.get("cache_channel_id") or 0):
        return {"ok": False, "error": "cache channel is not configured"}
    updated = await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ACTIVE, clear_error=True)
    result = await wl_start_runtime(context.application.bot_data, dict(updated or row))
    if not result.get("ok"):
        await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ERROR, last_error=str(result.get("error") or "runtime start failed"))
    return result


async def _verify_white_label_bot_token(token: str) -> dict[str, Any]:
    clean_token = str(token or "").strip()
    if not clean_token or ":" not in clean_token:
        raise ValueError("bot token looks invalid")
    bot_client = wl_build_bot_client(clean_token)
    try:
        me = await bot_client.get_me()
        return {
            "bot_telegram_id": int(me.id or 0),
            "bot_username": str(me.username or "").strip(),
            "bot_first_name": str(me.first_name or "").strip(),
        }
    finally:
        try:
            await bot_client.shutdown()
        except Exception:
            pass


async def _perform_white_label_add_bot(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    raw_token: str,
    lang: str,
) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    token = str(raw_token or "").strip()
    if not token or ":" not in token:
        await target_message.reply_text("⚠️ Send a valid Telegram bot token from BotFather.")
        return
    try:
        verified = await _verify_white_label_bot_token(token)
        if int(verified.get("bot_telegram_id") or 0) == int(getattr(context.bot, "id", 0) or 0):
            await target_message.reply_text("⚠️ This token belongs to the main bot. Connect a different bot token.")
            return
        encrypted = encrypt_bot_token(token, CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        fingerprint = fingerprint_bot_token(token, CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        row = await run_blocking(
            db_upsert_connected_bot,
            owner_telegram_id=int(update.effective_user.id or 0),
            bot_telegram_id=int(verified.get("bot_telegram_id") or 0),
            bot_username=str(verified.get("bot_username") or "").strip(),
            bot_first_name=str(verified.get("bot_first_name") or "").strip() or None,
            bot_token_encrypted=encrypted,
            bot_token_fingerprint=fingerprint,
            status=WL_STATUS_SUSPENDED,
            plan="MANUAL",
        )
        context.user_data.pop("pending_wl_add_bot", None)
        await target_message.reply_text(
            "\n".join(
                [
                    "✅ Connected bot saved.",
                    f"Bot: {format_connected_bot_reference(row)}",
                    f"ID: {row.get('id')}",
                    f"Token: {describe_token_for_owner(token)}",
                    f"Status: {row.get('status')}",
                    "Next: set a private cache channel with /wl_set_cache_channel",
                ]
            )
        )
    except Exception as exc:
        logger.error("White-label add bot failed: %s", wl_redact_token_like_strings(str(exc)), exc_info=True)
        await target_message.reply_text(f"⚠️ Could not add connected bot.\n{wl_redact_token_like_strings(str(exc))}")


async def _perform_white_label_set_cache_channel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    identifier: str,
    channel_id_raw: str,
    lang: str,
) -> None:
    del lang
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(identifier))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found. Use /wl_list_bots first.")
        return
    try:
        cache_channel_id = int(str(channel_id_raw or "").strip())
    except Exception:
        await target_message.reply_text("⚠️ Send a valid private cache channel ID, for example -1001234567890.")
        return
    try:
        decrypted_token = decrypt_bot_token(str(row.get("bot_token_encrypted") or ""), CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        connected_bot_client = wl_build_bot_client(decrypted_token)
        try:
            main_member = await context.bot.get_chat_member(cache_channel_id, context.bot.id)
            connected_member = await connected_bot_client.get_chat_member(cache_channel_id, int(row.get("bot_telegram_id") or 0))
            chat_info = await context.bot.get_chat(cache_channel_id)
            main_status = str(getattr(main_member, "status", "") or "").lower()
            connected_status = str(getattr(connected_member, "status", "") or "").lower()
            if main_status not in {"administrator", "creator"} or connected_status not in {"administrator", "creator"}:
                await target_message.reply_text(
                    "⚠️ Both the main bot and the connected bot must be admins in that private cache channel before saving it."
                )
                return
            updated = await run_blocking(
                db_update_connected_bot_cache_channel,
                str(row.get("id") or ""),
                int(cache_channel_id),
                str(getattr(chat_info, "username", "") or "").strip() or None,
            )
            context.user_data.pop("pending_wl_set_cache_channel", None)
            await target_message.reply_text(
                "\n".join(
                    [
                        "✅ Cache channel saved.",
                        f"Bot: {format_connected_bot_reference(updated or row)}",
                        f"Cache channel: {cache_channel_id}",
                        "Next: run /wl_activate_bot or /wl_test_cache",
                    ]
                )
            )
        finally:
            try:
                await connected_bot_client.shutdown()
            except Exception:
                pass
    except Exception as exc:
        logger.error("White-label cache channel setup failed: %s", wl_redact_token_like_strings(str(exc)), exc_info=True)
        await target_message.reply_text(f"⚠️ Could not verify the cache channel.\n{wl_redact_token_like_strings(str(exc))}")


async def _pick_white_label_test_book() -> dict[str, Any] | None:
    candidates = await run_blocking(db_get_random_books, 10, True)
    for book in list(candidates or []):
        path = str((book or {}).get("path") or "").strip().lower()
        file_id = str((book or {}).get("file_id") or "").strip()
        if path and not path.endswith(".pdf"):
            continue
        if path or file_id:
            return dict(book)
    return None


async def process_pending_white_label_owner_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message or not update.effective_user:
        return False
    user_is_owner = _is_owner_user(update.effective_user.id)
    had_pending = bool(
        context.user_data.get("pending_wl_connect_request")
        or context.user_data.get("pending_wl_accept_cache_channel")
        or context.user_data.get("pending_wl_reject_request")
        or context.user_data.get("pending_wl_add_bot")
        or context.user_data.get("pending_wl_set_cache_channel")
    )
    feature_error = _white_label_feature_error()
    if feature_error:
        context.user_data.pop("pending_wl_connect_request", None)
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        context.user_data.pop("pending_wl_reject_request", None)
        context.user_data.pop("pending_wl_add_bot", None)
        context.user_data.pop("pending_wl_set_cache_channel", None)
        if had_pending:
            await update.message.reply_text(feature_error)
            return True
        return False

    pending_public = context.user_data.get("pending_wl_connect_request")
    if pending_public:
        if time.time() > float((pending_public or {}).get("expires_at", 0) or 0):
            context.user_data.pop("pending_wl_connect_request", None)
            return False
        token_text = str(update.message.text or "").strip()
        if _main_menu_text_action(token_text):
            return False
        if token_text.lower() in {"cancel", "stop", "/cancel"}:
            context.user_data.pop("pending_wl_connect_request", None)
            await update.message.reply_text(_wl_text(lang, "wlreq_cancelled", "❌ Bot connection request cancelled."))
            return True
        await _perform_white_label_public_request_token(update, context, raw_token=token_text, lang=lang)
        return True

    if not user_is_owner:
        return False

    if context.user_data.get("pending_wl_accept_cache_channel"):
        return await _perform_wl_accept_cache_channel(update, context, cache_channel_raw=str(update.message.text or ""), lang=lang)

    if context.user_data.get("pending_wl_reject_request"):
        return await _perform_wl_reject_reason(update, context, reason_raw=str(update.message.text or ""), lang=lang)

    pending_add = context.user_data.get("pending_wl_add_bot")
    if pending_add:
        if time.time() > float((pending_add or {}).get("expires_at", 0) or 0):
            context.user_data.pop("pending_wl_add_bot", None)
            return False
        token_text = str(update.message.text or "").strip()
        if token_text.lower() in {"cancel", "stop", "/cancel"}:
            context.user_data.pop("pending_wl_add_bot", None)
            await update.message.reply_text("✖️ White-label bot token input cancelled.")
            return True
        await _perform_white_label_add_bot(update, context, raw_token=token_text, lang=lang)
        return True

    pending_cache = context.user_data.get("pending_wl_set_cache_channel")
    if pending_cache:
        if time.time() > float((pending_cache or {}).get("expires_at", 0) or 0):
            context.user_data.pop("pending_wl_set_cache_channel", None)
            return False
        owner_text = str(update.message.text or "").strip()
        if owner_text.lower() in {"cancel", "stop", "/cancel"}:
            context.user_data.pop("pending_wl_set_cache_channel", None)
            await update.message.reply_text("✖️ White-label cache channel input cancelled.")
            return True
        identifier = str((pending_cache or {}).get("identifier") or "").strip()
        if not identifier:
            parts = owner_text.split(None, 1)
            if len(parts) < 2:
                await update.message.reply_text("⚠️ Send: @botusername -1001234567890")
                return True
            identifier, channel_id_raw = parts[0], parts[1]
        else:
            channel_id_raw = owner_text
        await _perform_white_label_set_cache_channel(
            update,
            context,
            identifier=identifier,
            channel_id_raw=channel_id_raw,
            lang=lang,
        )
        return True

    return False


async def _white_label_list_bots_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    await _show_wl_connected_bots_page(update, context, lang, 0)


async def _white_label_add_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    raw_args = " ".join(str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()).strip()
    if raw_args:
        await _perform_white_label_add_bot(update, context, raw_token=raw_args, lang=lang)
        return
    context.user_data["pending_wl_add_bot"] = {"expires_at": time.time() + 300}
    await target_message.reply_text("🤖 Send the connected bot token now.\nCancel: /cancel")


async def _white_label_set_cache_channel_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if len(args) >= 2:
        await _perform_white_label_set_cache_channel(update, context, identifier=args[0], channel_id_raw=args[1], lang=lang)
        return
    context.user_data["pending_wl_set_cache_channel"] = {
        "identifier": args[0] if args else "",
        "expires_at": time.time() + 300,
    }
    await target_message.reply_text("📦 Send: @botusername -1001234567890\nBoth the main bot and the connected bot must already be admins there.")


async def _white_label_test_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_test_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    try:
        decrypted_token = decrypt_bot_token(str(row.get("bot_token_encrypted") or ""), CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        verified = await _verify_white_label_bot_token(decrypted_token)
        await run_blocking(db_record_connected_bot_verification, str(row.get("id") or ""), last_error=None)
        await target_message.reply_text(
            "\n".join(
                [
                    "✅ Connected bot token is valid.",
                    f"Bot: @{str(verified.get('bot_username') or '').strip() or row.get('bot_username')}",
                    f"Telegram ID: {verified.get('bot_telegram_id')}",
                ]
            )
        )
    except Exception as exc:
        error_text = wl_redact_token_like_strings(str(exc))
        await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ERROR, last_error=error_text)
        await target_message.reply_text(f"⚠️ Token verification failed.\n{error_text}")


async def _white_label_activate_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_activate_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    if not int(row.get("cache_channel_id") or 0):
        await target_message.reply_text("⚠️ Set the cache channel first with /wl_set_cache_channel.")
        return
    try:
        decrypted_token = decrypt_bot_token(str(row.get("bot_token_encrypted") or ""), CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        await _verify_white_label_bot_token(decrypted_token)
        updated = await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ACTIVE, clear_error=True)
        await run_blocking(db_record_connected_bot_verification, str(row.get("id") or ""), last_error=None)
        runtime_result = await wl_start_runtime(context.application.bot_data, dict(updated or row))
        if not runtime_result.get("ok"):
            runtime_line = f"Runtime: start failed ({runtime_result.get('error') or 'unknown error'})"
        else:
            runtime_line = "Runtime: already running" if runtime_result.get("already_running") else f"Runtime: started, PID {runtime_result.get('pid') or '-'}"
        await target_message.reply_text(
            "\n".join(
                [
                    f"✅ Connected bot activated: {format_connected_bot_reference(updated or row)}",
                    runtime_line,
                    f"Stop later: /wl_stop_bot {format_connected_bot_reference(updated or row)}",
                ]
            )
        )
    except Exception as exc:
        error_text = wl_redact_token_like_strings(str(exc))
        await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ERROR, last_error=error_text)
        await target_message.reply_text(f"⚠️ Could not activate the connected bot.\n{error_text}")


async def _white_label_suspend_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_suspend_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    updated = await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_SUSPENDED, clear_error=False)
    runtime_result = await wl_stop_runtime(context.application.bot_data, str(row.get("id") or ""))
    runtime_line = "Runtime: already stopped" if runtime_result.get("already_stopped") else f"Runtime: {runtime_result.get('state') or 'STOPPED'}"
    await target_message.reply_text(
        "\n".join(
            [
                f"⏸️ Connected bot suspended: {format_connected_bot_reference(updated or row)}",
                runtime_line,
            ]
        )
    )


async def _white_label_delete_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_delete_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    await wl_stop_runtime(context.application.bot_data, str(row.get("id") or ""))
    deleted = await run_blocking(db_delete_connected_bot, str(row.get("id") or ""))
    if int(deleted or 0) <= 0:
        await target_message.reply_text("⚠️ Connected bot could not be deleted.")
        return
    await target_message.reply_text(f"🗑️ Connected bot deleted: {format_connected_bot_reference(row)}")


async def _white_label_start_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_start_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    if not int(row.get("cache_channel_id") or 0):
        await target_message.reply_text("⚠️ Set the cache channel first with /wl_set_cache_channel.")
        return
    try:
        if str(row.get("status") or "").upper() != WL_STATUS_ACTIVE:
            row = await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_ACTIVE, clear_error=True) or row
        result = await wl_start_runtime(context.application.bot_data, dict(row))
        if not result.get("ok"):
            await target_message.reply_text(f"⚠️ Connected bot runtime did not start.\n{result.get('error') or 'unknown error'}")
            return
        if result.get("already_running"):
            await target_message.reply_text(
                "\n".join(
                    [
                        f"✅ Connected bot runtime is already running: {format_connected_bot_reference(row)}",
                        f"PID: {result.get('pid') or '-'}",
                    ]
                )
            )
            return
        await target_message.reply_text(
            "\n".join(
                [
                    f"✅ Connected bot runtime started: {format_connected_bot_reference(row)}",
                    f"PID: {result.get('pid') or '-'}",
                    f"Stop: /wl_stop_bot {format_connected_bot_reference(row)}",
                ]
            )
        )
    except Exception as exc:
        error_text = wl_redact_token_like_strings(str(exc))
        await target_message.reply_text(f"⚠️ Could not start connected bot runtime.\n{error_text}")


async def _white_label_stop_bot_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_stop_bot @botusername")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    await run_blocking(db_update_connected_bot_status, str(row.get("id") or ""), WL_STATUS_SUSPENDED, clear_error=False)
    result = await wl_stop_runtime(context.application.bot_data, str(row.get("id") or ""))
    if result.get("already_stopped"):
        await target_message.reply_text(f"ℹ️ Connected bot runtime was already stopped: {format_connected_bot_reference(row)}")
        return
    await target_message.reply_text(
        "\n".join(
            [
                f"🛑 Connected bot runtime stopped: {format_connected_bot_reference(row)}",
                f"Result: {result.get('state') or 'STOPPED'}",
                f"PID: {result.get('pid') or '-'}",
            ]
        )
    )


async def _white_label_runtime_status_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if args:
        row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
        if not row:
            await target_message.reply_text("⚠️ Connected bot not found.")
            return
        status = await wl_get_runtime_status(context.application.bot_data, str(row.get("id") or ""))
        await target_message.reply_text(wl_format_runtime_status(row, status))
        return
    rows = await run_blocking(db_list_connected_bots)
    if not rows:
        await target_message.reply_text("No connected bots yet.")
        return
    blocks = ["🤖 Connected bot runtimes"]
    for row in rows:
        status = await wl_get_runtime_status(context.application.bot_data, str(row.get("id") or ""))
        blocks.append(wl_format_runtime_status(row, status))
    await target_message.reply_text("\n\n".join(blocks))


async def _white_label_test_cache_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message or not update.effective_user:
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await target_message.reply_text(feature_error)
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await target_message.reply_text("⚠️ Usage: /wl_test_cache @botusername [book_id]")
        return
    row = await run_blocking(db_get_connected_bot_by_identifier, normalize_connected_bot_identifier(args[0]))
    if not row:
        await target_message.reply_text("⚠️ Connected bot not found.")
        return
    if not int(row.get("cache_channel_id") or 0):
        await target_message.reply_text("⚠️ Cache channel is not configured for this connected bot.")
        return
    if len(args) > 1:
        book = await run_blocking(db_get_book_by_id, str(args[1] or "").strip())
    else:
        book = await _pick_white_label_test_book()
    if not book:
        await target_message.reply_text("⚠️ No accessible PDF test book was found in the central catalog.")
        return
    seed_token = uuid.uuid4().hex
    seed_job = await run_blocking(
        db_create_connected_bot_cache_seed_job,
        connected_bot_id=str(row.get("id") or ""),
        book_id=str(book.get("id") or ""),
        requesting_chat_id=int(target_message.chat_id),
        requesting_user_id=int(update.effective_user.id),
        requesting_message_id=int(target_message.message_id),
        cache_channel_id=int(row.get("cache_channel_id") or 0),
        seed_token=seed_token,
    )
    result = await seed_connected_bot_cache(row, dict(book), seed_job, main_bot=context.bot)
    if not result.get("ok"):
        await target_message.reply_text(f"⚠️ Cache seed send failed.\n{result.get('error') or 'unknown error'}")
        return
    deadline = time.monotonic() + max(5, int(WHITE_LABEL_CACHE_WAIT_SECONDS))
    while time.monotonic() < deadline:
        cache_row = await run_blocking(
            db_get_connected_bot_file_cache,
            str(row.get("id") or ""),
            str(book.get("id") or ""),
            only_valid=True,
        )
        if cache_row and str(cache_row.get("telegram_file_id") or "").strip():
            await target_message.reply_text(
                "\n".join(
                    [
                        "✅ Cache test succeeded.",
                        f"Bot: {format_connected_bot_reference(row)}",
                        f"Book: {get_display_name(book)}",
                        f"Cache message: {cache_row.get('cache_message_id') or '-'}",
                    ]
                )
            )
            return
        await asyncio.sleep(1.0)
    await target_message.reply_text(
        "⚠️ Cache seed was sent, but the connected bot did not confirm the cached file within the wait window.\n"
        "Make sure the connected bot runtime is running with /wl_start_bot and both bots are admins in the cache channel."
    )


async def wl_add_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_add_bot_impl)


async def wl_set_cache_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_set_cache_channel_impl)


async def wl_list_bots_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_list_bots_impl)


async def wl_suspend_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_suspend_bot_impl)


async def wl_activate_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_activate_bot_impl)


async def wl_start_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_start_bot_impl)


async def wl_stop_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_stop_bot_impl)


async def wl_runtime_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_runtime_status_impl)


async def wl_delete_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_delete_bot_impl)


async def wl_test_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_test_bot_impl)


async def wl_test_cache_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _white_label_test_cache_impl)


async def _botconnectreq_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = ensure_user_language(update, context)
    feature_error = _white_label_feature_error()
    if feature_error:
        await safe_reply(update, feature_error)
        return
    await _show_wl_requests_page(update, context, lang, 0)


async def _connected_bots_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = ensure_user_language(update, context)
    feature_error = _white_label_feature_error()
    if feature_error:
        await safe_reply(update, feature_error)
        return
    await _show_wl_connected_bots_page(update, context, lang, 0)


async def botconnectreq_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _botconnectreq_impl)


async def connected_bots_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await owner_only_command(update, context, _connected_bots_impl)


async def handle_white_label_public_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    data = str(query.data or "")
    await query.answer()
    if data == "wlreq:sendtoken":
        context.user_data["pending_wl_connect_request"] = {"expires_at": time.time() + _WL_PUBLIC_TOKEN_TTL_SECONDS}
        await query.message.reply_text(_wl_text(lang, "wlreq_send_token_prompt", "🔐 Send the API token from BotFather.\nCancel: /cancel")) if query.message else None
        return
    if data == "wlreq:cancel":
        context.user_data.pop("pending_wl_connect_request", None)
        if query.message:
            await query.message.edit_text(_wl_text(lang, "wlreq_cancelled", "❌ Bot connection request cancelled."))


async def handle_white_label_owner_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await query.answer(MESSAGES[lang].get("owner_only", "Owner only."), show_alert=True)
        return
    await query.answer()
    await _show_white_label_owner_menu(update, context, lang)


async def handle_white_label_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await query.answer(MESSAGES[lang].get("owner_only", "Owner only."), show_alert=True)
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await query.answer(feature_error, show_alert=True)
        return
    data = str(query.data or "")
    await query.answer()
    if data.startswith("wlreqpage:"):
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 0
        await _show_wl_requests_page(update, context, lang, page)
        return
    if data.startswith("wlreqview:"):
        await _show_wl_request_detail(update, context, lang, data.split(":", 1)[1])
        return
    if data.startswith("wlreqaccept:"):
        request_id = data.split(":", 1)[1].strip()
        row = await run_blocking(db_get_connected_bot_request_by_id, request_id)
        if not row:
            await _reply_or_edit_wl(update, "⚠️ Request not found.", parse_mode=None)
            return
        context.user_data["pending_wl_accept_cache_channel"] = {
            "request_id": request_id,
            "expires_at": time.time() + _WL_OWNER_FLOW_TTL_SECONDS,
        }
        if query.message:
            await query.message.reply_text(_wl_text(lang, "wlreq_accept_cache_prompt", "Send the cache channel ID."))
        return
    if data.startswith("wlreqreject:"):
        request_id = data.split(":", 1)[1].strip()
        row = await run_blocking(db_get_connected_bot_request_by_id, request_id)
        if not row:
            await _reply_or_edit_wl(update, "⚠️ Request not found.", parse_mode=None)
            return
        context.user_data["pending_wl_reject_request"] = {
            "request_id": request_id,
            "expires_at": time.time() + _WL_OWNER_FLOW_TTL_SECONDS,
        }
        if query.message:
            await query.message.reply_text(_wl_text(lang, "wlreq_reject_reason_prompt", "Send the rejection reason or /skip."))


async def _run_wl_cache_test_for_row(update: Update, context: ContextTypes.DEFAULT_TYPE, row: dict, lang: str) -> None:
    query = update.callback_query
    if not int(row.get("cache_channel_id") or 0):
        await _reply_or_edit_wl(update, "⚠️ Cache channel is not configured.", parse_mode=None)
        return
    book = await _pick_white_label_test_book()
    if not book:
        await _reply_or_edit_wl(update, "⚠️ No accessible PDF test book was found.", parse_mode=None)
        return
    seed_token = uuid.uuid4().hex
    seed_job = await run_blocking(
        db_create_connected_bot_cache_seed_job,
        connected_bot_id=str(row.get("id") or ""),
        book_id=str(book.get("id") or ""),
        requesting_chat_id=int(query.message.chat_id if query and query.message else OWNER_ID),
        requesting_user_id=int(update.effective_user.id if update.effective_user else OWNER_ID),
        requesting_message_id=int(query.message.message_id) if query and query.message else None,
        cache_channel_id=int(row.get("cache_channel_id") or 0),
        seed_token=seed_token,
    )
    result = await seed_connected_bot_cache(row, dict(book), seed_job, main_bot=context.bot)
    if not result.get("ok"):
        await _reply_or_edit_wl(update, f"⚠️ Cache seed send failed.\n{result.get('error') or 'unknown error'}", parse_mode=None)
        return
    deadline = time.monotonic() + max(5, int(WHITE_LABEL_CACHE_WAIT_SECONDS))
    while time.monotonic() < deadline:
        cache_row = await run_blocking(
            db_get_connected_bot_file_cache,
            str(row.get("id") or ""),
            str(book.get("id") or ""),
            only_valid=True,
        )
        if cache_row and str(cache_row.get("telegram_file_id") or "").strip():
            await _reply_or_edit_wl(
                update,
                "\n".join(
                    [
                        "✅ Cache test succeeded.",
                        f"Bot: {format_connected_bot_reference(row)}",
                        f"Book: {_wl_escape(get_display_name(book))}",
                        f"Cache message: {cache_row.get('cache_message_id') or '-'}",
                    ]
                ),
            )
            return
        await asyncio.sleep(1.0)
    await _reply_or_edit_wl(
        update,
        "⚠️ Cache seed was sent, but the connected bot did not confirm the cached file within the wait window.\n"
        "Make sure the connected bot runtime is running and both bots are admins in the cache channel.",
        parse_mode=None,
    )


async def handle_white_label_connected_bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if not update.effective_user or not _is_owner_user(update.effective_user.id):
        await query.answer(MESSAGES[lang].get("owner_only", "Owner only."), show_alert=True)
        return
    feature_error = _white_label_feature_error()
    if feature_error:
        await query.answer(feature_error, show_alert=True)
        return
    data = str(query.data or "")
    await query.answer()
    if data.startswith("wlbotpage:"):
        try:
            page = int(data.split(":", 1)[1])
        except Exception:
            page = 0
        await _show_wl_connected_bots_page(update, context, lang, page)
        return
    if data.startswith("wlbotview:"):
        await _show_wl_connected_bot_detail(update, context, lang, data.split(":", 1)[1])
        return
    action, _, connected_bot_id = data.partition(":")
    row = await run_blocking(db_get_connected_bot_by_id, connected_bot_id)
    if not row:
        await _reply_or_edit_wl(update, "⚠️ Connected bot not found.", parse_mode=None)
        return
    if action == "wlbotstart" or action == "wlbotresume":
        result = await _start_wl_connected_bot_from_row(context, dict(row))
        if not result.get("ok"):
            await _reply_or_edit_wl(update, f"⚠️ Runtime did not start.\n{wl_redact_token_like_strings(str(result.get('error') or 'unknown error'))}", parse_mode=None)
            return
        await _show_wl_connected_bot_detail(update, context, lang, connected_bot_id)
        return
    if action == "wlbotstop" or action == "wlbotsuspend":
        await run_blocking(db_update_connected_bot_status, connected_bot_id, WL_STATUS_SUSPENDED, clear_error=False)
        await wl_stop_runtime(context.application.bot_data, connected_bot_id)
        await _show_wl_connected_bot_detail(update, context, lang, connected_bot_id)
        return
    if action == "wlbotrestart":
        await wl_stop_runtime(context.application.bot_data, connected_bot_id)
        result = await _start_wl_connected_bot_from_row(context, dict(row))
        if not result.get("ok"):
            await _reply_or_edit_wl(update, f"⚠️ Runtime did not restart.\n{wl_redact_token_like_strings(str(result.get('error') or 'unknown error'))}", parse_mode=None)
            return
        await _show_wl_connected_bot_detail(update, context, lang, connected_bot_id)
        return
    if action == "wlbottest":
        await _run_wl_cache_test_for_row(update, context, dict(row), lang)
        return
    if action == "wlbotdelete":
        await _reply_or_edit_wl(
            update,
            f"🗑 Delete connected bot {_wl_escape(_wl_bot_reference(row))}?\nCentral books will not be deleted.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Confirm delete", callback_data=f"wlbotdeleteconfirm:{connected_bot_id}")],
                    [InlineKeyboardButton(_wl_text(lang, "wlreq_back_button", "⬅️ Back"), callback_data=f"wlbotview:{connected_bot_id}")],
                ]
            ),
        )
        return
    if action == "wlbotdeleteconfirm":
        await wl_stop_runtime(context.application.bot_data, connected_bot_id)
        await run_blocking(db_delete_connected_bot, connected_bot_id)
        await _show_wl_connected_bots_page(update, context, lang, 0)


def get_public_commands(lang: str = "en"):
    return _command_sync.get_public_commands(lang)


def get_public_commands_for_menu(lang: str = "en", user_id: int | None = None):
    del user_id
    return _command_sync.get_public_commands_for_menu(lang)


def get_group_commands(lang: str = "en"):
    return _command_sync.get_group_commands(lang)


def get_admin_commands(lang: str = "en", user_id: int | None = None):
    if user_id is not None and not _is_owner_user(user_id):
        return _command_sync.get_public_commands_for_menu(lang)
    return _command_sync.get_admin_commands(lang)


def _build_help_text(lang: str, user_id: int | None = None) -> str:
    return _menu_ui_build_help_text(lang, MESSAGES, _is_admin_user, user_id=user_id)


async def set_bot_commands(application):
    await _command_sync.set_bot_commands(
        application,
        owner_id=OWNER_ID,
        logger=logger,
    )


async def _sync_user_commands_if_needed(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int | None,
    lang: str,
    *,
    force: bool = False,
):
    await _command_sync.sync_user_commands_if_needed(
        context,
        user_id=user_id,
        lang=lang,
        owner_id=OWNER_ID,
        logger=logger,
        force=force,
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

    async def _bg_backfill_awards():
        try:
            await run_blocking_db_retry(
                db_backfill_user_awards_if_empty,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.error(f"Failed to backfill user awards: {e}")

    async def _bg_sync_unindexed_books():
        try:
            if not ENABLE_ELASTICSEARCH:
                return
            meta = await run_blocking(
                db_create_background_job,
                "SEARCH_REINDEX_ALL",
                int(OWNER_ID or 1),
                {"lang": "uz", "source": "startup"},
                priority=80,
                max_attempts=2,
                idempotency_key="startup-search-reindex-all",
                ignore_limits=True,
            )
            if meta and meta.get("ok"):
                logger.info("Queued SEARCH_REINDEX_ALL startup job: %s", meta.get("job_id"))
        except Exception as e:
            logger.error(f"Failed to sync unindexed books to Elasticsearch: {e}", exc_info=True)

    async def _bg_ensure_upload_local_backup_worker(context):
        try:
            starter = getattr(_upload_flow, "start_upload_local_backup_worker", None)
            if callable(starter):
                starter(application)
        except Exception as e:
            logger.error(f"Failed to ensure local backup worker: {e}")

    async def _bg_ensure_audiobook_local_backup_worker(context):
        try:
            starter = getattr(_search_flow, "start_audiobook_local_backup_worker", None)
            if callable(starter):
                starter(application)
        except Exception as e:
            logger.error(f"Failed to ensure audiobook local backup worker: {e}")

    async def _bg_ensure_background_job_workers(context):
        try:
            start_background_job_workers(application)
        except Exception as e:
            logger.error(f"Failed to ensure background job workers: {e}")

    async def _bg_recover_stale_background_jobs(context):
        try:
            recovered = await run_blocking(db_recover_stale_background_jobs, JOB_LOCK_TIMEOUT_MINUTES)
            if recovered and (recovered.get("recovered") or recovered.get("failed")):
                logger.info("Recovered stale background jobs: %s", recovered)
        except Exception as e:
            logger.error("Failed to recover stale background jobs: %s", e, exc_info=True)

    async def _bg_cleanup_job_dirs(context):
        try:
            cleaned = await asyncio.to_thread(_prune_job_dirs, TEMP_JOB_TTL_HOURS, FAILED_JOB_TTL_HOURS)
            if cleaned and (cleaned.get("deleted") or cleaned.get("failed_deleted")):
                logger.info("Cleaned job temp dirs: %s", cleaned)
        except Exception as e:
            logger.error("Failed to clean job temp dirs: %s", e, exc_info=True)

    _spawn_managed_background_task(application, _bg_set_commands())
    try:
        application.job_queue.run_repeating(prune_blocked_users, interval=3 * 60 * 60, first=60)
        logger.debug("Scheduled prune_blocked_users every 3 hours.")
    except Exception as e:
        logger.error(f"Failed to schedule prune_blocked_users: {e}")
    _spawn_managed_background_task(application, _bg_backfill_awards())
    _spawn_managed_background_task(application, _bg_sync_unindexed_books())
    try:
        application.job_queue.run_once(_bg_ensure_upload_local_backup_worker, when=1)
        application.job_queue.run_repeating(_bg_ensure_upload_local_backup_worker, interval=60, first=60)
    except Exception as e:
        logger.error(f"Failed to start local backup worker: {e}")
    try:
        application.job_queue.run_once(_bg_ensure_audiobook_local_backup_worker, when=1)
        application.job_queue.run_repeating(_bg_ensure_audiobook_local_backup_worker, interval=60, first=60)
    except Exception as e:
        logger.error(f"Failed to start audiobook local backup worker: {e}")
    try:
        application.job_queue.run_once(_bg_ensure_background_job_workers, when=1)
        application.job_queue.run_repeating(_bg_ensure_background_job_workers, interval=60, first=60)
    except Exception as e:
        logger.error(f"Failed to start background job workers: {e}")
    try:
        application.job_queue.run_once(_bg_recover_stale_background_jobs, when=3)
        application.job_queue.run_repeating(_bg_recover_stale_background_jobs, interval=10 * 60, first=10 * 60)
    except Exception as e:
        logger.error("Failed to schedule stale background job recovery: %s", e)
    try:
        application.job_queue.run_once(_bg_cleanup_job_dirs, when=60)
        application.job_queue.run_repeating(_bg_cleanup_job_dirs, interval=60 * 60, first=60 * 60)
    except Exception as e:
        logger.error("Failed to schedule job temp cleanup: %s", e)


def get_es():
    global _ES_CLIENT
    if _ES_CLIENT is not None:
        return _ES_CLIENT
    if not ENABLE_ELASTICSEARCH:
        logger.info("Elasticsearch feature flag disabled; DB fallback search only.")
        return None
    if not ES_URL:
        logger.debug("ES_URL not set; Elasticsearch disabled.")
        return None
    kwargs = {"request_timeout": ES_TIMEOUT_SECONDS}
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
        if not ENABLE_ELASTICSEARCH:
            return False
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
        logger.error("Failed to index in ES for book_id=%s: %s", book_id, e)
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
        logger.warning("ES search failed for query=%r: %s", query, e)
        return []


def _inline_search_books(query: str, size: int = 10) -> list[dict]:
    query = str(query or "").strip()
    if not query:
        return []

    results: list[dict] = []
    seen_ids: set[str] = set()

    for source, _score, book_id in search_es(query, size=size):
        bid = str(book_id or source.get("id") or "").strip()
        if not bid or bid in seen_ids:
            continue
        book = {}
        try:
            book = dict(db_get_book_by_id(bid) or {})
        except Exception as e:
            logger.debug("Inline DB hydration failed for book %s: %s", bid, e, exc_info=True)
        if not book:
            book = dict(source or {})
        book["id"] = bid
        results.append(book)
        seen_ids.add(bid)
        if len(results) >= size:
            return results

    try:
        query_norm = normalize(query).lower()
        query_tokens = [token for token in query_norm.split() if token]
        if not query_tokens:
            return results

        for book in list(db_list_books() or []):
            bid = str(book.get("id") or "").strip()
            if not bid or bid in seen_ids:
                continue
            haystacks = [
                normalize(str(book.get("book_name") or "")).lower(),
                normalize(str(book.get("display_name") or "")).lower(),
                normalize(str(book.get("path") or "")).lower(),
            ]
            if any(query_norm in text for text in haystacks if text):
                results.append(book)
                seen_ids.add(bid)
            elif all(any(token in text for text in haystacks if text) for token in query_tokens):
                results.append(book)
                seen_ids.add(bid)
            if len(results) >= size:
                break
    except Exception as e:
        logger.debug("Inline DB fallback search failed for '%s': %s", query, e, exc_info=True)

    return results


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

def _normalize_uzbek_apostrophes(text: str) -> str:
    if not text:
        return ""
    s = str(text)
    # Keep the Uzbek apostrophe as a real character and repair common
    # "o zbek" / "g ulom" spacing mistakes.
    s = s.replace("'", "ʻ").replace("’", "ʻ").replace("ʼ", "ʻ")
    s = re.sub(r"\b([og])ʻ\s+([^\W\d_])", r"\1ʻ\2", s, flags=re.UNICODE)
    s = re.sub(r"\b([og])\s+([^\W\d_])", r"\1ʻ\2", s, flags=re.UNICODE)
    return s


def normalize(text: str) -> str:
    text = text.lower()
    # Preserve Uzbek apostrophes and standardize common variants to "ʻ".
    text = _normalize_uzbek_apostrophes(text)
    text = re.sub(r'@[\w]+', ' ', text)                 # remove @usernames
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)  # remove links
    text = text.replace("_", " ")
    text = re.sub(r"[^\w\sʻ]+", " ", text, flags=re.UNICODE)  # remove punctuation/symbols but keep Uzbek apostrophe
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


def clean_query(text: str) -> str:
    return normalize(text)


async def apply_book_rename(
    bot,
    book_id: str,
    new_title: str,
    actor_user_id: int | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "ok": False,
        "changed": False,
        "retryable": False,
        "book_id": str(book_id or "").strip(),
        "book_name": None,
        "display_name": None,
        "path": None,
        "file_id": None,
        "file_unique_id": None,
        "error": None,
    }

    try:
        if actor_user_id is not None:
            rename_allowed_fn = globals().get("can_rename_books")
            if not callable(rename_allowed_fn) or not rename_allowed_fn(actor_user_id):
                result["error"] = "not authorized"
                return result

        bid = str(book_id or "").strip()
        if not bid:
            result["error"] = "missing book_id"
            return result

        book = await run_blocking(db_get_book_by_id, bid)
        if not book:
            result["error"] = "book not found"
            return result

        requested_title = _normalize_title_apostrophes(new_title)
        if not requested_title:
            result["error"] = "empty book name"
            result["retryable"] = True
            return result
        display_title = requested_title

        current_display = str(get_display_name(book) or "").strip()
        if display_title == current_display:
            result.update(
                {
                    "ok": True,
                    "changed": False,
                    "book_name": display_title,
                    "display_name": display_title,
                    "path": str(book.get("path") or "").strip() or None,
                    "file_id": str(book.get("file_id") or "").strip() or None,
                    "file_unique_id": str(book.get("file_unique_id") or "").strip() or None,
                }
            )
            return result

        local_path_str = str(book.get("path") or "").strip()
        if not local_path_str:
            result["error"] = "local copy not ready"
            result["retryable"] = True
            return result

        old_path = Path(local_path_str)
        if not old_path.exists():
            result["error"] = "local copy missing"
            result["retryable"] = True
            return result

        file_id = str(book.get("file_id") or "").strip()
        if not file_id:
            result["error"] = "missing file_id"
            result["retryable"] = True
            return result

        updated_book = dict(book)
        updated_book["book_name"] = display_title
        updated_book["display_name"] = display_title

        # Keep the local file path unchanged so owner-edited names stay exact in DB/UI
        # and are never transformed by filesystem-safe filename rules.
        target_path = old_path
        old_ext = old_path.suffix or ".pdf"
        telegram_file_name = f"{display_title}{old_ext}"

        refresh_info = await _upload_flow._refresh_uploaded_book_file_id(
            bot,
            bid,
            str(target_path),
            telegram_filename=telegram_file_name,
            use_thumbnail=True,
        )
        if not refresh_info.get("file_id"):
            result["error"] = f"file_id refresh failed: {refresh_info.get('error') or 'unknown error'}"
            result["retryable"] = True
            return result

        new_file_id = str(refresh_info.get("file_id") or "").strip()
        new_file_unique_id = str(refresh_info.get("file_unique_id") or "").strip() or None

        update_rowcount = await run_blocking(
            db_update_book_rename_meta,
            bid,
            display_title,
            display_title,
            str(target_path),
            new_file_id,
            new_file_unique_id,
            True,
        )
        if not update_rowcount:
            result["error"] = "database update failed"
            result["retryable"] = True
            logger.error("db_update_book_rename_meta returned rowcount=%s for book_id=%s", update_rowcount, bid)
            return result

        updated_book["path"] = str(target_path)
        updated_book["file_id"] = new_file_id
        if new_file_unique_id:
            updated_book["file_unique_id"] = new_file_unique_id

        if es_available():
            try:
                await run_blocking(
                    index_book,
                    display_title,
                    new_file_id,
                    str(target_path),
                    bid,
                    display_title,
                    new_file_unique_id,
                )
            except Exception as e:
                logger.warning("Failed to reindex renamed book %s: %s", bid, e, exc_info=True)

        try:
            search_cache_ns = str(globals().get("SEARCH_CACHE_NS", os.getenv("SEARCH_CACHE_NS", "v1")) or "v1")
            clear_pattern = globals().get("cache_clear_pattern")
            delete_cache_key = globals().get("cache_delete")
            if callable(clear_pattern):
                clear_pattern(f"search:books:entries:{search_cache_ns}:*")
                clear_pattern("top_results:*")
            if callable(delete_cache_key):
                delete_cache_key("top:books:entries")
        except Exception as e:
            logger.debug("Failed to clear discovery caches after rename for %s: %s", bid, e)

        result.update(
            {
                "ok": True,
                "changed": True,
                "book_name": display_title,
                "display_name": display_title,
                "path": str(target_path),
                "file_id": new_file_id,
                "file_unique_id": new_file_unique_id,
            }
        )
        return result
    except Exception as e:
        result["error"] = str(e)
        result["retryable"] = True
        logger.error("apply_book_rename failed for %s: %s", book_id, e, exc_info=True)
        return result


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

            # Normalize only for Elasticsearch search; keep DB-stored names exact.
            clean_name = normalize(raw_name)
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
                audio_allowed=bool(u.get("audio_allowed", False)),
                rename_allowed=bool(u.get("rename_allowed", False)),
                language_selected=u.get("language_selected"),
                group_language=u.get("group_language"),
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


def _is_group_chat(chat) -> bool:
    return str(getattr(chat, "type", "") or "").lower() in {"group", "supergroup"}


def _has_private_bot_start(user: dict | None) -> bool:
    if not isinstance(user, dict):
        return False
    return bool(user.get("language_selected")) and bool(user.get("language"))


async def _build_private_start_url(context: ContextTypes.DEFAULT_TYPE, payload: str = "group_start") -> str | None:
    username = getattr(context.bot, "username", None)
    if not username:
        try:
            me = await context.bot.get_me()
            username = getattr(me, "username", None)
        except Exception:
            username = None
    if not username:
        return None
    return f"https://t.me/{username}?start={payload}"


async def _reply_group_private_start_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
    user_id: int | None = None,
) -> bool:
    if not update.message:
        return False
    start_token = uuid.uuid4().hex
    start_url = await _build_private_start_url(context, payload=f"gstart_{int(user_id or 0)}_{start_token}")
    text = MESSAGES.get(lang, MESSAGES["en"]).get(
        "group_private_start_required",
        MESSAGES["en"]["group_private_start_required"],
    )
    reply_markup = None
    if start_url:
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(MESSAGES.get(lang, MESSAGES["en"]).get("group_private_start_button", "🚀 Start Bot"), url=start_url)]]
        )
    sent = await _send_with_retry(lambda: update.message.reply_text(text, reply_markup=reply_markup))
    if sent and user_id and start_url:
        try:
            await run_blocking_db_retry(
                db_upsert_group_private_start_prompt,
                start_token,
                int(user_id),
                int(getattr(sent.chat, "id", update.effective_chat.id)),
                int(getattr(sent, "message_id", 0)),
                lang,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.error(
                "Failed to persist group private-start prompt for user %s in chat %s: %s",
                user_id,
                getattr(sent.chat, "id", None),
                e,
                exc_info=True,
            )
    return bool(sent)


async def _reply_group_language_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message:
        return False
    text = MESSAGES.get(lang, MESSAGES["en"]).get("group_choose_language", MESSAGES["en"]["choose_language"])
    await _send_with_retry(lambda: update.message.reply_text(text, reply_markup=get_language_keyboard()))
    return True


async def _send_group_search_ready_message(target_message, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not target_message:
        return None
    text = MESSAGES.get(lang, MESSAGES["en"]).get("group_search_ready", MESSAGES["en"]["group_search_ready"])
    return await _send_with_retry(
        lambda: target_message.reply_text(
            text,
            reply_markup=ReplyKeyboardRemove(),
        )
    )


async def _resolve_group_private_start_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    actor_user_id: int,
    payload_user_id: int | None,
    prompt_token: str | None,
    lang: str,
) -> bool:
    if not actor_user_id:
        return False
    prompt = None
    try:
        if prompt_token:
            prompt = await run_blocking_db_retry(
                db_get_group_private_start_prompt_by_token,
                prompt_token,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        if not prompt:
            prompt = await run_blocking_db_retry(
                db_get_latest_pending_group_private_start_prompt,
                actor_user_id,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        if not prompt:
            return False
        prompt_user_id = int(prompt.get("user_id") or 0)
        if prompt_user_id and prompt_user_id != int(actor_user_id):
            return False
        if payload_user_id and prompt_user_id and payload_user_id != prompt_user_id:
            return False
        chat_id = int(prompt.get("chat_id") or 0)
        message_id = int(prompt.get("message_id") or 0)
        if not chat_id or not message_id:
            return False
        prompt_lang = str(prompt.get("prompt_lang") or lang or "en").strip() or "en"
        text = MESSAGES.get(prompt_lang, MESSAGES["en"]).get(
            "group_private_start_ready",
            "✅ Great! You have started the bot.\n🌐 Please choose your language below:",
        )
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=get_language_keyboard(),
            )
        except Exception as e:
            logger.warning(
                "Failed to edit private-start prompt for user %s in chat %s message %s: %s",
                actor_user_id,
                chat_id,
                message_id,
                e,
                exc_info=True,
            )
            return False
        try:
            await run_blocking_db_retry(
                db_set_group_private_start_prompt_status,
                str(prompt.get("token") or prompt_token or ""),
                "resolved",
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.warning(
                "Failed to mark private-start prompt resolved for user %s: %s",
                actor_user_id,
                e,
                exc_info=True,
            )
        return True
    except Exception as e:
        logger.error(
            "Failed to resolve group private-start prompt for user %s: %s",
            actor_user_id,
            e,
            exc_info=True,
        )
        return False


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
            rename_allowed=False,
            joined_date=datetime.now().date(),
            left_date=None,
            language=lang,
            language_selected=True,
        )
    else:
        update_user_language(user_id, lang)


def set_user_group_language(user_id: int, lang: str):
    user = get_user(user_id)
    if not user:
        upsert_user(
            user_id=user_id,
            username=None,
            first_name=None,
            last_name=None,
            blocked=False,
            allowed=False,
            rename_allowed=False,
            joined_date=datetime.now().date(),
            left_date=None,
            language=None,
            language_selected=False,
            group_language=lang,
        )
    else:
        update_user_group_language(user_id, lang)


def _language_picker_prompt_lang(user: dict | None, tg_user) -> str:
    if isinstance(user, dict) and user.get("language") and bool(user.get("language_selected")) is True:
        return detect_language_code(str(user.get("language")))
    return detect_language_code(getattr(tg_user, "language_code", None))


def ensure_user_language(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    if not update.effective_user:
        return "en"
    user = get_user_record(update.effective_user.id)
    if _is_group_chat(getattr(update, "effective_chat", None)):
        lang = context.user_data.get("group_language")
        if lang:
            return lang
        if user and user.get("group_language"):
            context.user_data["group_language"] = user["group_language"]
            return str(user["group_language"])
        if user and user.get("language") and bool(user.get("language_selected")) is True:
            return str(user["language"])
        return detect_language_code(getattr(update.effective_user, "language_code", None))

    lang = context.user_data.get("language")
    if lang:
        return lang
    if user and user.get("language") and bool(user.get("language_selected")) is True:
        context.user_data["language"] = user["language"]
        return str(user["language"])
    # Do not auto-apply Telegram locale in private chat before explicit setup.
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
            rename_allowed=bool(existing.get("rename_allowed", False)),
            joined_date=existing.get("joined_date") or today,
            left_date=existing.get("left_date"),
            language=effective_lang,
            delete_allowed=bool(existing.get("delete_allowed", False)),
            stopped=bool(existing.get("stopped", False)),
            audio_allowed=bool(existing.get("audio_allowed", False)),
            language_selected=lang_selected,
            retries=DB_RETRY_ATTEMPTS,
            base_delay=DB_RETRY_BASE_DELAY_SEC,
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
    if _is_owner_user(user_id):
        return True
    user = get_user(user_id)
    return bool(user.get("allowed")) if user else False


def is_audio_allowed(user_id: int) -> bool:
    if _is_owner_user(user_id):
        return True
    try:
        return bool(db_is_user_audio_allowed(user_id))
    except Exception:
        user = get_user(user_id)
        return bool(user.get("audio_allowed")) if user else False


def can_rename_books(user_id: int) -> bool:
    if _is_owner_user(user_id):
        return True
    try:
        return bool(db_is_user_rename_allowed(user_id))
    except Exception:
        user = get_user(user_id)
        return bool(user.get("rename_allowed")) if user else False


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

        user_record = get_user_record(update.effective_user.id) or {}
        if _is_group_chat(update.effective_chat):
            prompt_lang = detect_language_code(
                user_record.get("group_language")
                or user_record.get("language")
                or getattr(update.effective_user, "language_code", None)
            )
            referrer_id = parse_referral_payload(context.args[0] if context.args else None)
            if not _has_private_bot_start(user_record):
                await _reply_group_private_start_prompt(update, context, prompt_lang, update.effective_user.id)
            elif user_record.get("group_language"):
                await _send_group_search_ready_message(update.message, context, str(user_record.get("group_language") or prompt_lang))
            else:
                await _reply_group_language_prompt(update, context, prompt_lang)
            _schedule_application_task(
                context.application,
                _post_start_background_sync(update, context, referrer_id),
            )
            return

        # Reset menu/search state until the user explicitly chooses a language.
        context.user_data.pop("main_menu_section", None)
        context.user_data["awaiting_book_search"] = False
        context.user_data.pop("search_mode", None)
        start_payload = context.args[0] if context.args else None
        referrer_id = parse_referral_payload(start_payload)
        group_prompt_user_id, group_prompt_token = parse_group_private_start_payload(start_payload)
        guest_handoff_token = parse_guest_private_handoff_payload(start_payload)
        prompt_lang = _language_picker_prompt_lang(user_record, update.effective_user)
        selected_lang = str(user_record.get("language") or "").strip()
        language_selected = bool(user_record.get("language_selected")) and bool(selected_lang)
        if guest_handoff_token:
            handled = await _handle_guest_private_start_payload(
                update,
                context,
                payload_token=guest_handoff_token,
                user_record=user_record,
            )
            if handled:
                _schedule_application_task(
                    context.application,
                    _post_start_background_sync(update, context, referrer_id),
                )
                return
        if language_selected:
            await _send_animated_start_greeting(update, context, detect_language_code(selected_lang))
        else:
            # First-time users see the language picker on /start.
            await safe_reply(
                update,
                MESSAGES[prompt_lang]["choose_language"],
                reply_markup=get_language_keyboard()
            )
        if start_payload and (str(start_payload).startswith("gstart_") or str(start_payload) == "group_start"):
            try:
                await _resolve_group_private_start_prompt(
                    context,
                    update.effective_user.id,
                    group_prompt_user_id,
                    group_prompt_token,
                    prompt_lang,
                )
            except Exception as e:
                logger.warning("Failed to reveal group private-start prompt for %s: %s", update.effective_user.id, e, exc_info=True)
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
    user_record = get_user_record(update.effective_user.id) or {}
    if _is_group_chat(update.effective_chat):
        prompt_lang = detect_language_code(
            user_record.get("group_language")
            or user_record.get("language")
            or getattr(update.effective_user, "language_code", None)
        )
        if not _has_private_bot_start(user_record):
            await _reply_group_private_start_prompt(update, context, prompt_lang, update.effective_user.id)
        else:
            await _reply_group_language_prompt(update, context, prompt_lang)
        return
    prompt_lang = _language_picker_prompt_lang(user_record, update.effective_user)
    # Keep menu hidden/reset until language is explicitly chosen.
    context.user_data.pop("main_menu_section", None)
    context.user_data["awaiting_book_search"] = False
    context.user_data.pop("search_mode", None)
    await update.message.reply_text(
        MESSAGES[prompt_lang]["choose_language"],
        reply_markup=get_language_keyboard()
    )


def _build_start_greeting_text(lang: str, tg_user) -> str:
    first_name = (getattr(tg_user, "first_name", None) or "").strip() or "Friend"
    template = MESSAGES.get(lang, MESSAGES["en"]).get("greeting") or MESSAGES["en"]["greeting"]
    try:
        return template.format(first_name=html.escape(first_name))
    except Exception:
        return template


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
        is_owner_user_fn=_is_owner_user,
        white_label_enabled=ENABLE_WHITE_LABEL,
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
    if section == "main":
        context.user_data["awaiting_book_search"] = True
        context.user_data["search_mode"] = "book"
    else:
        context.user_data["awaiting_book_search"] = False
        context.user_data.pop("search_mode", None)
    return True


async def _send_main_menu_to_chat_id(context: ContextTypes.DEFAULT_TYPE, chat_id: int, lang: str, section: str = "main", user_id: int | None = None):
    text = _menus_build_main_menu_chat_text(lang=lang, section=section, messages=MESSAGES)
    try:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=_main_menu_keyboard(lang, section, user_id or chat_id))
        context.user_data["main_menu_section"] = section
        if section == "main":
            context.user_data["awaiting_book_search"] = True
            context.user_data["search_mode"] = "book"
        else:
            context.user_data["awaiting_book_search"] = False
            context.user_data.pop("search_mode", None)
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

    # Cancel audiobook adding flow if active
    pending_abook = context.user_data.get("pending_abook")
    if pending_abook and (not user_id or True):  # Audiobook flow is user-specific
        context.user_data.pop("pending_abook", None)
        cancelled = True

    if context.user_data.get("awaiting_request"):
        context.user_data["awaiting_request"] = False
        context.user_data.pop("awaiting_request_until", None)
        cancelled = True
    if context.user_data.get("pending_book_reaction_edit"):
        context.user_data.pop("pending_book_reaction_edit", None)
        cancelled = True
    if context.user_data.get("pending_book_comment"):
        context.user_data.pop("pending_book_comment", None)
        cancelled = True
    if context.user_data.get("pending_comment_relay"):
        context.user_data.pop("pending_comment_relay", None)
        cancelled = True
    if context.user_data.get("pending_forbidden_books"):
        context.user_data.pop("pending_forbidden_books", None)
        cancelled = True
    if context.user_data.get("pending_seedbookstats"):
        context.user_data.pop("pending_seedbookstats", None)
        cancelled = True
    if context.user_data.get("pending_wl_add_bot"):
        context.user_data.pop("pending_wl_add_bot", None)
        cancelled = True
    if context.user_data.get("pending_wl_set_cache_channel"):
        context.user_data.pop("pending_wl_set_cache_channel", None)
        cancelled = True
    if context.user_data.get("pending_wl_connect_request"):
        context.user_data.pop("pending_wl_connect_request", None)
        cancelled = True
    if context.user_data.get("pending_wl_accept_cache_channel"):
        context.user_data.pop("pending_wl_accept_cache_channel", None)
        cancelled = True
    if context.user_data.get("pending_wl_reject_request"):
        context.user_data.pop("pending_wl_reject_request", None)
        cancelled = True
    if context.user_data.get("admin_menu_prompt"):
        context.user_data.pop("admin_menu_prompt", None)
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
        context.user_data["search_mode"] = "book"
        search_prompt_text = m.get("menu_search_prompt", "Send a book name to search.")
        await update.message.reply_text(
            search_prompt_text,
            reply_markup=_main_menu_keyboard(lang, "main", user_id),
            parse_mode="HTML",
        )
        context.user_data["main_menu_section"] = "main"
        return True
    if action == "request":
        context.user_data["awaiting_request"] = True
        context.user_data["awaiting_request_until"] = time.time() + 30
        context.user_data["main_menu_section"] = "main"
        await update.message.reply_text(
            m.get("request_prompt", "✍️ Send the title of the book you need."),
            reply_markup=_main_menu_keyboard(lang, "main", user_id),
        )
        return True
    if action == "connect_book_bot":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await _start_white_label_public_request_flow(update, context, lang)
        return True
    context.user_data["awaiting_book_search"] = False
    context.user_data.pop("search_mode", None)
    if action == "favorites":
        context.user_data["main_menu_section"] = "other" if current_section == "other" else "main"
        context.user_data["_skip_spam_check_once"] = True
        await favorites_command(update, context)
        return True
    if action == "other":
        context.user_data["main_menu_section"] = "main"
        await _send_main_menu(update, context, lang, "main")
        return True
    if action == "back":
        prev = str(context.user_data.get("main_menu_section") or "main")
        if prev in {"admin_maintenance", "admin_duplicates", "admin_tasks"}:
            await _send_main_menu(update, context, lang, "admin")
        elif prev == "admin":
            await _send_main_menu(update, context, lang, "main")
        else:
            await _send_main_menu(update, context, lang, "main")
        return True
    if action == "myprofile":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await myprofile_command(update, context)
        return True
    if action == "top_books":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await top_command(update, context)
        return True
    if action == "top_users":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await top_users_command(update, context)
        return True
    if action == "help":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await help_command(update, context)
        return True
    if action == "upload":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await upload_command(update, context)
        return True
    if action == "contact_admin":
        context.user_data["main_menu_section"] = "main"
        context.user_data["_skip_spam_check_once"] = True
        await contact_admin_command(update, context)
        return True
    if action == "admin_white_label":
        if not user_id or not _is_owner_user(user_id):
            await update.message.reply_text(m.get("owner_only", m.get("admin_only", "Owner only.")))
            return True
        context.user_data["main_menu_section"] = "admin"
        context.user_data["_skip_spam_check_once"] = True
        await _show_white_label_owner_menu(update, context, lang)
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
        guest_audit_command_fn=guest_audit_command,
        inline_audit_command_fn=inline_audit_command,
        prune_command_fn=prune_command,
        missing_command_fn=missing_command,
        db_dupes_command_fn=db_dupes_command,
        es_dupes_command_fn=es_dupes_command,
        dupes_status_command_fn=dupes_status_command,
        cancel_task_command_fn=cancel_task_command,
        worker_status_command_fn=worker_status_command,
        live_activity_command_fn=live_activity_command,
    )
    if handled_admin:
        return True
    return False


async def _send_animated_start_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not update.message:
        return False
    final_text = _build_start_greeting_text(lang, update.effective_user)
    draft_started_at: float | None = None
    if getattr(update.effective_chat, "type", None) == "private":
        try:
            if await push_private_text_draft(
                context.bot,
                update.effective_chat.id,
                final_text,
                parse_mode="HTML",
                message_thread_id=getattr(update.message, "message_thread_id", None),
            ):
                draft_started_at = time.monotonic()
        except Exception:
            draft_started_at = None
    await hold_private_transient_draft("greeting", draft_started_at)
    uid = update.effective_user.id if update.effective_user else None
    sent = await _send_with_retry(
        lambda: update.message.reply_text(
            final_text,
            reply_markup=_main_menu_keyboard(lang, "main", uid),
            parse_mode="HTML",
        )
    )
    if not sent:
        return False
    context.user_data["main_menu_section"] = "main"
    context.user_data["awaiting_book_search"] = True
    context.user_data["search_mode"] = "book"
    return True


def _set_main_menu_ready_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["main_menu_section"] = "main"
    context.user_data["awaiting_book_search"] = True
    context.user_data["search_mode"] = "book"


async def _edit_or_send_animated_start_greeting(query, context: ContextTypes.DEFAULT_TYPE, lang: str):
    final_text = _build_start_greeting_text(lang, getattr(query, "from_user", None))
    target_message = getattr(query, "message", None)
    uid = getattr(getattr(query, "from_user", None), "id", None)
    target_chat_id = getattr(getattr(target_message, "chat", None), "id", None) or uid
    reply_markup = _main_menu_keyboard(lang, "main", uid)
    draft_started_at: float | None = None
    if target_chat_id:
        try:
            if await push_private_text_draft(
                context.bot,
                target_chat_id,
                final_text,
                parse_mode="HTML",
                message_thread_id=getattr(target_message, "message_thread_id", None) if target_message else None,
            ):
                draft_started_at = time.monotonic()
        except Exception:
            draft_started_at = None
    await hold_private_transient_draft("greeting", draft_started_at)

    async def _cleanup_old_picker():
        if not target_message:
            return
        try:
            await target_message.delete()
        except Exception:
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass

    # Send main menu immediately; cleanup of old picker message is done in background.
    try:
        sent = await _send_with_retry(
            lambda: context.bot.send_message(
                chat_id=target_chat_id,
                text=final_text,
                reply_markup=reply_markup,
                parse_mode="HTML",
            )
        )
        if sent is not None:
            _set_main_menu_ready_state(context)
            _schedule_application_task(context.application, _cleanup_old_picker())
            return sent
    except Exception as e:
        logger.warning("Primary start greeting send failed for user %s in chat %s: %s", uid, target_chat_id, e)

    if target_message:
        try:
            sent = await _send_with_retry(
                lambda: target_message.reply_text(
                    final_text,
                    reply_markup=reply_markup,
                    parse_mode="HTML",
                )
            )
            if sent is not None:
                _set_main_menu_ready_state(context)
                _schedule_application_task(context.application, _cleanup_old_picker())
                return sent
        except Exception as e:
            logger.error("Fallback start greeting send failed for user %s: %s", uid, e)

    return None


async def handle_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    lang = query.data.split("_")[1]  # uz / en / ru
    is_group_context = _is_group_chat(getattr(query, "message", None).chat if getattr(query, "message", None) else None)
    if is_group_context:
        context.user_data["group_language"] = lang
    else:
        context.user_data["language"] = lang
    await safe_answer(query)
    async def _persist_language():
        try:
            await run_blocking_db_retry(
                set_user_group_language if is_group_context else set_user_language,
                query.from_user.id,
                lang,
                retries=DB_RETRY_ATTEMPTS,
                base_delay=DB_RETRY_BASE_DELAY_SEC,
            )
        except Exception as e:
            logger.error(f"Failed to persist user language {query.from_user.id}: {e}")

    _schedule_application_task(context.application, _persist_language())
    if is_group_context:
        sent = await _send_group_search_ready_message(getattr(query, "message", None), context, lang)
        if getattr(query, "message", None):
            try:
                await query.message.delete()
            except Exception:
                try:
                    await query.edit_message_reply_markup(reply_markup=None)
                except Exception:
                    pass
    else:
        sent = await _edit_or_send_animated_start_greeting(query, context, lang)
    try:
        if not is_group_context:
            uid = query.from_user.id
            _schedule_application_task(
                context.application,
                _sync_user_commands_if_needed(context, uid, lang, force=True),
            )
    except Exception as e:
        logger.error(f"Failed to update user commands language: {e}")
    if sent is None:
        logger.error("Language selection completed but follow-up message was not delivered for user %s", query.from_user.id)


# Upload flow extracted module bridge
_UPLOAD_FLOW_OPTIONAL_DEP_KEYS = (
    "run_blocking_heavy",
    "_schedule_application_task",
    "bulk_index_books",
    "db_update_book_path",
    "db_get_book_by_id",
    "update_book_file_id",
    "SEARCH_CACHE_NS",
    "cache_clear_pattern",
    "cache_delete",
    "BOOK_STORAGE_CHANNEL_ID",
)


def _build_upload_flow_deps() -> dict[str, object]:
    deps: dict[str, object] = {}
    missing: list[str] = []
    required = tuple(getattr(_upload_flow, "_CONFIG_REQUIRED_KEYS", ()) or ())
    for key in required:
        if key not in globals():
            missing.append(key)
            continue
        deps[key] = globals()[key]
    for key in _UPLOAD_FLOW_OPTIONAL_DEP_KEYS:
        if key in globals():
            deps[key] = globals()[key]
    if missing:
        raise RuntimeError(f"Missing upload_flow dependencies: {', '.join(missing)}")
    return deps


_upload_flow.configure(_build_upload_flow_deps())

upload_command = _upload_flow.upload_command
_process_upload = _upload_flow._process_upload
_raw_handle_file = _upload_flow.handle_file
_raw_handle_photo_message = _upload_flow.handle_photo_message
sync_unindexed_books = _upload_flow.sync_unindexed_books


async def handle_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback photo handler for the core bot."""
    lang = ensure_user_language(update, context)
    chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
    if chat_type in {"group", "supergroup"}:
        await safe_reply(update, MESSAGES[lang].get("group_upload_not_needed", MESSAGES["en"]["group_upload_not_needed"]))
        return
    relay_handler = getattr(_search_flow, "handle_pending_comment_relay_message", None)
    if callable(relay_handler) and context.user_data.get("pending_comment_relay"):
        if await relay_handler(update, context):
            return
    await _raw_handle_photo_message(update, context)


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback document handler for uploads and book search."""
    lang = ensure_user_language(update, context)
    chat_type = str(getattr(update.effective_chat, "type", "") or "").lower()
    if chat_type in {"group", "supergroup"}:
        await safe_reply(update, MESSAGES[lang].get("group_upload_not_needed", MESSAGES["en"]["group_upload_not_needed"]))
        return
    relay_handler = getattr(_search_flow, "handle_pending_comment_relay_message", None)
    if callable(relay_handler) and context.user_data.get("pending_comment_relay"):
        if await relay_handler(update, context):
            return
    await _raw_handle_file(update, context)


async def handle_video_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ignore standalone video/GIF media for the core bot unless a comment relay is pending."""
    relay_handler = getattr(_search_flow, "handle_pending_comment_relay_message", None)
    if callable(relay_handler) and context.user_data.get("pending_comment_relay"):
        if await relay_handler(update, context):
            return
    return


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


def _audit_display_user(row: dict[str, Any] | None) -> str:
    item = row or {}
    user_id = item.get("user_id")
    username = str(item.get("username") or "").strip().lstrip("@")
    full_name = " ".join(
        part for part in [str(item.get("first_name") or "").strip(), str(item.get("last_name") or "").strip()] if part
    ).strip()
    label = full_name or (f"@{username}" if username else (str(user_id) if user_id else "unknown"))
    extras: list[str] = []
    if username and f"@{username}" != label:
        extras.append(f"@{username}")
    if user_id:
        extras.append(str(user_id))
    return f"{label} ({' | '.join(extras)})" if extras else label


def _audit_shorten(value: str | None, limit: int = 64) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _audit_format_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value or "-")


async def _build_guest_audit_report(lang: str) -> str:
    guest_group_stats = await run_blocking(db_get_guest_group_audit_stats, 10)
    guest_user_stats = await run_blocking(db_get_guest_user_audit_stats, 10)
    lines = [
        "👥 Guest Audit",
        "──────────",
        f"- Guest searches: {guest_user_stats.get('total_searches', 0)}",
        f"- Guest users: {guest_user_stats.get('total_users', 0)}",
        f"- Guest groups: {guest_group_stats.get('total_groups', 0)}",
        f"- Group searches: {guest_group_stats.get('total_group_searches', 0)}",
    ]

    top_users = guest_user_stats.get("top_users") or []
    if top_users:
        lines.append("──────────")
        lines.append("Top guest users")
        for row in top_users:
            lines.append(
                f"- {_audit_display_user(row)} | searches={int(row.get('searches') or 0)} | last={_audit_format_dt(row.get('last_seen_at'))}"
            )

    recent_searches = guest_user_stats.get("recent_searches") or []
    if recent_searches:
        lines.append("──────────")
        lines.append("Recent guest searches")
        for row in recent_searches:
            group_title = str(row.get("group_title") or row.get("group_username") or row.get("chat_id") or "-").strip()
            query_text = _audit_shorten(row.get("query_text"), 70)
            lines.append(
                f"- {_audit_display_user(row)} | {group_title} | {_audit_format_dt(row.get('searched_at'))} | {query_text or '—'}"
            )

    groups = guest_group_stats.get("groups") or []
    if groups:
        lines.append("──────────")
        lines.append("Recent guest groups")
        for group in groups:
            group_title = str(group.get("title") or group.get("username") or group.get("chat_id") or "-").strip()
            public_link = str(group.get("public_link") or "").strip()
            if not public_link:
                username = str(group.get("username") or "").strip().lstrip("@")
                public_link = f"https://t.me/{username}" if username else "no public link"
            lines.append(
                f"- {group_title} | {group.get('chat_id') or '-'} | searches={int(group.get('searches') or 0)} | {public_link}"
            )

    return "\n".join(lines)


async def _build_inline_audit_report(lang: str) -> str:
    inline_stats = await run_blocking(db_get_inline_audit_stats, 10)
    lines = [
        "🔎 Inline Audit",
        "──────────",
        f"- Inline searches: {inline_stats.get('total_searches', 0)}",
        f"- Inline users: {inline_stats.get('total_users', 0)}",
        f"- Chosen inline results: {inline_stats.get('total_choices', 0)}",
    ]

    top_users = inline_stats.get("top_users") or []
    if top_users:
        lines.append("──────────")
        lines.append("Top inline users")
        for row in top_users:
            lines.append(
                f"- {_audit_display_user(row)} | searches={int(row.get('searches') or 0)} | last={_audit_format_dt(row.get('last_seen_at'))}"
            )

    recent_searches = inline_stats.get("recent_searches") or []
    if recent_searches:
        lines.append("──────────")
        lines.append("Recent inline searches")
        for row in recent_searches:
            lines.append(
                f"- {_audit_display_user(row)} | {_audit_format_dt(row.get('searched_at'))} | {_audit_shorten(row.get('query_text'), 80) or '—'}"
            )

    recent_choices = inline_stats.get("recent_choices") or []
    if recent_choices:
        lines.append("──────────")
        lines.append("Recent chosen inline results")
        for row in recent_choices:
            lines.append(
                f"- {_audit_display_user(row)} | {_audit_format_dt(row.get('chosen_at'))} | result={row.get('result_id') or '-'} | {_audit_shorten(row.get('query_text'), 60) or '—'}"
            )

    return "\n".join(lines)


async def guest_audit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        report = await _build_guest_audit_report(lang)
        await target_message.reply_text(report)
    except Exception as e:
        logger.error("Guest audit command failed: %s", e, exc_info=True)
        await target_message.reply_text(MESSAGES[lang]["audit_failed"])


async def inline_audit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    try:
        report = await _build_inline_audit_report(lang)
        await target_message.reply_text(report)
    except Exception as e:
        logger.error("Inline audit command failed: %s", e, exc_info=True)
        await target_message.reply_text(MESSAGES[lang]["audit_failed"])


async def audit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

        try:
            local_backup_queue = await run_blocking(db_get_book_local_download_job_status_counts)
        except Exception as e:
            logger.error(f"Local backup queue stats failed: {e}")
            local_backup_queue = {'queued': 0, 'downloading': 0, 'done': 0, 'failed': 0, 'total': 0, 'pending': 0}

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
            "upload_request_created",
            "reaction_like",
            "reaction_dislike",
            "reaction_berry",
            "reaction_whale",
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
            f"- Upload requests created: {counters.get('upload_request_created', 0)}",
            f"- {MESSAGES[lang]['audit_upload_accept_total']}: {counters.get('upload_accept', 0)}",
            f"- {MESSAGES[lang]['audit_upload_reject_total']}: {counters.get('upload_reject', 0)}",
        ]

        lines += [
            "──────────",
            "💾 Storage",
            f"- Total files: {storage_stats.get('total_files', 0)}",
            f"- Total size: {_format_bytes(storage_stats.get('total_size', 0))}",
            f"- 📚 Local books: {storage_stats.get('book_count', 0)} files ({_format_bytes(storage_stats.get('total_book_size', 0))})",
            f"- Audio: {storage_stats.get('audio_count', 0)} files ({_format_bytes(storage_stats.get('total_audio_size', 0))})",
            f"- Avg book size: {_format_bytes(storage_stats.get('avg_book_size', 0))}",
            f"- Avg audio size: {_format_bytes(storage_stats.get('avg_audio_size', 0))}",
        ]

        lines += [
            "──────────",
            "📦 Local backup queue",
            f"- Queued: {local_backup_queue.get('queued', 0)}",
            f"- Downloading: {local_backup_queue.get('downloading', 0)}",
            f"- Done: {local_backup_queue.get('done', 0)}",
            f"- Failed: {local_backup_queue.get('failed', 0)}",
            f"- Left to process: {local_backup_queue.get('pending', 0)}",
            "──────────",
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
_search_flow.configure(_build_search_flow_deps())

transliterate_to_latin = _search_flow.transliterate_to_latin
search_books = _search_flow.search_books
handle_book_selection = _search_flow.handle_book_selection
handle_audiobook_callback = _search_flow.handle_audiobook_callback
handle_audiobook_part_callback = _search_flow.handle_audiobook_part_callback
handle_audiobook_part_delete_callback = _search_flow.handle_audiobook_part_delete_callback
handle_audiobook_delete_callback = _search_flow.handle_audiobook_delete_callback
handle_audiobook_delete_by_book_callback = _search_flow.handle_audiobook_delete_by_book_callback
handle_audiobook_listen_callback = _search_flow.handle_audiobook_listen_callback
handle_audiobook_play_all_callback = _search_flow.handle_audiobook_play_all_callback
handle_audiobook_page_callback = _search_flow.handle_audiobook_page_callback
handle_audiobook_part_play_callback = _search_flow.handle_audiobook_part_play_callback
handle_audiobook_add_callback = _search_flow.handle_audiobook_add_callback
handle_book_rename_callback = _search_flow.handle_book_rename_callback
handle_book_reaction_edit_callback = _search_flow.handle_book_reaction_edit_callback
handle_book_reaction_policy_callback = _search_flow.handle_book_reaction_policy_callback
handle_book_comments_callback = _search_flow.handle_book_comments_callback
handle_book_comment_thread_callback = _search_flow.handle_book_comment_thread_callback
handle_book_comment_add_callback = _search_flow.handle_book_comment_add_callback
handle_book_comment_reply_callback = _search_flow.handle_book_comment_reply_callback
handle_my_comments_page_callback = _search_flow.handle_my_comments_page_callback
handle_my_comment_view_callback = _search_flow.handle_my_comment_view_callback
handle_my_comment_edit_callback = _search_flow.handle_my_comment_edit_callback
handle_my_comment_delete_callback = _search_flow.handle_my_comment_delete_callback
handle_my_chats_page_callback = _search_flow.handle_my_chats_page_callback
handle_my_chat_view_callback = _search_flow.handle_my_chat_view_callback
handle_my_chat_delete_callback = _search_flow.handle_my_chat_delete_callback
comment_inbox_command = _search_flow.send_comments_inbox
my_chats_command = _search_flow.send_my_chats_panel
handle_comment_inbox_callback = _search_flow.handle_comment_inbox_callback
handle_comment_conversation_callback = _search_flow.handle_comment_conversation_callback
handle_comment_conversation_mute_callback = _search_flow.handle_comment_conversation_mute_callback
handle_book_comment_relay_reply_callback = _search_flow.handle_book_comment_relay_reply_callback
handle_book_comment_identity_request_callback = _search_flow.handle_book_comment_identity_request_callback
handle_book_comment_identity_resolve_callback = _search_flow.handle_book_comment_identity_resolve_callback
handle_book_comment_report_callback = _search_flow.handle_book_comment_report_callback
handle_book_comment_user_ban_toggle_callback = _search_flow.handle_book_comment_user_ban_toggle_callback
handle_book_comment_relay_block_callback = _search_flow.handle_book_comment_relay_block_callback
handle_book_comment_relay_report_callback = _search_flow.handle_book_comment_relay_report_callback
handle_book_comment_moderation_callback = _search_flow.handle_book_comment_moderation_callback
handle_abook_audio = _search_flow.handle_abook_audio
handle_page_callback = _search_flow.handle_page_callback
handle_user_page_callback = _search_flow.handle_user_page_callback
handle_user_select_callback = _search_flow.handle_user_select_callback


# Engagement handlers extracted module bridge
_engagement_handlers.configure(_build_engagement_handlers_deps())

handle_user_action_callback = _engagement_handlers.handle_user_action_callback
top_users_command = _engagement_handlers.top_users_command
handle_top_users_toggle_callback = _engagement_handlers.handle_top_users_toggle_callback
top_command = _engagement_handlers.top_command
handle_top_page_callback = _engagement_handlers.handle_top_page_callback
handle_favorite_callback = _engagement_handlers.handle_favorite_callback
handle_reaction_callback = _engagement_handlers.handle_reaction_callback
handle_summary_placeholder_callback = _engagement_handlers.handle_summary_placeholder_callback
handle_delete_book_callback = _engagement_handlers.handle_delete_book_callback


# User interactions extracted module bridge
_user_interactions.configure(_build_user_interactions_deps())

handle_request_callback = _user_interactions.handle_request_callback
handle_request_status_callback = _user_interactions.handle_request_status_callback
handle_requests_page_callback = _user_interactions.handle_requests_page_callback
handle_requests_view_callback = _user_interactions.handle_requests_view_callback
handle_request_cancel_callback = _user_interactions.handle_request_cancel_callback
handle_upload_request_status_callback = _user_interactions.handle_upload_request_status_callback
handle_upload_help_callback = _user_interactions.handle_upload_help_callback
favorites_command = _user_interactions.favorites_command
random_command = _user_interactions.random_command
help_command = _user_interactions.help_command
request_command = _user_interactions.request_command
requests_command = _user_interactions.requests_command
myprofile_command = _user_interactions.myprofile_command
mystats_command = _user_interactions.mystats_command


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_admin_user(update.effective_user.id):
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
    transient_failures = 0

    def _definitive_user_unreachable_error(exc: Exception) -> bool:
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
            await context.bot.send_message(chat_id=user_id, text=message)
            sent_count += 1
        except Exception as e:
            if _definitive_user_unreachable_error(e):
                logger.info("Broadcast: marking unreachable user %s: %s", user_id, e)
                await run_blocking(update_user_left_date, user_id, datetime.now().date())
                blocked_users.append(user)
            elif isinstance(e, RetryAfter):
                transient_failures += 1
                delay = int(getattr(e, "retry_after", 1) or 1)
                await asyncio.sleep(max(1, delay))
            elif isinstance(e, (NetworkError, TimedOut)):
                transient_failures += 1
                logger.warning("Broadcast: transient error for user %s: %s", user_id, e)
            else:
                transient_failures += 1
                logger.warning("Broadcast: non-definitive send error for user %s: %s", user_id, e)

    if blocked_users:
        await run_blocking(insert_removed_users, blocked_users)

    await update.message.reply_text(
        MESSAGES[lang]["broadcast_done"].format(sent=sent_count, blocked=len(blocked_users))
    )
    if transient_failures:
        logger.warning("Broadcast completed with %s transient/non-definitive failures", transient_failures)

    
async def inlinequery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and is_blocked(update.effective_user.id):
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    query = update.inline_query.query.strip()
    lang = ensure_user_language(update, context)
    msgs = MESSAGES.get(lang, MESSAGES["en"])
    results = []
    inline_cache_time = 30
    inline_action_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(msgs.get("inline_more_books_button", "📚 Ko'proq kitoblar"), url="https://t.me/pdf_audio_kitoblar_bot")]]
    )

    def _inline_format_label(book_path: str | None) -> str:
        ext = os.path.splitext(str(book_path or "").strip())[1].strip().lstrip(".").upper()
        return ext or "BOOK"

    if not query:
        await update.inline_query.answer([], cache_time=inline_cache_time, is_personal=True)
        return

    token = query.split()[0].strip()
    if token.startswith("mshare_"):
        await update.inline_query.answer([], cache_time=inline_cache_time, is_personal=True)
        return

    if not (update.effective_user and _is_admin_user(update.effective_user.id)):
        limited, _wait_s = rate_limited(context, "last_inline_search_ts", INLINE_SEARCH_COOLDOWN_SEC)
        if limited:
            await update.inline_query.answer([], cache_time=1, is_personal=True)
            return

    try:
        if update.effective_user:
            await run_blocking(
                db_record_inline_search_activity,
                update.effective_user.id,
                update.effective_user.username,
                update.effective_user.first_name,
                update.effective_user.last_name,
                query,
            )
            _invalidate_audit_report_cache(context)
    except Exception as e:
        logger.warning("Failed to record inline search activity: %s", e, exc_info=True)

    try:
        books = await run_blocking(_inline_search_books, query, 10)

        for book in books:
            book_id = str(book.get("id") or "").strip()
            if not book_id:
                continue
            title = get_result_title(book)
            path = book.get("path")
            file_id = book.get("file_id")
            display_title = f"📖 {title}"
            safe_title = html.escape(title)
            inline_caption = f"📖 {safe_title}"
            format_label = _inline_format_label(path)

            if file_id:
                result = InlineQueryResultCachedDocument(
                    id=book_id,
                    title=display_title,
                    document_file_id=file_id,
                    description=msgs.get("inline_cached_description", "⚡ Ready to send • {format}").format(format=format_label),
                    caption=inline_caption,
                    reply_markup=inline_action_markup,
                    parse_mode="HTML",
                )
            elif path and os.path.exists(path):
                message = f"📖 {safe_title}"
                result = InlineQueryResultArticle(
                    id=book_id,
                    title=display_title,
                    input_message_content=InputTextMessageContent(
                        message, parse_mode="HTML"
                    ),
                    reply_markup=inline_action_markup,
                    description=msgs.get("inline_local_description", "📚 Available in the bot library • {format}").format(format=format_label),
                )
            else:
                result = InlineQueryResultArticle(
                    id=book_id,
                    title=display_title,
                    input_message_content=InputTextMessageContent(
                        f"📖 {safe_title}",
                        parse_mode="HTML"
                    ),
                    reply_markup=inline_action_markup,
                    description=msgs.get("inline_info_description", "ℹ️ Info card • {format}").format(format=format_label),
                )

            results.append(result)

    except Exception as e:
        logger.error(f"Inline search failed: {e}", exc_info=True)
        await update.inline_query.answer([], cache_time=1, is_personal=True)
        return

    await update.inline_query.answer(results, cache_time=inline_cache_time, is_personal=True)


async def chosen_inline_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chosen = getattr(update, "chosen_inline_result", None)
    user = getattr(chosen, "from_user", None)
    if not chosen or not user:
        return
    try:
        await run_blocking(
            db_record_inline_chosen_activity,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            getattr(chosen, "query", None),
            getattr(chosen, "result_id", None),
        )
        _invalidate_audit_report_cache(context)
    except Exception as e:
        logger.warning("Failed to record chosen inline result: %s", e, exc_info=True)



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
    if not _is_admin_user(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["admin_only"])
        return

    await target_message.reply_text(MESSAGES[lang]["prune_started"])
    chat_id = update.effective_chat.id if update.effective_chat else getattr(target_message, "chat_id", None)
    _schedule_application_task(context.application, _prune_and_notify(context, chat_id, lang))


async def missing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not _is_admin_user(update.effective_user.id):
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


# Refresh search_flow dependencies after late-bound feature bridges.
_search_flow.configure(_build_search_flow_deps())


# Admin runtime extracted module bridge
_admin_runtime.configure(_build_admin_runtime_deps())

_list_running_background_tasks = _admin_runtime._list_running_background_tasks
_background_tasks_keyboard = _admin_runtime._background_tasks_keyboard
_format_background_tasks_text = _admin_runtime._format_background_tasks_text
_admin_panel_snapshot_text = _admin_runtime._admin_panel_snapshot_text
_admin_panel_keyboard = _admin_runtime._admin_panel_keyboard
_admin_panel_send_or_edit = _admin_runtime._admin_panel_send_or_edit
_admin_panel_send_missing_preview = _admin_runtime._admin_panel_send_missing_preview
admin_panel_command = _admin_runtime.admin_panel_command
handle_admin_panel_callback = _admin_runtime.handle_admin_panel_callback
smoke_check_command = _admin_runtime.smoke_check_command
cancel_task_command = _admin_runtime.cancel_task_command
worker_status_command = _admin_runtime.worker_status_command
live_activity_command = _admin_runtime.live_activity_command
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

# Refresh search_flow again after admin runtime aliases (e.g. user_search_command).
_search_flow.configure(_build_search_flow_deps())


_BACKGROUND_JOB_WORKERS_KEY = "background_job_workers"
_BACKGROUND_JOB_WORKER_TARGET_KEY = "background_job_worker_target"
_BACKGROUND_JOB_TYPE_INFLIGHT_KEY = "background_job_type_inflight"
_BACKGROUND_JOB_TYPE_LIMITS_KEY = "background_job_type_limits"
_BACKGROUND_JOB_RUNTIME_LOCK_KEY = "background_job_runtime_lock"


def _get_background_job_runtime_state(app: Application) -> tuple[asyncio.Lock, dict[str, int], dict[str, int]]:
    data = app.bot_data
    lock = data.get(_BACKGROUND_JOB_RUNTIME_LOCK_KEY)
    if lock is None:
        lock = asyncio.Lock()
        data[_BACKGROUND_JOB_RUNTIME_LOCK_KEY] = lock
    inflight = data.get(_BACKGROUND_JOB_TYPE_INFLIGHT_KEY)
    if not isinstance(inflight, dict):
        inflight = {}
        data[_BACKGROUND_JOB_TYPE_INFLIGHT_KEY] = inflight
    limits = data.get(_BACKGROUND_JOB_TYPE_LIMITS_KEY)
    if not isinstance(limits, dict):
        limits = dict(BACKGROUND_JOB_TYPE_LIMITS)
        data[_BACKGROUND_JOB_TYPE_LIMITS_KEY] = limits
    return lock, inflight, limits


async def _claim_background_job_for_worker(app: Application, worker_id: str) -> dict | None:
    lock, inflight, limits = _get_background_job_runtime_state(app)
    async with lock:
        excluded_types = [
            job_type
            for job_type, limit in limits.items()
            if int(limit or 0) > 0 and int(inflight.get(job_type, 0) or 0) >= int(limit or 0)
        ]
        job = await run_blocking(
            db_claim_background_job,
            worker_id,
            BACKGROUND_JOB_STALE_AFTER_SECONDS,
            None,
            excluded_types,
        )
        if job:
            job_type = str(job.get("job_type") or "").strip() or "unknown"
            inflight[job_type] = int(inflight.get(job_type, 0) or 0) + 1
        return job


async def _finish_background_job_for_worker(app: Application, job_type: str) -> None:
    lock, inflight, _limits = _get_background_job_runtime_state(app)
    async with lock:
        key = str(job_type or "").strip() or "unknown"
        current = int(inflight.get(key, 0) or 0) - 1
        if current > 0:
            inflight[key] = current
        else:
            inflight.pop(key, None)


def start_background_job_workers(app: Application) -> None:
    if app.bot_data.get(_SHUTDOWN_IN_PROGRESS_KEY):
        return
    data = app.bot_data
    data[_BACKGROUND_JOB_WORKER_TARGET_KEY] = BACKGROUND_JOB_WORKER_COUNT
    data[_BACKGROUND_JOB_TYPE_LIMITS_KEY] = dict(BACKGROUND_JOB_TYPE_LIMITS)
    workers = data.get(_BACKGROUND_JOB_WORKERS_KEY)
    live_workers = []
    if isinstance(workers, list):
        live_workers = [task for task in workers if task is not None and not task.done()]
        data[_BACKGROUND_JOB_WORKERS_KEY] = live_workers
    elif workers and not getattr(workers, "done", lambda: True)():
        live_workers = [workers]
        data[_BACKGROUND_JOB_WORKERS_KEY] = live_workers
    missing_workers = max(0, BACKGROUND_JOB_WORKER_COUNT - len(live_workers))
    if missing_workers <= 0:
        return
    start_index = len(live_workers)
    created_workers = live_workers + [
        _spawn_managed_background_task(app, _process_background_jobs(app, worker_index=start_index + index + 1))
        for index in range(missing_workers)
    ]
    data[_BACKGROUND_JOB_WORKERS_KEY] = created_workers
    logger.info(
        "Started %s background job worker(s) with type limits=%s",
        missing_workers,
        BACKGROUND_JOB_TYPE_LIMITS,
    )


async def _process_background_jobs(app: Application, worker_index: int = 1) -> None:
    """Background task to process queued jobs."""
    worker_id = f"bg-worker:{os.getpid()}:{worker_index}:{uuid.uuid4().hex[:6]}"
    logger.info("Starting background job processor: %s", worker_id)
    try:
        while True:
            job = None
            job_type = ""
            job_id = ""
            started_mono = 0.0
            claimed = False
            released = False
            try:
                job = await _claim_background_job_for_worker(app, worker_id)
                if not job:
                    await asyncio.sleep(BACKGROUND_JOB_IDLE_SECONDS)
                    continue

                job_id = str(job.get("id") or "").strip()
                job_type = str(job.get("job_type") or "").strip()
                user_id = int(job.get("user_id") or 0)
                chat_id = int(job.get("chat_id") or user_id or 0)
                payload_raw = job.get("payload_json")
                if payload_raw is None:
                    payload_raw = job.get("data_json", "{}")
                data = db_deserialize_background_job_payload(payload_raw)
                if not isinstance(data, dict):
                    data = {}

                started_mono = time.monotonic()
                claimed = True
                logger.info(
                    "job claimed: job_id=%s job_type=%s user_id=%s chat_id=%s worker=%s attempts=%s/%s",
                    job_id,
                    job_type,
                    user_id,
                    chat_id,
                    worker_id,
                    int(job.get("attempts", 0) or 0),
                    int(job.get("max_attempts", 0) or 0),
                )
                _set_background_job_activity(
                    app,
                    worker_id,
                    worker="background_job",
                    worker_index=worker_index,
                    job_id=job_id,
                    job_type=job_type,
                    user_id=user_id,
                    chat_id=chat_id,
                    stage="processing payload",
                )
                _job_workspace_dir(job_id)
                await run_blocking(db_update_background_job_progress, job_id, 5, {"stage": "started"})

                success = False
                result_data = None
                error_msg = None

                try:
                    with _job_temp_environment(job_id):
                        result_data = await _process_background_job_payload(job_type, data, app)

                    if result_data:
                        success = True
                        _set_background_job_activity(
                            app,
                            worker_id,
                            worker="background_job",
                            worker_index=worker_index,
                            job_id=job_id,
                            job_type=job_type,
                            user_id=user_id,
                            chat_id=chat_id,
                            stage="sending result",
                        )
                        await run_blocking(db_update_background_job_progress, job_id, 90, {"stage": "sending_result"})
                        await _send_job_result(app, job, data, job_type, result_data)
                    else:
                        error_msg = error_msg or "Job processing failed"

                except Exception as e:
                    error_msg = str(e)
                    logger.error(
                        "job failed during processing: job_id=%s job_type=%s user_id=%s chat_id=%s error=%s",
                        job_id,
                        job_type,
                        user_id,
                        chat_id,
                        error_msg,
                        exc_info=True,
                    )

                if success:
                    _set_background_job_activity(
                        app,
                        worker_id,
                        worker="background_job",
                        worker_index=worker_index,
                        job_id=job_id,
                        job_type=job_type,
                        user_id=user_id,
                        chat_id=chat_id,
                        stage="completed",
                    )
                    await run_blocking(db_complete_background_job, job_id, result_data if isinstance(result_data, dict) else None)
                    released = True
                    _clear_job_failed_workspace(job_id)
                    logger.info(
                        "job done: job_id=%s job_type=%s user_id=%s chat_id=%s duration=%.2fs",
                        job_id,
                        job_type,
                        user_id,
                        chat_id,
                        max(0.0, time.monotonic() - started_mono),
                    )
                else:
                    attempts = int(job.get("attempts", 0) or 0)
                    max_attempts = int(job.get("max_attempts", 3) or 3)
                    if attempts >= max_attempts:
                        await run_blocking(db_fail_background_job, job_id, error_msg or "Max attempts reached")
                        released = True
                        _mark_job_failed_workspace(job_id)
                        logger.error(
                            "job failed permanently: job_id=%s job_type=%s user_id=%s chat_id=%s attempts=%s duration=%.2fs error=%s",
                            job_id,
                            job_type,
                            user_id,
                            chat_id,
                            attempts,
                            max(0.0, time.monotonic() - started_mono),
                            error_msg,
                        )
                        await _send_job_failure(app, job, data, job_type, error_msg or "Processing failed")
                    else:
                        await run_blocking(db_retry_background_job, job_id, error_msg or "Processing failed")
                        released = True
                        _mark_job_failed_workspace(job_id)
                        logger.warning(
                            "job retry scheduled: job_id=%s job_type=%s user_id=%s chat_id=%s attempts=%s/%s error=%s",
                            job_id,
                            job_type,
                            user_id,
                            chat_id,
                            attempts,
                            max_attempts,
                            error_msg,
                        )

            except asyncio.CancelledError:
                if claimed and job_id and not released:
                    try:
                        await run_blocking(db_retry_background_job, job_id, "Worker shutdown")
                        released = True
                    except Exception:
                        logger.exception("Failed to release background job during shutdown: %s", job_id)
                raise
            except Exception as e:
                logger.error("Background job processor error (%s): %s", worker_id, e, exc_info=True)
                await asyncio.sleep(15.0)
            finally:
                if job_type:
                    await _finish_background_job_for_worker(app, job_type)
    finally:
        _clear_background_job_activity(app, worker_id)
        current_task = _safe_asyncio_current_task()
        workers = app.bot_data.get(_BACKGROUND_JOB_WORKERS_KEY)
        if current_task is not None and isinstance(workers, list):
            remaining = [task for task in workers if task is not current_task and not task.done()]
            if remaining:
                app.bot_data[_BACKGROUND_JOB_WORKERS_KEY] = remaining
            else:
                app.bot_data.pop(_BACKGROUND_JOB_WORKERS_KEY, None)


async def _process_book_summary_job(data: dict, app: Application) -> dict | None:
    logger.info(
        "Ignoring legacy disabled background job: book_summary payload_keys=%s",
        sorted((data or {}).keys()),
    )
    return {"suppress_send": True, "disabled": True}


async def _process_search_index_book_job(data: dict) -> dict | None:
    doc = dict(data.get("doc") or {})
    receipt_id = str(data.get("receipt_id") or "").strip() or None
    book_id = str(data.get("book_id") or doc.get("id") or "").strip()
    file_unique_id = str(doc.get("file_unique_id") or data.get("file_unique_id") or "").strip()

    if not book_id and receipt_id:
        try:
            receipt = await run_blocking(db_get_upload_receipt_by_id, receipt_id)
            if receipt:
                book_id = str(receipt.get("book_id") or "").strip()
        except Exception:
            logger.exception("Failed to recover book_id from upload receipt: %s", receipt_id)

    if not book_id and file_unique_id:
        try:
            recovered_book = await run_blocking(db_get_book_by_file_unique_id, file_unique_id)
            if recovered_book:
                book_id = str(recovered_book.get("id") or "").strip()
                doc.setdefault("book_name", recovered_book.get("book_name"))
                doc.setdefault("display_name", recovered_book.get("display_name") or recovered_book.get("book_name"))
                doc.setdefault("file_id", recovered_book.get("file_id"))
                doc.setdefault("file_unique_id", recovered_book.get("file_unique_id"))
                doc.setdefault("path", recovered_book.get("path"))
        except Exception:
            logger.exception("Failed to recover book_id from file_unique_id: %s", file_unique_id)

    if not book_id:
        logger.warning(
            "SEARCH_INDEX_BOOK skipped: missing book_id in payload keys=%s receipt_id=%s file_unique_id=%s",
            sorted((data or {}).keys()),
            receipt_id,
            file_unique_id,
        )
        if receipt_id:
            try:
                await run_blocking(
                    db_update_upload_receipt,
                    receipt_id,
                    status="index_failed",
                    saved_to_db=True,
                    saved_to_es=False,
                    error="missing book id",
                )
            except Exception:
                logger.exception("Failed to update upload receipt after missing book_id: %s", receipt_id)
        return {"text": "index skipped: missing book id", "suppress_send": True}

    ok = False
    err = None
    try:
        indexed_id = await run_blocking(
            index_book,
            doc.get("book_name"),
            doc.get("file_id"),
            doc.get("path"),
            book_id,
            doc.get("display_name") or doc.get("book_name"),
            doc.get("file_unique_id"),
        )
        ok = bool(indexed_id)
        if ok:
            await run_blocking(update_book_indexed, book_id, True)
        else:
            err = "indexing failed"
    except Exception as e:
        err = str(e)
        logger.warning("Background ES index failed for book_id=%s: %s", book_id, e, exc_info=True)

    if receipt_id:
        try:
            await run_blocking(
                db_update_upload_receipt,
                receipt_id,
                status="indexed" if ok else "index_failed",
                book_id=book_id,
                saved_to_db=True,
                saved_to_es=ok,
                error=err,
            )
        except Exception:
            logger.exception("Failed to update upload receipt after background ES index: %s", receipt_id)

    if ok:
        return {"text": "indexed", "suppress_send": True}
    raise RuntimeError(err or "indexing failed")


async def _process_search_reindex_all_job(data: dict) -> dict | None:
    await run_blocking(sync_unindexed_books)
    return {"text": "reindex complete", "suppress_send": True}


async def _send_job_result(app: Application, job: dict, payload: dict, job_type: str, result: dict) -> None:
    """Send job result to user."""
    user_id = int(job.get("user_id") or 0)
    try:
        if result.get("suppress_send"):
            return
        multiple = result.get("multiple")
        if multiple:
            for item in multiple:
                await _send_single_result(app, job, payload, item)
        else:
            await _send_single_result(app, job, payload, result)
    except Exception as e:
        logger.error(f"Failed to send job result to user {user_id}: {e}")


async def _send_single_result(app: Application, job: dict, payload: dict, result: dict) -> None:
    """Send a single result item."""
    user_id = int(job.get("user_id") or 0)
    sticker_bytes = result.get("sticker_bytes")
    sticker_ext = result.get("sticker_ext")
    file_bytes = result.get("file_bytes")
    file_path = str(result.get("file_path") or "").strip()
    file_content = result.get("file_content")
    filename = result.get("filename")
    caption = result.get("caption", "Result")
    text = result.get("text")
    chat_id = result.get("chat_id") or payload.get("chat_id") or job.get("chat_id") or user_id
    reply_to_message_id = result.get("reply_to_message_id") or payload.get("message_id") or job.get("message_id")
    output_key = result.get("output_key")
    media_kind = str(result.get("kind") or "").strip().lower()
    cleanup_dir = str(result.get("cleanup_dir") or "").strip()

    try:
        if sticker_bytes and sticker_ext:
            bio = io.BytesIO(sticker_bytes)
            bio.name = f"sticker{sticker_ext}"
            await app.bot.send_sticker(chat_id=chat_id, sticker=bio, reply_to_message_id=reply_to_message_id)
        elif file_path and os.path.exists(file_path):
            if media_kind == "audio":
                with open(file_path, "rb") as audio_f:
                    await app.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_f,
                        caption=caption,
                        title=str(result.get("title") or Path(file_path).stem)[:64],
                        filename=filename or os.path.basename(file_path),
                        reply_to_message_id=reply_to_message_id,
                    )
            elif media_kind == "video":
                with open(file_path, "rb") as video_f:
                    await app.bot.send_video(
                        chat_id=chat_id,
                        video=video_f,
                        caption=caption,
                        supports_streaming=True,
                        filename=filename or os.path.basename(file_path),
                        reply_to_message_id=reply_to_message_id,
                    )
            else:
                with open(file_path, "rb") as file_f:
                    await app.bot.send_document(
                        chat_id=chat_id,
                        document=file_f,
                        caption=caption,
                        filename=filename or os.path.basename(file_path),
                        reply_to_message_id=reply_to_message_id,
                    )
        elif file_bytes and filename:
            bio = io.BytesIO(file_bytes)
            bio.name = filename
            if output_key == "voice":
                await app.bot.send_voice(chat_id=chat_id, voice=bio, caption=caption, reply_to_message_id=reply_to_message_id)
            else:
                await app.bot.send_document(chat_id=chat_id, document=bio, caption=caption, reply_to_message_id=reply_to_message_id)
        elif file_content and filename:
            bio = io.BytesIO(file_content.encode('utf-8'))
            bio.name = filename
            await app.bot.send_document(chat_id=chat_id, document=bio, caption=caption, reply_to_message_id=reply_to_message_id)
        elif text:
            await app.bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=reply_to_message_id)
        else:
            await app.bot.send_message(chat_id=chat_id, text=f"✅ {caption}", reply_to_message_id=reply_to_message_id)
    finally:
        if cleanup_dir:
            shutil.rmtree(cleanup_dir, ignore_errors=True)


async def _send_job_failure(app: Application, job: dict, payload: dict, job_type: str, error: str) -> None:
    """Send job failure notification to user."""
    user_id = int(job.get("user_id") or 0)
    chat_id = int(job.get("chat_id") or payload.get("chat_id") or user_id or 0)
    lang = str(payload.get("lang") or payload.get("lang_ui") or "uz")
    fail_text = MESSAGES.get(lang, MESSAGES["en"]).get(
        "background_job_failed",
        "❌ Vazifani bajarib bo‘lmadi. Keyinroq qayta urinib ko‘ring.",
    )
    try:
        await app.bot.send_message(chat_id=chat_id or user_id, text=fail_text)
    except Exception as e:
        logger.error(f"Failed to send job failure to user {user_id}: {e}")


def _build_handler_registry_deps() -> dict[str, object]:
    deps: dict[str, object] = {}
    missing: list[str] = []
    for key in _handler_registry.REQUIRED_DEP_KEYS:
        if key not in globals():
            missing.append(key)
            continue
        deps[key] = globals()[key]
    if missing:
        raise RuntimeError(f"Missing handler dependencies: {', '.join(missing)}")
    return deps


def main():
    if not _acquire_single_instance_lock():
        return
    try:
        config_errors = validate_runtime_config()
        if config_errors:
            logger.error("Configuration validation failed. Startup aborted.")
            for err in config_errors:
                logger.error("Config error: %s", err)
            return
        logger.debug("Runtime configuration validated")

        bot_api_base_url = _normalize_bot_api_base_url(os.getenv("TELEGRAM_BOT_API_BASE_URL", ""))
        bot_api_base_file_url = _normalize_bot_api_base_file_url(
            os.getenv("TELEGRAM_BOT_API_BASE_FILE_URL", ""),
            bot_api_base_url,
        )
        bot_api_local_mode = _env_bool("TELEGRAM_BOT_API_LOCAL_MODE", False)

        wait_for_runtime_dependencies(
            "Main bot",
            require_db=True,
            require_es=ENABLE_ELASTICSEARCH,
            bot_api_base_url=bot_api_base_url,
            require_bot_api=bool(bot_api_base_url or bot_api_local_mode),
            bot_api_local_mode=bot_api_local_mode,
        )

        # Initialize DB
        init_db()
        logger.debug("DB connected")
        try:
            _load_private_live_status_settings()
        except Exception as e:
            logger.warning("Failed to load private draft timing settings: %s", e, exc_info=True)
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
        audio_channels_for_log = list(AUDIO_UPLOAD_CHANNEL_IDS or [])
        if not audio_channels_for_log and AUDIO_UPLOAD_CHANNEL_ID:
            audio_channels_for_log = [AUDIO_UPLOAD_CHANNEL_ID]
        book_storage_channel = BOOK_STORAGE_CHANNEL_ID or 0
        logger.info(
            "Upload channels: audio=%s book_storage=%s",
            ",".join(str(x) for x in audio_channels_for_log) or "none",
            str(book_storage_channel) if book_storage_channel else "none",
        )
        es_status = "down"
        es_health = "down"
        es_count = 0
        es = get_es()
        if es:
            try:
                es.info()
                ensure_index()
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
            .post_stop(post_stop)
            .connect_timeout(20)
            .read_timeout(60)
            .write_timeout(1200)
            .pool_timeout(BOT_POOL_TIMEOUT)
            .connection_pool_size(BOT_CONNECTION_POOL_SIZE)
            .concurrent_updates(BOT_CONCURRENT_UPDATES)
        )

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
        _handler_registry.register_handlers(app, _build_handler_registry_deps())

        logger.debug("Handlers registered. Starting polling...")
        print("Bot is running...")
        startup_retry_max = max(0, int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0") or "0"))
        bootstrap_retries = -1 if startup_retry_max == 0 else startup_retry_max
        drop_pending_updates = _env_bool("DROP_PENDING_UPDATES", False)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        allowed_updates = list(Update.ALL_TYPES)
        if "guest_message" not in allowed_updates:
            allowed_updates.append("guest_message")
        app.run_polling(
            allowed_updates=allowed_updates,
            drop_pending_updates=drop_pending_updates,
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
