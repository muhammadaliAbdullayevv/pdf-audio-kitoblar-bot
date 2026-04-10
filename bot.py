import logging
import warnings
import os
import json
import re
import traceback
import tempfile
import io
import time
import math
import asyncio
import fcntl
import atexit
import html
from urllib.parse import quote_plus
from logging.handlers import RotatingFileHandler
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
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update, InputFile
from book_thumbnail import get_book_thumbnail_input

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
    REQUEST_CHAT_ID,
    BOOK_STORAGE_CHANNEL_ID,
    COIN_SEARCH,
    COIN_DOWNLOAD,
    COIN_REACTION,
    COIN_FAVORITE,
    COIN_REFERRAL,
    TOP_USERS_LIMIT,
    AUDIO_UPLOAD_CHANNEL_IDS,
    AUDIO_UPLOAD_CHANNEL_ID,
    VIDEO_UPLOAD_CHANNEL_IDS,
    VIDEO_UPLOAD_CHANNEL_ID,
    validate_runtime_config,
)

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
    increment_book_download as db_increment_book_download,
    increment_book_searches as db_increment_book_searches,
    set_book_reaction as db_set_book_reaction,
    get_book_stats as db_get_book_stats,
    get_top_books as db_get_top_books,
    get_top_users as db_get_top_users,
    insert_book as db_insert_book,
    bulk_upsert_books,
    update_book_file_id,
    update_book_indexed,
    update_book_path as db_update_book_path,
    update_book_by_path,
    enqueue_book_local_download_job as db_enqueue_book_local_download_job,
    claim_book_local_download_job as db_claim_book_local_download_job,
    complete_book_local_download_job as db_complete_book_local_download_job,
    retry_book_local_download_job as db_retry_book_local_download_job,
    fail_book_local_download_job as db_fail_book_local_download_job,
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
    search_users_by_name as db_search_users_by_name,
    is_user_delete_allowed as db_is_user_delete_allowed,
    is_user_audio_allowed as db_is_user_audio_allowed,
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

import audio_converter as _audio_converter
import sticker_tools as _sticker_tools
import video_downloader as _video_downloader
import search_flow as _search_flow
import tts_tools as _tts_tools

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
    db_get_book_stats,
    db_get_top_books,
    db_get_top_users,
    db_insert_book,
    bulk_upsert_books,
    update_book_file_id,
    update_book_indexed,
    db_update_book_path,
    update_book_by_path,
    db_enqueue_book_local_download_job,
    db_claim_book_local_download_job,
    db_complete_book_local_download_job,
    db_retry_book_local_download_job,
    db_fail_book_local_download_job,
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
    "_audio_conv_handle_media_input",
    "_audio_conv_handle_text_input",
    "_book_filename",
    "_cancel_menu_conflicting_flows",
    "_handle_main_menu_action",
    "_is_admin_user",
    "_is_owner_user",
    "_main_menu_keyboard",
    "_main_menu_text_action",
    "_pdf_maker_handle_text_input",
    "_pdfed_handle_text_input",
    "_reply_search_menu_click_hint",
    "_sticker_handle_text_input",
    "_today_str",
    "_tts_handle_text_input",
    "_video_dl_handle_text_input",
    "add_recent_download",
    "broadcast",
    "build_book_caption",
    "build_book_keyboard",
    "build_request_admin_keyboard",
    "build_upload_admin_keyboard",
    "cache_request",
    "can_delete_books",
    "count_pending_audiobook_requests",
    "db_add_user_coin_adjustment",
    "db_get_book_by_id",
    "db_get_book_stats",
    "db_get_user_reaction",
    "db_list_requests",
    "db_increment_book_download",
    "db_increment_book_searches",
    "db_increment_counter",
    "db_insert_book",
    "ensure_user_language",
    "es_available",
    "format_request_admin_text",
    "format_upload_request_admin_text",
    "format_user_name",
    "format_user_tag",
    "find_book_by_id",
    "get_display_name",
    "get_audio_book_by_id",
    "get_es",
    "get_result_title",
    "get_user",
    "increment_analytics",
    "increment_user_analytics",
    "index_book",
    "is_blocked",
    "is_favorited",
    "is_audio_allowed",
    "is_stopped_user",
    "load_requests",
    "mark_request_fulfilled",
    "load_books",
    "normalize",
    "rate_limited",
    "run_blocking",
    "run_blocking_db_retry",
    "safe_answer",
    "safe_reply",
    "_schedule_application_task",
    "search_es",
    "send_request_to_admin",
    "set_user_allowed",
    "set_user_audio_allowed",
    "spam_check_callback",
    "spam_check_message",
    "suggest_books",
    "update_book_file_id",
    "update_request_status",
    "update_upload_request_status",
    "update_user_info",
    "user_search_command",
)

_USER_INTERACTIONS_DEP_KEYS = (
    "COIN_REFERRAL",
    "InlineKeyboardButton",
    "InlineKeyboardMarkup",
    "MESSAGES",
    "REQUESTS_PAGE_SIZE",
    "_build_help_text",
    "_is_admin_user",
    "build_results_keyboard",
    "build_results_text",
    "cache_search_results",
    "build_referral_link",
    "build_request_admin_keyboard",
    "build_requests_keyboard",
    "build_simple_book_keyboard",
    "compute_coin_breakdown",
    "db_delete_request",
    "db_increment_book_searches",
    "db_get_request_by_id",
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
    "ensure_user_language",
    "format_request_admin_text",
    "get_request_by_id",
    "get_result_title",
    "get_upload_request_by_id",
    "is_blocked",
    "is_stopped_user",
    "math",
    "quote_plus",
    "refresh_requests_list",
    "run_blocking",
    "safe_answer",
    "send_request_to_admin",
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
    "_send_with_retry",
    "add_favorite",
    "asyncio",
    "build_book_caption",
    "build_book_keyboard",
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
    "count_pending_audiobook_requests",
    "db_award_favorite_action",
    "db_award_reaction_action",
    "db_get_book_by_id",
    "db_get_book_stats",
    "db_get_book_summary",
    "db_get_top_books",
    "db_get_top_users",
    "db_get_user_reaction",
    "db_increment_counter",
    "db_set_book_reaction",
    "db_upsert_book_summary",
    "delete_audio_books_by_book_id",
    "delete_book_and_related",
    "es_available",
    "format_user_name",
    "get_audio_book_for_book",
    "get_cached_top_entries",
    "get_display_name",
    "get_es",
    "get_result_title",
    "get_top_cache",
    "get_user",
    "hashlib",
    "is_blocked",
    "is_favorited",
    "is_audio_allowed",
    "is_stopped_user",
    "remove_favorite",
    "set_cached_top_entries",
    "set_user_allowed",
    "set_user_audio_allowed",
    "set_user_blocked",
    "set_user_delete_allowed",
    "set_user_stopped",
    "socket",
    "spam_check_message",
    "update_user_info",
    "urllib",
)

_PDF_MAKER_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "A4",
    "BadRequest",
    "LETTER",
    "_send_with_retry",
    "canvas",
    "datetime",
    "ensure_user_language",
    "is_blocked",
    "is_stopped_user",
    "rl_landscape",
    "run_blocking",
    "safe_answer",
    "spam_check_message",
    "update_user_info",
)

_PDF_MAKER_OPTIONAL_DEP_KEYS = (
    "_ensure_dupes_pdf_font",
)

_PDF_EDITOR_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "_main_menu_keyboard",
    "_send_with_retry",
    "ensure_user_language",
    "is_blocked",
    "is_stopped_user",
    "run_blocking",
    "safe_answer",
    "spam_check_callback",
    "spam_check_message",
    "update_user_info",
)

_PDF_EDITOR_OPTIONAL_DEP_KEYS = ()

_TTS_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "BadRequest",
    "_main_menu_keyboard",
    "_send_with_retry",
    "edge_tts",
    "ensure_user_language",
    "is_blocked",
    "is_stopped_user",
    "run_blocking",
    "safe_answer",
    "spam_check_message",
    "update_user_info",
)

_VIDEO_DOWNLOADER_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "_main_menu_keyboard",
    "_send_with_retry",
    "db_get_counters",
    "db_increment_counter",
    "ensure_user_language",
    "run_blocking",
    "safe_answer",
    "spam_check_callback",
)

_AUDIO_CONVERTER_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "_main_menu_keyboard",
    "_send_with_retry",
    "ensure_user_language",
    "run_blocking",
    "safe_answer",
    "spam_check_callback",
)

_STICKER_TOOLS_REQUIRED_DEP_KEYS = (
    "MESSAGES",
    "_main_menu_keyboard",
    "_send_with_retry",
    "ensure_user_language",
    "is_blocked",
    "is_stopped_user",
    "run_blocking",
    "safe_answer",
    "spam_check_callback",
    "spam_check_message",
    "update_user_info",
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
    "_tts_tools_available",
    "_update_dupes_status",
    "asyncio",
    "audit_command",
    "build_user_results_keyboard",
    "build_user_results_text",
    "cache_user_results",
    "canvas",
    "datetime",
    "db_insert_admin_task_run",
    "db_list_admin_task_runs",
    "db_list_books",
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


def _build_pdf_maker_deps() -> dict[str, object]:
    return _build_bridge_deps(
        _PDF_MAKER_REQUIRED_DEP_KEYS,
        _PDF_MAKER_OPTIONAL_DEP_KEYS,
        "pdf_maker",
    )


def _build_pdf_editor_deps() -> dict[str, object]:
    return _build_bridge_deps(
        _PDF_EDITOR_REQUIRED_DEP_KEYS,
        _PDF_EDITOR_OPTIONAL_DEP_KEYS,
        "pdf_editor",
    )


def _build_tts_tools_deps() -> dict[str, object]:
    return _build_bridge_deps(_TTS_REQUIRED_DEP_KEYS, (), "tts_tools")


def _build_video_downloader_deps() -> dict[str, object]:
    return _build_bridge_deps(
        _VIDEO_DOWNLOADER_REQUIRED_DEP_KEYS,
        (),
        "video_downloader",
    )


def _build_audio_converter_deps() -> dict[str, object]:
    return _build_bridge_deps(
        _AUDIO_CONVERTER_REQUIRED_DEP_KEYS,
        (),
        "audio_converter",
    )


def _build_sticker_tools_deps() -> dict[str, object]:
    return _build_bridge_deps(_STICKER_TOOLS_REQUIRED_DEP_KEYS, (), "sticker_tools")


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
import pdf_maker as _pdf_maker_mod
import pdf_editor as _pdf_editor
import engagement_handlers as _engagement_handlers
import admin_runtime as _admin_runtime
import user_interactions as _user_interactions
import upload_flow as _upload_flow
import command_sync as _command_sync
import handler_registry as _handler_registry


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


async def upload_command_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Upload permission is controlled by DB "allowed" and owner check inside upload flow.
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


def _safe_asyncio_current_task():
    try:
        return asyncio.current_task()
    except RuntimeError:
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
    show_personal_state: bool = True,
) -> InlineKeyboardMarkup:
    like = counts.get("like", 0)
    dislike = counts.get("dislike", 0)
    berry = counts.get("berry", 0)
    whale = counts.get("whale", 0)
    fav_label = "❌ Remove" if show_personal_state and is_fav else "⭐ Favorite"
    m = MESSAGES.get(lang, MESSAGES["en"])
    ## summary_label = m.get("summary_button", "🧠 Summarize")

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
    is_owner_user = bool(_is_owner_user(user_id)) if user_id and callable(globals().get("_is_owner_user")) else False
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
        try:
            await bot.send_document(
                chat_id=chat_id,
                document=book["file_id"],
                caption=caption,
                reply_markup=reactions_kb,
            )
            return
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
_ES_CLIENT = None
BOOK_LOVERS_GROUP_URL = (os.getenv("BOOK_LOVERS_GROUP_URL", "https://t.me/book_lovers_clubb") or "").strip()
_BOOK_LOVERS_GROUP_HANDLE_RAW = (os.getenv("BOOK_LOVERS_GROUP_HANDLE", "") or "").strip()
BOT_OWNER_USERNAME = (os.getenv("BOT_OWNER_USERNAME", "@MuhammadaliAbdullayev") or "@MuhammadaliAbdullayev").strip()
BOT_DISPLAY_NAME = (os.getenv("BOT_DISPLAY_NAME", "Pdf va audio kitoblar") or "Pdf va audio kitoblar").strip()
BOT_PUBLIC_USERNAME = (os.getenv("BOT_PUBLIC_USERNAME", "@pdf_audio_kitoblar_bot") or "@pdf_audio_kitoblar_bot").strip()

ANALYTICS_FILE = "analytics.json"
REQUESTS_FILE = "requests.json"
UPLOAD_REQUESTS_FILE = "upload_requests.json"
PAGE_SIZE = 10
SEARCH_COOLDOWN_SEC = 0
DOWNLOAD_COOLDOWN_SEC = 0
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
BOOK_SEARCH_RESULT_CACHE_TTL = max(10, int(os.getenv("BOOK_SEARCH_RESULT_CACHE_TTL", "120") or "120"))
TOP_USERS_CACHE_TTL = max(5, int(os.getenv("TOP_USERS_CACHE_TTL", "45") or "45"))
SEARCH_CACHE_NS = (os.getenv("SEARCH_CACHE_NS", "v1") or "v1").strip()
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
    # Disabled intentionally: book uploads and rapid actions should not be throttled by the bot itself.
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
            await run_blocking(sync_unindexed_books)
        except Exception as e:
            logger.error(f"Failed to sync unindexed books to Elasticsearch: {e}", exc_info=True)

    async def _bg_ensure_upload_local_backup_worker(context):
        try:
            starter = getattr(_upload_flow, "start_upload_local_backup_worker", None)
            if callable(starter):
                starter(application)
        except Exception as e:
            logger.error(f"Failed to ensure local backup worker: {e}")

    asyncio.create_task(_bg_set_commands())
    try:
        application.job_queue.run_repeating(prune_blocked_users, interval=3 * 60 * 60, first=60)
        logger.debug("Scheduled prune_blocked_users every 3 hours.")
    except Exception as e:
        logger.error(f"Failed to schedule prune_blocked_users: {e}")
    asyncio.create_task(_bg_backfill_awards())
    asyncio.create_task(_bg_sync_unindexed_books())
    try:
        starter = getattr(_upload_flow, "start_upload_local_backup_worker", None)
        if callable(starter):
            starter(application)
        application.job_queue.run_repeating(_bg_ensure_upload_local_backup_worker, interval=60, first=60)
    except Exception as e:
        logger.error(f"Failed to start local backup worker: {e}")


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
                audio_allowed=bool(u.get("audio_allowed", False)),
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
) -> bool:
    if not update.message:
        return False
    text = MESSAGES.get(lang, MESSAGES["en"]).get(
        "group_private_start_required",
        MESSAGES["en"]["group_private_start_required"],
    )
    start_url = await _build_private_start_url(context)
    reply_markup = None
    if start_url:
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton(MESSAGES.get(lang, MESSAGES["en"]).get("group_private_start_button", "🚀 Start Bot"), url=start_url)]]
        )
    await _send_with_retry(lambda: update.message.reply_text(text, reply_markup=reply_markup))
    return True


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
                await _reply_group_private_start_prompt(update, context, prompt_lang)
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
        referrer_id = parse_referral_payload(context.args[0] if context.args else None)
        prompt_lang = _language_picker_prompt_lang(user_record, update.effective_user)
        # Always ask user to choose language on /start (do not auto-use Telegram locale).
        await safe_reply(
            update,
            MESSAGES[prompt_lang]["choose_language"],
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
    user_record = get_user_record(update.effective_user.id) or {}
    if _is_group_chat(update.effective_chat):
        prompt_lang = detect_language_code(
            user_record.get("group_language")
            or user_record.get("language")
            or getattr(update.effective_user, "language_code", None)
        )
        if not _has_private_bot_start(user_record):
            await _reply_group_private_start_prompt(update, context, prompt_lang)
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

    pdf_editor_session = _pdfed_get_session(context) if callable(globals().get("_pdfed_get_session")) else None
    if pdf_editor_session and (not user_id or pdf_editor_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(pdf_editor_session)
        _pdfed_clear_session(context)
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

    audio_conv_session = _audio_conv_get_session(context) if callable(globals().get("_audio_conv_get_session")) else None
    if audio_conv_session and (not user_id or audio_conv_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(audio_conv_session)
        _audio_conv_clear_session(context)
        cancelled = True

    sticker_session = _sticker_get_session(context) if callable(globals().get("_sticker_get_session")) else None
    if sticker_session and (not user_id or sticker_session.get("user_id") in {None, user_id}):
        await _edit_prompt_if_any(sticker_session)
        _sticker_clear_session(context)
        cancelled = True

    if context.user_data.get("awaiting_request"):
        context.user_data["awaiting_request"] = False
        context.user_data.pop("awaiting_request_until", None)
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
        await update.message.reply_text(
            m.get("menu_search_prompt", "Send a book name to search."),
            reply_markup=_main_menu_keyboard(lang, "main", user_id),
        )
        context.user_data["main_menu_section"] = "main"
        return True
    context.user_data["awaiting_book_search"] = False
    context.user_data.pop("search_mode", None)
    if action == "tts":
        context.user_data["main_menu_section"] = "main"
        await _tts_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "pdf":
        context.user_data["main_menu_section"] = "other"
        await _pdf_maker_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "pdf_editor":
        context.user_data["main_menu_section"] = "other"
        await _pdfed_start_session_from_message(update.message, update, context, lang)
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
    if action == "video_downloader":
        context.user_data["main_menu_section"] = "video_downloader"
        await _video_dl_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "audio_converter":
        context.user_data["main_menu_section"] = "other"
        await _audio_conv_start_session_from_message(update.message, update, context, lang)
        return True
    if action == "sticker_tools":
        context.user_data["main_menu_section"] = "other"
        await _sticker_start_session_from_message(update.message, update, context, lang)
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
    )
    if handled_admin:
        return True
    return False


async def _send_animated_start_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    if not update.message:
        return False
    final_text = _build_start_greeting_text(lang, update.effective_user)
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
    "bulk_index_books",
    "db_update_book_path",
    "db_get_book_by_id",
    "update_book_file_id",
    "BOOK_STORAGE_CHANNEL_ID",
    "VIDEO_UPLOAD_CHANNEL_IDS",
    "VIDEO_UPLOAD_CHANNEL_ID",
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
    """Route photo to audio converter first (cover upload mode), then fallback to photo search hint."""
    audio_conv_handler = globals().get("_audio_conv_handle_media_input")
    if callable(audio_conv_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await audio_conv_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("audio converter photo handler failed: %s", e, exc_info=True)
    sticker_handler = globals().get("_sticker_handle_media_input")
    if callable(sticker_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await sticker_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("sticker tools photo handler failed: %s", e, exc_info=True)
    await _raw_handle_photo_message(update, context)


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route document/image to audio converter first (cover upload mode), then fallback to upload flow."""
    audio_conv_handler = globals().get("_audio_conv_handle_media_input")
    if callable(audio_conv_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await audio_conv_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("audio converter file handler failed: %s", e, exc_info=True)
    sticker_handler = globals().get("_sticker_handle_media_input")
    if callable(sticker_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await sticker_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("sticker tools file handler failed: %s", e, exc_info=True)
    pdf_editor_handler = globals().get("_pdfed_handle_media_input")
    if callable(pdf_editor_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await pdf_editor_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("pdf editor file handler failed: %s", e, exc_info=True)
    await _raw_handle_file(update, context)


async def handle_video_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route video/GIF to sticker tools only."""
    sticker_handler = globals().get("_sticker_handle_media_input")
    if callable(sticker_handler):
        try:
            lang = ensure_user_language(update, context)
            handled = await sticker_handler(update, context, lang)
            if handled:
                return
        except ApplicationHandlerStop:
            raise
        except Exception as e:
            logger.warning("sticker tools video handler failed: %s", e, exc_info=True)
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
            "video_downloads",
            "ai_pdf_created",
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
            f"- Video downloader downloads: {counters.get('video_downloads', 0)}",
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
            "📄 AI PDF Tools",
            f"- PDF created: {counters.get('ai_pdf_created', 0)}",
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
handle_audiobook_page_callback = _search_flow.handle_audiobook_page_callback
handle_audiobook_part_play_callback = _search_flow.handle_audiobook_part_play_callback
handle_audiobook_add_callback = _search_flow.handle_audiobook_add_callback
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
    results = []

    if not query:
        await update.inline_query.answer([], cache_time=0)
        return

    token = query.split()[0].strip()
    if token.startswith("mshare_"):
        await update.inline_query.answer([], cache_time=0, is_personal=True)
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


# PDF Maker extracted module bridge
_pdf_maker_mod.configure(_build_pdf_maker_deps())

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

# PDF Editor extracted module bridge
_pdf_editor.configure(_build_pdf_editor_deps())

_pdfed_clear_session = _pdf_editor._pdf_editor_clear_session
_pdfed_get_session = _pdf_editor._pdf_editor_get_session
_pdfed_start_session_from_message = _pdf_editor._pdf_editor_start_session_from_message
_pdfed_handle_text_input = _pdf_editor._pdf_editor_handle_text_input
_pdfed_handle_media_input = _pdf_editor._pdf_editor_handle_media_input
pdf_editor_command = _pdf_editor.pdf_editor_command
handle_pdf_editor_callback = _pdf_editor.handle_pdf_editor_callback


# TTS extracted module bridge
_tts_tools.configure(_build_tts_tools_deps())

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

# Video downloader extracted module bridge
_video_downloader.configure(_build_video_downloader_deps())

_video_dl_clear_session = _video_downloader._video_dl_clear_session
_video_dl_get_session = _video_downloader._video_dl_get_session
_video_dl_start_session_from_message = _video_downloader._video_dl_start_session_from_message
_video_dl_handle_text_input = _video_downloader._video_dl_handle_text_input
handle_video_downloader_callback = _video_downloader.handle_video_downloader_callback

# Audio converter extracted module bridge
_audio_converter.configure(_build_audio_converter_deps())

_audio_conv_clear_session = _audio_converter._audio_conv_clear_session
_audio_conv_get_session = _audio_converter._audio_conv_get_session
_audio_conv_start_session_from_message = _audio_converter._audio_conv_start_session_from_message
_audio_conv_handle_text_input = _audio_converter._audio_conv_handle_text_input
_audio_conv_handle_media_input = _audio_converter._audio_conv_handle_media_input
handle_audio_converter_callback = _audio_converter.handle_audio_converter_callback

# Sticker tools extracted module bridge
_sticker_tools.configure(_build_sticker_tools_deps())

_sticker_clear_session = _sticker_tools._sticker_clear_session
_sticker_get_session = _sticker_tools._sticker_get_session
_sticker_start_session_from_message = _sticker_tools._sticker_start_session_from_message
_sticker_handle_text_input = _sticker_tools._sticker_handle_text_input
_sticker_handle_media_input = _sticker_tools._sticker_handle_media_input
handle_sticker_tools_callback = _sticker_tools.handle_sticker_tools_callback
sticker_tools_command = _sticker_tools.sticker_tools_command

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
        video_channels_for_log = list(VIDEO_UPLOAD_CHANNEL_IDS or [])
        if not video_channels_for_log and VIDEO_UPLOAD_CHANNEL_ID:
            video_channels_for_log = [VIDEO_UPLOAD_CHANNEL_ID]
        audio_channels_for_log = list(AUDIO_UPLOAD_CHANNEL_IDS or [])
        if not audio_channels_for_log and AUDIO_UPLOAD_CHANNEL_ID:
            audio_channels_for_log = [AUDIO_UPLOAD_CHANNEL_ID]
        book_storage_channel = BOOK_STORAGE_CHANNEL_ID or 0
        logger.info(
            "Upload channels: audio=%s video=%s book_storage=%s",
            ",".join(str(x) for x in audio_channels_for_log) or "none",
            ",".join(str(x) for x in video_channels_for_log) or "none",
            str(book_storage_channel) if book_storage_channel else "none",
        )

        logger.info(
            "Ollama config: url=%s pdf_maker_model=%s tts_model=%s",
            os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/"),
            os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b"),
            os.getenv("TTS_OLLAMA_MODEL", os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b")),
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
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
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
