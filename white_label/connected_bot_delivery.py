from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from typing import Any

from db import get_book_by_id as db_get_book_by_id, increment_book_download as db_increment_book_download
from . import WL_CACHE_STATUS_VALID, WL_SEED_STATUS_PENDING, WL_SEED_STATUS_SENT_TO_CACHE
from .cache_seeding import seed_connected_bot_cache
from .db_helpers import (
    create_connected_bot_cache_seed_job,
    expire_stale_connected_bot_cache_seed_jobs,
    get_active_connected_bot_cache_seed_job,
    get_connected_bot_file_cache,
    increment_connected_bot_usage,
    mark_connected_bot_file_cache_invalid,
    touch_connected_bot_file_cache_usage,
)

logger = logging.getLogger(__name__)
_BOOK_SEND_LOCKS: dict[tuple[str, str], asyncio.Lock] = {}


def _book_caption(book: dict[str, Any]) -> str:
    title = str(book.get("display_name") or book.get("book_name") or book.get("id") or "Kitob").strip()
    return f"📚 {title}"


def _is_invalid_file_id_error(exc: Exception | str) -> bool:
    text = str(exc or "").lower()
    markers = (
        "wrong file identifier",
        "file reference has expired",
        "wrong remote file identifier",
        "there is no file",
        "specified file not found",
    )
    return any(marker in text for marker in markers)


def _is_pdf_accessible_book(book: dict[str, Any]) -> bool:
    if book.get("white_label_enabled") is False:
        return False
    path = str(book.get("path") or "").strip().lower()
    file_id = str(book.get("file_id") or "").strip()
    if not path and not file_id:
        return False
    if path and not path.endswith(".pdf"):
        return False
    return True


def _lock_for(connected_bot_id: str, book_id: str) -> asyncio.Lock:
    key = (str(connected_bot_id or "").strip(), str(book_id or "").strip())
    lock = _BOOK_SEND_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _BOOK_SEND_LOCKS[key] = lock
    return lock


async def _send_cached_document(bot, chat_id: int, book: dict, file_id: str):
    return await bot.send_document(
        chat_id=chat_id,
        document=str(file_id),
        caption=_book_caption(book),
    )


async def send_book_via_connected_bot(
    *,
    connected_bot: dict,
    chat_id: int,
    user_id: int,
    book_id: str,
    requesting_message_id: int | None,
    bot,
    wait_seconds: int,
    preparing_text: str | None = None,
) -> dict[str, Any]:
    expire_stale_connected_bot_cache_seed_jobs()
    current_book = dict(db_get_book_by_id(str(book_id or "").strip()) or {})
    if not current_book:
        return {"ok": False, "error_code": "book_not_found"}
    if not _is_pdf_accessible_book(current_book):
        return {"ok": False, "error_code": "book_unavailable"}

    connected_bot_id = str((connected_bot or {}).get("id") or "").strip()
    async with _lock_for(connected_bot_id, str(book_id or "").strip()):
        cache_row = get_connected_bot_file_cache(connected_bot_id, str(book_id or "").strip(), only_valid=True)
        if cache_row and str(cache_row.get("telegram_file_id") or "").strip():
            try:
                sent = await _send_cached_document(bot, int(chat_id), current_book, str(cache_row.get("telegram_file_id") or "").strip())
                touch_connected_bot_file_cache_usage(connected_bot_id, str(book_id or "").strip())
                increment_connected_bot_usage(connected_bot_id, sends=1, cache_hits=1)
                db_increment_book_download(str(book_id or "").strip())
                return {"ok": True, "sent_message_id": getattr(sent, "message_id", None), "cache": "hit"}
            except Exception as exc:
                if _is_invalid_file_id_error(exc):
                    mark_connected_bot_file_cache_invalid(connected_bot_id, str(book_id or "").strip(), str(exc))
                else:
                    increment_connected_bot_usage(connected_bot_id, errors=1)
                    return {"ok": False, "error_code": "send_failed", "error": str(exc)}

        increment_connected_bot_usage(connected_bot_id, cache_misses=1)
        await bot.send_message(
            chat_id=int(chat_id),
            text=str(preparing_text or "Kitob tayyorlanmoqda, bir necha soniya kuting..."),
        )

        seed_job = get_active_connected_bot_cache_seed_job(connected_bot_id, str(book_id or "").strip())
        if not seed_job:
            seed_job = create_connected_bot_cache_seed_job(
                connected_bot_id=connected_bot_id,
                book_id=str(book_id or "").strip(),
                requesting_chat_id=int(chat_id),
                requesting_user_id=int(user_id),
                requesting_message_id=int(requesting_message_id) if requesting_message_id else None,
                cache_channel_id=int((connected_bot or {}).get("cache_channel_id") or 0),
                seed_token=uuid.uuid4().hex,
            )

        should_seed_now = not seed_job or (
            str(seed_job.get("status") or "").strip().upper() == WL_SEED_STATUS_PENDING
            and not int(seed_job.get("main_bot_cache_message_id") or 0)
        )
        if should_seed_now:
            seed_result = await seed_connected_bot_cache(connected_bot, current_book, seed_job)
            if not seed_result.get("ok"):
                increment_connected_bot_usage(connected_bot_id, errors=1)
                return {"ok": False, "error_code": "cache_seed_failed", "error": seed_result.get("error")}

        deadline = time.monotonic() + max(5, int(wait_seconds or 30))
        last_error = None
        retried_invalid_cache = False
        while time.monotonic() < deadline:
            await asyncio.sleep(1.0)
            cache_row = get_connected_bot_file_cache(connected_bot_id, str(book_id or "").strip(), only_valid=True)
            if not cache_row or not str(cache_row.get("telegram_file_id") or "").strip():
                continue
            try:
                sent = await _send_cached_document(bot, int(chat_id), current_book, str(cache_row.get("telegram_file_id") or "").strip())
                touch_connected_bot_file_cache_usage(connected_bot_id, str(book_id or "").strip())
                increment_connected_bot_usage(connected_bot_id, sends=1)
                db_increment_book_download(str(book_id or "").strip())
                return {"ok": True, "sent_message_id": getattr(sent, "message_id", None), "cache": "seeded"}
            except Exception as exc:
                last_error = str(exc)
                if _is_invalid_file_id_error(exc) and not retried_invalid_cache:
                    retried_invalid_cache = True
                    mark_connected_bot_file_cache_invalid(connected_bot_id, str(book_id or "").strip(), str(exc))
                    seed_job = create_connected_bot_cache_seed_job(
                        connected_bot_id=connected_bot_id,
                        book_id=str(book_id or "").strip(),
                        requesting_chat_id=int(chat_id),
                        requesting_user_id=int(user_id),
                        requesting_message_id=int(requesting_message_id) if requesting_message_id else None,
                        cache_channel_id=int((connected_bot or {}).get("cache_channel_id") or 0),
                        seed_token=uuid.uuid4().hex,
                    )
                    seed_result = await seed_connected_bot_cache(connected_bot, current_book, seed_job)
                    if not seed_result.get("ok"):
                        increment_connected_bot_usage(connected_bot_id, errors=1)
                        return {"ok": False, "error_code": "cache_seed_failed", "error": seed_result.get("error")}
                    continue
                increment_connected_bot_usage(connected_bot_id, errors=1)
                return {"ok": False, "error_code": "send_failed", "error": str(exc)}

        if last_error:
            logger.warning("White-label cache wait expired for bot=%s book=%s after send error: %s", connected_bot_id, book_id, last_error)
        return {"ok": False, "error_code": "cache_wait_timeout"}
