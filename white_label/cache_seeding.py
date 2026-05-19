from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from telegram import InputFile

from . import WL_SEED_STATUS_FAILED, WL_SEED_STATUS_SENT_TO_CACHE
from .crypto import redact_token_like_strings
from .db_helpers import create_white_label_audit_log, update_connected_bot_cache_seed_job
from .runtime_utils import build_bot_client

logger = logging.getLogger(__name__)


def build_cache_seed_caption(seed_token: str, book_id: str, connected_bot_id: str) -> str:
    return "\n".join(
        [
            f"WL_CACHE_SEED:{str(seed_token or '').strip()}",
            f"BOOK_ID:{str(book_id or '').strip()}",
            f"CONNECTED_BOT_ID:{str(connected_bot_id or '').strip()}",
        ]
    )


def parse_cache_seed_caption(text: str | None) -> dict[str, str] | None:
    lines = [str(line or "").strip() for line in str(text or "").splitlines() if str(line or "").strip()]
    data: dict[str, str] = {}
    for line in lines:
        if line.startswith("WL_CACHE_SEED:"):
            data["seed_token"] = line.split(":", 1)[1].strip()
        elif line.startswith("BOOK_ID:"):
            data["book_id"] = line.split(":", 1)[1].strip()
        elif line.startswith("CONNECTED_BOT_ID:"):
            data["connected_bot_id"] = line.split(":", 1)[1].strip()
    if data.get("seed_token") and data.get("book_id") and data.get("connected_bot_id"):
        return data
    return None


def _audit_cache_seed_failed(connected_bot: dict, seed_job: dict, book: dict, error: str) -> None:
    try:
        create_white_label_audit_log(
            action="CACHE_SEED_FAILED",
            actor_user_id=None,
            connected_bot_id=str((connected_bot or {}).get("id") or "").strip() or None,
            request_id=None,
            target_bot_username=str((connected_bot or {}).get("bot_username") or "").strip() or None,
            details={
                "seed_job_id": str((seed_job or {}).get("id") or ""),
                "book_id": str((book or {}).get("id") or ""),
                "cache_channel_id": int((connected_bot or {}).get("cache_channel_id") or 0),
            },
            error_message=error,
        )
    except Exception:
        logger.debug("Failed to write cache seed failure audit log", exc_info=True)


def _book_filename(book: dict[str, Any]) -> str:
    title = str(book.get("display_name") or book.get("book_name") or book.get("id") or "book").strip() or "book"
    safe_title = "".join(ch for ch in title if ch.isalnum() or ch in {" ", "-", "_", ".", "(", ")"})
    safe_title = safe_title.strip() or "book"
    return safe_title if safe_title.lower().endswith(".pdf") else f"{safe_title}.pdf"


async def _send_seed_via_source_message(main_bot, cache_channel_id: int, book: dict, caption: str) -> tuple[bool, int | None, str | None]:
    source_chat_id = int(book.get("storage_channel_id") or book.get("channel_id") or 0)
    source_message_id = int(book.get("storage_message_id") or book.get("channel_message_id") or 0)
    if not source_chat_id or not source_message_id:
        return False, None, "source message unavailable"
    try:
        copied = await main_bot.copy_message(
            chat_id=cache_channel_id,
            from_chat_id=source_chat_id,
            message_id=source_message_id,
            caption=caption,
            disable_notification=True,
        )
        return True, int(getattr(copied, "message_id", 0) or 0) or None, None
    except Exception as exc:
        return False, None, redact_token_like_strings(str(exc))


async def _send_seed_via_main_file_id(main_bot, cache_channel_id: int, file_id: str, caption: str) -> tuple[bool, int | None, str | None]:
    try:
        sent = await main_bot.send_document(
            chat_id=cache_channel_id,
            document=str(file_id),
            caption=caption,
            disable_notification=True,
        )
        return True, int(getattr(sent, "message_id", 0) or 0) or None, None
    except Exception as exc:
        return False, None, redact_token_like_strings(str(exc))


async def _send_seed_via_local_path(main_bot, cache_channel_id: int, local_path: str, caption: str, book: dict) -> tuple[bool, int | None, str | None]:
    path_obj = Path(str(local_path or "").strip())
    if not path_obj.exists():
        return False, None, "local file missing"
    try:
        with open(path_obj, "rb") as fh:
            sent = await main_bot.send_document(
                chat_id=cache_channel_id,
                document=InputFile(fh, filename=_book_filename(book)),
                caption=caption,
                disable_notification=True,
            )
        return True, int(getattr(sent, "message_id", 0) or 0) or None, None
    except Exception as exc:
        return False, None, redact_token_like_strings(str(exc))


async def seed_connected_bot_cache(
    connected_bot: dict,
    book: dict,
    seed_job: dict,
    *,
    main_bot=None,
) -> dict[str, Any]:
    cache_channel_id = int((connected_bot or {}).get("cache_channel_id") or 0)
    if not cache_channel_id:
        error = "cache channel is not configured"
        update_connected_bot_cache_seed_job(str(seed_job.get("id") or ""), status=WL_SEED_STATUS_FAILED, error_message=error)
        _audit_cache_seed_failed(connected_bot, seed_job, book, error)
        return {"ok": False, "error": error}

    created_client = False
    if main_bot is None:
        from config import TOKEN

        main_bot = build_bot_client(TOKEN)
        created_client = True

    caption = build_cache_seed_caption(
        str(seed_job.get("seed_token") or ""),
        str(book.get("id") or ""),
        str(connected_bot.get("id") or ""),
    )
    try:
        ok, message_id, error = await _send_seed_via_source_message(main_bot, cache_channel_id, book, caption)
        source = "copy_message"
        if not ok:
            file_id = str(book.get("file_id") or "").strip()
            if file_id:
                ok, message_id, error = await _send_seed_via_main_file_id(main_bot, cache_channel_id, file_id, caption)
                source = "main_file_id"
        if not ok:
            local_path = str(book.get("path") or "").strip()
            if local_path:
                ok, message_id, error = await _send_seed_via_local_path(main_bot, cache_channel_id, local_path, caption, book)
                source = "local_path"
        if not ok:
            update_connected_bot_cache_seed_job(
                str(seed_job.get("id") or ""),
                status=WL_SEED_STATUS_FAILED,
                error_message=error or "no cache seed source available",
            )
            _audit_cache_seed_failed(connected_bot, seed_job, book, error or "no cache seed source available")
            return {"ok": False, "error": error or "no cache seed source available"}

        update_connected_bot_cache_seed_job(
            str(seed_job.get("id") or ""),
            status=WL_SEED_STATUS_SENT_TO_CACHE,
            main_bot_cache_message_id=message_id,
            error_message=None,
        )
        return {"ok": True, "main_bot_cache_message_id": message_id, "source": source}
    except Exception as exc:
        error = redact_token_like_strings(str(exc))
        logger.error("White-label cache seeding failed: %s", error, exc_info=True)
        update_connected_bot_cache_seed_job(
            str(seed_job.get("id") or ""),
            status=WL_SEED_STATUS_FAILED,
            error_message=error,
        )
        _audit_cache_seed_failed(connected_bot, seed_job, book, error)
        return {"ok": False, "error": error}
    finally:
        if created_client:
            try:
                await main_bot.shutdown()
            except Exception:
                pass
