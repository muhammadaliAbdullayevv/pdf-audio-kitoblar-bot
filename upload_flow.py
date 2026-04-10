from __future__ import annotations

import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Any

from book_thumbnail import get_book_thumbnail_input
from language import MESSAGES
from telegram.error import NetworkError, RetryAfter, TimedOut

try:
    from pdf_editor import _pdf_editor_watermark_blocking as _shared_pdf_watermark_blocking
except Exception:
    _shared_pdf_watermark_blocking = None

import uuid

logger = logging.getLogger(__name__)

_CONFIG_REQUIRED_KEYS = (
    "MESSAGES",
    "run_blocking",
    "ensure_user_language",
    "is_blocked",
    "is_stopped_user",
    "spam_check_message",
    "update_user_info",
    "is_allowed",
    "clean_query",
    "_send_with_retry",
    "ApplicationHandlerStop",
    "safe_reply",
    "db_find_duplicate_book",
    "db_update_upload_receipt",
    "db_insert_book",
    "db_update_book_upload_meta",
    "db_enqueue_book_local_download_job",
    "db_claim_book_local_download_job",
    "db_complete_book_local_download_job",
    "db_retry_book_local_download_job",
    "db_fail_book_local_download_job",
    "index_book",
    "notify_request_matches",
    "db_insert_upload_receipt",
    "_reply_search_image_hint",
    "es_available",
    "ensure_index",
    "load_books",
    "get_display_name",
    "update_book_indexed",
    "InlineKeyboardMarkup",
    "InlineKeyboardButton",
)

def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
    except Exception:
        return float(default)


_UPLOAD_MODE_KEY = "upload_mode_state"
_UPLOAD_MODE_BOOK = "book"
_UPLOAD_LOCAL_DIR = Path(os.getenv("UPLOAD_LOCAL_DIR", str(Path(__file__).resolve().parent / "downloads" / "localbooks")))
_UPLOAD_AUTO_DOWNLOAD_LOCAL = _env_bool("UPLOAD_AUTO_DOWNLOAD_LOCAL", True)
_UPLOAD_LOCAL_WORKER_COUNT = max(1, _env_int("UPLOAD_LOCAL_WORKER_COUNT", 3))
_UPLOAD_LOCAL_DOWNLOAD_RETRIES = max(1, _env_int("UPLOAD_LOCAL_DOWNLOAD_RETRIES", 6))
_UPLOAD_LOCAL_RETRY_BASE_DELAY_SEC = max(0.5, _env_float("UPLOAD_LOCAL_RETRY_BASE_DELAY_SEC", 2.0))
_UPLOAD_LOCAL_RETRY_MIN_DELAY_SEC = max(1.0, _env_float("UPLOAD_LOCAL_RETRY_MIN_DELAY_SEC", 10.0))
_UPLOAD_LOCAL_WORKER_POLL_SECONDS = max(0.5, _env_float("UPLOAD_LOCAL_WORKER_POLL_SECONDS", 1.0))
_UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS = max(0.0, _env_float("UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS", 0.1))
_UPLOAD_LOCAL_JOB_STALE_AFTER_SECONDS = max(60, _env_int("UPLOAD_LOCAL_JOB_STALE_AFTER_SECONDS", 3600))
_UPLOAD_LOCAL_GET_FILE_READ_TIMEOUT_SEC = max(60.0, _env_float("UPLOAD_LOCAL_GET_FILE_READ_TIMEOUT_SEC", 300.0))
_UPLOAD_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC = max(120.0, _env_float("UPLOAD_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC", 600.0))
_UPLOAD_LOCAL_CONNECT_TIMEOUT_SEC = max(10.0, _env_float("UPLOAD_LOCAL_CONNECT_TIMEOUT_SEC", 45.0))
_UPLOAD_LOCAL_POOL_TIMEOUT_SEC = max(10.0, _env_float("UPLOAD_LOCAL_POOL_TIMEOUT_SEC", 45.0))
_UPLOAD_LOCAL_REFRESH_FILE_ID = _env_bool("UPLOAD_LOCAL_REFRESH_FILE_ID", True)
_UPLOAD_LOCAL_WATERMARK_PDF = _env_bool("UPLOAD_LOCAL_WATERMARK_PDF", True)
_UPLOAD_LOCAL_WATERMARK_TEXT = (
    str(os.getenv("BOOK_WATERMARK_TEXT", "") or "").strip()
    or "Pdf va audio kitoblar"
)
_UPLOAD_LOCAL_WORKER_KEY = "upload_local_backup_workers"


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v
    missing = [key for key in _CONFIG_REQUIRED_KEYS if key not in globals()]
    if missing:
        raise RuntimeError(f"upload_flow missing configured dependencies: {', '.join(missing)}")


def _set_user_upload_mode(context: ContextTypes.DEFAULT_TYPE, mode: str | None) -> None:
    try:
        if mode == _UPLOAD_MODE_BOOK:
            context.user_data[_UPLOAD_MODE_KEY] = mode
        else:
            context.user_data.pop(_UPLOAD_MODE_KEY, None)
    except Exception:
        pass


def _get_user_upload_mode(context: ContextTypes.DEFAULT_TYPE) -> str:
    try:
        mode = str(context.user_data.get(_UPLOAD_MODE_KEY) or "").strip().lower()
    except Exception:
        mode = ""
    if mode in {_UPLOAD_MODE_BOOK}:
        return mode
    return ""

_BOOK_ADULT_FILTER_PATTERNS = (
    re.compile(r"(?<!\d)18\s*\+"),
    re.compile(r"\b18\s*yosh\b"),
    re.compile(r"\b18\s*plus\b"),
    re.compile(r"\badult\b"),
    re.compile(r"\bnsfw\b"),
    re.compile(r"\bporn\w*\b"),
    re.compile(r"\berot\w*\b"),
    re.compile(r"\bsex\b"),
    re.compile(r"\bseks\b"),
    re.compile(r"\bxxx\b"),
    re.compile(r"\bhentai\b"),
    re.compile(r"\bonlyfans\b"),
    re.compile(r"\bэрот\w*\b"),
    re.compile(r"\bпорн\w*\b"),
    re.compile(r"\bсекс\w*\b"),
    re.compile(r"\bjinsiy\b"),
    re.compile(r"\bvoyaga\s+yetgan\w*\b"),
)


def _extra_book_adult_keywords() -> list[str]:
    raw = str(os.getenv("BOOK_ADULT_FILTER_KEYWORDS", "") or "").strip()
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        token = str(item or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _book_filter_haystack(*parts: Any) -> str:
    base = " ".join(str(p or "") for p in parts).lower()
    base = re.sub(r"[\s_\-./|\\:;,(){}\[\]<>]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def _book_has_adult_markers(*parts: Any) -> bool:
    haystack = _book_filter_haystack(*parts)
    if not haystack:
        return False
    for pattern in _BOOK_ADULT_FILTER_PATTERNS:
        if pattern.search(haystack):
            return True
    for token in _extra_book_adult_keywords():
        if token in haystack:
            return True
    return False


_BOOK_UPLOAD_ALLOWED_EXTENSIONS = {
    ".pdf",
    ".epub",
    ".mobi",
    ".djvu",
    ".fb2",
    ".txt",
    ".doc",
    ".docx",
    ".rtf",
    ".azw",
    ".azw3",
    ".odt",
}
_BOOK_UPLOAD_ALLOWED_FORMATS_TEXT = "PDF, EPUB, MOBI, DJVU, FB2, TXT, DOC, DOCX, RTF, AZW, AZW3, ODT"


def _book_upload_file_extension(file_name: str) -> str:
    _, ext = os.path.splitext(str(file_name or "").strip())
    return ext.lower().strip()


def _book_upload_is_allowed_file(file_name: str) -> bool:
    return _book_upload_file_extension(file_name) in _BOOK_UPLOAD_ALLOWED_EXTENSIONS


def _coerce_int_id_list(raw: Any) -> list[int]:
    values: list[Any]
    if raw is None:
        values = []
    elif isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        values = str(raw).split(",")

    out: list[int] = []
    seen: set[int] = set()
    for item in values:
        text = str(item).strip()
        if not text:
            continue
        try:
            value = int(text)
        except Exception:
            continue
        if value == 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _resolve_video_upload_channel_ids() -> list[int]:
    # Preferred: VIDEO_UPLOAD_CHANNEL_IDS (list/comma-separated), with
    # VIDEO_UPLOAD_CHANNEL_ID kept as backward-compatible fallback.
    ids = _coerce_int_id_list(globals().get("VIDEO_UPLOAD_CHANNEL_IDS"))
    if ids:
        return ids
    ids = _coerce_int_id_list(os.getenv("VIDEO_UPLOAD_CHANNEL_IDS", ""))
    if ids:
        return ids
    ids = _coerce_int_id_list([globals().get("VIDEO_UPLOAD_CHANNEL_ID")])
    if ids:
        return ids
    return _coerce_int_id_list([os.getenv("VIDEO_UPLOAD_CHANNEL_ID", "")])


async def _pick_video_upload_channel_id(context: ContextTypes.DEFAULT_TYPE, channel_ids: list[int] | None = None) -> int | None:
    ids = list(channel_ids or _resolve_video_upload_channel_ids())
    if not ids:
        return None

    app = getattr(context, "application", None)
    if app is None:
        return ids[0]

    data = app.bot_data
    lock = data.get("video_upload_channel_lock")
    if lock is None:
        lock = asyncio.Lock()
        data["video_upload_channel_lock"] = lock

    async with lock:
        idx = int(data.get("video_upload_channel_index", 0) or 0)
        channel_id = ids[idx % len(ids)]
        data["video_upload_channel_index"] = idx + 1
        return channel_id


def _safe_asyncio_current_task():
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


async def _send_status_with_retry(status_msg, text: str, reply_only: bool = False) -> None:
    if not status_msg:
        return
    try:
        send_with_retry = globals().get("_send_with_retry")
        action = (lambda: status_msg.reply_text(text)) if reply_only else (lambda: status_msg.edit_text(text))
        if callable(send_with_retry):
            await send_with_retry(action, retries=4)
        else:
            await action()
    except Exception as e:
        logger.warning("Failed to send upload status message: %s", e)


async def _bulk_index_flush(app, batch: list[dict]) -> None:
    if not batch:
        return
    docs = [b.get("doc") for b in batch if isinstance(b, dict) and isinstance(b.get("doc"), dict)]
    if not docs:
        return

    results: dict[str, tuple[bool, str | None]] = {}
    bulk_fn = globals().get("bulk_index_books")
    run_blocking_heavy_fn = globals().get("run_blocking_heavy")
    index_book_fn = globals().get("index_book")

    if callable(bulk_fn):
        try:
            if callable(run_blocking_heavy_fn):
                res = await run_blocking_heavy_fn(bulk_fn, docs, "false")
            else:
                res = await run_blocking(bulk_fn, docs, "false")
            if isinstance(res, dict):
                for book_id, payload in res.items():
                    if isinstance(payload, dict):
                        ok = bool(payload.get("ok"))
                        err = payload.get("error")
                        results[str(book_id)] = (ok, str(err) if err else None)
                    else:
                        results[str(book_id)] = (bool(payload), None if payload else "bulk indexing failed")
        except Exception as e:
            logger.error("Bulk index call failed: %s", e, exc_info=True)

    # Fallback to one-by-one indexing when bulk failed/unavailable.
    if not results and callable(index_book_fn):
        for d in docs:
            book_id = str(d.get("id") or "")
            try:
                if callable(run_blocking_heavy_fn):
                    out_id = await run_blocking_heavy_fn(
                        index_book_fn,
                        d.get("book_name"),
                        d.get("file_id"),
                        d.get("path"),
                        d.get("id"),
                        d.get("display_name"),
                        d.get("file_unique_id"),
                        "false",
                    )
                else:
                    out_id = await run_blocking(
                        index_book_fn,
                        d.get("book_name"),
                        d.get("file_id"),
                        d.get("path"),
                        d.get("id"),
                        d.get("display_name"),
                        d.get("file_unique_id"),
                        "false",
                    )
                results[book_id] = (bool(out_id), None if out_id else "index_book returned empty id")
            except Exception as e:
                results[book_id] = (False, str(e))

    for job in batch:
        if not isinstance(job, dict):
            continue
        doc = job.get("doc") or {}
        book_id = str(doc.get("id") or "")
        if not book_id:
            continue
        ok, err = results.get(book_id, (False, "indexing failed"))
        receipt_id = job.get("receipt_id")

        if ok:
            try:
                await run_blocking(update_book_indexed, book_id, True)
            except Exception:
                logger.exception("Failed to set indexed=True for book_id=%s", book_id)
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="indexed",
                        book_id=book_id,
                        saved_to_db=True,
                        saved_to_es=True,
                        error=None,
                    )
                except Exception:
                    logger.exception("Failed to mark receipt indexed: %s", receipt_id)
            continue

        if receipt_id:
            try:
                await run_blocking(
                    db_update_upload_receipt,
                    receipt_id,
                    status="index_failed",
                    book_id=book_id,
                    saved_to_db=True,
                    saved_to_es=False,
                    error=(str(err)[:1000] if err else "indexing failed"),
                )
            except Exception:
                logger.exception("Failed to mark receipt index_failed: %s", receipt_id)
        # Keep indexing outcome in DB receipt only; no extra user message.


async def upload_bulk_index_worker(app):
    data = app.bot_data
    q: asyncio.Queue | None = data.get("upload_bulk_index_queue")
    if q is None:
        return
    batch_size = max(1, _env_int("UPLOAD_ES_BULK_SIZE", 100))
    # Flush policy:
    # - when batch reaches batch_size, OR
    # - when max_wait_sec elapsed since first enqueued job in current batch.
    max_wait_sec = max(2.0, _env_float("UPLOAD_ES_BULK_IDLE_TIMEOUT_SEC", 10.0))

    try:
        while True:
            try:
                first = await asyncio.wait_for(q.get(), timeout=max_wait_sec)
            except asyncio.TimeoutError:
                if q.empty():
                    break
                continue

            batch = [first]
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max_wait_sec
            # Collect until size reached or deadline elapsed from first item.
            while len(batch) < batch_size:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(q.get(), timeout=remaining))
                except asyncio.TimeoutError:
                    break

            await _bulk_index_flush(app, batch)
            for _ in batch:
                q.task_done()
    finally:
        current_task = _safe_asyncio_current_task()
        if current_task is not None and data.get("upload_bulk_index_worker") is current_task:
            data.pop("upload_bulk_index_worker", None)


async def enqueue_bulk_index_job(app, job: dict):
    data = app.bot_data
    q = data.get("upload_bulk_index_queue")
    if q is None:
        q = asyncio.Queue()
        data["upload_bulk_index_queue"] = q
    await q.put(job)
    task = data.get("upload_bulk_index_worker")
    if not task or task.done():
        data["upload_bulk_index_worker"] = app.create_task(upload_bulk_index_worker(app))


def _upload_local_normalized_label(value: str, default: str = "book") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    cleaner = globals().get("clean_query")
    if callable(cleaner):
        try:
            text = str(cleaner(text) or "").strip()
        except Exception:
            pass
    text = re.sub(r"[\x00-\x1f\x7f/\\]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return (text[:180] or default)


def _upload_local_filename(book: dict[str, Any], file_name: str | None, file_path: str | None = None) -> str:
    raw = str(file_name or file_path or "").strip()
    ext = Path(raw).suffix.strip()
    if not ext and file_path:
        ext = Path(str(file_path)).suffix.strip()
    if not ext:
        ext = ".pdf"
    base = _upload_local_normalized_label(
        (book or {}).get("book_name")
        or (book or {}).get("display_name")
        or Path(raw).stem
        or "book"
    )
    return f"{base}{ext}"


def _upload_local_is_pdf_path(path: Path) -> bool:
    return str(path.suffix or "").lower() == ".pdf"


async def _upload_local_watermark_pdf_in_place(local_path: Path, watermark_text: str) -> None:
    if _shared_pdf_watermark_blocking is None:
        raise RuntimeError("watermark tools missing")
    if not local_path.exists():
        raise FileNotFoundError(str(local_path))

    run_blocking_fn = globals().get("run_blocking")
    if not callable(run_blocking_fn):
        raise RuntimeError("run_blocking dependency missing")

    pdf_bytes = await asyncio.to_thread(local_path.read_bytes)
    watermarked_bytes = await run_blocking_fn(_shared_pdf_watermark_blocking, pdf_bytes, watermark_text)
    tmp_wm_path = local_path.with_name(local_path.name + ".wm.part")
    try:
        if tmp_wm_path.exists():
            tmp_wm_path.unlink()
    except Exception:
        pass
    await asyncio.to_thread(tmp_wm_path.write_bytes, watermarked_bytes)
    await asyncio.to_thread(tmp_wm_path.replace, local_path)


def _upload_local_target_path(book: dict[str, Any], file_name: str | None, file_path: str | None = None) -> Path:
    folder_name = _upload_local_normalized_label(
        book.get("book_name")
        or book.get("display_name")
        or file_name
        or "book"
    )
    book_id = str(book.get("id") or "").strip()
    if book_id:
        folder_name = f"{folder_name}__{book_id}"
    return _UPLOAD_LOCAL_DIR / folder_name / _upload_local_filename(book, file_name, file_path)


async def _upload_local_retry(call, *, desc: str):
    delay = _UPLOAD_LOCAL_RETRY_BASE_DELAY_SEC
    for attempt in range(1, _UPLOAD_LOCAL_DOWNLOAD_RETRIES + 1):
        try:
            return await call()
        except RetryAfter as e:
            if attempt >= _UPLOAD_LOCAL_DOWNLOAD_RETRIES:
                raise
            sleep_for = max(1.0, float(getattr(e, "retry_after", 1) or 1.0)) + 0.5
            logger.warning("%s transient error (%s), retrying in %.1fs (attempt %s/%s)", desc, e, sleep_for, attempt, _UPLOAD_LOCAL_DOWNLOAD_RETRIES)
            await asyncio.sleep(sleep_for)
        except (TimedOut, NetworkError) as e:
            if attempt >= _UPLOAD_LOCAL_DOWNLOAD_RETRIES:
                raise
            logger.warning("%s transient error (%s), retrying in %.1fs (attempt %s/%s)", desc, e, delay, attempt, _UPLOAD_LOCAL_DOWNLOAD_RETRIES)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)


async def _save_uploaded_book_local(bot, book: dict[str, Any], file_id: str, file_name: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "book_id": str(book.get("id") or ""),
        "path": None,
        "source_kind": "missing",
        "error": None,
        "db_updated": False,
        "file_id_refreshed": False,
    }
    book_id = result["book_id"]
    try:
        if not file_id or not book_id:
            result["error"] = "missing file_id or book_id"
            return result

        _UPLOAD_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        target_path = _upload_local_target_path(book, file_name)
        if target_path.exists() and target_path.is_file() and target_path.stat().st_size > 0:
            result["path"] = str(target_path)
            result["source_kind"] = "reused-existing"
        else:
            get_file_kwargs = {
                "read_timeout": _UPLOAD_LOCAL_GET_FILE_READ_TIMEOUT_SEC,
                "connect_timeout": _UPLOAD_LOCAL_CONNECT_TIMEOUT_SEC,
                "pool_timeout": _UPLOAD_LOCAL_POOL_TIMEOUT_SEC,
            }
            tg_file = await _upload_local_retry(
                lambda: bot.get_file(file_id, **get_file_kwargs),
                desc=f"get_file {book_id or file_id}",
            )
            file_path = str(getattr(tg_file, "file_path", "") or "")
            target_path = _upload_local_target_path(book, file_name, file_path=file_path or None)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target_path.with_name(target_path.name + ".part")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            download_kwargs = {
                "read_timeout": _UPLOAD_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC,
                "connect_timeout": _UPLOAD_LOCAL_CONNECT_TIMEOUT_SEC,
                "pool_timeout": _UPLOAD_LOCAL_POOL_TIMEOUT_SEC,
            }
            await _upload_local_retry(
                lambda: tg_file.download_to_drive(custom_path=str(tmp_path), **download_kwargs),
                desc=f"download {book_id or file_id}",
            )
            if _UPLOAD_LOCAL_WATERMARK_PDF and _upload_local_is_pdf_path(target_path):
                await _upload_local_watermark_pdf_in_place(tmp_path, _UPLOAD_LOCAL_WATERMARK_TEXT)
                result["source_kind"] = "telegram-file-watermarked"
            else:
                result["source_kind"] = "telegram-file"
            await asyncio.to_thread(tmp_path.replace, target_path)
            result["path"] = str(target_path)

        update_book_path_fn = globals().get("db_update_book_path")
        if callable(update_book_path_fn) and result["path"]:
            try:
                await run_blocking(update_book_path_fn, book_id, result["path"])
                result["db_updated"] = True
            except Exception as e:
                result["error"] = f"db_update_book_path failed: {e}"
                logger.warning("db_update_book_path failed for %s: %s", book_id, e, exc_info=True)
        elif result["path"]:
            result["error"] = "db_update_book_path dependency missing"
            logger.warning("db_update_book_path dependency missing; saved local file without DB path update for %s", book_id)
        book["path"] = result["path"]

        if result["path"] and _UPLOAD_LOCAL_REFRESH_FILE_ID:
            refresh_info = await _refresh_uploaded_book_file_id(bot, book_id, result["path"])
            if refresh_info.get("file_id"):
                new_file_id = str(refresh_info.get("file_id") or "").strip()
                new_file_unique_id = str(refresh_info.get("file_unique_id") or "").strip() or None
                update_book_file_id_fn = globals().get("update_book_file_id")
                if callable(update_book_file_id_fn):
                    try:
                        await run_blocking(update_book_file_id_fn, book_id, new_file_id, True, new_file_unique_id)
                        result["file_id_refreshed"] = True
                        book["file_id"] = new_file_id
                        if new_file_unique_id:
                            book["file_unique_id"] = new_file_unique_id
                    except Exception as e:
                        result["error"] = f"update_book_file_id failed: {e}"
                        logger.warning("update_book_file_id failed for %s: %s", book_id, e, exc_info=True)
                else:
                    result["error"] = "update_book_file_id dependency missing"
                    logger.warning("update_book_file_id dependency missing; kept old file_id for %s", book_id)

                if result["file_id_refreshed"]:
                    db_get_book_by_id_fn = globals().get("db_get_book_by_id")
                    if callable(db_get_book_by_id_fn) and es_available():
                        try:
                            refreshed_book = await run_blocking(db_get_book_by_id_fn, book_id)
                            if refreshed_book:
                                await run_blocking(
                                    index_book,
                                    refreshed_book.get("book_name"),
                                    new_file_id,
                                    result["path"],
                                    book_id,
                                    refreshed_book.get("display_name") or refreshed_book.get("book_name"),
                                    new_file_unique_id,
                                )
                        except Exception as e:
                            logger.warning("Failed to reindex refreshed book file_id for %s: %s", book_id, e, exc_info=True)
            elif not result["error"]:
                result["error"] = f"file_id refresh failed: {refresh_info.get('error') or 'unknown error'}"
        logger.info("Saved uploaded book locally: book_id=%s path=%s", book_id, result["path"])
        return result
    except Exception as e:
        result["error"] = str(e)
        logger.error("Failed to save uploaded book locally for %s: %s", book_id, e, exc_info=True)
        return result


async def _refresh_uploaded_book_file_id(bot, book_id: str, local_path: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "file_id": None,
        "file_unique_id": None,
        "error": None,
    }
    if not _UPLOAD_LOCAL_REFRESH_FILE_ID:
        result["error"] = "refresh disabled"
        return result

    storage_channel_id = _resolve_book_storage_channel_id()
    # Telegram channel IDs are negative. Only 0 means "missing".
    if storage_channel_id == 0:
        result["error"] = "missing BOOK_STORAGE_CHANNEL_ID"
        return result

    if not local_path or not Path(local_path).exists():
        result["error"] = "missing local path"
        return result

    try:
        sent = await _upload_local_retry(
            lambda: bot.send_document(
                chat_id=storage_channel_id,
                document=str(local_path),
                thumbnail=get_book_thumbnail_input(),
                disable_notification=True,
            ),
            desc=f"refresh file_id {book_id}",
        )
        document = getattr(sent, "document", None)
        file_id = str(getattr(document, "file_id", "") or "").strip()
        if not file_id:
            result["error"] = "Telegram did not return a document file_id"
            return result
        result["file_id"] = file_id
        result["file_unique_id"] = str(getattr(document, "file_unique_id", "") or "").strip() or None
        return result
    except Exception as e:
        result["error"] = str(e)
        logger.warning(
            "Failed to refresh file_id for %s from local file %s via storage channel %s: %s",
            book_id,
            local_path,
            storage_channel_id,
            e,
            exc_info=True,
        )
        return result


def _resolve_book_storage_channel_id() -> int:
    candidates: list[Any] = []
    try:
        from config import BOOK_STORAGE_CHANNEL_ID as _config_book_storage_channel_id  # type: ignore

        candidates.append(_config_book_storage_channel_id)
    except Exception:
        pass
    candidates.extend(
        [
            globals().get("BOOK_STORAGE_CHANNEL_ID"),
            os.getenv("BOOK_STORAGE_CHANNEL_ID", ""),
        ]
    )
    for env_name in ("BOOK_MIGRATION_TARGET_CHANNEL_IDS", "UPLOAD_CHANNEL_IDS"):
        candidates.append(os.getenv(env_name, ""))
    candidates.extend(
        [
            globals().get("TELEGRAM_OWNER_ID"),
            os.getenv("TELEGRAM_OWNER_ID", ""),
        ]
    )
    # Final hard fallback so uploads keep working even if the runtime env is stale.
    # This is the known private storage channel used for minted file_id refreshes.
    candidates.append(-1003970604636)
    for raw in candidates:
        ids = _coerce_int_id_list(raw)
        if ids:
            return ids[0]
        try:
            value = int(str(raw or "").strip())
        except Exception:
            value = 0
        if value:
            return value
    return 0


def start_upload_local_backup_worker(app) -> None:
    if not _UPLOAD_AUTO_DOWNLOAD_LOCAL:
        return
    logger.info(
        "Local backup worker config: storage_channel_id=%s auto_download=%s refresh_file_id=%s workers=%s",
        _resolve_book_storage_channel_id(),
        _UPLOAD_AUTO_DOWNLOAD_LOCAL,
        _UPLOAD_LOCAL_REFRESH_FILE_ID,
        _UPLOAD_LOCAL_WORKER_COUNT,
    )
    data = app.bot_data
    workers = data.get(_UPLOAD_LOCAL_WORKER_KEY)
    if isinstance(workers, list):
        live_workers = [task for task in workers if task is not None and not task.done()]
        if live_workers:
            data[_UPLOAD_LOCAL_WORKER_KEY] = live_workers
            return
    elif workers and not getattr(workers, "done", lambda: True)():
        return
    created_workers = [
        app.create_task(_upload_local_backup_worker(app, worker_index=index + 1))
        for index in range(_UPLOAD_LOCAL_WORKER_COUNT)
    ]
    data[_UPLOAD_LOCAL_WORKER_KEY] = created_workers
    logger.info("Started %s local backup worker(s)", len(created_workers))


async def _upload_local_backup_worker(app, worker_index: int = 1) -> None:
    worker_id = f"upload-local-backup:{os.getpid()}:{worker_index}"
    try:
        while True:
            try:
                claim_fn = globals().get("db_claim_book_local_download_job")
                retry_fn = globals().get("db_retry_book_local_download_job")
                fail_fn = globals().get("db_fail_book_local_download_job")
                complete_fn = globals().get("db_complete_book_local_download_job")
                if not (callable(claim_fn) and callable(retry_fn) and callable(fail_fn) and callable(complete_fn)):
                    logger.warning("Local backup worker dependencies missing; sleeping")
                    await asyncio.sleep(_UPLOAD_LOCAL_WORKER_POLL_SECONDS)
                    continue

                job = await run_blocking(
                    claim_fn,
                    worker_id,
                    _UPLOAD_LOCAL_JOB_STALE_AFTER_SECONDS,
                )
                if not job:
                    await asyncio.sleep(_UPLOAD_LOCAL_WORKER_POLL_SECONDS)
                    continue

                job_id = str(job.get("id") or "").strip()
                book_id = str(job.get("book_id") or "").strip()
                file_id = str(job.get("file_id") or "").strip()
                file_name = str(job.get("file_name") or "").strip()
                attempts = int(job.get("attempts") or 0)
                max_attempts = int(job.get("max_attempts") or 12)
                logger.info(
                    "Local backup job claimed: job_id=%s book_id=%s status=downloading attempts=%s/%s",
                    job_id,
                    book_id,
                    attempts,
                    max_attempts,
                )

                if not job_id or not book_id or not file_id or not file_name:
                    error = "missing local backup job payload"
                    if job_id:
                        if attempts >= max_attempts:
                            await run_blocking(fail_fn, job_id, error)
                        else:
                            await run_blocking(retry_fn, job_id, error, 60.0)
                    if _UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                result = await _save_uploaded_book_local(app.bot, {"id": book_id}, file_id, file_name)
                refresh_required = bool(_UPLOAD_LOCAL_REFRESH_FILE_ID)
                refresh_ok = bool(result.get("file_id_refreshed")) or not refresh_required
                if result.get("path") and result.get("db_updated") and refresh_ok:
                    logger.info("Local backup job done: job_id=%s book_id=%s path=%s", job_id, book_id, result["path"])
                    await run_blocking(complete_fn, job_id)
                    if _UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                error = str(result.get("error") or "local backup failed")
                if result.get("path") and not result.get("db_updated"):
                    error = f"{error} (local file saved, DB path update pending)"
                elif result.get("path") and result.get("db_updated") and refresh_required and not result.get("file_id_refreshed"):
                    error = f"{error} (local file saved, file_id refresh pending)"
                if attempts >= max_attempts:
                    logger.error(
                        "Local backup job failed permanently: job_id=%s book_id=%s attempts=%s/%s error=%s",
                        job_id,
                        book_id,
                        attempts,
                        max_attempts,
                        error,
                    )
                    await run_blocking(fail_fn, job_id, error)
                else:
                    backoff = max(
                        _UPLOAD_LOCAL_RETRY_MIN_DELAY_SEC,
                        min(3600.0, _UPLOAD_LOCAL_RETRY_BASE_DELAY_SEC * (2 ** max(0, attempts - 1))),
                    )
                    logger.warning(
                        "Local backup job retry scheduled: job_id=%s book_id=%s attempts=%s/%s backoff=%.1fs error=%s",
                        job_id,
                        book_id,
                        attempts,
                        max_attempts,
                        backoff,
                        error,
                    )
                    await run_blocking(retry_fn, job_id, error, backoff)
                if _UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                    await asyncio.sleep(_UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Local backup worker loop failed: %s", e, exc_info=True)
                await asyncio.sleep(5.0)
    finally:
        current_task = _safe_asyncio_current_task()
        workers = app.bot_data.get(_UPLOAD_LOCAL_WORKER_KEY)
        if current_task is not None and isinstance(workers, list):
            remaining = [task for task in workers if task is not current_task and not task.done()]
            if remaining:
                app.bot_data[_UPLOAD_LOCAL_WORKER_KEY] = remaining
            else:
                app.bot_data.pop(_UPLOAD_LOCAL_WORKER_KEY, None)
        elif current_task is not None and app.bot_data.get(_UPLOAD_LOCAL_WORKER_KEY) is current_task:
            app.bot_data.pop(_UPLOAD_LOCAL_WORKER_KEY, None)


async def _enqueue_upload_local_backup(app, book: dict[str, Any], file_id: str, file_name: str) -> None:
    if not _UPLOAD_AUTO_DOWNLOAD_LOCAL:
        return
    enqueue_fn = globals().get("db_enqueue_book_local_download_job")
    if not callable(enqueue_fn):
        logger.warning("db_enqueue_book_local_download_job dependency missing; skipping local backup enqueue")
        return
    book_id = str((book or {}).get("id") or "").strip()
    if not book_id:
        logger.warning("Skipping local backup enqueue for missing book_id")
        return
    try:
        job_id = await run_blocking(
            enqueue_fn,
            book_id,
            str(file_id or "").strip(),
            str(file_name or "").strip(),
            str((book or {}).get("file_unique_id") or "").strip() or None,
        )
        logger.info("Queued local backup job: book_id=%s job_id=%s", book_id, job_id)
    except Exception as e:
        logger.error("Failed to enqueue local backup job for %s: %s", book_id, e, exc_info=True)


async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global upload_mode
    try:
        lang = ensure_user_language(update, context)

        if is_blocked(update.effective_user.id):
            await update.message.reply_text(MESSAGES[lang]["blocked"])
            return
        if update.effective_user and await is_stopped_user(update.effective_user.id):
            return
        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            return
        await update_user_info(update, context)
        if is_allowed(update.effective_user.id):
            _set_user_upload_mode(context, _UPLOAD_MODE_BOOK)
            upload_mode = True
            await update.message.reply_text(MESSAGES[lang]["upload_activated"])
        else:
            await update.message.reply_text(MESSAGES[lang]["not_authorized"])
            keyboard = InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(MESSAGES[lang]["answer_yes"], callback_data="upload_help_yes"),
                    InlineKeyboardButton(MESSAGES[lang]["answer_no"], callback_data="upload_help_no")
                ]]
            )
            await update.message.reply_text(
                MESSAGES[lang]["upload_help_prompt"],
                reply_markup=keyboard
            )
    except Exception as e:
        logger.error(f"/upload failed: {e}")   
        lang = ensure_user_language(update, context)
        await update.message.reply_text(MESSAGES[lang]["error"])
        raise


_QUOTE_CHARS = "\"'“”«»„‟"
_TITLE_QUOTED_RE = re.compile(r"[\"“«„](.{2,180}?)[\"”»‟]")
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2}|21\d{2})\b")
_FIELD_PATTERNS = {
    "title": re.compile(r"^(?:nomi|nom|title|name|название)\s*[:\-]\s*(.+)$", re.IGNORECASE),
    "year": re.compile(r"^(?:yili|yil|year|год|йили|йил)\s*[:\-]\s*(.+)$", re.IGNORECASE),
    "genre": re.compile(r"^(?:janr|genre|жанр)\s*[:\-]\s*(.+)$", re.IGNORECASE),
    "country": re.compile(r"^(?:davlat|давлат|country|страна)\s*[:\-]\s*(.+)$", re.IGNORECASE),
    "language": re.compile(r"^(?:tili|til|тили|тил|language|язык)\s*[:\-]\s*(.+)$", re.IGNORECASE),
    "rating": re.compile(r"^(?:kinopoisk|kino\s*poisk|imdb|rating|reyting|рейтинг|кинопоиск|sifati|quality|качество)\s*[:\-]\s*(.+)$", re.IGNORECASE),
}
_LANG_INLINE_PATTERNS = [
    re.compile(r"^(.+?)\s+tilida$", re.IGNORECASE),
    re.compile(r"^(.+?)\s+тилида$", re.IGNORECASE),
]
_PROMO_LINE_RE = re.compile(r"^(?:#\s*)?(?:premyera|premiere|премьера)\b", re.IGNORECASE)
_CAPTION_LINK_RE = re.compile(r"(?:https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+)", re.IGNORECASE)


def _clean_caption_line(raw: str) -> str:
    line = str(raw or "").strip()
    if not line:
        return ""
    line = line.strip("•*-_ \t")
    line = re.sub(r"\s+", " ", line).strip()
    return line


def _clean_meta_value(raw: str) -> str:
    text = _clean_caption_line(raw)
    return text.strip(_QUOTE_CHARS + " ")


def _line_for_match(raw: str) -> str:
    line = _clean_caption_line(raw)
    if not line:
        return ""
    # Remove leading emoji/decorative symbols before field matching.
    line = re.sub(r"^[^\w]+", "", line, flags=re.UNICODE)
    return line.strip()


def _is_separator_line(raw: str) -> bool:
    line = str(raw or "").strip()
    if not line:
        return True
    # Decorative separators like ───── or ***** should be ignored.
    return not bool(re.search(r"[\w]", line, flags=re.UNICODE))


def _is_channel_or_link_line(raw: str, normalized: str = "") -> bool:
    raw_s = str(raw or "").strip().lower()
    norm_s = str(normalized or "").strip().lower()
    if not raw_s and not norm_s:
        return False
    if raw_s.startswith("@"):
        return True
    if "t.me/" in raw_s or "telegram.me/" in raw_s:
        return True
    if raw_s.startswith(("http://", "https://", "www.")):
        return True
    if norm_s.startswith(("manba:", "source:")):
        return True
    return False


async def _process_upload(
    context: ContextTypes.DEFAULT_TYPE,
    status_msg,
    file_id: str,
    file_name: str,
    lang: str,
    file_unique_id: str | None,
    caption_text: str | None = None,
    receipt_id: str | None = None,
    uploader_user_id: int | None = None,
):
    receipt_saved_to_db = False
    receipt_saved_to_es = False
    receipt_book_id = None
    logger.info(f"_process_upload started for: {file_name}")
    try:
        skip_name_duplicate_check = _env_bool("UPLOAD_SKIP_NAME_DUP_CHECK", False)
        skip_request_notify = _env_bool("UPLOAD_SKIP_REQUEST_NOTIFY", False)
        no_status_edits = _env_bool("UPLOAD_NO_STATUS_EDITS", False)
        book_name, _ = os.path.splitext(file_name)
        cleaned_name = clean_query(book_name)
        if _book_has_adult_markers(file_name, book_name, cleaned_name, caption_text):
            logger.info("Book upload skipped by adult filter: file_name=%s", file_name)
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="filtered",
                        saved_to_db=False,
                        saved_to_es=False,
                        error="blocked_by_adult_filter",
                    )
                except Exception:
                    logger.exception("Failed to update upload receipt as filtered")
            blocked_msg = str(MESSAGES.get(lang, {}).get("upload_adult_filtered", "⚠️ This book was ignored by content filter."))
            if status_msg:
                await _send_status_with_retry(
                    status_msg,
                    f"{blocked_msg} {file_name}",
                    reply_only=no_status_edits,
                )
            return
        if not _book_upload_is_allowed_file(file_name):
            file_ext = _book_upload_file_extension(file_name) or "<none>"
            logger.info(
                "Book upload skipped by file type filter: file_name=%s ext=%s",
                file_name,
                file_ext,
            )
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="filtered",
                        saved_to_db=False,
                        saved_to_es=False,
                        error="unsupported_book_file_type",
                    )
                except Exception:
                    logger.exception("Failed to update upload receipt as file type filtered")
            blocked_msg = str(
                MESSAGES.get(lang, {}).get(
                    "upload_file_type_filtered",
                    "⚠️ This file type is not allowed for book uploads: {file_name}\n📚 Allowed book formats: {formats}",
                )
            )
            if status_msg:
                await _send_status_with_retry(
                    status_msg,
                    blocked_msg.format(file_name=file_name, formats=_BOOK_UPLOAD_ALLOWED_FORMATS_TEXT),
                    reply_only=no_status_edits,
                )
            return
        # prevent duplicates - prioritize file_unique_id over name for accuracy
        existing = await run_blocking(db_find_duplicate_book, None, None, file_unique_id)
        logger.info(f"Duplicate check for {file_name}: {'FOUND' if existing else 'NOT FOUND'} (by file_unique_id)")
        if existing:
            logger.info(f"Duplicate detected by file_unique_id: {file_unique_id}")
        else:
            # Only check by name if file_unique_id check failed AND we have a meaningful name
            if (not skip_name_duplicate_check) and cleaned_name and len(cleaned_name) > 3:  # Only check names longer than 3 chars
                existing_by_name = await run_blocking(db_find_duplicate_book, cleaned_name, None, None)
                if existing_by_name:
                    logger.info(f"Additional duplicate found by name: {cleaned_name}")
                    existing = existing_by_name
        logger.info(f"Final duplicate check for {file_name}: {'FOUND' if existing else 'NOT FOUND'}")

        if existing:
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="duplicate",
                        book_id=existing.get("id") if isinstance(existing, dict) else None,
                        saved_to_db=True,
                        saved_to_es=bool((existing or {}).get("indexed", False)) if isinstance(existing, dict) else False,
                        error=None,
                    )
                except Exception:
                    logger.exception("Failed to update upload receipt as duplicate")
            if status_msg:
                await _send_status_with_retry(
                    status_msg,
                    f"{MESSAGES[lang]['duplicate']} {file_name}",
                    reply_only=no_status_edits,
                )
            return

        # add new book with permanent UUID and indexed flag
        normalized_title = cleaned_name or book_name
        new_book = {
            "id": str(uuid.uuid4()),   # permanent ID
            "book_name": normalized_title,  # normalized for search
            "display_name": normalized_title,  # normalized permanent display name
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "path": None,
            "indexed": False
        }
        inserted = await run_blocking(db_insert_book, new_book)
        logger.info(f"Book insertion for {file_name}: {'SUCCESS' if inserted is not False else 'FAILED'}")
        if inserted is False:
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="duplicate",
                        saved_to_db=True,
                        error=None,
                    )
                except Exception:
                    logger.exception("Failed to update upload receipt after duplicate insert conflict")
            if status_msg:
                await _send_status_with_retry(
                    status_msg,
                    f"{MESSAGES[lang]['duplicate']} {file_name}",
                    reply_only=no_status_edits,
                )
            return

        if uploader_user_id:
            try:
                await run_blocking(db_update_book_upload_meta, new_book["id"], uploader_user_id, "user_upload")
            except Exception:
                logger.exception("Failed to update book upload metadata")
        if receipt_id:
            try:
                await run_blocking(
                    db_update_upload_receipt,
                    receipt_id,
                    status="saved_db",
                    book_id=new_book["id"],
                    saved_to_db=True,
                    saved_to_es=False,
                    error=None,
                )
            except Exception:
                logger.exception("Failed to update upload receipt after DB save")
        receipt_saved_to_db = True
        receipt_book_id = new_book["id"]
        try:
            await _enqueue_upload_local_backup(context.application, new_book, file_id, file_name)
        except Exception as e:
            logger.error("Failed to enqueue local backup job for %s: %s", new_book["id"], e, exc_info=True)
        # User-facing confirmation: send only once when DB save succeeds.
        await _send_status_with_retry(
            status_msg,
            f"{MESSAGES[lang]['saved']} {file_name}",
            reply_only=no_status_edits,
        )

        async def _index_uploaded_book():
            nonlocal receipt_saved_to_es
            try:
                if receipt_id:
                    try:
                        await run_blocking(db_update_upload_receipt, receipt_id, status="indexing", error=None)
                    except Exception:
                        logger.exception("Failed to update upload receipt status=indexing")
                run_blocking_heavy_fn = globals().get("run_blocking_heavy")
                if callable(run_blocking_heavy_fn):
                    await run_blocking_heavy_fn(
                        index_book,
                        new_book["book_name"],
                        new_book["file_id"],
                        new_book["path"],
                        new_book["id"],
                        new_book.get("display_name"),
                        new_book.get("file_unique_id"),
                        "false",
                    )
                else:
                    await run_blocking(
                        index_book,
                        new_book["book_name"],
                        new_book["file_id"],
                        new_book["path"],
                        new_book["id"],
                        new_book.get("display_name"),
                        new_book.get("file_unique_id"),
                        "false",
                    )
                new_book["indexed"] = True
                await run_blocking(update_book_indexed, new_book["id"], True)
                receipt_saved_to_es = True
                if receipt_id:
                    try:
                        await run_blocking(
                            db_update_upload_receipt,
                            receipt_id,
                            status="indexed",
                            book_id=new_book["id"],
                            saved_to_db=True,
                            saved_to_es=True,
                            error=None,
                        )
                    except Exception:
                        logger.exception("Failed to update upload receipt after ES index")
            except Exception as e:
                logger.error(f"Background index failed: {e}", exc_info=True)
                if receipt_id:
                    try:
                        await run_blocking(
                            db_update_upload_receipt,
                            receipt_id,
                            status="index_failed",
                            book_id=new_book["id"],
                            saved_to_db=True,
                            saved_to_es=False,
                            error=str(e)[:1000],
                        )
                    except Exception:
                        logger.exception("Failed to update upload receipt after index failure")

        # respond fast, index in background
        logger.info(f"Preparing confirmation message for {file_name}")
        if es_available():
            if receipt_id:
                try:
                    await run_blocking(db_update_upload_receipt, receipt_id, status="indexing", error=None)
                except Exception:
                    logger.exception("Failed to update upload receipt status=indexing")
            enqueue_fn = globals().get("enqueue_bulk_index_job")
            if callable(enqueue_fn):
                await enqueue_fn(
                    context.application,
                    {
                        "doc": {
                            "id": new_book["id"],
                            "book_name": new_book["book_name"],
                            "display_name": new_book.get("display_name"),
                            "file_id": new_book.get("file_id"),
                            "file_unique_id": new_book.get("file_unique_id"),
                            "path": new_book.get("path"),
                            "indexed": True,
                        },
                        "book_id": new_book["id"],
                        "receipt_id": receipt_id,
                        "status_msg": status_msg,
                        "lang": lang,
                        "file_name": file_name,
                        "no_status_edits": no_status_edits,
                    },
                )
            else:
                context.application.create_task(_index_uploaded_book())
        else:
            if receipt_id:
                try:
                    await run_blocking(
                        db_update_upload_receipt,
                        receipt_id,
                        status="saved_db_no_es",
                        book_id=new_book["id"],
                        saved_to_db=True,
                        saved_to_es=False,
                        error="ES unavailable",
                    )
                except Exception:
                    logger.exception("Failed to update upload receipt for ES unavailable")

        # Notify users who requested this book
        if not skip_request_notify:
            async def _notify_request_matches_safe():
                try:
                    await notify_request_matches(context.bot, new_book)
                except Exception as e:
                    logger.error("Request notify failed for book %s: %s", new_book.get("id"), e)

            context.application.create_task(_notify_request_matches_safe())
    except Exception as e:
        logger.error(f"process upload failed: {e}", exc_info=True)
        if receipt_id:
            try:
                final_status = "failed"
                if receipt_saved_to_es:
                    final_status = "post_index_failed"
                elif receipt_saved_to_db:
                    final_status = "post_save_failed"
                await run_blocking(
                    db_update_upload_receipt,
                    receipt_id,
                    status=final_status,
                    book_id=receipt_book_id,
                    saved_to_db=receipt_saved_to_db,
                    saved_to_es=receipt_saved_to_es,
                    error=str(e)[:1000],
                )
            except Exception:
                logger.exception("Failed to update upload receipt after processing failure")
        try:
            if status_msg:
                await _send_status_with_retry(
                    status_msg,
                    f"❌ Upload failed: {str(e)[:100]}",
                    reply_only=no_status_edits,
                )
                logger.info(f"Sent error message for {file_name}")
        except Exception as msg_e:
            logger.error(f"Failed to edit status message: {msg_e}")
            # Try to send a new message if editing fails
            try:
                await status_msg.reply_text(f"❌ Upload failed: {str(e)[:100]}")
                logger.info(f"Sent error reply message for {file_name}")
            except Exception as reply_e:
                logger.error(f"Failed to send error reply: {reply_e}")
                # Last resort - try to send any message
                try:
                    if status_msg:
                        await status_msg.reply_text("❌ Upload processing failed")
                        logger.info("Sent fallback error message")
                except Exception as fallback_e:
                    logger.error(f"All error message attempts failed: {fallback_e}")


async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        lang = ensure_user_language(update, context)

        if is_blocked(update.effective_user.id):
            await update.message.reply_text(MESSAGES[lang]["blocked"])
            return
        if update.effective_user and await is_stopped_user(update.effective_user.id):
            return
        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            return

        await update_user_info(update, context)
        doc = update.message.document if update.message else None
        doc_mime = str(getattr(doc, "mime_type", "") or "").lower()

        user_mode = _get_user_upload_mode(context)
        logger.info(
            "handle_file: user_mode=%s, user_id=%s, is_allowed=%s",
            user_mode or "none",
            update.effective_user.id if update.effective_user else "N/A",
            is_allowed(update.effective_user.id) if update.effective_user else "N/A",
        )
        if user_mode == _UPLOAD_MODE_BOOK and is_allowed(update.effective_user.id):
            if not update.message.document:
                await update.message.reply_text(MESSAGES[lang]["send_doc"])
                return

            no_status_edits = _env_bool("UPLOAD_NO_STATUS_EDITS", False)
            if no_status_edits:
                # In reply-only mode, avoid creating a separate "processing" message.
                status_msg = update.message
            else:
                status_msg = await _send_with_retry(
                    lambda: update.message.reply_text(MESSAGES[lang]["upload_processing"])
                )
            file = doc
            file_id = file.file_id
            file_unique_id = getattr(file, "file_unique_id", None)
            file_name = file.file_name or "unknown"
            receipt_id = str(uuid.uuid4())
            try:
                await run_blocking(
                    db_insert_upload_receipt,
                    {
                        "id": receipt_id,
                        "user_id": update.effective_user.id if update.effective_user else None,
                        "file_id": file_id,
                        "file_unique_id": file_unique_id,
                        "file_name": file_name,
                        "status": "received",
                        "saved_to_db": False,
                        "saved_to_es": False,
                    },
                )
            except Exception:
                logger.exception("Failed to insert upload receipt")
                receipt_id = None

            logger.info(f"Starting upload process for file: {file_name} by user {update.effective_user.id if update.effective_user else 'unknown'}")
            context.application.create_task(
                _process_upload(
                    context,
                    status_msg,
                    file_id,
                    file_name,
                    lang,
                    file_unique_id,
                    caption_text=str(getattr(update.message, "caption", "") or ""),
                    receipt_id=receipt_id,
                    uploader_user_id=update.effective_user.id if update.effective_user else None,
                )
            )
        else:
            logger.info(
                "handle_file: ELSE branch - user_mode=%s, is_allowed=%s",
                user_mode or "none",
                is_allowed(update.effective_user.id) if update.effective_user else "N/A",
            )
            if doc_mime.startswith("image/"):
                await _reply_search_image_hint(update, context, lang)
            else:
                logger.info(f"handle_file: Sending 'upload_inactive' message")
                await update.message.reply_text(MESSAGES[lang]["upload_inactive"])
    except Exception as e:
        logger.error(f"handle_file failed: {e}", exc_info=True)
        try:
            lang = ensure_user_language(update, context)
            await safe_reply(update, f"❌ File processing failed: {str(e)[:100]}")
        except Exception as reply_e:
            logger.error(f"Failed to send error reply in handle_file: {reply_e}")
            # Last resort
            try:
                if update.message:
                    await update.message.reply_text("❌ File processing failed")
            except Exception:
                logger.error("All error messaging failed in handle_file")
        raise


async def handle_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message or not update.message.photo:
            return
        lang = ensure_user_language(update, context)
        user_id = update.effective_user.id if update.effective_user else None
        if user_id and is_blocked(user_id):
            await update.message.reply_text(MESSAGES[lang]["blocked"])
            return
        if user_id and await is_stopped_user(user_id):
            return
        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            return
        await update_user_info(update, context)
        await _reply_search_image_hint(update, context, lang)
    except Exception as e:
        logger.error(f"handle_photo_message failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang]["error"])



def sync_unindexed_books():
    """
    Scan DB for entries with 'indexed': False
    and push them into Elasticsearch using stable UUIDs.
    """
    if not es_available():
        logger.error("Elasticsearch not available, skipping sync.")
        return

    try:
        logger.info("🔄 Syncing unindexed books to Elasticsearch...")
        ensure_index()
        books = load_books()
        count = 0

        for book in books:
            if not book.get("indexed", True):  # default True if missing
                book_name = book.get("book_name")
                file_id = book.get("file_id")
                path = book.get("path")
                book_id = book.get("id")

                if not book_name:
                    continue

                # skip if id missing (should not happen in DB)
                if not book_id:
                    logger.debug("Book missing id; skipping indexing")
                    continue

                # index with stable UUID
                index_book(
                    book_name,
                    file_id=file_id,
                    path=path,
                    book_id=book_id,
                    display_name=get_display_name(book),
                    file_unique_id=book.get("file_unique_id"),
                )
                book["indexed"] = True
                update_book_indexed(book_id, True)
                count += 1

        logger.info(f"✅ Synced {count} previously unindexed books into Elasticsearch.")
    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)


# --- Audit command ---
