from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any

import ai_tools as _ai_tools
from language import MESSAGES

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
    "db_find_duplicate_movie",
    "db_insert_movie",
    "es_available",
    "update_movie_indexed",
    "_send_with_retry",
    "ApplicationHandlerStop",
    "safe_reply",
    "db_find_duplicate_book",
    "db_update_upload_receipt",
    "db_insert_book",
    "db_update_book_upload_meta",
    "UPLOAD_CHANNEL_IDS",
    "enqueue_upload_fanout",
    "index_book",
    "notify_request_matches",
    "db_insert_upload_receipt",
    "_reply_search_image_hint",
    "ensure_index",
    "load_books",
    "get_display_name",
    "update_book_indexed",
    "ensure_movies_index",
    "index_movie",
    "db_list_unindexed_movies",
    "InlineKeyboardMarkup",
    "InlineKeyboardButton",
)

_UPLOAD_MODE_KEY = "upload_mode_state"
_UPLOAD_MODE_BOOK = "book"
_UPLOAD_MODE_MOVIE = "movie_removed"


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
        if mode in {_UPLOAD_MODE_BOOK, _UPLOAD_MODE_MOVIE}:
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


async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global upload_mode, movie_upload_mode
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
            movie_upload_mode = False
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


def _normalize_title_candidate(raw: str) -> str:
    value = _clean_meta_value(raw)
    value = value.strip("#").strip()
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _is_bad_movie_title_candidate(raw: str) -> bool:
    value = _normalize_title_candidate(raw)
    if not value:
        return True
    lower = value.lower()
    if _PROMO_LINE_RE.match(value):
        return True
    if _is_channel_or_link_line(value, value):
        return True
    if lower.startswith("video_") and len(lower) >= 12:
        return True
    if re.fullmatch(r"video[_\-\s]*agad[a-z0-9_\-]{5,}", lower):
        return True
    if re.fullmatch(r"agad[a-z0-9_\-]{6,}", lower):
        return True
    return False


def _humanize_movie_filename_stem(stem: str) -> str:
    text = str(stem or "").strip()
    if not text:
        return ""
    text = re.sub(r"[_\.]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _sanitize_caption_for_storage(caption: str) -> str:
    raw = str(caption or "").strip()
    if not raw:
        return ""

    lines_out: list[str] = []
    blank_emitted = False
    for raw_line in raw.splitlines():
        line = _CAPTION_LINK_RE.sub("", str(raw_line))
        line = re.sub(r"\s{2,}", " ", line).strip()
        if line:
            lines_out.append(line)
            blank_emitted = False
            continue
        if lines_out and not blank_emitted:
            lines_out.append("")
            blank_emitted = True

    cleaned = "\n".join(lines_out).strip()
    if len(cleaned) > 1024:
        cleaned = cleaned[:1021].rstrip() + "..."
    return cleaned


def _normalize_search_text(raw: str) -> str:
    cleaner = globals().get("clean_query")
    text = str(raw or "").strip()
    if callable(cleaner):
        try:
            return str(cleaner(text) or "").strip()
        except Exception:
            pass
    return re.sub(r"\s+", " ", text).strip().lower()


def _parse_movie_caption(caption: str) -> dict[str, Any]:
    text = str(caption or "").strip()
    if not text:
        return {}

    lines = [_clean_caption_line(x) for x in text.splitlines()]
    lines = [x for x in lines if x]
    if not lines:
        return {}

    title = ""
    for m in _TITLE_QUOTED_RE.finditer(text):
        candidate = _normalize_title_candidate(m.group(1))
        if candidate and not _is_bad_movie_title_candidate(candidate):
            title = candidate
            break

    year = None
    genre = ""
    country = ""
    language = ""
    rating = ""

    for line in lines:
        if _is_separator_line(line):
            continue
        line_for_match = _line_for_match(line)
        if not line_for_match:
            continue

        parsed = False
        for key, pattern in _FIELD_PATTERNS.items():
            m = pattern.match(line_for_match)
            if not m:
                continue
            value = _clean_meta_value(m.group(1))
            if not value:
                parsed = True
                break
            if key == "title":
                candidate = _normalize_title_candidate(value)
                if candidate and not _is_bad_movie_title_candidate(candidate):
                    title = candidate
            elif key == "year":
                ym = _YEAR_RE.search(value)
                if ym:
                    try:
                        year = int(ym.group(1))
                    except Exception:
                        year = None
            elif key == "genre":
                genre = value
            elif key == "country":
                country = value
            elif key == "language":
                language = value
            elif key == "rating":
                rating = value
            parsed = True
            break
        if parsed:
            continue

        if not language:
            for pat in _LANG_INLINE_PATTERNS:
                m = pat.match(line_for_match)
                if m:
                    language = _clean_meta_value(m.group(1))
                    parsed = True
                    break
        if parsed:
            continue

        if year is None:
            ym = _YEAR_RE.search(line_for_match)
            if ym and any(tok in line_for_match.lower() for tok in ("yil", "year", "год", "йил")):
                try:
                    year = int(ym.group(1))
                except Exception:
                    year = None

    if not title:
        for line in lines:
            if _is_separator_line(line):
                continue
            line_for_match = _line_for_match(line)
            if not line_for_match:
                continue
            if _is_channel_or_link_line(line, line_for_match):
                continue
            if _PROMO_LINE_RE.match(line_for_match):
                continue
            if any(p.match(line_for_match) for p in _FIELD_PATTERNS.values()):
                continue
            if any(p.match(line_for_match) for p in _LANG_INLINE_PATTERNS):
                continue
            if _YEAR_RE.search(line_for_match) and len(line_for_match) <= 12:
                continue
            candidate = _normalize_title_candidate(line_for_match)
            if candidate and not _is_bad_movie_title_candidate(candidate):
                title = candidate
                break

    search_parts = [title, genre, country, language, rating]
    if year:
        search_parts.append(str(year))
    search_parts.append(text)
    search_text = _normalize_search_text(" ".join([p for p in search_parts if p]))
    latinize = globals().get("latinize_text")
    if callable(latinize) and search_text:
        try:
            latinized = _normalize_search_text(latinize(search_text))
            if latinized and latinized != search_text:
                search_text = f"{search_text} {latinized}".strip()
        except Exception:
            pass

    return {
        "parsed_title": title or None,
        "release_year": year,
        "genre": genre or None,
        "country": country or None,
        "movie_lang": language or None,
        "rating": rating or None,
        "caption_text": text,
        "search_text": search_text or None,
    }


def _movie_media_payload_from_message(msg) -> dict | None:
    if not msg:
        return None
    caption = str(getattr(msg, "caption", "") or "").strip()
    storage_caption = _sanitize_caption_for_storage(caption)
    caption_for_parse = storage_caption or caption
    caption_meta = _parse_movie_caption(caption_for_parse)
    video = getattr(msg, "video", None)
    if video:
        movie_name = getattr(video, "file_name", None) or f"video_{getattr(video, 'file_unique_id', '') or uuid.uuid4().hex[:8]}.mp4"
        return {
            "kind": "video",
            "file_id": getattr(video, "file_id", None),
            "file_unique_id": getattr(video, "file_unique_id", None),
            "file_name": movie_name,
            "mime_type": getattr(video, "mime_type", None),
            "duration_seconds": getattr(video, "duration", None),
            "file_size": getattr(video, "file_size", None),
            "caption": caption,
            "storage_caption": storage_caption or None,
            **caption_meta,
        }
    doc = getattr(msg, "document", None)
    if doc:
        mime = str(getattr(doc, "mime_type", "") or "").lower()
        name = getattr(doc, "file_name", None) or ""
        lowered = name.lower()
        if mime.startswith("video/") or lowered.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v")):
            movie_name = name or f"video_{getattr(doc, 'file_unique_id', '') or uuid.uuid4().hex[:8]}.mp4"
            return {
                "kind": "document",
                "file_id": getattr(doc, "file_id", None),
                "file_unique_id": getattr(doc, "file_unique_id", None),
                "file_name": movie_name,
                "mime_type": getattr(doc, "mime_type", None),
                "duration_seconds": None,
                "file_size": getattr(doc, "file_size", None),
                "caption": caption,
                "storage_caption": storage_caption or None,
                **caption_meta,
            }
    return None


async def movie_upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    await safe_reply(
        update,
        MESSAGES[lang].get("movie_feature_removed", "🎬 Movie feature has been removed. Please use book search instead."),
    )


async def _process_movie_upload(
    context: ContextTypes.DEFAULT_TYPE,
    status_msg,
    media: dict,
    lang: str,
    uploader_user_id: int | None = None,
):
    try:
        no_status_edits = _env_bool("UPLOAD_NO_STATUS_EDITS", False)
        skip_name_duplicate_check = _env_bool("UPLOAD_SKIP_NAME_DUP_CHECK", False)

        video_channel_ids = _resolve_video_upload_channel_ids()
        if not video_channel_ids:
            logger.warning(
                "Movie upload rejected: no valid video channel configured (VIDEO_UPLOAD_CHANNEL_IDS=%r, VIDEO_UPLOAD_CHANNEL_ID=%r)",
                globals().get("VIDEO_UPLOAD_CHANNEL_IDS"),
                globals().get("VIDEO_UPLOAD_CHANNEL_ID"),
            )
            await _send_status_with_retry(
                status_msg,
                MESSAGES[lang].get(
                    "movie_upload_no_channel",
                    "⚠️ VIDEO_UPLOAD_CHANNEL_IDS/VIDEO_UPLOAD_CHANNEL_ID is not set. Please configure video channel IDs first.",
                ),
                reply_only=no_status_edits,
            )
            return
        video_channel_id = await _pick_video_upload_channel_id(context, video_channel_ids)
        if video_channel_id is None:
            await _send_status_with_retry(
                status_msg,
                MESSAGES[lang].get(
                    "movie_upload_no_channel",
                    "⚠️ VIDEO_UPLOAD_CHANNEL_IDS/VIDEO_UPLOAD_CHANNEL_ID is not set. Please configure video channel IDs first.",
                ),
                reply_only=no_status_edits,
            )
            return

        file_id = str(media.get("file_id") or "")
        file_unique_id = str(media.get("file_unique_id") or "").strip() or None
        file_name = str(media.get("file_name") or "video.mp4").strip() or "video.mp4"
        fallback_name_raw, _ = os.path.splitext(file_name)
        parsed_title = _normalize_title_candidate(str(media.get("parsed_title") or "").strip())
        if _is_bad_movie_title_candidate(parsed_title):
            parsed_title = ""
        fallback_name = _normalize_title_candidate(_humanize_movie_filename_stem(fallback_name_raw))
        if _is_bad_movie_title_candidate(fallback_name):
            fallback_name = ""
        movie_name_raw = parsed_title or fallback_name
        if not movie_name_raw:
            token = str(file_unique_id or uuid.uuid4().hex[:10])[:10]
            movie_name_raw = f"movie {token}"
        cleaned_name = clean_query(movie_name_raw)
        if not cleaned_name:
            cleaned_name = clean_query(_humanize_movie_filename_stem(fallback_name_raw))
            movie_name_raw = _humanize_movie_filename_stem(fallback_name_raw) or movie_name_raw
        display_label = movie_name_raw or file_name

        release_year = media.get("release_year")
        try:
            release_year = int(release_year) if release_year is not None else None
        except Exception:
            release_year = None
        genre = str(media.get("genre") or "").strip() or None
        movie_lang = str(media.get("movie_lang") or "").strip() or None
        country = str(media.get("country") or "").strip() or None
        rating = str(media.get("rating") or "").strip() or None
        caption_text = str(media.get("caption_text") or media.get("caption") or "").strip() or None
        search_text = str(media.get("search_text") or "").strip() or None
        if not search_text:
            search_text = _normalize_search_text(
                " ".join(
                    [
                        movie_name_raw,
                        str(release_year or ""),
                        genre or "",
                        movie_lang or "",
                        country or "",
                        rating or "",
                        caption_text or "",
                    ]
                )
            ) or None

        existing = await run_blocking(db_find_duplicate_movie, None, file_unique_id)
        if not existing and (not skip_name_duplicate_check) and cleaned_name and len(cleaned_name) > 3:
            existing = await run_blocking(db_find_duplicate_movie, cleaned_name, None)
        if existing:
            await _send_status_with_retry(
                status_msg,
                f"{MESSAGES[lang].get('movie_duplicate', MESSAGES[lang].get('duplicate', '⚠️ Duplicate ignored:'))} {display_label}",
                reply_only=no_status_edits,
            )
            return

        storage_caption = str(
            media.get("storage_caption")
            or media.get("caption_text")
            or media.get("caption")
            or ""
        ).strip()
        if storage_caption:
            storage_caption = _sanitize_caption_for_storage(storage_caption)
        kind = str(media.get("kind") or "").strip().lower()
        sent = None
        send_retry_max = 5
        for attempt in range(1, send_retry_max + 1):
            try:
                if kind == "video":
                    send_kwargs: dict[str, Any] = {
                        "chat_id": video_channel_id,
                        "video": file_id,
                    }
                    duration_seconds = media.get("duration_seconds")
                    try:
                        duration_int = int(duration_seconds) if duration_seconds is not None else None
                    except Exception:
                        duration_int = None
                    if duration_int and duration_int > 0:
                        send_kwargs["duration"] = duration_int
                    if storage_caption:
                        send_kwargs["caption"] = storage_caption
                    sent = await context.bot.send_video(**send_kwargs)
                else:
                    send_kwargs = {
                        "chat_id": video_channel_id,
                        "document": file_id,
                    }
                    if storage_caption:
                        send_kwargs["caption"] = storage_caption
                    sent = await context.bot.send_document(**send_kwargs)
                break
            except Exception as e:
                retry_after = getattr(e, "retry_after", None)
                if retry_after is not None and attempt < send_retry_max:
                    await asyncio.sleep(float(retry_after or 1) + 0.5)
                    continue
                transient_text = str(e).lower()
                transient = any(x in transient_text for x in ("timed out", "timeout", "network", "connection"))
                if transient and attempt < send_retry_max:
                    await asyncio.sleep(min(10.0, 0.5 * (2 ** (attempt - 1))))
                    continue
                logger.error("Failed to send movie to video channel %s: %s", video_channel_id, e)
                break

        if not sent:
            await _send_status_with_retry(
                status_msg,
                "❌ Failed to store the movie in video channel.",
                reply_only=no_status_edits,
            )
            return

        sent_media = getattr(sent, "document", None) or getattr(sent, "video", None)
        stored_file_id = getattr(sent_media, "file_id", None) or file_id
        stored_file_unique_id = getattr(sent_media, "file_unique_id", None) or file_unique_id
        duration_seconds = media.get("duration_seconds")
        if duration_seconds is None:
            duration_seconds = getattr(sent_media, "duration", None)

        movie = {
            "id": str(uuid.uuid4()),
            "movie_name": cleaned_name,
            "display_name": movie_name_raw,
            "file_id": stored_file_id,
            "file_unique_id": stored_file_unique_id,
            "path": None,
            "mime_type": media.get("mime_type"),
            "duration_seconds": duration_seconds,
            "file_size": media.get("file_size"),
            "channel_id": int(getattr(sent, "chat_id", video_channel_id) or video_channel_id),
            "channel_message_id": int(getattr(sent, "message_id", 0) or 0) or None,
            "release_year": release_year,
            "genre": genre,
            "movie_lang": movie_lang,
            "country": country,
            "rating": rating,
            "caption_text": caption_text,
            "search_text": search_text,
            "indexed": False,
            "uploaded_by_user_id": uploader_user_id,
            "upload_source": "user_upload",
        }
        inserted = await run_blocking(db_insert_movie, movie)
        if inserted is False:
            await _send_status_with_retry(
                status_msg,
                f"{MESSAGES[lang].get('movie_duplicate', MESSAGES[lang].get('duplicate', '⚠️ Duplicate ignored:'))} {display_label}",
                reply_only=no_status_edits,
            )
            return

        async def _index_uploaded_movie():
            try:
                if not es_available():
                    return
                index_movie_fn = globals().get("index_movie")
                if not callable(index_movie_fn):
                    return
                run_blocking_heavy_fn = globals().get("run_blocking_heavy")
                if callable(run_blocking_heavy_fn):
                    out_id = await run_blocking_heavy_fn(
                        index_movie_fn,
                        movie.get("movie_name"),
                        movie.get("file_id"),
                        movie.get("path"),
                        movie.get("id"),
                        movie.get("display_name"),
                        movie.get("file_unique_id"),
                        movie.get("mime_type"),
                        movie.get("duration_seconds"),
                        movie.get("file_size"),
                        movie.get("channel_id"),
                        movie.get("channel_message_id"),
                        movie.get("release_year"),
                        movie.get("genre"),
                        movie.get("movie_lang"),
                        movie.get("country"),
                        movie.get("rating"),
                        movie.get("caption_text"),
                        movie.get("search_text"),
                        True,
                        "false",
                    )
                else:
                    out_id = await run_blocking(
                        index_movie_fn,
                        movie.get("movie_name"),
                        movie.get("file_id"),
                        movie.get("path"),
                        movie.get("id"),
                        movie.get("display_name"),
                        movie.get("file_unique_id"),
                        movie.get("mime_type"),
                        movie.get("duration_seconds"),
                        movie.get("file_size"),
                        movie.get("channel_id"),
                        movie.get("channel_message_id"),
                        movie.get("release_year"),
                        movie.get("genre"),
                        movie.get("movie_lang"),
                        movie.get("country"),
                        movie.get("rating"),
                        movie.get("caption_text"),
                        movie.get("search_text"),
                        True,
                        "false",
                    )
                if out_id:
                    try:
                        await run_blocking(update_movie_indexed, movie.get("id"), True)
                    except Exception:
                        logger.exception("Failed to set indexed=True for movie_id=%s", movie.get("id"))
            except Exception as e:
                logger.error("Background movie index failed: %s", e, exc_info=True)

        try:
            context.application.create_task(_index_uploaded_movie())
        except Exception:
            logger.exception("Failed to schedule background movie indexing task")

        await _send_status_with_retry(
            status_msg,
            f"{MESSAGES[lang].get('movie_saved', MESSAGES[lang].get('saved', '✅ Saved:'))} {display_label}",
            reply_only=no_status_edits,
        )
    except Exception as e:
        logger.error("Movie upload failed: %s", e, exc_info=True)
        no_status_edits = _env_bool("UPLOAD_NO_STATUS_EDITS", False)
        await _send_status_with_retry(
            status_msg,
            f"❌ Movie upload failed: {str(e)[:100]}",
            reply_only=no_status_edits,
        )


async def _start_movie_upload_from_media(update: Update, context: ContextTypes.DEFAULT_TYPE, media: dict, lang: str):
    no_status_edits = _env_bool("UPLOAD_NO_STATUS_EDITS", False)
    if no_status_edits:
        status_msg = update.message
    else:
        status_msg = await _send_with_retry(
            lambda: update.message.reply_text(MESSAGES[lang].get("upload_processing", "⏳ Processing..."))
        )
    context.application.create_task(
        _process_movie_upload(
            context,
            status_msg,
            media,
            lang,
            uploader_user_id=update.effective_user.id if update.effective_user else None,
        )
    )


async def handle_movie_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return
        media = _movie_media_payload_from_message(update.message)
        if not media:
            return
        if _get_user_upload_mode(context) != _UPLOAD_MODE_MOVIE:
            return
        lang = ensure_user_language(update, context)
        user_id = update.effective_user.id if update.effective_user else None
        if user_id and is_blocked(user_id):
            await update.message.reply_text(MESSAGES[lang]["blocked"])
            raise ApplicationHandlerStop()
        if user_id and await is_stopped_user(user_id):
            raise ApplicationHandlerStop()
        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
            raise ApplicationHandlerStop()
        await update_user_info(update, context)

        if user_id and is_allowed(user_id):
            await _start_movie_upload_from_media(update, context, media, lang)
            raise ApplicationHandlerStop()
        await update.message.reply_text(MESSAGES[lang]["upload_inactive"])
        raise ApplicationHandlerStop()
    except ApplicationHandlerStop:
        raise
    except Exception as e:
        logger.error(f"handle_movie_video failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        await safe_reply(update, MESSAGES[lang]["error"])
        raise

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

        # add new book with permanent UUID + indexed flag
        new_book = {
            "id": str(uuid.uuid4()),   # permanent ID
            "book_name": cleaned_name,  # normalized for search
            "display_name": book_name,
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
        # User-facing confirmation: send only once when DB save succeeds.
        await _send_status_with_retry(
            status_msg,
            f"{MESSAGES[lang]['saved']} {file_name}",
            reply_only=no_status_edits,
        )

        async def _fanout_to_channels():
            if not UPLOAD_CHANNEL_IDS:
                return
            try:
                await enqueue_upload_fanout(context.application, file_id, book_id=new_book["id"])
            except Exception as e:
                logger.error(f"Failed to enqueue upload fanout: {e}")

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

        # fan out to upload channels (background)
        context.application.create_task(_fanout_to_channels())

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

        user_mode = _get_user_upload_mode(context)
        logger.info(
            "handle_file: user_mode=%s, user_id=%s, is_allowed=%s",
            user_mode or "none",
            update.effective_user.id if update.effective_user else "N/A",
            is_allowed(update.effective_user.id) if update.effective_user else "N/A",
        )
        doc = update.message.document
        doc_mime = str(getattr(doc, "mime_type", "") or "").lower() if doc else ""
        doc_name = str(getattr(doc, "file_name", "") or "").lower() if doc else ""
        looks_like_video_doc = bool(
            doc and (
                doc_mime.startswith("video/")
                or doc_name.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"))
            )
        )

        if user_mode == _UPLOAD_MODE_MOVIE and is_allowed(update.effective_user.id) and looks_like_video_doc:
            media = _movie_media_payload_from_message(update.message)
            if media:
                await _start_movie_upload_from_media(update, context, media, lang)
                return

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
            file = update.message.document
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


def sync_unindexed_movies():
    logger.debug("Movie sync skipped: movie feature removed.")
        
# --- Audit command ---
