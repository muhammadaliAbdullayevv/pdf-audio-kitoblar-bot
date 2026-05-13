from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, InputMediaDocument, Update
from telegram.ext import ContextTypes
from book_thumbnail import get_book_thumbnail_input, get_book_thumbnail_payload
from language import get_language_keyboard
import safe_subprocess

try:
    from telegram import ReactionTypeEmoji
except Exception:
    ReactionTypeEmoji = None  # type: ignore

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)

# Import Redis cache for performance
try:
    from cache import cache_get, cache_set, cache_delete, cache_clear_pattern
    REDIS_CACHE_AVAILABLE = True
except ImportError:
    REDIS_CACHE_AVAILABLE = False
    cache_get = lambda k: None
    cache_set = lambda k, v, ttl=300: False
    cache_delete = lambda k: False
    cache_clear_pattern = lambda p: 0

def _ttl_value(name: str, default: int, minimum: int = 1) -> int:
    try:
        raw = globals().get(name, None)
        if raw is None:
            raw = os.getenv(name, str(default))
        return max(minimum, int(raw))
    except Exception:
        return max(minimum, int(default))


def _env_int(name: str, default: int = 0) -> int:
    try:
        raw = globals().get(name, None)
        if raw is None:
            raw = os.getenv(name, str(default))
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float = 0.0) -> float:
    try:
        raw = globals().get(name, None)
        if raw is None:
            raw = os.getenv(name, str(default))
        return float(str(raw).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    try:
        raw = globals().get(name, None)
        if raw is None:
            raw = os.getenv(name, "1" if default else "0")
        if isinstance(raw, bool):
            return raw
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)
    except Exception:
        return bool(default)


def _audiobook_ffmpeg_time(seconds: float) -> str:
    total_ms = max(0, int(round(float(seconds) * 1000)))
    hours, rem = divmod(total_ms, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    secs, millis = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"


def _audio_conv_transform_blocking(
    input_bytes: bytes,
    *,
    output_mode: str,
    start_s: float | None = None,
    end_s: float | None = None,
) -> bytes:
    if not input_bytes:
        raise RuntimeError("empty input")
    if output_mode not in {"mp3", "voice"}:
        raise RuntimeError("unsupported output mode")

    with tempfile.TemporaryDirectory(prefix="audioconv_") as td:
        in_path = os.path.join(td, "input.bin")
        out_path = os.path.join(td, "output.mp3" if output_mode == "mp3" else "output.ogg")
        with open(in_path, "wb") as fp:
            fp.write(input_bytes)

        cmd = ["ffmpeg", "-y", "-i", in_path]
        if start_s is not None:
            cmd[2:2] = ["-ss", _audiobook_ffmpeg_time(float(start_s))]
        if end_s is not None:
            duration = max(0.01, float(end_s) - float(start_s or 0.0))
            cmd.extend(["-t", _audiobook_ffmpeg_time(duration)])
        cmd.extend(["-vn"])
        if output_mode == "voice":
            cmd.extend(["-ac", "1", "-ar", "48000", "-c:a", "libopus", "-b:a", "32k", out_path])
        else:
            cmd.extend(["-c:a", "libmp3lame", "-q:a", "4", out_path])

        timeout_s = float(os.getenv("AUDIO_CONVERTER_FFMPEG_TIMEOUT_S", "150") or "150")
        proc = safe_subprocess.run(cmd, timeout_s=timeout_s, max_output_chars=8000, text=False)
        if proc.returncode != 0:
            err = proc.stderr
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            raise RuntimeError((str(err or "ffmpeg failed")).strip()[-800:])
        if not os.path.exists(out_path):
            raise RuntimeError("ffmpeg output missing")
        with open(out_path, "rb") as fp:
            return fp.read()


def _audio_conv_apply_cover_blocking(audio_bytes: bytes, cover_bytes: bytes) -> bytes:
    if not audio_bytes:
        raise RuntimeError("empty audio")
    if not cover_bytes:
        raise RuntimeError("empty cover")

    with tempfile.TemporaryDirectory(prefix="audiocover_") as td:
        in_audio = os.path.join(td, "in_audio.mp3")
        in_cover = os.path.join(td, "cover.img")
        out_mp3 = os.path.join(td, "out_with_cover.mp3")

        with open(in_audio, "wb") as fp:
            fp.write(audio_bytes)
        with open(in_cover, "wb") as fp:
            fp.write(cover_bytes)

        timeout_s = float(os.getenv("AUDIO_CONVERTER_FFMPEG_TIMEOUT_S", "150") or "150")
        cmd_copy = [
            "ffmpeg", "-y", "-i", in_audio, "-i", in_cover,
            "-map", "0:a", "-map", "1:v",
            "-c:a", "copy", "-c:v", "mjpeg",
            "-id3v2_version", "3",
            "-metadata:s:v", "title=Album cover",
            "-metadata:s:v", "comment=Cover (front)",
            out_mp3,
        ]
        proc = safe_subprocess.run(cmd_copy, timeout_s=timeout_s, max_output_chars=8000, text=False)
        if proc.returncode != 0:
            cmd_reencode = [
                "ffmpeg", "-y", "-i", in_audio, "-i", in_cover,
                "-map", "0:a", "-map", "1:v",
                "-c:a", "libmp3lame", "-q:a", "4", "-c:v", "mjpeg",
                "-id3v2_version", "3",
                "-metadata:s:v", "title=Album cover",
                "-metadata:s:v", "comment=Cover (front)",
                out_mp3,
            ]
            proc = safe_subprocess.run(cmd_reencode, timeout_s=timeout_s, max_output_chars=8000, text=False)
        if proc.returncode != 0:
            err = proc.stderr
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            raise RuntimeError((str(err or "ffmpeg cover failed")).strip()[-800:])
        if not os.path.exists(out_mp3):
            raise RuntimeError("ffmpeg cover output missing")
        with open(out_mp3, "rb") as fp:
            return fp.read()


def _search_cache_namespace() -> str:
    return str(globals().get("SEARCH_CACHE_NS", os.getenv("SEARCH_CACHE_NS", "v1")) or "v1")


_AUDIOBOOK_LOCAL_DIR = Path(
    os.getenv(
        "AUDIOBOOK_LOCAL_DIR",
        str(Path(__file__).resolve().parent / "downloads" / "localaudiobooks"),
    )
)
_AUDIOBOOK_AUTO_DOWNLOAD_LOCAL = _env_bool(
    "AUDIOBOOK_AUTO_DOWNLOAD_LOCAL",
    _env_bool("UPLOAD_AUTO_DOWNLOAD_LOCAL", True),
)
_AUDIOBOOK_LOCAL_REFRESH_FILE_ID = _env_bool("AUDIOBOOK_LOCAL_REFRESH_FILE_ID", True)
_AUDIOBOOK_LOCAL_WORKER_COUNT = max(
    1,
    _env_int("AUDIOBOOK_LOCAL_WORKER_COUNT", _env_int("UPLOAD_LOCAL_WORKER_COUNT", 2)),
)
_AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES = max(
    1,
    _env_int("AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES", _env_int("UPLOAD_LOCAL_DOWNLOAD_RETRIES", 6)),
)
_AUDIOBOOK_LOCAL_RETRY_BASE_DELAY_SEC = max(
    0.5,
    _env_float("AUDIOBOOK_LOCAL_RETRY_BASE_DELAY_SEC", _env_float("UPLOAD_LOCAL_RETRY_BASE_DELAY_SEC", 2.0)),
)
_AUDIOBOOK_LOCAL_RETRY_MIN_DELAY_SEC = max(
    1.0,
    _env_float("AUDIOBOOK_LOCAL_RETRY_MIN_DELAY_SEC", _env_float("UPLOAD_LOCAL_RETRY_MIN_DELAY_SEC", 10.0)),
)
_AUDIOBOOK_LOCAL_WORKER_POLL_SECONDS = max(
    0.5,
    _env_float("AUDIOBOOK_LOCAL_WORKER_POLL_SECONDS", _env_float("UPLOAD_LOCAL_WORKER_POLL_SECONDS", 1.0)),
)
_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS = max(
    0.0,
    _env_float("AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS", _env_float("UPLOAD_LOCAL_JOB_COOLDOWN_SECONDS", 0.1)),
)
_AUDIOBOOK_LOCAL_JOB_STALE_AFTER_SECONDS = max(
    60,
    _env_int("AUDIOBOOK_LOCAL_JOB_STALE_AFTER_SECONDS", _env_int("UPLOAD_LOCAL_JOB_STALE_AFTER_SECONDS", 3600)),
)
_AUDIOBOOK_LOCAL_GET_FILE_READ_TIMEOUT_SEC = max(
    60.0,
    _env_float("AUDIOBOOK_LOCAL_GET_FILE_READ_TIMEOUT_SEC", _env_float("UPLOAD_LOCAL_GET_FILE_READ_TIMEOUT_SEC", 300.0)),
)
_AUDIOBOOK_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC = max(
    120.0,
    _env_float("AUDIOBOOK_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC", _env_float("UPLOAD_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC", 600.0)),
)
_AUDIOBOOK_LOCAL_SEND_READ_TIMEOUT_SEC = _env_float("AUDIOBOOK_LOCAL_SEND_READ_TIMEOUT_SEC", 0.0)
_AUDIOBOOK_LOCAL_SEND_WRITE_TIMEOUT_SEC = _env_float("AUDIOBOOK_LOCAL_SEND_WRITE_TIMEOUT_SEC", 0.0)
_AUDIOBOOK_LOCAL_CONNECT_TIMEOUT_SEC = max(
    10.0,
    _env_float("AUDIOBOOK_LOCAL_CONNECT_TIMEOUT_SEC", _env_float("UPLOAD_LOCAL_CONNECT_TIMEOUT_SEC", 45.0)),
)
_AUDIOBOOK_LOCAL_POOL_TIMEOUT_SEC = max(
    10.0,
    _env_float("AUDIOBOOK_LOCAL_POOL_TIMEOUT_SEC", _env_float("UPLOAD_LOCAL_POOL_TIMEOUT_SEC", 45.0)),
)
_AUDIOBOOK_LOCAL_SEND_MIN_INTERVAL_SEC = max(
    0.0,
    _env_float("AUDIOBOOK_LOCAL_SEND_MIN_INTERVAL_SEC", _env_float("AUDIO_UPLOAD_SEND_DELAY_SEC", 1.0)),
)
_AUDIOBOOK_LOCAL_WORKER_KEY = "audiobook_local_backup_workers"


def _query_fingerprint(query: str) -> str:
    text = str(query or "").strip().lower()
    if not text:
        return "empty"
    norm_fn = globals().get("normalize")
    if callable(norm_fn):
        try:
            normalized = str(norm_fn(text)).strip().lower()
            if normalized:
                text = normalized
        except Exception:
            pass
    text = re.sub(r"\s+", " ", text).strip()
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _book_search_entries_key(query: str) -> str:
    ns = _search_cache_namespace()
    return f"search:books:entries:{ns}:{_query_fingerprint(query)}"


def get_cached_book_search_entries(query: str) -> list[dict[str, str]] | None:
    if not REDIS_CACHE_AVAILABLE:
        return None
    payload = cache_get(_book_search_entries_key(query))
    if isinstance(payload, dict):
        payload = payload.get("entries")
    if not isinstance(payload, list):
        return None
    entries: list[dict[str, str]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        rid = str(row.get("id") or "").strip()
        title = str(row.get("title") or "").strip()
        if rid and title:
            entries.append({"id": rid, "title": title})
    return entries or None


def set_cached_book_search_entries(query: str, entries: list[dict[str, str]]) -> None:
    if not REDIS_CACHE_AVAILABLE or not entries:
        return
    ttl = _ttl_value("BOOK_SEARCH_RESULT_CACHE_TTL", 120, minimum=10)
    cache_set(_book_search_entries_key(query), {"entries": entries}, ttl=ttl)


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


def _normalize_audiobook_part_title(raw_name: str | None, part_index: int | None = None) -> str:
    """
    Normalize audiobook part titles using the project's existing book normalizer.
    Falls back to a safe lowercase stem if the normalizer is unavailable.
    """
    candidate = str(raw_name or "").strip()
    if candidate:
        candidate = Path(candidate).stem or candidate
    else:
        candidate = f"Part {part_index or 1}"

    norm_fn = globals().get("normalize")
    if callable(norm_fn):
        try:
            normalized = str(norm_fn(candidate)).strip()
            if normalized:
                return normalized
        except Exception:
            pass

    candidate = re.sub(r"\s+", " ", candidate).strip().lower()
    return candidate or f"part {part_index or 1}"


def _normalize_audiobook_folder_title(raw_name: str | None, fallback_id: str | None = None) -> str:
    """
    Normalize the audiobook folder name to match the book layout style.
    """
    candidate = str(raw_name or "").strip()
    if candidate:
        candidate = Path(candidate).stem or candidate
    else:
        candidate = str(fallback_id or "audiobook").strip()

    norm_fn = globals().get("normalize")
    if callable(norm_fn):
        try:
            normalized = str(norm_fn(candidate)).strip()
            if normalized:
                return normalized
        except Exception:
            pass
    candidate = re.sub(r"\s+", " ", candidate).strip().lower()
    return candidate or str(fallback_id or "audiobook").strip().lower() or "audiobook"


def _safe_asyncio_current_task():
    try:
        return asyncio.current_task()
    except Exception:
        return None


_ABOOK_ADD_FLOW_LOCKS: dict[str, asyncio.Lock] = {}


def _get_abook_add_flow_lock(audio_book_id: str, user_id: int | None = None) -> asyncio.Lock:
    key = f"{str(audio_book_id or '').strip()}:{int(user_id or 0)}"
    lock = _ABOOK_ADD_FLOW_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _ABOOK_ADD_FLOW_LOCKS[key] = lock
    return lock


_AUDIOBOOK_LOCAL_ACTIVITY_KEY = "audiobook_local_backup_activity"


def _set_audiobook_local_activity(app, worker_id: str, **fields: Any) -> None:
    try:
        bot_data = getattr(app, "bot_data", None)
        if not isinstance(bot_data, dict):
            return
        state = bot_data.setdefault(_AUDIOBOOK_LOCAL_ACTIVITY_KEY, {})
        if not isinstance(state, dict):
            state = {}
            bot_data[_AUDIOBOOK_LOCAL_ACTIVITY_KEY] = state
        payload = dict(fields)
        payload["updated_at"] = time.time()
        state[str(worker_id or "worker")] = payload
    except Exception:
        pass


def _clear_audiobook_local_activity(app, worker_id: str) -> None:
    try:
        bot_data = getattr(app, "bot_data", None)
        state = bot_data.get(_AUDIOBOOK_LOCAL_ACTIVITY_KEY) if isinstance(bot_data, dict) else None
        if isinstance(state, dict):
            state.pop(str(worker_id or "worker"), None)
            if not state:
                bot_data.pop(_AUDIOBOOK_LOCAL_ACTIVITY_KEY, None)
    except Exception:
        pass


def _audiobook_storage_clean_title(value: str | None, default: str = "audio") -> str:
    text = Path(str(value or "").strip()).stem
    if not text:
        return default
    text = text.replace("'", "ʻ").replace("’", "ʻ").replace("ʼ", "ʻ").replace("`", "ʻ")
    text = re.sub(r'@[\w_]+', ' ', text)
    text = re.sub(r'https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+', ' ', text, flags=re.IGNORECASE)
    text = text.replace("_", " ")
    text = re.sub(r"[\x00-\x1f\x7f/\\<>:*?\"|]+", " ", text)
    text = re.sub(r"[^\w\sʻ().,\-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip(" .-_")
    return (text[:160] or default)


def _audiobook_original_extension(part: dict, fallback_name: str | None = None) -> str:
    for candidate in (
        fallback_name,
        part.get("file_name"),
        part.get("title"),
        part.get("path"),
    ):
        suffix = Path(str(candidate or "").strip()).suffix.strip().lower()
        if suffix:
            return suffix
    kind = str(part.get("media_kind") or "").strip().lower()
    if kind == "voice":
        return ".ogg"
    return ".mp3"


def _audiobook_local_filename(part: dict, ext: str) -> str:
    try:
        part_index = int(part.get("part_index") or 0)
    except Exception:
        part_index = 0
    base_title = _audiobook_storage_clean_title(
        part.get("title") or part.get("file_name"),
        default=f"Part {part_index or 1}",
    )
    return f"{base_title}{ext}"


def _audiobook_local_target_path(audio_book: dict | None, book: dict | None, part: dict, ext: str) -> Path:
    folder_title = _audiobook_storage_clean_title(
        (book or {}).get("display_name")
        or (book or {}).get("book_name")
        or (audio_book or {}).get("display_title")
        or (audio_book or {}).get("title")
        or "audiobook",
        default="audiobook",
    )
    audio_book_id = str((audio_book or {}).get("id") or part.get("audio_book_id") or "").strip()
    if audio_book_id:
        folder_title = f"{folder_title}__{audio_book_id}"
    return _AUDIOBOOK_LOCAL_DIR / folder_title / _audiobook_local_filename(part, ext)


def _audiobook_send_title(part: dict, local_path: str | None = None) -> str | None:
    fallback = f"Part {part.get('part_index', 0)}"
    base_title = str(part.get("title") or "").strip()
    if not base_title and local_path:
        base_title = Path(local_path).stem.strip()
    normalized_title = _audiobook_storage_clean_title(base_title or fallback, default=fallback)
    return normalized_title or None


def _delete_local_audiobook_paths(paths: list[str] | tuple[str, ...]) -> dict[str, int]:
    deleted = 0
    failed = 0
    parent_dirs: set[Path] = set()
    unique_paths = []
    seen: set[str] = set()
    for raw in paths or []:
        path_value = str(raw or "").strip()
        if not path_value or path_value in seen:
            continue
        seen.add(path_value)
        unique_paths.append(path_value)

    for path_value in unique_paths:
        path_obj = Path(path_value)
        parent_dirs.add(path_obj.parent)
        for candidate in (path_obj, path_obj.with_name(path_obj.name + ".part")):
            if not candidate.exists():
                continue
            try:
                if candidate.is_file():
                    candidate.unlink()
                    deleted += 1
                elif candidate.is_dir():
                    failed += 1
                    logger.warning("Expected audiobook local file but found directory: %s", candidate)
            except Exception as e:
                failed += 1
                logger.warning("Failed to delete audiobook local path %s: %s", candidate, e, exc_info=True)

    for folder in sorted(parent_dirs, key=lambda item: len(item.parts), reverse=True):
        current = folder
        while True:
            try:
                if current == _AUDIOBOOK_LOCAL_DIR or current == _AUDIOBOOK_LOCAL_DIR.parent:
                    break
            except Exception:
                break
            try:
                current.rmdir()
            except OSError:
                break
            except Exception as e:
                logger.debug("Failed to remove empty audiobook folder %s: %s", current, e)
                break
            current = current.parent

    return {"deleted": deleted, "failed": failed}


async def _audiobook_local_retry(call, *, desc: str):
    delay = _AUDIOBOOK_LOCAL_RETRY_BASE_DELAY_SEC
    for attempt in range(1, _AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES + 1):
        try:
            return await call()
        except Exception as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None and attempt < _AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES:
                sleep_for = max(1.0, float(retry_after or 1.0)) + 0.5
                logger.warning(
                    "%s transient error (%s), retrying in %.1fs (attempt %s/%s)",
                    desc,
                    e,
                    sleep_for,
                    attempt,
                    _AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES,
                )
                await asyncio.sleep(sleep_for)
                continue
            msg_text = str(e).lower()
            transient = any(
                marker in msg_text
                for marker in ("timed out", "timeout", "network", "connection reset", "temporary failure")
            )
            if transient and attempt < _AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES:
                logger.warning(
                    "%s transient error (%s), retrying in %.1fs (attempt %s/%s)",
                    desc,
                    e,
                    delay,
                    attempt,
                    _AUDIOBOOK_LOCAL_DOWNLOAD_RETRIES,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            raise


def _resolve_audiobook_refresh_channel_id(part: dict | None = None) -> int:
    try:
        stored_channel_id = int((part or {}).get("channel_id") or 0)
    except Exception:
        stored_channel_id = 0
    if stored_channel_id:
        return stored_channel_id
    audio_channel_ids = _resolve_audio_upload_channel_ids()
    if audio_channel_ids:
        return int(audio_channel_ids[0])
    candidates = [
        globals().get("BOOK_STORAGE_CHANNEL_ID"),
        os.getenv("BOOK_STORAGE_CHANNEL_ID", ""),
        globals().get("AUDIO_UPLOAD_CHANNEL_ID"),
        os.getenv("AUDIO_UPLOAD_CHANNEL_ID", ""),
        os.getenv("TELEGRAM_OWNER_ID", ""),
        -1003970604636,
    ]
    for raw in candidates:
        try:
            value = int(str(raw or "").strip())
        except Exception:
            value = 0
        if value:
            return value
    return 0


async def _download_audiobook_source_bytes(bot, file_id: str, part_id: str) -> bytes:
    get_file_kwargs = {
        "read_timeout": _AUDIOBOOK_LOCAL_GET_FILE_READ_TIMEOUT_SEC,
        "connect_timeout": _AUDIOBOOK_LOCAL_CONNECT_TIMEOUT_SEC,
        "pool_timeout": _AUDIOBOOK_LOCAL_POOL_TIMEOUT_SEC,
    }
    tg_file = await _audiobook_local_retry(
        lambda: bot.get_file(file_id, **get_file_kwargs),
        desc=f"audiobook get_file {part_id or file_id}",
    )
    download_kwargs = {
        "read_timeout": _AUDIOBOOK_LOCAL_DOWNLOAD_READ_TIMEOUT_SEC,
        "connect_timeout": _AUDIOBOOK_LOCAL_CONNECT_TIMEOUT_SEC,
        "pool_timeout": _AUDIOBOOK_LOCAL_POOL_TIMEOUT_SEC,
    }
    payload = await _audiobook_local_retry(
        lambda: tg_file.download_as_bytearray(**download_kwargs),
        desc=f"audiobook download {part_id or file_id}",
    )
    return bytes(payload or b"")


async def _refresh_audiobook_part_file_id(
    bot,
    part: dict,
    local_path: str,
    *,
    app=None,
    telegram_filename: str | None = None,
    title: str | None = None,
    media_kind: str | None = None,
    duration_seconds: int | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "file_id": None,
        "file_unique_id": None,
        "channel_id": None,
        "channel_message_id": None,
        "media_kind": None,
        "duration_seconds": duration_seconds,
        "error": None,
    }
    if not _AUDIOBOOK_LOCAL_REFRESH_FILE_ID:
        result["error"] = "refresh disabled"
        return result

    storage_channel_id = _resolve_audiobook_refresh_channel_id(part)
    if storage_channel_id == 0:
        result["error"] = "missing audiobook storage channel"
        return result

    path_obj = Path(str(local_path or "").strip())
    if not path_obj.exists():
        result["error"] = "missing local path"
        return result

    resolved_media_kind = str(media_kind or part.get("media_kind") or _audiobook_part_media_kind(part)).strip().lower()
    if resolved_media_kind not in {"audio", "voice", "document"}:
        resolved_media_kind = "audio" if path_obj.suffix.lower() in {".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".oga", ".opus"} else "document"
    resolved_title = str(title or part.get("title") or path_obj.stem or "").strip() or None
    resolved_filename = str(telegram_filename or path_obj.name or "audio.mp3").strip() or path_obj.name
    read_timeout = _AUDIOBOOK_LOCAL_SEND_READ_TIMEOUT_SEC if _AUDIOBOOK_LOCAL_SEND_READ_TIMEOUT_SEC > 0 else None
    write_timeout = _AUDIOBOOK_LOCAL_SEND_WRITE_TIMEOUT_SEC if _AUDIOBOOK_LOCAL_SEND_WRITE_TIMEOUT_SEC > 0 else None
    send_kwargs = {
        "read_timeout": read_timeout,
        "write_timeout": write_timeout,
        "connect_timeout": _AUDIOBOOK_LOCAL_CONNECT_TIMEOUT_SEC,
        "pool_timeout": _AUDIOBOOK_LOCAL_POOL_TIMEOUT_SEC,
        "disable_notification": True,
    }
    channel_lock, channel_state = _get_audio_channel_send_guard_for_app(app, int(storage_channel_id))

    async def _send_with_channel_guard(sender, desc: str):
        try:
            if channel_lock is not None:
                async with channel_lock:
                    if isinstance(channel_state, dict):
                        now_ts = asyncio.get_running_loop().time()
                        next_allowed_at = float(channel_state.get("next_allowed_at", 0.0) or 0.0)
                        if next_allowed_at > now_ts:
                            await asyncio.sleep(next_allowed_at - now_ts)
                    sent_message = await sender()
                    if isinstance(channel_state, dict) and _AUDIOBOOK_LOCAL_SEND_MIN_INTERVAL_SEC > 0:
                        channel_state["next_allowed_at"] = asyncio.get_running_loop().time() + _AUDIOBOOK_LOCAL_SEND_MIN_INTERVAL_SEC
                    return sent_message
            return await sender()
        except Exception as e:
            logger.warning("%s failed without retry to avoid duplicate uploads: %s", desc, e)
            raise

    try:
        if resolved_media_kind == "audio":
            async def _send_audio():
                with open(path_obj, "rb") as fh:
                    return await bot.send_audio(
                        chat_id=storage_channel_id,
                        audio=InputFile(fh, filename=resolved_filename),
                        title=resolved_title,
                        duration=duration_seconds,
                        thumbnail=get_book_thumbnail_input(),
                        **send_kwargs,
                    )

            sent = await _send_with_channel_guard(
                _send_audio,
                desc=f"audiobook refresh audio {part.get('id') or ''}".strip(),
            )
            media = getattr(sent, "audio", None)
        elif resolved_media_kind == "voice":
            async def _send_voice():
                with open(path_obj, "rb") as fh:
                    return await bot.send_voice(
                        chat_id=storage_channel_id,
                        voice=InputFile(fh, filename=resolved_filename),
                        duration=duration_seconds,
                        **send_kwargs,
                    )

            sent = await _send_with_channel_guard(
                _send_voice,
                desc=f"audiobook refresh voice {part.get('id') or ''}".strip(),
            )
            media = getattr(sent, "voice", None)
        else:
            async def _send_document():
                with open(path_obj, "rb") as fh:
                    return await bot.send_document(
                        chat_id=storage_channel_id,
                        document=InputFile(fh, filename=resolved_filename),
                        thumbnail=get_book_thumbnail_input(),
                        **send_kwargs,
                    )

            sent = await _send_with_channel_guard(
                _send_document,
                desc=f"audiobook refresh document {part.get('id') or ''}".strip(),
            )
            media = getattr(sent, "document", None)

        file_id = str(getattr(media, "file_id", "") or "").strip()
        if not file_id:
            result["error"] = "Telegram did not return refreshed file_id"
            return result
        result["file_id"] = file_id
        result["file_unique_id"] = str(getattr(media, "file_unique_id", "") or "").strip() or None
        result["channel_id"] = int(storage_channel_id)
        result["channel_message_id"] = int(getattr(sent, "message_id", 0) or 0) or None
        result["media_kind"] = resolved_media_kind
        if result["duration_seconds"] is None:
            try:
                duration_value = int(getattr(media, "duration", 0) or 0)
            except Exception:
                duration_value = 0
            if duration_value > 0:
                result["duration_seconds"] = duration_value
        return result
    except Exception as e:
        result["error"] = str(e)
        logger.warning(
            "Failed to refresh audiobook part file_id for %s from %s via channel %s: %s",
            part.get("id"),
            local_path,
            storage_channel_id,
            e,
            exc_info=True,
        )
        return result


async def _save_audiobook_part_local(
    bot,
    audio_book: dict | None,
    book: dict | None,
    part: dict,
    *,
    app=None,
    status_update=None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "part_id": str(part.get("id") or ""),
        "audio_book_id": str((audio_book or {}).get("id") or part.get("audio_book_id") or ""),
        "path": None,
        "source_kind": "missing",
        "media_kind": None,
        "duration_seconds": None,
        "error": None,
        "db_updated": False,
        "file_id_refreshed": False,
    }
    part_id = result["part_id"]
    file_id = str(part.get("file_id") or "").strip()
    if not part_id or not file_id:
        result["error"] = "missing audiobook part_id or file_id"
        return result

    try:
        _AUDIOBOOK_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        source_ext = _audiobook_original_extension(part, part.get("title"))
        try:
            source_duration = int(part.get("duration_seconds") or 0) or None
        except Exception:
            source_duration = None
        clean_title = _audiobook_storage_clean_title(
            part.get("title") or part.get("file_name"),
            default=f"Part {part.get('part_index') or 1}",
        )
        target_path = _audiobook_local_target_path(audio_book, book, part, ".mp3")
        output_media_kind = "audio"

        if target_path.exists() and target_path.is_file() and target_path.stat().st_size > 0:
            if callable(status_update):
                status_update(stage="reusing local audio", title=clean_title)
            result["path"] = str(target_path)
            result["source_kind"] = "reused-existing"
            result["media_kind"] = output_media_kind
            result["duration_seconds"] = source_duration
        else:
            if callable(status_update):
                status_update(stage="downloading source audio", title=clean_title)
            source_bytes = await _download_audiobook_source_bytes(bot, file_id, part_id)
            if not source_bytes:
                result["error"] = "empty source audio"
                return result

            output_bytes = source_bytes
            output_ext = source_ext
            output_media_kind = _audiobook_part_media_kind(part)
            try:
                if callable(status_update):
                    status_update(stage="converting to mp3", title=clean_title)
                output_bytes = await run_blocking(
                    _audio_conv_transform_blocking,
                    source_bytes,
                    output_mode="mp3",
                )
                output_ext = ".mp3"
                output_media_kind = "audio"
                result["source_kind"] = "telegram-file-mp3"
            except Exception as e:
                result["source_kind"] = "telegram-file-original"
                logger.warning(
                    "Audiobook MP3 transform failed for %s; keeping original media: %s",
                    part_id,
                    e,
                    exc_info=True,
                )

            if output_media_kind == "audio":
                cover_payload = get_book_thumbnail_payload()
                if cover_payload:
                    try:
                        if callable(status_update):
                            status_update(stage="embedding cover image", title=clean_title)
                        cover_bytes, _ = cover_payload
                        output_bytes = await run_blocking(_audio_conv_apply_cover_blocking, output_bytes, cover_bytes)
                        result["source_kind"] = "telegram-file-covered-mp3"
                    except Exception as e:
                        logger.warning(
                            "Audiobook cover apply failed for %s; continuing without embedded cover: %s",
                            part_id,
                            e,
                            exc_info=True,
                        )

            target_path = _audiobook_local_target_path(audio_book, book, part, output_ext)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target_path.with_name(target_path.name + ".part")
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            if callable(status_update):
                status_update(stage="saving local audio", title=clean_title)
            await asyncio.to_thread(tmp_path.write_bytes, output_bytes)
            await asyncio.to_thread(tmp_path.replace, target_path)
            result["path"] = str(target_path)
            result["media_kind"] = output_media_kind
            result["duration_seconds"] = source_duration

        await run_blocking(
            update_audio_book_part_media,
            part_id,
            path=str(result["path"]),
            title=clean_title,
            media_kind=str(result["media_kind"] or output_media_kind),
            duration_seconds=result["duration_seconds"],
        )
        result["db_updated"] = True

        if result["path"] and _AUDIOBOOK_LOCAL_REFRESH_FILE_ID:
            if callable(status_update):
                status_update(stage="uploading to storage channel", title=clean_title)
            refreshed_part = dict(part or {})
            refreshed_part["path"] = result["path"]
            refreshed_part["media_kind"] = str(result["media_kind"] or output_media_kind)
            refresh_info = await _refresh_audiobook_part_file_id(
                bot,
                refreshed_part,
                str(result["path"]),
                app=app,
                telegram_filename=Path(str(result["path"])).name,
                title=clean_title,
                media_kind=str(result["media_kind"] or output_media_kind),
                duration_seconds=result["duration_seconds"],
            )
            if refresh_info.get("file_id"):
                await run_blocking(
                    update_audio_book_part_media,
                    part_id,
                    file_id=str(refresh_info.get("file_id") or ""),
                    file_unique_id=str(refresh_info.get("file_unique_id") or "").strip() or None,
                    path=str(result["path"]),
                    title=clean_title,
                    media_kind=str(refresh_info.get("media_kind") or result["media_kind"] or output_media_kind),
                    duration_seconds=refresh_info.get("duration_seconds"),
                    channel_id=refresh_info.get("channel_id"),
                    channel_message_id=refresh_info.get("channel_message_id"),
                )
                result["file_id_refreshed"] = True
                result["media_kind"] = str(refresh_info.get("media_kind") or result["media_kind"] or output_media_kind)
                result["duration_seconds"] = refresh_info.get("duration_seconds") or result["duration_seconds"]
            elif not result["error"]:
                result["error"] = f"file_id refresh failed: {refresh_info.get('error') or 'unknown error'}"

        logger.info(
            "Saved audiobook part locally: part_id=%s audio_book_id=%s path=%s",
            part_id,
            result["audio_book_id"],
            result["path"],
        )
        return result
    except Exception as e:
        result["error"] = str(e)
        logger.error("Failed to save audiobook part locally for %s: %s", part_id, e, exc_info=True)
        return result


def start_audiobook_local_backup_worker(app) -> None:
    if not _AUDIOBOOK_AUTO_DOWNLOAD_LOCAL:
        return
    if app.bot_data.get("_shutdown_in_progress"):
        return
    logger.info(
        "Audiobook local backup worker config: storage_channel_id=%s auto_download=%s refresh_file_id=%s workers=%s",
        _resolve_audiobook_refresh_channel_id({}),
        _AUDIOBOOK_AUTO_DOWNLOAD_LOCAL,
        _AUDIOBOOK_LOCAL_REFRESH_FILE_ID,
        _AUDIOBOOK_LOCAL_WORKER_COUNT,
    )
    data = app.bot_data
    data["audiobook_local_backup_worker_target"] = _AUDIOBOOK_LOCAL_WORKER_COUNT
    workers = data.get(_AUDIOBOOK_LOCAL_WORKER_KEY)
    live_workers = []
    if isinstance(workers, list):
        live_workers = [task for task in workers if task is not None and not task.done()]
        data[_AUDIOBOOK_LOCAL_WORKER_KEY] = live_workers
    elif workers and not getattr(workers, "done", lambda: True)():
        live_workers = [workers]
        data[_AUDIOBOOK_LOCAL_WORKER_KEY] = live_workers
    missing_workers = max(0, _AUDIOBOOK_LOCAL_WORKER_COUNT - len(live_workers))
    if missing_workers <= 0:
        return
    start_index = len(live_workers)
    scheduler = globals().get("_schedule_application_task")
    created_workers = live_workers + [
        scheduler(app, _audiobook_local_backup_worker(app, worker_index=start_index + index + 1))
        if callable(scheduler)
        else app.create_task(_audiobook_local_backup_worker(app, worker_index=start_index + index + 1))
        for index in range(missing_workers)
    ]
    data[_AUDIOBOOK_LOCAL_WORKER_KEY] = created_workers
    logger.info("Started %s audiobook local backup worker(s)", missing_workers)


async def _audiobook_local_backup_worker(app, worker_index: int = 1) -> None:
    worker_id = f"audiobook-local-backup:{os.getpid()}:{worker_index}"
    try:
        while True:
            job_id = ""
            attempts = 0
            max_attempts = 12
            released = False
            try:
                job = await run_blocking(
                    claim_audio_book_part_local_download_job,
                    worker_id,
                    _AUDIOBOOK_LOCAL_JOB_STALE_AFTER_SECONDS,
                )
                if not job:
                    await asyncio.sleep(_AUDIOBOOK_LOCAL_WORKER_POLL_SECONDS)
                    continue

                job_id = str(job.get("id") or "").strip()
                audio_book_id = str(job.get("audio_book_id") or "").strip()
                part_id = str(job.get("audio_book_part_id") or "").strip()
                attempts = int(job.get("attempts") or 0)
                max_attempts = int(job.get("max_attempts") or 12)
                logger.info(
                    "Audiobook local backup job claimed: job_id=%s audio_book_id=%s part_id=%s attempts=%s/%s",
                    job_id,
                    audio_book_id,
                    part_id,
                    attempts,
                    max_attempts,
                )
                _set_audiobook_local_activity(
                    app,
                    worker_id,
                    worker="audiobook_local_backup",
                    worker_index=worker_index,
                    job_id=job_id,
                    audio_book_id=audio_book_id,
                    part_id=part_id,
                    stage="queued payload loaded",
                    title=part_id,
                )

                if not job_id or not audio_book_id or not part_id:
                    error = "missing audiobook local backup job payload"
                    if job_id:
                        if attempts >= max_attempts:
                            await run_blocking(fail_audio_book_part_local_download_job, job_id, error)
                            released = True
                        else:
                            await run_blocking(retry_audio_book_part_local_download_job, job_id, error, 60.0)
                            released = True
                    if _AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                audio_book = await run_blocking(get_audio_book_by_id, audio_book_id)
                part = await run_blocking(get_audio_book_part, part_id)
                if not audio_book or not part:
                    error = "audiobook or part not found"
                    if attempts >= max_attempts:
                        await run_blocking(fail_audio_book_part_local_download_job, job_id, error)
                        released = True
                    else:
                        await run_blocking(retry_audio_book_part_local_download_job, job_id, error, 60.0)
                        released = True
                    if _AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                book = None
                book_id = str((audio_book or {}).get("book_id") or "").strip()
                db_get_book_by_id_fn = globals().get("db_get_book_by_id")
                if book_id and callable(db_get_book_by_id_fn):
                    try:
                        book = await run_blocking(db_get_book_by_id_fn, book_id)
                    except Exception as e:
                        logger.warning("Failed to load parent book %s for audiobook local backup: %s", book_id, e)

                result = await _save_audiobook_part_local(
                    app.bot,
                    audio_book,
                    book,
                    part,
                    app=app,
                    status_update=lambda **fields: _set_audiobook_local_activity(
                        app,
                        worker_id,
                        worker="audiobook_local_backup",
                        worker_index=worker_index,
                        job_id=job_id,
                        audio_book_id=audio_book_id,
                        part_id=part_id,
                        **fields,
                    ),
                )
                refresh_required = bool(_AUDIOBOOK_LOCAL_REFRESH_FILE_ID)
                refresh_ok = bool(result.get("file_id_refreshed")) or not refresh_required
                if result.get("path") and result.get("db_updated") and refresh_ok:
                    _set_audiobook_local_activity(
                        app,
                        worker_id,
                        worker="audiobook_local_backup",
                        worker_index=worker_index,
                        job_id=job_id,
                        audio_book_id=audio_book_id,
                        part_id=part_id,
                        title=str((part or {}).get("title") or part_id),
                        stage="completed",
                    )
                    logger.info(
                        "Audiobook local backup job done: job_id=%s part_id=%s path=%s",
                        job_id,
                        part_id,
                        result["path"],
                    )
                    await run_blocking(complete_audio_book_part_local_download_job, job_id)
                    released = True
                    if _AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                timeout_like_refresh_failure = (
                    bool(result.get("path"))
                    and bool(result.get("db_updated"))
                    and refresh_required
                    and not result.get("file_id_refreshed")
                    and "timed out" in str(result.get("error") or "").lower()
                )
                if timeout_like_refresh_failure:
                    logger.warning(
                        "Audiobook local backup refresh timed out after local save; completing job without retry to avoid duplicate channel uploads: job_id=%s part_id=%s path=%s error=%s",
                        job_id,
                        part_id,
                        result.get("path"),
                        result.get("error"),
                    )
                    await run_blocking(complete_audio_book_part_local_download_job, job_id)
                    released = True
                    if _AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                        await asyncio.sleep(_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS)
                    continue

                error = str(result.get("error") or "audiobook local backup failed")
                if result.get("path") and not result.get("db_updated"):
                    error = f"{error} (local file saved, DB path update pending)"
                elif result.get("path") and result.get("db_updated") and refresh_required and not result.get("file_id_refreshed"):
                    error = f"{error} (local file saved, file_id refresh pending)"

                if attempts >= max_attempts:
                    logger.error(
                        "Audiobook local backup job failed permanently: job_id=%s part_id=%s attempts=%s/%s error=%s",
                        job_id,
                        part_id,
                        attempts,
                        max_attempts,
                        error,
                    )
                    await run_blocking(fail_audio_book_part_local_download_job, job_id, error)
                    released = True
                else:
                    backoff = max(
                        _AUDIOBOOK_LOCAL_RETRY_MIN_DELAY_SEC,
                        min(3600.0, _AUDIOBOOK_LOCAL_RETRY_BASE_DELAY_SEC * (2 ** max(0, attempts - 1))),
                    )
                    logger.warning(
                        "Audiobook local backup job retry scheduled: job_id=%s part_id=%s attempts=%s/%s backoff=%.1fs error=%s",
                        job_id,
                        part_id,
                        attempts,
                        max_attempts,
                        backoff,
                        error,
                    )
                    await run_blocking(retry_audio_book_part_local_download_job, job_id, error, backoff)
                    released = True
                if _AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS > 0:
                    await asyncio.sleep(_AUDIOBOOK_LOCAL_JOB_COOLDOWN_SECONDS)
            except asyncio.CancelledError:
                if job_id and not released:
                    try:
                        await run_blocking(retry_audio_book_part_local_download_job, job_id, "Worker shutdown", 5.0)
                    except Exception:
                        logger.exception("Failed to release audiobook local backup job during shutdown: %s", job_id)
                raise
            except Exception as e:
                logger.error("Audiobook local backup worker loop failed: %s", e, exc_info=True)
                await asyncio.sleep(5.0)
    finally:
        _clear_audiobook_local_activity(app, worker_id)
        current_task = _safe_asyncio_current_task()
        workers = app.bot_data.get(_AUDIOBOOK_LOCAL_WORKER_KEY)
        if current_task is not None and isinstance(workers, list):
            remaining = [task for task in workers if task is not current_task and not task.done()]
            if remaining:
                app.bot_data[_AUDIOBOOK_LOCAL_WORKER_KEY] = remaining
            else:
                app.bot_data.pop(_AUDIOBOOK_LOCAL_WORKER_KEY, None)
        elif current_task is not None and app.bot_data.get(_AUDIOBOOK_LOCAL_WORKER_KEY) is current_task:
            app.bot_data.pop(_AUDIOBOOK_LOCAL_WORKER_KEY, None)


async def _enqueue_audiobook_local_backup(app, audio_book_id: str, part: dict[str, Any]) -> None:
    if not _AUDIOBOOK_AUTO_DOWNLOAD_LOCAL:
        return
    audio_book_id = str(audio_book_id or "").strip()
    part_id = str((part or {}).get("id") or "").strip()
    file_id = str((part or {}).get("file_id") or "").strip()
    file_name = str((part or {}).get("title") or (part or {}).get("file_name") or "").strip()
    media_kind = str((part or {}).get("media_kind") or "").strip().lower() or None
    file_unique_id = str((part or {}).get("file_unique_id") or "").strip() or None
    if not audio_book_id or not part_id or not file_id:
        logger.warning("Skipping audiobook local backup enqueue for incomplete payload: audio_book_id=%s part_id=%s", audio_book_id, part_id)
        return
    if not file_name:
        ext = _audiobook_original_extension(part, part.get("title"))
        file_name = f"{_audiobook_storage_clean_title(part.get('title'), default='audio')}{ext}"
    try:
        job_id = await run_blocking(
            enqueue_audio_book_part_local_download_job,
            audio_book_id,
            part_id,
            file_id,
            file_name,
            file_unique_id,
            media_kind,
        )
        logger.info("Queued audiobook local backup job: audio_book_id=%s part_id=%s job_id=%s", audio_book_id, part_id, job_id)
        start_audiobook_local_backup_worker(app)
    except Exception as e:
        logger.error("Failed to enqueue audiobook local backup job for part %s: %s", part_id, e, exc_info=True)

def _callback_reaction_state_key(query) -> str | None:
    msg = getattr(query, "message", None)
    chat = getattr(msg, "chat", None) if msg else None
    message_id = getattr(msg, "message_id", None) if msg else None
    user = getattr(query, "from_user", None)
    user_id = getattr(user, "id", None) if user else None
    if not chat or not message_id:
        return None
    return f"{chat.id}:{message_id}:{user_id or 0}"


def _reserve_callback_reaction_seq(query, context: ContextTypes.DEFAULT_TYPE) -> tuple[str | None, int]:
    key = _callback_reaction_state_key(query)
    if key is None:
        return None, 0
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", None) if app else None
    if not isinstance(bot_data, dict):
        return key, 0
    state = bot_data.setdefault("_callback_reaction_seq", {})
    if not isinstance(state, dict):
        state = {}
        bot_data["_callback_reaction_seq"] = state
    seq = int(state.get(key, 0) or 0) + 1
    state[key] = seq
    return key, seq


def _is_callback_reaction_latest(context: ContextTypes.DEFAULT_TYPE, state_key: str, state_seq: int) -> bool:
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", None) if app else None
    if not isinstance(bot_data, dict):
        return True
    state = bot_data.get("_callback_reaction_seq")
    if not isinstance(state, dict):
        return True
    return int(state.get(state_key, 0) or 0) == int(state_seq)


async def _send_reaction_for_callback_message(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    emoji: str,
    *,
    state_key: str | None = None,
    state_seq: int = 0,
) -> bool:
    msg = getattr(query, "message", None)
    chat = getattr(msg, "chat", None) if msg else None
    bot = getattr(context, "bot", None)
    message_id = getattr(msg, "message_id", None) if msg else None
    if not msg or not chat or not bot or not emoji or not message_id:
        return False

    if state_key is not None and state_seq > 0 and not _is_callback_reaction_latest(context, state_key, state_seq):
        return False

    if hasattr(bot, "set_message_reaction") and ReactionTypeEmoji is not None:
        try:
            await bot.set_message_reaction(
                chat_id=chat.id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
                is_big=False,
            )
            return True
        except Exception as e:
            logger.debug("callback reaction (native) failed: %s", e)

    try:
        reaction_payload = json.dumps([{"type": "emoji", "emoji": emoji}], ensure_ascii=False)
        await bot._post(
            "setMessageReaction",
            data={
                "chat_id": chat.id,
                "message_id": message_id,
                "reaction": reaction_payload,
                "is_big": False,
            },
        )
        return True
    except Exception as e:
        logger.debug("callback reaction (raw) failed: %s", e)
        return False


async def _send_callback_reaction_with_fallbacks(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    emojis: tuple[str, ...] | list[str],
    *,
    state_key: str | None = None,
    state_seq: int = 0,
) -> bool:
    seen: set[str] = set()
    for emoji in emojis:
        e = str(emoji or "").strip()
        if not e or e in seen:
            continue
        seen.add(e)
        sent = await _send_reaction_for_callback_message(
            query,
            context,
            e,
            state_key=state_key,
            state_seq=state_seq,
        )
        if sent:
            return True
    return False


async def _send_salute_reaction_for_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_reaction_for_message(update, context, "🫡")


def _get_audio_channel_send_guard_for_app(app, channel_id: int):
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


def _get_audio_channel_send_guard(context: ContextTypes.DEFAULT_TYPE, channel_id: int):
    app = getattr(context, "application", None)
    return _get_audio_channel_send_guard_for_app(app, channel_id)


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


def _resolve_audio_upload_channel_ids() -> list[int]:
    # Preferred: AUDIO_UPLOAD_CHANNEL_IDS (list/comma-separated),
    # with AUDIO_UPLOAD_CHANNEL_ID kept as backward-compatible fallback.
    ids = _coerce_int_id_list(globals().get("AUDIO_UPLOAD_CHANNEL_IDS"))
    if ids:
        return ids
    ids = _coerce_int_id_list(os.getenv("AUDIO_UPLOAD_CHANNEL_IDS", ""))
    if ids:
        return ids
    ids = _coerce_int_id_list([globals().get("AUDIO_UPLOAD_CHANNEL_ID")])
    if ids:
        return ids
    return _coerce_int_id_list([os.getenv("AUDIO_UPLOAD_CHANNEL_ID", "")])


async def _pick_audio_upload_channel_id(
    context: ContextTypes.DEFAULT_TYPE,
    channel_ids: list[int] | None = None,
) -> int | None:
    ids = list(channel_ids or _resolve_audio_upload_channel_ids())
    if not ids:
        return None

    app = getattr(context, "application", None)
    if app is None:
        return ids[0]

    data = app.bot_data
    lock = data.get("audio_upload_channel_lock")
    if lock is None:
        lock = asyncio.Lock()
        data["audio_upload_channel_lock"] = lock

    async with lock:
        idx = int(data.get("audio_upload_channel_index", 0) or 0)
        channel_id = ids[idx % len(ids)]
        data["audio_upload_channel_index"] = idx + 1
        return channel_id


def _resolve_audio_storage_channel_id() -> int | None:
    """
    Resolve the single Telegram channel used for audiobook storage.
    Reuse the book storage channel so audiobook parts follow the same pipeline.
    """
    raw_candidates: tuple[Any, ...] = (
        globals().get("BOOK_STORAGE_CHANNEL_ID"),
        os.getenv("BOOK_STORAGE_CHANNEL_ID", ""),
    )
    for raw in raw_candidates:
        try:
            value = int(str(raw or "").strip())
        except Exception:
            continue
        if value != 0:
            return value
    return None


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
        return False
    return True


def _is_group_chat(update_or_chat) -> bool:
    """Return True for either an Update or a Chat object in a group/supergroup."""
    chat = getattr(update_or_chat, "effective_chat", None)
    if chat is None:
        chat = getattr(update_or_chat, "chat", None)
    if chat is None and hasattr(update_or_chat, "type"):
        chat = update_or_chat

    chat_type = str(getattr(chat, "type", "") or "").lower()
    if not chat_type and hasattr(update_or_chat, "callback_query"):
        cb = getattr(update_or_chat, "callback_query", None)
        msg = getattr(cb, "message", None) or getattr(update_or_chat, "effective_message", None)
        chat_type = str(getattr(getattr(msg, "chat", None), "type", "") or "").lower()
    return chat_type in {"group", "supergroup"}


def _detect_picker_lang(code: str | None) -> str:
    text = str(code or "").strip().lower()
    if text.startswith("uz"):
        return "uz"
    if text.startswith("ru"):
        return "ru"
    return "en"


def _user_language_selected(user_record: dict[str, Any] | None) -> bool:
    if not isinstance(user_record, dict):
        return False
    return bool(user_record.get("language_selected")) and bool(str(user_record.get("language") or "").strip())


async def _reply_private_language_picker_again(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if _is_group_chat(getattr(update, "effective_chat", None)):
        return False
    user = getattr(update, "effective_user", None)
    if not user:
        return False

    user_record: dict[str, Any] | None = None
    get_user_fn = globals().get("get_user")
    if callable(get_user_fn):
        try:
            fetched = get_user_fn(user.id)
            user_record = fetched if isinstance(fetched, dict) else None
        except Exception as e:
            logger.warning("language selection check failed for user %s: %s", user.id, e)

    if _user_language_selected(user_record):
        return False

    context.user_data.pop("main_menu_section", None)
    context.user_data["awaiting_book_search"] = False
    context.user_data.pop("search_mode", None)
    context.user_data.pop("language", None)

    prompt_lang = _detect_picker_lang(getattr(user, "language_code", None))
    await safe_reply(
        update,
        MESSAGES[prompt_lang]["choose_language"],
        reply_markup=get_language_keyboard(),
    )
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
    if REDIS_CACHE_AVAILABLE:
        cached = cache_get("top:books:entries")
        if isinstance(cached, dict):
            cached_entries = cached.get("entries")
            if isinstance(cached_entries, list):
                return cached_entries
        elif isinstance(cached, list):
            return cached

    cache = context.application.bot_data.get("top_entries_cache")
    if not cache:
        return None
    if time.time() - cache.get("ts", 0) > TOP_CACHE_TTL:
        return None
    return cache.get("entries")


def set_cached_top_entries(context: ContextTypes.DEFAULT_TYPE, entries: list):
    if REDIS_CACHE_AVAILABLE:
        ttl = _ttl_value("TOP_CACHE_TTL", 60, minimum=5)
        cache_set("top:books:entries", {"entries": entries}, ttl=ttl)

    context.application.bot_data["top_entries_cache"] = {
        "entries": entries,
        "ts": time.time(),
    }


def invalidate_top_caches(context: ContextTypes.DEFAULT_TYPE) -> None:
    if REDIS_CACHE_AVAILABLE:
        cache_delete("top:books:entries")
        cache_clear_pattern("top_results:*")
    try:
        context.application.bot_data.pop("top_entries_cache", None)
    except Exception:
        pass
    try:
        context.user_data.pop("top_cache", None)
    except Exception:
        pass


def get_cached_audit_report(context: ContextTypes.DEFAULT_TYPE, lang: str):
    if REDIS_CACHE_AVAILABLE:
        cache_key = f"audit:report:{lang}"
        payload = cache_get(cache_key)
        if isinstance(payload, dict):
            text = payload.get("text")
            if isinstance(text, str) and text.strip():
                return text
        elif isinstance(payload, str) and payload.strip():
            return payload

    cache = context.application.bot_data.get("audit_cache", {})
    entry = cache.get(lang)
    if not entry:
        return None
    if time.time() - entry.get("ts", 0) > AUDIT_CACHE_TTL:
        return None
    return entry.get("text")


def set_cached_audit_report(context: ContextTypes.DEFAULT_TYPE, lang: str, text: str):
    if REDIS_CACHE_AVAILABLE and text:
        ttl = _ttl_value("AUDIT_CACHE_TTL", 30, minimum=5)
        cache_set(f"audit:report:{lang}", {"text": text}, ttl=ttl)

    cache = context.application.bot_data.setdefault("audit_cache", {})
    cache[lang] = {"text": text, "ts": time.time()}


def invalidate_audit_caches(context: ContextTypes.DEFAULT_TYPE) -> None:
    if REDIS_CACHE_AVAILABLE:
        cache_clear_pattern("audit:report:*")
    try:
        context.application.bot_data.pop("audit_cache", None)
    except Exception:
        pass


def build_results_text(query: str, entries: list, page: int, lang: str):
    total = len(entries)
    pages = max(1, int(math.ceil(total / PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_entries = entries[start:end]

    header = MESSAGES[lang]["results_header"].format(query=query, page=page + 1, pages=pages, total=total)
    subtitle = MESSAGES[lang]["results_pick_hint"]
    page_line = MESSAGES[lang]["results_page_line"].format(page=page + 1, pages=pages)
    lines = []
    for i, e in enumerate(page_entries, start=start + 1):
        title = str(e.get("title") or "").strip()
        if len(title) > 88:
            title = title[:85].rstrip() + "..."
        lines.append(MESSAGES[lang]["results_item_line"].format(index=i, title=title))
        subtitle = str(e.get("subtitle") or "").strip()
        if subtitle:
            lines.append(f"   {subtitle}")
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return "\n".join([header, subtitle, page_line, "", body]) + footer, page_entries, pages


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

    header = MESSAGES[lang]["user_results_header"].format(query=query, page=page + 1, pages=pages, total=total)
    subtitle = MESSAGES[lang]["user_results_pick_hint"]
    page_line = MESSAGES[lang]["results_page_line"].format(page=page + 1, pages=pages)
    lines = []
    for i, e in enumerate(page_entries, start=start + 1):
        lines.append(MESSAGES[lang]["results_item_line"].format(index=i, title=e["title"]))
        subtitle = str(e.get("subtitle") or "").strip()
        if subtitle:
            lines.append(f"   {subtitle}")
    body = "\n".join(lines)
    footer = "\n\n" + MESSAGES[lang]["use_buttons"]
    return "\n".join([header, subtitle, page_line, "", body]) + footer, page_entries, pages


def _book_entry_format(book: dict) -> str:
    path = str((book or {}).get("path") or "").strip()
    if path:
        ext = os.path.splitext(path)[1].lstrip(".").upper()
        if ext:
            return ext
    title = str((book or {}).get("title") or (book or {}).get("display_name") or (book or {}).get("book_name") or "").strip()
    if "." in title:
        ext = title.rsplit(".", 1)[-1].strip().upper()
        if 1 <= len(ext) <= 5 and ext.isalnum():
            return ext
    return ""


def _score_book_entry(book: dict, query_text: str, base_score: float = 0.0) -> float:
    query_norm = normalize(query_text).lower().strip()
    tokenize_fn = globals().get("tokenize")
    if callable(tokenize_fn):
        try:
            query_tokens = [token for token in tokenize_fn(query_text) if token]
        except Exception:
            query_tokens = [token for token in query_norm.split() if token]
    else:
        query_tokens = [token for token in query_norm.split() if token]
    title = normalize(get_result_title(book)).lower().strip()
    display = normalize(str(book.get("display_name") or "")).lower().strip()
    haystacks = [text for text in (title, display) if text]
    haystack_token_sets: list[set[str]] = []
    for text in haystacks:
        if callable(tokenize_fn):
            try:
                haystack_token_sets.append(set(tokenize_fn(text)))
                continue
            except Exception:
                pass
        haystack_token_sets.append(set(token for token in text.split() if token))

    score = float(base_score or 0.0)
    if query_tokens and not any(set(query_tokens) & token_set for token_set in haystack_token_sets):
        return -10000.0
    if query_norm:
        if any(text == query_norm for text in haystacks):
            score += 1000
        elif any(text.startswith(query_norm) for text in haystacks):
            score += 700
        elif query_tokens and any(all(token in text for token in query_tokens) for text in haystacks):
            score += 400
        elif any(query_norm in text for text in haystacks):
            score += 250

    file_id = str(book.get("file_id") or "").strip()
    path = str(book.get("path") or "").strip()
    if file_id:
        score += 120
    if path:
        score += 60
    if str(book.get("indexed") or "").lower() in {"1", "true"}:
        score += 15
    return score


def _build_book_entry(book: dict, query_text: str, base_score: float = 0.0) -> dict:
    return {
        "id": str(book.get("id") or "").strip(),
        "title": get_result_title(book),
        "subtitle": "",
        "score": _score_book_entry(book, query_text, base_score),
    }


def _book_entry_dedupe_key(entry: dict) -> str:
    title = normalize(str((entry or {}).get("title") or "")).lower().strip()
    title = re.sub(r"\b(pdf|epub|djvu|fb2|mobi|docx?|txt|rtf)\b", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


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


def build_user_info_text(user: dict) -> str:
    name = " ".join([p for p in [user.get("first_name"), user.get("last_name")] if p]).strip() or "—"
    username = f"@{user.get('username')}" if user.get("username") else "—"
    language = user.get("language") or "—"
    joined = user.get("joined_date") or "—"
    audio_allowed = "✅" if bool(user.get("audio_allowed")) else "❌"
    rename_allowed = "✅" if bool(user.get("rename_allowed")) else "❌"
    return "\n".join([
        f"👤 Name: {name}",
        f"🔤 Username: {username}",
        f"🆔 User ID: {user.get('id')}",
        f"🌐 Language: {language}",
        f"📅 Joined: {joined}",
        f"✏️ Rename allowed: {rename_allowed}",
        f"🎧 Audio allowed: {audio_allowed}",
    ])


def build_user_admin_keyboard(user: dict) -> InlineKeyboardMarkup:
    user_id = user.get("id")
    blocked = bool(user.get("blocked"))
    upload_allowed = bool(user.get("allowed"))
    delete_allowed = bool(user.get("delete_allowed"))
    rename_allowed = bool(user.get("rename_allowed"))
    audio_allowed = bool(user.get("audio_allowed"))
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
            InlineKeyboardButton(f"✏️ Rename {mark(rename_allowed)}", callback_data=f"uact:rename:{user_id}"),
        ],
        [
            InlineKeyboardButton(f"🔇 Stop {mark(stopped)}", callback_data=f"uact:stop:{user_id}"),
            InlineKeyboardButton(f"🎧 Audio {mark(audio_allowed)}", callback_data=f"uact:audio:{user_id}"),
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


def transliterate_to_cyrillic(text: str) -> str:
    if not text:
        return ""
    s = str(text).lower()
    s = s.replace("ʻ", "'").replace("’", "'").replace("ʼ", "'")

    digraphs = (
        ("shch", "щ"),
        ("yo", "ё"),
        ("yu", "ю"),
        ("ya", "я"),
        ("ye", "е"),
        ("zh", "ж"),
        ("kh", "х"),
        ("ts", "ц"),
        ("ch", "ч"),
        ("sh", "ш"),
        ("o'", "ў"),
        ("g'", "ғ"),
    )
    single = {
        "a": "а", "b": "б", "v": "в", "g": "г", "d": "д", "e": "е",
        "z": "з", "i": "и", "y": "й", "k": "к", "l": "л", "m": "м",
        "n": "н", "o": "о", "p": "п", "r": "р", "s": "с", "t": "т",
        "u": "у", "f": "ф", "h": "х", "q": "қ", "x": "кс", "j": "ж",
        "c": "с", "w": "в",
    }

    out: list[str] = []
    i = 0
    while i < len(s):
        matched = False
        for latin, cyr in digraphs:
            if s.startswith(latin, i):
                out.append(cyr)
                i += len(latin)
                matched = True
                break
        if matched:
            continue
        ch = s[i]
        out.append(single.get(ch, ch))
        i += 1
    return "".join(out)


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
    reply_to_message_id: int | None = None,
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
            target_reply_to_message_id = reply_to_message_id
            if target_reply_to_message_id is None:
                try:
                    target_reply_to_message_id = int(getattr(fallback_message, "message_id", 0) or 0) or None
                except Exception:
                    target_reply_to_message_id = None
            await fallback_message.reply_text(
                text,
                reply_markup=reply_markup,
                reply_to_message_id=target_reply_to_message_id,
            )
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
_AUDIOBOOK_PLAY_ALL_ACTIVE_KEY = "audiobook_play_all_active_jobs"
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


def _extract_audiobook_message_media(message):
    return (
        getattr(message, "audio", None)
        or getattr(message, "voice", None)
        or getattr(message, "document", None)
    )


async def _cache_audiobook_part_file_id(part: dict, message) -> None:
    media = _extract_audiobook_message_media(message)
    if not media:
        return
    new_file_id = getattr(media, "file_id", None)
    new_file_unique_id = getattr(media, "file_unique_id", None)
    if not new_file_id:
        return
    try:
        preserve_storage_source = bool(
            int(part.get("channel_id") or 0)
            and int(part.get("channel_message_id") or 0)
            and str(part.get("file_id") or "").strip()
        )
    except Exception:
        preserve_storage_source = False
    if preserve_storage_source:
        logger.debug(
            "Skipping audiobook file_id cache overwrite for storage-backed part_id=%s",
            part.get("id"),
        )
        return
    part["file_id"] = new_file_id
    if new_file_unique_id:
        part["file_unique_id"] = new_file_unique_id
    try:
        await run_blocking(
            update_audio_book_part_media,
            str(part.get("id") or ""),
            new_file_id,
            new_file_unique_id,
        )
    except Exception as e:
        logger.warning(
            "Failed to cache audiobook part file_id for %s: %s",
            part.get("id"),
            e,
        )


def _audiobook_part_media_kind(part: dict) -> str:
    kind = str(part.get("media_kind") or "").strip().lower()
    if kind in {"audio", "voice", "document"}:
        return kind
    path = str(part.get("path") or "").strip().lower()
    if path.endswith((".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".oga", ".opus")):
        return "audio"
    if path.endswith((".pdf", ".txt", ".doc", ".docx", ".epub", ".rtf", ".mobi", ".djvu", ".fb2")):
        return "document"
    return "document"


async def _send_audiobook_part_to_chat(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    part: dict,
    *,
    caption: str | None = None,
    reply_markup=None,
):
    file_id = str(part.get("file_id") or "").strip()
    duration = part.get("duration_seconds")
    media_kind = _audiobook_part_media_kind(part)
    local_path = str(part.get("path") or "").strip()
    title = _audiobook_send_title(part, local_path if local_path else None)
    cover_input = get_book_thumbnail_input() if media_kind in {"audio", "document"} else None
    local_path_exists = bool(local_path and Path(local_path).exists() and media_kind in {"audio", "document"})
    try:
        channel_id = int(part.get("channel_id") or 0)
        channel_message_id = int(part.get("channel_message_id") or 0)
    except Exception:
        channel_id = 0
        channel_message_id = 0
    has_storage_source = bool(channel_id and channel_message_id and file_id)

    async def _send_by_file_id(target_file_id: str):
        send_attempts = (
            ("audio", lambda: context.bot.send_audio(chat_id=chat_id, audio=target_file_id, caption=caption, title=title, duration=duration, reply_markup=reply_markup)),
            ("voice", lambda: context.bot.send_voice(chat_id=chat_id, voice=target_file_id, caption=caption, duration=duration, reply_markup=reply_markup)),
            ("document", lambda: context.bot.send_document(chat_id=chat_id, document=target_file_id, caption=caption, reply_markup=reply_markup)),
        )
        for kind, sender in send_attempts:
            try:
                sent = await sender()
                await _cache_audiobook_part_file_id(part, sent)
                return sent
            except Exception as e:
                logger.debug(
                    "Audiobook part send by %s failed for %s: %s",
                    kind,
                    part.get("id"),
                    e,
                )
        return None

    async def _send_by_local_path(path_value: str):
        try:
            logger.info(
                "Sending audiobook part from processed local file: part_id=%s path=%s media_kind=%s",
                part.get("id"),
                path_value,
                media_kind,
            )
            filename = Path(path_value).name
            if media_kind == "audio":
                with open(path_value, "rb") as fh:
                    sent = await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=InputFile(fh, filename=filename),
                        caption=caption,
                        title=title,
                        duration=duration,
                        reply_markup=reply_markup,
                        thumbnail=cover_input,
                    )
            elif media_kind == "voice":
                with open(path_value, "rb") as fh:
                    sent = await context.bot.send_voice(
                        chat_id=chat_id,
                        voice=InputFile(fh, filename=filename),
                        caption=caption,
                        duration=duration,
                        reply_markup=reply_markup,
                    )
            else:
                with open(path_value, "rb") as fh:
                    sent = await context.bot.send_document(
                        chat_id=chat_id,
                        document=InputFile(fh, filename=filename),
                        caption=caption,
                        reply_markup=reply_markup,
                        thumbnail=cover_input,
                    )
            await _cache_audiobook_part_file_id(part, sent)
            return sent
        except Exception as e:
            logger.debug(
                "Audiobook part local-path send failed for %s (%s): %s",
                part.get("id"),
                path_value,
                e,
            )
            return None

    if has_storage_source:
        logger.info(
            "Sending audiobook part by refreshed storage file_id first: part_id=%s channel=%s message=%s",
            part.get("id"),
            channel_id,
            channel_message_id,
        )
        sent = await _send_by_file_id(file_id)
        if sent:
            return sent

    if file_id and not has_storage_source:
        logger.info(
            "Sending audiobook part by current source file_id while storage refresh is not ready yet: part_id=%s",
            part.get("id"),
        )
        sent = await _send_by_file_id(file_id)
        if sent:
            return sent

    if local_path_exists:
        sent = await _send_by_local_path(local_path)
        if sent:
            return sent
        if not has_storage_source:
            logger.warning(
                "Audiobook local processed fallback send failed for part_id=%s path=%s after current source file_id path also failed",
                part.get("id"),
                local_path,
            )

    if channel_id and channel_message_id:
        try:
            forwarded = await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=channel_id,
                message_id=channel_message_id,
            )
            await _cache_audiobook_part_file_id(part, forwarded)
            try:
                trimmed_caption = str(caption or "").strip()
                if trimmed_caption:
                    await forwarded.edit_caption(caption=trimmed_caption, reply_markup=reply_markup)
                elif reply_markup is not None:
                    await forwarded.edit_reply_markup(reply_markup=reply_markup)
            except Exception as e:
                logger.debug(
                    "Audiobook forwarded message edit skipped for %s: %s",
                    part.get("id"),
                    e,
                )
                if reply_markup is not None:
                    try:
                        await forwarded.edit_reply_markup(reply_markup=reply_markup)
                    except Exception:
                        pass
            return forwarded
        except Exception as e:
            logger.error(
                "Audiobook storage forward failed for %s (channel=%s message=%s): %s",
                part.get("id"),
                channel_id,
                channel_message_id,
                e,
            )
    return None


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
        caption = audio_book.get("display_title") or audio_book.get("title") or ""
        sent = await _send_audiobook_part_to_chat(
            context,
            query.message.chat_id,
            part,
            caption=caption,
        )
        if not sent:
            await safe_answer(query, MESSAGES[lang].get("audio_send_failed", "Failed to send audio"), show_alert=True)
            return
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
    part_id = part.get("id")
    caption = f"{part_index}/{len(all_parts)}"
    # Build keyboard with delete button (admin only)
    kb = None
    if _is_admin_user(query.from_user.id) and not _is_group_chat(getattr(update, "effective_chat", None)):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Delete Part", callback_data=f"apdel:{part_id}")]])
    sent = await _send_audiobook_part_to_chat(
        context,
        query.message.chat_id,
        part,
        caption=caption,
        reply_markup=kb,
    )
    if not sent:
        await safe_answer(query, MESSAGES[lang].get("audio_send_failed", "Failed to send audio"), show_alert=True)
        return
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


def _resolve_audiobook_request_book_id(record: dict | None) -> str | None:
    if not isinstance(record, dict):
        return None
    direct = str(record.get("book_id") or "").strip()
    if direct:
        return direct
    return _extract_requested_book_id(str(record.get("query") or ""))


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
    notified_users: set[int] = set()

    async def _mark_done_for_request(req: dict) -> None:
        req_id = req.get("id")
        if not req_id:
            return
        try:
            if callable(mark_done_fn):
                await run_blocking(mark_done_fn, req_id, book_id)
            elif callable(update_status_fn):
                await run_blocking(update_status_fn, req_id, "done", None, "Audiobook added automatically")
        except Exception as e:
            logger.warning("Failed to mark audiobook request as done (request_id=%s): %s", req.get("id"), e)

    for req in requests:
        if str(req.get("status") or "") not in {"open", "seen"}:
            continue
        req_book_id = _resolve_audiobook_request_book_id(req)
        if not req_book_id or str(req_book_id).strip() != str(book_id).strip():
            continue

        user_id = req.get("user_id")
        if not user_id:
            continue
        try:
            target_user_id = int(user_id)
        except Exception:
            continue
        if target_user_id <= 0:
            continue

        # Multiple open requests can exist for one user+book. Notify once, then just mark the rest done.
        if target_user_id in notified_users:
            await _mark_done_for_request(req)
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
            await context.bot.send_message(chat_id=target_user_id, text=text, reply_markup=keyboard)
        except Exception as e:
            logger.warning(
                "Failed to send audiobook-ready message to user=%s for book_id=%s: %s",
                target_user_id,
                book_id,
                e,
            )
            continue

        await _mark_done_for_request(req)
        notified_users.add(target_user_id)
        notified += 1

    return notified


async def handle_abook_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capture audio/voice/document while an audiobook add flow is pending."""
    pending = context.user_data.get("pending_abook")
    logger.debug("handle_abook_audio called (pending=%s)", pending is not None)
    if not pending:
        logger.debug("handle_abook_audio: no pending audiobook flow, returning")
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

    lang = ensure_user_language(update, context)
    audio_book_id = str(pending.get("audio_book_id") or "").strip()
    title = getattr(file, "file_name", None)
    file_id = file.file_id
    file_unique = getattr(file, "file_unique_id", None)
    duration = getattr(file, "duration", None)
    media_kind = "audio" if getattr(msg, "audio", None) else "voice" if getattr(msg, "voice", None) else "document"
    stored_channel_id = None
    stored_channel_message_id = None
    lock = _get_abook_add_flow_lock(audio_book_id, update.effective_user.id if update.effective_user else None)
    pending_snapshot = dict(pending)
    async with lock:
        pending = context.user_data.get("pending_abook")
        if not pending or str(pending.get("audio_book_id") or "").strip() != audio_book_id:
            logger.debug("handle_abook_audio: audiobook flow changed before save, aborting")
            raise ApplicationHandlerStop()

        part_index = int(pending.get("next_part_index", 1) or 1)
        is_insert_mode = "insert_max" in pending and not pending.get("awaiting_insert_index", False)

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

        # Keep the original incoming file_id for now. The background local-backup worker
        # will download it, clean the name, apply the cover, and upload the processed
        # file to the storage channel once, producing the fresh final file_id.
        logger.info(
            "Audiobook part queued for local processing before storage upload: audiobook=%s part_index=%s media_kind=%s",
            audio_book_id,
            part_index,
            media_kind,
        )

        # In insert mode: shift existing parts >= part_index up by 1 to make room
        if is_insert_mode:
            try:
                await run_blocking(shift_audio_book_parts_from, audio_book_id, part_index)
            except Exception as e:
                logger.error(f"Failed to shift audiobook parts: {e}")
                try:
                    await msg.reply_text(MESSAGES[lang]["audiobook_insert_prepare_failed"])
                except Exception:
                    pass
                raise ApplicationHandlerStop()

        try:
            part_id = await run_blocking(
                insert_audio_book_part,
                audio_book_id=audio_book_id,
                part_index=part_index,
                title=title,
                media_kind=media_kind,
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
                    await msg.reply_text(MESSAGES[lang]["audiobook_part_save_constraint"])
                except Exception:
                    pass
                logger.warning(f"Audio part insertion failed (constraint): {e}")
                raise ApplicationHandlerStop()
            error_text = str(e).strip()[:200] or "unknown error"
            failure_text = MESSAGES[lang]["audiobook_part_save_failed"].format(error=error_text)
            logger.error("Audio part insertion failed for audiobook=%s part_index=%s: %s", audio_book_id, part_index, e, exc_info=True)
            try:
                await msg.reply_text(failure_text)
            except Exception:
                pass
            raise ApplicationHandlerStop()

        if is_insert_mode:
            # Single-file insert: clear the flow after insertion
            context.user_data.pop("pending_abook", None)
        else:
            pending["next_part_index"] = part_index + 1
            context.user_data["pending_abook"] = pending
        pending_snapshot = dict(pending)
    try:
        await msg.reply_text(MESSAGES[lang].get("audiobook_part_saved", "✅ Part #{index} saved.").format(index=part_index))
    except Exception:
        pass

    _schedule_bg_task(
        context,
        _enqueue_audiobook_local_backup(
            context.application,
            audio_book_id,
            {
                "id": part_id,
                "part_index": part_index,
                "title": title,
                "file_id": file_id,
                "file_unique_id": file_unique,
                "media_kind": media_kind,
                "duration_seconds": duration,
                "channel_id": stored_channel_id,
                "channel_message_id": stored_channel_message_id,
            },
        ),
    )

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
                            book_row.get("display_name")
                            or book_row.get("book_name")
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

    try:
        sender = update.effective_user or getattr(query, "from_user", None)
        send_request = globals().get("send_request_to_admin")
        request_query = msgs.get(
            "audiobook_missing_request_query",
            "🎧 Audiobook request: {title} [book_id: {book_id}]",
        ).format(title=book_title, book_id=book_id)
        if callable(send_request) and sender:
            await send_request(context, sender, request_query, lang, book_id=book_id)
    except Exception as e:
        logger.warning("Failed to create audiobook request (book_id=%s): %s", book_id, e)

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


def _audiobook_part_button_text(lang: str, part_index: int) -> str:
    if lang == "uz":
        return f"{part_index}-qism"
    if lang == "ru":
        return f"Часть {part_index}"
    return f"Part {part_index}"


def _format_audiobook_duration(total_seconds: int | None) -> str:
    try:
        seconds = max(0, int(total_seconds or 0))
    except Exception:
        seconds = 0
    if seconds <= 0:
        return "0m"
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{max(1, minutes)}m"


def _resolve_audiobook_total_duration(audio_book: dict | None, parts: list[dict]) -> int:
    try:
        total_seconds = int((audio_book or {}).get("total_duration_seconds") or 0)
    except Exception:
        total_seconds = 0
    if total_seconds > 0:
        return total_seconds
    resolved = 0
    for part in parts or []:
        try:
            resolved += max(0, int(part.get("duration_seconds") or 0))
        except Exception:
            continue
    return resolved


def _build_audiobook_panel_text(
    audio_book: dict,
    parts: list[dict],
    lang: str,
) -> str:
    msgs = MESSAGES.get(lang, MESSAGES.get("en", {}))
    title = str(audio_book.get("display_title") or audio_book.get("title") or "Audiobook").strip() or "Audiobook"
    parts_count = len(parts)
    duration_text = _format_audiobook_duration(_resolve_audiobook_total_duration(audio_book, parts))
    template = msgs.get(
        "audiobook_listen_panel",
        "🎧 {title}\n🎵 {parts} parts • 🕒 {duration}\n👇 Start listening or choose a specific part.",
    )
    return template.format(title=title, parts=parts_count, duration=duration_text)


def _build_audiobook_listen_keyboard(
    book_id: str,
    parts: list[dict],
    lang: str,
    page: int = 0,
) -> InlineKeyboardMarkup:
    msgs = MESSAGES.get(lang, MESSAGES.get("en", {}))
    parts_per_page = 10
    total_pages = max(1, (len(parts) + parts_per_page - 1) // parts_per_page)
    page = max(0, min(page, total_pages - 1))
    page_parts = parts[page * parts_per_page:(page + 1) * parts_per_page]
    rows: list[list[InlineKeyboardButton]] = []

    rows.append(
        [
            InlineKeyboardButton(
                msgs.get("audiobook_play_all_button", "🎧 Barchasini tinglash"),
                callback_data=f"abplayall:{book_id}",
            )
        ]
    )

    row: list[InlineKeyboardButton] = []
    for part in page_parts:
        part_id = str(part.get("id") or "").strip()
        try:
            part_index = int(part.get("part_index") or 0)
        except Exception:
            part_index = 0
        if not part_id or part_index <= 0:
            continue
        label = _audiobook_part_button_text(lang, part_index)
        row.append(
            InlineKeyboardButton(
                label,
                callback_data=f"abplay:{part_id}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        rows.append(
            [
                InlineKeyboardButton(
                    msgs.get("audiobook_prev_page", "◀ Prev"),
                    callback_data=f"abpage:{book_id}:{'prev' if page > 0 else 'stay'}",
                ),
                InlineKeyboardButton(
                    msgs.get("audiobook_page_indicator", "📄 {page}/{total}").format(page=page + 1, total=total_pages),
                    callback_data=f"abpage:{book_id}:stay",
                ),
                InlineKeyboardButton(
                    msgs.get("audiobook_next_page", "Next ▶"),
                    callback_data=f"abpage:{book_id}:{'next' if page < total_pages - 1 else 'stay'}",
                ),
            ]
        )

    return InlineKeyboardMarkup(rows)


def _build_audiobook_part_controls(
    *,
    book_id: str,
    parts: list[dict],
    current_part_id: str,
    lang: str,
    can_manage_audio: bool = False,
) -> InlineKeyboardMarkup | None:
    msgs = MESSAGES.get(lang, MESSAGES.get("en", {}))
    prev_part_id = ""
    next_part_id = ""
    current_index = -1
    for idx, item in enumerate(parts):
        if str(item.get("id") or "") == current_part_id:
            current_index = idx
            if idx > 0:
                prev_part_id = str(parts[idx - 1].get("id") or "")
            if idx < len(parts) - 1:
                next_part_id = str(parts[idx + 1].get("id") or "")
            break
    if current_index < 0 and not can_manage_audio:
        return None

    rows: list[list[InlineKeyboardButton]] = []
    nav_row: list[InlineKeyboardButton] = []
    if prev_part_id:
        nav_row.append(
            InlineKeyboardButton(
                msgs.get("audiobook_prev_part_button", "◀ Previous"),
                callback_data=f"abplay:{prev_part_id}",
            )
        )
    nav_row.append(
        InlineKeyboardButton(
            msgs.get("audiobook_parts_button", "📚 All Parts"),
            callback_data=f"abook:{book_id}",
        )
    )
    if next_part_id:
        nav_row.append(
            InlineKeyboardButton(
                msgs.get("audiobook_next_part_button", "Next ▶"),
                callback_data=f"abplay:{next_part_id}",
            )
        )
    if nav_row:
        rows.append(nav_row)

    if can_manage_audio:
        rows.append([InlineKeyboardButton("🗑️ Delete Audio", callback_data=f"apdel:{current_part_id}")])

    return InlineKeyboardMarkup(rows) if rows else None


async def _send_full_audiobook_parts(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    audio_book: dict,
    parts: list[dict],
    lang: str,
) -> tuple[int, int]:
    sent_count = 0
    total = len(parts)
    title = str(audio_book.get("display_title") or audio_book.get("title") or "Audiobook").strip() or "Audiobook"

    for idx, part in enumerate(parts, start=1):
        caption = MESSAGES[lang].get(
            "audiobook_part_caption",
            "🎧 {title}\n🎵 Part {current}/{total}",
        ).format(
            title=title,
            current=int(part.get("part_index") or idx) or idx,
            total=total,
        )
        sent = await _send_audiobook_part_to_chat(
            context,
            chat_id,
            part,
            caption=caption,
            reply_markup=None,
        )
        if sent:
            sent_count += 1
            await asyncio.sleep(0.25)

    return sent_count, total


def _get_audiobook_play_all_active_jobs(context: ContextTypes.DEFAULT_TYPE) -> set[tuple[int, str]]:
    data = getattr(context.application, "bot_data", None)
    if not isinstance(data, dict):
        return set()
    jobs = data.get(_AUDIOBOOK_PLAY_ALL_ACTIVE_KEY)
    if isinstance(jobs, set):
        return jobs
    jobs = set()
    data[_AUDIOBOOK_PLAY_ALL_ACTIVE_KEY] = jobs
    return jobs


async def handle_audiobook_listen_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user request to listen to an audiobook (show audio parts)."""
    logger.debug("handle_audiobook_listen_callback called")
    query = update.callback_query
    if not query:
        logger.debug("handle_audiobook_listen_callback: no query")
        return
    lang = ensure_user_language(update, context)
    guest_inline_delivery = bool(getattr(query, "inline_message_id", None) and not query.message)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    if guest_inline_delivery:
        await safe_answer(
            query,
            MESSAGES[lang].get(
                "guest_audiobook_private_only",
                MESSAGES["en"].get("guest_audiobook_private_only", "Audiobooks can't be sent from guest mode yet."),
            ),
            show_alert=True,
        )
        return
    data = query.data or ""
    logger.debug("audiobook callback data=%s", data)
    if not data.startswith("abook:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    await safe_answer(query)

    reaction_state_key, reaction_state_seq = _reserve_callback_reaction_seq(query, context)
    await _send_callback_reaction_with_fallbacks(
        query,
        context,
        ("🥰", "❤️", "👍"),
        state_key=reaction_state_key,
        state_seq=reaction_state_seq,
    )

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

    context.user_data[f"abook_page_{book_id}"] = 0
    text = _build_audiobook_panel_text(audio_book, all_parts, lang)
    keyboard = _build_audiobook_listen_keyboard(
        book_id,
        all_parts,
        lang,
        page=0,
    )

    try:
        await query.message.reply_text(text, reply_markup=keyboard)
    except Exception as e:
        logger.debug("Failed to send audiobook parts message: %s", e)
        return


async def handle_audiobook_play_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send all parts of an audiobook in order while keeping the current parts panel unchanged."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    guest_inline_delivery = bool(getattr(query, "inline_message_id", None) and not query.message)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    if guest_inline_delivery:
        await safe_answer(
            query,
            MESSAGES[lang].get(
                "guest_audiobook_private_only",
                MESSAGES["en"].get("guest_audiobook_private_only", "Audiobooks can't be sent from guest mode yet."),
            ),
            show_alert=True,
        )
        return
    data = query.data or ""
    if not data.startswith("abplayall:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    await safe_answer(query)

    audio_book = await run_blocking(get_audio_book_for_book, book_id)
    if not audio_book:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_not_found", "Audiobook not found"))
        return

    all_parts = await run_blocking(list_audio_book_parts, audio_book.get("id"))
    if not all_parts:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_no_parts", "No audio parts found"))
        return

    try:
        chat_id = int(query.message.chat_id)
    except Exception:
        chat_id = 0
    audio_book_id = str(audio_book.get("id") or "").strip()
    active_jobs = _get_audiobook_play_all_active_jobs(context)
    job_key = (chat_id, audio_book_id)
    if chat_id and audio_book_id and job_key in active_jobs:
        await query.message.reply_text(
            MESSAGES[lang].get("audiobook_play_all_busy", "⏳ This audiobook is already being sent."),
        )
        return
    if chat_id and audio_book_id:
        active_jobs.add(job_key)

    async def _runner():
        try:
            try:
                await query.message.reply_text(
                    MESSAGES[lang].get("audiobook_play_all_started", "🎧 Sending all audiobook parts..."),
                )
            except Exception:
                pass
            sent_count, total = await _send_full_audiobook_parts(
                context,
                chat_id=query.message.chat_id,
                audio_book=audio_book,
                parts=all_parts,
                lang=lang,
            )
            if sent_count <= 0:
                try:
                    await query.message.reply_text(
                        MESSAGES[lang].get("audiobook_play_all_failed", "Failed to send audiobook parts."),
                    )
                except Exception:
                    pass
                return
            _schedule_bg_task(context, _run_db_retry(increment_audio_book_download, str(audio_book.get("id") or "")))
            if sent_count < total:
                try:
                    await query.message.reply_text(
                        MESSAGES[lang].get(
                            "audiobook_play_all_partial",
                            "Sent {sent}/{total} parts.",
                        ).format(sent=sent_count, total=total),
                    )
                except Exception:
                    pass
        finally:
            if chat_id and audio_book_id:
                active_jobs.discard(job_key)

    _schedule_bg_task(context, _runner())


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

    text = _build_audiobook_panel_text(audio_book, all_parts, lang)
    keyboard = _build_audiobook_listen_keyboard(
        book_id,
        all_parts,
        lang,
        page=current_page,
    )
    
    try:
        await query.edit_message_text(text, reply_markup=keyboard)
    except Exception:
        await query.message.reply_text(text, reply_markup=keyboard)
    
    await safe_answer(query)


async def handle_audiobook_part_play_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle playing an individual audiobook part."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    guest_inline_delivery = bool(getattr(query, "inline_message_id", None) and not query.message)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    if guest_inline_delivery:
        await safe_answer(
            query,
            MESSAGES[lang].get(
                "guest_audiobook_private_only",
                MESSAGES["en"].get("guest_audiobook_private_only", "Audiobooks can't be sent from guest mode yet."),
            ),
            show_alert=True,
        )
        return
    data = query.data or ""
    if not data.startswith("abplay:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    part_id = data.split(":", 1)[1]
    await safe_answer(query)
    
    # Get the audio part
    part = await run_blocking(get_audio_book_part, part_id)
    if not part:
        await query.message.reply_text(MESSAGES[lang].get("audio_part_not_found", "Audio part not found"))
        return
    
    audio_book_id = str(part.get("audio_book_id") or "").strip()
    audio_book = await run_blocking(get_audio_book_by_id, audio_book_id) if audio_book_id else None
    if not audio_book:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_not_found", "Audiobook not found"))
        return
    all_parts = await run_blocking(list_audio_book_parts, audio_book_id)
    if not all_parts:
        await query.message.reply_text(MESSAGES[lang].get("audiobook_no_parts", "No audio parts found"))
        return

    # Send the audio file with richer navigation controls
    can_manage_audio = False
    if not _is_group_chat(getattr(update, "effective_chat", None)) and callable(globals().get("is_audio_allowed")):
        try:
            can_manage_audio = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
        except Exception:
            can_manage_audio = False
    book_id = str(audio_book.get("book_id") or "").strip()
    reply_markup = _build_audiobook_part_controls(
        book_id=book_id,
        parts=all_parts,
        current_part_id=part_id,
        lang=lang,
        can_manage_audio=can_manage_audio,
    )
    current_part_number = 1
    for idx, item in enumerate(all_parts, start=1):
        if str(item.get("id") or "") == part_id:
            try:
                current_part_number = int(item.get("part_index") or 0) or idx
            except Exception:
                current_part_number = idx
            break
    caption = MESSAGES[lang].get(
        "audiobook_part_caption",
        "🎧 {title}\n🎵 Part {current}/{total}",
    ).format(
        title=str(audio_book.get("display_title") or audio_book.get("title") or "Audiobook").strip() or "Audiobook",
        current=current_part_number,
        total=len(all_parts),
    )
    sent = await _send_audiobook_part_to_chat(
        context,
        query.message.chat_id,
        part,
        caption=caption,
        reply_markup=reply_markup,
    )
    if sent:
        _schedule_bg_task(context, _run_db_retry(increment_audio_book_download, audio_book_id))
        await safe_answer(query)
        return
    await safe_answer(query, MESSAGES[lang].get("audio_send_failed", "Failed to send audio"), show_alert=True)


async def handle_audiobook_part_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a single audiobook part (admin only)."""
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    if _is_group_chat(getattr(update, "effective_chat", None)):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    can_manage_audio = False
    if callable(globals().get("is_audio_allowed")):
        try:
            can_manage_audio = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
        except Exception:
            can_manage_audio = False
    if not can_manage_audio:
        await safe_answer(query, MESSAGES[lang]["audiobook_add_not_allowed"], show_alert=True)
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
    local_paths = [str(part.get("path") or "").strip()] if str(part.get("path") or "").strip() else []
    deleted = await run_blocking(delete_audio_book_part, part_id)
    if deleted and local_paths:
        cleanup = await asyncio.to_thread(_delete_local_audiobook_paths, local_paths)
        logger.info(
            "Audiobook part delete cleanup: part_id=%s local_deleted=%s local_failed=%s",
            part_id,
            cleanup.get("deleted", 0),
            cleanup.get("failed", 0),
        )
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
    if _is_group_chat(getattr(update, "effective_chat", None)):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    can_manage_audio = False
    if callable(globals().get("is_audio_allowed")):
        try:
            can_manage_audio = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
        except Exception:
            can_manage_audio = False
    if not can_manage_audio:
        await safe_answer(query, MESSAGES[lang]["audiobook_add_not_allowed"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abdel:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    audio_book_id = data.split(":", 1)[1]
    parts = await run_blocking(list_audio_book_parts, audio_book_id)
    local_paths = [str(item.get("path") or "").strip() for item in (parts or []) if str(item.get("path") or "").strip()]
    # delete the audiobook (cascades to parts)
    deleted = await run_blocking(delete_audio_book, audio_book_id)
    if deleted and local_paths:
        cleanup = await asyncio.to_thread(_delete_local_audiobook_paths, local_paths)
        logger.info(
            "Audiobook delete cleanup: audio_book_id=%s local_deleted=%s local_failed=%s",
            audio_book_id,
            cleanup.get("deleted", 0),
            cleanup.get("failed", 0),
        )
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
    if _is_group_chat(getattr(update, "effective_chat", None)):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    can_manage_audio = False
    if callable(globals().get("is_audio_allowed")):
        try:
            can_manage_audio = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
        except Exception:
            can_manage_audio = False
    if not can_manage_audio:
        await safe_answer(query, MESSAGES[lang]["audiobook_add_not_allowed"], show_alert=True)
        return
    data = query.data or ""
    if not data.startswith("abdelbook:"):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    book_id = data.split(":", 1)[1]
    audio_books = await run_blocking(list_audio_books_by_book_id, book_id)
    local_paths: list[str] = []
    for audio_book in audio_books or []:
        audio_book_id = str((audio_book or {}).get("id") or "").strip()
        if not audio_book_id:
            continue
        parts = await run_blocking(list_audio_book_parts, audio_book_id)
        local_paths.extend(
            str(item.get("path") or "").strip()
            for item in (parts or [])
            if str(item.get("path") or "").strip()
        )
    deleted = await run_blocking(delete_audio_books_by_book_id, book_id)
    if deleted > 0:
        if local_paths:
            cleanup = await asyncio.to_thread(_delete_local_audiobook_paths, local_paths)
            logger.info(
                "Audiobook delete-by-book cleanup: book_id=%s local_deleted=%s local_failed=%s",
                book_id,
                cleanup.get("deleted", 0),
                cleanup.get("failed", 0),
            )
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
    if _is_group_chat(getattr(update, "effective_chat", None)):
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return
    can_manage_audio = False
    if callable(globals().get("is_audio_allowed")):
        try:
            can_manage_audio = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
        except Exception:
            can_manage_audio = False
    if not can_manage_audio:
        await safe_answer(query, MESSAGES[lang]["audiobook_add_not_allowed"], show_alert=True)
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
            await query.message.reply_text(MESSAGES[lang]["audiobook_create_failed"])
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

        if await _reply_private_language_picker_again(update, context):
            return

        menu_action = _main_menu_text_action(update.message.text.strip())
        if menu_action:
            await _cancel_menu_conflicting_flows(update, context, lang)
            handled = await _handle_main_menu_action(update, context, lang, menu_action)
            if handled:
                return

        if context.user_data.get("awaiting_request"):
            expires_at = float(context.user_data.get("awaiting_request_until") or 0)
            if expires_at and time.time() > expires_at:
                context.user_data["awaiting_request"] = False
                context.user_data.pop("awaiting_request_until", None)
            else:
                query_text = update.message.text.strip()
                if query_text.lower() in {"cancel", "stop", "/cancel"}:
                    context.user_data["awaiting_request"] = False
                    context.user_data.pop("awaiting_request_until", None)
                    await update.message.reply_text(MESSAGES[lang].get("menu_flow_cancelled", "❌ Cancelled."))
                    return
                if query_text:
                    context.user_data["awaiting_request"] = False
                    context.user_data.pop("awaiting_request_until", None)
                    send_request = globals().get("send_request_to_admin")
                    if callable(send_request):
                        await send_request(context, update.effective_user, query_text, lang)
                        await update.message.reply_text(MESSAGES[lang]["request_sent"])
                        return

        # Keep search mode stable between messages.
        search_mode = str(context.user_data.get("search_mode") or "").strip().lower()
        if search_mode == "book":
            context.user_data["awaiting_book_search"] = True

        limited, wait_s = spam_check_message(update, context)
        if limited:
            await update.message.reply_text(
                MESSAGES[lang].get("slow_down_soft", "Juda tez so‘rov yuboryapsiz. Bir oz kuting.")
            )
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

        pending_bonus = context.user_data.get("awaiting_user_bonus")
        if pending_bonus and _is_admin_user(update.effective_user.id):
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

        pending_request = context.user_data.get("pending_request_reply")
        if pending_request and _is_admin_user(update.effective_user.id):
            if time.time() > pending_request.get("expires_at", 0):
                context.user_data.pop("pending_request_reply", None)
            else:
                admin_text = update.message.text.strip()
                if admin_text.lower() in {"cancel", "stop"}:
                    context.user_data.pop("pending_request_reply", None)
                    await update.message.reply_text(MESSAGES[lang]["request_admin_cancelled"])
                    return

                record = await run_blocking(
                    update_request_status,
                    pending_request["request_id"],
                    pending_request["status"],
                    update.effective_user,
                    admin_text,
                )
                if not record:
                    await update.message.reply_text(MESSAGES[lang]["page_expired"])
                    context.user_data.pop("pending_request_reply", None)
                    return

                req_lang = str(record.get("language") or "en").strip() or "en"
                query_text = str(record.get("query") or "").strip()
                base_template = MESSAGES[req_lang].get(
                    f"request_reply_{record.get('status')}",
                    MESSAGES[req_lang].get("request_reply_seen_auto", ""),
                )
                msg = base_template.format(query=query_text)
                if admin_text:
                    msg += "\n\n" + MESSAGES[req_lang]["request_reply_note"].format(note=admin_text)

                notified_ok = True
                try:
                    await context.bot.send_message(chat_id=record["user_id"], text=msg)
                except Exception as e:
                    notified_ok = False
                    logger.error(
                        "Failed to notify user %s for request %s: %s",
                        record.get("user_id"),
                        record.get("id"),
                        e,
                        exc_info=True,
                    )

                try:
                    keyboard = build_request_admin_keyboard(record.get("status", "open"), record.get("id"))
                    await context.bot.edit_message_text(
                        chat_id=pending_request["admin_chat_id"],
                        message_id=pending_request["admin_message_id"],
                        text=format_request_admin_text(record),
                        reply_markup=keyboard,
                    )
                except Exception:
                    pass

                context.user_data.pop("pending_request_reply", None)
                admin_reply_text = MESSAGES[lang]["request_status_updated_admin"].format(status=record.get("status"))
                if not notified_ok:
                    admin_reply_text += "\n" + MESSAGES[lang]["request_user_notify_failed_admin"]
                await update.message.reply_text(admin_reply_text)
                return

        # Admin reply to upload access request
        pending_upload = context.user_data.get("pending_upload_reply")
        if pending_upload and _is_admin_user(update.effective_user.id):
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

        pending_book_rename = context.user_data.get("pending_book_rename")
        can_rename_books_fn = globals().get("can_rename_books")
        if pending_book_rename and callable(can_rename_books_fn) and can_rename_books_fn(update.effective_user.id):
            if time.time() > pending_book_rename.get("expires_at", 0):
                context.user_data.pop("pending_book_rename", None)
            else:
                rename_text = update.message.text.strip()
                if rename_text.lower() in {"cancel", "stop", "/cancel"}:
                    context.user_data.pop("pending_book_rename", None)
                    await update.message.reply_text(
                        MESSAGES[lang].get("book_rename_cancelled", "✖️ Book rename cancelled.")
                    )
                    return

                rename_fn = globals().get("apply_book_rename")
                if not callable(rename_fn):
                    context.user_data.pop("pending_book_rename", None)
                    await update.message.reply_text(MESSAGES[lang]["error"])
                    return

                book_id = str(pending_book_rename.get("book_id") or "").strip()
                if not book_id:
                    context.user_data.pop("pending_book_rename", None)
                    await update.message.reply_text(MESSAGES[lang]["page_expired"])
                    return

                progress_text = MESSAGES[lang].get(
                    "book_rename_processing",
                    "⏳ Updating book name...",
                )
                progress_msg = None
                try:
                    progress_msg = await update.message.reply_text(progress_text)
                except Exception:
                    progress_msg = None

                result = await rename_fn(context.bot, book_id, rename_text, update.effective_user.id)
                if result.get("ok"):
                    context.user_data.pop("pending_book_rename", None)
                    new_name = str(result.get("display_name") or result.get("book_name") or rename_text).strip()
                    if result.get("changed"):
                        done_text = MESSAGES[lang].get(
                            "book_rename_done",
                            "✅ Book name updated to: {name}",
                        ).format(name=new_name)
                    else:
                        done_text = MESSAGES[lang].get(
                            "book_rename_unchanged",
                            "ℹ️ Book name is already set to: {name}",
                        ).format(name=new_name)
                    if progress_msg:
                        try:
                            await progress_msg.edit_text(done_text)
                        except Exception:
                            await update.message.reply_text(done_text)
                    else:
                        await update.message.reply_text(done_text)
                    return

                error_text = str(result.get("error") or MESSAGES[lang]["error"])
                if bool(result.get("retryable")):
                    retry_text = MESSAGES[lang].get(
                        "book_rename_retry",
                        "⚠️ Could not update that name. Send another name or /cancel.",
                    )
                    if progress_msg:
                        try:
                            await progress_msg.edit_text(f"{retry_text}\n\n{error_text}")
                        except Exception:
                            await update.message.reply_text(f"{retry_text}\n\n{error_text}")
                    else:
                        await update.message.reply_text(f"{retry_text}\n\n{error_text}")
                else:
                    context.user_data.pop("pending_book_rename", None)
                    if progress_msg:
                        try:
                            await progress_msg.edit_text(error_text)
                        except Exception:
                            await update.message.reply_text(error_text)
                    else:
                        await update.message.reply_text(error_text)
                return

        # Admin adding audiobook parts
        pending_abook = context.user_data.get("pending_abook")
        if pending_abook and _is_admin_user(update.effective_user.id):
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
            # If in audiobook flow and not a command, don't process as search.
            # This prevents interfering with normal search when audiobook mode is active.
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

        # Simple thanks replies
        thanks_lang = _detect_thanks_reply_lang(update.message.text)
        if thanks_lang:
            await _send_heart_reaction_for_message(update, context)
            reply_lang = thanks_lang if thanks_lang in MESSAGES else lang
            await safe_reply(update, MESSAGES[reply_lang]["thanks_reply"])
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
        has_active_menu_context = active_menu_section in {"main", "other", "admin"}
        # Allow direct search on main menu (for old users used to typing book names)
        # and when no menu context is active (e.g. before/without opening menus).
        # Keep explicit Search Books requirement in submenus/admin sections to avoid confusion.
        require_search_button = active_menu_section in {"other", "admin"}

        if require_search_button and not bool(context.user_data.get("awaiting_book_search")) and not is_reply_search_in_group:
            if is_group_chat:
                # In groups, avoid noisy prompts on normal messages; search is allowed on replies.
                return
            await _reply_search_menu_click_hint(update, context, lang)
            return

        if not _is_admin_user(update.effective_user.id):
            limited, wait_s = rate_limited(context, "last_search_ts", SEARCH_COOLDOWN_SEC)
            if limited:
                await update.message.reply_text(
                    MESSAGES[lang].get("slow_down_soft", "Juda tez so‘rov yuboryapsiz. Bir oz kuting.")
                )
                return
        query = update.message.text.strip()
        if not query:
            await update.message.reply_text(MESSAGES[lang]["enter_specific"])
            return
        await _send_salute_reaction_for_message(update, context)
        try:
            progress_message = await update.message.reply_text(
                MESSAGES[lang].get("processing_search", "🔎 Searching... Please wait."),
                reply_to_message_id=update.message.message_id,
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
        query_variants: list[str] = []
        for candidate in (cleaned_query, cleaned_query.replace("ʻ", "")):
            candidate = str(candidate or "").strip()
            if candidate and candidate not in query_variants:
                query_variants.append(candidate)
        translit_variants: list[str] = []
        for candidate in query_variants:
            for translit_candidate in (
                transliterate_to_latin(candidate),
                transliterate_to_cyrillic(candidate),
            ):
                translit_candidate = str(translit_candidate or "").strip()
                if translit_candidate and translit_candidate not in translit_variants:
                    translit_variants.append(translit_candidate)

        entries = get_cached_book_search_entries(query) or []
        entries = entries[:MAX_SEARCH_RESULTS]
        results = []

        # --- Search in ES if available ---
        if not entries and es_available():
            for candidate in query_variants:
                results += await run_blocking(search_es, candidate)  # raw Cyrillic or Latin
            for candidate in translit_variants:
                if candidate not in query_variants:
                    results += await run_blocking(search_es, candidate)  # transliterated Latin
        elif not entries:
            books = await run_blocking(load_books)
            # --- Fallback: local substring search ---
            for candidate in query_variants:
                results += [(b, 1.0, b.get("id")) for b in books if candidate in str(b.get("book_name") or "").lower()]
            for candidate in translit_variants:
                if candidate not in query_variants:
                    results += [(b, 1.0, b.get("id")) for b in books if candidate in str(b.get("book_name") or "").lower()]

        # ✅ Deduplicate by UUID and build entries
        if not entries:
            unique_matches = {}
            for book, score, es_id in results:
                book_id = str(book.get("id") or es_id).strip() if book else None
                if not book_id:
                    continue
                full_book = None
                try:
                    full_book = db_get_book_by_id(book_id)
                except Exception:
                    full_book = None
                merged_book = dict(book or {})
                if isinstance(full_book, dict) and full_book:
                    merged_book.update(full_book)
                merged_book["id"] = book_id
                entry = _build_book_entry(merged_book, query, score)
                if book_id not in unique_matches or entry["score"] > unique_matches[book_id]["score"]:
                    unique_matches[book_id] = entry

            deduped_entries: dict[str, dict] = {}
            for entry in unique_matches.values():
                dedupe_key = _book_entry_dedupe_key(entry) or str(entry.get("id") or "")
                current = deduped_entries.get(dedupe_key)
                if current is None or float(entry.get("score") or 0.0) > float(current.get("score") or 0.0):
                    deduped_entries[dedupe_key] = entry

            entries = sorted(
                deduped_entries.values(),
                key=lambda e: (
                    -float(e.get("score") or 0.0),
                    len(str(e.get("title") or "")),
                    str(e.get("title") or "").lower(),
                ),
            )
            entries = entries[:MAX_SEARCH_RESULTS]
            if entries:
                set_cached_book_search_entries(query, entries)

        if not entries:
            if es_available():
                books = await run_blocking(load_books)
            suggestions = suggest_books(books, query_variants[0] if query_variants else cleaned_query, limit=5)
            if not suggestions and len(query_variants) > 1:
                suggestions = suggest_books(books, query_variants[1], limit=5)
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
                await _edit_progress_or_reply(
                    progress_message,
                    update.message,
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            else:
                request_id = uuid.uuid4().hex[:10]
                pending_requests = context.user_data.setdefault("requests", {})
                pending_requests[request_id] = {
                    "query": query,
                    "created_ts": time.time(),
                }
                reply_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton(MESSAGES[lang]["request_button"], callback_data=f"req:{request_id}")]]
                )
                await _edit_progress_or_reply(
                    progress_message,
                    update.message,
                    MESSAGES[lang]["not_found"],
                    reply_markup=reply_markup,
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
    get_audio_book_by_id,
    create_audio_book_for_book,
    insert_audio_book_part,
    list_audio_book_parts,
    get_audio_book_part,
    update_audio_book_part_media,
    get_audio_book_part_by_file_unique_id_and_audio_book,
    delete_audio_book_part,
    delete_audio_book,
    delete_audio_books_by_book_id,
    increment_audio_book_download,
    increment_audio_book_searches,
    shift_audio_book_parts_from,
    enqueue_audio_book_part_local_download_job,
    claim_audio_book_part_local_download_job,
    complete_audio_book_part_local_download_job,
    retry_audio_book_part_local_download_job,
    fail_audio_book_part_local_download_job,
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
        guest_inline_delivery = bool(getattr(query, "inline_message_id", None) and query.message is None)
        guest_handoff_token = None
        guest_handoff = None
        source_guest_chat_id = None
        if data.startswith("gbook:"):
            parts = data.split(":", 2)
            if len(parts) < 3:
                await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
                return
            book_id = parts[1].strip()
            guest_handoff_token = str(parts[2] or "").strip() or None
        elif data.startswith("book:"):
            book_id = data.split(":", 1)[1].strip()
        else:
            book_id = data  # backward compatibility for old buttons
        if guest_inline_delivery and guest_handoff_token:
            handoff_lookup_fn = globals().get("db_get_guest_private_handoff_by_token")
            capability_lookup_fn = globals().get("db_get_guest_group_delivery_capability")
            if callable(handoff_lookup_fn):
                try:
                    guest_handoff = await run_blocking(handoff_lookup_fn, guest_handoff_token)
                except Exception as e:
                    logger.warning("Failed to load guest handoff %s before delivery: %s", guest_handoff_token, e, exc_info=True)
                    guest_handoff = None
            if guest_handoff:
                try:
                    source_guest_chat_id = int((guest_handoff or {}).get("source_chat_id") or 0) or None
                except Exception:
                    source_guest_chat_id = None
            if source_guest_chat_id and callable(capability_lookup_fn):
                try:
                    delivery_capability = await run_blocking(capability_lookup_fn, source_guest_chat_id)
                except Exception as e:
                    logger.warning("Failed to load guest delivery capability for group %s: %s", source_guest_chat_id, e, exc_info=True)
                    delivery_capability = {}
                if bool((delivery_capability or {}).get("skip_same_chat_delivery")):
                    markup_builder = globals().get("build_guest_private_handoff_reply_markup")
                    if callable(markup_builder):
                        try:
                            new_markup = await markup_builder(
                                context,
                                handoff_token=guest_handoff_token,
                                selected_book_id=book_id,
                                actor_user_id=query.from_user.id,
                                lang=lang,
                            )
                            if new_markup is not None:
                                await context.bot.edit_message_reply_markup(
                                    inline_message_id=query.inline_message_id,
                                    reply_markup=new_markup,
                                )
                        except Exception as markup_error:
                            logger.warning(
                                "Failed to rebuild remembered guest private handoff markup for %s: %s",
                                book_id,
                                markup_error,
                                exc_info=True,
                            )
                    await safe_answer(query, MESSAGES[lang].get("guest_delivery_forbidden", MESSAGES["en"].get("guest_delivery_forbidden", "Guest delivery is not allowed in this chat.")), show_alert=True)
                    return
        if guest_inline_delivery:
            await safe_answer(query, MESSAGES[lang].get("guest_sending", MESSAGES["en"].get("guest_sending", "Sending book...")))
        else:
            await safe_answer(query)
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
            if guest_inline_delivery:
                await safe_answer(query, MESSAGES[lang]["book_not_found"], show_alert=True)
            else:
                await query.message.reply_text(MESSAGES[lang]["book_not_found"])
            return

        async def _load_delivery_snapshot() -> dict:
            snapshot_fn = globals().get("db_get_book_delivery_snapshot")
            if callable(snapshot_fn):
                snapshot = await run_blocking(snapshot_fn, book_id, query.from_user.id)
                if snapshot:
                    return snapshot
            return dict(book)

        def _snapshot_reaction_counts(snapshot: dict) -> dict[str, int]:
            return {
                "like": int(snapshot.get("like_count", snapshot.get("like", 0)) or 0),
                "dislike": int(snapshot.get("dislike_count", snapshot.get("dislike", 0)) or 0),
                "berry": int(snapshot.get("berry_count", snapshot.get("berry", 0)) or 0),
                "whale": int(snapshot.get("whale_count", snapshot.get("whale", 0)) or 0),
            }

        book = await _load_delivery_snapshot()

        local_path = book.get("path")
        file_id = book.get("file_id")

        status_msg = None
        if not guest_inline_delivery:
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
        downloads = int(book.get("downloads") or 0)
        fav_count = int(book.get("fav_count", 0) or 0)
        counts = _snapshot_reaction_counts(book)
        caption = build_book_caption(book, downloads, fav_count, counts)
        is_fav_now = bool(book.get("is_favorited"))
        user_reaction = book.get("user_reaction")
        if guest_inline_delivery:
            can_delete = False
            is_group_chat = True
            allow_management_buttons = False
        else:
            can_delete = await _can_show_delete_button(update, query.from_user.id)
            is_group_chat = _is_group_chat(update)
            allow_management_buttons = not is_group_chat
        # Audiobook flags: show listen if audiobook exists; allow add for admins
        has_ab = bool(book.get("has_audiobook"))
        can_add_ab = False
        if allow_management_buttons and callable(globals().get("is_audio_allowed")):
            try:
                can_add_ab = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
            except Exception:
                can_add_ab = False
        is_owner_user = bool(_is_owner_user(query.from_user.id)) if callable(globals().get("_is_owner_user")) else False
        show_listen_btn = False if guest_inline_delivery else (has_ab if (is_group_chat or is_owner_user) else True)
        ab_request_count = 0
        if can_add_ab and is_owner_user and callable(globals().get("count_pending_audiobook_requests")):
            try:
                ab_request_count = await run_blocking(count_pending_audiobook_requests, book_id)
            except Exception:
                ab_request_count = 0
        can_rename_books_fn = globals().get("can_rename_books")
        can_rename_book = bool(callable(can_rename_books_fn) and can_rename_books_fn(query.from_user.id) and not is_group_chat)
        more_books_url = None
        if guest_inline_delivery:
            username = (getattr(context.bot, "username", None) or "pdf_audio_kitoblar_bot").strip("@")
            if username:
                more_books_url = f"https://t.me/{username}"
        reactions_kb = build_book_keyboard(
            book_id,
            counts,
            is_fav_now,
            user_reaction,
            can_delete,
            can_rename_book,
            lang,
            has_audiobook=has_ab,
            can_add_audiobook=can_add_ab,
            show_listen_button=show_listen_btn,
            audiobook_request_count=ab_request_count,
            show_personal_state=not is_group_chat,
            show_favorite_button=not is_group_chat,
            more_books_url=more_books_url,
        )

        if guest_inline_delivery:
            if file_id:
                try:
                    sent = await context.bot.edit_message_media(
                        inline_message_id=query.inline_message_id,
                        media=InputMediaDocument(media=file_id, caption=caption),
                        reply_markup=None,
                    )
                    sent_ok = bool(sent)
                except Exception as e:
                    logger.error(f"Failed guest inline delivery by file_id {file_id}: {e}")
                    text = str(e or "")
                    if "Chat_send_docs_forbidden" in text or "CHAT_SEND_DOCS_FORBIDDEN" in text:
                        if source_guest_chat_id:
                            mark_forbidden_fn = globals().get("db_mark_guest_group_delivery_forbidden")
                            if callable(mark_forbidden_fn):
                                try:
                                    await _run_db_retry(mark_forbidden_fn, source_guest_chat_id)
                                except Exception as capability_error:
                                    logger.warning(
                                        "Failed to remember guest delivery forbidden state for group %s: %s",
                                        source_guest_chat_id,
                                        capability_error,
                                        exc_info=True,
                                    )
                        markup_builder = globals().get("build_guest_private_handoff_reply_markup")
                        if callable(markup_builder) and guest_handoff_token:
                            try:
                                new_markup = await markup_builder(
                                    context,
                                    handoff_token=guest_handoff_token,
                                    selected_book_id=book_id,
                                    actor_user_id=query.from_user.id,
                                    lang=lang,
                                )
                                if new_markup is not None:
                                    await context.bot.edit_message_reply_markup(
                                        inline_message_id=query.inline_message_id,
                                        reply_markup=new_markup,
                                    )
                            except Exception as markup_error:
                                logger.warning(
                                    "Failed to rebuild guest private handoff markup for %s: %s",
                                    book_id,
                                    markup_error,
                                    exc_info=True,
                                )
                        error_key = "guest_delivery_forbidden"
                    else:
                        error_key = "book_unavailable_send_failed"
            else:
                error_key = "book_unavailable_no_file"

        # --- Case 1: File ID available (prefer cache) ---
        elif file_id:
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
                        thumbnail = get_book_thumbnail_input()
                        with open(local_path, "rb") as f:
                            sent = await context.bot.send_document(
                                chat_id=query.message.chat_id,
                                document=InputFile(f, filename=_book_filename(book)),
                                caption=caption,
                                reply_markup=reactions_kb,
                                thumbnail=thumbnail,
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
                    thumbnail = get_book_thumbnail_input()
                    with open(local_path, "rb") as f:
                        sent = await context.bot.send_document(
                            chat_id=query.message.chat_id,
                            document=InputFile(f, filename=_book_filename(book)),
                            caption=caption,
                            reply_markup=reactions_kb,
                            thumbnail=thumbnail,
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
            if guest_inline_delivery:
                await safe_answer(query, message, show_alert=True)
            elif status_msg:
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
            if guest_inline_delivery:
                try:
                    await _run_db_retry(db_increment_counter, "guest_download_total", 1)
                except Exception as e:
                    logger.warning("guest download counter update failed: %s", e)
                if source_guest_chat_id:
                    mark_success_fn = globals().get("db_mark_guest_group_delivery_success")
                    if callable(mark_success_fn):
                        try:
                            await _run_db_retry(mark_success_fn, source_guest_chat_id)
                        except Exception as capability_error:
                            logger.warning(
                                "Failed to remember guest delivery success state for group %s: %s",
                                source_guest_chat_id,
                                capability_error,
                                exc_info=True,
                            )
                invalidate_audit_caches(context)

            # Update downloads count + refresh caption/keyboard
            try:
                await _run_db_retry(db_increment_book_download, book_id)
                invalidate_top_caches(context)
                book = await _load_delivery_snapshot()
                new_downloads = int(book.get("downloads") or 0)
                fav_count = int(book.get("fav_count", 0) or 0)
                counts = _snapshot_reaction_counts(book)
                if guest_inline_delivery:
                    is_fav_now = bool(book.get("is_favorited"))
                    user_reaction = book.get("user_reaction")
                    has_ab2 = bool(book.get("has_audiobook"))
                    can_add_ab2 = False
                    is_owner_user2 = bool(_is_owner_user(query.from_user.id)) if callable(globals().get("_is_owner_user")) else False
                    show_listen_btn2 = False
                    ab_request_count2 = 0
                    await context.bot.edit_message_caption(
                        inline_message_id=query.inline_message_id,
                        caption=build_book_caption(book, new_downloads, fav_count, counts),
                        reply_markup=build_book_keyboard(
                            book_id,
                            counts,
                            is_fav_now,
                            user_reaction,
                            can_delete,
                            False,
                            lang,
                            has_audiobook=has_ab2,
                            can_add_audiobook=False,
                            show_listen_button=show_listen_btn2,
                            audiobook_request_count=0,
                            show_personal_state=not is_group_chat,
                            show_favorite_button=False,
                            more_books_url=more_books_url,
                        ),
                    )
                elif sent:
                    is_fav_now = bool(book.get("is_favorited"))
                    user_reaction = book.get("user_reaction")
                    # Recompute audiobook flags for refreshed keyboard
                    has_ab2 = bool(book.get("has_audiobook"))
                    can_add_ab2 = False
                    if allow_management_buttons and callable(globals().get("is_audio_allowed")):
                        try:
                            can_add_ab2 = bool(await run_blocking(globals().get("is_audio_allowed"), query.from_user.id))
                        except Exception:
                            can_add_ab2 = False
                    is_owner_user2 = bool(_is_owner_user(query.from_user.id)) if callable(globals().get("_is_owner_user")) else False
                    show_listen_btn2 = has_ab2 if (is_group_chat or is_owner_user2) else True
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
                            can_rename_book,
                            lang,
                            has_audiobook=has_ab2,
                            can_add_audiobook=can_add_ab2,
                            show_listen_button=show_listen_btn2,
                            audiobook_request_count=ab_request_count2,
                            show_personal_state=not is_group_chat,
                            show_favorite_button=not is_group_chat,
                        ),
                    )
            except Exception as e:
                logger.error(f"Failed to update book stats caption: {e}", exc_info=True)

        if status_msg:
            try:
                await status_msg.edit_text(MESSAGES[lang]["sent"])
            except Exception:
                pass

    except Exception as e:
        logger.error(f"handle_book_selection failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        query = update.callback_query
        if query and getattr(query, "message", None) is not None:
            await query.message.reply_text(MESSAGES[lang]["error"])
        elif query:
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        raise


async def handle_book_rename_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
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
        if not data.startswith("bookrename:"):
            await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
            return

        book_id = data.split(":", 1)[1].strip()
        if not book_id:
            await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
            return

        can_rename_books_fn = globals().get("can_rename_books")
        if not callable(can_rename_books_fn) or not can_rename_books_fn(query.from_user.id):
            await safe_answer(query, MESSAGES[lang].get("not_authorized", "Not authorized"), show_alert=True)
            return
        if _is_group_chat(update):
            await safe_answer(
                query,
                MESSAGES[lang].get(
                    "book_rename_private_only",
                    "✏️ Book renaming is available only in private chat.",
                ),
                show_alert=True,
            )
            return

        book = await run_blocking(db_get_book_by_id, book_id)
        if not book:
            await safe_answer(query, MESSAGES[lang]["book_not_found"], show_alert=True)
            return

        context.user_data["pending_book_rename"] = {
            "book_id": book_id,
            "expires_at": time.time() + 300,
            "book_name": book.get("book_name"),
            "display_name": book.get("display_name"),
        }
        prompt = MESSAGES[lang].get(
            "book_rename_prompt",
            "✏️ Send the new name for this book.\nSend /cancel to abort.",
        )
        try:
            await query.message.reply_text(prompt.format(title=get_display_name(book)))
        except Exception:
            pass
        await safe_answer(query)
    except Exception as e:
        logger.error(f"handle_book_rename_callback failed: {e}", exc_info=True)
        lang = ensure_user_language(update, context)
        try:
            await query.message.reply_text(MESSAGES[lang]["error"])
        except Exception:
            pass
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
