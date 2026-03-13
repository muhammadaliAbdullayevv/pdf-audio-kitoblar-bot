from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any
from urllib.parse import urlparse

import safe_subprocess

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)
MESSAGES: dict[str, dict[str, str]] = {}


# Runtime-injected by bot.py via explicit configure(...) dependencies.
# These placeholders keep static analyzers quiet and produce a clear error if
# the module is used before configuration.
def _vdl_missing_dep(*args, **kwargs):
    raise RuntimeError("video_downloader module is not configured")


async def _vdl_missing_dep_async(*args, **kwargs):
    raise RuntimeError("video_downloader module is not configured")


_send_with_retry = _vdl_missing_dep_async
run_blocking = _vdl_missing_dep_async
_main_menu_keyboard = _vdl_missing_dep
safe_answer = _vdl_missing_dep_async
ensure_user_language = _vdl_missing_dep
spam_check_callback = _vdl_missing_dep
db_get_counters = _vdl_missing_dep
db_increment_counter = _vdl_missing_dep


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith("__") and k.endswith("__"):
            continue
        globals()[k] = v


_VIDEO_DL_SESSION_KEY = "video_dl_session"
_VIDEO_DL_AUDIO_KEY = "audio"
_VIDEO_DL_VIDEO_BEST_KEY = "video_best"
_VIDEO_DL_VIDEO_HEIGHT_OPTIONS = (144, 240, 360, 480, 720, 1080)
_VIDEO_DL_PROGRESS_RE = re.compile(r"(?P<pct>\d+(?:\.\d+)?)%")
_VIDEO_DL_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_VIDEO_DL_URL_RE = re.compile(r"https?://[^\s<>\"]+", re.IGNORECASE)
_VIDEO_DL_BARE_URL_RE = re.compile(
    r"(?<!@)\b(?:www\.)?[a-z0-9][a-z0-9.-]*\.[a-z]{2,}(?:/[^\s<>\"]*)?\b",
    re.IGNORECASE,
)


def _video_dl_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "start": (
                "🎬 Video yuklab olish\n\n"
                "Istalgan public video havolasini yuboring.\n"
                "Masalan: https://example.com/video"
            ),
            "checking": "🔎 Havola tekshirilmoqda...",
            "choose_quality": "👇 Format/sifatni tanlang:",
            "downloading": "⬇️ Video yuklab olinmoqda...",
            "sending": "📤 Telegramga yuborilmoqda...",
            "done": "✅ Yuklab olindi va yuborildi.",
            "cancelled": "Video yuklab olish bekor qilindi.",
            "expired": "Sessiya tugadi. Pastdagi menyudan Video Downloader bo‘limini qayta tanlang.",
            "empty": "Iltimos, havola yuboring.",
            "invalid_url": "❌ Havola topilmadi yoki noto‘g‘ri. Iltimos, to‘g‘ridan-to‘g‘ri URL yuboring.",
            "unsupported": "⚠️ Bu platforma bilan ishlashda xatolik yuz berdi. Boshqa havola yuboring.",
            "tools_missing": "⚠️ `yt-dlp` topilmadi. Serverda o‘rnatish kerak.",
            "ffmpeg_missing": "⚠️ Audio yuklab olish uchun `ffmpeg` kerak.",
            "metadata_failed": "⚠️ Havola ma'lumotlarini olib bo‘lmadi. Linkni tekshirib qayta urinib ko‘ring.",
            "download_failed": "⚠️ Yuklab olishda xatolik yuz berdi. Keyinroq qayta urinib ko‘ring.",
            "download_timeout": "⏱ Yuklab olish juda uzoq davom etdi. Iltimos, keyinroq qayta urinib ko‘ring.",
            "send_timeout": "⏱ Telegramga yuborishda vaqt tugadi. Iltimos, qayta urinib ko‘ring.",
            "send_rate_limited": "⏳ Telegram cheklovi sabab yuborish kechikdi. Birozdan keyin qayta urinib ko‘ring.",
            "send_network_error": "🌐 Tarmoq xatosi sabab yuborib bo‘lmadi. Qayta urinib ko‘ring.",
            "file_too_large": "⚠️ Fayl juda katta ({size_mb} MB). Telegram uchun kichikroq format tanlang.",
            "link_too_large_all_video": (
                "⚠️ Bu video hajmi bot yuborish limitidan katta ko‘rinadi.\n"
                "📩 Iltimos, hajmi kichikroq video link yuboring."
            ),
            "test_limit_reached": (
                "🧪 Bu funksiya hozir test rejimida.\n"
                "⏳ 3 ta yuklab olish limitiga yetdingiz. To‘liq ishga tushishini kuting."
            ),
            "session_other": "Bu video yuklab olish sessiyasi boshqa foydalanuvchiga tegishli.",
            "btn_audio": "🎵 Audio (MP3)",
            "btn_mp3_short": "🎧 MP3",
            "btn_preview": "🖼 Preview",
            "btn_trim": "✂️ Trim",
            "btn_open_source": "🔗 Open Source",
            "btn_cancel": "❌ Bekor qilish",
            "caption_audio": "🎵 Yuklab olingan audio",
            "caption_video": "🎬 Yuklab olingan video",
            "preview_title": "🎬 Video Downloader",
            "preview_source": "Manba",
            "preview_duration": "Davomiyligi",
            "preview_channel": "Kanal",
            "preview_formats": "Mavjud video balandliklari",
            "preview_estimates": "Taxminiy hajmlar",
            "quality_note": "Audio yoki video sifatini tanlang.",
            "downloading_progress": "⬇️ Yuklab olinmoqda: {progress}",
            "formats_header": "Formatlar yuklab olish uchun ↓",
            "quality_ok_icon": "✅",
            "quality_big_icon": "⚡",
            "quality_unknown_icon": "📹",
            "trim_coming_soon": "✂️ Video qirqish funksiyasi tez orada qo‘shiladi.",
            "preview_resent": "🖼 Preview qayta yuborildi.",
        }
    if lang == "ru":
        return {
            "start": (
                "🎬 Загрузка видео\n\n"
                "Отправьте любую публичную ссылку на видео.\n"
                "Например: https://example.com/video"
            ),
            "checking": "🔎 Проверяю ссылку...",
            "choose_quality": "👇 Выберите формат/качество:",
            "downloading": "⬇️ Скачиваю видео...",
            "sending": "📤 Отправляю в Telegram...",
            "done": "✅ Скачано и отправлено.",
            "cancelled": "Загрузка видео отменена.",
            "expired": "Сессия истекла. Снова откройте Video Downloader через меню ниже.",
            "empty": "Пожалуйста, отправьте ссылку.",
            "invalid_url": "❌ Ссылка не найдена или некорректна. Отправьте прямой URL.",
            "unsupported": "⚠️ Не удалось обработать эту платформу. Отправьте другую ссылку.",
            "tools_missing": "⚠️ `yt-dlp` не найден. Его нужно установить на сервере.",
            "ffmpeg_missing": "⚠️ Для загрузки аудио нужен `ffmpeg`.",
            "metadata_failed": "⚠️ Не удалось получить данные по ссылке. Проверьте ссылку и попробуйте снова.",
            "download_failed": "⚠️ Ошибка загрузки. Попробуйте позже.",
            "download_timeout": "⏱ Скачивание заняло слишком много времени. Попробуйте позже.",
            "send_timeout": "⏱ Истекло время отправки в Telegram. Попробуйте снова.",
            "send_rate_limited": "⏳ Ограничение Telegram. Повторите попытку немного позже.",
            "send_network_error": "🌐 Сетевая ошибка при отправке. Попробуйте снова.",
            "file_too_large": "⚠️ Файл слишком большой ({size_mb} MB). Выберите меньший формат.",
            "link_too_large_all_video": (
                "⚠️ Похоже, это видео превышает лимит отправки бота.\n"
                "📩 Пожалуйста, отправьте ссылку на видео меньшего размера."
            ),
            "test_limit_reached": (
                "🧪 Эта функция сейчас в тестовом режиме.\n"
                "⏳ Вы достигли лимита 3 загрузок. Пожалуйста, дождитесь полного запуска."
            ),
            "session_other": "Эта сессия загрузки видео принадлежит другому пользователю.",
            "btn_audio": "🎵 Аудио (MP3)",
            "btn_mp3_short": "🎧 MP3",
            "btn_preview": "🖼 Превью",
            "btn_trim": "✂️ Обрезка",
            "btn_open_source": "🔗 Открыть источник",
            "btn_cancel": "❌ Отмена",
            "caption_audio": "🎵 Загруженное аудио",
            "caption_video": "🎬 Загруженное видео",
            "preview_title": "🎬 Video Downloader",
            "preview_source": "Источник",
            "preview_duration": "Длительность",
            "preview_channel": "Канал",
            "preview_formats": "Доступные высоты видео",
            "preview_estimates": "Примерные размеры",
            "quality_note": "Выберите аудио или качество видео.",
            "downloading_progress": "⬇️ Скачивание: {progress}",
            "formats_header": "Форматы для скачивания ↓",
            "quality_ok_icon": "✅",
            "quality_big_icon": "⚡",
            "quality_unknown_icon": "📹",
            "trim_coming_soon": "✂️ Обрезка видео будет добавлена скоро.",
            "preview_resent": "🖼 Превью отправлено снова.",
        }
    return {
        "start": (
            "🎬 Video Downloader\n\n"
            "Send any public video link.\n"
            "Example: https://example.com/video"
        ),
        "checking": "🔎 Checking link...",
        "choose_quality": "👇 Choose format/quality:",
        "downloading": "⬇️ Downloading media...",
        "sending": "📤 Sending to Telegram...",
        "done": "✅ Downloaded and sent.",
        "cancelled": "Video downloader cancelled.",
        "expired": "Session expired. Please open Video Downloader again from the menu below.",
        "empty": "Please send a link.",
        "invalid_url": "❌ Link not found or invalid. Please send a direct URL.",
        "unsupported": "⚠️ Could not process this platform. Please send another link.",
        "tools_missing": "⚠️ `yt-dlp` is missing on the server.",
        "ffmpeg_missing": "⚠️ `ffmpeg` is required for audio download.",
        "metadata_failed": "⚠️ Could not read link metadata. Please check the link and try again.",
        "download_failed": "⚠️ Download failed. Please try again later.",
        "download_timeout": "⏱ Download took too long. Please try again later.",
        "send_timeout": "⏱ Sending to Telegram timed out. Please try again.",
        "send_rate_limited": "⏳ Telegram rate limit hit. Please try again shortly.",
        "send_network_error": "🌐 Network error while sending. Please try again.",
        "file_too_large": "⚠️ File is too large ({size_mb} MB). Choose a smaller format.",
        "link_too_large_all_video": (
            "⚠️ This video looks larger than the bot upload limit.\n"
            "📩 Please send another video link with a smaller size."
        ),
        "test_limit_reached": (
            "🧪 This feature is in test mode right now.\n"
            "⏳ You reached the 3-download limit. Please wait until it becomes fully available."
        ),
        "session_other": "This video download session belongs to another user.",
        "btn_audio": "🎵 Audio (MP3)",
        "btn_mp3_short": "🎧 MP3",
        "btn_preview": "🖼 Preview",
        "btn_trim": "✂️ Trim",
        "btn_open_source": "🔗 Open Source",
        "btn_cancel": "❌ Cancel",
        "caption_audio": "🎵 Downloaded audio",
        "caption_video": "🎬 Downloaded video",
        "preview_title": "🎬 Video Downloader",
        "preview_source": "Source",
        "preview_duration": "Duration",
        "preview_channel": "Channel",
        "preview_formats": "Available video heights",
        "preview_estimates": "Estimated sizes",
        "quality_note": "Choose audio or video quality.",
        "downloading_progress": "⬇️ Downloading: {progress}",
        "formats_header": "Download formats ↓",
        "quality_ok_icon": "✅",
        "quality_big_icon": "⚡",
        "quality_unknown_icon": "📹",
        "trim_coming_soon": "✂️ Video trimming will be added soon.",
        "preview_resent": "🖼 Preview sent again.",
    }


def _video_dl_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_VIDEO_DL_SESSION_KEY, None)


def _video_dl_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_VIDEO_DL_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _video_dl_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_VIDEO_DL_SESSION_KEY] = dict(session)


def _video_dl_extract_url_candidate(text: str) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None

    match = _VIDEO_DL_URL_RE.search(raw)
    candidate = match.group(0) if match else ""

    if not candidate:
        match = _VIDEO_DL_BARE_URL_RE.search(raw)
        if match:
            candidate = match.group(0)
            if not candidate.lower().startswith(("http://", "https://")):
                candidate = f"https://{candidate}"

    if not candidate:
        return None

    candidate = candidate.strip().rstrip(".,!?;:)]}'\"")
    return candidate or None


def _video_dl_supported_url(url: str) -> tuple[bool, str | None]:
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False, None
    if parsed.scheme not in {"http", "https"}:
        return False, None
    host = (parsed.netloc or "").lower().split(":")[0]
    if not host:
        return False, None
    if host.startswith("www."):
        host = host[4:]

    if host in {"youtube.com", "m.youtube.com", "youtu.be"} or host.endswith(".youtube.com"):
        return True, "youtube"
    if host in {"instagram.com", "m.instagram.com"} or host.endswith(".instagram.com"):
        return True, "instagram"
    if host in {"facebook.com", "m.facebook.com", "fb.watch"} or host.endswith(".facebook.com"):
        return True, "facebook"
    if host in {"x.com", "twitter.com", "mobile.twitter.com", "t.co"} or host.endswith(".x.com") or host.endswith(".twitter.com"):
        return True, "x"
    if host in {"tiktok.com", "m.tiktok.com", "vm.tiktok.com", "vt.tiktok.com"} or host.endswith(".tiktok.com"):
        return True, "tiktok"

    return True, "generic"


def _video_dl_tools_available() -> bool:
    return bool(shutil.which("yt-dlp"))


def _video_dl_is_quality_key(key: str) -> bool:
    if key in {_VIDEO_DL_AUDIO_KEY, _VIDEO_DL_VIDEO_BEST_KEY}:
        return True
    if not key.startswith("video_"):
        return False
    try:
        h = int(key.split("_", 1)[1])
    except Exception:
        return False
    return h in _VIDEO_DL_VIDEO_HEIGHT_OPTIONS


def _video_dl_quality_height(key: str) -> int | None:
    if not key.startswith("video_") or key == _VIDEO_DL_VIDEO_BEST_KEY:
        return None
    try:
        return int(key.split("_", 1)[1])
    except Exception:
        return None


def _video_dl_format_duration(seconds: int | float | None) -> str:
    try:
        total = int(seconds or 0)
    except Exception:
        total = 0
    if total <= 0:
        return "—"
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def _video_dl_human_size(size_bytes: int | float | None) -> str:
    try:
        num = float(size_bytes or 0)
    except Exception:
        num = 0.0
    if num <= 0:
        return "—"
    units = ["B", "KB", "MB", "GB"]
    idx = 0
    while num >= 1024 and idx < len(units) - 1:
        num /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(num)} {units[idx]}"
    return f"{num:.1f} {units[idx]}"


def _video_dl_size_mb_label(size_bytes: int | float | None) -> str | None:
    try:
        num = float(size_bytes or 0)
    except Exception:
        return None
    if num <= 0:
        return None
    mb = num / (1024 * 1024)
    return f"{mb:.1f} MB"


def _video_dl_local_mode_enabled() -> bool:
    val = str(os.getenv("TELEGRAM_BOT_API_LOCAL_MODE", "")).strip().lower()
    return val in {"1", "true", "yes", "on"}


def _video_dl_unlimited_mode_enabled() -> bool:
    force = str(os.getenv("VIDEO_DL_UNLIMITED", "")).strip().lower()
    if force in {"1", "true", "yes", "on"}:
        return True
    if _video_dl_local_mode_enabled():
        return True
    raw = str(os.getenv("VIDEO_DL_MAX_MB", "")).strip()
    if raw:
        try:
            if int(raw) <= 0:
                return True
        except Exception:
            pass
    return False


def _video_dl_max_mb_limit() -> int | None:
    if _video_dl_unlimited_mode_enabled():
        return None
    try:
        return max(1, int(os.getenv("VIDEO_DL_MAX_MB", "15")))
    except Exception:
        return 15


def _video_dl_max_bytes_limit() -> int | None:
    max_mb = _video_dl_max_mb_limit()
    if max_mb is None:
        return None
    return max_mb * 1024 * 1024


def _video_dl_user_counter_key(user_id: int) -> str:
    return f"video_dl_test_user_{int(user_id)}"


async def _video_dl_get_user_download_count(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> int:
    data = await run_blocking(db_get_counters, [_video_dl_user_counter_key(user_id)])
    try:
        return int((data or {}).get(_video_dl_user_counter_key(user_id), 0) or 0)
    except Exception:
        return 0


async def _video_dl_increment_user_download_count(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> int:
    return int(await run_blocking(db_increment_counter, _video_dl_user_counter_key(user_id), 1) or 0)


def _video_dl_clean_progress_line(line: str) -> str:
    text = _VIDEO_DL_ANSI_RE.sub("", line or "").strip()
    if text.startswith("[download]"):
        text = text[len("[download]"):].strip()
    return re.sub(r"\s+", " ", text)


def _video_dl_fmt_size(f: dict | None) -> int | None:
    if not isinstance(f, dict):
        return None
    for key in ("filesize", "filesize_approx"):
        try:
            val = int(f.get(key) or 0)
        except Exception:
            val = 0
        if val > 0:
            return val
    return None


def _video_dl_estimate_sizes(formats: list[dict]) -> dict[str, int]:
    valid = [f for f in formats if isinstance(f, dict)]
    audio_only = [
        f for f in valid
        if str(f.get("vcodec") or "none") == "none" and str(f.get("acodec") or "none") != "none"
    ]
    best_audio = None
    if audio_only:
        best_audio = max(
            audio_only,
            key=lambda f: (
                int(f.get("abr") or 0),
                int(f.get("tbr") or 0),
                int(_video_dl_fmt_size(f) or 0),
            ),
        )
    best_audio_size = _video_dl_fmt_size(best_audio) or 0
    estimates: dict[str, int] = {}
    if best_audio_size > 0:
        # MP3 re-encode often ends up smaller; this is a rough upper-ish estimate.
        estimates["audio"] = int(best_audio_size * 0.9)

    progressive = [
        f for f in valid
        if str(f.get("vcodec") or "none") != "none" and str(f.get("acodec") or "none") != "none"
    ]
    video_only = [
        f for f in valid
        if str(f.get("vcodec") or "none") != "none" and str(f.get("acodec") or "none") == "none"
    ]

    def _pick_progressive(max_h: int | None) -> dict | None:
        cands = []
        for f in progressive:
            h = f.get("height")
            if max_h is not None:
                try:
                    if not h or int(h) > max_h:
                        continue
                except Exception:
                    continue
            cands.append(f)
        if not cands:
            return None
        return max(cands, key=lambda f: (int(f.get("height") or 0), int(_video_dl_fmt_size(f) or 0)))

    def _pick_video_only(max_h: int | None) -> dict | None:
        cands = []
        for f in video_only:
            h = f.get("height")
            if max_h is not None:
                try:
                    if not h or int(h) > max_h:
                        continue
                except Exception:
                    continue
            cands.append(f)
        if not cands:
            return None
        return max(cands, key=lambda f: (int(f.get("height") or 0), int(_video_dl_fmt_size(f) or 0)))

    def _estimate_video(max_h: int | None) -> int | None:
        sizes = []
        prog = _pick_progressive(max_h)
        prog_size = _video_dl_fmt_size(prog)
        if prog_size:
            sizes.append(int(prog_size))
        vonly = _pick_video_only(max_h)
        vonly_size = _video_dl_fmt_size(vonly)
        if vonly_size:
            combined = int(vonly_size + (best_audio_size or 0))
            sizes.append(combined)
        return max(sizes) if sizes else None

    for hh in _VIDEO_DL_VIDEO_HEIGHT_OPTIONS:
        key = f"video_{hh}"
        size_est = _estimate_video(hh)
        if size_est:
            estimates[key] = int(size_est)
    for key, max_h in (("video_best", None),):
        size_est = _estimate_video(max_h)
        if size_est:
            estimates[key] = int(size_est)
    return estimates


def _video_dl_available_video_height_buttons(meta: dict) -> list[int]:
    heights = []
    for h in meta.get("heights") or []:
        try:
            hh = int(h)
        except Exception:
            continue
        if hh in _VIDEO_DL_VIDEO_HEIGHT_OPTIONS:
            heights.append(hh)
    heights = sorted(set(heights))
    if not heights and meta.get("size_estimates"):
        for hh in _VIDEO_DL_VIDEO_HEIGHT_OPTIONS:
            if (meta.get("size_estimates") or {}).get(f"video_{hh}"):
                heights.append(hh)
    return heights


def _video_dl_quality_keyboard(lang: str, meta: dict | None = None, size_estimates: dict[str, int] | None = None) -> InlineKeyboardMarkup:
    t = _video_dl_texts(lang)
    est = size_estimates or dict((meta or {}).get("size_estimates") or {})
    max_bytes = _video_dl_max_bytes_limit()

    def _label(base: str, quality_key: str) -> str:
        size = est.get(quality_key)
        size_mb = _video_dl_size_mb_label(size)
        if size_mb:
            return f"{base} {size_mb}"
        return base

    rows: list[list[InlineKeyboardButton]] = []
    video_buttons: list[InlineKeyboardButton] = []
    for h in _video_dl_available_video_height_buttons(meta or {}):
        qk = f"video_{h}"
        icon = "🎞️"
        if isinstance(est.get(qk), (int, float)):
            icon = "⚡" if (max_bytes is not None and int(est[qk]) > max_bytes) else "✅"
        video_buttons.append(
            InlineKeyboardButton(_label(f"{icon} {h}p", qk), callback_data=f"vdl:pick:{qk}")
        )
    if not video_buttons:
        # Fallback to common choices if metadata heights are missing
        for h in (360, 720):
            qk = f"video_{h}"
            video_buttons.append(InlineKeyboardButton(_label(f"🎞️ {h}p", qk), callback_data=f"vdl:pick:{qk}"))
    for i in range(0, len(video_buttons), 3):
        rows.append(video_buttons[i:i + 3])

    util_row = [InlineKeyboardButton(_label(t["btn_mp3_short"], "audio"), callback_data="vdl:pick:audio")]
    util_row.append(InlineKeyboardButton(t["btn_preview"], callback_data="vdl:preview"))
    rows.append(util_row)
    rows.append([InlineKeyboardButton(t["btn_trim"], callback_data="vdl:trim")])
    webpage_url = str((meta or {}).get("webpage_url") or "").strip()
    if webpage_url:
        rows.append([InlineKeyboardButton(t["btn_open_source"], url=webpage_url)])
    rows.append([InlineKeyboardButton(t["btn_cancel"], callback_data="vdl:cancel")])
    return InlineKeyboardMarkup(rows)


def _video_dl_extract_metadata_blocking(url: str) -> dict:
    if not _video_dl_tools_available():
        raise RuntimeError("yt-dlp-missing")
    max_output_chars = int(os.getenv("VIDEO_DL_META_MAX_OUTPUT_CHARS", "2000000") or "2000000")
    if max_output_chars < 20000:
        max_output_chars = 20000
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--no-playlist",
        "--skip-download",
        "--no-warnings",
        url,
    ]
    timeout_s = float(os.getenv("VIDEO_DL_META_TIMEOUT_S", "25") or "25")
    p = safe_subprocess.run(cmd, timeout_s=timeout_s, max_output_chars=max_output_chars, text=True)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "metadata failed").strip()[-800:])
    raw = (p.stdout or "").strip()
    if not raw:
        raise RuntimeError("empty metadata response")
    try:
        data = json.loads(raw)
    except Exception as e:
        if "…(truncated)…" in raw:
            raise RuntimeError("metadata JSON truncated; increase VIDEO_DL_META_MAX_OUTPUT_CHARS") from e
        raise
    formats = data.get("formats") or []
    heights = sorted(
        {
            int(f.get("height"))
            for f in formats
            if isinstance(f, dict) and f.get("height") and str(f.get("vcodec") or "none") != "none"
        }
    )
    return {
        "title": str(data.get("title") or "Video").strip()[:200],
        "duration": data.get("duration"),
        "uploader": str(data.get("uploader") or data.get("channel") or "").strip()[:120],
        "extractor": str(data.get("extractor_key") or data.get("extractor") or "").strip()[:60],
        "webpage_url": str(data.get("webpage_url") or url).strip(),
        "thumbnail": str(data.get("thumbnail") or "").strip(),
        "heights": heights[-8:],
        "size_estimates": _video_dl_estimate_sizes(formats),
    }


def _video_dl_preview_text(meta: dict, lang: str) -> str:
    t = _video_dl_texts(lang)
    title = str(meta.get("title") or "Video").strip()
    uploader = str(meta.get("uploader") or "—").strip() or "—"
    est = dict(meta.get("size_estimates") or {})
    max_bytes = _video_dl_max_bytes_limit()
    quality_lines = []
    for h in _video_dl_available_video_height_buttons(meta):
        qk = f"video_{h}"
        size = est.get(qk)
        icon = t["quality_unknown_icon"]
        if isinstance(size, (int, float)) and int(size) > 0:
            icon = t["quality_big_icon"] if (max_bytes is not None and int(size) > max_bytes) else t["quality_ok_icon"]
        size_lbl = _video_dl_size_mb_label(size) or "—"
        quality_lines.append(f"{icon} {h}p: {size_lbl}")
    audio_size_lbl = _video_dl_size_mb_label(est.get("audio"))
    if audio_size_lbl:
        quality_lines.append(f"🎧 MP3: {audio_size_lbl}")
    if not quality_lines:
        quality_lines.append(f"{t['quality_unknown_icon']} {t['preview_formats']}: —")

    caption_lines = [
        f"🎬 {title[:220]}",
        f"👤 {uploader[:120]}",
        "",
        *quality_lines[:10],
        "",
        t["formats_header"],
    ]
    return "\n".join(caption_lines).strip()


def _video_dl_find_output_file(temp_dir: str, stdout_text: str) -> str | None:
    lines = [ln.strip() for ln in (stdout_text or "").splitlines() if ln.strip()]
    for ln in reversed(lines):
        if os.path.isfile(ln):
            return ln
    files = []
    for name in os.listdir(temp_dir):
        path = os.path.join(temp_dir, name)
        if os.path.isfile(path):
            files.append(path)
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


async def _video_dl_send_media_with_retry(send_coro_factory, retries: int | None = None):
    attempts = retries or int(os.getenv("VIDEO_DL_SEND_RETRIES", "6") or "6")
    attempts = max(1, attempts)
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            return await send_coro_factory()
        except RetryAfter as e:
            last_err = e
            wait_s = float(getattr(e, "retry_after", 1) or 1) + 0.5
            await asyncio.sleep(min(wait_s, 30.0))
        except (TimedOut, NetworkError) as e:
            last_err = e
            await asyncio.sleep(min(1.0 * (2**i), 8.0))
    if last_err is not None:
        raise last_err
    raise RuntimeError("send-rate-limited")


async def _video_dl_download_with_progress(
    url: str,
    quality: str,
    *,
    lang: str,
    status_msg=None,
) -> dict:
    if not _video_dl_tools_available():
        raise RuntimeError("yt-dlp-missing")
    if quality == "audio" and not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg-missing")

    max_mb = _video_dl_max_mb_limit()
    max_bytes = _video_dl_max_bytes_limit()

    if not _video_dl_is_quality_key(quality):
        raise RuntimeError("unsupported-quality")

    t = _video_dl_texts(lang)
    td = tempfile.mkdtemp(prefix="vdl_")
    keep_temp_dir = False
    try:
        out_tpl = os.path.join(td, "%(title).80s [%(id)s].%(ext)s")
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-warnings",
            "--restrict-filenames",
            "--newline",
            "--print",
            "after_move:filepath",
            "-o",
            out_tpl,
        ]
        if quality == _VIDEO_DL_AUDIO_KEY:
            cmd += ["-x", "--audio-format", "mp3", "--audio-quality", "5"]
        else:
            if quality == _VIDEO_DL_VIDEO_BEST_KEY:
                fmt = "bestvideo+bestaudio/best"
            else:
                h = _video_dl_quality_height(quality)
                if not h:
                    raise RuntimeError("unsupported-quality")
                fmt = f"bestvideo*[height<={h}]+bestaudio/best[height<={h}]/best"
            cmd += ["-f", fmt, "--merge-output-format", "mp4"]
        cmd += [url]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []
        last_progress_text = ""
        last_progress_pct = -1.0
        last_edit_ts = 0.0

        max_lines = int(os.getenv("VIDEO_DL_MAX_LOG_LINES", "600") or "600")

        async def _read_stdout():
            if not proc.stdout:
                return
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                stdout_lines.append(line.decode("utf-8", errors="replace").strip())
                if len(stdout_lines) > max_lines:
                    del stdout_lines[: len(stdout_lines) - max_lines]

        async def _read_stderr():
            nonlocal last_progress_text, last_progress_pct, last_edit_ts
            if not proc.stderr:
                return
            while True:
                line_b = await proc.stderr.readline()
                if not line_b:
                    break
                raw = line_b.decode("utf-8", errors="replace").strip()
                if raw:
                    stderr_lines.append(raw)
                    if len(stderr_lines) > max_lines:
                        del stderr_lines[: len(stderr_lines) - max_lines]
                cleaned = _video_dl_clean_progress_line(raw)
                if not cleaned:
                    continue
                m = _VIDEO_DL_PROGRESS_RE.search(cleaned)
                now = time.time()
                if m:
                    try:
                        pct = float(m.group("pct"))
                    except Exception:
                        pct = last_progress_pct
                    if status_msg and (pct >= 100 or pct - last_progress_pct >= 1.0 or now - last_edit_ts >= 1.5):
                        last_progress_pct = pct
                        last_progress_text = cleaned
                        last_edit_ts = now
                        try:
                            await status_msg.edit_text(t["downloading_progress"].format(progress=cleaned))
                        except Exception:
                            pass

        stdout_task = asyncio.create_task(_read_stdout())
        stderr_task = asyncio.create_task(_read_stderr())
        timeout_s = float(os.getenv("VIDEO_DL_TIMEOUT_S", "600") or "600")
        try:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await proc.wait()
            except Exception:
                pass
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise RuntimeError(f"download-timeout:{int(timeout_s)}s")
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        if rc != 0:
            combined = "\n".join([*(stderr_lines[-20:]), *(stdout_lines[-5:])]).strip()
            raise RuntimeError((combined or "download failed")[-1200:])

        out_path = _video_dl_find_output_file(td, "\n".join(stdout_lines))
        if not out_path or not os.path.exists(out_path):
            raise RuntimeError("download-output-missing")

        size_bytes = os.path.getsize(out_path)
        if max_bytes is not None and size_bytes > max_bytes:
            raise RuntimeError(f"file-too-large:{size_bytes}:{max_mb}")

        keep_temp_dir = True
        return {
            "temp_dir": td,
            "file_path": out_path,
            "filename": os.path.basename(out_path),
            "size_bytes": size_bytes,
            "kind": "audio" if quality == _VIDEO_DL_AUDIO_KEY else "video",
        }
    finally:
        if not keep_temp_dir:
            shutil.rmtree(td, ignore_errors=True)


async def _video_dl_send_result(update: Update, result: dict, title: str, lang: str):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    t = _video_dl_texts(lang)
    caption_key = "caption_audio" if result.get("kind") == "audio" else "caption_video"
    caption = f"{t[caption_key]}\n📌 {title[:180]}"
    file_path = str(result.get("file_path") or "").strip()
    if not file_path or not os.path.exists(file_path):
        raise RuntimeError("download-output-missing")
    if result.get("kind") == "audio":
        async def _send_audio():
            with open(file_path, "rb") as audio_f:
                return await target_message.reply_audio(
                    audio=audio_f,
                    caption=caption,
                    title=title[:64],
                    filename=str(result.get("filename") or "audio.mp3"),
                )

        return await _video_dl_send_media_with_retry(_send_audio)
    try:
        async def _send_video():
            with open(file_path, "rb") as video_f:
                return await target_message.reply_video(
                    video=video_f,
                    caption=caption,
                    supports_streaming=True,
                    filename=str(result.get("filename") or "video.mp4"),
                )

        return await _video_dl_send_media_with_retry(_send_video)
    except Exception:
        async def _send_document():
            with open(file_path, "rb") as doc_f:
                return await target_message.reply_document(
                    document=doc_f,
                    caption=caption,
                    filename=str(result.get("filename") or "video.mp4"),
                )

        return await _video_dl_send_media_with_retry(_send_document)


def _video_dl_fail_reason(exc: Exception, raw_msg: str) -> str:
    msg = str(raw_msg or "").lower()
    if "yt-dlp-missing" in msg:
        return "tools_missing"
    if "ffmpeg-missing" in msg:
        return "ffmpeg_missing"
    if "download-timeout:" in msg or "timeout" in msg:
        return "timeout"
    if "retry after" in msg or "rate limit" in msg:
        return "rate_limit"
    if "file-too-large" in msg or "too large" in msg:
        return "too_large"
    if "invalid" in msg or "unsupported" in msg or "not found" in msg:
        return "invalid"
    if isinstance(exc, TimedOut):
        return "send_timeout"
    if isinstance(exc, NetworkError):
        return "network"
    if isinstance(exc, RetryAfter):
        return "rate_limit"
    return "other"


async def _video_dl_run_download_job(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    url: str,
    quality: str,
    title: str,
    lang: str,
    status_msg,
    job_key: str,
    platform: str = "generic",
):
    jobs = context.application.bot_data.setdefault("video_dl_jobs", {})
    t = _video_dl_texts(lang)
    result: dict[str, Any] | None = None
    started_mono = time.monotonic()
    try:
        result = await _video_dl_download_with_progress(url, quality, lang=lang, status_msg=status_msg)
        if status_msg:
            try:
                await status_msg.edit_text(t["sending"])
            except Exception:
                pass
        sent = await _video_dl_send_result(update, result, title, lang)
        if sent:
            try:
                await run_blocking(db_increment_counter, "video_downloads", 1)
                await run_blocking(db_increment_counter, "video_dl_jobs_total", 1)
                await run_blocking(db_increment_counter, "video_dl_success_total", 1)
                await run_blocking(db_increment_counter, f"video_dl_success_platform_{platform}", 1)
                elapsed_ms = max(0, int((time.monotonic() - started_mono) * 1000))
                await run_blocking(db_increment_counter, "video_dl_duration_total_ms", elapsed_ms)
            except Exception:
                logger.exception("video downloader: failed to increment success counters")
        if sent and update.effective_user:
            try:
                await _video_dl_increment_user_download_count(context, int(update.effective_user.id))
            except Exception:
                logger.exception("video downloader: failed to increment per-user test quota counter")
        if status_msg:
            try:
                await status_msg.edit_text(t["done"] if sent else MESSAGES[lang]["error"])
            except Exception:
                pass
    except Exception as e:
        msg = str(e)
        logger.error("video downloader job failed: %s", e, exc_info=True)
        fail = t["download_failed"]
        fail_reason = _video_dl_fail_reason(e, msg)
        try:
            await run_blocking(db_increment_counter, "video_dl_jobs_total", 1)
            await run_blocking(db_increment_counter, "video_dl_fail_total", 1)
            await run_blocking(db_increment_counter, f"video_dl_fail_platform_{platform}", 1)
            await run_blocking(db_increment_counter, f"video_dl_fail_reason_{fail_reason}", 1)
            elapsed_ms = max(0, int((time.monotonic() - started_mono) * 1000))
            await run_blocking(db_increment_counter, "video_dl_duration_total_ms", elapsed_ms)
        except Exception:
            logger.exception("video downloader: failed to increment failure counters")
        if "yt-dlp-missing" in msg:
            fail = t["tools_missing"]
        elif "ffmpeg-missing" in msg:
            fail = t["ffmpeg_missing"]
        elif "download-timeout:" in msg:
            fail = t["download_timeout"]
        elif isinstance(e, TimedOut):
            fail = t["send_timeout"]
        elif isinstance(e, NetworkError):
            fail = t["send_network_error"]
        elif isinstance(e, RetryAfter):
            fail = t["send_rate_limited"]
        elif "retry after" in msg.lower():
            fail = t["send_rate_limited"]
        elif msg.startswith("file-too-large:"):
            try:
                size_bytes = int(msg.split(":")[1])
                fail = t["file_too_large"].format(size_mb=max(1, round(size_bytes / (1024 * 1024))))
            except Exception:
                fail = t["file_too_large"].format(size_mb="?")
        if status_msg:
            try:
                await status_msg.edit_text(fail)
            except Exception:
                pass
    finally:
        temp_dir = str((result or {}).get("temp_dir") or "").strip()
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
        jobs.pop(job_key, None)


def _video_dl_link_all_video_options_too_large(meta: dict) -> bool:
    est = dict(meta.get("size_estimates") or {})
    video_sizes = [v for k, v in est.items() if str(k).startswith("video_")]
    known = [int(v) for v in video_sizes if isinstance(v, (int, float)) and int(v) > 0]
    if not known:
        return False
    max_bytes = _video_dl_max_bytes_limit()
    if max_bytes is None:
        return False
    return all(v > max_bytes for v in known)


async def _video_dl_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    try:
        _video_dl_clear_session(context)
        session = {
            "user_id": update.effective_user.id if update.effective_user else None,
            "phase": "awaiting_url",
            "expires_at": time.time() + 1800,
        }
        _video_dl_save_session(context, session)
        sent = await _send_with_retry(
            lambda: target_message.reply_text(
                _video_dl_texts(lang)["start"],
            )
        )
        if sent:
            session["prompt_chat_id"] = sent.chat_id
            session["prompt_message_id"] = sent.message_id
            _video_dl_save_session(context, session)
    except Exception:
        logger.exception("video downloader: failed to start session")
        _video_dl_clear_session(context)
        try:
            await target_message.reply_text(MESSAGES.get(lang, MESSAGES.get("en", {})).get("error", "⚠️ An error occurred."))
        except Exception:
            pass


async def _video_dl_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    try:
        if not update.message or not update.message.text:
            return False
        session = _video_dl_get_session(context)
        if not session:
            return False
        t = _video_dl_texts(lang)
        if time.time() > float(session.get("expires_at", 0) or 0):
            _video_dl_clear_session(context)
            await update.message.reply_text(t["expired"])
            return True
        if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
            return False

        txt = (update.message.text or "").strip()
        if not txt:
            await update.message.reply_text(t["empty"])
            return True
        if txt.lower() in {"cancel", "stop"}:
            _video_dl_clear_session(context)
            await update.message.reply_text(t["cancelled"])
            return True

        phase = str(session.get("phase") or "awaiting_url")
        if phase != "awaiting_url":
            await update.message.reply_text(t["choose_quality"])
            return True

        normalized_url = _video_dl_extract_url_candidate(txt)
        if not normalized_url:
            await update.message.reply_text(t["invalid_url"])
            return True

        ok, _platform = _video_dl_supported_url(normalized_url)
        if not ok:
            await update.message.reply_text(t["invalid_url"])
            return True
        if not _video_dl_tools_available():
            await update.message.reply_text(t["tools_missing"])
            return True

        status = await _send_with_retry(lambda: update.message.reply_text(t["checking"]))
        try:
            meta = await run_blocking(_video_dl_extract_metadata_blocking, normalized_url)
        except Exception as e:
            logger.info("video metadata extraction failed: %s", e)
            if status:
                try:
                    await status.edit_text(t["metadata_failed"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(t["metadata_failed"])
            return True

        if _video_dl_link_all_video_options_too_large(meta):
            _video_dl_clear_session(context)
            if status:
                try:
                    await status.edit_text(t["link_too_large_all_video"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(t["link_too_large_all_video"])
            return True

        session["phase"] = "awaiting_quality"
        session["url"] = normalized_url
        session["platform"] = str(_platform or "generic")
        session["meta"] = meta
        session["expires_at"] = time.time() + 1800
        _video_dl_save_session(context, session)
        preview_text = _video_dl_preview_text(meta, lang)
        thumb_url = str(meta.get("thumbnail") or "").strip()
        preview_keyboard = _video_dl_quality_keyboard(lang, meta)
        if thumb_url:
            sent = await _send_with_retry(
                lambda: update.message.reply_photo(
                    photo=thumb_url,
                    caption=preview_text[:1024],
                    reply_markup=preview_keyboard,
                )
            )
            if not sent:
                sent = await _send_with_retry(
                    lambda: update.message.reply_text(
                        preview_text,
                        reply_markup=preview_keyboard,
                    )
                )
        else:
            sent = await _send_with_retry(
                lambda: update.message.reply_text(
                    preview_text,
                    reply_markup=preview_keyboard,
                )
            )
        if sent:
            session["prompt_chat_id"] = sent.chat_id
            session["prompt_message_id"] = sent.message_id
            _video_dl_save_session(context, session)
        if status:
            try:
                await status.delete()
            except Exception:
                pass
        return True
    except Exception:
        logger.exception("video downloader: text handler failed")
        _video_dl_clear_session(context)
        try:
            await update.message.reply_text(_video_dl_texts(lang)["download_failed"])
        except Exception:
            pass
        return True


async def handle_video_downloader_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang = ensure_user_language(update, context)
    t = _video_dl_texts(lang)
    try:
        data = str(query.data or "")
        if not data.startswith("vdl:"):
            await safe_answer(query)
            return
        limited, wait_s = spam_check_callback(update, context)
        if limited:
            await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
            return
        session = _video_dl_get_session(context)
        if not session:
            await safe_answer(query, t["expired"], show_alert=True)
            return
        if time.time() > float(session.get("expires_at", 0) or 0):
            _video_dl_clear_session(context)
            await safe_answer(query, t["expired"], show_alert=True)
            return
        if (query.from_user.id if query.from_user else None) != session.get("user_id"):
            await safe_answer(query, t["session_other"], show_alert=True)
            return

        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        value = parts[2] if len(parts) > 2 else ""
        if action == "cancel":
            _video_dl_clear_session(context)
            await safe_answer(query, t["cancelled"])
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        if str(session.get("phase") or "") != "awaiting_quality":
            await safe_answer(query, t["expired"], show_alert=True)
            return

        url = str(session.get("url") or "").strip()
        meta = dict(session.get("meta") or {})
        if not url:
            _video_dl_clear_session(context)
            await safe_answer(query, t["expired"], show_alert=True)
            return

        if action == "preview":
            await safe_answer(query, t["preview_resent"])
            preview_keyboard = _video_dl_quality_keyboard(lang, meta)
            preview_text = _video_dl_preview_text(meta, lang)
            thumb_url = str(meta.get("thumbnail") or "").strip()
            if thumb_url and query.message:
                sent = await _send_with_retry(
                    lambda: query.message.reply_photo(
                        photo=thumb_url,
                        caption=preview_text[:1024],
                        reply_markup=preview_keyboard,
                    )
                )
                if not sent:
                    await _send_with_retry(lambda: query.message.reply_text(preview_text, reply_markup=preview_keyboard))
            elif query.message:
                await _send_with_retry(lambda: query.message.reply_text(preview_text, reply_markup=preview_keyboard))
            return

        if action == "trim":
            await safe_answer(query, t["trim_coming_soon"], show_alert=True)
            return

        if action != "pick" or not _video_dl_is_quality_key(value):
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return

        est = dict(meta.get("size_estimates") or {})
        size_est = est.get(value)
        max_bytes = _video_dl_max_bytes_limit()
        if max_bytes is not None and isinstance(size_est, (int, float)) and int(size_est) > max_bytes:
            await safe_answer(query, t["file_too_large"].format(size_mb=max(1, round(int(size_est) / (1024 * 1024)))), show_alert=True)
            return

        session["phase"] = "downloading"
        session["expires_at"] = time.time() + 3600
        _video_dl_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        status_msg = await _send_with_retry(lambda: query.message.reply_text(t["downloading"]))
        job_key = f"{query.from_user.id}:{int(time.time())}"
        jobs = context.application.bot_data.setdefault("video_dl_jobs", {})
        task = asyncio.create_task(
            _video_dl_run_download_job(
                update,
                context,
                url=url,
                quality=value,
                title=str(meta.get("title") or "Video"),
                lang=lang,
                status_msg=status_msg,
                job_key=job_key,
                platform=str(session.get("platform") or "generic"),
            )
        )
        jobs[job_key] = task
        _video_dl_clear_session(context)
    except Exception:
        logger.exception("video downloader: callback handler failed")
        _video_dl_clear_session(context)
        try:
            await safe_answer(query, MESSAGES.get(lang, MESSAGES.get("en", {})).get("error", "⚠️ An error occurred."), show_alert=True)
        except Exception:
            pass
        try:
            if query.message:
                await query.message.reply_text(t["download_failed"])
        except Exception:
            pass
