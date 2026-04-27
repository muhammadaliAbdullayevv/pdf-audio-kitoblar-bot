from __future__ import annotations

import io
import logging
import os
import re
import shutil
import tempfile
import time
from typing import Any

import safe_subprocess

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)
MESSAGES: dict[str, dict[str, str]] = {}


def _audio_missing_dep(*args, **kwargs):
    raise RuntimeError("audio_converter module is not configured")


async def _audio_missing_dep_async(*args, **kwargs):
    raise RuntimeError("audio_converter module is not configured")


_send_with_retry = _audio_missing_dep_async
run_blocking = _audio_missing_dep_async
safe_answer = _audio_missing_dep_async
ensure_user_language = _audio_missing_dep
spam_check_callback = _audio_missing_dep
_main_menu_keyboard = _audio_missing_dep


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith("__") and k.endswith("__"):
            continue
        globals()[k] = v


_AUDIO_CONV_SESSION_KEY = "audio_converter_session"
_AUDIO_CONV_RANGE_RE = re.compile(r"^\s*([0-9:.]+)\s*[-–]\s*([0-9:.]+)\s*$")


def _audio_conv_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "start": "🎛️ Audio muharriri yoqildi.\n📥 Ovozli xabar yoki audio yuboring.\n👇 Tugmalar bilan kesing, formatni o‘zgartiring, muqova qo‘shing va nomini o‘zgartiring.",
            "prompt_send_audio": "📥 Ovozli xabar yoki audio yuboring.",
            "source_saved": "✅ Qabul qilindi: {name}",
            "choose_action": "👇 Amalni tanlang.",
            "ask_range_mp3": "✂️ MP3 kesish: `1:30-2:50` yozing.",
            "ask_range_voice": "✂️ Ovozli xabarni kesish: `1:30-2:50` yozing.",
            "invalid_range": "⚠️ Noto‘g‘ri format. Misol: `1:30-2:50`.",
            "ask_name": "🏷️ Yangi nom yuboring (masalan: `lesson_1`).",
            "name_set": "✅ Nom saqlandi: {name}",
            "name_invalid": "⚠️ Noto‘g‘ri nom. Harf/raqam ishlating.",
            "ask_cover": "🖼 MP3 uchun muqova rasmi yuboring (photo yoki image).",
            "cover_set": "✅ Muqova saqlandi. MP3 ga qo‘llanmoqda...",
            "cover_invalid": "⚠️ Faqat photo yoki image yuboring.",
            "cover_too_large": "⚠️ Rasm katta ({size_mb} MB). Kichikroq yuboring.",
            "cover_only_mp3": "ℹ️ Muqova faqat MP3 ga qo‘shiladi.",
            "cover_failed": "⚠️ Muqova qo‘shilmadi. Fayl muqovasiz yuborildi.",
            "working": "⏳ Qayta ishlanmoqda...",
            "done_mp3": "✅ Tayyor. MP3 yuborildi.",
            "done_voice": "✅ Tayyor. Ovozli xabar yuborildi.",
            "failed": "⚠️ Qayta ishlashda xatolik. Qayta urinib ko‘ring.",
            "tools_missing": "⚠️ `ffmpeg` topilmadi.",
            "session_other": "Bu sessiya boshqa foydalanuvchiga tegishli.",
            "expired": "⌛ Sessiya tugadi. Menyudan qayta oching.",
            "cancelled": "❌ Bekor qilindi.",
            "completed": "✅ Yakunlandi. Yakuniy fayl yuborildi.",
            "too_large": "⚠️ Fayl katta ({size_mb} MB). Kichikroq yuboring.",
            "unsupported": "⚠️ Faqat ovozli xabar/audio fayllar qabul qilinadi.",
            "no_source": "⚠️ Avval ovozli xabar yoki audio yuboring.",
            "already_mp3": "ℹ️ Bu fayl allaqachon MP3.",
            "already_voice": "ℹ️ Bu fayl allaqachon ovozli xabar formatida.",
            "btn_to_mp3": "🎵 MP3",
            "btn_to_voice": "🎙️ Ovozli xabar",
            "btn_cut_mp3": "✂️ MP3 kesish",
            "btn_cut_voice": "✂️ Ovozli xabarni kesish",
            "btn_set_cover": "🖼 Muqova",
            "btn_rename": "🏷️ Nom",
            "btn_complete": "✅ Yakunlash",
            "btn_cancel": "❌ Bekor",
            "caption_mp3": "🎵 MP3: {name}",
            "caption_voice": "🎙️ Ovozli xabar: {name}",
        }
    if lang == "ru":
        return {
            "start": "🎛️ Аудиоредактор запущен.\n📥 Отправьте голосовое или аудио.\n👇 Кнопками можно обрезать, конвертировать, добавить обложку и переименовать.",
            "prompt_send_audio": "📥 Отправьте голосовое или аудио.",
            "source_saved": "✅ Получено: {name}",
            "choose_action": "👇 Выберите действие.",
            "ask_range_mp3": "✂️ Обрезка MP3: отправьте `1:30-2:50`.",
            "ask_range_voice": "✂️ Обрезка голосового: отправьте `1:30-2:50`.",
            "invalid_range": "⚠️ Неверный формат. Пример: `1:30-2:50`.",
            "ask_name": "🏷️ Отправьте новое имя (например: `lesson_1`).",
            "name_set": "✅ Имя сохранено: {name}",
            "name_invalid": "⚠️ Неверное имя. Используйте буквы/цифры.",
            "ask_cover": "🖼 Отправьте обложку для MP3 (photo или image).",
            "cover_set": "✅ Обложка сохранена. Применяю к MP3...",
            "cover_invalid": "⚠️ Отправьте только photo или image.",
            "cover_too_large": "⚠️ Картинка слишком большая ({size_mb} MB).",
            "cover_only_mp3": "ℹ️ Обложка доступна только для MP3.",
            "cover_failed": "⚠️ Не удалось добавить обложку. Файл отправлен без обложки.",
            "working": "⏳ Обработка...",
            "done_mp3": "✅ Готово. MP3 отправлен.",
            "done_voice": "✅ Готово. Голосовое отправлено.",
            "failed": "⚠️ Ошибка обработки. Попробуйте снова.",
            "tools_missing": "⚠️ `ffmpeg` не найден.",
            "session_other": "Эта сессия принадлежит другому пользователю.",
            "expired": "⌛ Сессия истекла. Откройте меню снова.",
            "cancelled": "❌ Отменено.",
            "completed": "✅ Завершено. Финальный файл отправлен.",
            "too_large": "⚠️ Файл слишком большой ({size_mb} MB).",
            "unsupported": "⚠️ Поддерживаются только голосовые/аудио файлы.",
            "no_source": "⚠️ Сначала отправьте голосовое или аудио.",
            "already_mp3": "ℹ️ Этот файл уже MP3.",
            "already_voice": "ℹ️ Этот файл уже в голосовом формате.",
            "btn_to_mp3": "🎵 MP3",
            "btn_to_voice": "🎙️ В голосовое",
            "btn_cut_mp3": "✂️ Обрезать MP3",
            "btn_cut_voice": "✂️ Обрезать голосовое",
            "btn_set_cover": "🖼 Обложка",
            "btn_rename": "🏷️ Имя",
            "btn_complete": "✅ Готово",
            "btn_cancel": "❌ Отмена",
            "caption_mp3": "🎵 MP3: {name}",
            "caption_voice": "🎙️ Голосовое: {name}",
        }
    return {
        "start": "🎛️ Audio Editor is on.\n📥 Send a voice or audio file.\n👇 Use buttons to cut, convert, add cover, and rename.",
        "prompt_send_audio": "📥 Send voice or audio.",
        "source_saved": "✅ Received: {name}",
        "choose_action": "👇 Choose an action.",
        "ask_range_mp3": "✂️ Cut MP3: send `1:30-2:50`.",
        "ask_range_voice": "✂️ Cut voice: send `1:30-2:50`.",
        "invalid_range": "⚠️ Invalid format. Example: `1:30-2:50`.",
        "ask_name": "🏷️ Send a new name (example: `lesson_1`).",
        "name_set": "✅ Name saved: {name}",
        "name_invalid": "⚠️ Invalid name. Use letters/numbers.",
        "ask_cover": "🖼 Send a cover image for MP3 (photo or image).",
        "cover_set": "✅ Cover saved. Applying to MP3...",
        "cover_invalid": "⚠️ Send only photo or image.",
        "cover_too_large": "⚠️ Image is too large ({size_mb} MB).",
        "cover_only_mp3": "ℹ️ Cover is available only for MP3.",
        "cover_failed": "⚠️ Cover failed. File sent without cover.",
        "working": "⏳ Processing...",
        "done_mp3": "✅ Done. MP3 sent.",
        "done_voice": "✅ Done. Voice sent.",
        "failed": "⚠️ Processing failed. Try again.",
        "tools_missing": "⚠️ `ffmpeg` is missing.",
        "session_other": "This session belongs to another user.",
        "expired": "⌛ Session expired. Open the menu again.",
        "cancelled": "❌ Cancelled.",
        "completed": "✅ Completed. Final file sent.",
        "too_large": "⚠️ File is too large ({size_mb} MB).",
        "unsupported": "⚠️ Only voice/audio files are supported.",
        "no_source": "⚠️ Send voice or audio first.",
        "already_mp3": "ℹ️ This file is already MP3.",
        "already_voice": "ℹ️ This file is already voice format.",
        "btn_to_mp3": "🎵 MP3",
        "btn_to_voice": "🎙️ To voice",
        "btn_cut_mp3": "✂️ Cut MP3",
        "btn_cut_voice": "✂️ Cut voice",
        "btn_set_cover": "🖼 Cover",
        "btn_rename": "🏷️ Rename",
        "btn_complete": "✅ Complete",
        "btn_cancel": "❌ Cancel",
        "caption_mp3": "🎵 MP3: {name}",
        "caption_voice": "🎙️ Voice: {name}",
    }

def _audio_conv_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_AUDIO_CONV_SESSION_KEY, None)


def _audio_conv_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_AUDIO_CONV_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _audio_conv_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_AUDIO_CONV_SESSION_KEY] = dict(session)


def _audio_conv_sanitize_name(name: str) -> str:
    cleaned = re.sub(r"[^\w\-. ]+", "", str(name or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned).strip("._ ")
    if not cleaned:
        return ""
    if "." in cleaned:
        cleaned = cleaned.rsplit(".", 1)[0]
    return cleaned[:64]


def _audio_conv_default_name(source_name: str | None, source_kind: str) -> str:
    candidate = _audio_conv_sanitize_name(source_name or "")
    if candidate:
        return candidate
    stamp = int(time.time())
    prefix = "voice" if source_kind == "voice" else "audio"
    return f"{prefix}_{stamp}"


def _audio_conv_max_bytes() -> int:
    try:
        max_mb = max(1, int(os.getenv("AUDIO_CONVERTER_MAX_MB", "64") or "64"))
    except Exception:
        max_mb = 64
    return max_mb * 1024 * 1024


def _audio_conv_cover_max_bytes() -> int:
    try:
        max_mb = max(1, int(os.getenv("AUDIO_CONVERTER_COVER_MAX_MB", "5") or "5"))
    except Exception:
        max_mb = 5
    return max_mb * 1024 * 1024


def _audio_conv_time_to_ffmpeg(v: float) -> str:
    if v < 0:
        v = 0.0
    return f"{v:.3f}"


def _audio_conv_parse_time_token(token: str) -> float | None:
    t = str(token or "").strip()
    if not t:
        return None
    try:
        if re.fullmatch(r"\d+(?:\.\d+)?", t):
            return float(t)
        parts = t.split(":")
        if len(parts) == 2:
            mm, ss = parts
            return float(mm) * 60.0 + float(ss)
        if len(parts) == 3:
            hh, mm, ss = parts
            return float(hh) * 3600.0 + float(mm) * 60.0 + float(ss)
    except Exception:
        return None
    return None


def _audio_conv_parse_range(text: str) -> tuple[float, float] | None:
    m = _AUDIO_CONV_RANGE_RE.match(str(text or ""))
    if not m:
        return None
    start_s = _audio_conv_parse_time_token(m.group(1))
    end_s = _audio_conv_parse_time_token(m.group(2))
    if start_s is None or end_s is None:
        return None
    if start_s < 0 or end_s <= start_s:
        return None
    if end_s - start_s > 3600 * 4:
        return None
    return start_s, end_s


def _audio_conv_detect_source_format(source_kind: str, file_name: str | None, mime_type: str | None) -> str:
    if source_kind == "voice":
        return "voice"
    name = str(file_name or "").lower()
    mime = str(mime_type or "").lower()

    if mime in {"audio/mpeg", "audio/mp3", "audio/x-mp3"} or name.endswith(".mp3"):
        return "mp3"
    if mime in {"audio/ogg", "audio/opus", "audio/x-opus+ogg", "audio/oga"} or name.endswith(".ogg") or name.endswith(".opus"):
        return "voice"
    return "audio"


def _audio_conv_effective_output_mode(source_format: str | None) -> str:
    return "voice" if str(source_format or "") == "voice" else "mp3"


def _audio_conv_action_keyboard(
    lang: str,
    *,
    has_source: bool,
    current_name: str | None = None,
    source_format: str | None = None,
) -> InlineKeyboardMarkup:
    t = _audio_conv_texts(lang)
    rename_label = t["btn_rename"]
    if current_name:
        rename_label = f"{rename_label} ({str(current_name)[:16]})"

    rows: list[list[InlineKeyboardButton]] = []
    fmt = str(source_format or "")

    if has_source:
        if fmt == "voice":
            rows.append(
                [
                    InlineKeyboardButton(t["btn_to_mp3"], callback_data="atool:to_mp3"),
                    InlineKeyboardButton(t["btn_cut_voice"], callback_data="atool:cut_voice"),
                ]
            )
            rows.append([InlineKeyboardButton(rename_label, callback_data="atool:rename")])
        elif fmt == "mp3":
            rows.append(
                [
                    InlineKeyboardButton(t["btn_to_voice"], callback_data="atool:to_voice"),
                    InlineKeyboardButton(t["btn_cut_mp3"], callback_data="atool:cut_mp3"),
                ]
            )
            rows.append(
                [
                    InlineKeyboardButton(t["btn_set_cover"], callback_data="atool:set_cover"),
                    InlineKeyboardButton(rename_label, callback_data="atool:rename"),
                ]
            )
        else:
            rows.append(
                [
                    InlineKeyboardButton(t["btn_to_mp3"], callback_data="atool:to_mp3"),
                    InlineKeyboardButton(t["btn_to_voice"], callback_data="atool:to_voice"),
                ]
            )
            rows.append(
                [
                    InlineKeyboardButton(t["btn_cut_mp3"], callback_data="atool:cut_mp3"),
                    InlineKeyboardButton(t["btn_cut_voice"], callback_data="atool:cut_voice"),
                ]
            )
            rows.append([InlineKeyboardButton(rename_label, callback_data="atool:rename")])

    rows.append(
        [
            InlineKeyboardButton(t["btn_complete"], callback_data="atool:complete"),
            InlineKeyboardButton(t["btn_cancel"], callback_data="atool:cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _audio_conv_extract_media_info(message) -> dict | None:
    if not message:
        return None

    if getattr(message, "voice", None):
        v = message.voice
        return {
            "file_id": v.file_id,
            "file_unique_id": getattr(v, "file_unique_id", None),
            "file_size": int(getattr(v, "file_size", 0) or 0),
            "file_name": "voice_message",
            "mime_type": "audio/ogg",
            "source_kind": "voice",
            "source_format": "voice",
        }

    if getattr(message, "audio", None):
        a = message.audio
        file_name = getattr(a, "file_name", None) or getattr(a, "title", None) or "audio"
        mime_type = str(getattr(a, "mime_type", "") or "")
        source_kind = "audio"
        return {
            "file_id": a.file_id,
            "file_unique_id": getattr(a, "file_unique_id", None),
            "file_size": int(getattr(a, "file_size", 0) or 0),
            "file_name": file_name,
            "mime_type": mime_type,
            "source_kind": source_kind,
            "source_format": _audio_conv_detect_source_format(source_kind, file_name, mime_type),
        }

    doc = getattr(message, "document", None)
    mime = str(getattr(doc, "mime_type", "") or "").lower() if doc else ""
    if doc and mime.startswith("audio/"):
        file_name = getattr(doc, "file_name", None) or "audio_file"
        source_kind = "audio"
        return {
            "file_id": doc.file_id,
            "file_unique_id": getattr(doc, "file_unique_id", None),
            "file_size": int(getattr(doc, "file_size", 0) or 0),
            "file_name": file_name,
            "mime_type": mime,
            "source_kind": source_kind,
            "source_format": _audio_conv_detect_source_format(source_kind, file_name, mime),
        }

    return None


def _audio_conv_extract_cover_info(message) -> dict | None:
    if not message:
        return None

    if getattr(message, "photo", None):
        p = message.photo[-1]
        return {
            "file_id": p.file_id,
            "file_unique_id": getattr(p, "file_unique_id", None),
            "file_size": int(getattr(p, "file_size", 0) or 0),
            "mime_type": "image/jpeg",
        }

    doc = getattr(message, "document", None)
    mime = str(getattr(doc, "mime_type", "") or "").lower() if doc else ""
    if doc and mime.startswith("image/"):
        return {
            "file_id": doc.file_id,
            "file_unique_id": getattr(doc, "file_unique_id", None),
            "file_size": int(getattr(doc, "file_size", 0) or 0),
            "mime_type": mime,
        }

    return None


async def _audio_conv_download_source_bytes(context: ContextTypes.DEFAULT_TYPE, file_id: str) -> bytes:
    f = await context.bot.get_file(file_id)
    content = await f.download_as_bytearray()
    return bytes(content or b"")


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
            cmd[2:2] = ["-ss", _audio_conv_time_to_ffmpeg(float(start_s))]
        if end_s is not None:
            duration = max(0.01, float(end_s) - float(start_s or 0.0))
            cmd.extend(["-t", _audio_conv_time_to_ffmpeg(duration)])
        cmd.extend(["-vn"])
        if output_mode == "voice":
            cmd.extend(["-ac", "1", "-ar", "48000", "-c:a", "libopus", "-b:a", "32k", out_path])
        else:
            cmd.extend(["-c:a", "libmp3lame", "-q:a", "4", out_path])

        timeout_s = float(os.getenv("AUDIO_CONVERTER_FFMPEG_TIMEOUT_S", "150") or "150")
        p = safe_subprocess.run(cmd, timeout_s=timeout_s, max_output_chars=8000, text=False)
        if p.returncode != 0:
            err = p.stderr
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
            "ffmpeg",
            "-y",
            "-i",
            in_audio,
            "-i",
            in_cover,
            "-map",
            "0:a",
            "-map",
            "1:v",
            "-c:a",
            "copy",
            "-c:v",
            "mjpeg",
            "-id3v2_version",
            "3",
            "-metadata:s:v",
            "title=Album cover",
            "-metadata:s:v",
            "comment=Cover (front)",
            out_mp3,
        ]
        p = safe_subprocess.run(cmd_copy, timeout_s=timeout_s, max_output_chars=8000, text=False)

        if p.returncode != 0:
            cmd_reencode = [
                "ffmpeg",
                "-y",
                "-i",
                in_audio,
                "-i",
                in_cover,
                "-map",
                "0:a",
                "-map",
                "1:v",
                "-c:a",
                "libmp3lame",
                "-q:a",
                "4",
                "-c:v",
                "mjpeg",
                "-id3v2_version",
                "3",
                "-metadata:s:v",
                "title=Album cover",
                "-metadata:s:v",
                "comment=Cover (front)",
                out_mp3,
            ]
            p = safe_subprocess.run(cmd_reencode, timeout_s=timeout_s, max_output_chars=8000, text=False)

        if p.returncode != 0:
            err = p.stderr
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            raise RuntimeError((str(err or "ffmpeg cover failed")).strip()[-800:])

        if not os.path.exists(out_mp3):
            raise RuntimeError("ffmpeg cover output missing")
        with open(out_mp3, "rb") as fp:
            return fp.read()


async def _audio_conv_send_result(
    update: Update,
    *,
    output_bytes: bytes,
    output_mode: str,
    base_name: str,
    lang: str,
):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    t = _audio_conv_texts(lang)
    bio = io.BytesIO(output_bytes)
    if output_mode == "voice":
        bio.name = f"{base_name}.ogg"
        return await _send_with_retry(
            lambda: target_message.reply_voice(
                voice=bio,
                caption=t["caption_voice"].format(name=base_name),
            )
        )

    bio.name = f"{base_name}.mp3"
    return await _send_with_retry(
        lambda: target_message.reply_audio(
            audio=bio,
            caption=t["caption_mp3"].format(name=base_name),
            title=base_name[:64],
        )
    )


async def _audio_conv_prompt_actions(update: Update, context: ContextTypes.DEFAULT_TYPE, *, lang: str) -> None:
    session = _audio_conv_get_session(context)
    if not session or not session.get("active"):
        return
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    t = _audio_conv_texts(lang)
    await target_message.reply_text(
        t["choose_action"],
        reply_markup=_audio_conv_action_keyboard(
            lang,
            has_source=bool(session.get("file_id")),
            current_name=str(session.get("target_name") or ""),
            source_format=str(session.get("source_format") or ""),
        ),
    )


async def _audio_conv_process_and_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    lang: str,
    output_mode: str,
    start_s: float | None = None,
    end_s: float | None = None,
) -> bool:
    t = _audio_conv_texts(lang)
    session = _audio_conv_get_session(context)
    if not session or not session.get("file_id"):
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if target_message:
            await target_message.reply_text(t["no_source"])
        return False

    if not shutil.which("ffmpeg"):
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if target_message:
            await target_message.reply_text(t["tools_missing"])
        return False

    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return False

    status = await _send_with_retry(lambda: target_message.reply_text(t["working"]))
    try:
        source_bytes = await _audio_conv_download_source_bytes(context, str(session.get("file_id")))
        out_bytes = await run_blocking(
            _audio_conv_transform_blocking,
            source_bytes,
            output_mode=output_mode,
            start_s=start_s,
            end_s=end_s,
        )

        if output_mode == "mp3" and session.get("cover_file_id"):
            try:
                cover_bytes = await _audio_conv_download_source_bytes(context, str(session.get("cover_file_id")))
                out_bytes = await run_blocking(_audio_conv_apply_cover_blocking, out_bytes, cover_bytes)
            except Exception as cover_exc:
                logger.warning("audio converter cover apply failed: %s", cover_exc, exc_info=True)
                try:
                    await target_message.reply_text(t["cover_failed"])
                except Exception:
                    pass

        base_name = _audio_conv_sanitize_name(str(session.get("target_name") or "")) or _audio_conv_default_name(
            str(session.get("file_name") or ""),
            str(session.get("source_kind") or "audio"),
        )
        sent = await _audio_conv_send_result(
            update,
            output_bytes=out_bytes,
            output_mode=output_mode,
            base_name=base_name,
            lang=lang,
        )

        if sent:
            session.update(
                {
                    "phase": "ready",
                    "expires_at": time.time() + 3600,
                    "file_id": (getattr(getattr(sent, "voice", None), "file_id", None) or getattr(getattr(sent, "audio", None), "file_id", None)),
                    "file_unique_id": (
                        getattr(getattr(sent, "voice", None), "file_unique_id", None)
                        or getattr(getattr(sent, "audio", None), "file_unique_id", None)
                    ),
                    "file_size": (
                        int(getattr(getattr(sent, "voice", None), "file_size", 0) or 0)
                        or int(getattr(getattr(sent, "audio", None), "file_size", 0) or 0)
                    ),
                    "file_name": f"{base_name}.ogg" if output_mode == "voice" else f"{base_name}.mp3",
                    "source_kind": "voice" if output_mode == "voice" else "audio",
                    "source_format": "voice" if output_mode == "voice" else "mp3",
                    "target_name": base_name,
                }
            )
            _audio_conv_save_session(context, session)

        if status:
            try:
                await status.edit_text(t["done_mp3"] if output_mode == "mp3" else t["done_voice"])
            except Exception:
                pass

        if sent:
            await _audio_conv_prompt_actions(update, context, lang=lang)
        return bool(sent)
    except Exception as e:
        logger.warning("audio converter processing failed: %s", e, exc_info=True)
        if status:
            try:
                await status.edit_text(t["failed"])
            except Exception:
                pass
        else:
            await target_message.reply_text(t["failed"])
        return False


async def _audio_conv_complete_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, *, lang: str) -> bool:
    t = _audio_conv_texts(lang)
    session = _audio_conv_get_session(context)
    if not session or not session.get("file_id"):
        target_message = update.message or (update.callback_query.message if update.callback_query else None)
        if target_message:
            await target_message.reply_text(t["no_source"])
        return False

    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return False

    base_name = _audio_conv_sanitize_name(str(session.get("target_name") or "")) or _audio_conv_default_name(
        str(session.get("file_name") or ""),
        str(session.get("source_kind") or "audio"),
    )
    mode = _audio_conv_effective_output_mode(str(session.get("source_format") or ""))

    if mode == "voice":
        sent = await _send_with_retry(
            lambda: target_message.reply_voice(
                voice=str(session.get("file_id")),
                caption=t["caption_voice"].format(name=base_name),
            )
        )
    else:
        sent = await _send_with_retry(
            lambda: target_message.reply_audio(
                audio=str(session.get("file_id")),
                caption=t["caption_mp3"].format(name=base_name),
                title=base_name[:64],
            )
        )

    if not sent:
        return False

    _audio_conv_clear_session(context)
    uid = update.effective_user.id if update.effective_user else None
    await target_message.reply_text(t["completed"], reply_markup=_main_menu_keyboard(lang, "other", uid))
    return True


async def _audio_conv_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    _audio_conv_clear_session(context)
    session = {
        "active": True,
        "user_id": update.effective_user.id if update.effective_user else None,
        "phase": "awaiting_media",
        "expires_at": time.time() + 3600,
        "file_id": None,
        "file_name": None,
        "file_unique_id": None,
        "source_kind": None,
        "source_format": None,
        "target_name": None,
        "cover_file_id": None,
        "cover_file_unique_id": None,
    }
    _audio_conv_save_session(context, session)

    t = _audio_conv_texts(lang)
    uid = update.effective_user.id if update.effective_user else None
    sent = await _send_with_retry(
        lambda: target_message.reply_text(
            t["start"],
            reply_markup=_main_menu_keyboard(lang, "other", uid),
        )
    )
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _audio_conv_save_session(context, session)


async def _audio_conv_handle_media_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    session = _audio_conv_get_session(context)
    if not session or not session.get("active"):
        return False
    if not update.message:
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    t = _audio_conv_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _audio_conv_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    phase = str(session.get("phase") or "")
    if phase == "awaiting_cover":
        cover = _audio_conv_extract_cover_info(update.message)
        if not cover:
            await update.message.reply_text(t["cover_invalid"])
            return True

        size_bytes = int(cover.get("file_size", 0) or 0)
        if size_bytes > _audio_conv_cover_max_bytes():
            await update.message.reply_text(
                t["cover_too_large"].format(size_mb=max(1, round(size_bytes / (1024 * 1024))))
            )
            return True

        session.update(
            {
                "cover_file_id": cover.get("file_id"),
                "cover_file_unique_id": cover.get("file_unique_id"),
                "phase": "ready",
                "expires_at": time.time() + 3600,
            }
        )
        _audio_conv_save_session(context, session)

        if str(session.get("source_format") or "") != "mp3":
            await update.message.reply_text(
                t["cover_only_mp3"],
                reply_markup=_audio_conv_action_keyboard(
                    lang,
                    has_source=bool(session.get("file_id")),
                    current_name=str(session.get("target_name") or ""),
                    source_format=str(session.get("source_format") or ""),
                ),
            )
            return True

        await update.message.reply_text(t["cover_set"])
        await _audio_conv_process_and_send(update, context, lang=lang, output_mode="mp3")
        return True

    media = _audio_conv_extract_media_info(update.message)
    if not media:
        return False

    size_bytes = int(media.get("file_size", 0) or 0)
    if size_bytes > _audio_conv_max_bytes():
        await update.message.reply_text(t["too_large"].format(size_mb=max(1, round(size_bytes / (1024 * 1024)))))
        return True

    source_name = str(media.get("file_name") or "")
    source_kind = str(media.get("source_kind") or "audio")
    source_format = str(media.get("source_format") or _audio_conv_detect_source_format(source_kind, source_name, media.get("mime_type")))
    current_name = _audio_conv_sanitize_name(str(session.get("target_name") or ""))

    session.update(
        {
            "phase": "ready",
            "expires_at": time.time() + 3600,
            "file_id": media.get("file_id"),
            "file_unique_id": media.get("file_unique_id"),
            "file_size": size_bytes,
            "file_name": source_name,
            "source_kind": source_kind,
            "source_format": source_format,
            "target_name": current_name or _audio_conv_default_name(source_name, source_kind),
        }
    )
    _audio_conv_save_session(context, session)

    await update.message.reply_text(
        f"{t['source_saved'].format(name=session['target_name'])}\n{t['choose_action']}",
        reply_markup=_audio_conv_action_keyboard(
            lang,
            has_source=True,
            current_name=str(session.get("target_name") or ""),
            source_format=str(session.get("source_format") or ""),
        ),
    )
    return True


async def _audio_conv_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message or not update.message.text:
        return False

    session = _audio_conv_get_session(context)
    if not session or not session.get("active"):
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    t = _audio_conv_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _audio_conv_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    raw = (update.message.text or "").strip()
    if raw.lower() in {"cancel", "stop"}:
        _audio_conv_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        return True

    phase = str(session.get("phase") or "")
    if phase == "awaiting_rename":
        clean_name = _audio_conv_sanitize_name(raw)
        if not clean_name:
            await update.message.reply_text(t["name_invalid"])
            return True

        session["target_name"] = clean_name
        session["phase"] = "ready" if session.get("file_id") else "awaiting_media"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)

        await update.message.reply_text(
            t["name_set"].format(name=clean_name),
            reply_markup=_audio_conv_action_keyboard(
                lang,
                has_source=bool(session.get("file_id")),
                current_name=clean_name,
                source_format=str(session.get("source_format") or ""),
            ),
        )
        return True

    if phase in {"awaiting_cut_mp3", "awaiting_cut_voice"}:
        parsed = _audio_conv_parse_range(raw)
        if not parsed:
            await update.message.reply_text(t["invalid_range"])
            return True
        start_s, end_s = parsed
        session["phase"] = "ready"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)

        out_mode = "mp3" if phase == "awaiting_cut_mp3" else "voice"
        # Enqueue background job
        user_id = update.effective_user.id if update.effective_user else 0
        job_data = {
            "session": session,
            "output_mode": out_mode,
            "start_s": start_s,
            "end_s": end_s,
            "lang": lang,
        }
        job_id = db_enqueue_background_job("audio_convert", user_id, job_data)
        if not job_id:
            await update.message.reply_text(t["failed"])
            return True
        await update.message.reply_text("✅ Audio cutting queued! You'll receive the file soon.")
        return True

    if phase == "awaiting_cover":
        await update.message.reply_text(t["ask_cover"])
        return True

    if not session.get("file_id"):
        await update.message.reply_text(t["prompt_send_audio"])
    else:
        await update.message.reply_text(
            t["choose_action"],
            reply_markup=_audio_conv_action_keyboard(
                lang,
                has_source=True,
                current_name=str(session.get("target_name") or ""),
                source_format=str(session.get("source_format") or ""),
            ),
        )
    return True


async def handle_audio_converter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    lang = ensure_user_language(update, context)
    t = _audio_conv_texts(lang)

    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return

    session = _audio_conv_get_session(context)
    if not session or not session.get("active"):
        await safe_answer(query, t["expired"], show_alert=True)
        return
    if time.time() > float(session.get("expires_at", 0) or 0):
        _audio_conv_clear_session(context)
        await safe_answer(query, t["expired"], show_alert=True)
        return
    if query.from_user and session.get("user_id") and int(session.get("user_id")) != int(query.from_user.id):
        await safe_answer(query, t["session_other"], show_alert=True)
        return

    action = str(query.data or "")

    if action == "atool:cancel":
        _audio_conv_clear_session(context)
        await safe_answer(query, t["cancelled"])
        uid = query.from_user.id if query.from_user else None
        try:
            await query.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        except Exception:
            pass
        return

    if action == "atool:rename":
        session["phase"] = "awaiting_rename"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.reply_text(t["ask_name"])
        except Exception:
            pass
        return

    if not session.get("file_id"):
        await safe_answer(query, t["no_source"], show_alert=True)
        return

    if action == "atool:complete":
        await safe_answer(query)
        await _audio_conv_complete_and_send(update, context, lang=lang)
        return

    source_format = str(session.get("source_format") or "")

    if action == "atool:set_cover":
        if source_format != "mp3":
            await safe_answer(query, t["cover_only_mp3"], show_alert=True)
            return
        session["phase"] = "awaiting_cover"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.reply_text(t["ask_cover"])
        except Exception:
            pass
        return

    if action == "atool:to_mp3":
        if source_format == "mp3":
            await safe_answer(query, t["already_mp3"], show_alert=True)
            return
        await safe_answer(query)
        # Enqueue background job
        user_id = query.from_user.id if query.from_user else 0
        job_data = {
            "session": session,
            "output_mode": "mp3",
            "lang": lang,
        }
        job_id = db_enqueue_background_job("audio_convert", user_id, job_data)
        if not job_id:
            await _send_with_retry(lambda: query.message.reply_text(t["failed"]))
            return
        await _send_with_retry(lambda: query.message.reply_text("✅ Audio conversion to MP3 queued! You'll receive the file soon."))
        return

    if action == "atool:to_voice":
        if source_format == "voice":
            await safe_answer(query, t["already_voice"], show_alert=True)
            return
        await safe_answer(query)
        # Enqueue background job
        user_id = query.from_user.id if query.from_user else 0
        job_data = {
            "session": session,
            "output_mode": "voice",
            "lang": lang,
        }
        job_id = db_enqueue_background_job("audio_convert", user_id, job_data)
        if not job_id:
            await _send_with_retry(lambda: query.message.reply_text(t["failed"]))
            return
        await _send_with_retry(lambda: query.message.reply_text("✅ Audio conversion to Voice queued! You'll receive the file soon."))
        return

    if action == "atool:cut_mp3":
        session["phase"] = "awaiting_cut_mp3"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.reply_text(t["ask_range_mp3"])
        except Exception:
            pass
        return

    if action == "atool:cut_voice":
        session["phase"] = "awaiting_cut_voice"
        session["expires_at"] = time.time() + 3600
        _audio_conv_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.reply_text(t["ask_range_voice"])
        except Exception:
            pass
        return

    await safe_answer(query)
