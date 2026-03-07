from __future__ import annotations

import asyncio
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
            "start": (
                "🎛️ Audio Cutter & Converter yoqildi.\n\n"
                "📥 Endi voice yoki audio fayl yuboring.\n"
                "✅ Keyin: MP3/Voice formatga o‘tkazish, kesish va nomini o‘zgartirish mumkin."
            ),
            "prompt_send_audio": "📥 Voice yoki audio fayl yuboring.",
            "source_saved": "✅ Audio qabul qilindi: {name}",
            "choose_action": "👇 Kerakli amalni tanlang.",
            "ask_range_mp3": "✂️ MP3 kesish uchun oraliq yuboring: `12-48` yoki `00:12-00:48`",
            "ask_range_voice": "✂️ Voice kesish uchun oraliq yuboring: `12-48` yoki `00:12-00:48`",
            "invalid_range": "⚠️ Oraliq noto‘g‘ri. Masalan: `10-35` yoki `00:10-00:35`",
            "ask_name": "🏷️ Yangi nom yuboring (kengaytmasiz). Masalan: `lesson_1`",
            "name_set": "✅ Yangi nom saqlandi: {name}",
            "name_invalid": "⚠️ Nomi noto‘g‘ri. Harf/raqam bilan yuboring.",
            "working": "⏳ Audio qayta ishlanmoqda...",
            "done_mp3": "✅ Tayyor. MP3 yuborildi.",
            "done_voice": "✅ Tayyor. Voice yuborildi.",
            "failed": "⚠️ Audio qayta ishlanmadi. Qayta urinib ko‘ring.",
            "tools_missing": "⚠️ `ffmpeg` topilmadi. Serverda o‘rnatish kerak.",
            "session_other": "Bu audio konvertor sessiyasi boshqa foydalanuvchiga tegishli.",
            "expired": "⌛ Sessiya tugagan. Menyudan Audio Cutter & Converter ni qayta oching.",
            "cancelled": "❌ Audio Cutter & Converter bekor qilindi.",
            "too_large": "⚠️ Fayl juda katta ({size_mb} MB). Kichikroq audio yuboring.",
            "unsupported": "⚠️ Faqat voice/audio fayllar qo‘llab-quvvatlanadi.",
            "no_source": "⚠️ Avval voice yoki audio fayl yuboring.",
            "btn_to_mp3": "🎵 MP3 ga o‘tkazish",
            "btn_to_voice": "🎙️ Voice ga o‘tkazish",
            "btn_cut_mp3": "✂️ MP3 kesish",
            "btn_cut_voice": "✂️ Voice kesish",
            "btn_rename": "🏷️ Nomini o‘zgartirish",
            "btn_cancel": "❌ Bekor qilish",
            "caption_mp3": "🎵 Konvert qilingan MP3: {name}",
            "caption_voice": "🎙️ Konvert qilingan Voice: {name}",
        }
    if lang == "ru":
        return {
            "start": (
                "🎛️ Audio Cutter & Converter запущен.\n\n"
                "📥 Отправьте voice или аудио файл.\n"
                "✅ Дальше можно: конвертировать в MP3/Voice, обрезать и переименовать."
            ),
            "prompt_send_audio": "📥 Отправьте voice или аудио файл.",
            "source_saved": "✅ Аудио получено: {name}",
            "choose_action": "👇 Выберите действие.",
            "ask_range_mp3": "✂️ Для обрезки MP3 отправьте диапазон: `12-48` или `00:12-00:48`",
            "ask_range_voice": "✂️ Для обрезки Voice отправьте диапазон: `12-48` или `00:12-00:48`",
            "invalid_range": "⚠️ Неверный диапазон. Пример: `10-35` или `00:10-00:35`",
            "ask_name": "🏷️ Отправьте новое имя (без расширения). Например: `lesson_1`",
            "name_set": "✅ Новое имя сохранено: {name}",
            "name_invalid": "⚠️ Некорректное имя. Используйте буквы/цифры.",
            "working": "⏳ Обрабатываю аудио...",
            "done_mp3": "✅ Готово. MP3 отправлен.",
            "done_voice": "✅ Готово. Voice отправлен.",
            "failed": "⚠️ Не удалось обработать аудио. Попробуйте снова.",
            "tools_missing": "⚠️ Не найден `ffmpeg`. Установите его на сервер.",
            "session_other": "Эта сессия audio converter принадлежит другому пользователю.",
            "expired": "⌛ Сессия истекла. Откройте Audio Cutter & Converter снова через меню.",
            "cancelled": "❌ Audio Cutter & Converter отменен.",
            "too_large": "⚠️ Файл слишком большой ({size_mb} MB). Отправьте файл поменьше.",
            "unsupported": "⚠️ Поддерживаются только voice/audio файлы.",
            "no_source": "⚠️ Сначала отправьте voice или аудио файл.",
            "btn_to_mp3": "🎵 В MP3",
            "btn_to_voice": "🎙️ В Voice",
            "btn_cut_mp3": "✂️ Обрезать MP3",
            "btn_cut_voice": "✂️ Обрезать Voice",
            "btn_rename": "🏷️ Переименовать",
            "btn_cancel": "❌ Отмена",
            "caption_mp3": "🎵 Конвертированный MP3: {name}",
            "caption_voice": "🎙️ Конвертированный Voice: {name}",
        }
    return {
        "start": (
            "🎛️ Audio Cutter & Converter is active.\n\n"
            "📥 Send a voice or audio file.\n"
            "✅ Then you can convert MP3/Voice, cut, and rename."
        ),
        "prompt_send_audio": "📥 Send a voice or audio file.",
        "source_saved": "✅ Audio received: {name}",
        "choose_action": "👇 Choose an action.",
        "ask_range_mp3": "✂️ Send cut range for MP3: `12-48` or `00:12-00:48`",
        "ask_range_voice": "✂️ Send cut range for Voice: `12-48` or `00:12-00:48`",
        "invalid_range": "⚠️ Invalid range. Example: `10-35` or `00:10-00:35`",
        "ask_name": "🏷️ Send new name (without extension). Example: `lesson_1`",
        "name_set": "✅ New name saved: {name}",
        "name_invalid": "⚠️ Invalid name. Use letters/numbers.",
        "working": "⏳ Processing audio...",
        "done_mp3": "✅ Done. MP3 sent.",
        "done_voice": "✅ Done. Voice sent.",
        "failed": "⚠️ Failed to process audio. Please try again.",
        "tools_missing": "⚠️ `ffmpeg` is missing on the server.",
        "session_other": "This audio converter session belongs to another user.",
        "expired": "⌛ Session expired. Open Audio Cutter & Converter again from the menu.",
        "cancelled": "❌ Audio converter cancelled.",
        "too_large": "⚠️ File is too large ({size_mb} MB). Send a smaller audio file.",
        "unsupported": "⚠️ Only voice/audio files are supported.",
        "no_source": "⚠️ Send a voice or audio file first.",
        "btn_to_mp3": "🎵 Convert to MP3",
        "btn_to_voice": "🎙️ Convert to Voice",
        "btn_cut_mp3": "✂️ Cut as MP3",
        "btn_cut_voice": "✂️ Cut as Voice",
        "btn_rename": "🏷️ Rename",
        "btn_cancel": "❌ Cancel",
        "caption_mp3": "🎵 Converted MP3: {name}",
        "caption_voice": "🎙️ Converted Voice: {name}",
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
        # Hard cap at 4h to avoid accidental huge operations.
        return None
    return start_s, end_s


def _audio_conv_action_keyboard(lang: str, *, has_source: bool, current_name: str | None = None) -> InlineKeyboardMarkup:
    t = _audio_conv_texts(lang)
    rename_label = t["btn_rename"]
    if current_name:
        rename_label = f"{rename_label} ({str(current_name)[:16]})"
    rows: list[list[InlineKeyboardButton]] = []
    if has_source:
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
    rows.append([InlineKeyboardButton(t["btn_cancel"], callback_data="atool:cancel")])
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
            "source_kind": "voice",
        }
    if getattr(message, "audio", None):
        a = message.audio
        return {
            "file_id": a.file_id,
            "file_unique_id": getattr(a, "file_unique_id", None),
            "file_size": int(getattr(a, "file_size", 0) or 0),
            "file_name": getattr(a, "file_name", None) or getattr(a, "title", None) or "audio",
            "source_kind": "audio",
        }
    doc = getattr(message, "document", None)
    mime = str(getattr(doc, "mime_type", "") or "").lower() if doc else ""
    if doc and mime.startswith("audio/"):
        return {
            "file_id": doc.file_id,
            "file_unique_id": getattr(doc, "file_unique_id", None),
            "file_size": int(getattr(doc, "file_size", 0) or 0),
            "file_name": getattr(doc, "file_name", None) or "audio_file",
            "source_kind": "audio",
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
        if status:
            try:
                await status.edit_text(t["done_mp3"] if output_mode == "mp3" else t["done_voice"])
            except Exception:
                pass
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
        "target_name": None,
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

    media = _audio_conv_extract_media_info(update.message)
    if not media:
        return False
    size_bytes = int(media.get("file_size", 0) or 0)
    max_bytes = _audio_conv_max_bytes()
    if size_bytes > max_bytes:
        await update.message.reply_text(
            t["too_large"].format(size_mb=max(1, round(size_bytes / (1024 * 1024))))
        )
        return True

    source_name = str(media.get("file_name") or "")
    source_kind = str(media.get("source_kind") or "audio")
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
        await _audio_conv_process_and_send(
            update,
            context,
            lang=lang,
            output_mode=out_mode,
            start_s=start_s,
            end_s=end_s,
        )
        return True

    # Session is active, but user sent plain text in another phase.
    if not session.get("file_id"):
        await update.message.reply_text(t["prompt_send_audio"])
    else:
        await update.message.reply_text(
            t["choose_action"],
            reply_markup=_audio_conv_action_keyboard(
                lang,
                has_source=True,
                current_name=str(session.get("target_name") or ""),
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

    if action == "atool:to_mp3":
        await safe_answer(query)
        await _audio_conv_process_and_send(update, context, lang=lang, output_mode="mp3")
        return
    if action == "atool:to_voice":
        await safe_answer(query)
        await _audio_conv_process_and_send(update, context, lang=lang, output_mode="voice")
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
