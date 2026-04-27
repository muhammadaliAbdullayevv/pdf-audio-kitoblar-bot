from __future__ import annotations

import io
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from typing import Any

import safe_subprocess

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)
MESSAGES: dict[str, dict[str, str]] = {}


def _sticker_missing_dep(*args, **kwargs):
    raise RuntimeError("sticker_tools module is not configured")


async def _sticker_missing_dep_async(*args, **kwargs):
    raise RuntimeError("sticker_tools module is not configured")


_send_with_retry = _sticker_missing_dep_async
run_blocking = _sticker_missing_dep_async
run_blocking_heavy = None
safe_answer = _sticker_missing_dep_async
ensure_user_language = _sticker_missing_dep
spam_check_callback = _sticker_missing_dep
spam_check_message = _sticker_missing_dep
_main_menu_keyboard = _sticker_missing_dep
is_blocked = _sticker_missing_dep
is_stopped_user = _sticker_missing_dep_async
update_user_info = _sticker_missing_dep_async
db_enqueue_background_job = _sticker_missing_dep


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith("__") and k.endswith("__"):
            continue
        globals()[k] = v


_STICKER_SESSION_KEY = "sticker_tools_session"


def _sticker_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "start": "🧩 Sticker Tools yoqildi.\n📥 Rasm, video, GIF yoki sticker yuboring.",
            "prompt_send_media": "📥 Rasm/video/GIF/sticker yuboring.",
            "source_saved": "✅ Qabul qilindi: {name}",
            "choose_action": "👇 Amalni tanlang.",
            "working": "⏳ Sticker tayyorlanmoqda...",
            "queued": "✅ Sticker jarayoni fon rejimida boshlandi. Botdan foydalanishda davom etishingiz mumkin.",
            "working_background": "⏳ Sticker fon rejimida tayyorlanmoqda. Botdan foydalanishda davom etishingiz mumkin.",
            "done_static": "✅ Sticker tayyor (WEBP).",
            "done_video": "✅ Video sticker tayyor (WEBM).",
            "done_remove_bg": "✅ Fon olib tashlandi va sticker tayyorlandi.",
            "send_next": "📥 Yana media yuborish uchun Sticker Tools bo‘limini qayta oching yoki boshqaruv xabariga javob bering.",
            "failed": "⚠️ Amal bajarilmadi. Qayta urinib ko‘ring.",
            "tools_missing": "⚠️ `ffmpeg` topilmadi.",
            "remove_bg_unavailable": "⚠️ Background remove hozircha mavjud emas (`rembg` o'rnatilmagan).",
            "session_other": "Bu sessiya boshqa foydalanuvchiga tegishli.",
            "expired": "⌛ Sessiya tugadi. Menyudan qayta oching.",
            "cancelled": "❌ Bekor qilindi.",
            "completed": "✅ Sticker Tools yakunlandi.",
            "too_large": "⚠️ Fayl katta ({size_mb} MB). Kichikroq yuboring.",
            "unsupported": "⚠️ Faqat rasm/video/GIF/sticker qabul qilinadi.",
            "no_source": "⚠️ Avval media yuboring.",
            "already_static": "ℹ️ Bu allaqachon static stickerga mos.",
            "already_video": "ℹ️ Bu allaqachon video stickerga mos.",
            "btn_make_static": "🖼️ Static sticker",
            "btn_make_video": "🎬 Video sticker",
            "btn_remove_bg": "🪄 Fonni olib tashlash",
            "btn_complete": "✅ Yakunlash",
            "btn_cancel": "❌ Bekor",
            "hint_buttons": "💡 Yangi media yubormoqchi bo‘lsangiz shu xabarga javob bering yoki pastdagi tugmalarni bosing.",
        }
    if lang == "ru":
        return {
            "start": "🧩 Sticker Tools запущен.\n📥 Отправьте фото, видео, GIF или стикер.",
            "prompt_send_media": "📥 Отправьте фото/видео/GIF/стикер.",
            "source_saved": "✅ Получено: {name}",
            "choose_action": "👇 Выберите действие.",
            "working": "⏳ Готовлю стикер...",
            "queued": "✅ Задача стикера запущена в фоне. Можете продолжать пользоваться ботом.",
            "working_background": "⏳ Стикер готовится в фоне. Можете продолжать пользоваться ботом.",
            "done_static": "✅ Стикер готов (WEBP).",
            "done_video": "✅ Видео-стикер готов (WEBM).",
            "done_remove_bg": "✅ Фон удалён, стикер готов.",
            "send_next": "📥 Чтобы отправить ещё медиа, снова откройте Sticker Tools или ответьте на служебное сообщение.",
            "failed": "⚠️ Операция не выполнена. Попробуйте снова.",
            "tools_missing": "⚠️ `ffmpeg` не найден.",
            "remove_bg_unavailable": "⚠️ Удаление фона сейчас недоступно (`rembg` не установлен).",
            "session_other": "Эта сессия принадлежит другому пользователю.",
            "expired": "⌛ Сессия истекла. Откройте раздел снова.",
            "cancelled": "❌ Отменено.",
            "completed": "✅ Sticker Tools завершен.",
            "too_large": "⚠️ Файл слишком большой ({size_mb} MB).",
            "unsupported": "⚠️ Поддерживаются только фото/видео/GIF/стикеры.",
            "no_source": "⚠️ Сначала отправьте медиа.",
            "already_static": "ℹ️ Уже подходит как static sticker.",
            "already_video": "ℹ️ Уже подходит как video sticker.",
            "btn_make_static": "🖼️ Static sticker",
            "btn_make_video": "🎬 Video sticker",
            "btn_remove_bg": "🪄 Удалить фон",
            "btn_complete": "✅ Готово",
            "btn_cancel": "❌ Отмена",
            "hint_buttons": "💡 Если хотите отправить новый файл, ответьте на это сообщение или используйте кнопки ниже.",
        }
    return {
        "start": "🧩 Sticker Tools is on.\n📥 Send photo, video, GIF, or sticker.",
        "prompt_send_media": "📥 Send photo/video/GIF/sticker.",
        "source_saved": "✅ Received: {name}",
        "choose_action": "👇 Choose an action.",
        "working": "⏳ Building sticker...",
        "queued": "✅ Sticker task started in background. You can keep using the bot.",
        "working_background": "⏳ Sticker is being prepared in background. You can keep using the bot.",
        "done_static": "✅ Sticker is ready (WEBP).",
        "done_video": "✅ Video sticker is ready (WEBM).",
        "done_remove_bg": "✅ Background removed and sticker is ready.",
        "send_next": "📥 To send another media, open Sticker Tools again or reply to the control message.",
        "failed": "⚠️ Operation failed. Please try again.",
        "tools_missing": "⚠️ `ffmpeg` is missing.",
        "remove_bg_unavailable": "⚠️ Background removal is unavailable right now (`rembg` is not installed).",
        "session_other": "This session belongs to another user.",
        "expired": "⌛ Session expired. Open the section again.",
        "cancelled": "❌ Cancelled.",
        "completed": "✅ Sticker Tools completed.",
        "too_large": "⚠️ File is too large ({size_mb} MB).",
        "unsupported": "⚠️ Only photo/video/GIF/sticker is supported.",
        "no_source": "⚠️ Send media first.",
        "already_static": "ℹ️ Already suitable as a static sticker.",
        "already_video": "ℹ️ Already suitable as a video sticker.",
        "btn_make_static": "🖼️ Static sticker",
        "btn_make_video": "🎬 Video sticker",
        "btn_remove_bg": "🪄 Remove background",
        "btn_complete": "✅ Complete",
        "btn_cancel": "❌ Cancel",
        "hint_buttons": "💡 If you want to send new media, reply to this message or use the buttons below.",
    }


def _sticker_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_STICKER_SESSION_KEY, None)


def _sticker_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_STICKER_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _sticker_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_STICKER_SESSION_KEY] = dict(session)


def _sticker_set_control_message(session: dict, message) -> None:
    if not isinstance(session, dict) or not message:
        return
    try:
        session["control_chat_id"] = int(getattr(message, "chat_id", 0) or 0)
    except Exception:
        session["control_chat_id"] = 0
    try:
        session["control_message_id"] = int(getattr(message, "message_id", 0) or 0)
    except Exception:
        session["control_message_id"] = 0


def _sticker_is_reply_to_control(message, session: dict) -> bool:
    if not message or not isinstance(session, dict):
        return False
    reply = getattr(message, "reply_to_message", None)
    if not reply:
        return False
    try:
        reply_message_id = int(getattr(reply, "message_id", 0) or 0)
    except Exception:
        reply_message_id = 0
    try:
        control_message_id = int(session.get("control_message_id") or 0)
    except Exception:
        control_message_id = 0
    if not reply_message_id or not control_message_id or reply_message_id != control_message_id:
        return False
    try:
        reply_chat_id = int(getattr(reply, "chat_id", 0) or 0)
    except Exception:
        reply_chat_id = 0
    try:
        control_chat_id = int(session.get("control_chat_id") or 0)
    except Exception:
        control_chat_id = 0
    return not control_chat_id or reply_chat_id == control_chat_id


def _sticker_max_bytes() -> int:
    try:
        max_mb = max(1, int(os.getenv("STICKER_TOOLS_MAX_MB", "64") or "64"))
    except Exception:
        max_mb = 64
    return max_mb * 1024 * 1024


def _sticker_video_max_seconds() -> float:
    try:
        secs = float(os.getenv("STICKER_TOOLS_VIDEO_MAX_SECONDS", "3") or "3")
    except Exception:
        secs = 3.0
    return max(1.0, min(10.0, secs))


def _sticker_sanitize_name(name: str) -> str:
    cleaned = re.sub(r"[^\w\-. ]+", "", str(name or "").strip())
    cleaned = re.sub(r"\s+", "_", cleaned).strip("._ ")
    if not cleaned:
        return "media"
    if "." in cleaned:
        cleaned = cleaned.rsplit(".", 1)[0]
    return cleaned[:64] or "media"


def _sticker_doc_kind(name: str, mime: str) -> str | None:
    n = str(name or "").lower()
    m = str(mime or "").lower()
    if m.startswith("image/") or n.endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff", ".heic")):
        return "image"
    if m.startswith("video/") or n.endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".gif", ".m4v")):
        return "video"
    return None


def _sticker_guess_ext(file_name: str | None, mime_type: str | None, kind: str) -> str:
    name = str(file_name or "").lower()
    _, ext = os.path.splitext(name)
    if ext:
        return ext[:10]
    mime = str(mime_type or "").lower()
    if mime.startswith("image/"):
        sub = mime.split("/", 1)[1]
        return ".jpg" if sub in {"jpeg", "pjpeg"} else f".{sub[:8]}"
    if mime.startswith("video/"):
        sub = mime.split("/", 1)[1]
        return ".mp4" if sub in {"quicktime", "x-matroska"} else f".{sub[:8]}"
    return ".jpg" if kind == "image" else ".mp4"


def _sticker_action_keyboard(lang: str, *, kind: str | None, has_source: bool) -> InlineKeyboardMarkup:
    t = _sticker_texts(lang)
    rows: list[list[InlineKeyboardButton]] = []
    if has_source:
        if kind == "image":
            rows.append([
                InlineKeyboardButton(t["btn_make_static"], callback_data="stkr:make_static"),
                InlineKeyboardButton(t["btn_remove_bg"], callback_data="stkr:remove_bg"),
            ])
        elif kind == "video":
            rows.append([InlineKeyboardButton(t["btn_make_video"], callback_data="stkr:make_video")])
        else:
            rows.append(
                [
                    InlineKeyboardButton(t["btn_make_static"], callback_data="stkr:make_static"),
                    InlineKeyboardButton(t["btn_make_video"], callback_data="stkr:make_video"),
                ]
            )
    rows.append(
        [
            InlineKeyboardButton(t["btn_complete"], callback_data="stkr:complete"),
            InlineKeyboardButton(t["btn_cancel"], callback_data="stkr:cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def _sticker_extract_media_info(message) -> dict | None:
    if not message:
        return None

    if getattr(message, "photo", None):
        p = message.photo[-1]
        return {
            "kind": "image",
            "file_id": p.file_id,
            "file_unique_id": getattr(p, "file_unique_id", None),
            "file_size": int(getattr(p, "file_size", 0) or 0),
            "file_name": "photo.jpg",
            "mime_type": "image/jpeg",
        }

    if getattr(message, "sticker", None):
        s = message.sticker
        kind = "video" if bool(getattr(s, "is_video", False)) else "image"
        name = "sticker.webm" if kind == "video" else "sticker.webp"
        mime = "video/webm" if kind == "video" else "image/webp"
        return {
            "kind": kind,
            "file_id": s.file_id,
            "file_unique_id": getattr(s, "file_unique_id", None),
            "file_size": int(getattr(s, "file_size", 0) or 0),
            "file_name": name,
            "mime_type": mime,
        }

    if getattr(message, "animation", None):
        a = message.animation
        file_name = getattr(a, "file_name", None) or "animation.mp4"
        return {
            "kind": "video",
            "file_id": a.file_id,
            "file_unique_id": getattr(a, "file_unique_id", None),
            "file_size": int(getattr(a, "file_size", 0) or 0),
            "file_name": file_name,
            "mime_type": str(getattr(a, "mime_type", "") or "video/mp4"),
        }

    if getattr(message, "video", None):
        v = message.video
        file_name = getattr(v, "file_name", None) or "video.mp4"
        return {
            "kind": "video",
            "file_id": v.file_id,
            "file_unique_id": getattr(v, "file_unique_id", None),
            "file_size": int(getattr(v, "file_size", 0) or 0),
            "file_name": file_name,
            "mime_type": str(getattr(v, "mime_type", "") or "video/mp4"),
        }

    doc = getattr(message, "document", None)
    if doc:
        file_name = getattr(doc, "file_name", None) or "document"
        mime = str(getattr(doc, "mime_type", "") or "")
        kind = _sticker_doc_kind(file_name, mime)
        if kind:
            return {
                "kind": kind,
                "file_id": doc.file_id,
                "file_unique_id": getattr(doc, "file_unique_id", None),
                "file_size": int(getattr(doc, "file_size", 0) or 0),
                "file_name": file_name,
                "mime_type": mime,
            }

    return None


async def _sticker_download_source_bytes(context: ContextTypes.DEFAULT_TYPE, file_id: str) -> bytes:
    f = await context.bot.get_file(file_id)
    content = await f.download_as_bytearray()
    return bytes(content or b"")


def _sticker_make_static_blocking(input_bytes: bytes, *, input_ext: str) -> bytes:
    if not input_bytes:
        raise RuntimeError("empty input")

    with tempfile.TemporaryDirectory(prefix="stkr_static_") as td:
        in_path = os.path.join(td, f"in{input_ext or '.bin'}")
        out_path = os.path.join(td, "out.webp")
        with open(in_path, "wb") as fp:
            fp.write(input_bytes)

        vf = (
            "scale=512:512:force_original_aspect_ratio=decrease:flags=lanczos,"
            "pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black"
        )
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            in_path,
            "-frames:v",
            "1",
            "-vf",
            vf,
            "-vcodec",
            "libwebp",
            "-lossless",
            "1",
            "-q:v",
            "75",
            "-an",
            "-sn",
            "-threads",
            "0",
            out_path,
        ]
        timeout_s = float(os.getenv("STICKER_TOOLS_FFMPEG_TIMEOUT_S", "120") or "120")
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


def _sticker_make_video_blocking(input_bytes: bytes, *, input_ext: str, max_seconds: float) -> bytes:
    if not input_bytes:
        raise RuntimeError("empty input")

    with tempfile.TemporaryDirectory(prefix="stkr_video_") as td:
        in_path = os.path.join(td, f"in{input_ext or '.bin'}")
        out_path = os.path.join(td, "out.webm")
        with open(in_path, "wb") as fp:
            fp.write(input_bytes)

        vf = (
            "fps=25,"
            "scale=512:512:force_original_aspect_ratio=decrease:flags=lanczos,"
            "pad=512:512:(ow-iw)/2:(oh-ih)/2:color=black"
        )
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            in_path,
            "-t",
            f"{float(max_seconds):.2f}",
            "-an",
            "-vf",
            vf,
            "-c:v",
            "libvpx-vp9",
            "-b:v",
            "0",
            "-crf",
            "38",
            "-deadline",
            "good",
            "-cpu-used",
            "4",
            "-row-mt",
            "1",
            "-pix_fmt",
            "yuv420p",
            out_path,
        ]
        timeout_s = float(os.getenv("STICKER_TOOLS_FFMPEG_TIMEOUT_S", "120") or "120")
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


def _sticker_prepare_rembg_runtime() -> None:
    # Avoid numba cache locator errors in some service/venv setups.
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")

    # Keep model cache in a writable project-local folder.
    model_dir = os.path.join(tempfile.gettempdir(), "smartaitoolsbot_u2net")
    os.environ.setdefault("U2NET_HOME", model_dir)
    try:
        os.makedirs(os.environ["U2NET_HOME"], exist_ok=True)
    except Exception:
        pass


def _sticker_rembg_available() -> bool:
    _sticker_prepare_rembg_runtime()
    try:
        import rembg  # type: ignore  # noqa: F401
        return True
    except Exception:
        return False


def _sticker_remove_bg_blocking(input_bytes: bytes, *, input_ext: str) -> bytes:
    if not input_bytes:
        raise RuntimeError("empty input")

    _sticker_prepare_rembg_runtime()
    try:
        from rembg import remove  # type: ignore
        out = remove(input_bytes)
        if not out:
            raise RuntimeError("rembg-empty-output")
        return bytes(out)
    except Exception as e:
        raise RuntimeError(str(e))


async def _sticker_send_result(update: Update, sticker_bytes: bytes, *, ext: str):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    def _build_sticker():
        bio = io.BytesIO(sticker_bytes)
        bio.name = f"sticker{ext}"
        return bio
    return await _send_with_retry(lambda: target_message.reply_sticker(sticker=_build_sticker()))


async def _sticker_send_result_to_chat(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    sticker_bytes: bytes,
    *,
    ext: str,
    reply_to_message_id: int | None = None,
):
    def _build_sticker():
        bio = io.BytesIO(sticker_bytes)
        bio.name = f"sticker{ext}"
        return bio

    return await _send_with_retry(
        lambda: context.bot.send_sticker(
            chat_id=chat_id,
            sticker=_build_sticker(),
            reply_to_message_id=reply_to_message_id,
        )
    )


def _sticker_source_snapshot(session: dict) -> dict[str, Any]:
    return {
        "kind": str(session.get("kind") or ""),
        "file_id": str(session.get("file_id") or ""),
        "file_unique_id": str(session.get("file_unique_id") or ""),
        "file_size": int(session.get("file_size") or 0),
        "file_name": str(session.get("file_name") or ""),
        "mime_type": str(session.get("mime_type") or ""),
    }


async def _sticker_process_source_background(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    lang: str,
    output_kind: str,
    source: dict[str, Any],
    chat_id: int,
    reply_to_message_id: int | None = None,
) -> bool:
    t = _sticker_texts(lang)
    file_id = str(source.get("file_id") or "").strip()
    if not file_id:
        if chat_id:
            await _send_with_retry(lambda: context.bot.send_message(chat_id=chat_id, text=t["no_source"], reply_to_message_id=reply_to_message_id))
        return False

    if not shutil.which("ffmpeg"):
        if chat_id:
            await _send_with_retry(lambda: context.bot.send_message(chat_id=chat_id, text=t["tools_missing"], reply_to_message_id=reply_to_message_id))
        return False

    status = None
    if chat_id:
        status = await _send_with_retry(
            lambda: context.bot.send_message(
                chat_id=chat_id,
                text=t["working_background"],
                reply_to_message_id=reply_to_message_id,
            )
        )

    blocking_runner = run_blocking_heavy if callable(globals().get("run_blocking_heavy")) else run_blocking

    try:
        source_bytes = await _sticker_download_source_bytes(context, file_id)
        input_ext = _sticker_guess_ext(
            str(source.get("file_name") or ""),
            str(source.get("mime_type") or ""),
            str(source.get("kind") or "image"),
        )

        if output_kind == "static":
            out_bytes = await blocking_runner(_sticker_make_static_blocking, source_bytes, input_ext=input_ext)
            sent = await _sticker_send_result_to_chat(context, chat_id, out_bytes, ext=".webp", reply_to_message_id=reply_to_message_id)
            done_msg = t["done_static"]
        elif output_kind == "remove_bg":
            removed_bg_png = await blocking_runner(_sticker_remove_bg_blocking, source_bytes, input_ext=input_ext)
            out_bytes = await blocking_runner(_sticker_make_static_blocking, removed_bg_png, input_ext=".png")
            sent = await _sticker_send_result_to_chat(context, chat_id, out_bytes, ext=".webp", reply_to_message_id=reply_to_message_id)
            done_msg = t["done_remove_bg"]
        else:
            out_bytes = await blocking_runner(
                _sticker_make_video_blocking,
                source_bytes,
                input_ext=input_ext,
                max_seconds=_sticker_video_max_seconds(),
            )
            sent = await _sticker_send_result_to_chat(context, chat_id, out_bytes, ext=".webm", reply_to_message_id=reply_to_message_id)
            done_msg = t["done_video"]

        if status:
            try:
                await status.edit_text(done_msg if sent else t["failed"])
            except Exception:
                pass
        elif chat_id:
            await _send_with_retry(lambda: context.bot.send_message(chat_id=chat_id, text=done_msg if sent else t["failed"], reply_to_message_id=reply_to_message_id))
        return bool(sent)
    except Exception as e:
        logger.warning("sticker tools background processing failed: %s", e, exc_info=True)
        if status:
            try:
                await status.edit_text(t["failed"])
            except Exception:
                pass
        elif chat_id:
            await _send_with_retry(lambda: context.bot.send_message(chat_id=chat_id, text=t["failed"], reply_to_message_id=reply_to_message_id))
        return False


async def _sticker_process_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, *, lang: str, output_kind: str) -> bool:
    t = _sticker_texts(lang)
    session = _sticker_get_session(context)
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
        source_bytes = await _sticker_download_source_bytes(context, str(session.get("file_id")))
        input_ext = _sticker_guess_ext(
            str(session.get("file_name") or ""),
            str(session.get("mime_type") or ""),
            str(session.get("kind") or "image"),
        )

        blocking_runner = run_blocking_heavy if callable(globals().get("run_blocking_heavy")) else run_blocking

        if output_kind == "static":
            out_bytes = await blocking_runner(_sticker_make_static_blocking, source_bytes, input_ext=input_ext)
            sent = await _sticker_send_result(update, out_bytes, ext=".webp")
            done_msg = t["done_static"]
        elif output_kind == "remove_bg":
            removed_bg_png = await blocking_runner(_sticker_remove_bg_blocking, source_bytes, input_ext=input_ext)
            out_bytes = await blocking_runner(_sticker_make_static_blocking, removed_bg_png, input_ext=".png")
            sent = await _sticker_send_result(update, out_bytes, ext=".webp")
            done_msg = t["done_remove_bg"]
        else:
            out_bytes = await blocking_runner(
                _sticker_make_video_blocking,
                source_bytes,
                input_ext=input_ext,
                max_seconds=_sticker_video_max_seconds(),
            )
            sent = await _sticker_send_result(update, out_bytes, ext=".webm")
            done_msg = t["done_video"]

        if sent and getattr(sent, "sticker", None):
            s = sent.sticker
            next_kind = "video" if bool(getattr(s, "is_video", False)) else "image"
            session.update(
                {
                    "phase": "ready",
                    "expires_at": time.time() + 3600,
                    "kind": next_kind,
                    "file_id": getattr(s, "file_id", None),
                    "file_unique_id": getattr(s, "file_unique_id", None),
                    "file_size": int(getattr(s, "file_size", 0) or 0),
                    "file_name": "sticker.webm" if next_kind == "video" else "sticker.webp",
                    "mime_type": "video/webm" if next_kind == "video" else "image/webp",
                }
            )
            _sticker_save_session(context, session)

        if status:
            try:
                await status.edit_text(done_msg if sent else t["failed"])
            except Exception:
                pass

        if sent:
            await target_message.reply_text(
                t["send_next"],
                reply_markup=_sticker_action_keyboard(
                    lang,
                    kind=str(session.get("kind") or ""),
                    has_source=True,
                ),
            )
        return bool(sent)
    except Exception as e:
        logger.warning("sticker tools processing failed: %s", e, exc_info=True)
        if status:
            try:
                await status.edit_text(t["failed"])
            except Exception:
                pass
        else:
            await target_message.reply_text(t["failed"])
        return False


async def _sticker_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    _sticker_clear_session(context)
    session = {
        "active": True,
        "user_id": update.effective_user.id if update.effective_user else None,
        "phase": "awaiting_media",
        "expires_at": time.time() + 3600,
        "kind": None,
        "file_id": None,
        "file_unique_id": None,
        "file_size": 0,
        "file_name": None,
        "mime_type": None,
    }
    _sticker_save_session(context, session)

    t = _sticker_texts(lang)
    uid = update.effective_user.id if update.effective_user else None
    sent = await _send_with_retry(
        lambda: target_message.reply_text(
            t["start"],
            reply_markup=_main_menu_keyboard(lang, "other", uid),
        )
    )
    if sent:
        _sticker_set_control_message(session, sent)
        _sticker_save_session(context, session)


async def _sticker_handle_media_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    session = _sticker_get_session(context)
    if not session or not session.get("active"):
        return False
    if not update.message:
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    t = _sticker_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _sticker_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    replied_to_control = _sticker_is_reply_to_control(update.message, session)
    if session.get("processing"):
        if not replied_to_control:
            return False
        await update.message.reply_text(t["working_background"])
        return True

    if session.get("file_id") and not replied_to_control:
        return False

    media = _sticker_extract_media_info(update.message)
    if not media:
        return False

    size_bytes = int(media.get("file_size", 0) or 0)
    if size_bytes > _sticker_max_bytes():
        await update.message.reply_text(t["too_large"].format(size_mb=max(1, round(size_bytes / (1024 * 1024)))))
        return True

    file_name = _sticker_sanitize_name(str(media.get("file_name") or "media"))
    session.update(
        {
            "phase": "ready",
            "expires_at": time.time() + 3600,
            "kind": media.get("kind"),
            "file_id": media.get("file_id"),
            "file_unique_id": media.get("file_unique_id"),
            "file_size": size_bytes,
            "file_name": str(media.get("file_name") or "media"),
            "mime_type": str(media.get("mime_type") or ""),
        }
    )
    _sticker_save_session(context, session)

    sent = await update.message.reply_text(
        f"{t['source_saved'].format(name=file_name)}\n{t['choose_action']}",
        reply_markup=_sticker_action_keyboard(
            lang,
            kind=str(session.get("kind") or ""),
            has_source=True,
        ),
    )
    _sticker_set_control_message(session, sent)
    _sticker_save_session(context, session)
    return True


async def _sticker_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message or not update.message.text:
        return False
    session = _sticker_get_session(context)
    if not session or not session.get("active"):
        return False

    t = _sticker_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _sticker_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    txt = (update.message.text or "").strip()
    replied_to_control = _sticker_is_reply_to_control(update.message, session)

    if not replied_to_control:
        return False

    if session.get("processing"):
        await update.message.reply_text(t["working_background"])
        return True

    if txt.lower() in {"cancel", "stop"}:
        _sticker_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        return True

    if txt.startswith("/"):
        return False

    if not session.get("file_id"):
        sent = await update.message.reply_text(t["prompt_send_media"])
    else:
        sent = await update.message.reply_text(
            t["hint_buttons"],
            reply_markup=_sticker_action_keyboard(
                lang,
                kind=str(session.get("kind") or ""),
                has_source=True,
            ),
        )
    _sticker_set_control_message(session, sent)
    _sticker_save_session(context, session)
    return True


async def handle_sticker_tools_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    lang = ensure_user_language(update, context)
    t = _sticker_texts(lang)

    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return

    session = _sticker_get_session(context)
    if not session or not session.get("active"):
        await safe_answer(query, t["expired"], show_alert=True)
        return

    if time.time() > float(session.get("expires_at", 0) or 0):
        _sticker_clear_session(context)
        await safe_answer(query, t["expired"], show_alert=True)
        return

    if query.from_user and session.get("user_id") and int(session.get("user_id")) != int(query.from_user.id):
        await safe_answer(query, t["session_other"], show_alert=True)
        return

    action = str(query.data or "")
    if not action.startswith("stkr:"):
        await safe_answer(query)
        return

    if action == "stkr:cancel":
        _sticker_clear_session(context)
        await safe_answer(query, t["cancelled"])
        uid = query.from_user.id if query.from_user else None
        try:
            await query.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        except Exception:
            pass
        return

    if action == "stkr:complete":
        _sticker_clear_session(context)
        await safe_answer(query)
        uid = query.from_user.id if query.from_user else None
        try:
            await query.message.reply_text(t["completed"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        except Exception:
            pass
        return

    if not session.get("file_id"):
        await safe_answer(query, t["no_source"], show_alert=True)
        return

    media_kind = str(session.get("kind") or "")
    chat_id = query.message.chat_id if query.message else (query.from_user.id if query.from_user else None)
    reply_to_message_id = query.message.message_id if query.message else None
    if not chat_id:
        await safe_answer(query, t["failed"], show_alert=True)
        return

    async def _spawn_background(output_kind: str) -> None:
        source = _sticker_source_snapshot(session)
        user_id = query.from_user.id if query.from_user else 0
        job_data = {
            "lang": lang,
            "output_kind": output_kind,
            "source": source,
            "chat_id": int(chat_id),
            "reply_to_message_id": int(reply_to_message_id or 0),
        }
        job_id = db_enqueue_background_job("sticker_convert", user_id, job_data)
        if not job_id:
            await safe_answer(query, t["failed"], show_alert=True)
            if query.message:
                try:
                    await query.message.reply_text(t["failed"])
                except Exception:
                    pass
            return
        _sticker_clear_session(context)
        await safe_answer(query, t["queued"])
        if query.message:
            try:
                await query.message.reply_text(t["queued"])
            except Exception:
                pass

    if action == "stkr:make_static":
        if media_kind == "image" and str(session.get("mime_type") or "").startswith("image/webp"):
            await safe_answer(query, t["already_static"], show_alert=False)
            return
        if session.get("processing"):
            await safe_answer(query, t["working_background"], show_alert=True)
            return
        await _spawn_background("static")
        return

    if action == "stkr:make_video":
        if media_kind != "video":
            await safe_answer(query, t["unsupported"], show_alert=True)
            return
        if session.get("processing"):
            await safe_answer(query, t["working_background"], show_alert=True)
            return
        await _spawn_background("video")
        return

    if action == "stkr:remove_bg":
        if media_kind != "image":
            await safe_answer(query, t["unsupported"], show_alert=True)
            return
        if not _sticker_rembg_available():
            await safe_answer(query, t["remove_bg_unavailable"], show_alert=True)
            return
        if session.get("processing"):
            await safe_answer(query, t["working_background"], show_alert=True)
            return
        await _spawn_background("remove_bg")
        return

    await safe_answer(query)


async def sticker_tools_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not update.message:
        return

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
    await _sticker_start_session_from_message(update.message, update, context, lang)
