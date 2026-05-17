from __future__ import annotations

import argparse
import asyncio
import fcntl
import logging
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config import (
    CONNECTED_BOT_TOKEN_ENCRYPTION_KEY,
    ENABLE_WHITE_LABEL,
    WHITE_LABEL_CACHE_WAIT_SECONDS,
    WHITE_LABEL_SEARCH_RESULTS_LIMIT,
)
from db import init_db, increment_book_searches as db_increment_book_searches
from language import MESSAGES

from . import WL_STATUS_ACTIVE
from .cache_seeding import parse_cache_seed_caption
from .commands import format_connected_bot_reference
from .connected_bot_delivery import send_book_via_connected_bot
from .connected_bot_search import build_results_message, search_connected_books
from .crypto import decrypt_bot_token, redact_token_like_strings
from .db_helpers import (
    get_connected_bot_by_cache_channel_id,
    get_connected_bot_by_id,
    get_connected_bot_cache_seed_job_by_token,
    get_connected_bot_file_cache,
    get_connected_bot_usage,
    increment_connected_bot_usage,
    record_connected_bot_verification,
    update_connected_bot_cache_seed_job,
    update_connected_bot_status,
    upsert_connected_bot_file_cache,
)
from .runtime_utils import configure_application_builder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("white_label.runtime")
_LOCK_FH = None
_SUPPORTED_LANGS = {"uz", "en", "ru"}


def _default_lang_from_update(update: Update) -> str:
    code = str(getattr(getattr(update, "effective_user", None), "language_code", "") or "").strip().lower()
    if code.startswith("uz"):
        return "uz"
    if code.startswith("ru"):
        return "ru"
    return "en"


def _lang_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    user_id = int(getattr(getattr(update, "effective_user", None), "id", 0) or 0)
    user_languages = context.application.bot_data.setdefault("_wl_user_languages", {})
    lang = str(user_languages.get(user_id) or "").strip().lower()
    if lang in _SUPPORTED_LANGS:
        return lang
    return _default_lang_from_update(update)


def _set_user_lang(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> None:
    user_id = int(getattr(getattr(update, "effective_user", None), "id", 0) or 0)
    if not user_id:
        return
    lang = str(lang or "").strip().lower()
    if lang not in _SUPPORTED_LANGS:
        return
    context.application.bot_data.setdefault("_wl_user_languages", {})[user_id] = lang


def _messages(lang: str) -> dict[str, str]:
    localized_defaults = {
        "uz": {
            "start": "👋 Xush kelibsiz.\nTilni tanlang yoki kitob nomini yuboring.",
            "help": "📚 Kitob qidirish uchun shaxsiy chatda kitob nomini yuboring.\nPDF olish uchun raqamli tugmalardan birini bosing.",
            "choose_language": "🌐 Tilni tanlang:",
            "language_saved": "✅ Til tanlandi.",
            "private_only": "💬 Bu bot hozircha faqat shaxsiy chatda ishlaydi.",
            "results_empty": "❌ Mos PDF kitob topilmadi.",
            "limit_reached": "Bugungi limit tugadi. Bot egasi bilan bog‘laning.",
            "book_unavailable": "⚠️ Bu kitob hozircha ushbu botda mavjud emas.",
            "preparing": "Kitob tayyorlanmoqda, bir necha soniya kuting...",
            "retry_later": "Kitob tayyorlanmoqda. Iltimos, birozdan keyin qayta urinib ko‘ring.",
            "send_failed": "⚠️ Kitobni yuborib bo‘lmadi. Keyinroq qayta urinib ko‘ring.",
            "suspended": "⚠️ Bu ulangan bot hozir faol emas.",
            "trial_expired": "Trial muddati tugadi. Davom ettirish uchun bot egasiga murojaat qiling.",
        },
        "en": {
            "start": "👋 Welcome.\nChoose a language or send a book name.",
            "help": "📚 Send a book name in private chat to search.\nThen tap one of the numbered buttons to receive the PDF.",
            "choose_language": "🌐 Choose language:",
            "language_saved": "✅ Language saved.",
            "private_only": "💬 This bot works in private chat only for now.",
            "results_empty": "❌ No matching PDF books were found.",
            "limit_reached": "Today's limit is finished. Please contact the bot owner.",
            "book_unavailable": "⚠️ This book is not available for this bot right now.",
            "preparing": "The book is being prepared. Please wait a few seconds...",
            "retry_later": "The book is still being prepared. Please try again later.",
            "send_failed": "⚠️ Could not send the book. Please try again later.",
            "suspended": "⚠️ This connected bot is not active right now.",
            "trial_expired": "The trial period has ended. Please contact the bot owner to continue.",
        },
        "ru": {
            "start": "👋 Добро пожаловать.\nВыберите язык или отправьте название книги.",
            "help": "📚 Отправьте название книги в личном чате.\nЗатем нажмите одну из цифровых кнопок, чтобы получить PDF.",
            "choose_language": "🌐 Выберите язык:",
            "language_saved": "✅ Язык сохранен.",
            "private_only": "💬 Сейчас бот работает только в личном чате.",
            "results_empty": "❌ Подходящие PDF-книги не найдены.",
            "limit_reached": "Дневной лимит закончился. Свяжитесь с владельцем бота.",
            "book_unavailable": "⚠️ Эта книга сейчас недоступна в этом боте.",
            "preparing": "Книга готовится. Подождите несколько секунд...",
            "retry_later": "Книга все еще готовится. Пожалуйста, попробуйте позже.",
            "send_failed": "⚠️ Не удалось отправить книгу. Попробуйте позже.",
            "suspended": "⚠️ Этот подключенный бот сейчас не активен.",
            "trial_expired": "Trial закончился. Для продолжения свяжитесь с владельцем бота.",
        },
    }
    defaults = localized_defaults.get(lang, localized_defaults["en"])
    language_messages = MESSAGES.get(lang, MESSAGES.get("en", {}))
    return {key: language_messages.get(f"white_label_{key}", value) for key, value in defaults.items()}


def _language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇺🇿 Uzbek", callback_data="wllang:uz"),
                InlineKeyboardButton("🇬🇧 English", callback_data="wllang:en"),
                InlineKeyboardButton("🇷🇺 Русский", callback_data="wllang:ru"),
            ]
        ]
    )


def _numbered_keyboard(books: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, book in enumerate(books, start=1):
        book_id = str(book.get("id") or "").strip()
        if not book_id:
            continue
        row.append(InlineKeyboardButton(str(idx), callback_data=f"wlbook:{book_id}"))
        if len(row) >= 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def _acquire_instance_lock(connected_bot_id: str) -> bool:
    global _LOCK_FH
    lock_path = os.getenv("WHITE_LABEL_LOCK_FILE_TEMPLATE", "/tmp/pdf_audio_kitoblar_connected_bot.{id}.lock").format(id=connected_bot_id)
    try:
        fh = open(lock_path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()))
        fh.flush()
        _LOCK_FH = fh
        return True
    except BlockingIOError:
        logger.error("Connected bot %s is already running (lock %s)", connected_bot_id, lock_path)
        return False


def _release_instance_lock() -> None:
    global _LOCK_FH
    try:
        if _LOCK_FH:
            fcntl.flock(_LOCK_FH.fileno(), fcntl.LOCK_UN)
            _LOCK_FH.close()
    except Exception:
        pass
    _LOCK_FH = None


async def _load_connected_bot_config(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    connected_bot_id = str(context.application.bot_data.get("connected_bot_id") or "").strip()
    if not connected_bot_id:
        return None
    bot_row = await asyncio.to_thread(get_connected_bot_by_id, connected_bot_id)
    if bot_row:
        context.application.bot_data["connected_bot"] = bot_row
    return bot_row


def _send_rate_bucket(application, connected_bot_id: str):
    buckets = application.bot_data.setdefault("_wl_send_rate_buckets", {})
    bucket = buckets.get(connected_bot_id)
    if bucket is None:
        bucket = deque()
        buckets[connected_bot_id] = bucket
    return bucket


def _can_send_now(application, connected_bot: dict) -> bool:
    connected_bot_id = str(connected_bot.get("id") or "").strip()
    per_minute_limit = max(1, int(connected_bot.get("per_minute_send_limit") or 1))
    bucket = _send_rate_bucket(application, connected_bot_id)
    now = time.monotonic()
    while bucket and (now - bucket[0]) > 60.0:
        bucket.popleft()
    return len(bucket) < per_minute_limit


def _record_send(application, connected_bot: dict) -> None:
    bucket = _send_rate_bucket(application, str(connected_bot.get("id") or "").strip())
    bucket.append(time.monotonic())


def _daily_limit_reached(connected_bot: dict, usage: dict | None, *, for_send: bool) -> bool:
    usage = usage or {}
    if for_send:
        return int(usage.get("sends") or 0) >= int(connected_bot.get("daily_send_limit") or 0)
    return int(usage.get("searches") or 0) >= int(connected_bot.get("daily_search_limit") or 0)


def _connected_bot_trial_expired(connected_bot: dict) -> bool:
    plan = str((connected_bot or {}).get("plan") or "").strip().upper()
    subscription_status = str((connected_bot or {}).get("subscription_status") or "").strip().upper()
    if subscription_status == "EXPIRED":
        return True
    if plan != "TRIAL":
        return False
    trial_ends_at = (connected_bot or {}).get("trial_ends_at")
    if not trial_ends_at:
        return False
    try:
        if isinstance(trial_ends_at, datetime):
            return datetime.utcnow() > trial_ends_at.replace(tzinfo=None)
        text = str(trial_ends_at).replace("Z", "").split("+", 1)[0].strip()
        return datetime.utcnow() > datetime.fromisoformat(text).replace(tzinfo=None)
    except Exception:
        return False


async def _handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if getattr(update.effective_chat, "type", "") != "private":
        return
    lang = _lang_from_context(update, context)
    _set_user_lang(update, context, lang)
    text_bundle = _messages(lang)
    await update.message.reply_text(
        f"{text_bundle['choose_language']}\n\n{text_bundle['start']}",
        reply_markup=_language_keyboard(),
    )


async def _handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if getattr(update.effective_chat, "type", "") != "private":
        return
    lang = _lang_from_context(update, context)
    await update.message.reply_text(_messages(lang)["help"], reply_markup=_language_keyboard())


async def _handle_language_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    data = str(query.data or "")
    if not data.startswith("wllang:"):
        await query.answer()
        return
    lang = data.split(":", 1)[1].strip().lower()
    if lang not in _SUPPORTED_LANGS:
        await query.answer()
        return
    _set_user_lang(update, context, lang)
    text_bundle = _messages(lang)
    await query.answer(text_bundle["language_saved"])
    if query.message:
        try:
            await query.message.edit_text(
                f"{text_bundle['language_saved']}\n\n{text_bundle['start']}",
                reply_markup=_language_keyboard(),
            )
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                raise


async def _handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    lang = _lang_from_context(update, context)
    text_bundle = _messages(lang)
    if getattr(update.effective_chat, "type", "") != "private":
        await update.message.reply_text(text_bundle["private_only"])
        return
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot or str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
        await update.message.reply_text(text_bundle["suspended"])
        return
    if _connected_bot_trial_expired(connected_bot):
        await update.message.reply_text(text_bundle["trial_expired"])
        return
    usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
    if _daily_limit_reached(connected_bot, usage, for_send=False):
        await update.message.reply_text(text_bundle["limit_reached"])
        return

    query = str(update.message.text or "").strip()
    if not query:
        return
    books = await asyncio.to_thread(search_connected_books, query, WHITE_LABEL_SEARCH_RESULTS_LIMIT)
    await asyncio.to_thread(increment_connected_bot_usage, str(connected_bot.get("id") or ""), searches=1)
    if books:
        await asyncio.to_thread(db_increment_book_searches, [str(book.get("id") or "") for book in books if str(book.get("id") or "").strip()])
    if not books:
        await update.message.reply_text(text_bundle["results_empty"])
        return

    result_text = build_results_message(query, books, page=1, pages=1, lang=lang)
    await update.message.reply_text(result_text, reply_markup=_numbered_keyboard(books))


async def _handle_book_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    lang = _lang_from_context(update, context)
    text_bundle = _messages(lang)
    try:
        data = str(query.data or "")
        if not data.startswith("wlbook:"):
            await query.answer()
            return
        _, book_id = data.split(":", 1)
        connected_bot = await _load_connected_bot_config(context)
        if not connected_bot or str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
            await query.answer(text_bundle["suspended"], show_alert=True)
            return
        if _connected_bot_trial_expired(connected_bot):
            await query.answer(text_bundle["trial_expired"], show_alert=True)
            return
        usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
        if _daily_limit_reached(connected_bot, usage, for_send=True) or not _can_send_now(context.application, connected_bot):
            await query.answer(text_bundle["limit_reached"], show_alert=True)
            return
        await query.answer()
        result = await send_book_via_connected_bot(
            connected_bot=connected_bot,
            chat_id=int(query.message.chat_id if query.message else query.from_user.id),
            user_id=int(query.from_user.id or 0),
            book_id=str(book_id or "").strip(),
            requesting_message_id=int(query.message.message_id) if query.message else None,
            bot=context.bot,
            wait_seconds=WHITE_LABEL_CACHE_WAIT_SECONDS,
            preparing_text=text_bundle["preparing"],
        )
        if result.get("ok"):
            _record_send(context.application, connected_bot)
            return
        error_code = str(result.get("error_code") or "")
        if error_code == "cache_wait_timeout":
            await context.bot.send_message(chat_id=query.from_user.id, text=text_bundle["retry_later"])
        elif error_code == "book_unavailable":
            await context.bot.send_message(chat_id=query.from_user.id, text=text_bundle["book_unavailable"])
        else:
            await context.bot.send_message(chat_id=query.from_user.id, text=text_bundle["send_failed"])
    except Exception as exc:
        logger.error("connected bot delivery failed: %s", redact_token_like_strings(str(exc)), exc_info=True)
        await query.answer(text_bundle["send_failed"], show_alert=True)


async def _handle_cache_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    post = getattr(update, "channel_post", None)
    if not post:
        return
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot:
        return
    configured_channel_id = int(connected_bot.get("cache_channel_id") or 0)
    if not configured_channel_id or int(getattr(post.chat, "id", 0) or 0) != configured_channel_id:
        return
    marker = parse_cache_seed_caption(getattr(post, "caption", None))
    if not marker:
        return
    if str(marker.get("connected_bot_id") or "").strip() != str(connected_bot.get("id") or "").strip():
        return
    document = getattr(post, "document", None)
    if not document or not getattr(document, "file_id", None):
        return
    await asyncio.to_thread(
        upsert_connected_bot_file_cache,
        connected_bot_id=str(connected_bot.get("id") or ""),
        book_id=str(marker.get("book_id") or ""),
        telegram_file_id=str(document.file_id),
        telegram_file_unique_id=str(getattr(document, "file_unique_id", "") or "").strip() or None,
        cache_channel_id=configured_channel_id,
        cache_message_id=int(getattr(post, "message_id", 0) or 0) or None,
    )
    seed_job = await asyncio.to_thread(get_connected_bot_cache_seed_job_by_token, str(marker.get("seed_token") or ""))
    if not seed_job:
        return
    await asyncio.to_thread(
        update_connected_bot_cache_seed_job,
        str(seed_job.get("id") or ""),
        status="CACHED",
        connected_bot_cache_message_id=int(getattr(post, "message_id", 0) or 0) or None,
        error_message=None,
    )


async def _post_init(application) -> None:
    connected_bot_id = str(application.bot_data.get("connected_bot_id") or "").strip()
    connected_bot = await asyncio.to_thread(get_connected_bot_by_id, connected_bot_id)
    if not connected_bot:
        raise RuntimeError(f"Connected bot {connected_bot_id} was not found")
    me = await application.bot.get_me()
    if int(me.id or 0) != int(connected_bot.get("bot_telegram_id") or 0):
        await asyncio.to_thread(update_connected_bot_status, connected_bot_id, "ERROR", last_error="getMe bot id does not match the stored connected bot record")
        raise RuntimeError("Connected bot token does not match the stored bot record")
    await asyncio.to_thread(record_connected_bot_verification, connected_bot_id, last_error=None)
    logger.info("Connected bot runtime ready: id=%s ref=%s", connected_bot_id, format_connected_bot_reference(connected_bot))


def build_application(token: str, connected_bot_id: str):
    builder = ApplicationBuilder().token(token).post_init(_post_init)
    builder = configure_application_builder(builder)
    application = builder.build()
    application.bot_data["connected_bot_id"] = connected_bot_id
    application.add_handler(CommandHandler("start", _handle_start))
    application.add_handler(CommandHandler("help", _handle_help))
    application.add_handler(CommandHandler("language", _handle_start))
    application.add_handler(CallbackQueryHandler(_handle_language_pick, pattern=r"^wllang:"))
    application.add_handler(CallbackQueryHandler(_handle_book_pick, pattern=r"^wlbook:"))
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, _handle_cache_channel_post))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, _handle_search))
    return application


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one white-label connected bot by connected_bot_id")
    parser.add_argument("--connected-bot-id", required=True, help="Connected bot UUID")
    args = parser.parse_args(argv)

    if not ENABLE_WHITE_LABEL:
        logger.error("White-label feature is disabled. Set ENABLE_WHITE_LABEL=true to start connected bot runtimes.")
        return 1
    if not CONNECTED_BOT_TOKEN_ENCRYPTION_KEY:
        logger.error("CONNECTED_BOT_TOKEN_ENCRYPTION_KEY is required for white-label connected bot runtimes.")
        return 1

    init_db()
    connected_bot = get_connected_bot_by_id(str(args.connected_bot_id or "").strip())
    if not connected_bot:
        logger.error("Connected bot %s was not found", args.connected_bot_id)
        return 1
    if str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
        logger.error("Connected bot %s is not ACTIVE", args.connected_bot_id)
        return 1
    if not _acquire_instance_lock(str(args.connected_bot_id or "").strip()):
        return 1
    try:
        token = decrypt_bot_token(str(connected_bot.get("bot_token_encrypted") or ""), CONNECTED_BOT_TOKEN_ENCRYPTION_KEY)
        application = build_application(token, str(args.connected_bot_id or "").strip())
        application.run_polling(allowed_updates=["message", "callback_query", "channel_post"], drop_pending_updates=False)
        return 0
    except Exception as exc:
        error_text = redact_token_like_strings(str(exc))
        logger.error("Connected bot runtime failed for %s: %s", args.connected_bot_id, error_text, exc_info=True)
        update_connected_bot_status(str(args.connected_bot_id or "").strip(), "ERROR", last_error=error_text)
        return 1
    finally:
        _release_instance_lock()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
