from __future__ import annotations

import argparse
import asyncio
import fcntl
import html
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

from telegram import BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, InlineQueryResultCachedDocument, InputTextMessageContent, Message, Update
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes, InlineQueryHandler, MessageHandler, TypeHandler, filters

from config import (
    CONNECTED_BOT_TOKEN_ENCRYPTION_KEY,
    ENABLE_WHITE_LABEL,
    TOKEN as MAIN_BOT_TOKEN,
    WHITE_LABEL_CACHE_WAIT_SECONDS,
    WHITE_LABEL_SEARCH_RESULTS_LIMIT,
)
from db import init_db, increment_book_searches as db_increment_book_searches
from language import MESSAGES

from . import WL_STATUS_ACTIVE
from .cache_seeding import parse_cache_seed_caption
from .commands import format_connected_bot_reference
from .connected_bot_delivery import send_book_via_connected_bot
from .connected_bot_search import build_results_message, search_connected_books_page
from .crypto import decrypt_bot_token, redact_token_like_strings
from .db_helpers import (
    create_white_label_audit_log,
    get_connected_bot_by_cache_channel_id,
    get_connected_bot_by_id,
    get_connected_bot_cache_seed_job_by_token,
    get_connected_bot_file_cache,
    get_connected_bot_user,
    get_connected_bot_usage,
    increment_connected_bot_usage,
    mark_connected_bot_trial_expired,
    record_connected_bot_verification,
    touch_connected_bot_runtime_heartbeat,
    update_connected_bot_cache_seed_job,
    update_connected_bot_public_settings,
    update_connected_bot_status,
    upsert_connected_bot_user,
    upsert_connected_bot_file_cache,
)
from .plans import (
    PLAN_FEATURE_GUEST_MODE,
    PLAN_FEATURE_INLINE_SEARCH,
    PLAN_FEATURE_PDF_DELIVERY,
    PLAN_FEATURE_PRIVATE_SEARCH,
    normalize_plan,
    plan_allows,
    plan_feature_summary,
)
from .runtime_utils import build_bot_client, configure_application_builder

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


def _connected_bot_id(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(context.application.bot_data.get("connected_bot_id") or "").strip()


def _user_lang_cache_key(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str:
    return f"{_connected_bot_id(context)}:{int(user_id or 0)}"


async def _lang_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    user_id = int(getattr(getattr(update, "effective_user", None), "id", 0) or 0)
    cache_key = _user_lang_cache_key(context, user_id)
    user_languages = context.application.bot_data.setdefault("_wl_user_languages", {})
    lang = str(user_languages.get(cache_key) or "").strip().lower()
    if lang in _SUPPORTED_LANGS:
        return lang
    connected_bot_id = _connected_bot_id(context)
    if connected_bot_id and user_id:
        try:
            row = await asyncio.to_thread(get_connected_bot_user, connected_bot_id, user_id)
            lang = str((row or {}).get("language_code") or "").strip().lower()
            if lang in _SUPPORTED_LANGS:
                user_languages[cache_key] = lang
                return lang
        except Exception:
            logger.debug("Failed to load connected bot user language", exc_info=True)
    lang = _default_lang_from_update(update)
    user_languages[cache_key] = lang
    return lang


async def _touch_connected_bot_user(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str | None = None) -> None:
    user = getattr(update, "effective_user", None)
    user_id = int(getattr(user, "id", 0) or 0)
    connected_bot_id = _connected_bot_id(context)
    if not user_id or not connected_bot_id:
        return
    try:
        await asyncio.to_thread(
            upsert_connected_bot_user,
            connected_bot_id=connected_bot_id,
            telegram_user_id=user_id,
            username=str(getattr(user, "username", "") or "").strip() or None,
            first_name=str(getattr(user, "first_name", "") or "").strip() or None,
            language_code=lang if lang in _SUPPORTED_LANGS else None,
        )
    except Exception:
        logger.debug("Failed to touch connected bot user", exc_info=True)


def _messages(lang: str) -> dict[str, str]:
    localized_defaults = {
        "uz": {
            "start": "👋 Xush kelibsiz.\nKitob nomini yuboring.",
            "greeting": "👋 Xush kelibsiz!\n\n📚 Kitob nomini yuboring, men katalogdan PDF kitoblarni topib beraman.",
            "help": "📚 Kitob qidirish uchun shaxsiy chatda kitob nomini yuboring.\nPDF olish uchun raqamli tugmalardan birini bosing.",
            "search_prompt": "🔎 Kitob nomini yuboring.",
            "private_only": "💬 Bu bot hozircha faqat shaxsiy chatda ishlaydi.",
            "results_empty": "❌ Mos PDF kitob topilmadi.",
            "limit_reached": "Bugungi limit tugadi. Bot egasi bilan bog‘laning.",
            "book_unavailable": "⚠️ Bu kitob hozircha ushbu botda mavjud emas.",
            "preparing": "Kitob tayyorlanmoqda, bir necha soniya kuting...",
            "retry_later": "Kitob tayyorlanmoqda. Iltimos, birozdan keyin qayta urinib ko‘ring.",
            "send_failed": "⚠️ Kitobni yuborib bo‘lmadi. Keyinroq qayta urinib ko‘ring.",
            "suspended": "⚠️ Bu ulangan bot hozir faol emas.",
            "trial_expired": "Sizning 3 kunlik bepul trial muddatingiz tugadi. Botdan foydalanishni davom ettirish uchun Basic yoki Pro tarifiga obuna bo‘ling.",
            "trial_expired_owner": "Sizning 3 kunlik bepul trial muddatingiz tugadi.\n\nBot: {bot}\n\nBotdan foydalanishni davom ettirish uchun Basic yoki Pro tarifiga obuna bo‘ling.",
            "upgrade_plus": "⭐ Basic ga o‘tish",
            "upgrade_pro": "🚀 Pro ga o‘tish",
            "powered_by": "⚙️ Powered by @pdf_audio_kitoblar_bot",
            "inline_disabled": "Inline qidiruv bu tarifda yoqilmagan. Pro yoki Community tarifiga o‘ting.",
            "guest_disabled": "Guest qidiruv faqat Pro tarifida ishlaydi.",
            "open_bot": "📥 Botni ochish",
            "open_to_prepare": "Kitobni tayyorlash uchun botni oching.",
            "inline_cached": "⚡ Tayyor PDF",
            "owner_only": "⚠️ Bu buyruq faqat bot egasi uchun.",
            "owner_custom_disabled": "⚠️ Botni tahrirlash faqat Pro yoki Community obunalarida ishlaydi.",
            "owner_panel_title": "🛠 Bot egasi paneli",
            "owner_title_usage": "Bot nomini yuboring:\n/settitle Mening kitob botim",
            "owner_greeting_usage": "Greeting xabarini yuboring:\n/setgreeting uz Assalomu alaykum! Kitob nomini yuboring.\n/setgreeting en Welcome! Send a book name.\n/setgreeting ru Здравствуйте! Отправьте название книги.",
            "owner_reset_greeting_usage": "Greeting reset qilish:\n/resetgreeting uz\n/resetgreeting en\n/resetgreeting ru\n/resetgreeting all",
            "owner_about_usage": "Bot about qisqa matnini yuboring:\n/setabout uz PDF kitob qidiruvchi bot\n/setabout en PDF book search bot\n/setabout ru Бот для поиска PDF книг",
            "owner_description_usage": "Bot description matnini yuboring:\n/setdescription uz Bu bot orqali PDF kitoblarni qidirishingiz mumkin.",
            "owner_reset_about_usage": "About matnini reset qilish:\n/resetabout uz\n/resetabout en\n/resetabout ru\n/resetabout all",
            "owner_reset_description_usage": "Description matnini reset qilish:\n/resetdescription uz\n/resetdescription en\n/resetdescription ru\n/resetdescription all",
            "owner_results_usage": "Natija sonini yuboring: /setresults 5\nRuxsat: 1-10",
            "owner_settings_saved": "✅ Sozlama saqlandi.",
            "owner_settings_invalid": "⚠️ Noto‘g‘ri qiymat yuborildi.",
            "owner_commands_hint": "Buyruqlar: /settitle, /setgreeting, /resetgreeting, /setabout, /setdescription, /resetabout, /resetdescription, /setresults, /preview, /stats",
            "prev_page": "⬅️ Oldingi",
            "next_page": "Keyingi ➡️",
        },
        "en": {
            "start": "👋 Welcome.\nSend a book name.",
            "greeting": "👋 Welcome!\n\n📚 Send a book name and I will find matching PDF books in the catalog.",
            "help": "📚 Send a book name in private chat to search.\nThen tap one of the numbered buttons to receive the PDF.",
            "search_prompt": "🔎 Send a book name.",
            "private_only": "💬 This bot works in private chat only for now.",
            "results_empty": "❌ No matching PDF books were found.",
            "limit_reached": "Today's limit is finished. Please contact the bot owner.",
            "book_unavailable": "⚠️ This book is not available for this bot right now.",
            "preparing": "The book is being prepared. Please wait a few seconds...",
            "retry_later": "The book is still being prepared. Please try again later.",
            "send_failed": "⚠️ Could not send the book. Please try again later.",
            "suspended": "⚠️ This connected bot is not active right now.",
            "trial_expired": "Your 3-day free trial has ended. To keep using this bot, please subscribe to Basic or Pro.",
            "trial_expired_owner": "Your 3-day free trial has ended.\n\nBot: {bot}\n\nTo keep using this bot, please subscribe to Basic or Pro.",
            "upgrade_plus": "⭐ Upgrade to Basic",
            "upgrade_pro": "🚀 Upgrade to Pro",
            "powered_by": "⚙️ Powered by @pdf_audio_kitoblar_bot",
            "inline_disabled": "Inline search is not enabled on this plan. Upgrade to Pro or Community.",
            "guest_disabled": "Guest search is available on Pro only.",
            "open_bot": "📥 Open bot",
            "open_to_prepare": "Open the bot to prepare this book.",
            "inline_cached": "⚡ Ready PDF",
            "owner_only": "⚠️ This command is only for this bot owner.",
            "owner_custom_disabled": "⚠️ Bot editing is available only for Pro or Community subscribers.",
            "owner_panel_title": "🛠 Bot owner panel",
            "owner_title_usage": "Send the bot title:\n/settitle My book bot",
            "owner_greeting_usage": "Send a greeting message:\n/setgreeting uz Assalomu alaykum! Kitob nomini yuboring.\n/setgreeting en Welcome! Send a book name.\n/setgreeting ru Здравствуйте! Отправьте название книги.",
            "owner_reset_greeting_usage": "Reset greeting:\n/resetgreeting uz\n/resetgreeting en\n/resetgreeting ru\n/resetgreeting all",
            "owner_about_usage": "Send the short bot about text:\n/setabout uz PDF kitob qidiruvchi bot\n/setabout en PDF book search bot\n/setabout ru Бот для поиска PDF книг",
            "owner_description_usage": "Send the bot description:\n/setdescription en Search and receive PDF books from this bot.",
            "owner_reset_about_usage": "Reset about text:\n/resetabout uz\n/resetabout en\n/resetabout ru\n/resetabout all",
            "owner_reset_description_usage": "Reset description:\n/resetdescription uz\n/resetdescription en\n/resetdescription ru\n/resetdescription all",
            "owner_results_usage": "Send result count: /setresults 5\nAllowed: 1-10",
            "owner_settings_saved": "✅ Setting saved.",
            "owner_settings_invalid": "⚠️ Invalid value.",
            "owner_commands_hint": "Commands: /settitle, /setgreeting, /resetgreeting, /setabout, /setdescription, /resetabout, /resetdescription, /setresults, /preview, /stats",
            "prev_page": "⬅️ Prev",
            "next_page": "Next ➡️",
        },
        "ru": {
            "start": "👋 Добро пожаловать.\nОтправьте название книги.",
            "greeting": "👋 Добро пожаловать!\n\n📚 Отправьте название книги, и я найду подходящие PDF-книги в каталоге.",
            "help": "📚 Отправьте название книги в личном чате.\nЗатем нажмите одну из цифровых кнопок, чтобы получить PDF.",
            "search_prompt": "🔎 Отправьте название книги.",
            "private_only": "💬 Сейчас бот работает только в личном чате.",
            "results_empty": "❌ Подходящие PDF-книги не найдены.",
            "limit_reached": "Дневной лимит закончился. Свяжитесь с владельцем бота.",
            "book_unavailable": "⚠️ Эта книга сейчас недоступна в этом боте.",
            "preparing": "Книга готовится. Подождите несколько секунд...",
            "retry_later": "Книга все еще готовится. Пожалуйста, попробуйте позже.",
            "send_failed": "⚠️ Не удалось отправить книгу. Попробуйте позже.",
            "suspended": "⚠️ Этот подключенный бот сейчас не активен.",
            "trial_expired": "Ваш 3-дневный бесплатный trial закончился. Чтобы продолжить пользоваться ботом, подключите Basic или Pro.",
            "trial_expired_owner": "Ваш 3-дневный бесплатный trial закончился.\n\nBot: {bot}\n\nЧтобы продолжить пользоваться ботом, подключите Basic или Pro.",
            "upgrade_plus": "⭐ Перейти на Basic",
            "upgrade_pro": "🚀 Перейти на Pro",
            "powered_by": "⚙️ Powered by @pdf_audio_kitoblar_bot",
            "inline_disabled": "Inline-поиск недоступен на этом тарифе. Перейдите на Pro или Community.",
            "guest_disabled": "Guest-поиск доступен только на Pro.",
            "open_bot": "📥 Открыть бота",
            "open_to_prepare": "Откройте бота, чтобы подготовить эту книгу.",
            "inline_cached": "⚡ Готовый PDF",
            "owner_only": "⚠️ Эта команда только для владельца этого бота.",
            "owner_custom_disabled": "⚠️ Редактирование бота доступно только для подписчиков Pro или Community.",
            "owner_panel_title": "🛠 Панель владельца бота",
            "owner_title_usage": "Отправьте название бота:\n/settitle Мой книжный бот",
            "owner_greeting_usage": "Отправьте greeting сообщение:\n/setgreeting uz Assalomu alaykum! Kitob nomini yuboring.\n/setgreeting en Welcome! Send a book name.\n/setgreeting ru Здравствуйте! Отправьте название книги.",
            "owner_reset_greeting_usage": "Сброс greeting:\n/resetgreeting uz\n/resetgreeting en\n/resetgreeting ru\n/resetgreeting all",
            "owner_about_usage": "Отправьте короткий about текст:\n/setabout uz PDF kitob qidiruvchi bot\n/setabout en PDF book search bot\n/setabout ru Бот для поиска PDF книг",
            "owner_description_usage": "Отправьте описание бота:\n/setdescription ru Этот бот помогает искать PDF книги.",
            "owner_reset_about_usage": "Сброс about текста:\n/resetabout uz\n/resetabout en\n/resetabout ru\n/resetabout all",
            "owner_reset_description_usage": "Сброс description:\n/resetdescription uz\n/resetdescription en\n/resetdescription ru\n/resetdescription all",
            "owner_results_usage": "Отправьте число результатов: /setresults 5\nРазрешено: 1-10",
            "owner_settings_saved": "✅ Настройка сохранена.",
            "owner_settings_invalid": "⚠️ Неверное значение.",
            "owner_commands_hint": "Команды: /settitle, /setgreeting, /resetgreeting, /setabout, /setdescription, /resetabout, /resetdescription, /setresults, /preview, /stats",
            "prev_page": "⬅️ Назад",
            "next_page": "Далее ➡️",
        },
    }
    defaults = localized_defaults.get(lang, localized_defaults["en"])
    language_messages = MESSAGES.get(lang, MESSAGES.get("en", {}))
    return {key: language_messages.get(f"white_label_{key}", value) for key, value in defaults.items()}


def _branding_title(connected_bot: dict | None) -> str:
    title = str((connected_bot or {}).get("branding_title") or "").strip()
    if title:
        return title
    title = str((connected_bot or {}).get("bot_first_name") or "").strip()
    return title or "Book Search Bot"


def _welcome_text(connected_bot: dict | None, lang: str) -> str:
    custom_key = f"welcome_text_{lang}"
    custom = str((connected_bot or {}).get(custom_key) or "").strip()
    text_bundle = _messages(lang)
    body = custom or text_bundle["greeting"]
    powered_by = _powered_by_text(lang)
    plan = normalize_plan((connected_bot or {}).get("plan"))
    features = plan_feature_summary(plan)
    return f"{_branding_title(connected_bot)}\n\n{body}\n\n{powered_by}\nPlan: {plan} • {features}"


def _main_bot_username() -> str:
    raw = (
        os.getenv("BOT_PUBLIC_USERNAME")
        or os.getenv("MAIN_BOT_USERNAME")
        or os.getenv("TELEGRAM_BOT_USERNAME")
        or ""
    )
    return str(raw or "").strip().lstrip("@")


def _powered_by_text(lang: str) -> str:
    username = _main_bot_username() or "pdf_audio_kitoblar_bot"
    return _messages(lang)["powered_by"].replace("@pdf_audio_kitoblar_bot", f"@{username}")


def _connected_bot_username(context: ContextTypes.DEFAULT_TYPE, connected_bot: dict | None = None) -> str:
    bot_data_username = str(context.application.bot_data.get("connected_bot_username") or "").strip().lstrip("@")
    if bot_data_username:
        return bot_data_username
    return str((connected_bot or {}).get("bot_username") or "").strip().lstrip("@")


def _connected_bot_url(context: ContextTypes.DEFAULT_TYPE, connected_bot: dict | None = None, *, payload: str | None = None) -> str:
    username = _connected_bot_username(context, connected_bot)
    if not username:
        return ""
    base = f"https://t.me/{username}"
    clean_payload = str(payload or "").strip()
    return f"{base}?start={clean_payload}" if clean_payload else base


def _book_start_payload(book_id: str) -> str:
    return f"wlb_{str(book_id or '').strip()}"


def _parse_book_start_payload(payload: str | None) -> str:
    text = str(payload or "").strip()
    if not text.startswith("wlb_"):
        return ""
    return text[len("wlb_") :].strip()


def _upgrade_url_keyboard(lang: str) -> InlineKeyboardMarkup | None:
    username = _main_bot_username()
    if not username:
        return None
    text_bundle = _messages(lang)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(text_bundle["upgrade_plus"], url=f"https://t.me/{username}?start=wlupgrade_basic"),
                InlineKeyboardButton(text_bundle["upgrade_pro"], url=f"https://t.me/{username}?start=wlupgrade_pro"),
            ]
        ]
    )


async def _notify_trial_expired_owner(connected_bot: dict, lang: str = "uz") -> None:
    owner_id = int((connected_bot or {}).get("requested_by_user_id") or (connected_bot or {}).get("owner_telegram_id") or 0)
    if not owner_id or not MAIN_BOT_TOKEN:
        return
    text_bundle = _messages(lang)
    main_bot = build_bot_client(MAIN_BOT_TOKEN)
    try:
        await main_bot.send_message(
            chat_id=owner_id,
            text=text_bundle["trial_expired_owner"].format(bot=format_connected_bot_reference(connected_bot)),
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(text_bundle["upgrade_plus"], callback_data="wlupgpage:BASIC:0"),
                        InlineKeyboardButton(text_bundle["upgrade_pro"], callback_data="wlupgpage:PRO:0"),
                    ]
                ]
            ),
        )
    except Exception:
        logger.debug("Failed to notify connected bot owner about trial expiry", exc_info=True)
    finally:
        try:
            await main_bot.shutdown()
        except Exception:
            pass


def _search_results_limit(connected_bot: dict | None) -> int:
    try:
        return max(1, min(10, int((connected_bot or {}).get("search_results_limit") or 10)))
    except Exception:
        return 10


def _connected_bot_owner_id(connected_bot: dict | None) -> int:
    try:
        return int((connected_bot or {}).get("requested_by_user_id") or (connected_bot or {}).get("owner_telegram_id") or 0)
    except Exception:
        return 0


def _is_connected_bot_owner(update: Update, connected_bot: dict | None) -> bool:
    user_id = int(getattr(getattr(update, "effective_user", None), "id", 0) or 0)
    return bool(user_id and user_id == _connected_bot_owner_id(connected_bot))


def _search_cache(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.application.bot_data.setdefault("_wl_search_queries", {})


def _remember_search(context: ContextTypes.DEFAULT_TYPE, *, query: str, total: int, limit: int) -> str:
    cache = _search_cache(context)
    now = time.monotonic()
    for key, value in list(cache.items()):
        if now - float((value or {}).get("created_at", 0) or 0) > 900:
            cache.pop(key, None)
    query_id = uuid.uuid4().hex[:16]
    cache[query_id] = {
        "query": str(query or "").strip(),
        "total": int(total or 0),
        "limit": int(limit or 10),
        "created_at": now,
    }
    return query_id


def _numbered_keyboard(
    books: list[dict],
    *,
    query_id: str | None = None,
    page: int = 0,
    total: int | None = None,
    limit: int = 10,
    lang: str = "uz",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    start_index = max(0, int(page or 0)) * max(1, int(limit or 10)) + 1
    for idx, book in enumerate(books, start=start_index):
        book_id = str(book.get("id") or "").strip()
        if not book_id:
            continue
        row.append(InlineKeyboardButton(str(idx), callback_data=f"wlbook:{book_id}"))
        if len(row) >= 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if query_id and total is not None:
        text_bundle = _messages(lang)
        pages = max(1, (int(total or 0) + max(1, int(limit or 10)) - 1) // max(1, int(limit or 10)))
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text_bundle["prev_page"], callback_data=f"wlpage:{query_id}:{page - 1}"))
        if page + 1 < pages:
            nav.append(InlineKeyboardButton(text_bundle["next_page"], callback_data=f"wlpage:{query_id}:{page + 1}"))
        if nav:
            rows.append(nav)
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
    per_minute_limit = max(0, int(connected_bot.get("per_minute_send_limit") or 0))
    if per_minute_limit <= 0:
        return True
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
        limit = int(connected_bot.get("daily_send_limit") or 0)
        return limit > 0 and int(usage.get("sends") or 0) >= limit
    limit = int(connected_bot.get("daily_search_limit") or 0)
    return limit > 0 and int(usage.get("searches") or 0) >= limit


def _plan_allows(connected_bot: dict | None, feature: str) -> bool:
    return plan_allows(normalize_plan((connected_bot or {}).get("plan")), feature)


def _plan_is_paid_for_editing(connected_bot: dict | None) -> bool:
    return normalize_plan((connected_bot or {}).get("plan")) in {"PRO", "COMMUNITY"}


async def _require_paid_owner_edit(update: Update, connected_bot: dict | None, lang: str) -> bool:
    if _plan_is_paid_for_editing(connected_bot):
        return True
    if update.message:
        await update.message.reply_text(_messages(lang)["owner_custom_disabled"], reply_markup=_upgrade_url_keyboard(lang))
    return False


def _localized_text_args(args: list[str] | tuple[str, ...] | None, fallback_lang: str) -> tuple[str, str]:
    cleaned = [str(arg or "").strip() for arg in (args or []) if str(arg or "").strip()]
    target_lang = fallback_lang if fallback_lang in _SUPPORTED_LANGS else "en"
    if cleaned and cleaned[0].lower() in _SUPPORTED_LANGS:
        target_lang = cleaned.pop(0).lower()
    return target_lang, " ".join(cleaned).strip()


async def _wl_runtime_audit(
    connected_bot: dict | None,
    action: str,
    *,
    actor_user_id: int | None = None,
    details: dict | None = None,
    error_message: str | None = None,
) -> None:
    try:
        await asyncio.to_thread(
            create_white_label_audit_log,
            action=action,
            actor_user_id=actor_user_id,
            connected_bot_id=str((connected_bot or {}).get("id") or "").strip() or None,
            target_bot_username=str((connected_bot or {}).get("bot_username") or "").strip() or None,
            details=details or {},
            error_message=redact_token_like_strings(str(error_message or "")) if error_message else None,
        )
    except Exception:
        logger.debug("Failed to write connected bot audit log for %s", action, exc_info=True)


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


def _trial_seconds_remaining(connected_bot: dict) -> float | None:
    plan = str((connected_bot or {}).get("plan") or "").strip().upper()
    if plan != "TRIAL":
        return None
    trial_ends_at = (connected_bot or {}).get("trial_ends_at")
    if not trial_ends_at:
        return None
    try:
        if isinstance(trial_ends_at, datetime):
            expiry = trial_ends_at.replace(tzinfo=None)
        else:
            text = str(trial_ends_at).replace("Z", "").split("+", 1)[0].strip()
            expiry = datetime.fromisoformat(text).replace(tzinfo=None)
        return (expiry - datetime.utcnow()).total_seconds()
    except Exception:
        return None


async def _expire_trial_and_notify(context: ContextTypes.DEFAULT_TYPE, connected_bot: dict, lang: str) -> dict:
    connected_bot_id = str((connected_bot or {}).get("id") or "").strip()
    if not connected_bot_id:
        return connected_bot
    updated = await asyncio.to_thread(mark_connected_bot_trial_expired, connected_bot_id)
    if updated:
        context.application.bot_data["connected_bot"] = updated
        if updated.get("_should_notify_trial_expired"):
            await _notify_trial_expired_owner(dict(updated), lang)
            await _wl_runtime_audit(dict(updated), "TRIAL_EXPIRED", details={"trial_ends_at": updated.get("trial_ends_at")})
        return dict(updated)
    return connected_bot


async def _send_trial_expired_message(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    connected_bot: dict,
    lang: str,
    answer_callback: bool = False,
) -> None:
    text_bundle = _messages(lang)
    await _expire_trial_and_notify(context, connected_bot, lang)
    if answer_callback and update.callback_query:
        await update.callback_query.answer(text_bundle["trial_expired"][:180], show_alert=True)
        try:
            await context.bot.send_message(
                chat_id=int(update.callback_query.from_user.id),
                text=text_bundle["trial_expired"],
                reply_markup=_upgrade_url_keyboard(lang),
            )
        except Exception:
            logger.debug("Failed to send connected-bot trial expired callback message", exc_info=True)
        return
    if update.message:
        await update.message.reply_text(text_bundle["trial_expired"], reply_markup=_upgrade_url_keyboard(lang))


async def _load_active_connected_bot_or_warn(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
    *,
    feature: str | None = None,
    answer_callback: bool = False,
) -> dict | None:
    text_bundle = _messages(lang)
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot:
        if answer_callback and update.callback_query:
            await update.callback_query.answer(text_bundle["suspended"], show_alert=True)
        elif update.message:
            await update.message.reply_text(text_bundle["suspended"])
        return None
    if _connected_bot_trial_expired(connected_bot):
        await _send_trial_expired_message(
            update=update,
            context=context,
            connected_bot=connected_bot,
            lang=lang,
            answer_callback=answer_callback,
        )
        return None
    if str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
        if answer_callback and update.callback_query:
            await update.callback_query.answer(text_bundle["suspended"], show_alert=True)
        elif update.message:
            await update.message.reply_text(text_bundle["suspended"])
        return None
    if feature and not _plan_allows(connected_bot, feature):
        message = text_bundle["guest_disabled"] if feature == PLAN_FEATURE_GUEST_MODE else text_bundle["inline_disabled"]
        if answer_callback and update.callback_query:
            await update.callback_query.answer(message[:180], show_alert=True)
        elif update.message:
            await update.message.reply_text(message, reply_markup=_upgrade_url_keyboard(lang))
        return None
    return connected_bot


async def _send_book_to_private_user(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    connected_bot: dict,
    book_id: str,
    lang: str,
    requesting_message_id: int | None = None,
) -> None:
    text_bundle = _messages(lang)
    usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
    if (
        not _plan_allows(connected_bot, PLAN_FEATURE_PDF_DELIVERY)
        or _daily_limit_reached(connected_bot, usage, for_send=True)
        or not _can_send_now(context.application, connected_bot)
    ):
        await _wl_runtime_audit(
            connected_bot,
            "LIMIT_REACHED",
            actor_user_id=int(getattr(getattr(update, "effective_user", None), "id", 0) or 0) or None,
            details={"type": "send"},
        )
        if update.callback_query:
            await context.bot.send_message(chat_id=int(update.callback_query.from_user.id), text=text_bundle["limit_reached"])
        elif update.message:
            await update.message.reply_text(text_bundle["limit_reached"])
        return
    user_id = int(getattr(getattr(update, "effective_user", None), "id", 0) or 0)
    chat_id = int(getattr(getattr(update, "effective_chat", None), "id", 0) or user_id)
    result = await send_book_via_connected_bot(
        connected_bot=connected_bot,
        chat_id=chat_id,
        user_id=user_id,
        book_id=str(book_id or "").strip(),
        requesting_message_id=requesting_message_id,
        bot=context.bot,
        wait_seconds=WHITE_LABEL_CACHE_WAIT_SECONDS,
        preparing_text=text_bundle["preparing"],
    )
    if result.get("ok"):
        _record_send(context.application, connected_bot)
        return
    error_code = str(result.get("error_code") or "")
    await _wl_runtime_audit(
        connected_bot,
        "SEND_FAILED",
        actor_user_id=user_id or None,
        details={"book_id": book_id, "error_code": error_code},
        error_message=str(result.get("error") or error_code),
    )
    if error_code == "cache_wait_timeout":
        await context.bot.send_message(chat_id=user_id, text=text_bundle["retry_later"])
    elif error_code == "book_unavailable":
        await context.bot.send_message(chat_id=user_id, text=text_bundle["book_unavailable"])
    else:
        await context.bot.send_message(chat_id=user_id, text=text_bundle["send_failed"])


async def _load_owner_connected_bot_or_warn(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> dict | None:
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot or not _is_connected_bot_owner(update, connected_bot):
        if update.message:
            await update.message.reply_text(_messages(lang)["owner_only"])
        return None
    return connected_bot


def _owner_panel_text(connected_bot: dict, usage: dict | None, lang: str) -> str:
    text_bundle = _messages(lang)
    plan = normalize_plan(connected_bot.get("plan"))
    status = str(connected_bot.get("status") or "-").upper()
    owner_id = _connected_bot_owner_id(connected_bot)
    limits = (
        f"{int(connected_bot.get('daily_search_limit') or 0) or 'unlimited'} searches/day, "
        f"{int(connected_bot.get('daily_send_limit') or 0) or 'unlimited'} sends/day"
    )
    lines = [
        text_bundle["owner_panel_title"],
        "",
        f"Bot: {format_connected_bot_reference(connected_bot)}",
        f"Owner ID: {owner_id}",
        f"Status: {status}",
        f"Plan: {plan}",
        f"Features: {plan_feature_summary(plan)}",
        f"Limits: {limits}",
        f"Results/page: {int(connected_bot.get('search_results_limit') or 10)}",
        "",
        f"Today: searches {int((usage or {}).get('searches') or 0)} / sends {int((usage or {}).get('sends') or 0)}",
        f"Cache: hits {int((usage or {}).get('cache_hits') or 0)} / misses {int((usage or {}).get('cache_misses') or 0)}",
        "",
        text_bundle["owner_commands_hint"],
    ]
    return "\n".join(lines)


async def _handle_owner_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
    await update.message.reply_text(_owner_panel_text(connected_bot, usage, lang))


async def _handle_owner_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    await update.message.reply_text(_welcome_text(connected_bot, lang))


async def _handle_owner_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_owner_panel(update, context)


async def _handle_set_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    title = " ".join(str(arg or "").strip() for arg in (context.args or [])).strip()
    if not title:
        await update.message.reply_text(text_bundle["owner_title_usage"])
        return
    title = title[:80].strip()
    updated = await asyncio.to_thread(update_connected_bot_public_settings, str(connected_bot.get("id") or ""), branding_title=title)
    if updated:
        context.application.bot_data["connected_bot"] = updated
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    else:
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_set_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    target_lang, greeting = _localized_text_args(context.args, lang)
    if not greeting:
        await update.message.reply_text(text_bundle["owner_greeting_usage"])
        return
    greeting = greeting[:1200].strip()
    field = f"welcome_text_{target_lang}"
    updated = await asyncio.to_thread(update_connected_bot_public_settings, str(connected_bot.get("id") or ""), **{field: greeting})
    if updated:
        context.application.bot_data["connected_bot"] = updated
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    else:
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_reset_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    args = [str(arg or "").strip().lower() for arg in (context.args or []) if str(arg or "").strip()]
    target = args[0] if args else lang
    if target not in _SUPPORTED_LANGS and target != "all":
        await update.message.reply_text(text_bundle["owner_reset_greeting_usage"])
        return
    fields = {f"welcome_text_{code}": "" for code in (_SUPPORTED_LANGS if target == "all" else {target})}
    updated = await asyncio.to_thread(update_connected_bot_public_settings, str(connected_bot.get("id") or ""), **fields)
    if updated:
        context.application.bot_data["connected_bot"] = updated
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    else:
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_set_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    target_lang, about = _localized_text_args(context.args, lang)
    if not about:
        await update.message.reply_text(text_bundle["owner_about_usage"])
        return
    try:
        await context.bot.set_my_short_description(short_description=about[:120].strip(), language_code=target_lang)
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    except Exception:
        logger.debug("Failed to set connected bot short description", exc_info=True)
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_set_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    target_lang, description = _localized_text_args(context.args, lang)
    if not description:
        await update.message.reply_text(text_bundle["owner_description_usage"])
        return
    try:
        await context.bot.set_my_description(description=description[:512].strip(), language_code=target_lang)
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    except Exception:
        logger.debug("Failed to set connected bot description", exc_info=True)
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_reset_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    args = [str(arg or "").strip().lower() for arg in (context.args or []) if str(arg or "").strip()]
    target = args[0] if args else lang
    if target not in _SUPPORTED_LANGS and target != "all":
        await update.message.reply_text(text_bundle["owner_reset_about_usage"])
        return
    try:
        for code in (_SUPPORTED_LANGS if target == "all" else {target}):
            await context.bot.set_my_short_description(short_description="", language_code=code)
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    except Exception:
        logger.debug("Failed to reset connected bot short description", exc_info=True)
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_reset_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    if not await _require_paid_owner_edit(update, connected_bot, lang):
        return
    args = [str(arg or "").strip().lower() for arg in (context.args or []) if str(arg or "").strip()]
    target = args[0] if args else lang
    if target not in _SUPPORTED_LANGS and target != "all":
        await update.message.reply_text(text_bundle["owner_reset_description_usage"])
        return
    try:
        for code in (_SUPPORTED_LANGS if target == "all" else {target}):
            await context.bot.set_my_description(description="", language_code=code)
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    except Exception:
        logger.debug("Failed to reset connected bot description", exc_info=True)
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _handle_set_results(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_owner_connected_bot_or_warn(update, context, lang)
    if not connected_bot:
        return
    args = [str(arg or "").strip() for arg in (context.args or []) if str(arg or "").strip()]
    if not args:
        await update.message.reply_text(text_bundle["owner_results_usage"])
        return
    try:
        value = int(args[0])
    except Exception:
        await update.message.reply_text(text_bundle["owner_results_usage"])
        return
    if value < 1 or value > 10:
        await update.message.reply_text(text_bundle["owner_results_usage"])
        return
    updated = await asyncio.to_thread(update_connected_bot_public_settings, str(connected_bot.get("id") or ""), search_results_limit=value)
    if updated:
        context.application.bot_data["connected_bot"] = updated
        await update.message.reply_text(text_bundle["owner_settings_saved"])
    else:
        await update.message.reply_text(text_bundle["owner_settings_invalid"])


async def _trial_expiry_task(application) -> None:
    connected_bot_id = str(application.bot_data.get("connected_bot_id") or "").strip()
    if not connected_bot_id:
        return
    while True:
        connected_bot = await asyncio.to_thread(get_connected_bot_by_id, connected_bot_id)
        if not connected_bot:
            return
        if _connected_bot_trial_expired(connected_bot):
            break
        remaining = _trial_seconds_remaining(connected_bot)
        if remaining is None:
            return
        await asyncio.sleep(max(1.0, min(float(remaining), 3600.0)))
    updated = await asyncio.to_thread(mark_connected_bot_trial_expired, connected_bot_id)
    if updated and updated.get("_should_notify_trial_expired"):
        await _notify_trial_expired_owner(dict(updated), "uz")
        await _wl_runtime_audit(dict(updated), "TRIAL_EXPIRED", details={"trial_ends_at": updated.get("trial_ends_at")})
    stop_running = getattr(application, "stop_running", None)
    if callable(stop_running):
        stop_running()


async def _runtime_heartbeat_task(application) -> None:
    connected_bot_id = str(application.bot_data.get("connected_bot_id") or "").strip()
    if not connected_bot_id:
        return
    while True:
        try:
            updated = await asyncio.to_thread(touch_connected_bot_runtime_heartbeat, connected_bot_id, pid=os.getpid())
            if updated:
                application.bot_data["connected_bot"] = updated
        except Exception:
            logger.debug("Failed to update connected bot runtime heartbeat", exc_info=True)
        await asyncio.sleep(30)


async def _handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if getattr(update.effective_chat, "type", "") != "private":
        return
    connected_bot = await _load_connected_bot_config(context)
    lang = await _lang_from_context(update, context)
    await _touch_connected_bot_user(update, context, lang)
    if connected_bot and _is_connected_bot_owner(update, connected_bot):
        await _set_connected_bot_commands(context.application, connected_bot)
    payload = str((context.args or [""])[0] if getattr(context, "args", None) else "").strip()
    book_id = _parse_book_start_payload(payload)
    await update.message.reply_text(_welcome_text(connected_bot, lang))
    if book_id and connected_bot:
        active_bot = await _load_active_connected_bot_or_warn(update, context, lang, feature=PLAN_FEATURE_PDF_DELIVERY)
        if active_bot:
            await _send_book_to_private_user(
                update=update,
                context=context,
                connected_bot=active_bot,
                book_id=book_id,
                lang=lang,
                requesting_message_id=int(update.message.message_id),
            )


async def _handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if getattr(update.effective_chat, "type", "") != "private":
        return
    lang = await _lang_from_context(update, context)
    await _touch_connected_bot_user(update, context, lang)
    await update.message.reply_text(_messages(lang)["help"])


async def _handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    lang = await _lang_from_context(update, context)
    await _touch_connected_bot_user(update, context, lang)
    text_bundle = _messages(lang)
    if getattr(update.effective_chat, "type", "") != "private":
        await update.message.reply_text(text_bundle["private_only"])
        return
    connected_bot = await _load_active_connected_bot_or_warn(update, context, lang, feature=PLAN_FEATURE_PRIVATE_SEARCH)
    if not connected_bot:
        return
    usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
    if _daily_limit_reached(connected_bot, usage, for_send=False):
        await _wl_runtime_audit(
            connected_bot,
            "LIMIT_REACHED",
            actor_user_id=int(getattr(update.effective_user, "id", 0) or 0) or None,
            details={"type": "search"},
        )
        await update.message.reply_text(text_bundle["limit_reached"])
        return

    query = str(update.message.text or "").strip()
    if not query:
        return
    result_limit = _search_results_limit(connected_bot)
    books_result = await asyncio.to_thread(search_connected_books_page, query, limit=result_limit, offset=0)
    books = list(books_result.get("books") or [])
    total = int(books_result.get("total") or 0)
    await asyncio.to_thread(increment_connected_bot_usage, str(connected_bot.get("id") or ""), searches=1)
    if books:
        await asyncio.to_thread(db_increment_book_searches, [str(book.get("id") or "") for book in books if str(book.get("id") or "").strip()])
    if not books:
        await update.message.reply_text(text_bundle["results_empty"])
        return

    pages = max(1, (total + result_limit - 1) // result_limit)
    query_id = _remember_search(context, query=query, total=total, limit=result_limit)
    result_text = build_results_message(query, books, page=1, pages=pages, total=total, start_index=1, lang=lang)
    await update.message.reply_text(
        result_text,
        reply_markup=_numbered_keyboard(books, query_id=query_id, page=0, total=total, limit=result_limit, lang=lang),
    )


async def _handle_search_page(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    try:
        _prefix, query_id, page_raw = str(query.data or "").split(":", 2)
        page = max(0, int(page_raw or 0))
    except Exception:
        await query.answer()
        return
    search_state = _search_cache(context).get(query_id)
    if not search_state:
        await query.answer(text_bundle["results_empty"], show_alert=True)
        return
    connected_bot = await _load_active_connected_bot_or_warn(update, context, lang, feature=PLAN_FEATURE_PRIVATE_SEARCH, answer_callback=True)
    if not connected_bot:
        return
    search_query = str(search_state.get("query") or "").strip()
    result_limit = max(1, min(10, int(search_state.get("limit") or _search_results_limit(connected_bot))))
    books_result = await asyncio.to_thread(
        search_connected_books_page,
        search_query,
        limit=result_limit,
        offset=page * result_limit,
    )
    books = list(books_result.get("books") or [])
    total = int(books_result.get("total") or search_state.get("total") or 0)
    if not books:
        await query.answer(text_bundle["results_empty"], show_alert=True)
        return
    pages = max(1, (total + result_limit - 1) // result_limit)
    page = min(page, pages - 1)
    result_text = build_results_message(
        search_query,
        books,
        page=page + 1,
        pages=pages,
        total=total,
        start_index=page * result_limit + 1,
        lang=lang,
    )
    await query.answer()
    if query.message:
        await query.message.edit_text(
            result_text,
            reply_markup=_numbered_keyboard(books, query_id=query_id, page=page, total=total, limit=result_limit, lang=lang),
        )


async def _handle_book_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    try:
        data = str(query.data or "")
        if not data.startswith("wlbook:"):
            await query.answer()
            return
        _, book_id = data.split(":", 1)
        connected_bot = await _load_active_connected_bot_or_warn(update, context, lang, feature=PLAN_FEATURE_PDF_DELIVERY, answer_callback=True)
        if not connected_bot:
            return
        await query.answer()
        await _send_book_to_private_user(
            update=update,
            context=context,
            connected_bot=connected_bot,
            book_id=str(book_id or "").strip(),
            requesting_message_id=int(query.message.message_id) if query.message else None,
            lang=lang,
        )
    except Exception as exc:
        logger.error("connected bot delivery failed: %s", redact_token_like_strings(str(exc)), exc_info=True)
        await query.answer(text_bundle["send_failed"], show_alert=True)


def _book_title(book: dict | None) -> str:
    return str((book or {}).get("display_name") or (book or {}).get("book_name") or (book or {}).get("id") or "Book").strip()


def _inline_action_keyboard(context: ContextTypes.DEFAULT_TYPE, connected_bot: dict, lang: str, *, book_id: str | None = None) -> InlineKeyboardMarkup:
    text_bundle = _messages(lang)
    payload = _book_start_payload(book_id) if book_id else None
    url = _connected_bot_url(context, connected_bot, payload=payload)
    rows = []
    if url:
        rows.append([InlineKeyboardButton(text_bundle["open_bot"], url=url)])
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton(text_bundle["open_bot"], url=_connected_bot_url(context, connected_bot) or "https://t.me/pdf_audio_kitoblar_bot")]])


async def _answer_inline_disabled(update: Update, lang: str, message: str) -> None:
    if not update.inline_query:
        return
    result = InlineQueryResultArticle(
        id="white_label_plan_disabled",
        title=message[:64],
        description=message[:128],
        input_message_content=InputTextMessageContent(message),
    )
    await update.inline_query.answer([result], cache_time=3, is_personal=True)


async def _handle_inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    inline = update.inline_query
    if not inline:
        return
    lang = await _lang_from_context(update, context)
    text_bundle = _messages(lang)
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot or str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
        await _answer_inline_disabled(update, lang, text_bundle["suspended"])
        return
    if _connected_bot_trial_expired(connected_bot):
        await _expire_trial_and_notify(context, connected_bot, lang)
        await _answer_inline_disabled(update, lang, text_bundle["trial_expired"])
        return
    if not _plan_allows(connected_bot, PLAN_FEATURE_INLINE_SEARCH):
        await _answer_inline_disabled(update, lang, text_bundle["inline_disabled"])
        return
    query = str(inline.query or "").strip()
    if not query:
        await inline.answer([], cache_time=20, is_personal=True)
        return
    usage = await asyncio.to_thread(get_connected_bot_usage, str(connected_bot.get("id") or ""))
    if _daily_limit_reached(connected_bot, usage, for_send=False):
        await _wl_runtime_audit(
            connected_bot,
            "LIMIT_REACHED",
            actor_user_id=int(getattr(getattr(update, "effective_user", None), "id", 0) or 0) or None,
            details={"type": "inline_search"},
        )
        await _answer_inline_disabled(update, lang, text_bundle["limit_reached"])
        return
    result_limit = _search_results_limit(connected_bot)
    books_result = await asyncio.to_thread(search_connected_books_page, query, limit=result_limit, offset=0)
    books = list(books_result.get("books") or [])
    await asyncio.to_thread(increment_connected_bot_usage, str(connected_bot.get("id") or ""), searches=1)
    results = []
    for book in books[:10]:
        book_id = str(book.get("id") or "").strip()
        if not book_id:
            continue
        title = _book_title(book)
        safe_title = html.escape(title)
        cache_row = await asyncio.to_thread(
            get_connected_bot_file_cache,
            str(connected_bot.get("id") or ""),
            book_id,
            only_valid=True,
        )
        cached_file_id = str((cache_row or {}).get("telegram_file_id") or "").strip()
        if cached_file_id:
            results.append(
                InlineQueryResultCachedDocument(
                    id=f"cached_{book_id}",
                    title=f"📖 {title}",
                    document_file_id=cached_file_id,
                    description=text_bundle["inline_cached"],
                    caption=f"📖 {safe_title}\n\n{html.escape(_powered_by_text(lang))}",
                    parse_mode="HTML",
                    reply_markup=_inline_action_keyboard(context, connected_bot, lang),
                )
            )
        else:
            message = f"📖 {safe_title}\n\n{html.escape(text_bundle['open_to_prepare'])}\n{html.escape(_powered_by_text(lang))}"
            results.append(
                InlineQueryResultArticle(
                    id=f"open_{book_id}",
                    title=f"📖 {title}",
                    description=text_bundle["open_to_prepare"],
                    input_message_content=InputTextMessageContent(message, parse_mode="HTML"),
                    reply_markup=_inline_action_keyboard(context, connected_bot, lang, book_id=book_id),
                )
            )
    await inline.answer(results, cache_time=20, is_personal=True)


def _parse_guest_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[Message | None, dict[str, Any] | None]:
    raw = getattr(update, "api_kwargs", {}).get("guest_message")
    if not isinstance(raw, dict):
        return None, None
    try:
        return Message.de_json(raw, context.bot), raw
    except Exception:
        logger.debug("Failed to parse connected-bot guest_message update", exc_info=True)
        return None, raw


def _guest_lang(guest_message: Message | None, raw: dict[str, Any] | None) -> str:
    lang_code = getattr(getattr(guest_message, "from_user", None), "language_code", None)
    if not lang_code and isinstance(raw, dict):
        caller = raw.get("guest_bot_caller_user")
        if isinstance(caller, dict):
            lang_code = caller.get("language_code")
    text = str(lang_code or "").lower()
    if text.startswith("uz"):
        return "uz"
    if text.startswith("ru"):
        return "ru"
    return "en"


def _extract_guest_query_text(text: str, bot_username: str) -> tuple[str | None, str]:
    raw = str(text or "").strip()
    if not raw:
        return None, ""
    first_token = raw.split(None, 1)[0]
    command_match = re.match(r"^/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?$", first_token)
    if command_match:
        cmd = str(command_match.group(1) or "").lower()
        target = str(command_match.group(2) or "").lower()
        if not target or target == bot_username.lower():
            return cmd, raw[len(first_token):].strip()
    stripped = re.sub(rf"@{re.escape(bot_username)}\b", " ", raw, flags=re.IGNORECASE)
    return None, re.sub(r"\s+", " ", stripped).strip(" ,:-")


async def _answer_guest_query(
    context: ContextTypes.DEFAULT_TYPE,
    guest_query_id: str,
    text: str,
    *,
    title: str,
    description: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    result: dict[str, object] = {
        "type": "article",
        "id": uuid.uuid4().hex[:32],
        "title": str(title or "Book search")[:64],
        "description": str(description or "")[:128],
        "input_message_content": {
            "message_text": str(text or "")[:4096],
        },
    }
    if reply_markup:
        result["reply_markup"] = reply_markup.to_dict()
    await context.bot._post(
        "answerGuestQuery",
        {
            "guest_query_id": guest_query_id,
            "result": json.dumps(result, ensure_ascii=False),
        },
    )


async def _handle_guest_message_update(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    guest_message, raw = _parse_guest_message(update, context)
    if not guest_message:
        return
    guest_query_id = str((raw or {}).get("guest_query_id") or getattr(guest_message, "api_kwargs", {}).get("guest_query_id") or "").strip()
    if not guest_query_id:
        return
    lang = _guest_lang(guest_message, raw)
    text_bundle = _messages(lang)
    connected_bot = await _load_connected_bot_config(context)
    if not connected_bot or str(connected_bot.get("status") or "").upper() != WL_STATUS_ACTIVE:
        await _answer_guest_query(context, guest_query_id, text_bundle["suspended"], title="Bot stopped", description=text_bundle["suspended"])
        return
    if _connected_bot_trial_expired(connected_bot):
        await _expire_trial_and_notify(context, connected_bot, lang)
        await _answer_guest_query(context, guest_query_id, text_bundle["trial_expired"], title="Trial ended", description=text_bundle["trial_expired"])
        return
    if not _plan_allows(connected_bot, PLAN_FEATURE_GUEST_MODE):
        await _answer_guest_query(
            context,
            guest_query_id,
            text_bundle["guest_disabled"],
            title="Pro feature",
            description=text_bundle["guest_disabled"],
            reply_markup=_upgrade_url_keyboard(lang),
        )
        return
    bot_username = _connected_bot_username(context, connected_bot)
    raw_text = str(getattr(guest_message, "text", None) or getattr(guest_message, "caption", None) or "").strip()
    command_name, query_text = _extract_guest_query_text(raw_text, bot_username)
    if command_name in {"start", "help"} or not query_text:
        await _answer_guest_query(
            context,
            guest_query_id,
            _welcome_text(connected_bot, lang),
            title=_branding_title(connected_bot),
            description=text_bundle["help"],
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text_bundle["open_bot"], url=_connected_bot_url(context, connected_bot))]]),
        )
        return
    books_result = await asyncio.to_thread(search_connected_books_page, query_text, limit=5, offset=0)
    books = list(books_result.get("books") or [])
    await asyncio.to_thread(increment_connected_bot_usage, str(connected_bot.get("id") or ""), searches=1)
    if not books:
        await _answer_guest_query(
            context,
            guest_query_id,
            f"{text_bundle['results_empty']}\n\n{_powered_by_text(lang)}",
            title="No results",
            description=query_text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text_bundle["open_bot"], url=_connected_bot_url(context, connected_bot))]]),
        )
        return
    lines = [f"📚 {query_text}", ""]
    rows: list[list[InlineKeyboardButton]] = []
    number_row: list[InlineKeyboardButton] = []
    for idx, book in enumerate(books, start=1):
        book_id = str(book.get("id") or "").strip()
        lines.append(f"{idx}. {_book_title(book)}")
        url = _connected_bot_url(context, connected_bot, payload=_book_start_payload(book_id))
        if url:
            number_row.append(InlineKeyboardButton(str(idx), url=url))
    if number_row:
        rows.append(number_row)
    bot_url = _connected_bot_url(context, connected_bot)
    if bot_url:
        rows.append([InlineKeyboardButton(text_bundle["open_bot"], url=bot_url)])
    lines.extend(["", text_bundle["open_to_prepare"], _powered_by_text(lang)])
    await _answer_guest_query(
        context,
        guest_query_id,
        "\n".join(lines).strip(),
        title="Search results",
        description=f"{len(books)} results",
        reply_markup=InlineKeyboardMarkup(rows) if rows else None,
    )


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


async def _set_connected_bot_commands(application, connected_bot: dict) -> None:
    default_commands = [
        BotCommand("start", "Start bot"),
        BotCommand("help", "How to search books"),
    ]
    owner_commands = [
        BotCommand("start", "Start bot"),
        BotCommand("help", "How to search books"),
        BotCommand("owner", "Open owner panel"),
        BotCommand("settitle", "Edit bot title"),
        BotCommand("setgreeting", "Edit greeting text"),
        BotCommand("resetgreeting", "Reset greeting text"),
        BotCommand("setabout", "Edit bot about text"),
        BotCommand("setdescription", "Edit bot description"),
        BotCommand("resetabout", "Reset bot about text"),
        BotCommand("resetdescription", "Reset bot description"),
        BotCommand("setresults", "Set results per page"),
        BotCommand("preview", "Preview greeting"),
        BotCommand("stats", "Usage and plan stats"),
    ]
    try:
        await application.bot.set_my_commands(default_commands)
    except Exception:
        logger.debug("Failed to set connected bot default commands", exc_info=True)
    owner_id = _connected_bot_owner_id(connected_bot)
    if not owner_id:
        return
    try:
        await application.bot.set_my_commands(owner_commands, scope=BotCommandScopeChat(chat_id=owner_id))
    except Exception:
        logger.debug("Failed to set connected bot owner commands for %s", owner_id, exc_info=True)


async def _post_init(application) -> None:
    connected_bot_id = str(application.bot_data.get("connected_bot_id") or "").strip()
    connected_bot = await asyncio.to_thread(get_connected_bot_by_id, connected_bot_id)
    if not connected_bot:
        raise RuntimeError(f"Connected bot {connected_bot_id} was not found")
    me = await application.bot.get_me()
    if int(me.id or 0) != int(connected_bot.get("bot_telegram_id") or 0):
        await asyncio.to_thread(update_connected_bot_status, connected_bot_id, "ERROR", last_error="getMe bot id does not match the stored connected bot record")
        raise RuntimeError("Connected bot token does not match the stored bot record")
    application.bot_data["connected_bot_username"] = str(getattr(me, "username", "") or "").strip()
    await asyncio.to_thread(record_connected_bot_verification, connected_bot_id, last_error=None)
    await _set_connected_bot_commands(application, connected_bot)
    await asyncio.to_thread(touch_connected_bot_runtime_heartbeat, connected_bot_id, pid=os.getpid())
    application.create_task(_runtime_heartbeat_task(application))
    application.create_task(_trial_expiry_task(application))
    logger.info("Connected bot runtime ready: id=%s ref=%s", connected_bot_id, format_connected_bot_reference(connected_bot))


def build_application(token: str, connected_bot_id: str):
    builder = ApplicationBuilder().token(token).post_init(_post_init)
    builder = configure_application_builder(builder)
    application = builder.build()
    application.bot_data["connected_bot_id"] = connected_bot_id
    application.add_handler(CommandHandler("start", _handle_start))
    application.add_handler(CommandHandler("help", _handle_help))
    application.add_handler(CommandHandler(["owner", "admin", "settings"], _handle_owner_panel))
    application.add_handler(CommandHandler("settitle", _handle_set_title))
    application.add_handler(CommandHandler("setgreeting", _handle_set_greeting))
    application.add_handler(CommandHandler("resetgreeting", _handle_reset_greeting))
    application.add_handler(CommandHandler("setabout", _handle_set_about))
    application.add_handler(CommandHandler("setdescription", _handle_set_description))
    application.add_handler(CommandHandler("resetabout", _handle_reset_about))
    application.add_handler(CommandHandler("resetdescription", _handle_reset_description))
    application.add_handler(CommandHandler("setresults", _handle_set_results))
    application.add_handler(CommandHandler("preview", _handle_owner_preview))
    application.add_handler(CommandHandler("stats", _handle_owner_stats))
    application.add_handler(InlineQueryHandler(_handle_inline_query))
    application.add_handler(CallbackQueryHandler(_handle_search_page, pattern=r"^wlpage:"))
    application.add_handler(CallbackQueryHandler(_handle_book_pick, pattern=r"^wlbook:"))
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, _handle_cache_channel_post))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, _handle_search))
    application.add_handler(TypeHandler(Update, _handle_guest_message_update), group=100)
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
        application.run_polling(
            allowed_updates=["message", "callback_query", "channel_post", "inline_query", "guest_message"],
            drop_pending_updates=False,
        )
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
