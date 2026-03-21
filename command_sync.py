from __future__ import annotations

import logging
import time
from typing import Iterable

from telegram import BotCommand
from telegram import (
    BotCommandScopeAllChatAdministrators,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeDefault,
)
from telegram.error import NetworkError, RetryAfter, TimedOut

_PUBLIC_PREFERRED_ORDER = ("start", "random", "language", "myprofile", "favorite", "request", "requests")
_COMMAND_SYNC_BACKOFF_KEY = "command_sync_backoff_until"
_USER_COMMANDS_LANG_CACHE_KEY = "user_commands_lang_cache"
_USER_COMMANDS_LAST_SYNC_KEY = "user_commands_last_sync"
_USER_COMMANDS_MIN_INTERVAL_S = 6 * 60 * 60


def get_public_commands(lang: str = "en") -> list[BotCommand]:
    localized = {
        "en": [
            BotCommand("start", "🚀 Start / choose language"),
            BotCommand("random", "🎲 Get 10 random books"),
            BotCommand("help", "❓ How to use the bot"),
            BotCommand("pdf_maker", "📄 Make PDF from text"),
            BotCommand("pdf_editor", "🧰 Edit PDF files"),
            BotCommand("text_to_voice", "🎙️ Convert text to voice"),
            BotCommand("myprofile", "👤 My profile"),
            BotCommand("top", "🔥 Top books"),
            BotCommand("top_users", "🏆 Top users"),
            BotCommand("favorite", "⭐ Your favorites"),
            BotCommand("request", "📝 Request a book"),
            BotCommand("requests", "📋 Your requests"),
            BotCommand("my_quiz", "📝 My saved quiz tests"),
            BotCommand("upload", "⬆️ Upload books to the bot"),
            BotCommand("language", "🌐 Change language"),
        ],
        "ru": [
            BotCommand("start", "🚀 Запуск / выбор языка"),
            BotCommand("random", "🎲 10 случайных книг"),
            BotCommand("help", "❓ Как пользоваться ботом"),
            BotCommand("pdf_maker", "📄 PDF из текста"),
            BotCommand("pdf_editor", "🧰 Редактировать PDF"),
            BotCommand("text_to_voice", "🎙️ Текст в голос"),
            BotCommand("myprofile", "👤 Профиль"),
            BotCommand("top", "🔥 Топ книги"),
            BotCommand("top_users", "🏆 Топ пользователей"),
            BotCommand("favorite", "⭐ Избранные книги"),
            BotCommand("request", "📝 Запросить книгу"),
            BotCommand("requests", "📋 Мои запросы"),
            BotCommand("my_quiz", "📝 Мои quiz-тесты"),
            BotCommand("upload", "⬆️ Загружать книги в бота"),
            BotCommand("language", "🌐 Сменить язык"),
        ],
        "uz": [
            BotCommand("start", "🚀 Botni ishga tushirish / til tanlash"),
            BotCommand("random", "🎲 10 ta tasodifiy kitob"),
            BotCommand("help", "❓ Botdan foydalanish"),
            BotCommand("pdf_maker", "📄 Matndan PDF yaratish"),
            BotCommand("pdf_editor", "🧰 PDF fayl tahrirlash"),
            BotCommand("text_to_voice", "🎙️ Matndan ovoz yaratish"),
            BotCommand("myprofile", "👤 Profilim"),
            BotCommand("top", "🔥 Top kitoblar"),
            BotCommand("top_users", "🏆 Top foydalanuvchilar"),
            BotCommand("favorite", "⭐ Sevimli kitoblar"),
            BotCommand("request", "📝 Kitob so‘rash"),
            BotCommand("requests", "📋 So‘rovlarim"),
            BotCommand("my_quiz", "📝 Mening quiz testlarim"),
            BotCommand("upload", "⬆️ Botga kitob yuklash"),
            BotCommand("language", "🌐 Tilni o‘zgartirish"),
        ],
    }
    return localized.get(lang, localized["en"])


def _order_commands(
    commands: Iterable[BotCommand],
    preferred_order: Iterable[str],
) -> list[BotCommand]:
    by_name = {cmd.command: cmd for cmd in commands}
    ordered: list[BotCommand] = []
    used: set[str] = set()
    for name in preferred_order:
        cmd = by_name.get(name)
        if cmd:
            ordered.append(cmd)
            used.add(name)
    for cmd in commands:
        if cmd.command not in used:
            ordered.append(cmd)
    return ordered


def get_public_commands_for_menu(lang: str = "en") -> list[BotCommand]:
    return _order_commands(list(get_public_commands(lang)), _PUBLIC_PREFERRED_ORDER)


def _get_sync_backoff_until(application) -> float:
    try:
        return float(getattr(application, "bot_data", {}).get(_COMMAND_SYNC_BACKOFF_KEY, 0) or 0)
    except Exception:
        return 0.0


def _set_sync_backoff(application, seconds: float) -> None:
    try:
        until = time.time() + max(0.0, float(seconds))
        application.bot_data[_COMMAND_SYNC_BACKOFF_KEY] = until
    except Exception:
        pass


def get_group_commands(lang: str = "en") -> list[BotCommand]:
    by_name = {cmd.command: cmd for cmd in get_public_commands(lang)}
    ordered: list[BotCommand] = []
    for name in ("start", "language", "random"):
        cmd = by_name.get(name)
        if cmd:
            ordered.append(cmd)
    return ordered


def get_group_admin_commands(lang: str = "en") -> list[BotCommand]:
    del lang
    return []


def get_admin_commands(lang: str = "en") -> list[BotCommand]:
    # Keep upload commands out of public menu, but include them for admin command menu.
    base = list(get_public_commands_for_menu(lang))
    by_name = {cmd.command: cmd for cmd in get_public_commands(lang)}
    present = {cmd.command for cmd in base}
    for name in ("upload",):
        cmd = by_name.get(name)
        if cmd and name not in present:
            base.append(cmd)

    return base + [
        BotCommand("admin", "🛠️ Admin control panel"),
        BotCommand("smoke", "🧪 Smoke test checklist (admin)"),
        BotCommand("broadcast", "📣 Broadcast to all users (admin)"),
        BotCommand("audit", "🧾 Audit stats (admin)"),
        BotCommand("prune", "🧹 Remove blocked users (admin)"),
        BotCommand("missing", "⚠️ Missing book files (admin)"),
        BotCommand("db_dupes", "🧼 Clean DB duplicates (admin)"),
        BotCommand("es_dupes", "🧼 Clean ES duplicates (admin)"),
        BotCommand("dupes_status", "📊 Duplicate cleanup status (admin)"),
        BotCommand("cancel_task", "🛑 Cancel background task (admin)"),
        BotCommand("user", "👤 Search users (admin)"),
        BotCommand("pause_bot", "⏸️ Pause the bot (admin)"),
        BotCommand("resume_bot", "▶️ Resume the bot (admin)"),
        BotCommand("upload_local_books", "⬆️ Upload local books (admin)"),
    ]


async def set_bot_commands(
    application,
    *,
    owner_id: int | None,
    logger: logging.Logger,
) -> None:
    backoff_until = _get_sync_backoff_until(application)
    if backoff_until > time.time():
        logger.warning(
            "Skipping bot command sync due to active backoff for %.0fs",
            max(0.0, backoff_until - time.time()),
        )
        return
    try:
        await application.bot.delete_my_commands(scope=BotCommandScopeAllChatAdministrators())

        await application.bot.set_my_commands(
            get_public_commands_for_menu("en"),
            scope=BotCommandScopeDefault(),
        )
        await application.bot.set_my_commands(
            get_public_commands_for_menu("en"),
            scope=BotCommandScopeAllPrivateChats(),
        )
        await application.bot.set_my_commands(
            get_group_commands("en"),
            scope=BotCommandScopeAllGroupChats(),
        )
        for lang in ("en", "ru", "uz"):
            public_commands = get_public_commands_for_menu(lang)
            await application.bot.set_my_commands(
                public_commands,
                scope=BotCommandScopeDefault(),
                language_code=lang,
            )
            await application.bot.set_my_commands(
                public_commands,
                scope=BotCommandScopeAllPrivateChats(),
                language_code=lang,
            )
            await application.bot.set_my_commands(
                get_group_commands(lang),
                scope=BotCommandScopeAllGroupChats(),
                language_code=lang,
            )
            admin_commands = get_group_admin_commands(lang)
            if admin_commands:
                await application.bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeAllChatAdministrators(),
                    language_code=lang,
                )
        logger.debug("Bot commands set successfully")
    except RetryAfter as e:
        retry_after = float(getattr(e, "retry_after", 60) or 60)
        _set_sync_backoff(application, retry_after + 1)
        logger.warning("Bot command sync hit flood control. Retry after %.0fs", retry_after)
    except (TimedOut, NetworkError) as e:
        _set_sync_backoff(application, 120)
        logger.warning("Bot command sync deferred after transient Telegram error: %s", e)
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")


async def sync_user_commands_if_needed(
    context,
    *,
    user_id: int | None,
    lang: str,
    owner_id: int | None,
    logger: logging.Logger,
    force: bool = False,
) -> None:
    if not context or not user_id:
        return
    app = getattr(context, "application", None)
    if not app or not getattr(app, "running", False):
        return
    backoff_until = _get_sync_backoff_until(app)
    if backoff_until > time.time():
        logger.debug(
            "Skipping user command sync for %s due to active backoff for %.0fs",
            user_id,
            max(0.0, backoff_until - time.time()),
        )
        return
    try:
        cache = app.bot_data.setdefault(_USER_COMMANDS_LANG_CACHE_KEY, {})
        last_sync = app.bot_data.setdefault(_USER_COMMANDS_LAST_SYNC_KEY, {})
        cached_lang = cache.get(user_id)
        last_sync_at = float(last_sync.get(user_id, 0) or 0)
        if not force and cached_lang == lang and (time.time() - last_sync_at) < _USER_COMMANDS_MIN_INTERVAL_S:
            return
        commands = get_admin_commands(lang) if owner_id and int(user_id) == int(owner_id) else get_public_commands_for_menu(lang)
        await context.bot.set_my_commands(
            commands,
            scope=BotCommandScopeChat(chat_id=user_id),
        )
        cache[user_id] = lang
        last_sync[user_id] = time.time()
    except RetryAfter as e:
        retry_after = float(getattr(e, "retry_after", 60) or 60)
        _set_sync_backoff(app, retry_after + 1)
        logger.warning("User command sync hit flood control for %s. Retry after %.0fs", user_id, retry_after)
    except (TimedOut, NetworkError) as e:
        _set_sync_backoff(app, 120)
        logger.warning("User command sync deferred for %s after transient Telegram error: %s", user_id, e)
    except Exception as e:
        logger.error(f"Failed to sync user commands for {user_id}: {e}")
