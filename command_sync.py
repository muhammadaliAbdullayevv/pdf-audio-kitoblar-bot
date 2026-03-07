from __future__ import annotations

import logging
from typing import Iterable

from telegram import BotCommand
from telegram import (
    BotCommandScopeAllChatAdministrators,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeDefault,
)

_MENU_BACKED_PUBLIC_COMMANDS = {
    "help",
    "pdf_maker",
    "text_to_voice",
    "ramazon",
    "myprofile",
    "top",
    "top_users",
    "favorite",
    "request",
    "upload",
    "movie_upload",
}

_PUBLIC_PREFERRED_ORDER = ("start", "language", "requests")


def get_public_commands(lang: str = "en") -> list[BotCommand]:
    localized = {
        "en": [
            BotCommand("start", "🚀 Start / choose language"),
            BotCommand("help", "❓ How to use the bot"),
            BotCommand("pdf_maker", "📄 Make PDF from text"),
            BotCommand("text_to_voice", "🎙️ Convert text to voice"),
            BotCommand("ramazon", "🌙 Saharlik & iftorlik duas"),
            BotCommand("myprofile", "👤 My profile"),
            BotCommand("top", "🔥 Top books"),
            BotCommand("top_users", "🏆 Top users"),
            BotCommand("favorite", "⭐ Your favorites"),
            BotCommand("request", "📝 Request a book"),
            BotCommand("requests", "📋 Your requests"),
            BotCommand("my_quiz", "📝 My saved quiz tests"),
            BotCommand("upload", "⬆️ Upload books to the bot"),
            BotCommand("movie_upload", "⬆️ Upload movies to the bot"),
            BotCommand("language", "🌐 Change language"),
        ],
        "ru": [
            BotCommand("start", "🚀 Запуск / выбор языка"),
            BotCommand("help", "❓ Как пользоваться ботом"),
            BotCommand("pdf_maker", "📄 PDF из текста"),
            BotCommand("text_to_voice", "🎙️ Текст в голос"),
            BotCommand("ramazon", "🌙 Сахарлик и ифторлик дуалары"),
            BotCommand("myprofile", "👤 Профиль"),
            BotCommand("top", "🔥 Топ книги"),
            BotCommand("top_users", "🏆 Топ пользователей"),
            BotCommand("favorite", "⭐ Избранные книги"),
            BotCommand("request", "📝 Запросить книгу"),
            BotCommand("requests", "📋 Мои запросы"),
            BotCommand("my_quiz", "📝 Мои quiz-тесты"),
            BotCommand("upload", "⬆️ Загружать книги в бота"),
            BotCommand("movie_upload", "⬆️ Загружать фильмы в бота"),
            BotCommand("language", "🌐 Сменить язык"),
        ],
        "uz": [
            BotCommand("start", "🚀 Botni ishga tushirish / til tanlash"),
            BotCommand("help", "❓ Botdan foydalanish"),
            BotCommand("pdf_maker", "📄 Matndan PDF yaratish"),
            BotCommand("text_to_voice", "🎙️ Matndan ovoz yaratish"),
            BotCommand("ramazon", "🌙 Saharlik va iftorlik duolari"),
            BotCommand("myprofile", "👤 Profilim"),
            BotCommand("top", "🔥 Top kitoblar"),
            BotCommand("top_users", "🏆 Top foydalanuvchilar"),
            BotCommand("favorite", "⭐ Sevimli kitoblar"),
            BotCommand("request", "📝 Kitob so‘rash"),
            BotCommand("requests", "📋 So‘rovlarim"),
            BotCommand("my_quiz", "📝 Mening quiz testlarim"),
            BotCommand("upload", "⬆️ Botga kitob yuklash"),
            BotCommand("movie_upload", "⬆️ Botga kino yuklash"),
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
    commands = [cmd for cmd in get_public_commands(lang) if cmd.command not in _MENU_BACKED_PUBLIC_COMMANDS]
    return _order_commands(commands, _PUBLIC_PREFERRED_ORDER)


def get_group_commands(lang: str = "en") -> list[BotCommand]:
    by_name = {cmd.command: cmd for cmd in get_public_commands(lang)}
    ordered: list[BotCommand] = []
    for name in ("start", "language"):
        cmd = by_name.get(name)
        if cmd:
            ordered.append(cmd)
    return ordered


def get_group_admin_commands(lang: str = "en") -> list[BotCommand]:
    localized = {
        "en": [
            BotCommand("group_read_start", "📘 Start group reading"),
            BotCommand("group_read_status", "📊 Group reading status"),
            BotCommand("group_read_end", "✅ End group reading"),
        ],
        "ru": [
            BotCommand("group_read_start", "📘 Начать групповое чтение"),
            BotCommand("group_read_status", "📊 Статус группового чтения"),
            BotCommand("group_read_end", "✅ Завершить групповое чтение"),
        ],
        "uz": [
            BotCommand("group_read_start", "📘 Guruhda o‘qishni boshlash"),
            BotCommand("group_read_status", "📊 Guruh o‘qish holati"),
            BotCommand("group_read_end", "✅ Guruh o‘qishni yakunlash"),
        ],
    }
    return localized.get(lang, localized["en"])


def _owner_minimal_commands(lang: str = "en") -> list[BotCommand]:
    by_name = {cmd.command: cmd for cmd in get_public_commands(lang)}
    ordered: list[BotCommand] = []
    for name in ("start", "language", "requests"):
        cmd = by_name.get(name)
        if cmd:
            ordered.append(cmd)
    return ordered


def get_admin_commands(lang: str = "en", *, owner_user: bool = False) -> list[BotCommand]:
    if owner_user:
        return _owner_minimal_commands(lang)
    return get_public_commands_for_menu(lang) + [
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
    admin_id: int | None,
    logger: logging.Logger,
) -> None:
    try:
        await application.bot.delete_my_commands(scope=BotCommandScopeAllPrivateChats())
        await application.bot.delete_my_commands(scope=BotCommandScopeAllGroupChats())
        await application.bot.delete_my_commands(scope=BotCommandScopeAllChatAdministrators())

        await application.bot.set_my_commands(
            get_public_commands_for_menu("en"),
            scope=BotCommandScopeDefault(),
        )
        for lang in ("en", "ru", "uz"):
            await application.bot.set_my_commands(
                get_public_commands_for_menu(lang),
                scope=BotCommandScopeDefault(),
                language_code=lang,
            )
            await application.bot.set_my_commands(
                get_group_commands(lang),
                scope=BotCommandScopeAllGroupChats(),
                language_code=lang,
            )
            await application.bot.set_my_commands(
                get_group_admin_commands(lang),
                scope=BotCommandScopeAllChatAdministrators(),
                language_code=lang,
            )

        scoped_admin_id = owner_id or admin_id
        if scoped_admin_id:
            for lang in ("en", "ru", "uz"):
                await application.bot.set_my_commands(
                    get_admin_commands(lang, owner_user=(owner_id is not None and scoped_admin_id == owner_id)),
                    scope=BotCommandScopeChat(chat_id=scoped_admin_id),
                    language_code=lang,
                )
        logger.debug("Bot commands set successfully")
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")


async def sync_user_commands_if_needed(
    context,
    *,
    user_id: int | None,
    lang: str,
    owner_id: int | None,
    admin_id: int | None,
    logger: logging.Logger,
) -> None:
    if not context or not user_id:
        return
    app = getattr(context, "application", None)
    if not app or not getattr(app, "running", False):
        return
    try:
        cache = app.bot_data.setdefault("user_commands_lang_cache", {})
        cached_lang = cache.get(user_id)
        if cached_lang == lang:
            return
        is_admin_user = user_id in {x for x in (admin_id, owner_id) if x}
        owner_user = bool(owner_id and user_id == owner_id)
        commands = get_admin_commands(lang, owner_user=owner_user) if is_admin_user else get_public_commands_for_menu(lang)
        await context.bot.set_my_commands(
            commands,
            scope=BotCommandScopeChat(chat_id=user_id),
            language_code=lang,
        )
        cache[user_id] = lang
    except Exception as e:
        logger.error(f"Failed to sync user commands for {user_id}: {e}")
