from typing import Callable


ADMIN_MENU_LABELS = {
    "admin_panel": "🛠 Admin Control",
    "admin_system": "🖥 System",
    "admin_maintenance": "🛠 Maintenance",
    "admin_duplicates": "🧼 Duplicates",
    "admin_tasks": "🧵 Tasks",
    "admin_upload": "⬆ Upload Book",
    "admin_broadcast": "📣 Broadcast",
    "admin_user_search": "👤 User Search",
    "admin_audit": "🧾 Audit",
    "admin_pause": "⏸ Pause Bot",
    "admin_resume": "▶ Resume Bot",
    "admin_prune": "🧹 Prune Users",
    "admin_missing": "⚠ Missing Files",
    "admin_missing_confirm": "🗑 Missing Confirm",
    "admin_db_dupes": "🧼 DB Dupes",
    "admin_es_dupes": "🧼 ES Dupes",
    "admin_dupes_status": "📊 Dupes Status",
    "admin_cancel_task": "🛑 Cancel Task",
}


def admin_control_guide_text(admin_labels: dict[str, str] | None = None) -> str:
    a = admin_labels or ADMIN_MENU_LABELS
    lines = [
        f"{a['admin_panel']}",
        "──────────",
        f"{a['admin_user_search']} - Search by name, username, or user ID (full/partial)",
        f"{a['admin_upload']} - Open normal upload flow (single/manual uploads)",
        f"{a['admin_audit']} - Show audit/system statistics report",
        f"{a['admin_prune']} - Remove blocked users in background",
        f"{a['admin_broadcast']} - Send a message to all users (asks next text)",
        f"{a['admin_missing']} - Preview missing-file DB entries",
        f"{a['admin_missing_confirm']} - Delete missing-file entries (/missing confirm equivalent)",
        f"{a['admin_pause']} - Pause bot for public users",
        f"{a['admin_resume']} - Resume bot and process queued updates",
        f"{a['admin_cancel_task']} - Show/cancel running background tasks",
        f"{a['admin_dupes_status']} - Show duplicate-cleanup status",
        f"{a['admin_db_dupes']} - Build DB duplicates cleanup preview",
        f"{a['admin_es_dupes']} - Build ES duplicates cleanup preview",
        "⬅ Back - Return to the main user menu",
    ]
    return "\n".join(lines)


def build_help_text(
    lang: str,
    messages: dict,
    is_admin_user_fn: Callable[[int], bool],
    user_id: int | None = None,
) -> str:
    m = messages.get(lang, messages["en"])
    border = "──────────"

    if lang == "uz":
        title = "📚 Yordam"
        intro = "👇 Botdan menyu orqali foydalaning."
        main_title = "🏠 Asosiy menyu"
        other_title = "🛠️ Boshqa funksiyalar"
        notes_title = "ℹ️ Eslatma"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Kitob qidirish')} — kitob nomini yuborib qidiring.",
            f"{m.get('menu_favorites', '⭐ Sevimlilar')} — saqlangan kitoblaringiz.",
            f"{m.get('menu_myprofile', '👤 Mening profilim')} — statistika va tangalarni ko‘ring.",
        ]
        other_lines = [
            f"{m.get('menu_text_to_voice', '🎙️ Matndan ovoz')} — matndan audio yarating.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — matndan PDF tayyorlaydi.",
            f"{m.get('menu_pdf_editor', '🧰 PDF muharriri')} — PDF siqish, OCR, TXT/EPUB va suv belgisi.",
            f"{m.get('menu_audio_converter', '🎛️ Audio muharriri')} — audio kesish va formatlash.",
            f"{m.get('menu_sticker_tools', '🧩 Sticker vositalar')} — rasm/video dan sticker tayyorlash.",
            f"{m.get('menu_top_users', '🏆 Top foydalanuvchilar')} — eng faol foydalanuvchilar.",
            f"{m.get('menu_contact_admin', '📞 Admin bilan aloqa')} — bog‘lanish ma’lumoti.",
        ]
        note_lines = [
            "🌐 Tilni o‘zgartirish uchun Language bo‘limidan foydalaning.",
            "💡 Kitob topish uchun nomini oddiy xabar qilib yuborish kifoya.",
        ]
        admin_line = "🛠 Admin Control menyusi faqat adminlarga ko‘rinadi."
    elif lang == "ru":
        title = "📚 Помощь"
        intro = "👇 Пользуйтесь ботом через меню."
        main_title = "🏠 Главное меню"
        other_title = "🛠️ Другие функции"
        notes_title = "ℹ️ Примечание"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Поиск книг')} — отправьте название книги для поиска.",
            f"{m.get('menu_favorites', '⭐ Избранное')} — сохранённые книги.",
            f"{m.get('menu_myprofile', '👤 Мой профиль')} — статистика и монеты.",
        ]
        other_lines = [
            f"{m.get('menu_text_to_voice', '🎙️ Текст в голос')} — преобразование текста в аудио.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — создание PDF из текста.",
            f"{m.get('menu_pdf_editor', '🧰 PDF редактор')} — сжатие PDF, OCR, TXT/EPUB и водяной знак.",
            f"{m.get('menu_audio_converter', '🎛️ Аудиоредактор')} — обрезка и форматирование аудио.",
            f"{m.get('menu_sticker_tools', '🧩 Стикер инструменты')} — создание стикеров.",
            f"{m.get('menu_top_users', '🏆 Топ пользователей')} — самые активные пользователи.",
            f"{m.get('menu_contact_admin', '📞 Связаться с админом')} — контакты и группа.",
        ]
        note_lines = [
            "🌐 Язык можно изменить через раздел Language.",
            "💡 Для поиска книги достаточно отправить её название обычным сообщением.",
        ]
        admin_line = "🛠 Admin Control отображается только для админов."
    else:
        title = "📚 Help"
        intro = "👇 Use the bot through the menu."
        main_title = "🏠 Main Menu"
        other_title = "🛠️ Other Functions"
        notes_title = "ℹ️ Notes"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Search Books')} — send a book name to search.",
            f"{m.get('menu_favorites', '⭐ Favorites')} — your saved books.",
            f"{m.get('menu_myprofile', '👤 My Profile')} — stats and coins.",
        ]
        other_lines = [
            f"{m.get('menu_text_to_voice', '🎙️ Text to Voice')} — convert text into audio.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — create a PDF from text.",
            f"{m.get('menu_pdf_editor', '🧰 PDF Editor')} — compress PDF, OCR, TXT/EPUB, and watermark.",
            f"{m.get('menu_audio_converter', '🎛️ Audio Editor')} — trim and convert audio.",
            f"{m.get('menu_sticker_tools', '🧩 Sticker Tools')} — make stickers from media.",
            f"{m.get('menu_top_users', '🏆 Top Users')} — most active users.",
            f"{m.get('menu_contact_admin', '📞 Contact Admin')} — contact details and group.",
        ]
        note_lines = [
            "🌐 Use the Language section to change your language.",
            "💡 To find a book, just send its title as a normal message.",
        ]
        admin_line = "🛠 Admin Control is visible only to admins."

    blocks = [f"{title}\n{intro}"]

    blocks.extend([
            f"{main_title}\n" + "\n".join(main_lines),
            f"{other_title}\n" + "\n".join(other_lines),
        ])
    
    # Add admin section for admin users
    if user_id and is_admin_user_fn(user_id):
        admin_title = "🛠 Admin Commands" if lang == "en" else "🛠 Admin Buyruqlari" if lang == "uz" else "🛠 Админ команды"
        admin_commands = [
            "/admin — Admin panel",
            "/upload — Upload books",
            "/broadcast — Send broadcast",
            "/smoke — System health check",
        ]
        blocks.append(f"{admin_title}\n" + "\n".join(admin_commands))
        note_lines = [*note_lines, admin_line]
    
    blocks.append(f"{notes_title}\n" + "\n".join(note_lines))
    return f"\n{border}\n".join(blocks)


def get_item_description(key: str, lang: str) -> str:
    """Get description for menu item based on language."""
    descriptions = {
        "uz": {
            "menu_search_books": "kitob nomini yuborib qidiring.",
            "menu_text_to_voice": "matndan audio yarating.",
            "menu_pdf_maker": "matndan PDF tayyorlaydi.",
            "menu_pdf_editor": "PDF siqish, OCR, TXT/EPUB va suv belgisi.",
            "menu_top_books": "eng mashhur kitoblar.",
            "menu_favorites": "saqlangan kitoblar.",
            "menu_audio_converter": "voice/mp3 formatni o'zgartirish, kesish va nomini o'zgartirish.",
            "menu_sticker_tools": "sticker yasash va video sticker tayyorlash.",
            "menu_myprofile": "statistika va sovg'alar.",
            "menu_help": "ushbu yo'riqnoma.",
        },
        "ru": {
            "menu_search_books": "отправьте название книги для поиска.",
            "menu_text_to_voice": "преобразование текста в аудио.",
            "menu_pdf_maker": "создание PDF из текста.",
            "menu_pdf_editor": "сжатие PDF, OCR, TXT/EPUB и водяной знак.",
            "menu_top_books": "самые популярные книги.",
            "menu_favorites": "сохранённые книги.",
            "menu_audio_converter": "конвертация voice/mp3, обрезка и переименование.",
            "menu_sticker_tools": "создание стикеров и видео-стикеров.",
            "menu_myprofile": "статистика и бонусы.",
            "menu_help": "эта инструкция.",
        },
        "en": {
            "menu_search_books": "send a book name to search.",
            "menu_text_to_voice": "convert text into audio.",
            "menu_pdf_maker": "create a PDF from text.",
            "menu_pdf_editor": "compress PDF, OCR, TXT/EPUB, and watermark.",
            "menu_top_books": "most popular books.",
            "menu_favorites": "saved books.",
            "menu_audio_converter": "convert voice/mp3, cut audio, and rename files.",
            "menu_sticker_tools": "create static/video stickers from media.",
            "menu_myprofile": "stats and rewards.",
            "menu_help": "this guide.",
        }
    }
    
    return descriptions.get(lang, {}).get(key, "feature description")


def main_menu_text_action(
    text: str,
    messages: dict,
    admin_labels: dict[str, str] | None = None,
) -> str | None:
    if not text:
        return None
    labels = admin_labels or ADMIN_MENU_LABELS
    key_to_action = {
        "menu_search_books": "search",
        "menu_text_to_voice": "tts",
        "menu_pdf_maker": "pdf",
        "menu_pdf_editor": "pdf_editor",
        "menu_favorites": "favorites",
        "menu_request_book": "request",
        "menu_other_functions": "other",
        "menu_myprofile": "myprofile",
        "menu_top_books": "top_books",
        "menu_top_users": "top_users",
        "menu_upload": "upload",
        "menu_audio_converter": "audio_converter",
        "menu_sticker_tools": "sticker_tools",
        "menu_contact_admin": "contact_admin",
        "menu_help": "help",
        "menu_back": "back",
    }
    for lang_key in ("uz", "ru", "en"):
        m = messages.get(lang_key, {})
        for msg_key, action in key_to_action.items():
            if text == m.get(msg_key):
                return action

    admin_action_map = {
        "admin_panel": "admin_panel",
        "admin_system": "admin_system",
        "admin_maintenance": "admin_maintenance",
        "admin_duplicates": "admin_duplicates",
        "admin_tasks": "admin_tasks",
        "admin_upload": "admin_upload",
        "admin_broadcast": "admin_broadcast",
        "admin_user_search": "admin_user_search",
        "admin_audit": "admin_audit",
        "admin_pause": "admin_pause",
        "admin_resume": "admin_resume",
        "admin_prune": "admin_prune",
        "admin_missing": "admin_missing",
        "admin_missing_confirm": "admin_missing_confirm",
        "admin_db_dupes": "admin_db_dupes",
        "admin_es_dupes": "admin_es_dupes",
        "admin_dupes_status": "admin_dupes_status",
        "admin_cancel_task": "admin_cancel_task",
    }
    for label_key, action in admin_action_map.items():
        if text == labels[label_key]:
            return action
    return None
