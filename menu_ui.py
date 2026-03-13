from typing import Callable


ADMIN_MENU_LABELS = {
    "admin_panel": "🛠 Admin Control",
    "admin_system": "🖥 System",
    "admin_maintenance": "🛠 Maintenance",
    "admin_duplicates": "🧼 Duplicates",
    "admin_tasks": "🧵 Tasks",
    "admin_upload": "⬆ Upload Book",
    "admin_uploads": "⬆️ Local Uploads",
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
    "admin_upload_local_all": "⬆ All",
    "admin_upload_local_missing": "🩹 Missing",
    "admin_upload_local_unique": "🆔 Unique",
    "admin_upload_local_large": "📦 Large",
    "admin_upload_local_status": "📊 Upload Status",
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
        f"{a['admin_upload_local_status']} - Show local bulk-upload progress/status",
        f"{a['admin_upload_local_all']} - Start local bulk upload (all eligible books)",
        f"{a['admin_upload_local_missing']} - Start local bulk upload for missing cached files",
        f"{a['admin_upload_local_unique']} - Upload only books missing file_unique_id",
        f"{a['admin_upload_local_large']} - Bulk process large-file mode",
        "⬅ Back - Return to the main user menu",
    ]
    return "\n".join(lines)


def build_help_text(
    lang: str,
    messages: dict,
    is_admin_user_fn: Callable[[int], bool],
    user_id: int | None = None,
) -> str:
    # Import custom menu system
    try:
        from custom_menu import get_effective_menu
        custom_menu = get_effective_menu(user_id, lang) if user_id else None
    except ImportError:
        custom_menu = None
    
    m = messages.get(lang, messages["en"])
    border = "──────────"

    if lang == "uz":
        title = "📚 Yordam"
        intro = "👇 Botdan pastdagi menyu orqali foydalaning."
        main_title = "🏠 Asosiy menyu"
        ai_title = "🤖 AI vositalar"
        other_title = "🛠️ Boshqa funksiyalar"
        notes_title = "ℹ️ Eslatma"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Kitob qidirish')} — kitob nomini yuborib qidiring.",
            f"{m.get('menu_search_movies', '🎬 Kino qidirish')} — kino nomini yuborib qidiring.",
            f"{m.get('menu_ai_tools', '🤖 AI vositalar')} — AI yordamchi bo‘limlari.",
            f"{m.get('menu_text_to_voice', '🎙️ Matndan ovoz')} — matndan audio yarating.",
            f"{m.get('menu_ramadan_duas', '🌙 Ramazon duolari')} — Ramazon duolari bo‘limi.",
        ]
        ai_lines = [
            f"{m.get('menu_ai_chat', '💬 AI bilan chat')} — savollarga javob.",
            f"{m.get('menu_ai_translator', '🌐 AI tarjimon')} — tarjima (aniq format: `uz>en: matn`).",
            f"{m.get('menu_ai_pdf_translator', '🌐📄 AI PDF tarjimon')} — PDF/EPUB/DOCX/DOC/TXT/MD fayllarni tarjima qiladi.",
            f"{m.get('menu_ai_grammar', '✍️ AI grammatika tuzatish')} — matn xatolarini tuzatadi.",
            f"{m.get('menu_ai_email_writer', '📧 AI xat yozish')} — xat/letter draft tayyorlaydi.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — matndan PDF tayyorlaydi.",
            f"{m.get('menu_ai_quiz', '📝 AI quiz generator')} — mavzu bo‘yicha Telegram quiz savollar yaratadi.",
            f"{m.get('menu_ai_music', '🎵 AI music generator')} — prompt bo‘yicha instrumental musiqa yaratadi (lokal).",
            f"{m.get('menu_ai_song', '🎤 AI qo‘shiq generator')} — mavzu yoki kalit so‘zlardan qo‘shiq matni yozadi.",
        ]
        other_lines = [
            f"{m.get('menu_top_books', '🔥 Top kitoblar')} — eng mashhur kitoblar.",
            "⌨️ /upload va /movie_upload — yuklash buyruqlari (admin).",
            f"{m.get('menu_pdf_editor', '🧰 PDF muharriri')} — PDF siqish, OCR, TXT/EPUB va suv belgisi.",
            f"{m.get('menu_audio_converter', '🎛️ Audio Editor')} — voice/mp3 format o‘zgartirish, kesish va nomini o‘zgartirish.",
            f"{m.get('menu_sticker_tools', '🧩 Sticker Tools')} — rasm/video dan sticker tayyorlash.",
            f"{m.get('menu_name_meanings', '🪪 Ismlar ma’nosi')} — hozircha tez orada qo‘shiladi.",
            f"{m.get('menu_help', '❓ Yordam')} — ushbu yo‘riqnoma.",
            "⌨️ /myprofile, /favorite, /request — buyruqlar menyusida.",
        ]
        note_lines = [
            "🌐 Tilni o‘zgartirish uchun Language bo‘limidan foydalaning.",
            "⌨️ Buyruqlar fallback sifatida ishlaydi, lekin asosiy foydalanish menyu orqali.",
        ]
        admin_line = "🛠 Admin Control menyusi faqat adminlarga ko‘rinadi."
    elif lang == "ru":
        title = "📚 Помощь"
        intro = "👇 Пользуйтесь ботом через меню ниже."
        main_title = "🏠 Главное меню"
        ai_title = "🤖 AI инструменты"
        other_title = "🛠️ Другие функции"
        notes_title = "ℹ️ Примечание"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Поиск книг')} — отправьте название книги для поиска.",
            f"{m.get('menu_search_movies', '🎬 Поиск фильмов')} — отправьте название фильма для поиска.",
            f"{m.get('menu_ai_tools', '🤖 AI инструменты')} — раздел AI-помощников.",
            f"{m.get('menu_text_to_voice', '🎙️ Текст в голос')} — преобразование текста в аудио.",
            f"{m.get('menu_ramadan_duas', '🌙 Дуa Рамадана')} — раздел с дуа.",
        ]
        ai_lines = [
            f"{m.get('menu_ai_chat', '💬 Чат с AI')} — ответы на вопросы.",
            f"{m.get('menu_ai_translator', '🌐 AI переводчик')} — перевод (точный формат: `uz>en: текст`).",
            f"{m.get('menu_ai_pdf_translator', '🌐📄 AI PDF переводчик')} — переводит файлы PDF/EPUB/DOCX/DOC/TXT/MD.",
            f"{m.get('menu_ai_grammar', '✍️ AI исправление грамматики')} — исправляет ошибки в тексте.",
            f"{m.get('menu_ai_email_writer', '📧 AI письма')} — черновик письма/сообщения.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — создание PDF из текста.",
            f"{m.get('menu_ai_quiz', '📝 AI генератор викторины')} — создаёт Telegram quiz-вопросы по теме.",
            f"{m.get('menu_ai_music', '🎵 AI генератор музыки')} — создаёт инструментальную музыку по prompt (локально).",
            f"{m.get('menu_ai_song', '🎤 AI генератор песни')} — пишет текст песни по теме или ключевым словам.",
        ]
        other_lines = [
            f"{m.get('menu_top_books', '🔥 Топ книг')} — самые популярные книги.",
            "⌨️ /upload и /movie_upload — команды загрузки (админ).",
            f"{m.get('menu_pdf_editor', '🧰 PDF редактор')} — сжатие PDF, OCR, TXT/EPUB и водяной знак.",
            f"{m.get('menu_audio_converter', '🎛️ Audio Editor')} — конвертация voice/mp3, обрезка и переименование.",
            f"{m.get('menu_sticker_tools', '🧩 Sticker Tools')} — создание стикеров из фото/видео.",
            f"{m.get('menu_name_meanings', '🪪 Значение имени')} — скоро будет доступно.",
            f"{m.get('menu_help', '❓ Помощь')} — эта инструкция.",
            "⌨️ /myprofile, /favorite, /request — в меню команд.",
        ]
        note_lines = [
            "🌐 Язык можно изменить через раздел Language.",
            "⌨️ Команды остаются как резервный вариант, но основной способ — меню.",
        ]
        admin_line = "🛠 Admin Control отображается только для админов."
    else:
        title = "📚 Help"
        intro = "👇 Use the bot through the menu below."
        main_title = "🏠 Main Menu"
        ai_title = "🤖 AI Tools"
        other_title = "🛠️ Other Functions"
        notes_title = "ℹ️ Notes"
        main_lines = [
            f"{m.get('menu_search_books', '🔎 Search Books')} — send a book name to search.",
            f"{m.get('menu_search_movies', '🎬 Search Movies')} — send a movie name to search.",
            f"{m.get('menu_ai_tools', '🤖 AI Tools')} — AI assistant features.",
            f"{m.get('menu_text_to_voice', '🎙️ Text to Voice')} — convert text into audio.",
            f"{m.get('menu_ramadan_duas', '🌙 Ramadan Duas')} — Ramadan duas section.",
        ]
        ai_lines = [
            f"{m.get('menu_ai_chat', '💬 Chat with AI')} — ask questions and chat.",
            f"{m.get('menu_ai_translator', '🌐 AI Translator')} — translate text (best format: `uz>en: text`).",
            f"{m.get('menu_ai_pdf_translator', '🌐📄 AI PDF Translator')} — translate PDF/EPUB/DOCX/DOC/TXT/MD files.",
            f"{m.get('menu_ai_grammar', '✍️ AI Grammar Fix')} — fix grammar and spelling.",
            f"{m.get('menu_ai_email_writer', '📧 AI Email Writer')} — draft emails/letters.",
            f"{m.get('menu_pdf_maker', '🤖 AI PDF Maker')} — create a PDF from text.",
            f"{m.get('menu_ai_quiz', '📝 AI Quiz Generator')} — generate Telegram quiz questions from a topic.",
            f"{m.get('menu_ai_music', '🎵 AI Music Generator')} — generate instrumental music from a prompt (local).",
            f"{m.get('menu_ai_song', '🎤 AI Song Generator')} — write song lyrics from a theme or keywords.",
        ]
        other_lines = [
            f"{m.get('menu_top_books', '🔥 Top Books')} — most popular books.",
            "⌨️ /upload and /movie_upload — upload commands (admin).",
            f"{m.get('menu_pdf_editor', '🧰 PDF Editor')} — compress PDF, OCR, TXT/EPUB, and watermark.",
            f"{m.get('menu_audio_converter', '🎛️ Audio Editor')} — convert voice/mp3, cut audio, and rename files.",
            f"{m.get('menu_sticker_tools', '🧩 Sticker Tools')} — make stickers from photo/video.",
            f"{m.get('menu_name_meanings', '🪪 Name Meanings')} — coming soon.",
            f"{m.get('menu_help', '❓ Help')} — this guide.",
            "⌨️ /myprofile, /favorite, /request — available in command menu.",
        ]
        note_lines = [
            "🌐 Use the Language section to change your language.",
            "⌨️ Slash commands still work as a fallback, but the main UX is the menu.",
        ]
        admin_line = "🛠 Admin Control is visible only to admins."

    blocks = [f"{title}\n{intro}"]
    
    # Use default structure
    blocks.extend([
            f"{main_title}\n" + "\n".join(main_lines),
            f"{ai_title}\n" + "\n".join(ai_lines),
            f"{other_title}\n" + "\n".join(other_lines),
        ])
    
    # Add admin section for admin users
    if user_id and is_admin_user_fn(user_id):
        admin_title = "🛠 Admin Commands" if lang == "en" else "🛠 Admin Buyruqlari" if lang == "uz" else "🛠 Админ команды"
        admin_commands = [
            "/admin — Admin panel",
            "/upload — Upload books", 
            "/movie_upload — Upload movies",
            "/broadcast — Send broadcast",
            "/requests — Manage requests",
            "/smoke — System health check",
            "/admin_menu — Menu customization",
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
            "menu_search_movies": "kino nomini yuborib qidiring.",
            "menu_ai_tools": "AI yordamchi bo'limlari.",
            "menu_text_to_voice": "matndan audio yarating.",
            "menu_ramadan_duas": "Ramazon duolari bo'limi.",
            "menu_ai_chat": "savollarga javob.",
            "menu_ai_translator": "tarjima (aniq format: `uz>en: matn`).",
            "menu_ai_pdf_translator": "PDF/EPUB/DOCX/DOC/TXT/MD fayllarni tarjima qiladi.",
            "menu_ai_grammar": "matn xatolarini tuzatadi.",
            "menu_ai_email_writer": "xat/letter draft tayyorlaydi.",
            "menu_pdf_maker": "matndan PDF tayyorlaydi.",
            "menu_pdf_editor": "PDF siqish, OCR, TXT/EPUB va suv belgisi.",
            "menu_ai_quiz": "mavzu bo'yicha Telegram quiz savollar yaratadi.",
            "menu_ai_music": "prompt bo'yicha instrumental musiqa yaratadi (lokal).",
            "menu_ai_song": "mavzu yoki kalit so'zlardan qo'shiq matni yozadi.",
            "menu_top_books": "eng mashhur kitoblar.",
            "menu_movie_upload": "kinolarni saqlash uchun yuborish.",
            "menu_favorites": "saqlangan kitoblar.",
            "menu_audio_converter": "voice/mp3 formatni o'zgartirish, kesish va nomini o'zgartirish.",
            "menu_sticker_tools": "sticker yasash va video sticker tayyorlash.",
            "menu_name_meanings": "ism ma'nolari bo'limi tez orada qo'shiladi.",
            "menu_myprofile": "statistika va sovg'alar.",
            "menu_help": "ushbu yo'riqnoma.",
        },
        "ru": {
            "menu_search_books": "отправьте название книги для поиска.",
            "menu_search_movies": "отправьте название фильма для поиска.",
            "menu_ai_tools": "раздел AI-помощников.",
            "menu_text_to_voice": "преобразование текста в аудио.",
            "menu_ramadan_duas": "раздел с дуа.",
            "menu_ai_chat": "ответы на вопросы.",
            "menu_ai_translator": "перевод (точный формат: `uz>en: текст`).",
            "menu_ai_pdf_translator": "переводит файлы PDF/EPUB/DOCX/DOC/TXT/MD.",
            "menu_ai_grammar": "исправляет ошибки в тексте.",
            "menu_ai_email_writer": "черновик письма/сообщения.",
            "menu_pdf_maker": "создание PDF из текста.",
            "menu_pdf_editor": "сжатие PDF, OCR, TXT/EPUB и водяной знак.",
            "menu_ai_quiz": "создаёт Telegram quiz-вопросы по теме.",
            "menu_ai_music": "создаёт инструментальную музыку по prompt (локально).",
            "menu_ai_song": "пишет текст песни по теме или ключевым словам.",
            "menu_top_books": "самые популярные книги.",
            "menu_movie_upload": "отправка фильмов для сохранения.",
            "menu_favorites": "сохранённые книги.",
            "menu_audio_converter": "конвертация voice/mp3, обрезка и переименование.",
            "menu_sticker_tools": "создание стикеров и видео-стикеров.",
            "menu_name_meanings": "раздел значений имён будет добавлен скоро.",
            "menu_myprofile": "статистика и бонусы.",
            "menu_help": "эта инструкция.",
        },
        "en": {
            "menu_search_books": "send a book name to search.",
            "menu_search_movies": "send a movie name to search.",
            "menu_ai_tools": "AI assistant features.",
            "menu_text_to_voice": "convert text into audio.",
            "menu_ramadan_duas": "Ramadan duas section.",
            "menu_ai_chat": "ask questions and chat.",
            "menu_ai_translator": "translate text (best format: `uz>en: text`).",
            "menu_ai_pdf_translator": "translate PDF/EPUB/DOCX/DOC/TXT/MD files.",
            "menu_ai_grammar": "fix grammar and spelling.",
            "menu_ai_email_writer": "draft emails/letters.",
            "menu_pdf_maker": "create a PDF from text.",
            "menu_pdf_editor": "compress PDF, OCR, TXT/EPUB, and watermark.",
            "menu_ai_quiz": "generate Telegram quiz questions from a topic.",
            "menu_ai_music": "generate instrumental music from a prompt (local).",
            "menu_ai_song": "write song lyrics from a theme or keywords.",
            "menu_top_books": "most popular books.",
            "menu_movie_upload": "send movies for storage.",
            "menu_favorites": "saved books.",
            "menu_audio_converter": "convert voice/mp3, cut audio, and rename files.",
            "menu_sticker_tools": "create static/video stickers from media.",
            "menu_name_meanings": "name meanings section will be added soon.",
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
        "menu_search_movies": "search_movies",
        "menu_text_to_voice": "tts",
        "menu_pdf_maker": "pdf",
        "menu_pdf_editor": "pdf_editor",
        "menu_request_book": "request",
        "menu_favorites": "favorites",
        "menu_other_functions": "other",
        "menu_myprofile": "myprofile",
        "menu_top_books": "top_books",
        "menu_top_users": "top_users",
        "menu_upload": "upload",
        "menu_movie_upload": "movie_upload",
        "menu_audio_converter": "audio_converter",
        "menu_sticker_tools": "sticker_tools",
        "menu_name_meanings": "name_meanings",
        "menu_video_downloader": "video_downloader",
        "menu_contact_admin": "contact_admin",
        "menu_ramadan_duas": "ramazon",
        "menu_help": "help",
        "menu_ai_tools": "ai_tools",
        "menu_ai_chat": "ai_chat",
        "menu_ai_translator": "ai_translator",
        "menu_ai_pdf_translator": "ai_pdf_translator",
        "menu_ai_grammar": "ai_grammar",
        "menu_ai_email_writer": "ai_email_writer",
        "menu_ai_quiz": "ai_quiz",
        "menu_ai_music": "ai_music",
        "menu_ai_song": "ai_song",
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
        "admin_uploads": "admin_uploads",
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
        "admin_upload_local_all": "admin_upload_local_all",
        "admin_upload_local_missing": "admin_upload_local_missing",
        "admin_upload_local_unique": "admin_upload_local_unique",
        "admin_upload_local_large": "admin_upload_local_large",
        "admin_upload_local_status": "admin_upload_local_status",
    }
    for label_key, action in admin_action_map.items():
        if text == labels[label_key]:
            return action
    return None
