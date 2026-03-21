from typing import Callable

from telegram import ReplyKeyboardMarkup

from menu_ui import ADMIN_MENU_LABELS


def _pack_compact_rows(
    items: list[str],
    *,
    max_cols: int = 3,
    max_row_chars: int = 44,
) -> list[list[str]]:
    """Pack short labels into up to 3 columns while keeping long rows readable."""
    rows: list[list[str]] = []
    row: list[str] = []
    for item in items:
        candidate = row + [item]
        est_chars = sum(len(x) for x in candidate) + (len(candidate) - 1) * 2
        if len(candidate) > max_cols or est_chars > max_row_chars:
            if row:
                rows.append(row)
            row = [item]
        else:
            row = candidate
    if row:
        rows.append(row)
    return rows


def build_main_menu_keyboard(
    lang: str,
    section: str,
    user_id: int | None,
    messages: dict,
    is_admin_user_fn: Callable[[int], bool],
    admin_labels: dict[str, str] | None = None,
) -> ReplyKeyboardMarkup:
    m = messages.get(lang, messages["en"])
    labels = admin_labels or ADMIN_MENU_LABELS
    is_admin = bool(user_id and is_admin_user_fn(user_id))

    if section == "other":
        keyboard = [
            [m.get("menu_top_users", "🏆 Top Users")],
            [m.get("menu_audio_converter", "🎛️ Audio Editor"), m.get("menu_pdf_editor", "🧰 PDF Editor")],
            [m.get("menu_sticker_tools", "🧩 Sticker Tools"), m.get("menu_name_meanings", "🪪 Name Meanings")],
            [m.get("menu_contact_admin", "📞 Contact Admin"), m.get("menu_help", "❓ Help")],
            [m.get("menu_back", "⬅️ Back")],
        ]
    elif section == "ai_tools":
        keyboard = [
            [m.get("menu_ai_chat", "💬 Chat with AI"), m.get("menu_ai_translator", "🌐 AI Translator")],
            [m.get("menu_ai_grammar", "✍️ AI Grammar Fix"), m.get("menu_ai_email_writer", "📧 AI Email Writer")],
            [m.get("menu_ai_quiz", "📝 AI Quiz Generator"), m.get("menu_ai_music", "🎵 AI Music Generator")],
            [m.get("menu_ai_song", "🎤 AI Song Generator"), m.get("menu_pdf_maker", "🤖 AI PDF Maker")],
            [m.get("menu_ai_pdf_translator", "🌐📄 AI PDF Translator")],
            [m.get("menu_back", "⬅️ Back")],
        ]
    elif section == "admin":
        admin_items = [
            labels["admin_user_search"],
            labels["admin_upload"],
            labels["admin_audit"],
            labels["admin_prune"],
            labels["admin_broadcast"],
            labels["admin_missing"],
            labels["admin_pause"],
            labels["admin_resume"],
            labels["admin_cancel_task"],
            labels["admin_dupes_status"],
            labels["admin_db_dupes"],
            labels["admin_es_dupes"],
            labels["admin_upload_local_status"],
            labels["admin_upload_local_all"],
            labels["admin_upload_local_missing"],
            labels["admin_upload_local_unique"],
            labels["admin_upload_local_large"],
            labels["admin_missing_confirm"],
        ]
        keyboard = _pack_compact_rows(admin_items)
        keyboard.append([m.get("menu_back", "⬅️ Back")])
    elif section == "admin_maintenance":
        keyboard = [
            [labels["admin_prune"], labels["admin_missing"]],
            [labels["admin_missing_confirm"]],
            [m.get("menu_back", "⬅️ Back")],
        ]
    elif section == "admin_duplicates":
        keyboard = [
            [labels["admin_db_dupes"], labels["admin_es_dupes"]],
            [labels["admin_dupes_status"]],
            [m.get("menu_back", "⬅️ Back")],
        ]
    elif section == "admin_tasks":
        keyboard = [
            [labels["admin_cancel_task"]],
            [m.get("menu_back", "⬅️ Back")],
        ]
    elif section == "admin_uploads":
        keyboard = [
            [labels["admin_upload_local_status"]],
            [labels["admin_upload_local_all"], labels["admin_upload_local_missing"]],
            [labels["admin_upload_local_unique"], labels["admin_upload_local_large"]],
            [m.get("menu_back", "⬅️ Back")],
        ]
    else:
        keyboard = [
            [m.get("menu_search_books", "🔎 Search Books")],
            [m.get("menu_top_books", "🔥 Top Books"), m.get("menu_ai_tools", "🤖 AI Tools")],
            [m.get("menu_video_downloader", "⬇️ Insta Youtub"), m.get("menu_text_to_voice", "🎙️ Text to Voice")],
            [m.get("menu_other_functions", "🛠️ Other Functions")],
        ]
        if is_admin:
            keyboard.insert(1, [labels["admin_panel"]])

    is_main_section = section == "main"
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        is_persistent=False,
        one_time_keyboard=not is_main_section,
    )


def build_main_menu_message_text(
    lang: str,
    section: str,
    first_name: str,
    messages: dict,
    admin_labels: dict[str, str] | None = None,
    admin_guide_text_fn: Callable[[], str] | None = None,
) -> str:
    m = messages.get(lang, messages["en"])
    labels = admin_labels or ADMIN_MENU_LABELS

    if section == "other":
        title = m.get("menu_other_functions", "🛠️ Other Functions")
        subtitle = m.get("start_menu_subtitle", "Choose what you want to do 👇")
        return f"{title}\n{subtitle}"
    if section == "ai_tools":
        title = m.get("menu_ai_tools", "🤖 AI Tools")
        subtitle = m.get("menu_ai_tools_placeholder", m.get("start_menu_subtitle", "Choose what you want to do 👇"))
        return f"{title}\n{subtitle}"
    if section == "admin":
        return admin_guide_text_fn() if admin_guide_text_fn else labels["admin_panel"]
    if section.startswith("admin"):
        title_map = {
            "admin": labels["admin_panel"],
            "admin_maintenance": labels["admin_maintenance"],
            "admin_duplicates": labels["admin_duplicates"],
            "admin_tasks": labels["admin_tasks"],
            "admin_uploads": labels["admin_uploads"],
        }
        return f"{title_map.get(section, labels['admin_panel'])}\n{m.get('start_menu_subtitle', 'Choose what you want to do 👇')}"
    title = m.get("start_menu_title", "Welcome, {name} 👋").format(name=first_name)
    subtitle = m.get("start_menu_subtitle", "Choose what you want to do 👇")
    return f"{title}\n{subtitle}"


def build_main_menu_chat_text(
    lang: str,
    section: str,
    messages: dict,
) -> str:
    m = messages.get(lang, messages["en"])
    return (
        f"{m.get('menu_other_functions', '🛠️ Other Functions')}\n{m.get('start_menu_subtitle', 'Choose what you want to do 👇')}"
        if section == "other"
        else f"{m.get('menu_ai_tools', '🤖 AI Tools')}\n{m.get('menu_ai_tools_placeholder', m.get('start_menu_subtitle', 'Choose what you want to do 👇'))}"
        if section == "ai_tools"
        else m.get("start_menu_subtitle", "Choose what you want to do 👇")
    )
