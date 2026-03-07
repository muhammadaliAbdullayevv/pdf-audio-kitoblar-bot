from __future__ import annotations

from telegram import ReplyKeyboardMarkup


def ai_active_mode_ui_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "active_chat": "🟢 AI Chat faol",
            "active_translator": "🟢 AI tarjimon faol",
            "active_grammar": "🟢 AI grammatika faol",
            "active_email": "🟢 AI xat yozish faol",
            "active_quiz": "🟢 AI quiz faol",
            "active_music": "🟢 AI musiqa faol",
            "active_song": "🟢 AI qo‘shiq faol",
            "change_tool": "🔁 AI toolni almashtirish",
            "exit_tool": "❌ AI tooldan chiqish",
        }
    if lang == "ru":
        return {
            "active_chat": "🟢 AI Chat активен",
            "active_translator": "🟢 AI переводчик активен",
            "active_grammar": "🟢 AI грамматика активна",
            "active_email": "🟢 AI письма активны",
            "active_quiz": "🟢 AI викторина активна",
            "active_music": "🟢 AI музыка активна",
            "active_song": "🟢 AI песня активна",
            "change_tool": "🔁 Сменить AI инструмент",
            "exit_tool": "❌ Выйти из AI инструмента",
        }
    return {
        "active_chat": "🟢 AI Chat Active",
        "active_translator": "🟢 AI Translator Active",
        "active_grammar": "🟢 AI Grammar Fix Active",
        "active_email": "🟢 AI Email Writer Active",
        "active_quiz": "🟢 AI Quiz Generator Active",
        "active_music": "🟢 AI Music Generator Active",
        "active_song": "🟢 AI Song Generator Active",
        "change_tool": "🔁 Change AI Tool",
        "exit_tool": "❌ Exit AI Tool",
    }


def ai_active_mode_keyboard(lang: str, mode: str) -> ReplyKeyboardMarkup:
    ui = ai_active_mode_ui_texts(lang)
    label_map = {
        "chat": ui["active_chat"],
        "translator": ui["active_translator"],
        "grammar": ui["active_grammar"],
        "email": ui["active_email"],
        "quiz": ui["active_quiz"],
        "music": ui["active_music"],
        "song": ui["active_song"],
    }
    active_label = label_map.get(mode, ui["active_chat"])
    rows = [[active_label], [ui["change_tool"], ui["exit_tool"]]]
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=False,
        one_time_keyboard=False,
    )


def ai_active_mode_button_action(text: str, lang: str) -> str | None:
    if not text:
        return None
    ui = ai_active_mode_ui_texts(lang)
    if text == ui["change_tool"]:
        return "change"
    if text == ui["exit_tool"]:
        return "exit"
    if text in {
        ui["active_chat"],
        ui["active_translator"],
        ui["active_grammar"],
        ui["active_email"],
        ui["active_quiz"],
        ui["active_music"],
        ui["active_song"],
    }:
        return "noop"
    return None
