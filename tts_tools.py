from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import urllib.request
from typing import Any

import safe_subprocess

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)
edge_tts = None


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v


_TTS_SESSION_KEY = "tts_session"
_TTS_LANG_KEYS = ("auto", "uz", "ru", "en", "hi", "ar")
_TTS_SEX_KEYS = ("male", "female")
_TTS_TONE_BASE_KEYS = ("soft", "playful", "calm", "serious", "screaming", "laughing", "whispering", "crying")
_TTS_SPEED_KEYS = ("slow", "normal", "fast")
_TTS_OUTPUT_KEYS = ("voice", "audio")


def _tts_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "start": "🎙️ Text to Voice (natural)\n\nAvval sozlamalarni tanlang.",
            "panel": (
                "🎙️ Text to Voice (natural)\n\n"
                "🌐 Til: {lang_label}\n"
                "🧑 Ovoz turi: {sex_label}\n"
                "🎭 Ohang: {tone_label}\n"
                "⚡ Tezlik: {speed_label}\n"
                "📤 Format: {output_label}\n"
                "🤖 AI tozalash: {ai_label}\n\n"
                "Sozlang va `Davom etish` ni bosing."
            ),
            "prompt_text": "📝 Endi matn yuboring.\nQo‘shimcha matn yuborishingiz mumkin.\nBekor qilish: cancel",
            "confirm": "✅ Matn qabul qilindi.\n🧾 Belgilar: {chars}\n📄 Qatorlar: {lines}\n\nYana matn yuborsangiz qo‘shiladi. Tayyor bo‘lsa `Ovoz yaratish` ni bosing.",
            "added": "Qo‘shimcha matn qo‘shildi.",
            "working": "🎙️ Ovoz tayyorlanmoqda...",
            "done": "✅ Ovoz tayyor.",
            "done_next": "✅ Ovoz yuborildi.\n📝 Yana matn yuboring yoki bekor qilish uchun `cancel` yozing.",
            "cancelled": "Text to Voice bekor qilindi.",
            "expired": "Sessiya tugadi. Pastdagi menyudan Text to Voice bo‘limini qayta tanlang.",
            "empty": "Iltimos, matn yuboring.",
            "too_long": "Matn juda uzun. Iltimos, qisqaroq yuboring (taxminan 12000 belgi).",
            "tools_missing": "Natural TTS uchun `edge-tts` va `ffmpeg` kerak (internet ham kerak).",
            "gen_btn": "Ovoz yaratish",
            "opt_btn": "Sozlamalar",
            "continue_btn": "Davom etish",
            "cancel_btn": "Bekor qilish",
            "use_buttons_hint": "Sozlamalarni tugmalar orqali tanlang, keyin Davom etish tugmasini bosing.",
            "session_other": "Bu Text to Voice sessiyasi boshqa foydalanuvchiga tegishli.",
            "choose_sex_first": "Avval male/female ni tanlang.",
            "generating_short": "Yaratilmoqda...",
            "ai_toggle_prefix": "AI Voice Booster",
            "lang_auto": "Avto",
            "lang_uz": "O‘zbek",
            "lang_ru": "Rus",
            "lang_en": "Ingliz",
            "lang_hi": "Hindi",
            "lang_ar": "Arab",
            "sex_male": "Erkak",
            "sex_female": "Ayol",
            "tone_soft": "Yumshoq",
            "tone_playful": "Quvnoq",
            "tone_calm": "Sokin",
            "tone_serious": "Jiddiy",
            "tone_screaming": "Baqirib",
            "tone_laughing": "Kulib",
            "tone_whispering": "Shivirlab",
            "tone_crying": "Yig‘lab",
            "tone_young_girl": "Yosh qiz",
            "tone_young_boy": "Yosh bola",
            "speed_slow": "Sekin",
            "speed_normal": "O‘rtacha",
            "speed_fast": "Tez",
            "output_voice": "Telegram voice xabar",
            "output_audio": "MP3 audio",
            "ai_on": "Yoqilgan",
            "ai_off": "O‘chiq",
            "caption": "🎙️ Matndan yaratilgan ovoz",
            "ai_note": "AI matnni yumshatib/tinqlab berdi (lokal Ollama).",
        }
    if lang == "ru":
        return {
            "start": "🎙️ Text to Voice (natural)\n\nСначала выберите настройки.",
            "panel": (
                "🎙️ Text to Voice (natural)\n\n"
                "🌐 Язык: {lang_label}\n"
                "🧑 Пол: {sex_label}\n"
                "🎭 Тон: {tone_label}\n"
                "⚡ Скорость: {speed_label}\n"
                "📤 Формат: {output_label}\n"
                "🤖 AI очистка: {ai_label}\n\n"
                "Настройте и нажмите `Продолжить`."
            ),
            "prompt_text": "📝 Теперь отправьте текст.\nМожно отправить несколько частей.\nОтмена: cancel",
            "confirm": "✅ Текст получен.\n🧾 Символы: {chars}\n📄 Строки: {lines}\n\nОтправьте ещё текст для добавления или нажмите `Создать голос`.",
            "added": "Дополнительный текст добавлен.",
            "working": "🎙️ Создаю голос...",
            "done": "✅ Голос готов.",
            "done_next": "✅ Голос отправлен.\n📝 Отправьте следующий текст или напишите `cancel` для отмены.",
            "cancelled": "Text to Voice отменен.",
            "expired": "Сессия истекла. Снова выберите Text to Voice в меню ниже.",
            "empty": "Пожалуйста, отправьте текст.",
            "too_long": "Текст слишком длинный. Отправьте короче (примерно до 12000 символов).",
            "tools_missing": "Для natural TTS нужны `edge-tts` и `ffmpeg` (и интернет).",
            "gen_btn": "Создать голос",
            "opt_btn": "Настройки",
            "continue_btn": "Продолжить",
            "cancel_btn": "Отмена",
            "use_buttons_hint": "Выберите настройки кнопками, затем нажмите «Продолжить».",
            "session_other": "Эта сессия Text to Voice принадлежит другому пользователю.",
            "choose_sex_first": "Сначала выберите male/female.",
            "generating_short": "Создаю...",
            "ai_toggle_prefix": "AI Voice Booster",
            "lang_auto": "Авто",
            "lang_uz": "Узбекский",
            "lang_ru": "Русский",
            "lang_en": "Английский",
            "lang_hi": "Хинди",
            "lang_ar": "Арабский",
            "sex_male": "Мужской",
            "sex_female": "Женский",
            "tone_soft": "Мягкий",
            "tone_playful": "Игривый",
            "tone_calm": "Спокойный",
            "tone_serious": "Серьёзный",
            "tone_screaming": "Кричащий",
            "tone_laughing": "Смеясь",
            "tone_whispering": "Шёпот",
            "tone_crying": "Плачущий",
            "tone_young_girl": "Юная девушка",
            "tone_young_boy": "Юный парень",
            "speed_slow": "Медленно",
            "speed_normal": "Обычная",
            "speed_fast": "Быстро",
            "output_voice": "Голосовое Telegram",
            "output_audio": "MP3 аудио",
            "ai_on": "Вкл",
            "ai_off": "Выкл",
            "caption": "🎙️ Голос из текста",
            "ai_note": "AI подготовил текст (локальный Ollama).",
        }
    return {
        "start": "🎙️ Text to Voice (natural)\n\nChoose settings first.",
        "panel": (
            "🎙️ Text to Voice (natural)\n\n"
            "🌐 Language: {lang_label}\n"
            "🧑 Sex: {sex_label}\n"
            "🎭 Tone: {tone_label}\n"
            "⚡ Speed: {speed_label}\n"
            "📤 Output: {output_label}\n"
            "🤖 AI cleanup: {ai_label}\n\n"
            "Adjust settings and tap `Continue`."
        ),
        "prompt_text": "📝 Now send text.\nYou can send multiple parts.\nCancel: cancel",
        "confirm": "✅ Text received.\n🧾 Characters: {chars}\n📄 Lines: {lines}\n\nSend more text to append, or tap `Generate Voice`.",
        "added": "Added another text part.",
        "working": "🎙️ Generating voice...",
        "done": "✅ Voice is ready.",
        "done_next": "✅ Voice sent.\n📝 Send another text, or type `cancel` to stop.",
        "cancelled": "Text to Voice cancelled.",
        "expired": "Session expired. Please choose Text to Voice again from the menu below.",
        "empty": "Please send text.",
        "too_long": "Text is too long. Please send a shorter text (about 12,000 chars max).",
        "tools_missing": "Natural TTS requires `edge-tts` and `ffmpeg` (and internet).",
        "gen_btn": "Generate Voice",
        "opt_btn": "Options",
        "continue_btn": "Continue",
        "cancel_btn": "Cancel",
        "use_buttons_hint": "Use the buttons to choose settings, then tap Continue.",
        "session_other": "This Text to Voice session belongs to another user.",
        "choose_sex_first": "Choose male/female first.",
        "generating_short": "Generating...",
        "ai_toggle_prefix": "AI Voice Booster",
        "lang_auto": "Auto",
        "lang_uz": "Uzbek",
        "lang_ru": "Russian",
        "lang_en": "English",
        "lang_hi": "Hindi",
        "lang_ar": "Arabic",
        "sex_male": "Male",
        "sex_female": "Female",
        "tone_soft": "Soft",
        "tone_playful": "Playful",
        "tone_calm": "Calm",
        "tone_serious": "Serious",
        "tone_screaming": "Screaming",
        "tone_laughing": "Laughing",
        "tone_whispering": "Whispering",
        "tone_crying": "Crying",
        "tone_young_girl": "Young Girl",
        "tone_young_boy": "Young Boy",
        "speed_slow": "Slow",
        "speed_normal": "Normal (average)",
        "speed_fast": "Fast",
        "output_voice": "Telegram voice",
        "output_audio": "MP3 audio",
        "ai_on": "On",
        "ai_off": "Off",
        "caption": "🎙️ Voice generated from text",
        "ai_note": "AI cleaned/punctuated the text (local Ollama).",
    }


def _tts_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_TTS_SESSION_KEY, None)


def _tts_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_TTS_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _tts_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_TTS_SESSION_KEY] = dict(session)


def _tts_guess_lang_key(text: str, user_lang: str = "en") -> str:
    s = text or ""
    if any("\u0400" <= ch <= "\u04FF" for ch in s):
        return "ru"
    if any("\u0600" <= ch <= "\u06FF" for ch in s):
        return "ar"
    if any("\u0900" <= ch <= "\u097F" for ch in s):
        return "hi"
    lower = s.lower()
    # Prefer strong Uzbek signals first; avoid false positives from common English "sh/ch/x".
    strong_uz_markers = ("o‘", "g‘", "o'z", "g'", "oʻ", "gʻ", "oʼ", "gʼ", "ʻ", "ʼ")
    if any(tok in lower for tok in strong_uz_markers):
        return "uz"
    uzbek_words = (
        "salom", "assalomu", "alaykum", "kitob", "yaxshi", "rahmat", "iltimos",
        "menga", "menda", "siz", "biz", "uchun", "bilan", "bugun", "ertaga",
        "kecha", "hozir", "qanday", "nima", "nega", "yoq", "yo'q", "shahar",
        "markaz", "yordam", "bering",
    )
    word_hits = sum(1 for w in uzbek_words if re.search(rf"\b{re.escape(w)}\b", lower))
    has_uz_digraph = any(tok in lower for tok in ("sh", "ch", "ng"))
    has_uz_letters = any(ch in lower for ch in ("q", "x"))
    if word_hits >= 2:
        return "uz"
    if word_hits >= 1 and (has_uz_digraph or has_uz_letters):
        return "uz"
    return user_lang if user_lang in {"uz", "ru", "en", "hi", "ar"} else "en"


def _tts_label(key: str, category: str, lang: str) -> str:
    msgs = _tts_texts(lang)
    return msgs.get(f"{category}_{key}", key)


def _tts_session_labels(session: dict, lang: str) -> dict:
    sex_key = str(session.get("sex") or ("female" if str(session.get("voice")) == "female" else "male"))
    tone_key = str(session.get("tone") or "soft")
    return {
        "lang_label": _tts_label(str(session.get("lang") or "auto"), "lang", lang),
        "sex_label": _tts_label(sex_key, "sex", lang),
        "tone_label": _tts_label(tone_key, "tone", lang),
        "speed_label": _tts_label(str(session.get("speed") or "normal"), "speed", lang),
        "output_label": _tts_label(str(session.get("output") or "voice"), "output", lang),
        "ai_label": _tts_texts(lang)["ai_on"] if bool(session.get("ai")) else _tts_texts(lang)["ai_off"],
    }


def _tts_allowed_tones(sex_key: str) -> tuple[str, ...]:
    if sex_key == "female":
        return _TTS_TONE_BASE_KEYS + ("young_girl",)
    return _TTS_TONE_BASE_KEYS + ("young_boy",)


def _tts_tools_available() -> bool:
    return bool(edge_tts is not None and shutil.which("ffmpeg"))


def _tts_options_keyboard(session: dict, lang: str) -> InlineKeyboardMarkup:
    msgs = _tts_texts(lang)
    sex_current = str(session.get("sex") or ("female" if str(session.get("voice")) == "female" else "male"))
    tone_current = str(session.get("tone") or "soft")
    allowed_tones = _tts_allowed_tones(sex_current)
    if tone_current not in allowed_tones:
        tone_current = "soft"
    current = {
        "lang": str(session.get("lang") or "auto"),
        "sex": sex_current,
        "tone": tone_current,
        "speed": str(session.get("speed") or "normal"),
        "output": str(session.get("output") or "voice"),
        "ai": bool(session.get("ai")),
    }
    def mark(is_on: bool, label: str) -> str:
        return label
    tone_rows = []
    tone_buttons = [
        InlineKeyboardButton(
            mark(current["tone"] == tone_key, _tts_label(tone_key, "tone", lang)),
            callback_data=f"tts:set:tone:{tone_key}"
        )
        for tone_key in allowed_tones
    ]
    for i in range(0, len(tone_buttons), 2):
        tone_rows.append(tone_buttons[i:i + 2])
    rows = [
        [
            InlineKeyboardButton(mark(current["lang"] == "auto", "auto"), callback_data="tts:set:lang:auto"),
            InlineKeyboardButton(mark(current["lang"] == "uz", "uz"), callback_data="tts:set:lang:uz"),
        ],
        [
            InlineKeyboardButton(mark(current["lang"] == "ru", "ru"), callback_data="tts:set:lang:ru"),
            InlineKeyboardButton(mark(current["lang"] == "en", "en"), callback_data="tts:set:lang:en"),
        ],
        [
            InlineKeyboardButton(mark(current["lang"] == "hi", "hi"), callback_data="tts:set:lang:hi"),
            InlineKeyboardButton(mark(current["lang"] == "ar", "ar"), callback_data="tts:set:lang:ar"),
        ],
        [
            InlineKeyboardButton(mark(current["sex"] == "male", _tts_label("male", "sex", lang)), callback_data="tts:set:sex:male"),
            InlineKeyboardButton(mark(current["sex"] == "female", _tts_label("female", "sex", lang)), callback_data="tts:set:sex:female"),
        ],
        [
            InlineKeyboardButton(mark(current["speed"] == "slow", _tts_label("slow", "speed", lang)), callback_data="tts:set:speed:slow"),
            InlineKeyboardButton(mark(current["speed"] == "normal", _tts_label("normal", "speed", lang)), callback_data="tts:set:speed:normal"),
        ],
        [
            InlineKeyboardButton(mark(current["speed"] == "fast", _tts_label("fast", "speed", lang)), callback_data="tts:set:speed:fast"),
            InlineKeyboardButton(mark(current["ai"], f"{msgs['ai_toggle_prefix']} {msgs['ai_on'] if current['ai'] else msgs['ai_off']}"), callback_data="tts:toggle:ai"),
        ],
        [
            InlineKeyboardButton(mark(current["output"] == "voice", _tts_label("voice", "output", lang)), callback_data="tts:set:output:voice"),
            InlineKeyboardButton(mark(current["output"] == "audio", _tts_label("audio", "output", lang)), callback_data="tts:set:output:audio"),
        ],
        [
            InlineKeyboardButton(f"✅ {msgs['continue_btn']}", callback_data="tts:opt:done"),
            InlineKeyboardButton(f"❌ {msgs['cancel_btn']}", callback_data="tts:cancel"),
        ],
    ]
    rows = rows[:3] + tone_rows + rows[3:]
    return InlineKeyboardMarkup(rows)


_TTS_WIZARD_STEPS = (
    "awaiting_lang_select",
    "awaiting_sex_select",
    "awaiting_tone_select",
    "awaiting_speed_select",
    "awaiting_output_select",
    "awaiting_settings_confirm",
)


def _tts_wizard_prev_phase(phase: str) -> str | None:
    chain = {
        "awaiting_sex_select": "awaiting_lang_select",
        "awaiting_tone_select": "awaiting_sex_select",
        "awaiting_speed_select": "awaiting_tone_select",
        "awaiting_output_select": "awaiting_speed_select",
        "awaiting_settings_confirm": "awaiting_output_select",
        "awaiting_text": "awaiting_settings_confirm",
        "awaiting_confirm": "awaiting_text",
    }
    return chain.get(phase)


def _tts_wizard_prompt_text(session: dict, lang: str) -> str:
    msgs = _tts_texts(lang)
    labels = _tts_session_labels(session, lang)
    phase = str(session.get("phase") or "awaiting_lang_select")
    phase_map_all = {
        "uz": {
            "awaiting_lang_select": "1/6 🌐 Tilni tanlang",
            "awaiting_sex_select": "2/6 🧑 Ovoz turini tanlang",
            "awaiting_tone_select": "3/6 🎭 Ovoz ohangini tanlang",
            "awaiting_speed_select": "4/6 ⚡ Tezlikni tanlang",
            "awaiting_output_select": "5/6 📤 Formatni tanlang",
            "awaiting_settings_confirm": "6/6 ✅ Sozlamalarni tasdiqlang",
            "awaiting_text": "📝 Matn yuboring",
            "awaiting_confirm": "✅ Matnni tasdiqlang",
        },
        "ru": {
            "awaiting_lang_select": "1/6 🌐 Выберите язык",
            "awaiting_sex_select": "2/6 🧑 Выберите пол голоса",
            "awaiting_tone_select": "3/6 🎭 Выберите тон голоса",
            "awaiting_speed_select": "4/6 ⚡ Выберите скорость",
            "awaiting_output_select": "5/6 📤 Выберите формат",
            "awaiting_settings_confirm": "6/6 ✅ Подтвердите настройки",
            "awaiting_text": "📝 Отправьте текст",
            "awaiting_confirm": "✅ Подтвердите текст",
        },
        "en": {
            "awaiting_lang_select": "1/6 🌐 Select language",
            "awaiting_sex_select": "2/6 🧑 Select voice gender",
            "awaiting_tone_select": "3/6 🎭 Select voice tone",
            "awaiting_speed_select": "4/6 ⚡ Select speed",
            "awaiting_output_select": "5/6 📤 Select format",
            "awaiting_settings_confirm": "6/6 ✅ Confirm settings",
            "awaiting_text": "📝 Send text",
            "awaiting_confirm": "✅ Confirm text",
        },
    }
    phase_map = phase_map_all.get(lang, phase_map_all["en"])
    phase_hint = {
        "uz": "Tugmalar orqali tanlang.",
        "ru": "Выберите кнопками.",
        "en": "Choose using the buttons.",
    }.get(lang, "Choose using the buttons.")
    return (
        "🎙️ Text to Voice (natural)\n\n"
        f"{phase_map.get(phase, 'Choose settings')}\n"
        "──────────\n"
        f"🌐 {labels['lang_label']}\n"
        f"🧑 {labels['sex_label']}\n"
        f"🎭 {labels['tone_label']}\n"
        f"⚡ {labels['speed_label']}\n"
        f"📤 {labels['output_label']}\n"
        f"🤖 AI: {labels['ai_label']}\n\n"
        f"{phase_hint}"
    )


def _tts_wizard_keyboard(session: dict, lang: str) -> InlineKeyboardMarkup:
    msgs = _tts_texts(lang)
    phase = str(session.get("phase") or "awaiting_lang_select")
    back_label = MESSAGES.get(lang, MESSAGES["en"]).get("menu_back", "Back")
    def mark(on: bool, label: str) -> str:
        return label
    rows: list[list[InlineKeyboardButton]] = []
    if phase == "awaiting_lang_select":
        current = str(session.get("lang") or "auto")
        rows += [
            [
                InlineKeyboardButton(mark(current == "auto", "auto"), callback_data="tts:wiz:lang:auto"),
                InlineKeyboardButton(mark(current == "uz", "uz"), callback_data="tts:wiz:lang:uz"),
            ],
            [
                InlineKeyboardButton(mark(current == "ru", "ru"), callback_data="tts:wiz:lang:ru"),
                InlineKeyboardButton(mark(current == "en", "en"), callback_data="tts:wiz:lang:en"),
            ],
            [
                InlineKeyboardButton(mark(current == "hi", "hi"), callback_data="tts:wiz:lang:hi"),
                InlineKeyboardButton(mark(current == "ar", "ar"), callback_data="tts:wiz:lang:ar"),
            ],
        ]
    elif phase == "awaiting_sex_select":
        current = str(session.get("sex") or "male")
        rows += [[
            InlineKeyboardButton(mark(current == "male", _tts_label("male", "sex", lang)), callback_data="tts:wiz:sex:male"),
            InlineKeyboardButton(mark(current == "female", _tts_label("female", "sex", lang)), callback_data="tts:wiz:sex:female"),
        ]]
    elif phase == "awaiting_tone_select":
        current_sex = str(session.get("sex") or "male")
        current_tone = str(session.get("tone") or "soft")
        tones = _tts_allowed_tones(current_sex)
        btns = [
            InlineKeyboardButton(mark(current_tone == t, _tts_label(t, "tone", lang)), callback_data=f"tts:wiz:tone:{t}")
            for t in tones
        ]
        for i in range(0, len(btns), 2):
            rows.append(btns[i:i + 2])
    elif phase == "awaiting_speed_select":
        current = str(session.get("speed") or "normal")
        rows += [
            [
                InlineKeyboardButton(mark(current == "slow", _tts_label("slow", "speed", lang)), callback_data="tts:wiz:speed:slow"),
                InlineKeyboardButton(mark(current == "normal", _tts_label("normal", "speed", lang)), callback_data="tts:wiz:speed:normal"),
            ],
            [
                InlineKeyboardButton(mark(current == "fast", _tts_label("fast", "speed", lang)), callback_data="tts:wiz:speed:fast"),
            ],
        ]
    elif phase == "awaiting_output_select":
        current = str(session.get("output") or "voice")
        rows += [[
            InlineKeyboardButton(mark(current == "voice", _tts_label("voice", "output", lang)), callback_data="tts:wiz:output:voice"),
            InlineKeyboardButton(mark(current == "audio", _tts_label("audio", "output", lang)), callback_data="tts:wiz:output:audio"),
        ]]
    elif phase == "awaiting_settings_confirm":
        rows += [
            [InlineKeyboardButton(
                mark(bool(session.get("ai")), f"{msgs['ai_toggle_prefix']} {msgs['ai_on'] if bool(session.get('ai')) else msgs['ai_off']}"),
                callback_data="tts:wiz:toggleai"
            )],
            [InlineKeyboardButton(f"✅ {msgs['continue_btn']}", callback_data="tts:wiz:next:text")],
        ]

    nav_row = []
    if phase != "awaiting_lang_select":
        nav_row.append(InlineKeyboardButton(f"⬅️ {back_label}", callback_data="tts:wiz:back"))
    nav_row.append(InlineKeyboardButton(f"❌ {msgs['cancel_btn']}", callback_data="tts:cancel"))
    rows.append(nav_row)
    return InlineKeyboardMarkup(rows)


async def _tts_send_wizard_step(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict, lang: str):
    return await _tts_edit_or_send_prompt(
        update,
        context,
        session,
        _tts_wizard_prompt_text(session, lang),
        reply_markup=_tts_wizard_keyboard(session, lang),
        prefer_edit=True,
    )


def _tts_confirm_keyboard(lang: str) -> InlineKeyboardMarkup:
    msgs = _tts_texts(lang)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ {msgs['gen_btn']}", callback_data="tts:gen:confirm")],
        [
            InlineKeyboardButton(f"⚙️ {msgs['opt_btn']}", callback_data="tts:opt:back"),
            InlineKeyboardButton(f"❌ {msgs['cancel_btn']}", callback_data="tts:cancel"),
        ],
    ])


async def _tts_edit_or_send_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict, text: str, reply_markup=None, prefer_edit: bool = False):
    chat_id = session.get("prompt_chat_id")
    msg_id = session.get("prompt_message_id")
    if prefer_edit and chat_id and msg_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=reply_markup)
            return True
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return True
        except Exception:
            pass
    if chat_id and msg_id:
        try:
            await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except Exception:
            pass
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return False
    sent = await _send_with_retry(lambda: target_message.reply_text(text, reply_markup=reply_markup))
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _tts_save_session(context, session)
        return True
    return False


async def _tts_send_options_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict, lang: str):
    text = _tts_texts(lang)["panel"].format(**_tts_session_labels(session, lang))
    return await _tts_edit_or_send_prompt(update, context, session, text, reply_markup=_tts_options_keyboard(session, lang), prefer_edit=True)


def _tts_text_stats(text: str) -> dict:
    s = str(text or "")
    return {"chars": len(s), "lines": len(s.splitlines()) if s else 0}


def _tts_edge_voice_name(lang_key: str, sex_key: str, tone_key: str) -> str:
    # Tones are simulated mostly with rate/pitch/volume; some tones switch to a more suitable voice.
    def profile(base: str, *, playful: str | None = None, young: str | None = None, young_key: str | None = None) -> dict[str, str]:
        m = {"base": base}
        for t in _TTS_TONE_BASE_KEYS:
            m[t] = base
        if playful:
            m["playful"] = playful
            m["laughing"] = playful
        if young and young_key:
            m[young_key] = young
        return m

    voice_map = {
        "uz": {
            "female": profile("uz-UZ-MadinaNeural"),
            "male": profile("uz-UZ-SardorNeural"),
        },
        "ru": {
            "female": profile("ru-RU-SvetlanaNeural", playful="ru-RU-DariyaNeural", young="ru-RU-DariyaNeural", young_key="young_girl"),
            "male": profile("ru-RU-DmitryNeural"),
        },
        "en": {
            "female": profile("en-US-JennyNeural", playful="en-US-AnaNeural", young="en-US-AnaNeural", young_key="young_girl"),
            "male": profile("en-US-AndrewNeural", playful="en-US-BrianNeural", young="en-US-BrianNeural", young_key="young_boy"),
        },
        "hi": {
            "female": profile("hi-IN-SwaraNeural"),
            "male": profile("hi-IN-MadhurNeural"),
        },
        "ar": {
            "female": profile("ar-SA-ZariyahNeural"),
            "male": profile("ar-SA-HamedNeural"),
        },
    }
    lang_map = voice_map.get(lang_key) or voice_map["en"]
    sex_map = lang_map.get(sex_key) or lang_map["male"]
    return sex_map.get(tone_key) or sex_map["base"]


def _tts_edge_rate(speed_key: str, tone_key: str = "soft") -> str:
    base = {"slow": "-20%", "normal": "+0%", "fast": "+18%"}.get(speed_key, "+0%")
    if tone_key == "soft":
        return "-10%" if speed_key == "normal" else base
    if tone_key == "playful":
        return {"slow": "-8%", "normal": "+12%", "fast": "+22%"}.get(speed_key, "+12%")
    if tone_key == "calm":
        return {"slow": "-24%", "normal": "-12%", "fast": "-2%"}.get(speed_key, "-12%")
    if tone_key == "serious":
        return {"slow": "-15%", "normal": "-6%", "fast": "+4%"}.get(speed_key, "-6%")
    if tone_key == "screaming":
        return {"slow": "+8%", "normal": "+20%", "fast": "+34%"}.get(speed_key, "+20%")
    if tone_key == "laughing":
        return {"slow": "+0%", "normal": "+16%", "fast": "+28%"}.get(speed_key, "+16%")
    if tone_key == "whispering":
        return {"slow": "-28%", "normal": "-18%", "fast": "-8%"}.get(speed_key, "-18%")
    if tone_key == "crying":
        return {"slow": "-22%", "normal": "-10%", "fast": "+0%"}.get(speed_key, "-10%")
    if tone_key in {"young_girl", "young_boy"}:
        return {"slow": "-6%", "normal": "+8%", "fast": "+20%"}.get(speed_key, "+8%")
    return base


def _tts_edge_pitch(tone_key: str, sex_key: str = "male") -> str:
    return {
        "soft": "-4Hz",
        "playful": "+8Hz" if sex_key == "female" else "+4Hz",
        "calm": "-6Hz" if sex_key == "female" else "-8Hz",
        "serious": "-5Hz" if sex_key == "female" else "-8Hz",
        "screaming": "+12Hz" if sex_key == "female" else "+8Hz",
        "laughing": "+10Hz" if sex_key == "female" else "+6Hz",
        "whispering": "-6Hz" if sex_key == "female" else "-4Hz",
        "crying": "+6Hz" if sex_key == "female" else "+2Hz",
        "young_girl": "+16Hz",
        "young_boy": "+8Hz",
    }.get(tone_key, "+0Hz")


def _tts_edge_volume(tone_key: str) -> str:
    return {
        "soft": "-8%",
        "calm": "-6%",
        "serious": "-2%",
        "screaming": "+12%",
        "laughing": "+5%",
        "whispering": "-22%",
        "crying": "-4%",
        "young_girl": "+2%",
        "young_boy": "+2%",
    }.get(tone_key, "+0%")


async def _tts_edge_save_mp3_async(text: str, voice: str, rate: str, pitch: str, volume: str, out_path: str):
    if edge_tts is None:
        raise RuntimeError("edge-tts module is not installed")
    communicate = edge_tts.Communicate(text=text, voice=voice, rate=rate, pitch=pitch, volume=volume)
    await communicate.save(out_path)


def _tts_ollama_polish_text(text: str, lang_key: str) -> str:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv("TTS_OLLAMA_MODEL", os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b"))
    timeout_s = float(os.getenv("TTS_OLLAMA_TIMEOUT", "20"))
    prompt = (
        "Rewrite the text for text-to-speech ONLY.\n"
        "Keep the SAME language and SAME meaning.\n"
        "Add punctuation and natural sentence breaks.\n"
        "Do not summarize. Do not translate. Return plain text only.\n\n"
        f"Language hint: {lang_key}\n\n"
        f"{text[:5000]}"
    )
    payload = {"model": model, "prompt": prompt, "stream": False, "keep_alive": "10m", "options": {"temperature": 0.1}}
    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    out = str((data or {}).get("response") or "").strip()
    return out or text


def _tts_build_audio_bytes_blocking(text: str, opts: dict) -> tuple[bytes, str]:
    if not _tts_tools_available():
        raise RuntimeError("Natural TTS tools missing")
    lang_key = str(opts.get("lang") or "en")
    sex_key = str(opts.get("sex") or ("female" if str(opts.get("voice")) == "female" else "male"))
    tone_key = str(opts.get("tone") or "soft")
    speed_key = str(opts.get("speed") or "normal")
    output_key = str(opts.get("output") or "voice")
    use_ai = bool(opts.get("ai"))
    effective_text = (text or "").strip()
    ai_used = False
    if use_ai:
        try:
            effective_text = _tts_ollama_polish_text(effective_text, lang_key)
            ai_used = True
        except Exception as e:
            logger.info("TTS Ollama cleanup fallback: %s", e)
    if tone_key not in _tts_allowed_tones(sex_key):
        tone_key = "soft"
    edge_voice = _tts_edge_voice_name(lang_key, sex_key, tone_key)
    edge_rate = _tts_edge_rate(speed_key, tone_key)
    edge_pitch = _tts_edge_pitch(tone_key, sex_key)
    edge_volume = _tts_edge_volume(tone_key)
    with tempfile.TemporaryDirectory(prefix="tts_") as td:
        mp3_path = os.path.join(td, "tts.mp3")
        out_path = os.path.join(td, "tts.ogg" if output_key == "voice" else "tts.mp3")
        try:
            asyncio.run(_tts_edge_save_mp3_async(
                text=effective_text,
                voice=edge_voice,
                rate=edge_rate,
                pitch=edge_pitch,
                volume=edge_volume,
                out_path=mp3_path,
            ))
        except Exception as e:
            raise RuntimeError(f"edge-tts failed: {str(e)[:400]}") from e
        if not os.path.exists(mp3_path):
            raise RuntimeError("edge-tts failed to generate audio")
        if output_key == "voice":
            ffmpeg_cmd = ["ffmpeg", "-y", "-i", mp3_path, "-c:a", "libopus", "-b:a", "32k", out_path]
        else:
            # Re-encode to normalize output for Telegram and reduce size.
            ffmpeg_cmd = ["ffmpeg", "-y", "-i", mp3_path, "-codec:a", "libmp3lame", "-q:a", "4", out_path]
        timeout_s = float(os.getenv("TTS_FFMPEG_TIMEOUT_S", "60") or "60")
        p2 = safe_subprocess.run(ffmpeg_cmd, timeout_s=timeout_s, max_output_chars=8000, text=False)
        if p2.returncode != 0:
            raw_err = getattr(p2, "stderr", b"")
            if isinstance(raw_err, bytes):
                err_text = raw_err.decode("utf-8", errors="replace")
            else:
                err_text = str(raw_err or "")
            err_text = (err_text or "ffmpeg failed").strip()
            raise RuntimeError(err_text[-800:])
        if not os.path.exists(out_path):
            raise RuntimeError("ffmpeg output file missing")
        with open(out_path, "rb") as f:
            data = f.read()
    return data, ("voice_ai" if ai_used else "voice")


async def _tts_send_result(update: Update, audio_bytes: bytes, output_mode: str, caption: str):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    bio = io.BytesIO(audio_bytes)
    if output_mode == "voice":
        bio.name = "text_to_voice.ogg"
        return await _send_with_retry(lambda: target_message.reply_voice(voice=bio, caption=caption))
    bio.name = "text_to_voice.mp3"
    return await _send_with_retry(lambda: target_message.reply_audio(audio=bio, caption=caption, title="Text to Voice"))


async def _tts_generate_and_send(update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str, text: str, opts: dict) -> bool:
    msgs = _tts_texts(lang_ui)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return False
    if not _tts_tools_available():
        await target_message.reply_text(msgs["tools_missing"])
        return False
    clean = (text or "").strip()
    if not clean:
        await target_message.reply_text(msgs["empty"])
        return False
    if len(clean) > 12000:
        await target_message.reply_text(msgs["too_long"])
        return False
    lang_key = str(opts.get("lang") or "auto")
    if lang_key == "auto":
        lang_key = _tts_guess_lang_key(clean, lang_ui)
    opts = dict(opts)
    opts["lang"] = lang_key
    status = await _send_with_retry(lambda: target_message.reply_text(msgs["working"]))
    try:
        audio_bytes, mode_meta = await run_blocking(_tts_build_audio_bytes_blocking, clean, opts)
    except Exception as e:
        logger.error("text_to_voice generation failed: %s", e, exc_info=True)
        if status:
            try:
                await status.edit_text(f"{MESSAGES[lang_ui]['error']}\n{str(e)[:300]}")
            except Exception:
                pass
        else:
            await target_message.reply_text(f"{MESSAGES[lang_ui]['error']}\n{str(e)[:300]}")
        return False
    caption = msgs["caption"]
    if bool(opts.get("ai")) and mode_meta == "voice_ai":
        caption += f"\n🤖 {msgs['ai_note']}"
    sent = await _tts_send_result(update, audio_bytes, str(opts.get("output") or "voice"), caption)
    if status:
        try:
            await status.edit_text(msgs["done"] if sent else MESSAGES[lang_ui]["error"])
        except Exception:
            pass
    return sent is not None


async def _tts_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str) -> bool:
    if not update.message or not update.message.text:
        return False
    session = _tts_get_session(context)
    if not session:
        return False
    msgs = _tts_texts(lang_ui)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _tts_clear_session(context)
        await update.message.reply_text(msgs["expired"])
        return True
    if update.effective_user and session.get("user_id") and int(session["user_id"]) != int(update.effective_user.id):
        return False
    txt = (update.message.text or "").strip()
    if txt.lower() in {"cancel", "stop"}:
        _tts_clear_session(context)
        await update.message.reply_text(msgs["cancelled"])
        return True
    phase = str(session.get("phase") or "")
    if phase == "awaiting_options" or phase in _TTS_WIZARD_STEPS:
        await update.message.reply_text(msgs["use_buttons_hint"])
        return True
    if phase == "awaiting_text":
        session["text_buffer"] = txt
        session["phase"] = "awaiting_confirm"
        session["expires_at"] = time.time() + 1800
        _tts_save_session(context, session)
        await _tts_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["confirm"].format(**_tts_text_stats(session["text_buffer"])),
            reply_markup=_tts_confirm_keyboard(lang_ui),
            prefer_edit=True,
        )
        return True
    if phase == "awaiting_confirm":
        cur = str(session.get("text_buffer") or "")
        session["text_buffer"] = f"{cur}\n{txt}".strip() if cur else txt
        session["expires_at"] = time.time() + 1800
        _tts_save_session(context, session)
        await _tts_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["confirm"].format(**_tts_text_stats(session["text_buffer"])),
            reply_markup=_tts_confirm_keyboard(lang_ui),
            prefer_edit=True,
        )
        return True
    return False


async def text_to_voice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang_ui = ensure_user_language(update, context)
    if not update.message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang_ui]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang_ui]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    await _tts_start_session_from_message(update.message, update, context, lang_ui)


async def _tts_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str):
    _tts_clear_session(context)
    session = {
        "user_id": update.effective_user.id if update.effective_user else None,
        "phase": "awaiting_lang_select",
        "lang": "auto",
        "sex": "male",
        "tone": "soft",
        "speed": "normal",
        "output": "voice",
        "ai": False,
        "expires_at": time.time() + 1800,
    }
    _tts_save_session(context, session)
    text = _tts_wizard_prompt_text(session, lang_ui)
    sent = await _send_with_retry(lambda: target_message.reply_text(text, reply_markup=_tts_wizard_keyboard(session, lang_ui)))
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _tts_save_session(context, session)


async def handle_tts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = str(query.data or "")
    if not data.startswith("tts:"):
        await safe_answer(query)
        return
    lang_ui = ensure_user_language(update, context)
    msgs = _tts_texts(lang_ui)
    session = _tts_get_session(context)
    if not session:
        await safe_answer(query, msgs["expired"], show_alert=True)
        return
    if time.time() > float(session.get("expires_at", 0) or 0):
        _tts_clear_session(context)
        await safe_answer(query, msgs["expired"], show_alert=True)
        return
    if (query.from_user.id if query.from_user else None) != session.get("user_id"):
        await safe_answer(query, msgs["session_other"], show_alert=True)
        return
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    sub = parts[2] if len(parts) > 2 else ""
    value = parts[3] if len(parts) > 3 else ""
    phase = str(session.get("phase") or "")

    if action == "cancel":
        _tts_clear_session(context)
        await safe_answer(query, msgs["cancelled"])
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if action == "wiz":
        valid = False
        if sub == "lang" and phase == "awaiting_lang_select" and value in _TTS_LANG_KEYS:
            session["lang"] = value
            session["phase"] = "awaiting_sex_select"
            valid = True
        elif sub == "sex" and phase == "awaiting_sex_select" and value in _TTS_SEX_KEYS:
            session["sex"] = value
            if str(session.get("tone") or "soft") not in _tts_allowed_tones(value):
                session["tone"] = "soft"
            session["phase"] = "awaiting_tone_select"
            valid = True
        elif sub == "tone" and phase == "awaiting_tone_select":
            sex_now = str(session.get("sex") or "male")
            if value in _tts_allowed_tones(sex_now):
                session["tone"] = value
                session["phase"] = "awaiting_speed_select"
                valid = True
            else:
                await safe_answer(query, msgs["choose_sex_first"], show_alert=True)
                return
        elif sub == "speed" and phase == "awaiting_speed_select" and value in _TTS_SPEED_KEYS:
            session["speed"] = value
            session["phase"] = "awaiting_output_select"
            valid = True
        elif sub == "output" and phase == "awaiting_output_select" and value in _TTS_OUTPUT_KEYS:
            session["output"] = value
            session["phase"] = "awaiting_settings_confirm"
            valid = True
        elif sub == "toggleai" and phase == "awaiting_settings_confirm":
            session["ai"] = not bool(session.get("ai"))
            valid = True
        elif sub == "next" and value == "text" and phase == "awaiting_settings_confirm":
            session["phase"] = "awaiting_text"
            valid = True
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
            await safe_answer(query, msgs["continue_btn"])
            await _tts_edit_or_send_prompt(update, context, session, msgs["prompt_text"], reply_markup=None, prefer_edit=True)
            return
        elif sub == "back":
            prev = _tts_wizard_prev_phase(phase)
            if not prev:
                await safe_answer(query)
                return
            session["phase"] = prev
            valid = True

        if not valid:
            await safe_answer(query, MESSAGES[lang_ui]["error"], show_alert=True)
            return

        session["expires_at"] = time.time() + 1800
        _tts_save_session(context, session)
        if sub == "toggleai":
            await safe_answer(query, f"{msgs['ai_toggle_prefix']}: {msgs['ai_on'] if session['ai'] else msgs['ai_off']}")
        else:
            await safe_answer(query)
        await _tts_send_wizard_step(update, context, session, lang_ui)
        return

    if action == "set" and phase == "awaiting_options":
        if sub == "lang" and value in _TTS_LANG_KEYS:
            session["lang"] = value
        elif sub == "sex" and value in _TTS_SEX_KEYS:
            session["sex"] = value
            if str(session.get("tone") or "soft") not in _tts_allowed_tones(value):
                session["tone"] = "soft"
        elif sub == "tone":
            sex_now = str(session.get("sex") or "male")
            if value in _tts_allowed_tones(sex_now):
                session["tone"] = value
            else:
                await safe_answer(query, msgs["choose_sex_first"], show_alert=True)
                return
        elif sub == "speed" and value in _TTS_SPEED_KEYS:
            session["speed"] = value
        elif sub == "output" and value in _TTS_OUTPUT_KEYS:
            session["output"] = value
        else:
            await safe_answer(query, MESSAGES[lang_ui]["error"], show_alert=True)
            return
        session["expires_at"] = time.time() + 1800
        _tts_save_session(context, session)
        await safe_answer(query)
        await _tts_send_options_panel(update, context, session, lang_ui)
        return

    if action == "toggle" and phase == "awaiting_options":
        if sub == "ai":
            session["ai"] = not bool(session.get("ai"))
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
            await safe_answer(query, f"{msgs['ai_toggle_prefix']}: {msgs['ai_on'] if session['ai'] else msgs['ai_off']}")
            await _tts_send_options_panel(update, context, session, lang_ui)
            return

    if action == "opt":
        if sub == "done" and phase == "awaiting_options":
            session["phase"] = "awaiting_text"
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
            await safe_answer(query, msgs["continue_btn"])
            await _tts_edit_or_send_prompt(update, context, session, msgs["prompt_text"], reply_markup=None, prefer_edit=True)
            return
        if sub == "back" and phase == "awaiting_confirm":
            session["phase"] = "awaiting_settings_confirm"
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
            await safe_answer(query, msgs["opt_btn"])
            await _tts_send_wizard_step(update, context, session, lang_ui)
            return

    if action == "gen" and sub == "confirm":
        if phase != "awaiting_confirm":
            await safe_answer(query, msgs["expired"], show_alert=True)
            return
        final_text = str(session.get("text_buffer") or "").strip()
        if not final_text:
            await safe_answer(query, msgs["empty"], show_alert=True)
            return
        opts = {
            "lang": session.get("lang"),
            "sex": session.get("sex"),
            "tone": session.get("tone"),
            "speed": session.get("speed"),
            "output": session.get("output"),
            "ai": bool(session.get("ai")),
        }
        await safe_answer(query, msgs["generating_short"])
        sent_ok = await _tts_generate_and_send(update, context, lang_ui, final_text, opts)
        if sent_ok:
            session["phase"] = "awaiting_text"
            session["text_buffer"] = ""
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
            menu_markup = None
            main_menu_keyboard_fn = globals().get("_main_menu_keyboard")
            if callable(main_menu_keyboard_fn):
                try:
                    uid = update.effective_user.id if update.effective_user else session.get("user_id")
                    menu_markup = main_menu_keyboard_fn(lang_ui, "main", uid)
                except Exception:
                    menu_markup = None
            await _tts_edit_or_send_prompt(
                update,
                context,
                session,
                msgs["done_next"],
                reply_markup=menu_markup,
                prefer_edit=False,
            )
        else:
            session["expires_at"] = time.time() + 1800
            _tts_save_session(context, session)
        return

    await safe_answer(query, MESSAGES[lang_ui]["error"], show_alert=True)
