from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import math
import os
import re
import struct
import time
import urllib.parse
import urllib.request
import uuid
import wave
from typing import Any, Awaitable, Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import ContextTypes

from ai_mode_ui import (
    ai_active_mode_button_action as _ai_active_mode_button_action,
)

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)


async def _unconfigured_run_blocking(*args, **kwargs):
    raise RuntimeError("ai_tools module is not configured")


async def _unconfigured_run_blocking_heavy(*args, **kwargs):
    raise RuntimeError("ai_tools module is not configured")


def _unconfigured_main_menu_keyboard(*args, **kwargs):
    raise RuntimeError("ai_tools module is not configured")


async def _unconfigured_send_with_retry(*args, **kwargs):
    raise RuntimeError("ai_tools module is not configured")


async def _unconfigured_safe_answer(*args, **kwargs):
    raise RuntimeError("ai_tools module is not configured")


run_blocking: Callable[..., Awaitable[Any]] = _unconfigured_run_blocking
run_blocking_heavy: Callable[..., Awaitable[Any]] = _unconfigured_run_blocking_heavy
_send_with_retry: Callable[..., Awaitable[Any]] = _unconfigured_send_with_retry
_main_menu_keyboard: Callable[..., Any] = _unconfigured_main_menu_keyboard
safe_answer: Callable[..., Awaitable[Any]] = _unconfigured_safe_answer
_db_save_user_quiz = None
_db_get_user_quiz = None
_db_list_user_quizzes = None
_db_count_user_quizzes = None
_db_delete_user_quiz = None
_db_mark_user_quiz_started = None
_db_increment_user_quiz_share_count = None
_db_increment_counter = None
_WEBAPP_URL = ""  # Web app removed; keep for backward-compatible function signature
try:
    _MY_QUIZ_PAGE_SIZE = max(1, int(os.getenv("MY_QUIZ_PAGE_SIZE", "10") or "10"))
except Exception:
    _MY_QUIZ_PAGE_SIZE = 10


def configure(
    *,
    messages,
    logger_obj,
    run_blocking_fn,
    send_with_retry_fn,
    main_menu_keyboard_fn,
    safe_answer_fn=None,
    run_blocking_heavy_fn=None,
    db_save_user_quiz_fn=None,
    db_get_user_quiz_fn=None,
    db_list_user_quizzes_fn=None,
    db_count_user_quizzes_fn=None,
    db_delete_user_quiz_fn=None,
    db_mark_user_quiz_started_fn=None,
    db_increment_user_quiz_share_count_fn=None,
    db_increment_counter_fn=None,
) -> None:
    global MESSAGES, logger, run_blocking, run_blocking_heavy, _send_with_retry, _main_menu_keyboard, safe_answer
    global _db_save_user_quiz, _db_get_user_quiz, _db_list_user_quizzes, _db_count_user_quizzes
    global _db_delete_user_quiz, _db_mark_user_quiz_started, _db_increment_user_quiz_share_count, _db_increment_counter
    MESSAGES = messages
    logger = logger_obj
    run_blocking = run_blocking_fn
    run_blocking_heavy = run_blocking_heavy_fn or run_blocking_fn
    _send_with_retry = send_with_retry_fn
    _main_menu_keyboard = main_menu_keyboard_fn
    safe_answer = safe_answer_fn or _unconfigured_safe_answer
    _db_save_user_quiz = db_save_user_quiz_fn
    _db_get_user_quiz = db_get_user_quiz_fn
    _db_list_user_quizzes = db_list_user_quizzes_fn
    _db_count_user_quizzes = db_count_user_quizzes_fn
    _db_delete_user_quiz = db_delete_user_quiz_fn
    _db_mark_user_quiz_started = db_mark_user_quiz_started_fn
    _db_increment_user_quiz_share_count = db_increment_user_quiz_share_count_fn
    _db_increment_counter = db_increment_counter_fn


async def _ai_increment_counter(key: str, amount: int = 1) -> None:
    if not callable(_db_increment_counter):
        return
    try:
        await run_blocking(_db_increment_counter, key, amount)
    except Exception as e:
        logger.debug("AI counter update failed (%s): %s", key, e)


def _ai_schedule_counter_increment(context: ContextTypes.DEFAULT_TYPE, key: str, amount: int = 1) -> None:
    if not callable(_db_increment_counter):
        return
    app = getattr(context, "application", None)
    if app and hasattr(app, "create_task"):
        app.create_task(_ai_increment_counter(key, amount))
        return
    asyncio.create_task(_ai_increment_counter(key, amount))


_AI_CHAT_SESSION_KEY = "ai_chat_session"


def _ai_chat_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "greeting": (
                "💬 AI Chat\n\n"
                "Assalomu alaykum 👋\n"
                "Men lokal AI yordamchiman. Savollaringizni oddiy xabar qilib yuboring.\n"
                "Mavzuga mos, do‘stona javob beraman."
            ),
            "thinking": "🤖 AI o‘ylayapti...",
            "done": "✅ Javob tayyor.",
            "cancelled": "AI chat yopildi. Boshqa bo‘limni tanlashingiz mumkin.",
            "expired": "AI chat sessiyasi tugadi. Pastdagi `AI Tools` menyusidan qayta oching.",
            "empty": "Xabar yuboring.",
            "unavailable": "⚠️ Lokal AI hozir mavjud emas. Keyinroq urinib ko‘ring.",
            "failed": "⚠️ AI javob bera olmadi. Keyinroq qayta urinib ko‘ring.",
            "ai_note": "Lokal model: {model}",
        }
    if lang == "ru":
        return {
            "greeting": (
                "💬 AI Chat\n\n"
                "Здравствуйте 👋\n"
                "Я локальный AI-помощник. Отправляйте вопросы обычным сообщением.\n"
                "Буду отвечать дружелюбно и по смыслу."
            ),
            "thinking": "🤖 AI думает...",
            "done": "✅ Ответ готов.",
            "cancelled": "AI chat закрыт. Можете выбрать другой раздел.",
            "expired": "Сессия AI chat истекла. Снова откройте её через меню `AI Tools` ниже.",
            "empty": "Отправьте сообщение.",
            "unavailable": "⚠️ Локальный AI сейчас недоступен. Попробуйте позже.",
            "failed": "⚠️ AI не смог ответить. Попробуйте еще раз позже.",
            "ai_note": "Локальная модель: {model}",
        }
    return {
        "greeting": (
            "💬 AI Chat\n\n"
            "Hello 👋\n"
            "I'm your local AI assistant. Send your questions as normal messages.\n"
            "I’ll reply in a friendly, context-aware way."
        ),
        "thinking": "🤖 AI is thinking...",
        "done": "✅ Reply ready.",
        "cancelled": "AI chat closed. You can choose another menu section.",
        "expired": "AI chat session expired. Please open it again from the `AI Tools` menu below.",
        "empty": "Please send a message.",
        "unavailable": "⚠️ Local AI is unavailable right now. Please try again later.",
        "failed": "⚠️ AI could not reply. Please try again later.",
        "ai_note": "Local model: {model}",
    }


def _ai_chat_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_AI_CHAT_SESSION_KEY, None)


def _ai_chat_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_AI_CHAT_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _ai_chat_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_AI_CHAT_SESSION_KEY] = dict(session)


def _ai_chat_trim_history(history: list[dict], max_items: int = 12, max_chars: int = 7000) -> list[dict]:
    items = []
    total = 0
    for item in reversed(list(history or [])):
        role = str(item.get("role") or "user")
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        content = content[:1200]
        item_cost = len(content) + 16
        if len(items) >= max_items or (items and total + item_cost > max_chars):
            break
        items.append({"role": role, "content": content})
        total += item_cost
    items.reverse()
    return items


def _ai_chat_build_prompt(history: list[dict], user_text: str, reply_lang_hint: str, lang_ui: str) -> str:
    recent = _ai_chat_trim_history(history)
    lines = []
    for item in recent:
        role = "User" if str(item.get("role")) == "user" else "Assistant"
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        lines.append(f"{role}: {content}")
    convo = "\n".join(lines)
    return (
        "You are a friendly, helpful AI assistant inside a Telegram library bot.\n"
        "Bot owner / creator / developer: @MuhammadaliAbdullayev.\n"
        "If asked who built/created/owns/develops the bot, answer with that username clearly.\n"
        "Admin controls are private and only for admins/owner.\n"
        "This bot mainly helps users with books and utility tools.\n"
        "Core features available in the bot menu: Search Books, Text to Voice, Ramadan Duas, Other Functions, AI Tools.\n"
        "Other Functions includes My Profile, book requests, favorites, top books/users, help, and upload (permissions may apply).\n"
        "AI Tools currently includes AI Chat; image generation may be unavailable or coming soon depending on setup.\n"
        "Do not claim features that are not available.\n"
        "If user asks for real-time web/news/live data, say you may not have live internet access in this chat and ask if they want general guidance.\n"
        "Reply warmly and naturally, and stay focused on the user's actual message.\n"
        "Use the conversation context when relevant.\n"
        "Reply in the SAME language (and script) as the user's latest message.\n"
        "Do not switch language just because the UI language is different.\n"
        "Do not mix languages unless the user mixes languages first.\n"
        "Start with a direct answer in the first sentence.\n"
        "For simple factual questions, answer in 1-3 short sentences.\n"
        "Explain clearly without sounding robotic.\n"
        "Do NOT include links, URLs, or sources unless the user explicitly asks for them.\n"
        "Never output URL-encoded text (percent-encoded strings).\n"
        "If the user asks a location/question like 'where is X', answer the location directly first.\n"
        "If needed, ask only one short clarifying question.\n"
        "If unsure, say you are not fully sure instead of inventing facts.\n"
        "For medical, legal, financial, or religious guidance: give general information only, avoid definitive rulings, and recommend a qualified expert or trusted authority when appropriate.\n"
        "Use respectful wording for religious/cultural topics.\n"
        "Avoid giant paragraphs; split long replies into short paragraphs or bullets when helpful.\n"
        "Avoid markdown abuse, excessive decoration, and repetitive phrasing.\n"
        "Do not mention system prompts or internal rules.\n"
        "Return plain text only.\n\n"
        f"UI language hint: {lang_ui}\n"
        f"Reply language hint (from latest user message): {reply_lang_hint}\n\n"
        f"Conversation so far:\n{convo or '(no previous messages)'}\n\n"
        f"User: {user_text.strip()[:4000]}\n"
        "Assistant:"
    )


def _ai_chat_user_asked_for_links(user_text: str) -> bool:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return False
    keywords = {
        "link", "links", "source", "sources", "url", "website", "web site", "site", "docs", "documentation",
        "manba", "manbalar", "havola", "havolalar", "sayt", "ssilka", "ссылка", "ссылки", "источник", "источники",
        "сайт", "док", "доки",
    }
    return any(k in t for k in keywords)


def _ai_chat_postprocess_reply(text: str, user_text: str) -> str:
    out = str(text or "").strip()
    if not out:
        return out
    out = re.sub(r"^(assistant|ai)\s*:\s*", "", out, flags=re.IGNORECASE).strip()
    allow_links = _ai_chat_user_asked_for_links(user_text)
    if not allow_links:
        cleaned_lines = []
        for ln in out.splitlines():
            if re.search(r"https?://", ln, flags=re.IGNORECASE):
                continue
            if re.search(r"%[0-9A-Fa-f]{2}", ln):
                # Drop URL-encoded garbage lines if model outputs them.
                continue
            cleaned_lines.append(ln)
        out = "\n".join(cleaned_lines).strip()
        out = re.sub(r"(quyidagi manzillarda|more info.*)$", "", out, flags=re.IGNORECASE | re.MULTILINE).strip()
    # Reduce markdown noise / decorative formatting.
    out = re.sub(r"^[#>\-\*\s]{0,3}(assistant|answer)\s*[:\-]\s*", "", out, flags=re.IGNORECASE | re.MULTILINE)
    out = re.sub(r"`{3,}.*?`{3,}", "", out, flags=re.DOTALL)
    out = re.sub(r"\*{3,}", "**", out)
    out = re.sub(r"_{3,}", "__", out)
    # Split giant paragraphs into smaller readable chunks.
    paras = []
    for p in out.split("\n\n"):
        p = p.strip()
        if not p:
            continue
        if len(p) <= 420:
            paras.append(p)
            continue
        parts = re.split(r"(?<=[\.\!\?])\s+", p)
        cur = []
        cur_len = 0
        for part in parts:
            if not part:
                continue
            if cur and cur_len + 1 + len(part) > 320:
                paras.append(" ".join(cur).strip())
                cur = [part]
                cur_len = len(part)
            else:
                cur.append(part)
                cur_len += (len(part) + (1 if cur_len else 0))
        if cur:
            paras.append(" ".join(cur).strip())
    out = "\n\n".join(paras).strip()
    out = re.sub(r"\n{4,}", "\n\n\n", out).strip()
    if len(out) > 3900:
        out = out[:3890].rstrip() + "..."
    return out


def _ai_chat_guess_reply_lang(text: str, ui_lang: str = "en") -> str:
    s = str(text or "")
    if not s.strip():
        return ui_lang if ui_lang in {"uz", "ru", "en"} else "en"
    if any("\u0400" <= ch <= "\u04FF" for ch in s):
        return "ru"

    lower = s.lower()

    # Strong Uzbek markers first.
    strong_uz_markers = ("o‘", "g‘", "o'z", "g'", "oʻ", "gʻ", "oʼ", "gʼ", "ʻ", "ʼ")
    if any(tok in lower for tok in strong_uz_markers):
        return "uz"

    uzbek_words = (
        "salom", "assalomu", "alaykum", "kitob", "rahmat", "iltimos", "menga", "siz", "biz",
        "uchun", "bilan", "qanday", "nima", "nega", "qayerda", "qachon", "kim", "joylashgan",
        "davlat", "shahar", "yordam", "bering", "kerak", "ha", "yoq", "yo'q", "emas",
        "javob", "savol", "bolsa", "bo'lsa", "bo‘lsa", "ozing", "o'zing", "o‘zing", "gap", "gapingga",
        "chunding", "chundingmi", "dokonda", "daftar", "ruchka", "istaydi", "qoldi",
    )
    english_words = (
        "hello", "hi", "please", "thanks", "thank", "what", "where", "when", "why", "how", "who",
        "is", "are", "can", "could", "would", "tell", "explain", "book", "books", "search", "find",
        "pdf", "voice", "text", "chat", "bot", "help", "creator", "owner", "developer",
    )
    uz_hits = sum(1 for w in uzbek_words if re.search(rf"\b{re.escape(w)}\b", lower))
    en_hits = sum(1 for w in english_words if re.search(rf"\b{re.escape(w)}\b", lower))

    has_uz_digraph = any(tok in lower for tok in ("sh", "ch", "ng"))
    has_uz_letters = any(ch in lower for ch in ("q", "x"))
    if uz_hits >= 2:
        return "uz"
    if uz_hits >= 1 and (has_uz_digraph or has_uz_letters):
        return "uz"

    # Colloquial Uzbek (Latin, no apostrophes) heuristic using common suffixes.
    latin_tokens = re.findall(r"[a-z]+", lower)
    uz_suffixes = ("mi", "chi", "ga", "da", "dan", "ning", "lik", "lar", "lari", "ni", "siz")
    suffix_hits = sum(1 for tok in latin_tokens if len(tok) >= 4 and tok.endswith(uz_suffixes))
    if suffix_hits >= 2 and en_hits == 0:
        return "uz"

    # English detection for ASCII/Latin text.
    latin_letters = sum(1 for ch in s if ("a" <= ch.lower() <= "z"))
    non_space = sum(1 for ch in s if not ch.isspace())
    latin_ratio = (latin_letters / non_space) if non_space else 0.0
    if en_hits >= 2:
        return "en"
    if en_hits >= 1 and latin_ratio > 0.5 and uz_hits == 0:
        return "en"
    if latin_ratio > 0.85 and uz_hits == 0 and en_hits == 0:
        # Neutral Latin text defaults to English for AI chat (content-first).
        return "en"

    return ui_lang if ui_lang in {"uz", "ru", "en"} else "en"


def _ai_chat_needs_caution_notice(user_text: str) -> tuple[str | None, str | None]:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return None, None
    medical = ("medical", "medicine", "doctor", "diagnosis", "symptom", "treatment", "dori", "shifokor", "kasal", "симптом", "лечение", "врач")
    legal = ("legal", "law", "lawsuit", "contract", "court", "sud", "qonun", "advokat", "закон", "суд", "договор", "юрист")
    financial = ("financial", "investment", "stock", "crypto", "tax", "debt", "invest", "aksiy", "soliq", "qarz", "финанс", "инвест", "налог", "долг")
    religious = ("halol", "halal", "harom", "haram", "fatwa", "fiqh", "shariat", "sharia", "duo", "dua", "ислам", "фетва", "шариат", "религ")
    religious_word_only = ("din",)
    if any(k in t for k in medical):
        return "medical", t
    if any(k in t for k in legal):
        return "legal", t
    if any(k in t for k in financial):
        return "financial", t
    if any(k in t for k in religious):
        return "religious", t
    if any(re.search(rf"\\b{re.escape(k)}\\b", t) for k in religious_word_only):
        return "religious", t
    return None, None


def _ai_chat_add_caution_notice(answer: str, user_text: str, reply_lang_hint: str) -> str:
    topic, _ = _ai_chat_needs_caution_notice(user_text)
    if not topic:
        return answer
    out = str(answer or "").strip()
    if not out:
        return out
    lower_out = out.lower()
    if any(k in lower_out for k in ("not sure", "i may be wrong", "general info", "professional", "mutaxassis", "umumiy ma", "специалист", "общая информация")):
        return out
    note_lang = _ai_chat_guess_reply_lang(out, reply_lang_hint if reply_lang_hint in {"uz", "ru", "en"} else "en")
    if note_lang == "ru":
        note_map = {
            "medical": "Это общая информация, а не медицинский диагноз. Для точного совета лучше обратиться к врачу.",
            "legal": "Это общая информация, а не юридическая консультация. Для точного решения лучше обратиться к юристу.",
            "financial": "Это общая информация, а не финансовая рекомендация. Перед решением лучше проконсультироваться со специалистом.",
            "religious": "По религиозным вопросам лучше уточнять у доверенного знающего учёного/имама. Я могу дать только общую информацию.",
        }
    elif note_lang == "uz":
        note_map = {
            "medical": "Bu umumiy ma'lumot, tibbiy tashxis emas. Aniq maslahat uchun shifokorga murojaat qiling.",
            "legal": "Bu umumiy ma'lumot, yuridik maslahat emas. Aniq yechim uchun yurist bilan maslahat qiling.",
            "financial": "Bu umumiy ma'lumot, moliyaviy tavsiya emas. Qaror qilishdan oldin mutaxassis bilan maslahat qiling.",
            "religious": "Diniy masalalarda ishonchli ulamo/imomdan aniqlashtirish tavsiya etiladi. Men umumiy ma'lumot bera olaman.",
        }
    else:
        note_map = {
            "medical": "This is general information, not a medical diagnosis. For accurate advice, please consult a doctor.",
            "legal": "This is general information, not legal advice. For a reliable decision, please consult a lawyer.",
            "financial": "This is general information, not financial advice. Please consult a qualified professional before deciding.",
            "religious": "For religious questions, it is best to confirm with a trusted scholar/imam. I can only provide general information.",
        }
    note = note_map.get(topic)
    if not note:
        return out
    return f"{out}\n\n{note}"


_AI_TOOL_MODE_SESSION_KEY = "ai_tool_mode_session"
_AI_TOOL_MODE_KEYS = ("translator", "grammar", "email", "quiz", "music", "song")
_AI_TRANSLATOR_INLINE_ONLY_TEXT = "\u2063"
_NLLB_TRANSLATOR_CACHE: dict[tuple[str, str, bool], dict] = {}
_NLLB_TRANSLATOR_LOCK = Lock()
_AI_QUIZ_COUNT_CHOICES = (3, 5, 10)
_AI_QUIZ_INTERVAL_CHOICES = (0, 3, 5, 10)
_AI_MUSIC_DURATION_CHOICES = (8, 15, 30)
_AI_MUSIC_STYLE_CHOICES = ("lofi", "romantic", "calm", "epic")
_AI_MUSIC_STYLE_HINTS = {
    "lofi": "warm lo-fi hip hop, mellow groove, chill vinyl texture, no vocals",
    "romantic": "romantic cinematic instrumental, gentle strings and piano, emotional, no vocals",
    "calm": "calm ambient instrumental, soft pads, meditative, no vocals",
    "epic": "epic cinematic instrumental, powerful drums, dramatic tension, no vocals",
}


def _ai_tool_mode_clear_session(context: ContextTypes.DEFAULT_TYPE):
    raw = context.user_data.get(_AI_TOOL_MODE_SESSION_KEY)
    if isinstance(raw, dict):
        mode = str(raw.get("mode") or "")
        if mode == "quiz":
            token = str(raw.get("quiz_generation_token") or "").strip()
            if token:
                _ai_quiz_cancel_tokens(context.application.bot_data).add(token)
        elif mode == "music":
            token = str(raw.get("music_generation_token") or "").strip()
            if token:
                _ai_music_cancel_tokens(context.application.bot_data).add(token)
    context.user_data.pop(_AI_TOOL_MODE_SESSION_KEY, None)


def _ai_tool_mode_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_AI_TOOL_MODE_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _ai_tool_mode_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_AI_TOOL_MODE_SESSION_KEY] = dict(session)


def _ai_tool_mode_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "title_translator": "🌐 AI tarjimon",
            "title_grammar": "✍️ AI grammatika tuzatish",
            "title_email": "📧 AI xat yozish",
            "title_quiz": "📝 AI quiz generator",
            "title_music": "🎵 AI musiqa generator",
            "title_song": "🎤 AI qo‘shiq generator",
            "quiz_prompt_name": "📝 Quiz test nomini yuboring.\nMasalan: English Beginner Grammar Test 1",
            "quiz_name_saved": "✅ Test nomi saqlandi: {name}",
            "quiz_send_name_first": "Avval test nomini yuboring.",
            "prompt_translator": (
                "🌐 AI tarjimon\n\n"
                "Pastdagi tugmalardan target tilni tanlang, keyin matn yuboring.\n"
                "Tillar: uz, ru, en\n\n"
                "Shortcut ham ishlaydi:\n"
                "en: Assalomu alaykum\n"
                "uz>en: Assalomu alaykum"
            ),
            "translator_setup_text": "🌐 AI tarjimon\n\n1) 🎯 Target til: {target}\n2) ✍️ Tarjima qilinadigan matnni yuboring.\n\nMasalan: uz>en: Assalomu alaykum",
            "prompt_grammar": (
                "✍️ AI grammatika tuzatish\n\n"
                "Matn yuboring. Men:\n"
                "• imlo va grammatika xatolarini tuzataman\n"
                "• ma'noni saqlayman\n"
                "• matn tilini o‘zgartirmayman\n\n"
                "Masalan:\n"
                "• `men ertaga boramanmi`\n"
                "• `I has a question about this book`"
            ),
            "prompt_email": "📧 AI xat yozish\n\nNima haqida xat/letter kerakligini yozing.\nMasalan: ishga ariza, uzr xati, rasmiy so‘rov.",
            "prompt_quiz": "📝 AI Quiz Generator",
            "prompt_music": "🎵 AI musiqa generator\n🎚️ Rejimni tanlang\n🎨 Orqa fon uslubini tanlang\n⏱️ Davomiylikni tanlang\n✍️ Keyin prompt yuboring",
            "prompt_music_lyrics": "🎵 AI musiqa generator\n🎤 Rejim: Lyrics Music\n🎨 Orqa fon uslubini tanlang\n⏱️ Davomiylikni tanlang\n✍️ So‘zlar/matn yuboring",
            "prompt_song": "🎤 AI qo‘shiq generator\n\nSo‘zlar yoki mavzuni yuboring. Men shu so‘zlardan foydalangan holda qo‘shiq matni yozaman.\nMasalan: sevgi, yomg‘ir, tun, sog‘inch",
            "thinking": "🤖 AI ishlayapti...",
            "quiz_generating": "🧠 Quiz savollari tayyorlanmoqda...",
            "music_generating": "🎵 Musiqa yaratilmoqda... Bu biroz vaqt olishi mumkin.",
            "music_generating_progress": "🎵 Musiqa yaratilmoqda {spinner}\n⏱️ O‘tgan vaqt: {elapsed}s | Tanlangan davomiylik: {duration}s\n📌 Iltimos kuting, ayniqsa birinchi ishga tushirishda model yuklanishi uzoqroq bo‘lishi mumkin.",
            "music_prompt_hint": "Musiqa prompt yuboring (instrumental tavsiya).",
            "music_lyrics_shortcut": "🎤 Lyrics",
            "music_lyrics_switching": "🎤 Lyrics generatorga o‘tilmoqda...",
            "music_choose_duration": "⏱️ Musiqa davomiyligini tanlang (sekund), keyin prompt yuboring.",
            "music_duration_set": "✅ Davomiylik tanlandi: {seconds} sek. Endi prompt yuboring.",
            "music_running_hint": "🎵 Musiqa hali yaratilmoqda. Kuting yoki AI toolni almashtiring.",
            "music_done": "✅ Musiqa tayyor.",
            "music_caption": "🎵 AI musiqa (instrumental)\n⏱️ {seconds}s\n📝 {prompt}",
            "music_setup_text": "🎵 AI musiqa generator\n\n1) 🎨 Uslub: {style}\n2) ⏱️ Davomiylik: {seconds} soniya\n3) ✍️ Endi prompt yuboring.",
            "music_failed": "⚠️ Musiqa yaratib bo‘lmadi. Promptni soddalashtirib qayta urinib ko‘ring.",
            "music_mode_sound": "🎵 Sound Music",
            "music_mode_lyrics": "🎤 Lyrics Music",
            "music_mode_selected_sound": "✅ Rejim: Sound Music. Endi davomiylikni tanlab, prompt yuboring.",
            "music_mode_selected_lyrics": "✅ Rejim: Lyrics Music. Endi uslub/davomiylikni tanlab, so‘zlar yoki matn yuboring.",
            "music_style_choose_first": "🎨 Avval orqa fon uslubini tanlang.",
            "music_style_set": "✅ Uslub tanlandi: {style}. Endi matn/prompt yuboring.",
            "music_confirm_btn": "✅ Davom etish",
            "music_confirm_first": "✅ Davom etish tugmasini bosing, keyin matn yuboring.",
            "music_ready_for_text": "✅ Tayyor. Endi matn/prompt yuboring.",
            "music_style_lofi": "🌙 Lofi",
            "music_style_romantic": "💖 Romantik",
            "music_style_calm": "😌 Sokin",
            "music_style_epic": "⚡ Epik",
            "song_generating": "🎤 Qo‘shiq matni yozilmoqda...",
            "song_failed": "⚠️ Qo‘shiq matnini yaratib bo‘lmadi. So‘zlarni aniqroq yozib qayta urinib ko‘ring.",
            "song_empty": "So‘zlar yoki mavzuni yuboring.",
            "quiz_choose_source": "📝 Quiz testni nimadan yaratamiz?",
            "quiz_source_selected": "✅ Manba tanlandi: {source}\nEndi {next_step}.",
            "quiz_source_prompt": "prompt",
            "quiz_source_topic": "topic",
            "quiz_prompt_topic": "Quiz mavzusini yuboring.\nMasalan: English grammar beginner",
            "quiz_prompt_prompt": "Quiz uchun prompt yuboring.\nMasalan: Beginner English grammar test with plural nouns and tenses",
            "quiz_input_saved": "✅ Qabul qilindi. Endi savollar sonini tanlang.",
            "quiz_choose_count": "🔢 Nechta savol kerak?\nManba: {topic}",
            "quiz_choose_interval": "⏱️ Savollar orasidagi vaqtni tanlang (sekund).",
            "quiz_setup_text": "📝 AI quiz generator\n\n1) 🔢 Savollar soni: {count}\n2) ⏱️ Savollar oralig‘i: {interval} sek\n3) ✍️ Endi quiz mavzusini yuboring.",
            "quiz_invalid_interval": "⚠️ Vaqt 0-120 sekund oralig‘ida bo‘lsin.",
            "quiz_interval_set": "✅ Interval: {seconds} sek.",
            "quiz_delay_note": "⏱️ Savollar oralig‘i: {seconds} sek.",
            "quiz_invalid_count": "⚠️ Savollar soni 1-10 oralig‘ida bo‘lsin.",
            "quiz_send_topic_first": "Avval quiz mavzusini yuboring.",
            "quiz_ready_intro": "✅ Quiz boshlandi: {topic}\n📝 Savollar soni: {count}\n👇 Pastdagi quiz poll'larni javob bering.",
            "quiz_card_ready": "✅ Quiz test tayyor: {topic}",
            "quiz_card_caption": (
                "📝 <b>AI quiz testi</b>\n"
                "🏷️ Test nomi: <b>{name}</b>\n"
                "📚 Mavzu: <b>{topic}</b>\n"
                "❓ Savollar: <b>{count}</b>\n"
                "⏱️ Taxminiy vaqt: <b>{minutes} daqiqa</b>\n"
                "⏳ Savollar oralig‘i: <b>{interval}s</b>\n\n"
                "Pastdagi tugmalardan testni boshlang yoki ulashing."
            ),
            "quiz_btn_start": "▶️ Testni boshlash",
            "quiz_btn_group": "👥 Guruhda testni boshlash",
            "quiz_btn_open_web": "🌐 Web App’da ochish",
            "quiz_btn_share": "📤 Testni ulashish",
            "quiz_share_text": "📝 AI quiz testi: {topic}\n❓ Savollar: {count}\n👇 Testni boshlash uchun botga kiring.",
            "quiz_group_hint": "Bu test kartasini guruhga forward qiling va u yerda `Testni boshlash` tugmasini bosing.",
            "quiz_share_hint": "Testni ulashish uchun shu test kartasini boshqa chatga forward qiling.",
            "quiz_waiting_start_hint": "📝 Quiz test tayyor. Pastdagi `Testni boshlash` tugmasini bosing.",
            "quiz_card_expired": "⌛ Quiz test muddati tugagan. Yangisini yarating.",
            "quiz_running_hint": "📝 Quiz davom etmoqda. Poll savollariga javob bering yoki boshqa AI toolni tanlang.",
            "quiz_final": "🏁 Quiz tugadi!\n✅ To‘g‘ri javoblar: {correct}/{total} ({percent}%)",
            "quiz_failed": "⚠️ Quiz savollarini yaratib bo‘lmadi. Mavzuni aniqroq yozib, qayta urinib ko‘ring.",
            "my_quiz_empty": "📝 Sizda hali saqlangan quiz testlar yo‘q.",
            "my_quiz_title": "📝 Mening quiz testlarim (sahifa {page}/{pages}, jami {total})",
            "my_quiz_open_prompt": "👇 Testni tanlang:",
            "my_quiz_detail": "📝 <b>{name}</b>\n📚 Manba: {source_kind}\n📄 Matn: {source_preview}\n❓ Savollar: {count}\n⏱️ Interval: {interval}s",
            "my_quiz_btn_start": "▶️ Testni boshlash",
            "my_quiz_btn_group": "👥 Guruhda boshlash",
            "my_quiz_btn_open_web": "🌐 Web App",
            "my_quiz_btn_share": "📤 Ulashish",
            "my_quiz_btn_delete": "🗑️ O‘chirish",
            "my_quiz_btn_back": "⬅️ Ro‘yxatga qaytish",
            "my_quiz_deleted": "✅ Quiz test o‘chirildi.",
            "my_quiz_not_found": "⚠️ Quiz test topilmadi.",
            "done": "✅ Tayyor.",
            "cancelled": "AI vosita yopildi. Boshqa bo‘limni tanlashingiz mumkin.",
            "expired": "AI vosita sessiyasi tugadi. `AI Tools` bo‘limidan qayta oching.",
            "empty": "Xabar yuboring.",
            "too_long": "Matn juda uzun. Iltimos, qisqaroq yuboring.",
            "unavailable": "⚠️ Lokal AI hozir mavjud emas. Keyinroq urinib ko‘ring.",
            "failed": "⚠️ AI ishlov bera olmadi. Keyinroq qayta urinib ko‘ring.",
            "translator_default_target_note": "Target til ko‘rsatilmagan, standart target ishlatildi: {target}",
            "translator_choose_target_first": "Avval pastdagi tugmalardan target tilni tanlang (🇺🇿 / 🇷🇺 / 🇬🇧), keyin matn yuboring.",
            "translator_target_set": "🎯 Target til tanlandi: {target}. Endi matn yuboring.",
            "translator_pick_target_short": "🎯 Target tilni tanlang",
            "lang_uz": "O‘zbek",
            "lang_ru": "Rus",
            "lang_en": "Ingliz",
            "quiz_count_btn_prefix": "Savollar",
            "quiz_interval_btn_prefix": "Interval",
            "music_duration_btn_prefix": "Musiqa",
            "music_duration_quick": "⚡ Tez",
            "music_duration_standard": "🎧 Standart",
            "music_duration_long": "🎼 Uzun",
        }
    if lang == "ru":
        return {
            "title_translator": "🌐 AI переводчик",
            "title_grammar": "✍️ AI исправление грамматики",
            "title_email": "📧 AI письма",
            "title_quiz": "📝 AI генератор викторины",
            "title_music": "🎵 AI генератор музыки",
            "title_song": "🎤 AI генератор песни",
            "quiz_prompt_name": "📝 Отправьте название теста.\nНапример: English Beginner Grammar Test 1",
            "quiz_name_saved": "✅ Название теста сохранено: {name}",
            "quiz_send_name_first": "Сначала отправьте название теста.",
            "prompt_translator": (
                "🌐 AI переводчик\n\n"
                "Выберите целевой язык кнопками ниже, затем отправьте текст.\n"
                "Языки: uz, ru, en\n\n"
                "Shortcut тоже работает:\n"
                "en: Assalomu alaykum\n"
                "uz>en: Assalomu alaykum"
            ),
            "translator_setup_text": "🌐 AI переводчик\n\n1) 🎯 Целевой язык: {target}\n2) ✍️ Отправьте текст для перевода.\n\nПример: uz>en: Assalomu alaykum",
            "prompt_grammar": (
                "✍️ AI исправление грамматики\n\n"
                "Отправьте текст. Я:\n"
                "• исправлю орфографию и грамматику\n"
                "• сохраню смысл\n"
                "• не буду менять язык текста\n\n"
                "Примеры:\n"
                "• `я вчера ходить в магазин`\n"
                "• `I has a question about this book`"
            ),
            "prompt_email": "📧 AI письма\n\nНапишите, какое письмо/сообщение нужно составить.\nНапример: официальное письмо, просьба, извинение.",
            "prompt_quiz": "📝 AI Quiz Generator",
            "prompt_music": "🎵 AI генератор музыки\n🎚️ Выберите режим\n🎨 Выберите стиль фона\n⏱️ Выберите длительность\n✍️ Затем отправьте prompt",
            "prompt_music_lyrics": "🎵 AI генератор музыки\n🎤 Режим: Lyrics Music\n🎨 Выберите стиль фона\n⏱️ Выберите длительность\n✍️ Отправьте слова/текст",
            "prompt_song": "🎤 AI генератор песни\n\nОтправьте слова или тему. Я напишу текст песни, используя эти слова.\nНапример: любовь, дождь, ночь, тоска",
            "thinking": "🤖 AI обрабатывает...",
            "quiz_generating": "🧠 Готовлю вопросы викторины...",
            "music_generating": "🎵 Генерирую музыку... Это может занять некоторое время.",
            "music_generating_progress": "🎵 Генерирую музыку {spinner}\n⏱️ Прошло: {elapsed}s | Выбрано: {duration}s\n📌 Подождите, при первом запуске загрузка модели может занять больше времени.",
            "music_prompt_hint": "Отправьте prompt для музыки (рекомендуется instrumental).",
            "music_lyrics_shortcut": "🎤 Lyrics",
            "music_lyrics_switching": "🎤 Переключаю на генератор текста песни...",
            "music_choose_duration": "⏱️ Выберите длительность музыки (секунды), затем отправьте prompt.",
            "music_duration_set": "✅ Длительность выбрана: {seconds} сек. Теперь отправьте prompt.",
            "music_running_hint": "🎵 Музыка ещё генерируется. Подождите или смените AI-инструмент.",
            "music_done": "✅ Музыка готова.",
            "music_caption": "🎵 AI музыка (инструментал)\n⏱️ {seconds}s\n📝 {prompt}",
            "music_setup_text": "🎵 AI генератор музыки\n\n1) 🎨 Стиль: {style}\n2) ⏱️ Длительность: {seconds} сек\n3) ✍️ Теперь отправьте prompt.",
            "music_failed": "⚠️ Не удалось сгенерировать музыку. Попробуйте упростить prompt.",
            "music_mode_sound": "🎵 Sound Music",
            "music_mode_lyrics": "🎤 Lyrics Music",
            "music_mode_selected_sound": "✅ Режим: Sound Music. Теперь выберите длительность и отправьте prompt.",
            "music_mode_selected_lyrics": "✅ Режим: Lyrics Music. Теперь выберите стиль/длительность и отправьте слова или текст.",
            "music_style_choose_first": "🎨 Сначала выберите стиль фона.",
            "music_style_set": "✅ Стиль выбран: {style}. Теперь отправьте текст/prompt.",
            "music_confirm_btn": "✅ Продолжить",
            "music_confirm_first": "✅ Нажмите «Продолжить», затем отправьте текст.",
            "music_ready_for_text": "✅ Готово. Теперь отправьте текст/prompt.",
            "music_style_lofi": "🌙 Lofi",
            "music_style_romantic": "💖 Romantic",
            "music_style_calm": "😌 Calm",
            "music_style_epic": "⚡ Epic",
            "song_generating": "🎤 Пишу текст песни...",
            "song_failed": "⚠️ Не удалось сгенерировать текст песни. Попробуйте уточнить слова/тему.",
            "song_empty": "Отправьте слова или тему.",
            "quiz_choose_source": "📝 Из чего создать тест-викторину?",
            "quiz_source_selected": "✅ Источник выбран: {source}\nТеперь отправьте {next_step}.",
            "quiz_source_prompt": "prompt",
            "quiz_source_topic": "topic",
            "quiz_prompt_topic": "Отправьте тему викторины.\nНапример: English grammar beginner",
            "quiz_prompt_prompt": "Отправьте prompt для теста.\nНапример: Beginner English grammar test with plural nouns and tenses",
            "quiz_input_saved": "✅ Получено. Теперь выберите количество вопросов.",
            "quiz_choose_count": "🔢 Сколько вопросов нужно?\nИсточник: {topic}",
            "quiz_choose_interval": "⏱️ Выберите интервал между вопросами (секунды).",
            "quiz_setup_text": "📝 AI Quiz Generator\n\n1) 🔢 Количество вопросов: {count}\n2) ⏱️ Интервал между вопросами: {interval} сек\n3) ✍️ Теперь отправьте тему викторины.",
            "quiz_invalid_interval": "⚠️ Интервал должен быть от 0 до 120 секунд.",
            "quiz_interval_set": "✅ Интервал: {seconds} сек.",
            "quiz_delay_note": "⏱️ Интервал между вопросами: {seconds} сек.",
            "quiz_invalid_count": "⚠️ Количество вопросов должно быть от 1 до 10.",
            "quiz_send_topic_first": "Сначала отправьте тему викторины.",
            "quiz_ready_intro": "✅ Викторина запущена: {topic}\n📝 Вопросов: {count}\n👇 Ответьте на quiz-пулы ниже.",
            "quiz_card_ready": "✅ Тест-викторина готов: {topic}",
            "quiz_card_caption": (
                "📝 <b>AI тест-викторина</b>\n"
                "🏷️ Название: <b>{name}</b>\n"
                "📚 Тема: <b>{topic}</b>\n"
                "❓ Вопросов: <b>{count}</b>\n"
                "⏱️ Примерное время: <b>{minutes} мин</b>\n"
                "⏳ Интервал между вопросами: <b>{interval}s</b>\n\n"
                "Запустите или поделитесь тестом кнопками ниже."
            ),
            "quiz_btn_start": "▶️ Начать тест",
            "quiz_btn_group": "👥 Начать в группе",
            "quiz_btn_open_web": "🌐 Открыть в Web App",
            "quiz_btn_share": "📤 Поделиться тестом",
            "quiz_share_text": "📝 AI тест-викторина: {topic}\n❓ Вопросов: {count}\n👇 Откройте бота, чтобы начать тест.",
            "quiz_group_hint": "Перешлите карточку теста в группу и нажмите там `Начать тест`.",
            "quiz_share_hint": "Чтобы поделиться тестом, просто перешлите эту карточку в другой чат.",
            "quiz_waiting_start_hint": "📝 Тест готов. Нажмите кнопку `Начать тест` ниже.",
            "quiz_card_expired": "⌛ Срок действия теста истёк. Сгенерируйте новый.",
            "quiz_running_hint": "📝 Викторина уже идёт. Ответьте на опросы или смените AI инструмент.",
            "quiz_final": "🏁 Викторина завершена!\n✅ Верных ответов: {correct}/{total} ({percent}%)",
            "quiz_failed": "⚠️ Не удалось создать вопросы викторины. Попробуйте более конкретную тему.",
            "my_quiz_empty": "📝 У вас пока нет сохранённых quiz-тестов.",
            "my_quiz_title": "📝 Мои quiz-тесты (страница {page}/{pages}, всего {total})",
            "my_quiz_open_prompt": "👇 Выберите тест:",
            "my_quiz_detail": "📝 <b>{name}</b>\n📚 Источник: {source_kind}\n📄 Текст: {source_preview}\n❓ Вопросов: {count}\n⏱️ Интервал: {interval}s",
            "my_quiz_btn_start": "▶️ Начать тест",
            "my_quiz_btn_group": "👥 Начать в группе",
            "my_quiz_btn_open_web": "🌐 Web App",
            "my_quiz_btn_share": "📤 Поделиться",
            "my_quiz_btn_delete": "🗑️ Удалить",
            "my_quiz_btn_back": "⬅️ К списку",
            "my_quiz_deleted": "✅ Тест удалён.",
            "my_quiz_not_found": "⚠️ Тест не найден.",
            "done": "✅ Готово.",
            "cancelled": "AI-инструмент закрыт. Можете выбрать другой раздел.",
            "expired": "Сессия AI-инструмента истекла. Откройте снова через `AI Tools`.",
            "empty": "Отправьте сообщение.",
            "too_long": "Текст слишком длинный. Пожалуйста, сократите его.",
            "unavailable": "⚠️ Локальный AI сейчас недоступен. Попробуйте позже.",
            "failed": "⚠️ AI не смог обработать запрос. Попробуйте позже.",
            "translator_default_target_note": "Целевой язык не указан, использован язык по умолчанию: {target}",
            "translator_choose_target_first": "Сначала выберите целевой язык кнопками ниже (🇺🇿 / 🇷🇺 / 🇬🇧), затем отправьте текст.",
            "translator_target_set": "🎯 Целевой язык выбран: {target}. Теперь отправьте текст.",
            "translator_pick_target_short": "🎯 Выберите целевой язык",
            "lang_uz": "Узбекский",
            "lang_ru": "Русский",
            "lang_en": "Английский",
            "quiz_count_btn_prefix": "Вопросы",
            "quiz_interval_btn_prefix": "Интервал",
            "music_duration_btn_prefix": "Музыка",
            "music_duration_quick": "⚡ Быстро",
            "music_duration_standard": "🎧 Стандарт",
            "music_duration_long": "🎼 Длинно",
        }
    return {
        "title_translator": "🌐 AI Translator",
        "title_grammar": "✍️ AI Grammar Fix",
        "title_email": "📧 AI Email Writer",
        "title_quiz": "📝 AI Quiz Generator",
        "title_music": "🎵 AI Music Generator",
        "title_song": "🎤 AI Song Generator",
        "quiz_prompt_name": "📝 Send the quiz test name.\nExample: English Beginner Grammar Test 1",
        "quiz_name_saved": "✅ Test name saved: {name}",
        "quiz_send_name_first": "Send the test name first.",
        "prompt_translator": (
            "🌐 AI Translator\n\n"
            "Choose the target language with the buttons below, then send text.\n"
            "Languages: uz, ru, en\n\n"
            "Shortcut also works:\n"
            "en: Assalomu alaykum\n"
            "uz>en: Assalomu alaykum"
        ),
        "translator_setup_text": "🌐 AI Translator\n\n1) 🎯 Target language: {target}\n2) ✍️ Send text to translate.\n\nExample: uz>en: Assalomu alaykum",
        "prompt_grammar": (
            "✍️ AI Grammar Fix\n\n"
            "Send text. I will:\n"
            "• fix grammar and spelling\n"
            "• keep the meaning\n"
            "• keep the same language\n\n"
            "Examples:\n"
            "• `I has a question about this book`\n"
            "• `we was waiting for the bus yesterday`"
        ),
        "prompt_email": "📧 AI Email Writer\n\nDescribe what email/letter you need.\nExample: job application, apology email, formal request.",
        "prompt_quiz": "📝 AI Quiz Generator",
        "prompt_music": "🎵 AI Music Generator\n🎚️ Choose mode\n🎨 Choose background style\n⏱️ Choose duration\n✍️ Then send a prompt",
        "prompt_music_lyrics": "🎵 AI Music Generator\n🎤 Mode: Lyrics Music\n🎨 Choose background style\n⏱️ Choose duration\n✍️ Send words/text",
        "prompt_song": "🎤 AI Song Generator\n\nSend words or a theme. I will write song lyrics using those words.\nExample: love, rain, night, longing",
        "thinking": "🤖 AI is working...",
        "quiz_generating": "🧠 Generating quiz questions...",
        "music_generating": "🎵 Generating music... This may take a while.",
        "music_generating_progress": "🎵 Generating music {spinner}\n⏱️ Elapsed: {elapsed}s | Selected duration: {duration}s\n📌 Please wait. First-time model loading can take longer.",
        "music_prompt_hint": "Send a music prompt (instrumental is recommended).",
        "music_lyrics_shortcut": "🎤 Lyrics",
        "music_lyrics_switching": "🎤 Switching to song lyrics generator...",
        "music_choose_duration": "⏱️ Choose music duration (seconds), then send a prompt.",
        "music_duration_set": "✅ Duration selected: {seconds}s. Now send a prompt.",
        "music_running_hint": "🎵 Music is still generating. Please wait or change the AI tool.",
        "music_done": "✅ Music ready.",
        "music_caption": "🎵 AI music (instrumental)\n⏱️ {seconds}s\n📝 {prompt}",
        "music_setup_text": "🎵 AI Music Generator\n\n1) 🎨 Style: {style}\n2) ⏱️ Duration: {seconds}s\n3) ✍️ Now send your prompt text.",
        "music_failed": "⚠️ Music generation failed. Try a simpler prompt.",
        "music_mode_sound": "🎵 Sound Music",
        "music_mode_lyrics": "🎤 Lyrics Music",
        "music_mode_selected_sound": "✅ Mode: Sound Music. Now choose duration and send a prompt.",
        "music_mode_selected_lyrics": "✅ Mode: Lyrics Music. Now choose style/duration and send words or text.",
        "music_style_choose_first": "🎨 Choose a background style first.",
        "music_style_set": "✅ Style selected: {style}. Now send text/prompt.",
        "music_confirm_btn": "✅ Continue",
        "music_confirm_first": "✅ Tap Continue, then send your text.",
        "music_ready_for_text": "✅ Ready. Now send your text/prompt.",
        "music_style_lofi": "🌙 Lofi",
        "music_style_romantic": "💖 Romantic",
        "music_style_calm": "😌 Calm",
        "music_style_epic": "⚡ Epic",
        "song_generating": "🎤 Writing song lyrics...",
        "song_failed": "⚠️ I couldn't generate song lyrics. Try clearer words/theme.",
        "song_empty": "Send words or a theme.",
        "quiz_choose_source": "📝 What should I build the quiz test from?",
        "quiz_source_selected": "✅ Source selected: {source}\nNow send {next_step}.",
        "quiz_source_prompt": "prompt",
        "quiz_source_topic": "topic",
        "quiz_prompt_topic": "Send a quiz topic.\nExample: English grammar beginner",
        "quiz_prompt_prompt": "Send a quiz prompt.\nExample: Beginner English grammar test with plural nouns and tenses",
        "quiz_input_saved": "✅ Received. Now choose the number of questions.",
        "quiz_choose_count": "🔢 How many questions do you want?\nSource: {topic}",
        "quiz_choose_interval": "⏱️ Choose the time between questions (seconds).",
        "quiz_setup_text": "📝 AI Quiz Generator\n\n1) 🔢 Questions count: {count}\n2) ⏱️ Interval between questions: {interval}s\n3) ✍️ Now send the quiz topic.",
        "quiz_invalid_interval": "⚠️ Interval must be between 0 and 120 seconds.",
        "quiz_interval_set": "✅ Interval set: {seconds}s.",
        "quiz_delay_note": "⏱️ Time between questions: {seconds}s.",
        "quiz_invalid_count": "⚠️ Number of questions must be between 1 and 10.",
        "quiz_send_topic_first": "Send the quiz topic first.",
        "quiz_ready_intro": "✅ Quiz started: {topic}\n📝 Questions: {count}\n👇 Answer the quiz polls below.",
        "quiz_card_ready": "✅ Quiz test is ready: {topic}",
        "quiz_card_caption": (
            "📝 <b>AI Quiz Test</b>\n"
            "🏷️ Test name: <b>{name}</b>\n"
            "📚 Topic: <b>{topic}</b>\n"
            "❓ Questions: <b>{count}</b>\n"
            "⏱️ Estimated time: <b>{minutes} min</b>\n"
            "⏳ Time between questions: <b>{interval}s</b>\n\n"
            "Use the buttons below to start or share this test."
        ),
        "quiz_btn_start": "▶️ Start Test",
        "quiz_btn_group": "👥 Start in Group",
        "quiz_btn_open_web": "🌐 Open in Web App",
        "quiz_btn_share": "📤 Share Test",
        "quiz_share_text": "📝 AI Quiz Test: {topic}\n❓ Questions: {count}\n👇 Open the bot to start the test.",
        "quiz_group_hint": "Forward this test card to a group and press `Start Test` there.",
        "quiz_share_hint": "To share the test, forward this test card to another chat.",
        "quiz_waiting_start_hint": "📝 Quiz test is ready. Tap `Start Test` below.",
        "quiz_card_expired": "⌛ This quiz test has expired. Please generate a new one.",
        "quiz_running_hint": "📝 A quiz is already running. Answer the polls or change the AI tool.",
        "quiz_final": "🏁 Quiz finished!\n✅ Correct answers: {correct}/{total} ({percent}%)",
        "quiz_failed": "⚠️ I couldn’t generate quiz questions. Try a clearer topic and try again.",
        "my_quiz_empty": "📝 You don’t have any saved quiz tests yet.",
        "my_quiz_title": "📝 My Quiz Tests (page {page}/{pages}, total {total})",
        "my_quiz_open_prompt": "👇 Choose a test:",
        "my_quiz_detail": "📝 <b>{name}</b>\n📚 Source: {source_kind}\n📄 Text: {source_preview}\n❓ Questions: {count}\n⏱️ Interval: {interval}s",
        "my_quiz_btn_start": "▶️ Start Test",
        "my_quiz_btn_group": "👥 Start in Group",
        "my_quiz_btn_open_web": "🌐 Web App",
        "my_quiz_btn_share": "📤 Share",
        "my_quiz_btn_delete": "🗑️ Delete",
        "my_quiz_btn_back": "⬅️ Back to List",
        "my_quiz_deleted": "✅ Quiz test deleted.",
        "my_quiz_not_found": "⚠️ Quiz test not found.",
        "done": "✅ Done.",
        "cancelled": "AI tool closed. You can choose another section.",
        "expired": "AI tool session expired. Please open it again from `AI Tools`.",
        "empty": "Please send a message.",
        "too_long": "Text is too long. Please send a shorter message.",
        "unavailable": "⚠️ Local AI is unavailable right now. Please try again later.",
        "failed": "⚠️ AI could not process the request. Please try again later.",
        "translator_default_target_note": "Target language was not specified, so I used the default target: {target}",
        "translator_choose_target_first": "First choose the target language using the buttons below (🇺🇿 / 🇷🇺 / 🇬🇧), then send your text.",
        "translator_target_set": "🎯 Target language selected: {target}. Now send your text.",
        "translator_pick_target_short": "🎯 Choose target language",
        "lang_uz": "Uzbek",
        "lang_ru": "Russian",
        "lang_en": "English",
        "quiz_count_btn_prefix": "Questions",
        "quiz_interval_btn_prefix": "Interval",
        "music_duration_btn_prefix": "Music",
        "music_duration_quick": "⚡ Quick",
        "music_duration_standard": "🎧 Standard",
        "music_duration_long": "🎼 Long",
    }


def _ai_tool_mode_title(mode: str, lang: str) -> str:
    msgs = _ai_tool_mode_texts(lang)
    return msgs.get(f"title_{mode}", mode.title())


def _ai_tool_mode_prompt(mode: str, lang: str) -> str:
    msgs = _ai_tool_mode_texts(lang)
    return msgs.get(f"prompt_{mode}", msgs.get("empty", "Send a message."))


def _ai_tool_music_prompt_text(lang: str, music_kind: str = "sound") -> str:
    msgs = _ai_tool_mode_texts(lang)
    if (music_kind or "").lower() == "lyrics":
        return msgs.get("prompt_music_lyrics", msgs.get("prompt_song", msgs.get("prompt_music", "🎵 AI Music Generator")))
    return msgs.get("prompt_music", "🎵 AI Music Generator")


def _ai_tool_music_style_label(style_key: str, lang: str) -> str:
    msgs = _ai_tool_mode_texts(lang)
    return msgs.get(f"music_style_{style_key}", style_key.title())


def _ai_tool_music_setup_text(lang: str, style_key: str, duration_s: int) -> str:
    msgs = _ai_tool_mode_texts(lang)
    template = msgs.get(
        "music_setup_text",
        "🎵 AI Music Generator\n\n1) 🎨 Style: {style}\n2) ⏱️ Duration: {seconds}s\n3) ✍️ Now send your prompt text.",
    )
    return template.format(
        style=_ai_tool_music_style_label(style_key, lang),
        seconds=max(5, min(60, int(duration_s or _AI_MUSIC_DURATION_CHOICES[1]))),
    )


def _ai_tool_translator_setup_text(lang: str, target_lang: str) -> str:
    msgs = _ai_tool_mode_texts(lang)
    template = msgs.get(
        "translator_setup_text",
        "🌐 AI Translator\n\n1) 🎯 Target language: {target}\n2) ✍️ Send text to translate.",
    )
    return template.format(target=_ai_tool_lang_label(target_lang, lang))


def _ai_tool_quiz_setup_text(lang: str, count: int, interval_s: int) -> str:
    msgs = _ai_tool_mode_texts(lang)
    template = msgs.get(
        "quiz_setup_text",
        "📝 AI Quiz Generator\n\n1) 🔢 Questions count: {count}\n2) ⏱️ Interval between questions: {interval}s\n3) ✍️ Now send the quiz topic.",
    )
    return template.format(
        count=max(1, min(10, int(count or _AI_QUIZ_COUNT_CHOICES[1]))),
        interval=max(0, min(120, int(interval_s or _AI_QUIZ_INTERVAL_CHOICES[0]))),
    )


def _ai_tool_lang_label(lang_key: str, ui_lang: str) -> str:
    msgs = _ai_tool_mode_texts(ui_lang)
    return msgs.get(f"lang_{lang_key}", lang_key)


def _ai_tool_translator_target_buttons_row(ui_lang: str) -> list[str]:
    msgs = _ai_tool_mode_texts(ui_lang)
    return [
        f"🇺🇿 {msgs.get('lang_uz', 'Uzbek')}",
        f"🇷🇺 {msgs.get('lang_ru', 'Russian')}",
        f"🇬🇧 {msgs.get('lang_en', 'English')}",
    ]


def _ai_tool_translator_target_button_to_lang(text: str, ui_lang: str) -> str | None:
    txt = str(text or "").strip()
    row = _ai_tool_translator_target_buttons_row(ui_lang)
    mapping = {
        row[0]: "uz",
        row[1]: "ru",
        row[2]: "en",
    }
    return mapping.get(txt)


def _ai_tool_translator_target_inline_keyboard(ui_lang: str, selected: str | None = None) -> InlineKeyboardMarkup:
    row = _ai_tool_translator_target_buttons_row(ui_lang)
    selected = (selected or "").lower()
    labels = []
    for lang_key, label in (("uz", row[0]), ("ru", row[1]), ("en", row[2])):
        if lang_key == selected:
            labels.append(f"✅ {label}")
        else:
            labels.append(label)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(labels[0], callback_data="aitool:trgt:uz"),
        InlineKeyboardButton(labels[1], callback_data="aitool:trgt:ru"),
        InlineKeyboardButton(labels[2], callback_data="aitool:trgt:en"),
    ]])


def _ai_tool_parse_target_lang(text: str) -> tuple[str | None, str]:
    raw = str(text or "").strip()
    m = re.match(r"^(uz|ru|en)\s*:\s*(.+)$", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).lower(), m.group(2).strip()
    m = re.match(r"^(?:to|into)\s+(uz|ru|en)\s*[:, -]\s*(.+)$", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).lower(), m.group(2).strip()
    return None, raw


def _ai_tool_parse_translation_langs(text: str) -> tuple[str | None, str | None, str]:
    raw = str(text or "").strip()
    # Explicit source -> target format, e.g. "uz>en: salom" or "ru-en: привет"
    m = re.match(
        r"^(uz|ru|en)\s*(?:>|->|-)\s*(uz|ru|en)\s*:\s*(.+)$",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        src = m.group(1).lower()
        tgt = m.group(2).lower()
        return src, tgt, m.group(3).strip()
    tgt, body = _ai_tool_parse_target_lang(raw)
    return None, tgt, body


def _ai_tool_guess_translation_source_lang(text: str, ui_lang: str, target_lang: str | None) -> str:
    t = str(text or "").strip()
    if not t:
        return "en"
    target_lang = (target_lang or "").lower()

    # Cyrillic is a strong Russian signal for this bot's supported languages.
    if re.search(r"[А-Яа-яЁё]", t):
        return "ru"

    latin_words = re.findall(r"[A-Za-zʻ’'`-]+", t)
    word_count = len(latin_words) if latin_words else len(t.split())
    t_low = t.lower()

    uz_markers = (
        "oʻ", "gʻ", "o‘", "g‘", "o'", "g'", "sh", "ch", "ng", "q", "x",
    )
    uz_common = {
        "salom", "assalomu", "alaykum", "rahmat", "iltimos", "qalaysiz",
        "yaxshi", "tarvuz", "qovun", "qulupnay", "kitob", "men", "sen",
        "siz", "bu", "shu", "emas", "uchun", "bilan", "qayerda",
    }
    en_common = {
        "hello", "hi", "thanks", "please", "book", "where", "what", "how",
        "watermelon", "strawberry", "apple", "and", "the", "is", "are",
    }

    if any(m in t_low for m in uz_markers) or any(w in uz_common for w in re.findall(r"[a-zʻ’'`-]+", t_low)):
        guess = "uz"
    elif any(w in en_common for w in re.findall(r"[a-z'-]+", t_low)):
        guess = "en"
    else:
        # For short Latin words, prefer UI language over generic detector.
        if word_count <= 2 and ui_lang in {"uz", "ru", "en"} and ui_lang != target_lang:
            guess = ui_lang
        else:
            guess = _ai_chat_guess_reply_lang(t, ui_lang)

    return guess if guess in {"uz", "ru", "en"} else "en"


def _ai_tool_translation_output_is_suspicious(src_text: str, out_text: str) -> bool:
    src = str(src_text or "").strip()
    out = str(out_text or "").strip()
    if not out:
        return True
    if _ai_tool_translation_output_looks_bad(out):
        return True
    src_words = re.findall(r"\w+", src, flags=re.UNICODE)
    out_words = re.findall(r"\w+", out, flags=re.UNICODE)
    out_low = out.lower().strip()
    bad_exact = {
        "the following",
        "the following:",
        "other than",
    }
    if out_low in bad_exact:
        return True
    # For very short inputs, long generic phrases are often a bad guess.
    if len(src_words) <= 2 and len(out_words) >= 4:
        return True
    return False


def _ai_tools_ollama_generate_blocking(prompt: str, *, temperature: float = 0.2, num_predict: int = 700) -> tuple[str, str]:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv(
        "AI_TOOLS_OLLAMA_MODEL",
        os.getenv("AI_CHAT_OLLAMA_MODEL", os.getenv("TTS_OLLAMA_MODEL", os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b"))),
    )
    timeout_s = float(os.getenv("AI_TOOLS_OLLAMA_TIMEOUT", "90"))
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": "10m",
        "options": {"temperature": float(temperature), "num_predict": int(num_predict)},
    }
    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    out = str((data or {}).get("response") or "").strip()
    if not out:
        raise RuntimeError("empty ollama response")
    return out, model


def _ai_env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _ai_translator_backend() -> str:
    raw = str(os.getenv("AI_TRANSLATOR_BACKEND", "hybrid")).strip().lower()
    aliases = {
        "nllb200": "nllb",
        "nllb-200": "nllb",
        "nllb+ollama": "hybrid",
        "nllb_ollama": "hybrid",
        "nllb-hybrid": "hybrid",
    }
    return aliases.get(raw, raw)


def _ai_translator_nllb_lang_code(lang_key: str) -> str:
    mapping = {
        "uz": "uzn_Latn",
        "ru": "rus_Cyrl",
        "en": "eng_Latn",
    }
    return mapping.get(str(lang_key or "").lower(), "eng_Latn")


def _ai_translator_get_nllb_bundle():
    model_name = os.getenv("AI_TRANSLATOR_NLLB_MODEL", "facebook/nllb-200-distilled-600M").strip() or "facebook/nllb-200-distilled-600M"
    device = str(os.getenv("AI_TRANSLATOR_NLLB_DEVICE", "cpu")).strip().lower() or "cpu"
    local_only = str(os.getenv("AI_TRANSLATOR_NLLB_LOCAL_ONLY", "0")).strip().lower() in {"1", "true", "yes", "on"}
    cache_key = (model_name, device, local_only)

    with _NLLB_TRANSLATOR_LOCK:
        cached = _NLLB_TRANSLATOR_CACHE.get(cache_key)
        if cached:
            return cached

        try:
            import torch  # type: ignore
            from transformers import AutoModelForSeq2SeqLM, AutoTokenizer  # type: ignore
        except Exception as e:
            raise RuntimeError("nllb_requirements_missing") from e

        tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=local_only)
        model = AutoModelForSeq2SeqLM.from_pretrained(model_name, local_files_only=local_only)
        resolved_device = device
        if device == "auto":
            resolved_device = "cuda" if getattr(torch, "cuda", None) and torch.cuda.is_available() else "cpu"
        if resolved_device == "cuda" and not torch.cuda.is_available():
            resolved_device = "cpu"
        if resolved_device == "cuda":
            model = model.to("cuda")
        model.eval()
        bundle = {
            "tokenizer": tokenizer,
            "model": model,
            "torch": torch,
            "device": resolved_device,
            "model_name": model_name,
        }
        _NLLB_TRANSLATOR_CACHE[cache_key] = bundle
        return bundle


def _ai_tool_translate_nllb_blocking(user_text: str, target_lang: str, source_lang: str) -> str:
    bundle = _ai_translator_get_nllb_bundle()
    tokenizer = bundle["tokenizer"]
    model = bundle["model"]
    torch = bundle["torch"]
    device = str(bundle.get("device") or "cpu")

    src_code = _ai_translator_nllb_lang_code(source_lang)
    tgt_code = _ai_translator_nllb_lang_code(target_lang)
    tokenizer.src_lang = src_code
    max_input_tokens = max(64, min(2048, int(os.getenv("AI_TRANSLATOR_NLLB_MAX_INPUT_TOKENS", "512"))))
    max_new_tokens = max(32, min(2048, int(os.getenv("AI_TRANSLATOR_NLLB_MAX_NEW_TOKENS", "512"))))

    inputs = tokenizer(
        str(user_text or "")[:5000],
        return_tensors="pt",
        truncation=True,
        max_length=max_input_tokens,
    )
    if device == "cuda":
        inputs = {k: v.to("cuda") for k, v in inputs.items()}

    forced_bos_token_id = None
    try:
        forced_bos_token_id = int(tokenizer.lang_code_to_id[tgt_code])  # type: ignore[attr-defined]
    except Exception:
        try:
            forced_bos_token_id = int(tokenizer.convert_tokens_to_ids(tgt_code))
        except Exception:
            forced_bos_token_id = None
    if forced_bos_token_id is None or forced_bos_token_id < 0:
        raise RuntimeError(f"nllb_target_lang_unsupported:{target_lang}")

    with torch.no_grad():
        generated = model.generate(
            **inputs,
            forced_bos_token_id=forced_bos_token_id,
            max_new_tokens=max_new_tokens,
        )
    out = tokenizer.batch_decode(generated, skip_special_tokens=True)[0].strip()
    if not out:
        raise RuntimeError("empty_nllb_translation")
    return out


def _ai_tool_translation_output_looks_bad(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return True
    bad_patterns = (
        "async def ", "def ", "print(", "import asyncio", "return \"", "return '",
        "```python", "class ", "await ", "from ",
    )
    return any(p in t for p in bad_patterns)


def _ai_tool_translate_ollama_blocking(user_text: str, target_lang: str, reply_lang_hint: str) -> str:
    prompt = (
        "You are a translation engine.\n"
        "Translate the user's text exactly.\n"
        "Keep meaning, names, and tone.\n"
        "Do not summarize.\n"
        "Do not explain.\n"
        "Do not output code.\n"
        "Return ONLY the translated text.\n"
        f"Target language code: {target_lang}\n"
        f"Source language hint: {reply_lang_hint}\n\n"
        f"Text:\n{user_text[:5000]}"
    )
    out, _ = _ai_tools_ollama_generate_blocking(prompt, temperature=0.0, num_predict=900)
    out = _ai_chat_postprocess_reply(out, user_text)
    if _ai_tool_translation_output_looks_bad(out):
        retry_prompt = (
            "Translate only. No code. No examples. No explanation. Return one translation only.\n"
            f"Target language: {target_lang}\n"
            f"Source language hint: {reply_lang_hint}\n\n"
            f"{user_text[:5000]}"
        )
        out2, _ = _ai_tools_ollama_generate_blocking(retry_prompt, temperature=0.0, num_predict=900)
        out2 = _ai_chat_postprocess_reply(out2, user_text)
        if not _ai_tool_translation_output_looks_bad(out2):
            out = out2
    return out


def _ai_tool_translate_blocking(user_text: str, target_lang: str, reply_lang_hint: str) -> str:
    backend = _ai_translator_backend()
    allow_ollama_fallback = _ai_env_bool("AI_TRANSLATOR_FALLBACK_OLLAMA", True)

    if backend in {"nllb", "hybrid"}:
        try:
            nllb_out = _ai_tool_translate_nllb_blocking(user_text, target_lang, reply_lang_hint)
        except Exception as e:
            logger.info("AI Translator NLLB failed (backend=%s): %s", backend, e)
            if not allow_ollama_fallback:
                raise
            return _ai_tool_translate_ollama_blocking(user_text, target_lang, reply_lang_hint)

        if backend == "hybrid":
            nllb_suspicious = _ai_tool_translation_output_is_suspicious(user_text, nllb_out)
            recheck_always = _ai_env_bool("AI_TRANSLATOR_HYBRID_RECHECK_ALWAYS", False)
            if nllb_suspicious or recheck_always:
                try:
                    ollama_out = _ai_tool_translate_ollama_blocking(user_text, target_lang, reply_lang_hint)
                    ollama_suspicious = _ai_tool_translation_output_is_suspicious(user_text, ollama_out)
                    if nllb_suspicious and not ollama_suspicious:
                        return ollama_out
                    if not nllb_suspicious and ollama_suspicious:
                        return nllb_out
                    if not nllb_suspicious and not ollama_suspicious:
                        return nllb_out
                    return ollama_out if len(str(ollama_out or "").strip()) > len(str(nllb_out or "").strip()) else nllb_out
                except Exception as e:
                    logger.info("AI Translator hybrid Ollama recheck failed: %s", e)
                    return nllb_out
            return nllb_out

        return nllb_out

    if backend == "ollama":
        return _ai_tool_translate_ollama_blocking(user_text, target_lang, reply_lang_hint)

    # Unknown backend value -> safe fallback.
    return _ai_tool_translate_ollama_blocking(user_text, target_lang, reply_lang_hint)


def _ai_tool_translate_with_source_retry_blocking(
    user_text: str,
    target_lang: str,
    source_lang: str,
    *,
    source_explicit: bool = False,
    ui_lang: str = "en",
) -> str:
    src = source_lang if source_lang in {"uz", "ru", "en"} else "en"
    tried: list[str] = []
    candidates: list[str] = [src]
    if not source_explicit:
        for alt in (ui_lang, "uz", "en", "ru"):
            if alt in {"uz", "ru", "en"} and alt != target_lang and alt not in candidates:
                candidates.append(alt)

    last_output = ""
    last_error: Exception | None = None
    for cand in candidates:
        tried.append(cand)
        try:
            out = _ai_tool_translate_blocking(user_text, target_lang, cand)
            last_output = out
            if source_explicit or not _ai_tool_translation_output_is_suspicious(user_text, out):
                return out
        except Exception as e:
            last_error = e
            continue

    if last_output:
        return last_output
    if last_error:
        raise last_error
    raise RuntimeError("translation_failed")


def _ai_tool_quiz_count_inline_keyboard(selected: int | None = None) -> InlineKeyboardMarkup:
    row = []
    for n in _AI_QUIZ_COUNT_CHOICES:
        label = f"✅ {n}" if int(selected or 0) == n else str(n)
        row.append(InlineKeyboardButton(label, callback_data=f"aitool:quizcount:{n}"))
    return InlineKeyboardMarkup([row])


def _ai_tool_quiz_interval_inline_keyboard(selected: int | None = None) -> InlineKeyboardMarkup:
    row = []
    for n in _AI_QUIZ_INTERVAL_CHOICES:
        label = f"✅ {n}s" if int(selected or -1) == n else f"{n}s"
        row.append(InlineKeyboardButton(label, callback_data=f"aitool:quizinterval:{n}"))
    return InlineKeyboardMarkup([row])


def _ai_tool_quiz_setup_inline_keyboard(selected_count: int | None = None, selected_interval: int | None = None) -> InlineKeyboardMarkup:
    count_rows = _ai_tool_quiz_count_inline_keyboard(selected_count).inline_keyboard
    interval_rows = _ai_tool_quiz_interval_inline_keyboard(selected_interval).inline_keyboard
    return InlineKeyboardMarkup([*count_rows, *interval_rows])


def _ai_tool_music_duration_inline_keyboard(lang_ui: str, selected: int | None = None) -> InlineKeyboardMarkup:
    msgs = _ai_tool_mode_texts(lang_ui)
    options = [
        (_AI_MUSIC_DURATION_CHOICES[0], msgs.get("music_duration_quick", "⚡ Quick")),
        (_AI_MUSIC_DURATION_CHOICES[1], msgs.get("music_duration_standard", "🎧 Standard")),
        (_AI_MUSIC_DURATION_CHOICES[2], msgs.get("music_duration_long", "🎼 Long")),
    ]
    row = []
    for seconds, label in options:
        text_label = f"{label} ({seconds}s)"
        if int(selected or 0) == int(seconds):
            text_label = f"✅ {text_label}"
        row.append(InlineKeyboardButton(text_label, callback_data=f"aitool:musicdur:{seconds}"))
    return InlineKeyboardMarkup([row])


def _ai_tool_music_style_inline_keyboard(lang_ui: str, selected: str | None = None) -> InlineKeyboardMarkup:
    selected_key = str(selected or "").lower()
    row = []
    for style_key in _AI_MUSIC_STYLE_CHOICES:
        label = _ai_tool_music_style_label(style_key, lang_ui)
        if style_key == selected_key:
            label = f"✅ {label}"
        row.append(InlineKeyboardButton(label, callback_data=f"aitool:musicstyle:{style_key}"))
    return InlineKeyboardMarkup([row])


def _ai_tool_music_setup_inline_keyboard(
    lang_ui: str,
    selected_style: str | None = None,
    selected_duration: int | None = None,
) -> InlineKeyboardMarkup:
    duration_rows = _ai_tool_music_duration_inline_keyboard(lang_ui, selected_duration).inline_keyboard
    style_rows = _ai_tool_music_style_inline_keyboard(lang_ui, selected_style).inline_keyboard
    return InlineKeyboardMarkup([*duration_rows, *style_rows])


def _ai_tool_quiz_extract_json_array(text: str) -> list | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else None
    except Exception:
        pass
    m = re.search(r"\[\s*\{.*\}\s*\]", raw, flags=re.DOTALL)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
        return data if isinstance(data, list) else None
    except Exception:
        return None


_AI_QUIZ_STOPWORDS = {
    "the", "and", "for", "with", "from", "this", "that", "these", "those", "into", "your",
    "quiz", "test", "make", "build", "create", "using", "about", "topic", "prompt",
    "questions", "question", "english", "russian", "uzbek",
    "uchun", "bilan", "haqida", "mavzu", "savol", "savollar", "shu", "bu", "ham",
    "для", "про", "тема", "вопрос", "вопросы", "это", "как", "что", "или", "и",
}


def _ai_tool_quiz_focus_terms(source_kind: str, source_content: str) -> list[str]:
    text = " ".join(str(source_content or "").lower().split())
    if not text:
        return []
    tokens = re.findall(r"[a-zA-Zа-яА-ЯёЁʻ’'`-]{3,}", text)
    out: list[str] = []
    seen: set[str] = set()
    limit = 12 if source_kind == "prompt" else 8
    for tok in tokens:
        t = tok.strip("-'`ʻ’").lower()
        if not t or t in _AI_QUIZ_STOPWORDS or len(t) < 4:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= limit:
            break
    return out


def _ai_tool_quiz_topic_anchors(source_content: str) -> dict[str, bool]:
    t = " ".join(str(source_content or "").lower().split())
    return {
        "grammar": any(x in t for x in ("grammar", "grammatika", "граммат", "tense", "plural", "verb", "noun")),
        "vocab": any(x in t for x in ("vocabulary", "vocab", "word", "словар", "lug", "so'z")),
        "reading": any(x in t for x in ("reading", "read", "чтени", "o'qish")),
        "listening": any(x in t for x in ("listening", "listen", "eshit", "слушан")),
        "future_tense": any(x in t for x in ("future tense", "future", "kelasi zamon", "будущее время")),
    }


def _ai_tool_quiz_question_relevance_score(q: dict, *, source_kind: str, source_content: str, reply_lang_hint: str) -> int:
    blob = " ".join(
        [
            str(q.get("question") or ""),
            " ".join(str(x) for x in list(q.get("options") or [])[:4]),
            str(q.get("explanation") or ""),
        ]
    ).lower()
    score = 0
    try:
        q_lang = _ai_chat_guess_reply_lang(str(q.get("question") or ""), reply_lang_hint)
        if q_lang == reply_lang_hint:
            score += 2
        elif reply_lang_hint in {"uz", "ru", "en"}:
            score -= 1
    except Exception:
        pass

    focus = _ai_tool_quiz_focus_terms(source_kind, source_content)
    overlap = sum(1 for term in focus if term in blob)
    score += min(overlap, 6) * 2
    if source_kind in {"prompt", "topic"} and focus and overlap == 0 and not any(_ai_tool_quiz_topic_anchors(source_content).values()):
        score -= 5

    anchors = _ai_tool_quiz_topic_anchors(source_content)
    if anchors["grammar"]:
        grammar_hits = sum(1 for x in ("grammar", "tense", "plural", "singular", "verb", "noun", "adjective", "article", "preposition", "pronoun", "sentence", "clause", "correct form") if x in blob)
        score += 3 if grammar_hits >= 1 else -6
    if anchors.get("future_tense"):
        future_hits = sum(
            1
            for x in (
                "future", "will", "going to", "future perfect", "future continuous",
                "present continuous", "prediction", "plan", "arrangement",
            )
            if x in blob
        )
        score += 4 if future_hits >= 1 else -4
    if anchors["vocab"]:
        vocab_hits = sum(1 for x in ("meaning", "synonym", "antonym", "definition", "translate", "word") if x in blob)
        score += 2 if vocab_hits >= 1 else -3
    if any(anchors.values()) and any(x in blob for x in ("capital of", "president", "planet", "river", "country", "geography")):
        score -= 4
    return score


def _ai_tool_quiz_rerank_and_filter_questions(
    questions: list[dict],
    *,
    source_kind: str,
    source_content: str,
    reply_lang_hint: str,
    count: int,
) -> list[dict]:
    anchors = _ai_tool_quiz_topic_anchors(source_content)
    scored = [
        (
            _ai_tool_quiz_question_relevance_score(
                q,
                source_kind=source_kind,
                source_content=source_content,
                reply_lang_hint=reply_lang_hint,
            ),
            q,
        )
        for q in (questions or [])
    ]
    scored.sort(key=lambda x: x[0], reverse=True)
    min_score = 0 if any(anchors.values()) else 1
    good = [q for s, q in scored if s >= min_score]
    if len(good) >= max(1, min(3, count)):
        return good[:count]
    return [q for _, q in scored][:count]


def _ai_tool_quiz_normalize_questions(items: list, count: int) -> list[dict]:
    out: list[dict] = []
    for item in list(items or []):
        if not isinstance(item, dict):
            continue
        q = str(item.get("question") or item.get("q") or "").strip()
        options = item.get("options") or item.get("choices") or []
        if not isinstance(options, list):
            continue
        opts = [str(x).strip() for x in options if str(x).strip()]
        if len(opts) < 2:
            continue
        if len(opts) > 4:
            opts = opts[:4]
        # pad to 4 options if model returned 3
        while len(opts) < 4:
            opts.append(f"Option {len(opts)+1}")
        correct_idx = item.get("correct_option_id", item.get("correct_index", item.get("answer_index", item.get("correct"))))
        try:
            correct_idx = int(correct_idx)
        except Exception:
            correct_idx = 0
        if correct_idx < 0 or correct_idx >= len(opts):
            # Try to match answer text.
            ans_txt = str(item.get("answer") or item.get("correct_answer") or "").strip()
            if ans_txt and ans_txt in opts:
                correct_idx = opts.index(ans_txt)
            else:
                correct_idx = 0
        explanation = str(item.get("explanation") or "").strip()[:180]
        if not q:
            continue
        q = re.sub(r"\s+", " ", q).strip()[:290]
        if len(set(opts)) < 2:
            continue
        out.append({"question": q, "options": opts[:10], "correct_option_id": int(correct_idx), "explanation": explanation})
        if len(out) >= count:
            break
    return out


def _ai_tool_quiz_generate_blocking(source_kind: str, source_content: str, count: int, reply_lang_hint: str) -> list[dict]:
    count = max(1, min(10, int(count)))
    source_kind = (source_kind or "topic").lower()
    source_content = (source_content or "").strip()
    focus_terms = _ai_tool_quiz_focus_terms(source_kind, source_content)
    anchors = _ai_tool_quiz_topic_anchors(source_content)
    anchor_rules: list[str] = []
    if anchors.get("grammar"):
        anchor_rules.append("This is a grammar-focused quiz. Ask grammar questions (tenses, sentence correctness, parts of speech, plural/singular, etc.).")
    if anchors.get("vocab"):
        anchor_rules.append("This is a vocabulary-focused quiz. Prefer meanings, usage, synonyms/antonyms, or translation-style questions.")
    if anchors.get("reading"):
        anchor_rules.append("Prefer reading-comprehension style wording when suitable.")
    if anchors.get("listening"):
        anchor_rules.append("Prefer listening-practice style wording when suitable.")
    focus_line = ", ".join(focus_terms[:8])
    if source_kind == "prompt":
        source_instruction = (
            "Build the quiz according to the user's prompt/instructions.\n"
            "Follow requested topic, difficulty, style, and focus exactly if specified.\n"
            "Do NOT drift to unrelated subjects or general knowledge.\n"
        )
    elif source_kind in {"text", "pdf"}:
        source_instruction = (
            "Build the quiz ONLY from the provided content.\n"
            "Questions must be based on facts/details in the content.\n"
            "Do not invent external facts unless the content strongly implies them.\n"
        )
    else:
        source_instruction = (
            "Build the quiz from the provided topic request.\n"
            "Make questions suitable for the requested level/topic if mentioned.\n"
            "Do NOT drift to unrelated subjects.\n"
        )
    prompt = (
        "Generate a Telegram quiz in JSON.\n"
        "Return ONLY a JSON array.\n"
        "Each item must have:\n"
        "- question (string)\n"
        "- options (array of exactly 4 short strings)\n"
        "- correct_option_id (0-3 integer)\n"
        "- explanation (short string, optional but helpful)\n"
        "Rules:\n"
        "- Make questions clear and factual.\n"
        "- One correct answer only.\n"
        "- Keep options short.\n"
        "- Use the same language as the topic request unless the topic requests another language.\n"
        "- Stay strictly on the requested topic/prompt.\n"
        f"- Generate exactly {count} questions.\n\n"
        f"{source_instruction}\n"
        f"{''.join(rule + chr(10) for rule in anchor_rules)}"
        f"{('Important concepts to stay close to: ' + focus_line + chr(10)) if focus_line else ''}"
        f"Language hint: {reply_lang_hint}\n"
        f"Source kind: {source_kind}\n"
        f"Source content:\n{source_content[:7000]}\n"
    )
    out, _ = _ai_tools_ollama_generate_blocking(prompt, temperature=0.05, num_predict=2200)
    data = _ai_tool_quiz_extract_json_array(out)
    questions = _ai_tool_quiz_rerank_and_filter_questions(
        _ai_tool_quiz_normalize_questions(data or [], count),
        source_kind=source_kind,
        source_content=source_content,
        reply_lang_hint=reply_lang_hint,
        count=count,
    )
    if (not data) or len(questions) < max(1, min(3, count)):
        retry_prompt = (
            "Return ONLY valid JSON array for Telegram quiz.\n"
            "Stay strictly on-topic. No unrelated questions.\n"
            f"Exactly {count} items. 4 options each. correct_option_id integer.\n"
            f"{('Must stay close to: ' + focus_line + chr(10)) if focus_line else ''}"
            f"Language hint: {reply_lang_hint}\n"
            f"Source kind: {source_kind}\n"
            f"Source content:\n{source_content[:5000]}"
        )
        out2, _ = _ai_tools_ollama_generate_blocking(retry_prompt, temperature=0.0, num_predict=2200)
        data = _ai_tool_quiz_extract_json_array(out2)
        questions = _ai_tool_quiz_rerank_and_filter_questions(
            _ai_tool_quiz_normalize_questions(data or [], count),
            source_kind=source_kind,
            source_content=source_content,
            reply_lang_hint=reply_lang_hint,
            count=count,
        )
    if len(questions) < max(1, min(3, count)):
        # Final fallback: force simpler, highly constrained questions for better reliability.
        final_retry_prompt = (
            "Create a STRICT multiple-choice Telegram quiz and return ONLY a JSON array.\n"
            f"Generate exactly {count} questions.\n"
            "Each item keys: question, options, correct_option_id, explanation.\n"
            "Options: exactly 4 strings. correct_option_id: integer 0..3.\n"
            "No markdown. No commentary. No extra text.\n"
            "Keep each question simple and directly tied to the user's request.\n"
            "If the request is grammar-related, every question MUST be grammar-related.\n"
            f"Language hint: {reply_lang_hint}\n"
            f"{('Key focus: ' + focus_line + chr(10)) if focus_line else ''}"
            f"Source kind: {source_kind}\n"
            f"User request/content:\n{source_content[:4000]}"
        )
        out3, _ = _ai_tools_ollama_generate_blocking(final_retry_prompt, temperature=0.0, num_predict=2600)
        data = _ai_tool_quiz_extract_json_array(out3)
        questions = _ai_tool_quiz_rerank_and_filter_questions(
            _ai_tool_quiz_normalize_questions(data or [], count),
            source_kind=source_kind,
            source_content=source_content,
            reply_lang_hint=reply_lang_hint,
            count=count,
        )
    if len(questions) < max(1, min(3, count)):
        raise RuntimeError("quiz_generation_failed")
    return questions[:count]


def _ai_quiz_poll_map(bot_data: dict) -> dict:
    return bot_data.setdefault("ai_quiz_poll_map", {})


def _ai_quiz_runs(bot_data: dict) -> dict:
    return bot_data.setdefault("ai_quiz_runs", {})


def _ai_quiz_sets(bot_data: dict) -> dict:
    return bot_data.setdefault("ai_quiz_sets", {})


def _ai_quiz_cancel_tokens(bot_data: dict) -> set:
    return bot_data.setdefault("ai_quiz_cancel_tokens", set())


def _ai_music_cancel_tokens(bot_data: dict) -> set:
    return bot_data.setdefault("ai_music_cancel_tokens", set())


async def _ai_tool_quiz_send_polls(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id: int,
    lang_ui: str,
    topic: str,
    questions: list[dict],
    interval_s: int = 0,
):
    msgs = _ai_tool_mode_texts(lang_ui if lang_ui in {"uz", "ru", "en"} else "en")
    cleaned: list[dict] = []
    for idx, q in enumerate(list(questions or []), start=1):
        q_text = str(q.get("question") or f"Question {idx}").strip()[:300]
        opts = [str(x).strip()[:100] for x in list(q.get("options") or [])[:4] if str(x).strip()]
        if len(opts) < 2:
            continue
        while len(opts) < 4:
            opts.append(f"Option {len(opts)+1}")
        try:
            correct = int(q.get("correct_option_id", 0))
        except Exception:
            correct = 0
        if correct < 0 or correct >= len(opts):
            correct = 0
        cleaned.append({
            "question": q_text,
            "options": opts,
            "correct_option_id": correct,
            "explanation": str(q.get("explanation") or "").strip()[:180],
        })
    if not cleaned:
        raise RuntimeError("quiz_empty")

    interval_s = max(0, min(120, int(interval_s or 0)))
    run_id = uuid.uuid4().hex
    poll_map = _ai_quiz_poll_map(context.application.bot_data)
    runs = _ai_quiz_runs(context.application.bot_data)
    runs[run_id] = {
        "chat_id": int(chat_id),
        "user_id": int(user_id or 0),
        "total": len(cleaned),
        "answered": 0,
        "correct": 0,
        "poll_ids": [],
        "answered_poll_ids": set(),
        "lang_ui": lang_ui,
        "topic": str(topic or "Quiz")[:200],
        "created_at": time.time(),
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=msgs["quiz_ready_intro"].format(topic=str(topic or "Quiz")[:120], count=len(cleaned)),
    )
    for idx, q in enumerate(cleaned, start=1):
        sent = await context.bot.send_poll(
            chat_id=chat_id,
            question=q["question"],
            options=q["options"],
            type="quiz",
            correct_option_id=int(q["correct_option_id"]),
            explanation=q.get("explanation") or None,
            is_anonymous=False,
        )
        poll = getattr(sent, "poll", None)
        poll_id = str(getattr(poll, "id", "") or "")
        if poll_id:
            poll_map[poll_id] = {
                "run_id": run_id,
                "user_id": int(user_id or 0),
                "correct_option_id": int(q["correct_option_id"]),
            }
            runs[run_id]["poll_ids"].append(poll_id)
        if interval_s > 0 and idx < len(cleaned):
            await asyncio.sleep(interval_s)

    if not runs[run_id]["poll_ids"]:
        runs.pop(run_id, None)
        raise RuntimeError("quiz_poll_send_failed")


def _ai_quiz_cleanup_stale(bot_data: dict, *, max_age_s: int = 6 * 3600) -> None:
    now = time.time()
    sets = _ai_quiz_sets(bot_data)
    for qid, meta in list(sets.items()):
        try:
            created = float((meta or {}).get("created_at") or 0.0)
        except Exception:
            created = 0.0
        if created and now - created > max_age_s:
            sets.pop(qid, None)


def _ai_tool_quiz_card_inline_keyboard(
    *,
    lang_ui: str,
    quiz_id: str,
    bot_username: str | None = None,
    topic: str = "",
    count: int = 0,
) -> InlineKeyboardMarkup:
    msgs = _ai_tool_mode_texts(lang_ui)
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(msgs["quiz_btn_start"], callback_data=f"aitool:quizstart:{quiz_id}")],
        [InlineKeyboardButton(msgs["quiz_btn_group"], callback_data=f"aitool:quizgroup:{quiz_id}")],
    ]
    if bot_username:
        bot_username = bot_username.lstrip("@")
        deep_link = f"https://t.me/{bot_username}?start=quiz_{quiz_id}"
        share_text = msgs["quiz_share_text"].format(topic=topic[:120], count=int(count or 0))
        share_url = "https://t.me/share/url?" + urllib.parse.urlencode({
            "url": deep_link,
            "text": share_text,
        })
        rows.append([InlineKeyboardButton(msgs["quiz_btn_share"], url=share_url)])
    else:
        rows.append([InlineKeyboardButton(msgs["quiz_btn_share"], callback_data=f"aitool:quizshare:{quiz_id}")])
    return InlineKeyboardMarkup(rows)


def _my_quiz_list_keyboard(
    quizzes: list[dict],
    page: int,
    total: int,
    lang_ui: str,
) -> InlineKeyboardMarkup:
    msgs = _ai_tool_mode_texts(lang_ui)
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, row_item in enumerate(quizzes, start=1):
        qid = str(row_item.get("id") or "")
        if not qid:
            continue
        row.append(InlineKeyboardButton(str(idx + page * _MY_QUIZ_PAGE_SIZE), callback_data=f"myquiz:view:{qid}:{page}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav: list[InlineKeyboardButton] = []
    pages = max(1, (int(total) + _MY_QUIZ_PAGE_SIZE - 1) // _MY_QUIZ_PAGE_SIZE)
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"myquiz:page:{page-1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"myquiz:page:{page+1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("·", callback_data="myquiz:none")]])


def _my_quiz_source_label(source_kind: str, lang_ui: str) -> str:
    key = str(source_kind or "topic").lower()
    if lang_ui == "uz":
        return {"topic": "mavzu", "prompt": "prompt", "text": "matn", "pdf": "PDF"}.get(key, key or "mavzu")
    if lang_ui == "ru":
        return {"topic": "тема", "prompt": "prompt", "text": "текст", "pdf": "PDF"}.get(key, key or "тема")
    return {"topic": "topic", "prompt": "prompt", "text": "text", "pdf": "PDF"}.get(key, key or "topic")


def _my_quiz_list_text(lang_ui: str, quizzes: list[dict], page: int, total: int) -> str:
    msgs = _ai_tool_mode_texts(lang_ui)
    pages = max(1, (int(total) + _MY_QUIZ_PAGE_SIZE - 1) // _MY_QUIZ_PAGE_SIZE)
    lines = [
        msgs["my_quiz_title"].format(page=page + 1, pages=pages, total=int(total)),
        msgs["my_quiz_open_prompt"],
    ]
    base = int(page) * _MY_QUIZ_PAGE_SIZE
    for idx, q in enumerate(list(quizzes or []), start=1):
        name = str(q.get("quiz_name") or "AI Quiz Test")[:80]
        count = int(q.get("question_count") or 0)
        source_kind = _my_quiz_source_label(str(q.get("source_kind") or "topic"), lang_ui)
        lines.append(f"{base + idx}. {name} · {count}Q · {source_kind}")
    return "\n".join(lines)


def _my_quiz_detail_keyboard(lang_ui: str, quiz_id: str, page: int) -> InlineKeyboardMarkup:
    msgs = _ai_tool_mode_texts(lang_ui)
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(msgs["my_quiz_btn_start"], callback_data=f"myquiz:start:{quiz_id}:{page}"),
            InlineKeyboardButton(msgs["my_quiz_btn_group"], callback_data=f"myquiz:group:{quiz_id}:{page}"),
        ],
    ]
    rows.append(
        [
            InlineKeyboardButton(msgs["my_quiz_btn_share"], callback_data=f"myquiz:share:{quiz_id}:{page}"),
            InlineKeyboardButton(msgs["my_quiz_btn_delete"], callback_data=f"myquiz:delete:{quiz_id}:{page}"),
        ]
    )
    rows.append([InlineKeyboardButton(msgs["my_quiz_btn_back"], callback_data=f"myquiz:page:{page}")])
    return InlineKeyboardMarkup(rows)


def _my_quiz_detail_text(lang_ui: str, quiz: dict) -> str:
    msgs = _ai_tool_mode_texts(lang_ui)
    preview = " ".join(str(quiz.get("source_preview") or "").split()).strip()
    if len(preview) > 120:
        preview = preview[:117] + "..."
    if not preview:
        preview = "-"
    return msgs["my_quiz_detail"].format(
        name=str(quiz.get("quiz_name") or "AI Quiz Test")[:120],
        source_kind=_my_quiz_source_label(str(quiz.get("source_kind") or "topic"), lang_ui),
        source_preview=preview,
        count=int(quiz.get("question_count") or len(quiz.get("questions") or [])),
        interval=int(quiz.get("interval_s") or 0),
    )


async def _my_quiz_send_shareable_card(context: ContextTypes.DEFAULT_TYPE, chat_id: int, quiz: dict, lang_ui: str):
    msgs = _ai_tool_mode_texts(lang_ui)
    questions = list(quiz.get("questions") or [])
    total = int(quiz.get("question_count") or len(questions))
    interval_s = int(quiz.get("interval_s") or 0)
    minutes = max(1, int(round(max(1, total) * 1.5)))
    quiz_id = str(quiz.get("id") or quiz.get("quiz_id") or "")
    quiz_name = str(quiz.get("quiz_name") or "AI Quiz Test")
    topic = str(quiz.get("source_preview") or quiz_name or "Quiz")
    caption = msgs["quiz_card_caption"].format(
        name=quiz_name[:120],
        topic=topic[:120],
        count=total,
        minutes=minutes,
        interval=interval_s,
    )
    bot_username = getattr(getattr(context, "bot", None), "username", None)
    await context.bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=_ai_tool_quiz_card_inline_keyboard(
            lang_ui=lang_ui,
            quiz_id=quiz_id,
            bot_username=bot_username,
            topic=topic,
            count=total,
        ),
    )


async def my_quiz_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    lang_ui = str(context.user_data.get("language") or "en")
    if lang_ui not in {"uz", "ru", "en"}:
        lang_ui = "en"
    user_id = update.effective_user.id if update.effective_user else 0
    msgs = _ai_tool_mode_texts(lang_ui)
    if not callable(_db_list_user_quizzes) or not callable(_db_count_user_quizzes):
        await update.message.reply_text(msgs["failed"])
        return
    page = 0
    items = await run_blocking(_db_list_user_quizzes, user_id, _MY_QUIZ_PAGE_SIZE, 0)
    total = await run_blocking(_db_count_user_quizzes, user_id)
    if not items:
        await update.message.reply_text(msgs["my_quiz_empty"])
        return
    await update.message.reply_text(
        _my_quiz_list_text(lang_ui, items, page, total),
        reply_markup=_my_quiz_list_keyboard(items, page, total, lang_ui),
    )


async def handle_my_quiz_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = str(query.data or "")
    if not data.startswith("myquiz:"):
        return
    lang_ui = str(context.user_data.get("language") or "en")
    if lang_ui not in {"uz", "ru", "en"}:
        lang_ui = "en"
    msgs = _ai_tool_mode_texts(lang_ui)
    user_id = update.effective_user.id if update.effective_user else 0
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    if action == "none":
        try:
            await query.answer()
        except Exception:
            pass
        return
    if action == "page":
        page = 0
        if len(parts) >= 3 and str(parts[2]).isdigit():
            page = max(0, int(parts[2]))
        total = await run_blocking(_db_count_user_quizzes, user_id) if callable(_db_count_user_quizzes) else 0
        items = await run_blocking(_db_list_user_quizzes, user_id, _MY_QUIZ_PAGE_SIZE, page * _MY_QUIZ_PAGE_SIZE) if callable(_db_list_user_quizzes) else []
        if not items:
            try:
                await query.edit_message_text(msgs["my_quiz_empty"])
            except Exception:
                pass
            try:
                await query.answer()
            except Exception:
                pass
            return
        try:
            await query.edit_message_text(
                _my_quiz_list_text(lang_ui, items, page, total),
                reply_markup=_my_quiz_list_keyboard(items, page, total, lang_ui),
            )
        except Exception:
            pass
        try:
            await query.answer()
        except Exception:
            pass
        return
    if len(parts) < 4:
        try:
            await query.answer()
        except Exception:
            pass
        return
    quiz_id = parts[2]
    page = int(parts[3]) if str(parts[3]).isdigit() else 0
    quiz = await run_blocking(_db_get_user_quiz, quiz_id, user_id) if callable(_db_get_user_quiz) else None
    if not quiz:
        try:
            await query.answer(msgs["my_quiz_not_found"][:180], show_alert=False)
        except Exception:
            pass
        if action != "page":
            try:
                total = await run_blocking(_db_count_user_quizzes, user_id) if callable(_db_count_user_quizzes) else 0
                items = await run_blocking(_db_list_user_quizzes, user_id, _MY_QUIZ_PAGE_SIZE, page * _MY_QUIZ_PAGE_SIZE) if callable(_db_list_user_quizzes) else []
                if items:
                    await query.edit_message_text(
                        _my_quiz_list_text(lang_ui, items, page, total),
                        reply_markup=_my_quiz_list_keyboard(items, page, total, lang_ui),
                    )
            except Exception:
                pass
        return
    if action == "view":
        try:
            await query.edit_message_text(
                _my_quiz_detail_text(lang_ui, quiz),
                reply_markup=_my_quiz_detail_keyboard(lang_ui, quiz_id, page),
                parse_mode="HTML",
            )
        except Exception:
            pass
        try:
            await query.answer()
        except Exception:
            pass
        return
    if action == "delete":
        if callable(_db_delete_user_quiz):
            try:
                await run_blocking(_db_delete_user_quiz, quiz_id, user_id)
            except Exception:
                logger.exception("my_quiz delete failed")
        _ai_quiz_sets(context.application.bot_data).pop(quiz_id, None)
        try:
            await query.answer(msgs["my_quiz_deleted"][:180], show_alert=False)
        except Exception:
            pass
        # Return to the list page
        total = await run_blocking(_db_count_user_quizzes, user_id) if callable(_db_count_user_quizzes) else 0
        max_page = max(0, ((max(total, 1) - 1) // _MY_QUIZ_PAGE_SIZE))
        page = min(page, max_page)
        items = await run_blocking(_db_list_user_quizzes, user_id, _MY_QUIZ_PAGE_SIZE, page * _MY_QUIZ_PAGE_SIZE) if callable(_db_list_user_quizzes) else []
        if items:
            try:
                await query.edit_message_text(
                    _my_quiz_list_text(lang_ui, items, page, total),
                    reply_markup=_my_quiz_list_keyboard(items, page, total, lang_ui),
                )
            except Exception:
                pass
        else:
            try:
                await query.edit_message_text(msgs["my_quiz_empty"])
            except Exception:
                pass
        return
    if action == "share":
        if callable(_db_increment_user_quiz_share_count):
            try:
                await run_blocking(_db_increment_user_quiz_share_count, quiz_id)
            except Exception:
                logger.exception("my_quiz share count failed")
        try:
            await _my_quiz_send_shareable_card(context, query.message.chat_id, quiz, lang_ui)
        except Exception:
            logger.exception("my_quiz share send failed")
        try:
            await query.answer(msgs["quiz_share_hint"][:180], show_alert=False)
        except Exception:
            pass
        return
    if action == "group":
        try:
            await query.answer(msgs["quiz_group_hint"][:180], show_alert=True)
        except Exception:
            pass
        return
    if action == "start":
        _ai_quiz_sets(context.application.bot_data)[quiz_id] = {
            "quiz_id": quiz_id,
            "quiz_name": quiz.get("quiz_name") or "AI Quiz Test",
            "topic": quiz.get("source_preview") or quiz.get("quiz_name") or "Quiz",
            "questions": list(quiz.get("questions") or []),
            "lang_ui": quiz.get("lang_ui") or lang_ui,
            "count": int(quiz.get("question_count") or len(quiz.get("questions") or [])),
            "interval_s": int(quiz.get("interval_s") or 0),
            "created_at": time.time(),
            "created_chat_id": int(getattr(query.message, "chat_id", 0) or 0),
        }
        if callable(_db_mark_user_quiz_started):
            try:
                await run_blocking(_db_mark_user_quiz_started, quiz_id)
            except Exception:
                logger.exception("my_quiz mark started failed")
        try:
            await _ai_tool_quiz_send_polls(
                context=context,
                chat_id=query.message.chat_id if query.message else update.effective_chat.id,
                user_id=user_id,
                lang_ui=str(quiz.get("lang_ui") or lang_ui),
                topic=str(quiz.get("source_preview") or quiz.get("quiz_name") or "Quiz"),
                questions=list(quiz.get("questions") or []),
                interval_s=int(quiz.get("interval_s") or 0),
            )
        except Exception:
            logger.exception("my_quiz start failed")
        try:
            await query.answer()
        except Exception:
            pass
        return


async def handle_ai_quiz_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pa = getattr(update, "poll_answer", None)
        if not pa:
            return
        poll_id = str(getattr(pa, "poll_id", "") or "")
        if not poll_id:
            return
        poll_map = _ai_quiz_poll_map(context.application.bot_data)
        runs = _ai_quiz_runs(context.application.bot_data)
        meta = poll_map.get(poll_id)
        if not meta:
            return
        user = getattr(pa, "user", None)
        user_id = int(getattr(user, "id", 0) or 0)
        if int(meta.get("user_id") or 0) and user_id != int(meta.get("user_id")):
            return
        run_id = str(meta.get("run_id") or "")
        run = runs.get(run_id)
        if not run:
            poll_map.pop(poll_id, None)
            return
        answered_poll_ids = run.setdefault("answered_poll_ids", set())
        if poll_id in answered_poll_ids:
            return
        answered_poll_ids.add(poll_id)
        option_ids = list(getattr(pa, "option_ids", []) or [])
        selected = int(option_ids[0]) if option_ids else -1
        if selected == int(meta.get("correct_option_id", -999)):
            run["correct"] = int(run.get("correct") or 0) + 1
        run["answered"] = int(run.get("answered") or 0) + 1
        total = int(run.get("total") or 0)
        if run["answered"] < total:
            return
        lang_ui = str(run.get("lang_ui") or "en")
        msgs = _ai_tool_mode_texts(lang_ui if lang_ui in {"uz", "ru", "en"} else "en")
        correct = int(run.get("correct") or 0)
        percent = int(round((correct * 100 / total), 0)) if total > 0 else 0
        chat_id = int(run.get("chat_id") or 0)
        if chat_id:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=msgs["quiz_final"].format(correct=correct, total=total, percent=percent),
                )
            except Exception:
                logger.exception("ai quiz final score send failed")
        for pid in list(run.get("poll_ids") or []):
            poll_map.pop(str(pid), None)
        runs.pop(run_id, None)
    except Exception:
        logger.exception("handle_ai_quiz_poll_answer failed")


def _ai_chat_owner_identity_reply(user_text: str, reply_lang_hint: str) -> str | None:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return None
    if "muhammadaliabdullayev" in t or "@muhammadaliabdullayev" in t:
        asked_about_person = any(k in t for k in (
            "who", "kim", "кто", "dev", "developer", "creator", "owner", "builder",
            "yaratuv", "dasturchi", "egasi", "kim qurgan", "sozdatel", "razrabot", "vladel", "созд", "разработ", "владел"
        ))
        if asked_about_person:
            # Fall through to the same deterministic reply.
            pass
    owner_keywords = (
        "owner", "creator", "developer", "dev", "builder", "made you", "built you", "created you",
        "yaratuvchi", "dasturchi", "egasi", "kim yasagan", "kim yaratgan", "kim qurgan",
        "создатель", "разработчик", "владелец", "кто тебя", "кто создал", "кто сделал",
    )
    bot_refs = (
        "you", "your", "bot", "assistant", "ai",
        "sen", "sening", "bot", "yordamchi", "ai",
        "ты", "твой", "бот", "ассистент", "ии", "ai",
    )
    if not any(k in t for k in owner_keywords):
        return None
    if not any(k in t for k in bot_refs):
        return None

    username = "@MuhammadaliAbdullayev"
    if reply_lang_hint == "ru":
        return f"Меня создал и развивает {username}. Он мой разработчик и владелец."
    if reply_lang_hint == "uz":
        return f"Meni {username} yaratgan va rivojlantiryapti. U mening dasturchim va egam."
    return f"I was created and am maintained by {username}. He is my developer and owner."


def _ai_chat_capabilities_reply(user_text: str, reply_lang_hint: str) -> str | None:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return None
    capability_patterns = (
        "what can you do", "what do you do", "your features", "your feature", "capabilities", "abilities",
        "nima qila olasan", "nimalar qila olasan", "qanday yordam bera olasan", "funksiyalaring", "imkoniyatlaring",
        "что ты умеешь", "что умеешь", "какие функции", "какие возможности", "чем можешь помочь",
    )
    if any(p in t for p in capability_patterns):
        if reply_lang_hint == "ru":
            return (
                "Я могу помочь с функциями этого бота: поиск книг, Text to Voice, PDF Maker, профиль, избранное, запрос книги и AI Chat.\n\n"
                "Если хотите, я могу подсказать, как пользоваться любой из этих функций шаг за шагом."
            )
        if reply_lang_hint == "uz":
            return (
                "Men bu bot funksiyalarida yordam bera olaman: kitob qidirish, Text to Voice, PDF Maker, profil, sevimlilar, kitob so‘rash va AI Chat.\n\n"
                "Xohlasangiz, istalgan funksiyadan qanday foydalanishni bosqichma-bosqich tushuntiraman."
            )
        return (
            "I can help with this bot’s features: book search, Text to Voice, PDF Maker, profile, favorites, book requests, and AI Chat.\n\n"
            "If you want, I can explain any feature step by step."
        )
    return None


def _ai_chat_feature_help_reply(user_text: str, reply_lang_hint: str) -> str | None:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return None
    groups = [
        ("search", ("search book", "search books", "kitob qidir", "qidiruv", "поиск книг", "найти книгу")),
        ("request", ("request book", "request a book", "kitob so‘r", "kitob sor", "kitob so'r", "запрос книги", "request")),
        ("tts", ("text to voice", "matndan ovoz", "tts", "текст в голос", "voice")),
        ("pdf", ("pdf maker", "pdf", "pdf yarat", "создать pdf")),
        ("favorites", ("favorite", "favorites", "sevimli", "sevimlilar", "избранн")),
        ("profile", ("my profile", "profile", "profil", "profilim", "профил")),
        ("ai_tools", ("ai tools", "ai tool", "ai chat", "ai bilan chat", "чат с ai", "chat with ai")),
    ]
    key = None
    for k, pats in groups:
        if any(p in t for p in pats):
            key = k
            break
    if not key:
        return None

    if reply_lang_hint == "ru":
        mapping = {
            "search": "Для поиска книг откройте меню и нажмите 🔎 Поиск книг, затем отправьте название книги обычным сообщением.",
            "request": "Чтобы запросить книгу, откройте 🛠️ Другие функции -> 📝 Запрос книги и отправьте название нужной книги.",
            "tts": "Для Text to Voice откройте 🎙️ Text to Voice, выберите настройки кнопками и затем отправьте текст.",
            "pdf": "Для PDF Maker откройте 📄 PDF Maker, укажите имя PDF, выберите стиль/размер/ориентацию и затем отправьте текст.",
            "favorites": "Избранное находится в 🛠️ Другие функции -> ⭐ Избранное.",
            "profile": "Ваш профиль можно открыть через 🛠️ Другие функции -> 👤 Мой профиль.",
            "ai_tools": "AI Tools открываются из главного меню через 🤖 AI Tools. Там можно открыть 💬 Чат с AI.",
        }
        return mapping[key]
    if reply_lang_hint == "uz":
        mapping = {
            "search": "Kitob qidirish uchun menyudan 🔎 Kitob qidirish bo‘limini bosing va kitob nomini oddiy xabar qilib yuboring.",
            "request": "Kitob so‘rash uchun 🛠️ Boshqa funksiyalar -> 📝 Kitob so‘rash bo‘limiga kiring va kerakli kitob nomini yuboring.",
            "tts": "Text to Voice uchun 🎙️ Matndan ovoz bo‘limini oching, tugmalar bilan sozlang va keyin matn yuboring.",
            "pdf": "PDF Maker uchun 📄 PDF Maker bo‘limini oching, PDF nomini kiriting, sozlamalarni tanlang va keyin matn yuboring.",
            "favorites": "Sevimlilar bo‘limi 🛠️ Boshqa funksiyalar -> ⭐ Sevimlilar ichida.",
            "profile": "Profilingizni 🛠️ Boshqa funksiyalar -> 👤 Mening profilim bo‘limidan ochasiz.",
            "ai_tools": "AI Tools asosiy menyudagi 🤖 AI vositalar bo‘limida. U yerdan 💬 AI bilan chat ni ochasiz.",
        }
        return mapping[key]
    mapping = {
        "search": "To search books, tap 🔎 Search Books in the menu and send the book name as a normal message.",
        "request": "To request a book, open 🛠️ Other Functions -> 📝 Request a Book and send the book title you need.",
        "tts": "For Text to Voice, open 🎙️ Text to Voice, choose settings with buttons, then send your text.",
        "pdf": "For PDF Maker, open 📄 PDF Maker, set the PDF name/options, then send the text you want in the PDF.",
        "favorites": "Favorites are in 🛠️ Other Functions -> ⭐ Favorites.",
        "profile": "You can open your profile from 🛠️ Other Functions -> 👤 My Profile.",
        "ai_tools": "Open 🤖 AI Tools from the main menu, then choose 💬 Chat with AI.",
    }
    return mapping[key]


def _ai_chat_admin_contact_reply(user_text: str, reply_lang_hint: str) -> str | None:
    t = " ".join(str(user_text or "").lower().split())
    if not t:
        return None
    contact_keywords = (
        "contact admin", "contact owner", "admin username", "owner username", "who is admin", "who is owner",
        "admin kim", "owner kim", "egasi kim", "admin bilan bog", "owner bilan bog",
        "как связаться с админ", "кто админ", "кто владелец", "ник админа", "ник владельца",
    )
    if not any(k in t for k in contact_keywords):
        return None
    username = "@MuhammadaliAbdullayev"
    if reply_lang_hint == "ru":
        return f"Владелец и разработчик бота: {username}. Если нужен админ/создатель, можете обратиться к нему."
    if reply_lang_hint == "uz":
        return f"Bot egasi va dasturchisi: {username}. Admin/yaratuvchi kerak bo‘lsa, shu akkauntga murojaat qilishingiz mumkin."
    return f"The bot owner and developer is {username}. If you need the admin/creator, you can contact that account."


def _ai_chat_builtin_reply(user_text: str, reply_lang_hint: str) -> str | None:
    for fn in (_ai_chat_owner_identity_reply, _ai_chat_admin_contact_reply, _ai_chat_capabilities_reply, _ai_chat_feature_help_reply):
        out = fn(user_text, reply_lang_hint)
        if out:
            return out
    return None


def _ai_chat_ollama_reply_blocking(history: list[dict], user_text: str, reply_lang_hint: str, lang_ui: str) -> tuple[str, str]:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv(
        "AI_CHAT_OLLAMA_MODEL",
        os.getenv("TTS_OLLAMA_MODEL", os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b")),
    )
    timeout_s = float(os.getenv("AI_CHAT_OLLAMA_TIMEOUT", "60"))
    payload = {
        "model": model,
        "prompt": _ai_chat_build_prompt(history, user_text, reply_lang_hint, lang_ui),
        "stream": False,
        "keep_alive": "10m",
        "options": {"temperature": 0.1, "num_predict": 500},
    }
    req = urllib.request.Request(
        f"{base_url}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    out = str((data or {}).get("response") or "").strip()
    if not out:
        raise RuntimeError("empty ollama response")
    out = _ai_chat_postprocess_reply(out, user_text)
    return out, model


async def _ai_chat_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str):
    _ai_chat_clear_session(context)
    session = {
        "user_id": update.effective_user.id if update.effective_user else None,
        "active": True,
        "history": [],
        "expires_at": time.time() + 3600,
    }
    _ai_chat_save_session(context, session)
    msgs = _ai_chat_texts(lang_ui)
    uid = update.effective_user.id if update.effective_user else None
    await _send_with_retry(lambda: target_message.reply_text(msgs["greeting"]))
    _ai_schedule_counter_increment(context, "ai_chat_sessions", 1)


async def _ai_chat_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str) -> bool:
    if not update.message or not update.message.text:
        return False
    session = _ai_chat_get_session(context)
    if not session or not bool(session.get("active")):
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False
    msgs = _ai_chat_texts(lang_ui)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _ai_chat_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(msgs["expired"], reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
        return True
    user_text = (update.message.text or "").strip()
    action_btn = _ai_active_mode_button_action(user_text, lang_ui)
    if action_btn == "noop":
        return True
    if action_btn == "change":
        _ai_chat_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        context.user_data["main_menu_section"] = "ai_tools"
        await update.message.reply_text(
            MESSAGES.get(lang_ui, MESSAGES.get("en", {})).get("menu_ai_tools_placeholder", "Choose an AI feature 👇"),
            reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid),
        )
        return True
    if action_btn == "exit":
        _ai_chat_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        context.user_data["main_menu_section"] = "main"
        await update.message.reply_text(msgs["cancelled"], reply_markup=_main_menu_keyboard(lang_ui, "main", uid))
        return True

    if not user_text:
        await update.message.reply_text(msgs["empty"])
        return True
    if user_text.lower() in {"cancel", "stop"}:
        _ai_chat_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(msgs["cancelled"], reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
        return True

    session["expires_at"] = time.time() + 3600
    history = list(session.get("history") or [])
    reply_lang_hint = _ai_chat_guess_reply_lang(user_text, lang_ui)
    forced_answer = _ai_chat_builtin_reply(user_text, reply_lang_hint)
    if forced_answer:
        history.append({"role": "user", "content": user_text[:1200]})
        history.append({"role": "assistant", "content": forced_answer[:2000]})
        session["history"] = _ai_chat_trim_history(history, max_items=14, max_chars=9000)
        _ai_chat_save_session(context, session)
        await _send_with_retry(lambda: update.message.reply_text(forced_answer))
        return True
    status_msg = await _send_with_retry(lambda: update.message.reply_text(msgs["thinking"]))
    try:
        answer, model_name = await run_blocking_heavy(_ai_chat_ollama_reply_blocking, history, user_text, reply_lang_hint, lang_ui)
    except Exception as e:
        logger.info("ai_chat ollama unavailable/failure: %s", e)
        fail_text = msgs["unavailable"] if any(k in str(e).lower() for k in ["urlopen", "connection", "refused", "timed out"]) else msgs["failed"]
        if status_msg:
            try:
                await status_msg.edit_text(fail_text)
            except Exception:
                pass
        else:
            await update.message.reply_text(fail_text)
        return True

    answer = _ai_chat_add_caution_notice(answer, user_text, reply_lang_hint)

    history.append({"role": "user", "content": user_text[:1200]})
    history.append({"role": "assistant", "content": answer[:2000]})
    session["history"] = _ai_chat_trim_history(history, max_items=14, max_chars=9000)
    _ai_chat_save_session(context, session)

    await _send_with_retry(lambda: update.message.reply_text(answer))
    if status_msg:
        try:
            await status_msg.delete()
        except Exception:
            pass
    return True



def _ai_tool_grammar_fix_blocking(user_text: str, reply_lang_hint: str) -> str:
    prompt = (
        "You are a grammar and spelling fixer.\n"
        "Correct grammar, spelling, punctuation, and style lightly.\n"
        "Keep original meaning and language.\n"
        "Return ONLY the corrected text.\n"
        f"Language hint: {reply_lang_hint}\n\n"
        f"Text:\n{str(user_text or '')[:5000]}"
    )
    out, _ = _ai_tools_ollama_generate_blocking(prompt, temperature=0.0, num_predict=900)
    return _ai_chat_postprocess_reply(out, user_text)


def _ai_tool_email_writer_blocking(user_text: str, reply_lang_hint: str) -> str:
    prompt = (
        "You are an email writing assistant.\n"
        "Write a clear, polite, practical email draft based on user request.\n"
        "Keep language aligned with user language hint.\n"
        "Return ONLY the email text.\n"
        f"Language hint: {reply_lang_hint}\n\n"
        f"Request:\n{str(user_text or '')[:5000]}"
    )
    out, _ = _ai_tools_ollama_generate_blocking(prompt, temperature=0.2, num_predict=1100)
    return _ai_chat_postprocess_reply(out, user_text)


def _ai_music_backend() -> str:
    return "synth"


def _ai_music_pcm16_to_wav_bytes(pcm_bytes: bytes, sample_rate: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(max(8000, int(sample_rate or 22050)))
        wf.writeframes(bytes(pcm_bytes or b""))
    return buf.getvalue()


def _ai_tool_music_generate_wav_synth_blocking(prompt: str, duration_s: int, style_key: str) -> bytes:
    # Rich fallback synthesizer: deterministic per-prompt composition (no simple speed reuse).
    duration_s = max(5, min(60, int(duration_s or 15)))
    style = str(style_key or "lofi").lower()
    sample_rate = 22050
    total_samples = int(sample_rate * duration_s)

    style_profiles = {
        "lofi": {"tempo": 78, "gain": 0.31, "kick_hz": 52.0, "hat": 0.09, "bright": 0.11, "root": 57},
        "romantic": {"tempo": 92, "gain": 0.30, "kick_hz": 58.0, "hat": 0.07, "bright": 0.13, "root": 60},
        "calm": {"tempo": 66, "gain": 0.27, "kick_hz": 46.0, "hat": 0.05, "bright": 0.08, "root": 55},
        "epic": {"tempo": 120, "gain": 0.33, "kick_hz": 62.0, "hat": 0.12, "bright": 0.17, "root": 53},
    }
    profile = style_profiles.get(style, style_profiles["lofi"])

    clean_prompt = re.sub(r"\s+", " ", str(prompt or "").strip().lower())
    tokens = re.findall(r"[a-z0-9']+", clean_prompt)[:48]
    if not tokens:
        tokens = ["instrumental"]
    seed = hashlib.sha256(f"{style}|{clean_prompt}".encode("utf-8", errors="ignore")).digest()

    mood_minor_words = {"sad", "dark", "lonely", "night", "tension", "dramatic", "epic", "sokin", "g'amgin", "спокой", "груст"}
    use_minor = (style in {"calm", "epic"}) or any(tok in mood_minor_words for tok in tokens)
    scale = [0, 2, 3, 5, 7, 8, 10] if use_minor else [0, 2, 4, 5, 7, 9, 11]

    root_midi = int(profile["root"]) + int(seed[5] % 12) - 6
    root_midi = max(42, min(72, root_midi))
    tempo = int(profile["tempo"]) + int(seed[6] % 11) - 5 + min(10, len(tokens) // 2)
    tempo = max(58, min(155, tempo))
    beat_len = 60.0 / float(tempo)
    step_len = beat_len / (3.0 if style == "calm" else 4.0)
    bar_len = beat_len * 4.0
    swing = 0.06 if style in {"lofi", "romantic"} else (0.02 if style == "calm" else 0.0)

    token_hashes: list[int] = []
    for tok in tokens:
        h = int(hashlib.blake2s(tok.encode("utf-8", errors="ignore"), digest_size=4).hexdigest(), 16)
        token_hashes.append(h)
    motif_len = max(8, min(32, len(token_hashes) * 2))
    motif_freq: list[float] = []
    for idx in range(motif_len):
        h = token_hashes[idx % len(token_hashes)] ^ (int(seed[(idx * 5) % 32]) << 8)
        interval = scale[h % len(scale)]
        octave = (h >> 6) % 3  # 0..2 -> low/mid/high
        midi = root_midi + interval + (octave - 1) * 12
        midi = max(30, min(88, midi))
        freq = 440.0 * (2.0 ** ((float(midi) - 69.0) / 12.0))
        motif_freq.append(freq)

    kick_hz = float(profile["kick_hz"]) + float((seed[8] % 8) - 4)
    gain = float(profile["gain"])
    hat_amount = float(profile["hat"])
    bright = float(profile["bright"])

    pcm = bytearray()
    for i in range(total_samples):
        t = float(i) / float(sample_rate)
        beat_pos = t / beat_len
        beat_int = int(beat_pos)
        beat_phase = beat_pos - beat_int

        step_raw = t / step_len
        step_int = int(step_raw)
        step_phase = step_raw - step_int
        swing_shift = swing if (step_int % 2 == 1) else 0.0
        motif_idx = int((step_raw + swing_shift) % len(motif_freq))

        f = motif_freq[motif_idx]
        f_alt = motif_freq[(motif_idx + 5) % len(motif_freq)] * (0.5 if style != "epic" else 1.0)

        wobble = 1.0 + 0.015 * math.sin(2.0 * math.pi * (0.35 + bright) * t)
        lead_env = math.exp(-3.6 * step_phase)
        lead = math.sin(2.0 * math.pi * (f * wobble) * t + 0.27 * math.sin(2.0 * math.pi * (f * 0.5) * t)) * lead_env

        pad_phase = (t % bar_len) / bar_len
        pad_env = 0.45 - 0.30 * math.cos(2.0 * math.pi * pad_phase)
        pad = (
            0.50 * math.sin(2.0 * math.pi * (f * 0.5) * t)
            + 0.30 * math.sin(2.0 * math.pi * (f * 1.5) * t + 0.1)
            + 0.20 * math.sin(2.0 * math.pi * (f_alt * 0.75) * t + 0.2)
        ) * pad_env

        bass_freq = max(35.0, min(180.0, f_alt * 0.5))
        bass_env = math.exp(-5.0 * beat_phase)
        bass = math.sin(2.0 * math.pi * bass_freq * t) * bass_env

        kick_env = math.exp(-30.0 * beat_phase)
        kick = math.sin(2.0 * math.pi * kick_hz * t) * kick_env if beat_phase < 0.30 else 0.0

        noise_seed = ((i * 1103515245) + int.from_bytes(seed[12:16], "little")) & 0x7FFFFFFF
        noise = (float(noise_seed) / 1073741824.0) - 1.0
        half_beat = beat_len * 0.5
        hat_phase = (t % half_beat) / half_beat if half_beat > 0 else 0.0
        hat_env = math.exp(-35.0 * hat_phase)
        hat = noise * hat_env * hat_amount

        sample = (0.55 * lead) + (0.26 * pad) + (0.23 * bass) + (0.19 * kick) + (0.12 * hat)
        if style == "epic":
            sample += 0.06 * math.sin(2.0 * math.pi * 110.0 * t)
        if style == "calm":
            sample *= 0.86

        fade_in = min(1.0, t / 0.35)
        fade_out = min(1.0, max(0.0, (duration_s - t) / 0.55))
        env = min(fade_in, fade_out)
        out = max(-1.0, min(1.0, sample * gain * env))
        pcm.extend(struct.pack("<h", int(out * 32767.0)))

    return _ai_music_pcm16_to_wav_bytes(bytes(pcm), sample_rate)


def _ai_tool_music_generate_wav_blocking(prompt: str, duration_s: int, style_key: str) -> bytes:
    duration_s = max(5, min(60, int(duration_s or 15)))
    style = str(style_key or "lofi").lower()
    if style not in _AI_MUSIC_STYLE_CHOICES:
        style = _AI_MUSIC_STYLE_CHOICES[0]
    _ai_music_backend()
    return _ai_tool_music_generate_wav_synth_blocking(prompt, duration_s, style)


async def _ai_tool_mode_start_session_from_message(
    target_message,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lang_ui: str,
    mode: str,
):
    if mode not in _AI_TOOL_MODE_KEYS:
        mode = "translator"
    _ai_tool_mode_clear_session(context)
    session = {
        "mode": mode,
        "active": True,
        "user_id": update.effective_user.id if update.effective_user else None,
        "expires_at": time.time() + 3600,
    }
    if mode == "translator":
        session["target_lang"] = "en"
    elif mode == "quiz":
        session["quiz_count"] = int(_AI_QUIZ_COUNT_CHOICES[1])
        session["quiz_interval_s"] = int(_AI_QUIZ_INTERVAL_CHOICES[0])
        session["quiz_source_kind"] = "topic"
    elif mode == "music":
        session["music_kind"] = "sound"
        session["music_duration_s"] = int(_AI_MUSIC_DURATION_CHOICES[1])
        session["music_style"] = _AI_MUSIC_STYLE_CHOICES[0]
    _ai_tool_mode_save_session(context, session)

    msgs = _ai_tool_mode_texts(lang_ui)
    prompt_text = _ai_tool_mode_prompt(mode, lang_ui)
    uid = update.effective_user.id if update.effective_user else None

    if mode == "translator":
        target_lang = str(session.get("target_lang") or "en").lower()
        if target_lang not in {"uz", "ru", "en"}:
            target_lang = "en"
        await _send_with_retry(
            lambda: target_message.reply_text(
                _ai_tool_translator_setup_text(lang_ui, target_lang),
                reply_markup=_ai_tool_translator_target_inline_keyboard(lang_ui, target_lang),
            )
        )
    elif mode == "quiz":
        selected_count = max(1, min(10, int(session.get("quiz_count") or _AI_QUIZ_COUNT_CHOICES[1])))
        selected_interval = max(0, min(120, int(session.get("quiz_interval_s") or _AI_QUIZ_INTERVAL_CHOICES[0])))
        await _send_with_retry(
            lambda: target_message.reply_text(
                _ai_tool_quiz_setup_text(lang_ui, selected_count, selected_interval),
                reply_markup=_ai_tool_quiz_setup_inline_keyboard(selected_count, selected_interval),
            )
        )
    elif mode == "music":
        selected_duration = int(session.get("music_duration_s") or _AI_MUSIC_DURATION_CHOICES[1])
        selected_style = str(session.get("music_style") or _AI_MUSIC_STYLE_CHOICES[0]).lower()
        if selected_style not in _AI_MUSIC_STYLE_CHOICES:
            selected_style = _AI_MUSIC_STYLE_CHOICES[0]
        await _send_with_retry(
            lambda: target_message.reply_text(
                _ai_tool_music_setup_text(lang_ui, selected_style, selected_duration),
                reply_markup=_ai_tool_music_setup_inline_keyboard(
                    lang_ui,
                    selected_style=selected_style,
                    selected_duration=selected_duration,
                ),
            )
        )
    else:
        await _send_with_retry(
            lambda: target_message.reply_text(
                prompt_text,
                reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid),
            )
        )
    try:
        context.user_data["main_menu_section"] = "ai_tools"
    except Exception:
        pass


async def _ai_tool_mode_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang_ui: str) -> bool:
    if not update.message or not update.message.text:
        return False
    session = _ai_tool_mode_get_session(context)
    if not session or not bool(session.get("active")):
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    uid = update.effective_user.id if update.effective_user else None
    msgs = _ai_tool_mode_texts(lang_ui)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _ai_tool_mode_clear_session(context)
        await update.message.reply_text(msgs["expired"], reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
        return True

    user_text = (update.message.text or "").strip()
    btn_action = _ai_active_mode_button_action(user_text, lang_ui)
    if btn_action == "noop":
        return True
    if btn_action == "change":
        _ai_tool_mode_clear_session(context)
        await update.message.reply_text(msgs["done"], reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
        return True
    if btn_action == "exit":
        _ai_tool_mode_clear_session(context)
        await update.message.reply_text(msgs["cancelled"], reply_markup=_main_menu_keyboard(lang_ui, "main", uid))
        return True

    mode = str(session.get("mode") or "")
    session["expires_at"] = time.time() + 3600

    if mode == "translator":
        selected = _ai_tool_translator_target_button_to_lang(user_text, lang_ui)
        if selected:
            session["target_lang"] = selected
            _ai_tool_mode_save_session(context, session)
            await update.message.reply_text(
                msgs.get("translator_target_set", "Target language set: {target}").format(
                    target=_ai_tool_lang_label(selected, lang_ui)
                ),
                reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid),
            )
            return True

        src_explicit, tgt_explicit, body = _ai_tool_parse_translation_langs(user_text)
        if not body:
            await update.message.reply_text(msgs["empty"])
            return True
        target_lang = tgt_explicit or str(session.get("target_lang") or "en")
        source_lang = src_explicit or _ai_tool_guess_translation_source_lang(body, lang_ui, target_lang)
        _ai_tool_mode_save_session(context, session)
        status = await _send_with_retry(lambda: update.message.reply_text(msgs["thinking"]))
        try:
            out = await run_blocking_heavy(
                _ai_tool_translate_with_source_retry_blocking,
                body,
                target_lang,
                source_lang,
                source_explicit=bool(src_explicit),
                ui_lang=lang_ui,
            )
            if status:
                try:
                    await status.edit_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
                except Exception:
                    await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            else:
                await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            _ai_schedule_counter_increment(context, "ai_translator_uses", 1)
        except Exception:
            if status:
                try:
                    await status.edit_text(msgs["failed"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(msgs["failed"])
        return True

    if mode == "grammar":
        if not user_text:
            await update.message.reply_text(msgs["empty"])
            return True
        _ai_tool_mode_save_session(context, session)
        status = await _send_with_retry(lambda: update.message.reply_text(msgs["thinking"]))
        try:
            out = await run_blocking_heavy(_ai_tool_grammar_fix_blocking, user_text, lang_ui)
            if status:
                try:
                    await status.edit_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
                except Exception:
                    await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            else:
                await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            _ai_schedule_counter_increment(context, "ai_grammar_fixes", 1)
        except Exception:
            if status:
                try:
                    await status.edit_text(msgs["failed"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(msgs["failed"])
        return True

    if mode == "email":
        if not user_text:
            await update.message.reply_text(msgs["empty"])
            return True
        _ai_tool_mode_save_session(context, session)
        status = await _send_with_retry(lambda: update.message.reply_text(msgs["thinking"]))
        try:
            out = await run_blocking_heavy(_ai_tool_email_writer_blocking, user_text, lang_ui)
            if status:
                try:
                    await status.edit_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
                except Exception:
                    await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            else:
                await update.message.reply_text(out, reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid))
            _ai_schedule_counter_increment(context, "ai_email_writes", 1)
        except Exception:
            if status:
                try:
                    await status.edit_text(msgs["failed"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(msgs["failed"])
        return True

    if mode == "quiz":
        if not user_text:
            await update.message.reply_text(msgs.get("quiz_send_topic_first", msgs["empty"]))
            return True
        count = max(1, min(10, int(session.get("quiz_count") or 5)))
        interval_s = max(0, min(120, int(session.get("quiz_interval_s") or 0)))
        source_kind = str(session.get("quiz_source_kind") or "topic")
        source_text = user_text[:500]
        session["quiz_source_text"] = source_text
        gen_token = uuid.uuid4().hex
        session["quiz_generation_token"] = gen_token
        _ai_tool_mode_save_session(context, session)
        status = await _send_with_retry(lambda: update.message.reply_text(msgs.get("quiz_generating", msgs["thinking"])))
        try:
            questions = await run_blocking_heavy(
                _ai_tool_quiz_generate_blocking,
                source_kind,
                source_text,
                count,
                lang_ui,
            )
            cancel_set = _ai_quiz_cancel_tokens(context.application.bot_data)
            if gen_token in cancel_set:
                cancel_set.discard(gen_token)
                if status:
                    try:
                        await status.edit_text(msgs["cancelled"])
                    except Exception:
                        pass
                return True

            quiz_id = uuid.uuid4().hex
            quiz_name = f"AI Quiz: {source_text[:80]}" if source_text else "AI Quiz Test"
            if callable(_db_save_user_quiz) and update.effective_user:
                await run_blocking(
                    _db_save_user_quiz,
                    quiz_id,
                    int(update.effective_user.id),
                    quiz_name,
                    source_kind,
                    source_text,
                    lang_ui,
                    interval_s,
                    questions,
                )

            _ai_quiz_sets(context.application.bot_data)[quiz_id] = {
                "quiz_id": quiz_id,
                "quiz_name": quiz_name,
                "topic": source_text,
                "questions": questions,
                "lang_ui": lang_ui,
                "count": len(questions),
                "interval_s": interval_s,
                "created_at": time.time(),
                "created_chat_id": int(update.effective_chat.id if update.effective_chat else 0),
            }
            _ai_quiz_cleanup_stale(context.application.bot_data)

            minutes = max(1, int(round(len(questions) * 1.5)))
            card_caption = msgs["quiz_card_caption"].format(
                name=quiz_name[:120],
                topic=source_text[:120] or "Quiz",
                count=len(questions),
                minutes=minutes,
                interval=interval_s,
            )
            bot_username = getattr(getattr(context, "bot", None), "username", None)
            if status:
                try:
                    await status.edit_text(msgs["quiz_card_ready"].format(topic=source_text[:120] or "Quiz"))
                except Exception:
                    pass
            await update.message.reply_text(
                card_caption,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=_ai_tool_quiz_card_inline_keyboard(
                    lang_ui=lang_ui,
                    quiz_id=quiz_id,
                    bot_username=bot_username,
                    topic=source_text,
                    count=len(questions),
                ),
            )
            _ai_schedule_counter_increment(context, "ai_quiz_generated", 1)
        except Exception as e:
            logger.exception("ai quiz generation failed: %s", e)
            if status:
                try:
                    await status.edit_text(msgs["failed"])
                except Exception:
                    pass
            else:
                await update.message.reply_text(msgs["failed"])
        return True

    if mode == "music":
        if not user_text:
            await update.message.reply_text(msgs.get("music_prompt_hint", msgs["empty"]))
            return True
        duration_s = max(5, min(60, int(session.get("music_duration_s") or _AI_MUSIC_DURATION_CHOICES[1])))
        style_key = str(session.get("music_style") or _AI_MUSIC_STYLE_CHOICES[0]).lower()
        if style_key not in _AI_MUSIC_STYLE_CHOICES:
            style_key = _AI_MUSIC_STYLE_CHOICES[0]
        gen_token = uuid.uuid4().hex
        session["music_generation_token"] = gen_token
        _ai_tool_mode_save_session(context, session)
        status = await _send_with_retry(lambda: update.message.reply_text(msgs.get("music_generating", msgs["thinking"])))
        try:
            wav_bytes = await run_blocking_heavy(_ai_tool_music_generate_wav_blocking, user_text, duration_s, style_key)
            cancel_set = _ai_music_cancel_tokens(context.application.bot_data)
            if gen_token in cancel_set:
                cancel_set.discard(gen_token)
                if status:
                    try:
                        await status.edit_text(msgs["cancelled"])
                    except Exception:
                        pass
                return True

            style_label = _ai_tool_music_style_label(style_key, lang_ui)
            caption = msgs.get("music_caption", "🎵 AI music\n⏱️ {seconds}s\n📝 {prompt}").format(
                seconds=duration_s,
                prompt=user_text[:140],
            ) + f"\n🎨 {style_label}"

            audio_bio = io.BytesIO(wav_bytes)
            audio_bio.name = "ai_music.wav"
            await _send_with_retry(
                lambda: update.message.reply_audio(
                    audio=audio_bio,
                    caption=caption,
                    title="AI Music",
                    performer="SmartAIToolsBot",
                    reply_markup=_main_menu_keyboard(lang_ui, "ai_tools", uid),
                )
            )
            if status:
                try:
                    await status.delete()
                except Exception:
                    pass
            _ai_schedule_counter_increment(context, "ai_music_generated", 1)
        except Exception as e:
            logger.exception("ai music generation failed: %s", e)
            fail_text = msgs.get("music_failed", msgs["failed"])
            if status:
                try:
                    await status.edit_text(fail_text)
                except Exception:
                    pass
            else:
                await update.message.reply_text(fail_text)
        return True

    # Unknown or not implemented mode.
    return False


async def handle_ai_tools_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    lang_ui = str(context.user_data.get("language") or "en")
    if lang_ui not in {"uz", "ru", "en"}:
        lang_ui = "en"
    msgs = _ai_tool_mode_texts(lang_ui)

    data = str(query.data or "")
    parts = data.split(":")
    if len(parts) < 2 or parts[0] != "aitool":
        await safe_answer(query)
        return

    action = parts[1]

    if action == "trgt" and len(parts) >= 3:
        target = str(parts[2]).lower()
        if target not in {"uz", "ru", "en"}:
            await safe_answer(query)
            return
        session = _ai_tool_mode_get_session(context) or {}
        session.update(
            {
                "mode": "translator",
                "active": True,
                "target_lang": target,
                "user_id": query.from_user.id if query.from_user else None,
                "expires_at": time.time() + 3600,
            }
        )
        _ai_tool_mode_save_session(context, session)
        await safe_answer(query)
        try:
            await query.edit_message_text(
                _ai_tool_translator_setup_text(lang_ui, target),
                reply_markup=_ai_tool_translator_target_inline_keyboard(lang_ui, target),
            )
        except Exception:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_ai_tool_translator_target_inline_keyboard(lang_ui, target)
                )
            except Exception:
                pass
        return

    if action == "quizcount" and len(parts) >= 3:
        try:
            count = int(parts[2])
        except Exception:
            count = 5
        count = max(1, min(10, count))
        session = _ai_tool_mode_get_session(context) or {}
        session.update(
            {
                "mode": "quiz",
                "active": True,
                "quiz_count": count,
                "user_id": query.from_user.id if query.from_user else None,
                "expires_at": time.time() + 3600,
            }
        )
        _ai_tool_mode_save_session(context, session)
        await safe_answer(query)
        try:
            interval_s = max(0, min(120, int(session.get("quiz_interval_s") or _AI_QUIZ_INTERVAL_CHOICES[0])))
            await query.edit_message_text(
                _ai_tool_quiz_setup_text(lang_ui, count, interval_s),
                reply_markup=_ai_tool_quiz_setup_inline_keyboard(count, interval_s),
            )
        except Exception:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_ai_tool_quiz_setup_inline_keyboard(
                        count,
                        int(session.get("quiz_interval_s") or _AI_QUIZ_INTERVAL_CHOICES[0]),
                    )
                )
            except Exception:
                pass
        return

    if action == "quizinterval" and len(parts) >= 3:
        try:
            interval_s = int(parts[2])
        except Exception:
            interval_s = 0
        interval_s = max(0, min(120, interval_s))
        session = _ai_tool_mode_get_session(context) or {}
        session.update(
            {
                "mode": "quiz",
                "active": True,
                "quiz_interval_s": interval_s,
                "user_id": query.from_user.id if query.from_user else None,
                "expires_at": time.time() + 3600,
            }
        )
        _ai_tool_mode_save_session(context, session)
        await safe_answer(query)
        try:
            count = max(1, min(10, int(session.get("quiz_count") or _AI_QUIZ_COUNT_CHOICES[1])))
            await query.edit_message_text(
                _ai_tool_quiz_setup_text(lang_ui, count, interval_s),
                reply_markup=_ai_tool_quiz_setup_inline_keyboard(count, interval_s),
            )
        except Exception:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_ai_tool_quiz_setup_inline_keyboard(
                        int(session.get("quiz_count") or _AI_QUIZ_COUNT_CHOICES[1]),
                        interval_s,
                    )
                )
            except Exception:
                pass
        return

    if action == "musicdur" and len(parts) >= 3:
        try:
            duration_s = int(parts[2])
        except Exception:
            duration_s = _AI_MUSIC_DURATION_CHOICES[1]
        if duration_s not in _AI_MUSIC_DURATION_CHOICES:
            duration_s = _AI_MUSIC_DURATION_CHOICES[1]
        session = _ai_tool_mode_get_session(context) or {}
        session.update(
            {
                "mode": "music",
                "active": True,
                "music_duration_s": duration_s,
                "user_id": query.from_user.id if query.from_user else None,
                "expires_at": time.time() + 3600,
            }
        )
        _ai_tool_mode_save_session(context, session)
        await safe_answer(query)
        try:
            style_key = str(session.get("music_style") or _AI_MUSIC_STYLE_CHOICES[0]).lower()
            if style_key not in _AI_MUSIC_STYLE_CHOICES:
                style_key = _AI_MUSIC_STYLE_CHOICES[0]
            await query.edit_message_text(
                _ai_tool_music_setup_text(lang_ui, style_key, duration_s),
                reply_markup=_ai_tool_music_setup_inline_keyboard(
                    lang_ui,
                    selected_style=style_key,
                    selected_duration=duration_s,
                ),
            )
        except Exception:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_ai_tool_music_setup_inline_keyboard(
                        lang_ui,
                        selected_style=str(session.get("music_style") or _AI_MUSIC_STYLE_CHOICES[0]),
                        selected_duration=duration_s,
                    )
                )
            except Exception:
                pass
        return

    if action == "musicstyle" and len(parts) >= 3:
        style_key = str(parts[2]).lower()
        if style_key not in _AI_MUSIC_STYLE_CHOICES:
            await safe_answer(query)
            return
        session = _ai_tool_mode_get_session(context) or {}
        session.update(
            {
                "mode": "music",
                "active": True,
                "music_style": style_key,
                "user_id": query.from_user.id if query.from_user else None,
                "expires_at": time.time() + 3600,
            }
        )
        _ai_tool_mode_save_session(context, session)
        await safe_answer(query)
        try:
            duration_s = int(session.get("music_duration_s") or _AI_MUSIC_DURATION_CHOICES[1])
            await query.edit_message_text(
                _ai_tool_music_setup_text(lang_ui, style_key, duration_s),
                reply_markup=_ai_tool_music_setup_inline_keyboard(
                    lang_ui,
                    selected_style=style_key,
                    selected_duration=duration_s,
                ),
            )
        except Exception:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=_ai_tool_music_setup_inline_keyboard(
                        lang_ui,
                        selected_style=style_key,
                        selected_duration=int(session.get("music_duration_s") or _AI_MUSIC_DURATION_CHOICES[1]),
                    )
                )
            except Exception:
                pass
        return

    if action in {"quizstart", "quizgroup", "quizshare"} and len(parts) >= 3:
        quiz_id = str(parts[2] or "").strip()
        quiz = _ai_quiz_sets(context.application.bot_data).get(quiz_id)
        if not quiz and callable(_db_get_user_quiz):
            try:
                db_quiz = await run_blocking(_db_get_user_quiz, quiz_id, None)
            except Exception:
                db_quiz = None
            if db_quiz:
                quiz = {
                    "quiz_id": quiz_id,
                    "quiz_name": db_quiz.get("quiz_name") or "AI Quiz Test",
                    "topic": db_quiz.get("source_preview") or db_quiz.get("quiz_name") or "Quiz",
                    "questions": list(db_quiz.get("questions") or []),
                    "lang_ui": db_quiz.get("lang_ui") or lang_ui,
                    "count": int(db_quiz.get("question_count") or len(db_quiz.get("questions") or [])),
                    "interval_s": int(db_quiz.get("interval_s") or 0),
                    "created_at": time.time(),
                }
                _ai_quiz_sets(context.application.bot_data)[quiz_id] = quiz

        if not quiz:
            await safe_answer(query, msgs.get("quiz_card_expired", "Quiz expired")[:180], show_alert=True)
            return

        if action == "quizgroup":
            await safe_answer(query, msgs.get("quiz_group_hint", "Forward to group")[:180], show_alert=True)
            return

        if action == "quizshare":
            try:
                target_chat_id = query.message.chat_id if query.message else update.effective_chat.id
                await _my_quiz_send_shareable_card(context, target_chat_id, {
                    "id": quiz_id,
                    "quiz_name": quiz.get("quiz_name") or "AI Quiz Test",
                    "source_preview": quiz.get("topic") or "Quiz",
                    "question_count": int(quiz.get("count") or len(quiz.get("questions") or [])),
                    "interval_s": int(quiz.get("interval_s") or 0),
                    "questions": list(quiz.get("questions") or []),
                }, lang_ui)
                await safe_answer(query, msgs.get("quiz_share_hint", "Shared")[:180], show_alert=False)
            except Exception:
                logger.exception("quiz share callback failed")
                await safe_answer(query)
            return

        try:
            chat_id = query.message.chat_id if query.message else update.effective_chat.id
            await _ai_tool_quiz_send_polls(
                context=context,
                chat_id=int(chat_id),
                user_id=int(query.from_user.id if query.from_user else 0),
                lang_ui=str(quiz.get("lang_ui") or lang_ui),
                topic=str(quiz.get("topic") or "Quiz"),
                questions=list(quiz.get("questions") or []),
                interval_s=int(quiz.get("interval_s") or 0),
            )
            if callable(_db_mark_user_quiz_started):
                try:
                    await run_blocking(_db_mark_user_quiz_started, quiz_id)
                except Exception:
                    logger.exception("quiz mark started failed")
            await safe_answer(query)
        except Exception:
            logger.exception("quiz start callback failed")
            await safe_answer(query, msgs.get("failed", "Failed")[:180], show_alert=True)
        return

    await safe_answer(query)
