from __future__ import annotations

import io
import json
import logging
import os
import re
import socket
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

MESSAGES: dict[str, dict[str, str]] = {}
logger = logging.getLogger(__name__)


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v


def _pdf_maker_safe_font_name() -> str:
    helper = globals().get("_ensure_dupes_pdf_font")
    if callable(helper):
        try:
            name = str(helper() or "").strip()
            if name:
                return name
        except Exception:
            logger.debug("pdf_maker font helper unavailable, falling back to Helvetica", exc_info=True)
    return "Helvetica"


def _pdf_maker_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "prompt_name": "📄 PDF Maker\n\nPDF nomini yuboring.\nMasalan: Mening konspektim",
            "prompt_style": "2) PDF uslubini tanlang:\n\n📦 Nom: {name}",
            "prompt_options": "5) Tayyor.\n\n📦 Nom: {name}\n🎨 Uslub: {style}\n📐 O‘lcham: {paper}\n🧭 Yo‘nalish: {orientation}\n\n`Davom etish` ni bosing, keyin matn yuborasiz.",
            "prompt_paper": "3) Qog‘oz o‘lchamini tanlang:\n\n📦 Nom: {name}\n🎨 Uslub: {style}",
            "prompt_orientation": "4) Sahifa yo‘nalishini tanlang:\n\n📦 Nom: {name}\n🎨 Uslub: {style}\n📐 O‘lcham: {paper}",
            "prompt_title_page": "5) Alohida title page kerakmi?\n\n📦 Nom: {name}\n🎨 Uslub: {style}\n📐 O‘lcham: {paper}\n🧭 Yo‘nalish: {orientation}",
            "prompt_text": "📝 Endi matn yuboring.\nQo‘shimcha matn yuborishingiz mumkin.",
            "prompt_text_confirm": "✅ Matn qabul qilindi.\n\n🧾 Belgilar: {chars}\n📄 Qatorlar: {lines}\n\nAgar davom ettirmoqchi bo‘lsangiz yana matn yuboring.\nTayyor bo‘lsangiz pastdagi tugmani bosing.",
            "options_wait_tap": "Iltimos, `Davom etish` tugmasini bosing.",
            "style_wait_tap": "Iltimos, uslubni tugma orqali tanlang.",
            "paper_wait_tap": "Iltimos, qog‘oz o‘lchamini tugma orqali tanlang.",
            "orientation_wait_tap": "Iltimos, yo‘nalishni tugma orqali tanlang.",
            "title_page_wait_tap": "Iltimos, title page tanlovini tugma orqali tanlang.",
            "text_more_added": "Yana matn qo‘shildi.",
            "confirm_generate": "PDF yaratish",
            "name_saved": "Nom saqlandi: {name}",
            "name_invalid": "Nom bo‘sh bo‘lmasin. Iltimos, PDF nomini yuboring.",
            "name_too_long": "Nom juda uzun. 80 belgigacha yuboring.",
            "style_selected": "Uslub tanlandi: {style}",
            "style_ai_hint": "AI matnni tahlil qilib, matn hajmini avtomatik tanlaydi.",
            "font_size_selected": "Matn hajmi: {font_size}",
            "paper_selected": "O‘lcham tanlandi: {paper}",
            "orientation_selected": "Yo‘nalish tanlandi: {orientation}",
            "title_page_selected": "Title page: {title_page}",
            "cancelled": "PDF maker bekor qilindi.",
            "expired": "PDF maker sessiyasi tugadi. Pastdagi menyudan PDF Maker bo‘limini qayta tanlang.",
            "empty": "Iltimos, PDF uchun matn yuboring.",
            "working": "PDF tayyorlanmoqda...",
            "working_ai": "AI matnni tahlil qilmoqda va PDF tayyorlanmoqda...",
            "done": "PDF tayyor.",
            "too_long": "Matn juda uzun. Iltimos, qisqaroq yuboring (maksimum ~50000 belgi).",
            "unavailable": "PDF yaratish moduli hozir mavjud emas (reportlab o‘rnatilmagan).",
            "caption": "Siz yuborgan matndan PDF",
            "ai_fallback": "Auto (AI) mavjud emas, standart uslub qo‘llandi.",
            "ai_size_applied": "AI matn hajmini tanladi: {size}",
            "ai_size_fallback": "AI mavjud emas, aqlli avtomatik hajm qo‘llandi: {size}",
            "smart_size_applied": "Avtomatik matn hajmi: {size}",
            "style_auto": "AI Style",
            "style_modern": "Modern",
            "style_clean": "Clean",
            "style_formal": "Formal",
            "style_colorful": "Colorful",
            "style_plain": "Simple",
            "paper_a4": "A4",
            "paper_letter": "Letter",
            "orientation_portrait": "Portrait",
            "orientation_landscape": "Landscape",
            "title_page_yes": "Ha",
            "title_page_no": "Yo‘q",
            "continue_to_text": "Davom etish",
            "font_size": "{size}",
            "font_avg_hint": "O‘rtacha",
        }
    if lang == "ru":
        return {
            "prompt_name": "📄 PDF Maker\n\nОтправьте название PDF.\nНапример: Мой конспект",
            "prompt_style": "2) Выберите стиль PDF:\n\n📦 Имя: {name}",
            "prompt_options": "5) Готово.\n\n📦 Имя: {name}\n🎨 Стиль: {style}\n📐 Размер: {paper}\n🧭 Ориентация: {orientation}\n\nНажмите `Продолжить`, затем отправьте текст.",
            "prompt_paper": "3) Выберите размер страницы:\n\n📦 Имя: {name}\n🎨 Стиль: {style}",
            "prompt_orientation": "4) Выберите ориентацию:\n\n📦 Имя: {name}\n🎨 Стиль: {style}\n📐 Размер: {paper}",
            "prompt_title_page": "5) Нужна отдельная титульная страница?\n\n📦 Имя: {name}\n🎨 Стиль: {style}\n📐 Размер: {paper}\n🧭 Ориентация: {orientation}",
            "prompt_text": "📝 Теперь отправьте текст.\nМожно отправить несколько частей.",
            "prompt_text_confirm": "✅ Текст получен.\n\n🧾 Символы: {chars}\n📄 Строки: {lines}\n\nЕсли хотите добавить ещё — отправьте следующий фрагмент.\nКогда готовы, нажмите кнопку ниже.",
            "options_wait_tap": "Пожалуйста, нажмите кнопку `Продолжить`.",
            "style_wait_tap": "Пожалуйста, выберите стиль кнопкой ниже.",
            "paper_wait_tap": "Пожалуйста, выберите размер страницы кнопкой.",
            "orientation_wait_tap": "Пожалуйста, выберите ориентацию кнопкой.",
            "title_page_wait_tap": "Пожалуйста, выберите вариант титульной страницы кнопкой.",
            "text_more_added": "Добавлен ещё один фрагмент текста.",
            "confirm_generate": "Создать PDF",
            "name_saved": "Название сохранено: {name}",
            "name_invalid": "Название не должно быть пустым. Отправьте название PDF.",
            "name_too_long": "Название слишком длинное. До 80 символов.",
            "style_selected": "Стиль выбран: {style}",
            "style_ai_hint": "AI проанализирует текст и автоматически выберет размер текста.",
            "font_size_selected": "Размер текста: {font_size}",
            "paper_selected": "Размер выбран: {paper}",
            "orientation_selected": "Ориентация выбрана: {orientation}",
            "title_page_selected": "Титульная страница: {title_page}",
            "cancelled": "PDF maker отменен.",
            "expired": "Сессия PDF maker истекла. Снова выберите PDF Maker в меню ниже.",
            "empty": "Пожалуйста, отправьте текст для PDF.",
            "working": "Создаю PDF...",
            "working_ai": "AI анализирует текст и создаёт PDF...",
            "done": "PDF готов.",
            "too_long": "Текст слишком длинный. Отправьте короче (примерно до 50000 символов).",
            "unavailable": "Модуль создания PDF сейчас недоступен (reportlab не установлен).",
            "caption": "PDF из вашего текста",
            "ai_fallback": "Auto (AI) недоступен, применён стандартный стиль.",
            "ai_size_applied": "AI выбрал размер текста: {size}",
            "ai_size_fallback": "AI недоступен, применён умный авторазмер: {size}",
            "smart_size_applied": "Авторазмер текста: {size}",
            "style_auto": "AI Style",
            "style_modern": "Modern",
            "style_clean": "Clean",
            "style_formal": "Formal",
            "style_colorful": "Colorful",
            "style_plain": "Simple",
            "paper_a4": "A4",
            "paper_letter": "Letter",
            "orientation_portrait": "Portrait",
            "orientation_landscape": "Landscape",
            "title_page_yes": "Да",
            "title_page_no": "Нет",
            "continue_to_text": "Продолжить",
            "font_size": "{size}",
            "font_avg_hint": "Средний",
        }
    return {
        "prompt_name": "📄 PDF Maker\n\nSend a PDF file name.\nExample: My Notes",
        "prompt_style": "2) Choose a PDF style:\n\n📦 Name: {name}",
        "prompt_options": "5) Ready.\n\n📦 Name: {name}\n🎨 Style: {style}\n📐 Size: {paper}\n🧭 Orientation: {orientation}\n\nTap `Continue`, then send the text.",
        "prompt_paper": "3) Choose paper size:\n\n📦 Name: {name}\n🎨 Style: {style}",
        "prompt_orientation": "4) Choose page orientation:\n\n📦 Name: {name}\n🎨 Style: {style}\n📐 Size: {paper}",
        "prompt_title_page": "5) Add a separate title page?\n\n📦 Name: {name}\n🎨 Style: {style}\n📐 Size: {paper}\n🧭 Orientation: {orientation}",
        "prompt_text": "📝 Now send text.\nYou can send multiple parts.",
        "prompt_text_confirm": "✅ Text received.\n\n🧾 Characters: {chars}\n📄 Lines: {lines}\n\nIf you want to continue, send more text.\nWhen ready, tap the button below.",
        "options_wait_tap": "Please tap the `Continue` button.",
        "style_wait_tap": "Please choose a style using the buttons below.",
        "paper_wait_tap": "Please choose paper size using the buttons below.",
        "orientation_wait_tap": "Please choose page orientation using the buttons below.",
        "title_page_wait_tap": "Please choose title page option using the buttons below.",
        "text_more_added": "Added another text part.",
        "confirm_generate": "Generate PDF",
        "name_saved": "Name saved: {name}",
        "name_invalid": "Name cannot be empty. Please send a PDF name.",
        "name_too_long": "Name is too long. Please keep it under 80 characters.",
        "style_selected": "Style selected: {style}",
        "style_ai_hint": "AI will analyze the text and choose the body text size automatically.",
        "font_size_selected": "Text size: {font_size}",
        "paper_selected": "Paper size selected: {paper}",
        "orientation_selected": "Orientation selected: {orientation}",
        "title_page_selected": "Title page: {title_page}",
        "cancelled": "PDF maker cancelled.",
        "expired": "PDF maker session expired. Please choose PDF Maker again from the menu below.",
        "empty": "Please send text to create a PDF.",
        "working": "Creating PDF...",
        "working_ai": "AI is analyzing the text and creating the PDF...",
        "done": "PDF is ready.",
        "too_long": "Text is too long. Please send a shorter text (about 50,000 chars max).",
        "unavailable": "PDF generator is unavailable right now (reportlab is not installed).",
        "caption": "PDF from your text",
        "ai_fallback": "Auto (AI) is unavailable, used the default style.",
        "ai_size_applied": "AI selected text size: {size}",
        "ai_size_fallback": "AI unavailable, used smart auto size: {size}",
        "smart_size_applied": "Auto text size: {size}",
        "style_auto": "AI Style",
        "style_modern": "Modern",
        "style_clean": "Clean",
        "style_formal": "Formal",
        "style_colorful": "Colorful",
        "style_plain": "Simple",
        "paper_a4": "A4",
        "paper_letter": "Letter",
        "orientation_portrait": "Portrait",
        "orientation_landscape": "Landscape",
        "title_page_yes": "Yes",
        "title_page_no": "No",
        "continue_to_text": "Continue",
        "font_size": "{size}",
        "font_avg_hint": "Average",
    }


_PDF_MAKER_SESSION_KEY = "pdf_maker_session"
_PDF_MAKER_STYLE_KEYS = ("auto", "plain")
_PDF_MAKER_PAPER_KEYS = ("a4", "letter")
_PDF_MAKER_ORIENTATION_KEYS = ("portrait", "landscape")


def _pdf_maker_style_label(style_key: str, lang: str) -> str:
    msgs = _pdf_maker_texts(lang)
    return msgs.get(f"style_{style_key}", style_key.title())


def _pdf_maker_style_keyboard(lang: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(f"🤖 {_pdf_maker_style_label('auto', lang)}", callback_data="pdfmk:style:auto"),
            InlineKeyboardButton(f"📖 {_pdf_maker_style_label('plain', lang)}", callback_data="pdfmk:style:plain"),
        ],
        [
            InlineKeyboardButton("❌ Cancel", callback_data="pdfmk:cancel"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def _pdf_maker_paper_label(paper_key: str, lang: str) -> str:
    msgs = _pdf_maker_texts(lang)
    return msgs.get(f"paper_{paper_key}", paper_key.upper())


def _pdf_maker_orientation_label(orientation_key: str, lang: str) -> str:
    msgs = _pdf_maker_texts(lang)
    return msgs.get(f"orientation_{orientation_key}", orientation_key.title())


def _pdf_maker_paper_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"📄 {_pdf_maker_paper_label('a4', lang)}", callback_data="pdfmk:paper:a4"),
            InlineKeyboardButton(f"📑 {_pdf_maker_paper_label('letter', lang)}", callback_data="pdfmk:paper:letter"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="pdfmk:cancel")],
    ])


def _pdf_maker_orientation_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"📱 {_pdf_maker_orientation_label('portrait', lang)}", callback_data="pdfmk:orient:portrait"),
            InlineKeyboardButton(f"🖥️ {_pdf_maker_orientation_label('landscape', lang)}", callback_data="pdfmk:orient:landscape"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data="pdfmk:cancel")],
    ])


def _pdf_maker_generate_confirm_keyboard(lang: str) -> InlineKeyboardMarkup:
    msgs = _pdf_maker_texts(lang)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ {msgs['confirm_generate']}", callback_data="pdfmk:gen:confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="pdfmk:cancel")],
    ])


def _pdf_maker_options_keyboard(session: dict, lang: str) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(f"✅ {_pdf_maker_texts(lang)['continue_to_text']}", callback_data="pdfmk:opt:done"),
        InlineKeyboardButton("❌ Cancel", callback_data="pdfmk:cancel"),
    ]]
    return InlineKeyboardMarkup(rows)


def _pdf_maker_default_theme(style_key: str) -> dict:
    themes = {
        "modern": {
            "style_key": "modern",
            "icon": "✨",
            "header_bg": (0.07, 0.12, 0.18),
            "accent": (0.15, 0.62, 0.95),
            "card_bg": (0.97, 0.98, 1.0),
            "body_color": (0.12, 0.16, 0.20),
            "footer_color": (0.45, 0.52, 0.62),
            "show_border": False,
        },
        "clean": {
            "style_key": "clean",
            "icon": "🧼",
            "header_bg": (0.95, 0.97, 0.99),
            "accent": (0.22, 0.43, 0.65),
            "card_bg": (1.0, 1.0, 1.0),
            "body_color": (0.10, 0.10, 0.12),
            "footer_color": (0.45, 0.48, 0.55),
            "show_border": True,
            "border_color": (0.86, 0.89, 0.93),
            "title_dark": True,
        },
        "formal": {
            "style_key": "formal",
            "icon": "📘",
            "header_bg": (0.10, 0.14, 0.24),
            "accent": (0.78, 0.64, 0.35),
            "card_bg": (0.985, 0.985, 0.98),
            "body_color": (0.11, 0.10, 0.10),
            "footer_color": (0.42, 0.40, 0.38),
            "show_border": True,
            "border_color": (0.84, 0.80, 0.72),
        },
        "colorful": {
            "style_key": "colorful",
            "icon": "🎨",
            "header_bg": (0.14, 0.08, 0.20),
            "accent": (0.96, 0.44, 0.18),
            "accent_2": (0.18, 0.78, 0.72),
            "card_bg": (0.99, 0.98, 0.95),
            "body_color": (0.16, 0.13, 0.12),
            "footer_color": (0.47, 0.40, 0.37),
            "show_border": True,
            "border_color": (0.95, 0.89, 0.76),
        },
        "plain": {
            "style_key": "plain",
            "icon": "📖",
            "header_bg": (1.0, 1.0, 1.0),
            "accent": (0.25, 0.25, 0.25),
            "card_bg": (1.0, 1.0, 1.0),
            "body_color": (0.06, 0.06, 0.06),
            "footer_color": (0.45, 0.45, 0.45),
            "show_border": False,
            "show_header": False,
            "show_card": False,
            "show_footer": False,
            "show_page_numbers": False,
        },
    }
    return dict(themes.get(style_key) or themes["modern"])


def _pdf_maker_theme_from_ai(ai_meta: dict | None) -> dict:
    base_style = str((ai_meta or {}).get("style") or "modern").strip().lower()
    if base_style not in {"modern", "clean", "formal", "colorful", "plain"}:
        base_style = "modern"
    theme = _pdf_maker_default_theme(base_style)
    icon = str((ai_meta or {}).get("icon") or "").strip()
    if icon:
        theme["icon"] = icon[:2]
    accent = (ai_meta or {}).get("accent_rgb")
    if isinstance(accent, (list, tuple)) and len(accent) == 3:
        try:
            theme["accent"] = tuple(max(0.0, min(1.0, float(x))) for x in accent)
        except Exception:
            pass
    if "show_border" in (ai_meta or {}):
        theme["show_border"] = bool((ai_meta or {}).get("show_border"))
    theme["style_key"] = "auto"
    theme["resolved_style"] = base_style
    return theme


def _pdf_maker_build_blocks(text: str) -> list[dict]:
    blocks: list[dict] = []
    raw_lines = (text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    i = 0
    while i < len(raw_lines):
        line = raw_lines[i].strip()
        if not line:
            blocks.append({"type": "spacer"})
            i += 1
            continue
        if line.startswith(">"):
            quote_lines = [line.lstrip("> ").strip()]
            i += 1
            while i < len(raw_lines):
                nxt = raw_lines[i].strip()
                if not nxt.startswith(">"):
                    break
                quote_lines.append(nxt.lstrip("> ").strip())
                i += 1
            blocks.append({"type": "quote", "text": "\n".join([q for q in quote_lines if q])})
            continue
        if line[:1] in {"-", "*", "•"}:
            items = [line[1:].strip() or line]
            i += 1
            while i < len(raw_lines):
                nxt = raw_lines[i].strip()
                if not nxt or nxt[:1] not in {"-", "*", "•"}:
                    break
                items.append(nxt[1:].strip() or nxt)
                i += 1
            blocks.append({"type": "list", "items": items})
            continue
        is_short = len(line) <= 70
        looks_heading = (
            is_short
            and (
                line.endswith(":")
                or (line.isupper() and len(line) >= 4)
                or (line.count(" ") <= 8 and i + 1 < len(raw_lines) and raw_lines[i + 1].strip())
            )
        )
        if looks_heading:
            blocks.append({"type": "heading", "text": line.rstrip(":")})
            i += 1
            continue
        para = [line]
        i += 1
        while i < len(raw_lines):
            nxt = raw_lines[i].strip()
            if not nxt:
                break
            if nxt.startswith(">") or nxt[:1] in {"-", "*", "•"}:
                break
            if len(nxt) <= 70 and (nxt.endswith(":") or nxt.isupper()):
                break
            para.append(nxt)
            i += 1
        blocks.append({"type": "paragraph", "text": " ".join(para).strip()})
    compact: list[dict] = []
    prev_spacer = False
    for b in blocks:
        is_spacer = b.get("type") == "spacer"
        if is_spacer and prev_spacer:
            continue
        compact.append(b)
        prev_spacer = is_spacer
    return compact


def _pdf_wrap_by_width(c, font_name: str, font_size: float, text: str, max_width: float) -> list[str]:
    if not text:
        return [""]
    words = str(text).split()
    if not words:
        return [""]
    c.setFont(font_name, font_size)
    lines: list[str] = []
    cur = words[0]
    for w in words[1:]:
        trial = f"{cur} {w}"
        if c.stringWidth(trial, font_name, font_size) <= max_width:
            cur = trial
        else:
            lines.append(cur)
            cur = w
    lines.append(cur)
    return lines


def _build_modern_text_pdf_bytes(
    text: str,
    title: str | None = None,
    theme: dict | None = None,
    *,
    subtitle: str | None = None,
    blocks: list[dict] | None = None,
    paper_key: str = "a4",
    orientation_key: str = "portrait",
    title_page: bool = False,
    body_font_size: float | None = None,
) -> bytes | None:
    if not canvas or not A4:
        return None
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    title = (title or "PDF Maker").strip()[:80] or "PDF Maker"
    theme = dict(theme or _pdf_maker_default_theme("modern"))
    subtitle = " ".join(((subtitle or "").strip()).split())[:140]
    blocks = list(blocks or _pdf_maker_build_blocks(text))
    show_header = bool(theme.get("show_header", True))
    show_footer = bool(theme.get("show_footer", True))

    paper_key = (paper_key or "a4").lower()
    orientation_key = (orientation_key or "portrait").lower()
    base_size = LETTER if (paper_key == "letter" and LETTER) else A4
    page_size = rl_landscape(base_size) if (orientation_key == "landscape" and rl_landscape) else base_size

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=page_size)
    page_w, page_h = page_size
    font_name = _pdf_maker_safe_font_name()

    margin = 36
    header_h = (86 if subtitle else 72) if show_header else 0
    footer_h = 28 if show_footer else 0
    content_top = page_h - margin - header_h
    content_bottom = margin + footer_h
    content_left = margin
    content_width = page_w - 2 * margin
    body_font_size = float(body_font_size or 16)
    body_font_size = max(13.0, min(32.0, body_font_size))
    body_line_h = max(15, int(round(body_font_size * 1.38)))
    quote_font_size = max(12.0, body_font_size - 1.0)
    quote_line_h = max(14, int(round(quote_font_size * 1.33)))
    title_font_size = 20
    subtitle_font_size = 9.5

    page_no = 1

    def draw_frame(page_number: int, show_title: bool = True):
        header_bg = theme.get("header_bg", (0.07, 0.12, 0.18))
        accent = theme.get("accent", (0.15, 0.62, 0.95))
        card_bg = theme.get("card_bg", (0.97, 0.98, 1.0))
        footer_color = theme.get("footer_color", (0.45, 0.52, 0.62))
        body_color = theme.get("body_color", (0.12, 0.16, 0.2))
        icon = str(theme.get("icon") or "").strip()
        title_dark = bool(theme.get("title_dark"))
        show_border = bool(theme.get("show_border"))
        border_color = theme.get("border_color", (0.88, 0.90, 0.94))
        accent2 = theme.get("accent_2")
        show_header = bool(theme.get("show_header", True))
        show_card = bool(theme.get("show_card", True))
        show_footer = bool(theme.get("show_footer", True))
        show_page_numbers = bool(theme.get("show_page_numbers", True))

        if show_header and header_h > 0:
            c.setFillColorRGB(*header_bg)
            c.rect(0, page_h - header_h - 8, page_w, header_h + 8, stroke=0, fill=1)
            c.setFillColorRGB(*accent)
            c.rect(0, page_h - 8, page_w, 8, stroke=0, fill=1)
            if accent2 and isinstance(accent2, (list, tuple)) and len(accent2) == 3:
                c.setFillColorRGB(*accent2)
                c.rect(page_w * 0.58, page_h - 8, page_w * 0.42, 8, stroke=0, fill=1)

        if show_header and show_title:
            c.setFillColorRGB(*(header_bg if title_dark else (1, 1, 1)))
            if title_dark:
                c.setFillColorRGB(0.08, 0.10, 0.14)
            c.setFont(font_name, title_font_size)
            title_text = f"{icon} {title}" if icon else title
            c.drawString(content_left, page_h - margin - 28, title_text)

        if show_header:
            c.setFont(font_name, 9)
            c.setFillColorRGB(*(footer_color if title_dark else (0.86, 0.9, 0.96)))
            c.drawString(content_left, page_h - margin - 46, f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            if subtitle and show_title:
                c.setFont(font_name, subtitle_font_size)
                c.setFillColorRGB(*(footer_color if title_dark else (0.80, 0.86, 0.93)))
                for idx, ln in enumerate(_pdf_wrap_by_width(c, font_name, subtitle_font_size, subtitle, content_width - 6)[:2]):
                    c.drawString(content_left, page_h - margin - 58 - (idx * 11), ln)

        if show_card:
            c.setFillColorRGB(*card_bg)
            c.roundRect(content_left, content_bottom - 8, content_width, content_top - content_bottom + 16, 10, stroke=0, fill=1)
            if show_border:
                c.setStrokeColorRGB(*border_color)
                c.setLineWidth(1)
                c.roundRect(content_left, content_bottom - 8, content_width, content_top - content_bottom + 16, 10, stroke=1, fill=0)

        if show_footer and show_page_numbers:
            c.setFillColorRGB(*footer_color)
            c.setFont(font_name, 8)
            c.drawRightString(page_w - margin, margin - 2, f"Page {page_number}")
        c.setFillColorRGB(*body_color)

    def new_page(cur_page_no: int):
        c.showPage()
        draw_frame(cur_page_no, show_title=bool(theme.get("repeat_title_each_page", False)))
        c.setFillColorRGB(*theme.get("body_color", (0.12, 0.16, 0.2)))

    def ensure_space(y_pos: float, needed: float, cur_page_no: int) -> tuple[float, int]:
        if y_pos - needed > content_bottom + 6:
            return y_pos, cur_page_no
        cur_page_no += 1
        new_page(cur_page_no)
        return content_top - 16, cur_page_no

    def draw_title_page():
        header_bg = theme.get("header_bg", (0.07, 0.12, 0.18))
        accent = theme.get("accent", (0.15, 0.62, 0.95))
        card_bg = theme.get("card_bg", (0.97, 0.98, 1.0))
        body_color = theme.get("body_color", (0.12, 0.16, 0.2))
        footer_color = theme.get("footer_color", (0.45, 0.52, 0.62))
        icon = str(theme.get("icon") or "").strip()

        c.setFillColorRGB(*header_bg)
        c.rect(0, 0, page_w, page_h, stroke=0, fill=1)
        c.setFillColorRGB(*accent)
        c.rect(0, page_h - 12, page_w, 12, stroke=0, fill=1)
        c.setFillColorRGB(*card_bg)
        box_w = min(page_w - 80, 460)
        box_h = 220 if subtitle else 180
        box_x = (page_w - box_w) / 2
        box_y = (page_h - box_h) / 2
        c.roundRect(box_x, box_y, box_w, box_h, 14, stroke=0, fill=1)
        if theme.get("show_border"):
            c.setStrokeColorRGB(*(theme.get("border_color", (0.88, 0.90, 0.94))))
            c.setLineWidth(1)
            c.roundRect(box_x, box_y, box_w, box_h, 14, stroke=1, fill=0)
        c.setFillColorRGB(*accent)
        c.circle(page_w / 2, box_y + box_h - 34, 14, stroke=0, fill=1)
        c.setFillColorRGB(1, 1, 1)
        c.setFont(font_name, 12)
        c.drawCentredString(page_w / 2, box_y + box_h - 38, (icon or "📄")[:2])
        c.setFillColorRGB(*body_color)
        c.setFont(font_name, 22)
        title_lines = _pdf_wrap_by_width(c, font_name, 22, title, box_w - 40)[:3]
        ty = box_y + box_h - 74
        for ln in title_lines:
            c.drawCentredString(page_w / 2, ty, ln)
            ty -= 26
        if subtitle:
            c.setFillColorRGB(*footer_color)
            c.setFont(font_name, 10)
            for ln in _pdf_wrap_by_width(c, font_name, 10, subtitle, box_w - 60)[:4]:
                c.drawCentredString(page_w / 2, ty - 6, ln)
                ty -= 14
        c.setFillColorRGB(*footer_color)
        c.setFont(font_name, 9)
        c.drawCentredString(page_w / 2, box_y + 18, datetime.now().strftime("%Y-%m-%d %H:%M"))

    if title_page:
        draw_title_page()
        c.showPage()
        page_no = 1

    draw_frame(page_no, show_title=True)
    y = content_top - 16
    body_color = theme.get("body_color", (0.12, 0.16, 0.2))
    accent = theme.get("accent", (0.15, 0.62, 0.95))
    quote_bg = tuple(min(1.0, max(0.0, x + 0.05)) for x in theme.get("card_bg", (0.97, 0.98, 1.0)))

    text_x = content_left + 12
    max_text_w = content_width - 24

    for block in blocks:
        btype = block.get("type")
        if btype == "spacer":
            y -= 8
            continue

        if btype == "heading":
            heading_text = str(block.get("text") or "").strip()
            if heading_text:
                y, page_no = ensure_space(y, 24, page_no)
                c.setFillColorRGB(*accent)
                c.rect(text_x, y - 4, max_text_w * 0.22, 2, stroke=0, fill=1)
                c.setFillColorRGB(*body_color)
                c.setFont(font_name, 13)
                for ln in _pdf_wrap_by_width(c, font_name, 13, heading_text, max_text_w):
                    y, page_no = ensure_space(y, 18, page_no)
                    c.drawString(text_x, y, ln)
                    y -= 16
                y -= 2
            continue

        if btype == "list":
            items = [str(x).strip() for x in (block.get("items") or []) if str(x).strip()]
            for item in items:
                lines = _pdf_wrap_by_width(c, font_name, body_font_size, item, max_text_w - 18)
                needed = (len(lines) * body_line_h) + 2
                y, page_no = ensure_space(y, needed, page_no)
                c.setFillColorRGB(*accent)
                c.circle(text_x + 4, y - 3, 2, stroke=0, fill=1)
                c.setFillColorRGB(*body_color)
                c.setFont(font_name, body_font_size)
                for idx, ln in enumerate(lines):
                    x = text_x + 14
                    c.drawString(x, y, ln)
                    y -= body_line_h
                y -= 2
            continue

        if btype == "quote":
            quote_text = str(block.get("text") or "").strip()
            q_lines = []
            for raw in quote_text.split("\n"):
                q_lines.extend(_pdf_wrap_by_width(c, font_name, 10, raw, max_text_w - 24))
            q_lines = q_lines or [""]
            needed = 12 + len(q_lines) * quote_line_h + 8
            y, page_no = ensure_space(y, needed, page_no)
            box_h = 10 + len(q_lines) * quote_line_h + 6
            c.setFillColorRGB(*quote_bg)
            c.roundRect(text_x, y - box_h + 8, max_text_w, box_h, 8, stroke=0, fill=1)
            c.setFillColorRGB(*accent)
            c.rect(text_x + 6, y - box_h + 12, 3, box_h - 8, stroke=0, fill=1)
            c.setFillColorRGB(*body_color)
            c.setFont(font_name, quote_font_size)
            qy = y - 4
            for ln in q_lines:
                c.drawString(text_x + 16, qy, ln)
                qy -= quote_line_h
            y -= box_h + 2
            continue

        # paragraph (default)
        para_text = str(block.get("text") or "").strip()
        if not para_text:
            continue
        c.setFont(font_name, body_font_size)
        lines = _pdf_wrap_by_width(c, font_name, body_font_size, para_text, max_text_w)
        needed = len(lines) * body_line_h + 4
        y, page_no = ensure_space(y, needed, page_no)
        c.setFillColorRGB(*body_color)
        for ln in lines:
            c.drawString(text_x, y, ln)
            y -= body_line_h
        y -= 4

    c.save()
    return buf.getvalue()


def _build_text_only_pdf_bytes(
    text: str,
    *,
    paper_key: str = "a4",
    orientation_key: str = "portrait",
    body_font_size: float | None = None,
) -> bytes | None:
    if not canvas or not A4:
        return None
    raw_text = (text or "").replace("\r\n", "\n").replace("\r", "\n")

    paper_key = (paper_key or "a4").lower()
    orientation_key = (orientation_key or "portrait").lower()
    base_size = LETTER if (paper_key == "letter" and LETTER) else A4
    page_size = rl_landscape(base_size) if (orientation_key == "landscape" and rl_landscape) else base_size

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=page_size)
    page_w, page_h = page_size
    font_name = _pdf_maker_safe_font_name()

    margin = 42
    max_width = page_w - (2 * margin)
    font_size = float(body_font_size or 15)
    font_size = max(13.0, min(18.0, font_size))
    line_h = max(16, int(round(font_size * 1.48)))
    para_gap = max(6, int(round(font_size * 0.72)))
    line_tail_gap = max(1, int(round(font_size * 0.14)))
    body_color = (0.08, 0.08, 0.08)

    def start_page() -> float:
        c.setFont(font_name, font_size)
        c.setFillColorRGB(*body_color)
        return page_h - margin

    y = start_page()
    blank_streak = 0
    lines_src = raw_text.split("\n")

    for raw_line in lines_src:
        expanded = (raw_line or "").replace("\t", "    ").rstrip("\n\r")
        if not expanded.strip():
            blank_streak += 1
            gap = min(line_h, para_gap + ((blank_streak - 1) * max(2, para_gap // 2)))
            if y - gap < margin:
                c.showPage()
                y = start_page()
            y -= gap
            continue
        blank_streak = 0

        leading_spaces = len(expanded) - len(expanded.lstrip(" "))
        indent_spaces = min(leading_spaces, 16)
        indent_text = " " * indent_spaces
        indent_w = c.stringWidth(indent_text, font_name, font_size) if indent_spaces else 0.0
        content = expanded.lstrip(" ")
        usable_w = max(72.0, max_width - indent_w)
        wrapped = _pdf_wrap_by_width(c, font_name, font_size, content, usable_w) or [""]

        is_short_heading = (
            len(content) <= 70
            and not content.endswith((".", "!", "?", ",", ";"))
            and len(wrapped) == 1
            and ":" in content
        )
        for ln in wrapped:
            if y - line_h < margin:
                c.showPage()
                y = start_page()
            c.drawString(margin + indent_w, y, ln)
            y -= line_h
        y -= (0 if is_short_heading else line_tail_gap)

    c.save()
    return buf.getvalue()


def _pdf_maker_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_PDF_MAKER_SESSION_KEY, None)


def _pdf_maker_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_PDF_MAKER_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _pdf_maker_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_PDF_MAKER_SESSION_KEY] = dict(session)


def _pdf_maker_sanitize_name(name: str) -> str:
    name = " ".join((name or "").strip().split())
    name = re.sub(r"[\\\\/:*?\"<>|]+", "", name)
    return name[:80].strip()


def _pdf_maker_session_labels(session: dict, lang: str) -> dict:
    return {
        "name": _pdf_maker_sanitize_name(session.get("name") or "PDF Maker") or "PDF Maker",
        "style": _pdf_maker_style_label(str(session.get("style") or "plain"), lang),
        "paper": _pdf_maker_paper_label(str(session.get("paper") or "a4"), lang),
        "orientation": _pdf_maker_orientation_label(str(session.get("orientation") or "portrait"), lang),
    }


def _pdf_maker_text_buffer_stats(text: str) -> dict:
    s = str(text or "")
    return {
        "chars": len(s),
        "lines": max(1, len(s.splitlines())) if s else 0,
    }


async def _pdf_maker_send_options_panel(update: Update, context: ContextTypes.DEFAULT_TYPE, session: dict, lang: str):
    labels = _pdf_maker_session_labels(session, lang)
    text = _pdf_maker_texts(lang)["prompt_options"].format(**labels)
    return await _pdf_maker_edit_or_send_prompt(
        update,
        context,
        session,
        text,
        reply_markup=_pdf_maker_options_keyboard(session, lang),
        prefer_edit=True,
    )


async def _pdf_maker_edit_or_send_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    session: dict,
    text: str,
    reply_markup=None,
    prefer_edit: bool = True,
):
    chat_id = session.get("prompt_chat_id")
    msg_id = session.get("prompt_message_id")
    sent = None
    if prefer_edit and chat_id and msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=reply_markup,
            )
            return True
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return True
        except Exception:
            pass

    # Fallback to a new message only if editing the current wizard message fails.
    if chat_id and msg_id:
        try:
            await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except Exception:
            pass
    if update.message:
        try:
            sent = await _send_with_retry(lambda: update.message.reply_text(text, reply_markup=reply_markup))
        except Exception:
            sent = None
    elif update.callback_query and update.callback_query.message:
        try:
            sent = await _send_with_retry(lambda: update.callback_query.message.reply_text(text, reply_markup=reply_markup))
        except Exception:
            sent = None
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _pdf_maker_save_session(context, session)
        return True
    return False


def _pdf_maker_heuristic_auto_meta(text: str) -> dict:
    t = (text or "").lower()
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    bullet_count = sum(1 for ln in lines if ln[:1] in {"-", "*", "•"})
    emoji_like = sum(1 for ch in (text or "") if ord(ch) > 10000)
    subtitle = ""
    if len(lines) >= 2:
        subtitle = " ".join(lines[1:3])[:120]
    elif lines:
        subtitle = (lines[0][:120] if len(lines[0]) > 25 else "")
    if any(k in t for k in ["dua", "duosi"]):
        return {"style": "formal", "icon": "🕌", "accent_rgb": [0.78, 0.64, 0.35], "show_border": True, "subtitle": subtitle}
    if bullet_count >= 3:
        return {"style": "clean", "icon": "📝", "accent_rgb": [0.22, 0.43, 0.65], "show_border": True, "subtitle": subtitle}
    if emoji_like >= 5:
        return {"style": "colorful", "icon": "🎨", "accent_rgb": [0.96, 0.44, 0.18], "show_border": True, "subtitle": subtitle}
    if len(lines) <= 3 and len((text or "").strip()) < 400:
        return {"style": "modern", "icon": "✨", "accent_rgb": [0.15, 0.62, 0.95], "show_border": False, "subtitle": subtitle}
    return {"style": "modern", "icon": "📄", "accent_rgb": [0.15, 0.62, 0.95], "show_border": False, "subtitle": subtitle}


def _pdf_maker_call_ollama_auto_meta(text: str, title: str) -> dict | None:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b")
    timeout_s = float(os.getenv("PDF_MAKER_OLLAMA_TIMEOUT", "35"))
    url = f"{base_url}/api/generate"
    prompt = (
        "Choose the best PDF visual style for this text.\n"
        "Return JSON only with keys: style, icon, accent_rgb, show_border, subtitle.\n"
        "style: one of modern|clean|formal|colorful|plain\n"
        "icon: one emoji\n"
        "accent_rgb: [r,g,b] floats 0..1\n"
        "show_border: boolean\n\n"
        "subtitle: short subtitle max 120 chars (plain text)\n\n"
        f"Title: {title}\n"
        "Text:\n"
        f"{(text or '')[:3500]}"
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "keep_alive": "10m",
        "options": {"temperature": 0.1, "num_predict": 80},
    }
    def _request_once(req_timeout: float):
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=req_timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        data = _request_once(timeout_s)
    except (TimeoutError, socket.timeout):
        # First call may be slow if the model is cold-loading in Ollama.
        data = _request_once(max(timeout_s, 55.0))
    raw = (data or {}).get("response") or ""
    raw = raw.strip()
    if "```" in raw:
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    parsed = json.loads(raw[start : end + 1])
    style = str(parsed.get("style") or "").strip().lower()
    if style not in {"modern", "clean", "formal", "colorful", "plain"}:
        return None
    icon = str(parsed.get("icon") or "").strip()[:2]
    accent = parsed.get("accent_rgb")
    if not (isinstance(accent, list) and len(accent) == 3):
        accent = None
    else:
        accent = [max(0.0, min(1.0, float(x))) for x in accent]
    return {
        "style": style,
        "icon": icon or "📄",
        "accent_rgb": accent or [0.15, 0.62, 0.95],
        "show_border": bool(parsed.get("show_border")),
        "subtitle": " ".join(str(parsed.get("subtitle") or "").split())[:120],
        "source": "ollama",
        "model": model,
    }

def _pdf_maker_extract_subtitle(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    if len(lines) >= 2:
        return " ".join(lines[1:3])[:120]
    sample = lines[0]
    return (sample[:120] if len(sample) > 24 else "")


def _pdf_maker_heuristic_body_font_size(text: str, paper_key: str = "a4", orientation_key: str = "portrait") -> int:
    clean = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    chars = len(clean)
    lines = [ln for ln in clean.split("\n")]
    non_empty = [ln.strip() for ln in lines if ln.strip()]
    line_count = max(1, len(lines))
    avg_line = (sum(len(ln) for ln in non_empty) / len(non_empty)) if non_empty else 0.0
    long_lines = sum(1 for ln in non_empty if len(ln) > 90)
    dense_ratio = long_lines / max(1, len(non_empty))

    size = 15.0
    if orientation_key == "landscape":
        size += 0.5
    if paper_key == "letter":
        size -= 0.2

    if chars > 20000:
        size -= 2.0
    elif chars > 12000:
        size -= 1.3
    elif chars > 7000:
        size -= 0.8
    elif chars < 1800:
        size += 1.0

    if line_count > 300:
        size -= 0.8
    elif line_count < 40:
        size += 0.4

    if avg_line > 75:
        size -= 0.8
    elif avg_line < 35 and chars < 6000:
        size += 0.4

    if dense_ratio > 0.35:
        size -= 0.5

    return max(13, min(18, int(round(size))))


def _pdf_maker_call_ollama_font_size(text: str, paper_key: str, orientation_key: str) -> int | None:
    base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
    model = os.getenv("PDF_MAKER_OLLAMA_MODEL", "qwen2.5:7b")
    timeout_s = float(os.getenv("PDF_MAKER_OLLAMA_TIMEOUT", "35"))
    url = f"{base_url}/api/generate"
    prompt = (
        "Choose the best PDF body text font size for readability.\n"
        "The text can be in any language.\n"
        "Return JSON only: {\"font_size\": integer}\n"
        "Allowed font_size: 13..18\n"
        "Use only content density and readability.\n\n"
        f"Paper: {paper_key}\n"
        f"Orientation: {orientation_key}\n"
        "Text:\n"
        f"{(text or '')[:5000]}"
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "keep_alive": "10m",
        "options": {"temperature": 0.1, "num_predict": 40},
    }

    def _request_once(req_timeout: float):
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=req_timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        data = _request_once(timeout_s)
    except (TimeoutError, socket.timeout):
        data = _request_once(max(timeout_s, 55.0))

    raw = str((data or {}).get("response") or "").strip()
    if "```" in raw:
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    parsed = json.loads(raw[start : end + 1])
    try:
        size = int(parsed.get("font_size"))
    except Exception:
        return None
    return max(13, min(18, size))


def _pdf_maker_resolve_body_font_size(text: str, style_key: str, paper_key: str, orientation_key: str) -> tuple[int, bool]:
    style_key = (style_key or "plain").lower()
    heuristic = _pdf_maker_heuristic_body_font_size(text, paper_key, orientation_key)
    if style_key != "auto":
        return heuristic, False
    try:
        ai_size = _pdf_maker_call_ollama_font_size(text, paper_key, orientation_key)
        if ai_size is not None:
            return ai_size, False
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as e:
        logger.info("pdf_maker ai font-size fallback (ollama unavailable): %s", e)
    except Exception as e:
        logger.warning("pdf_maker ai font-size via ollama failed: %s", e)
    return heuristic, True


def _pdf_maker_resolve_auto_theme(text: str, title: str) -> tuple[dict, bool, dict]:
    ai_meta = None
    try:
        ai_meta = _pdf_maker_call_ollama_auto_meta(text, title)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as e:
        logger.info("pdf_maker auto style fallback (ollama unavailable): %s", e)
    except Exception as e:
        logger.warning("pdf_maker auto style via ollama failed: %s", e)
    if ai_meta:
        meta = {"subtitle": " ".join(str(ai_meta.get("subtitle") or "").split())[:120]}
        if not meta["subtitle"]:
            meta["subtitle"] = _pdf_maker_extract_subtitle(text)
        return _pdf_maker_theme_from_ai(ai_meta), False, meta
    heuristic = _pdf_maker_heuristic_auto_meta(text)
    return _pdf_maker_theme_from_ai(heuristic), True, {"subtitle": " ".join(str(heuristic.get("subtitle") or "").split())[:120]}


def _pdf_maker_theme_for_selected_style(style_key: str, text: str, title: str) -> tuple[dict, bool, dict]:
    style_key = (style_key or "modern").lower()
    if style_key == "auto":
        return _pdf_maker_resolve_auto_theme(text, title)
    return (
        _pdf_maker_default_theme(style_key if style_key in {"modern", "clean", "formal", "colorful", "plain"} else "modern"),
        False,
        {"subtitle": _pdf_maker_extract_subtitle(text)},
    )


async def _reply_pdf_document(update: Update, pdf_bytes: bytes, filename: str, caption: str | None = None):
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return None
    bio = io.BytesIO(pdf_bytes)
    bio.name = filename
    try:
        return await _send_with_retry(lambda: target_message.reply_document(document=bio, caption=caption))
    except Exception as e:
        logger.error(f"Failed to send generated PDF: {e}")
        return None


async def _pdf_maker_send_text_as_pdf(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
    text: str,
    *,
    title: str | None = None,
    style_key: str = "plain",
    paper_key: str = "a4",
    orientation_key: str = "portrait",
) -> bool:
    msgs = _pdf_maker_texts(lang)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return False
    if not canvas or not A4:
        await target_message.reply_text(msgs["unavailable"])
        return False
    clean = (text or "").strip()
    if not clean:
        await target_message.reply_text(msgs["empty"])
        return False
    if len(clean) > 50000:
        await target_message.reply_text(msgs["too_long"])
        return False

    # Enqueue background job instead of processing synchronously
    user_id = target_message.chat_id
    job_data = {
        "text": clean,
        "title": title,
        "style_key": style_key,
        "paper_key": paper_key,
        "orientation_key": orientation_key,
        "lang": lang,
    }
    job_id = db_enqueue_background_job("pdf_maker", user_id, job_data)
    if not job_id:
        await target_message.reply_text(msgs["unavailable"])
        return False

    # Send queued confirmation
    await target_message.reply_text("✅ PDF generation queued! You'll receive the file soon.")
    return True


async def _pdf_maker_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message or not update.message.text:
        return False
    session = _pdf_maker_get_session(context)
    if not session:
        return False

    if time.time() > float(session.get("expires_at", 0) or 0):
        _pdf_maker_clear_session(context)
        await update.message.reply_text(_pdf_maker_texts(lang)["expired"])
        return True

    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    msgs = _pdf_maker_texts(lang)
    user_text = (update.message.text or "").strip()
    if user_text.lower() in {"cancel", "stop"}:
        _pdf_maker_clear_session(context)
        await update.message.reply_text(msgs["cancelled"])
        return True

    phase = str(session.get("phase") or "")
    if phase == "awaiting_name":
        pdf_name = _pdf_maker_sanitize_name(user_text)
        if not pdf_name:
            await update.message.reply_text(msgs["name_invalid"])
            return True
        if len(pdf_name) > 80:
            await update.message.reply_text(msgs["name_too_long"])
            return True
        session["name"] = pdf_name
        session["phase"] = "awaiting_style"
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        labels = _pdf_maker_session_labels(session, lang)
        await _pdf_maker_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["prompt_style"].format(**labels),
            reply_markup=_pdf_maker_style_keyboard(lang),
        )
        return True

    if phase == "awaiting_options":
        await update.message.reply_text(msgs.get("options_wait_tap", msgs["style_wait_tap"]))
        return True

    if phase == "awaiting_style":
        await update.message.reply_text(msgs["style_wait_tap"])
        return True

    if phase == "awaiting_paper":
        await update.message.reply_text(msgs["paper_wait_tap"])
        return True

    if phase == "awaiting_orientation":
        await update.message.reply_text(msgs["orientation_wait_tap"])
        return True

    if phase == "awaiting_text":
        session["text_buffer"] = user_text
        session["phase"] = "awaiting_text_confirm"
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        stats = _pdf_maker_text_buffer_stats(session.get("text_buffer") or "")
        await _pdf_maker_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["prompt_text_confirm"].format(**stats),
            reply_markup=_pdf_maker_generate_confirm_keyboard(lang),
            prefer_edit=False,
        )
        return True

    if phase == "awaiting_text_confirm":
        current = str(session.get("text_buffer") or "")
        session["text_buffer"] = f"{current}\n{user_text}".strip() if current else user_text
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        stats = _pdf_maker_text_buffer_stats(session.get("text_buffer") or "")
        await _pdf_maker_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["prompt_text_confirm"].format(**stats),
            reply_markup=_pdf_maker_generate_confirm_keyboard(lang),
            prefer_edit=False,
        )
        return True

    _pdf_maker_clear_session(context)
    return False


async def pdf_maker_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if not update.message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)

    msgs = _pdf_maker_texts(lang)
    text_arg = " ".join(context.args or []).strip()
    if text_arg:
        _pdf_maker_clear_session(context)
        await _pdf_maker_send_text_as_pdf(update, context, lang, text_arg, title="PDF Maker", style_key="plain")
        return
    await _pdf_maker_start_session_from_message(update.message, update, context, lang)


async def _pdf_maker_start_session_from_message(target_message, update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    session = {
        "user_id": update.effective_user.id if update.effective_user else None,
        "phase": "awaiting_name",
        "style": "plain",
        "paper": "a4",
        "orientation": "portrait",
        "expires_at": time.time() + 1800,
    }
    _pdf_maker_save_session(context, session)
    msgs = _pdf_maker_texts(lang)
    sent = await _send_with_retry(lambda: target_message.reply_text(msgs["prompt_name"]))
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _pdf_maker_save_session(context, session)


async def handle_pdf_maker_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = str(query.data or "")
    if not data.startswith("pdfmk:"):
        await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    msgs = _pdf_maker_texts(lang)
    session = _pdf_maker_get_session(context)
    if not session:
        await safe_answer(query, msgs["expired"], show_alert=True)
        return
    if time.time() > float(session.get("expires_at", 0) or 0):
        _pdf_maker_clear_session(context)
        await safe_answer(query, msgs["expired"], show_alert=True)
        return
    if (query.from_user.id if query.from_user else None) != session.get("user_id"):
        await safe_answer(query, "This PDF maker session belongs to another user.", show_alert=True)
        return

    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""
    if action == "cancel":
        _pdf_maker_clear_session(context)
        await safe_answer(query, msgs["cancelled"])
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            await query.message.reply_text(msgs["cancelled"])
        except Exception:
            pass
        return
    phase = str(session.get("phase") or "")

    if phase == "awaiting_options":
        if action == "opt" and value == "done":
            session["phase"] = "awaiting_text"
            session["expires_at"] = time.time() + 1800
            _pdf_maker_save_session(context, session)
            labels = _pdf_maker_session_labels(session, lang)
            await safe_answer(query, msgs["continue_to_text"])
            await _pdf_maker_edit_or_send_prompt(update, context, session, msgs["prompt_text"].format(**labels), reply_markup=None)
            return
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    if action == "style":
        if value not in _PDF_MAKER_STYLE_KEYS:
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        if phase != "awaiting_style":
            await safe_answer(query, msgs["style_wait_tap"], show_alert=True)
            return
        session["style"] = value
        session["phase"] = "awaiting_paper"
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        labels = _pdf_maker_session_labels(session, lang)
        note = msgs["style_selected"].format(style=labels["style"])
        if value == "auto":
            note = f"{note}. {msgs.get('style_ai_hint', '')}".strip()
        await safe_answer(query, note[:180])
        await _pdf_maker_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["prompt_paper"].format(**labels),
            reply_markup=_pdf_maker_paper_keyboard(lang),
        )
        return

    if action == "paper":
        if value not in _PDF_MAKER_PAPER_KEYS:
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        if phase != "awaiting_paper":
            await safe_answer(query, msgs["paper_wait_tap"], show_alert=True)
            return
        session["paper"] = value
        session["phase"] = "awaiting_orientation"
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        labels = _pdf_maker_session_labels(session, lang)
        await safe_answer(query, msgs["paper_selected"].format(paper=labels["paper"]))
        await _pdf_maker_edit_or_send_prompt(
            update,
            context,
            session,
            msgs["prompt_orientation"].format(**labels),
            reply_markup=_pdf_maker_orientation_keyboard(lang),
        )
        return

    if action == "orient":
        if value not in _PDF_MAKER_ORIENTATION_KEYS:
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        if phase != "awaiting_orientation":
            await safe_answer(query, msgs["orientation_wait_tap"], show_alert=True)
            return
        session["orientation"] = value
        session["phase"] = "awaiting_options"
        session["expires_at"] = time.time() + 1800
        _pdf_maker_save_session(context, session)
        labels = _pdf_maker_session_labels(session, lang)
        await safe_answer(query, msgs["orientation_selected"].format(orientation=labels["orientation"]))
        await _pdf_maker_send_options_panel(update, context, session, lang)
        return

    if action == "gen":
        if value != "confirm":
            await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
            return
        if phase != "awaiting_text_confirm":
            await safe_answer(query, msgs["expired"], show_alert=True)
            return
        final_text = str(session.get("text_buffer") or "").strip()
        if not final_text:
            await safe_answer(query, msgs["empty"], show_alert=True)
            return
        labels = _pdf_maker_session_labels(session, lang)
        style_key = str(session.get("style") or "modern")
        paper_key = str(session.get("paper") or "a4")
        orientation_key = str(session.get("orientation") or "portrait")
        _pdf_maker_clear_session(context)
        await safe_answer(query, "Generating...")
        await _pdf_maker_send_text_as_pdf(
            update,
            context,
            lang,
            final_text,
            title=labels["name"],
            style_key=style_key,
            paper_key=paper_key,
            orientation_key=orientation_key,
        )
        return

    await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
