from __future__ import annotations

import html
import io
import logging
import os
import re
import shutil
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any

import safe_subprocess

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

try:
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.pdfbase import pdfmetrics
except Exception:
    rl_canvas = None
    pdfmetrics = None


logger = logging.getLogger(__name__)
MESSAGES: dict[str, dict[str, str]] = {}


def _pdf_missing_dep(*args, **kwargs):
    raise RuntimeError("pdf_editor module is not configured")


async def _pdf_missing_dep_async(*args, **kwargs):
    raise RuntimeError("pdf_editor module is not configured")


_send_with_retry = _pdf_missing_dep_async
run_blocking = _pdf_missing_dep_async
safe_answer = _pdf_missing_dep_async
ensure_user_language = _pdf_missing_dep
spam_check_callback = _pdf_missing_dep
spam_check_message = _pdf_missing_dep
_main_menu_keyboard = _pdf_missing_dep

# runtime functions injected from bot.py
is_blocked = _pdf_missing_dep
is_stopped_user = _pdf_missing_dep_async
update_user_info = _pdf_missing_dep_async


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith("__") and k.endswith("__"):
            continue
        globals()[k] = v


_PDF_EDITOR_SESSION_KEY = "pdf_editor_session"


def _pdf_editor_texts(lang: str) -> dict[str, str]:
    if lang == "uz":
        return {
            "start": "🧰 PDF muharriri yoqildi.\n📥 PDF fayl yuboring.\n👇 Siqish, OCR, TXT/EPUB va suv belgisi ishlaydi.",
            "prompt_send_pdf": "📥 PDF fayl yuboring.",
            "source_set": "✅ Fayl qabul qilindi: {name}",
            "choose_action": "👇 Amalni tanlang.",
            "current_file": "📄 Joriy fayl: {name} ({size_mb} MB)",
            "ask_watermark": "🏷️ Suv belgisi matnini yuboring.",
            "working": "⏳ PDF qayta ishlanmoqda...",
            "done": "✅ Tayyor.",
            "failed": "⚠️ Amal bajarilmadi. Qayta urinib ko‘ring.",
            "expired": "⌛ Sessiya tugadi. Menyudan qayta oching.",
            "session_other": "Bu sessiya boshqa foydalanuvchiga tegishli.",
            "cancelled": "❌ Bekor qilindi.",
            "completed": "✅ PDF muharriri yakunlandi.",
            "no_source": "⚠️ Avval fayl yuboring.",
            "not_pdf": "⚠️ Faqat PDF fayl qabul qilinadi.",
            "too_large": "⚠️ Fayl katta ({size_mb} MB). Kichikroq yuboring.",
            "tools_missing": "⚠️ `pypdf` topilmadi. PDF Editor ishlamaydi.",
            "ocr_text_only": "ℹ️ OCR text tayyor. Searchable PDF uchun `ocrmypdf` kerak.",
            "ocr_tools_missing": "⚠️ OCR vositalari topilmadi (`pdftoppm`, `tesseract`, yoki `ocrmypdf`).",
            "watermark_missing": "⚠️ Suv belgisi uchun `reportlab` kerak.",
            "btn_compress": "🗜️ Siqish",
            "btn_ocr": "🔎 OCR",
            "btn_to_txt": "📝 TXT",
            "btn_to_epub": "📚 EPUB",
            "btn_watermark": "🏷️ Suv belgisi",
            "btn_clear": "🧹 Tozalash",
            "btn_complete": "✅ Yakunlash",
            "btn_cancel": "❌ Bekor",
            "caption_pdf": "📄 PDF Editor natijasi",
            "caption_txt": "📝 PDF dan matn",
            "caption_epub": "📚 PDF dan EPUB",
            "compress_report": "🗜️ Siqildi: {before_mb} MB → {after_mb} MB",
            "watermark_done": "🏷️ Suv belgisi qo‘shildi.",
            "clear_done": "🧹 Sessiya fayli tozalandi.",
        }
    if lang == "ru":
        return {
            "start": "🧰 PDF-редактор включён.\n📥 Отправьте PDF файл.\n👇 Доступно: сжатие, OCR, TXT/EPUB и водяной знак.",
            "prompt_send_pdf": "📥 Отправьте PDF файл.",
            "source_set": "✅ Файл получен: {name}",
            "choose_action": "👇 Выберите действие.",
            "current_file": "📄 Текущий файл: {name} ({size_mb} MB)",
            "ask_watermark": "🏷️ Отправьте текст водяного знака.",
            "working": "⏳ Обработка PDF...",
            "done": "✅ Готово.",
            "failed": "⚠️ Операция не выполнена. Попробуйте снова.",
            "expired": "⌛ Сессия истекла. Откройте меню заново.",
            "session_other": "Эта сессия принадлежит другому пользователю.",
            "cancelled": "❌ Отменено.",
            "completed": "✅ PDF-редактор завершён.",
            "no_source": "⚠️ Сначала отправьте файл.",
            "not_pdf": "⚠️ Поддерживаются только PDF файлы.",
            "too_large": "⚠️ Файл слишком большой ({size_mb} MB).",
            "tools_missing": "⚠️ `pypdf` не найден. PDF Editor недоступен.",
            "ocr_text_only": "ℹ️ OCR-текст готов. Для searchable PDF нужен `ocrmypdf`.",
            "ocr_tools_missing": "⚠️ OCR инструменты не найдены (`pdftoppm`, `tesseract`, или `ocrmypdf`).",
            "watermark_missing": "⚠️ Для водяного знака нужен `reportlab`.",
            "btn_compress": "🗜️ Сжать",
            "btn_ocr": "🔎 OCR",
            "btn_to_txt": "📝 TXT",
            "btn_to_epub": "📚 EPUB",
            "btn_watermark": "🏷️ Водяной знак",
            "btn_clear": "🧹 Очистить",
            "btn_complete": "✅ Завершить",
            "btn_cancel": "❌ Отмена",
            "caption_pdf": "📄 Результат PDF Editor",
            "caption_txt": "📝 Текст из PDF",
            "caption_epub": "📚 EPUB из PDF",
            "compress_report": "🗜️ Сжато: {before_mb} MB → {after_mb} MB",
            "watermark_done": "🏷️ Водяной знак добавлен.",
            "clear_done": "🧹 Файл сессии очищен.",
        }
    return {
        "start": "🧰 PDF Editor is on.\n📥 Send a PDF file.\n👇 Available: compress, OCR, TXT/EPUB, watermark.",
        "prompt_send_pdf": "📥 Send a PDF file.",
        "source_set": "✅ File received: {name}",
        "choose_action": "👇 Choose an action.",
        "current_file": "📄 Current file: {name} ({size_mb} MB)",
        "ask_watermark": "🏷️ Send watermark text.",
        "working": "⏳ Processing PDF...",
        "done": "✅ Done.",
        "failed": "⚠️ Operation failed. Please try again.",
        "expired": "⌛ Session expired. Open from menu again.",
        "session_other": "This session belongs to another user.",
        "cancelled": "❌ Cancelled.",
        "completed": "✅ PDF Editor completed.",
        "no_source": "⚠️ Send a file first.",
        "not_pdf": "⚠️ Only PDF files are supported.",
        "too_large": "⚠️ File is too large ({size_mb} MB).",
        "tools_missing": "⚠️ `pypdf` is missing. PDF Editor is unavailable.",
        "ocr_text_only": "ℹ️ OCR text is ready. Install `ocrmypdf` for searchable PDF output.",
        "ocr_tools_missing": "⚠️ OCR tools are missing (`pdftoppm`, `tesseract`, or `ocrmypdf`).",
        "watermark_missing": "⚠️ Watermark needs `reportlab`.",
        "btn_compress": "🗜️ Compress",
        "btn_ocr": "🔎 OCR",
        "btn_to_txt": "📝 TXT",
        "btn_to_epub": "📚 EPUB",
        "btn_watermark": "🏷️ Watermark",
        "btn_clear": "🧹 Clear",
        "btn_complete": "✅ Complete",
        "btn_cancel": "❌ Cancel",
        "caption_pdf": "📄 PDF Editor output",
        "caption_txt": "📝 Text extracted from PDF",
        "caption_epub": "📚 EPUB converted from PDF",
        "compress_report": "🗜️ Compressed: {before_mb} MB → {after_mb} MB",
        "watermark_done": "🏷️ Watermark added.",
        "clear_done": "🧹 Session file cleared.",
    }


def _pdf_editor_clear_session(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop(_PDF_EDITOR_SESSION_KEY, None)


def _pdf_editor_get_session(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    raw = context.user_data.get(_PDF_EDITOR_SESSION_KEY)
    return raw if isinstance(raw, dict) else None


def _pdf_editor_save_session(context: ContextTypes.DEFAULT_TYPE, session: dict):
    context.user_data[_PDF_EDITOR_SESSION_KEY] = dict(session)


def _pdf_editor_sanitize_name(name: str, fallback: str = "pdf") -> str:
    clean = re.sub(r"[^\w\- .]+", "", str(name or "").strip())
    clean = re.sub(r"\s+", "_", clean).strip("._ ")
    clean = re.sub(r"\.(pdf|epub|docx|doc|txt|md|markdown)$", "", clean, flags=re.IGNORECASE).strip("._ ")
    if not clean:
        clean = fallback
    return clean[:80]


def _pdf_editor_max_bytes() -> int:
    try:
        mb = max(1, int(os.getenv("PDF_EDITOR_MAX_MB", "80") or "80"))
    except Exception:
        mb = 80
    return mb * 1024 * 1024


def _pdf_editor_now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _pdf_editor_target_message(update: Update):
    return update.message or (update.callback_query.message if update.callback_query else None)


def _pdf_editor_file_ext(name: str) -> str:
    return os.path.splitext(str(name or ""))[1].lower().strip()


def _pdf_editor_extract_document_info(message) -> dict | None:
    if not message:
        return None
    doc = getattr(message, "document", None)
    if not doc:
        return None
    name = str(getattr(doc, "file_name", "") or "").strip() or f"file_{int(time.time())}"
    return {
        "file_id": getattr(doc, "file_id", None),
        "file_unique_id": getattr(doc, "file_unique_id", None),
        "file_name": name,
        "file_size": int(getattr(doc, "file_size", 0) or 0),
        "mime_type": str(getattr(doc, "mime_type", "") or "").lower().strip(),
    }


def _pdf_editor_extract_pdf_info(message) -> dict | None:
    info = _pdf_editor_extract_document_info(message)
    if not info:
        return None
    name = str(info.get("file_name") or "")
    mime = str(info.get("mime_type") or "")
    if mime != "application/pdf" and _pdf_editor_file_ext(name) != ".pdf":
        return None
    return info


def _pdf_editor_touch_session(session: dict):
    session["expires_at"] = time.time() + 3600


def _pdf_editor_set_current_file(session: dict, info: dict):
    session["file"] = {
        "file_id": info.get("file_id"),
        "file_unique_id": info.get("file_unique_id"),
        "name": info.get("file_name") or f"file_{int(time.time())}",
        "ext": _pdf_editor_file_ext(info.get("file_name") or ""),
        "size": int(info.get("file_size") or 0),
        "updated_at": int(time.time()),
    }


def _pdf_editor_current_file(session: dict) -> dict | None:
    f = session.get("file")
    return f if isinstance(f, dict) else None


def _pdf_editor_action_keyboard(lang: str, session: dict | None = None) -> InlineKeyboardMarkup:
    t = _pdf_editor_texts(lang)
    session = session or {}
    has_source = bool(_pdf_editor_current_file(session))
    rows: list[list[InlineKeyboardButton]] = []
    if has_source:
        rows.extend(
            [
                [
                    InlineKeyboardButton(t["btn_compress"], callback_data="pdfed:cmp"),
                    InlineKeyboardButton(t["btn_ocr"], callback_data="pdfed:ocr"),
                ],
                [
                    InlineKeyboardButton(t["btn_to_txt"], callback_data="pdfed:txt"),
                    InlineKeyboardButton(t["btn_to_epub"], callback_data="pdfed:epub"),
                ],
                [InlineKeyboardButton(t["btn_watermark"], callback_data="pdfed:wm")],
                [InlineKeyboardButton(t["btn_clear"], callback_data="pdfed:clr")],
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(t["btn_complete"], callback_data="pdfed:complete"),
            InlineKeyboardButton(t["btn_cancel"], callback_data="pdfed:cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def _pdf_editor_send_actions(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    target = _pdf_editor_target_message(update)
    if not target:
        return
    session = _pdf_editor_get_session(context)
    if not session:
        return

    t = _pdf_editor_texts(lang)
    lines = [t["choose_action"]]
    current = _pdf_editor_current_file(session)
    if current:
        size = int(current.get("size") or 0)
        size_mb = round(size / (1024 * 1024), 2) if size else 0
        lines.append(t["current_file"].format(name=str(current.get("name") or "file.pdf"), size_mb=size_mb))

    await target.reply_text("\n\n".join(lines), reply_markup=_pdf_editor_action_keyboard(lang, session))


async def _pdf_editor_download_bytes(context: ContextTypes.DEFAULT_TYPE, file_id: str) -> bytes:
    f = await context.bot.get_file(file_id)
    data = await f.download_as_bytearray()
    return bytes(data)


async def _pdf_editor_get_current_bytes(context: ContextTypes.DEFAULT_TYPE, session: dict) -> tuple[bytes, dict] | tuple[None, None]:
    item = _pdf_editor_current_file(session)
    if not item:
        return None, None
    fid = str(item.get("file_id") or "").strip()
    if not fid:
        return None, None
    b = await _pdf_editor_download_bytes(context, fid)
    return b, item


def _pdf_editor_compress_blocking(pdf_bytes: bytes) -> bytes:
    if PdfReader is None or PdfWriter is None:
        raise RuntimeError("pypdf-missing")

    rd = PdfReader(io.BytesIO(pdf_bytes))
    if getattr(rd, "is_encrypted", False):
        raise RuntimeError("encrypted-pdf")

    wr = PdfWriter()
    for p in rd.pages:
        try:
            p.compress_content_streams()
        except Exception:
            pass
        wr.add_page(p)

    try:
        if getattr(rd, "metadata", None):
            wr.add_metadata(dict(rd.metadata or {}))
    except Exception:
        pass

    out = io.BytesIO()
    wr.write(out)
    return out.getvalue()


def _pdf_editor_tesseract_lang_candidates(lang: str) -> list[str]:
    base = {"uz": "uzb+eng", "ru": "rus+eng", "en": "eng"}.get(lang, "eng")
    cands = [base]
    if base != "eng":
        cands.append("eng")
    return cands


def _pdf_editor_ocr_text_from_path_blocking(file_path: str, lang: str, max_chars: int = 400000) -> str:
    if not shutil.which("pdftoppm") or not shutil.which("tesseract"):
        raise RuntimeError("ocr-tools-missing")

    max_pages = int(os.getenv("PDF_EDITOR_OCR_MAX_PAGES", "80") or "80")
    dpi = os.getenv("PDF_EDITOR_OCR_DPI", "160")
    render_timeout_s = float(os.getenv("PDF_EDITOR_OCR_RENDER_TIMEOUT_S", "40") or "40")
    tess_timeout_s = float(os.getenv("PDF_EDITOR_OCR_TESS_TIMEOUT_S", "40") or "40")
    chars_per_page_cap = max(1000, max_chars // max(1, max_pages))

    out: list[str] = []
    total = 0
    with tempfile.TemporaryDirectory(prefix="pdfed_ocr_") as td:
        for page_num in range(1, max_pages + 1):
            if total >= max_chars:
                break

            img_prefix = os.path.join(td, f"p{page_num}")
            render = safe_subprocess.run(
                [
                    "pdftoppm",
                    "-f",
                    str(page_num),
                    "-l",
                    str(page_num),
                    "-r",
                    str(dpi),
                    "-png",
                    "-singlefile",
                    file_path,
                    img_prefix,
                ],
                timeout_s=render_timeout_s,
                max_output_chars=5000,
                text=True,
            )
            img_path = f"{img_prefix}.png"
            if render.returncode != 0 or not os.path.exists(img_path):
                if page_num == 1:
                    raise RuntimeError("ocr-render-failed")
                break

            page_text = ""
            for code in _pdf_editor_tesseract_lang_candidates(lang):
                ocr = safe_subprocess.run(
                    ["tesseract", img_path, "stdout", "-l", code, "--psm", "6"],
                    timeout_s=tess_timeout_s,
                    max_output_chars=30000,
                    text=True,
                )
                if ocr.returncode == 0 and str(ocr.stdout or "").strip():
                    page_text = str(ocr.stdout or "").strip()
                    break

            if not page_text:
                continue

            page_text = re.sub(r"[ \t]+\n", "\n", page_text)
            page_text = re.sub(r"\n{3,}", "\n\n", page_text).strip()
            if not page_text:
                continue

            page_text = page_text[: min(chars_per_page_cap, max_chars - total)]
            if total:
                out.append("\n\n")
                total += 2
            out.append(page_text)
            total += len(page_text)

    return "".join(out).strip()


def _pdf_editor_extract_text_blocking(pdf_bytes: bytes, lang: str, max_chars: int = 500000) -> str:
    if PdfReader is None:
        raise RuntimeError("pypdf-missing")

    with tempfile.TemporaryDirectory(prefix="pdfed_txt_") as td:
        path = os.path.join(td, "in.pdf")
        with open(path, "wb") as f:
            f.write(pdf_bytes)

        rd = PdfReader(path)
        if getattr(rd, "is_encrypted", False):
            raise RuntimeError("encrypted-pdf")

        out: list[str] = []
        total = 0
        max_pages = int(os.getenv("PDF_EDITOR_TEXT_MAX_PAGES", "200") or "200")
        for idx, page in enumerate(rd.pages):
            if idx >= max_pages or total >= max_chars:
                break
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            txt = str(txt).strip()
            if not txt:
                continue
            txt = txt[: max_chars - total]
            if out:
                out.append("\n\n")
                total += 2
            out.append(txt)
            total += len(txt)

        extracted = "".join(out).strip()
        if len(extracted) >= 120:
            return extracted

        try:
            ocr_text = _pdf_editor_ocr_text_from_path_blocking(path, lang=lang, max_chars=max_chars)
            if len(ocr_text) > len(extracted):
                return ocr_text
        except Exception:
            pass

        return extracted


def _pdf_editor_ocr_pdf_blocking(pdf_bytes: bytes, lang: str) -> bytes | None:
    if not shutil.which("ocrmypdf"):
        return None

    ocr_lang = {"uz": "uzb+eng", "ru": "rus+eng", "en": "eng"}.get(lang, "eng")
    timeout_s = float(os.getenv("PDF_EDITOR_OCRPDF_TIMEOUT_S", "900") or "900")

    with tempfile.TemporaryDirectory(prefix="pdfed_ocrpdf_") as td:
        in_path = os.path.join(td, "in.pdf")
        out_path = os.path.join(td, "out.pdf")
        with open(in_path, "wb") as f:
            f.write(pdf_bytes)

        run = safe_subprocess.run(
            [
                "ocrmypdf",
                "--skip-text",
                "--rotate-pages",
                "--deskew",
                "-l",
                ocr_lang,
                in_path,
                out_path,
            ],
            timeout_s=timeout_s,
            max_output_chars=8000,
            text=True,
        )
        if run.returncode != 0 or not os.path.exists(out_path):
            raise RuntimeError("ocrmypdf-failed")

        with open(out_path, "rb") as f:
            return f.read()


def _pdf_editor_build_epub_blocking(title: str, text: str, lang: str = "en") -> bytes:
    uid = re.sub(r"[^a-f0-9]", "", _pdf_editor_sanitize_name(title, fallback="book").lower()) or "book"
    title_esc = html.escape(title)
    if not text.strip():
        text = "(empty)"

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if not paragraphs:
        paragraphs = [text.strip()]
    body = "\n".join(f"<p>{html.escape(p)}</p>" for p in paragraphs)

    index_xhtml = (
        "<?xml version='1.0' encoding='utf-8'?>\n"
        "<html xmlns='http://www.w3.org/1999/xhtml'>\n"
        "<head><title>{title}</title><meta charset='utf-8'/></head>\n"
        "<body><h1>{title}</h1>{body}</body></html>"
    ).format(title=title_esc, body=body)

    opf = (
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<package xmlns='http://www.idpf.org/2007/opf' unique-identifier='BookId' version='2.0'>"
        "<metadata xmlns:dc='http://purl.org/dc/elements/1.1/'>"
        f"<dc:title>{title_esc}</dc:title>"
        f"<dc:language>{html.escape(lang or 'en')}</dc:language>"
        f"<dc:identifier id='BookId'>urn:uuid:{uid}</dc:identifier>"
        "</metadata>"
        "<manifest>"
        "<item id='content' href='index.xhtml' media-type='application/xhtml+xml'/>"
        "<item id='ncx' href='toc.ncx' media-type='application/x-dtbncx+xml'/>"
        "</manifest>"
        "<spine toc='ncx'><itemref idref='content'/></spine>"
        "</package>"
    )

    ncx = (
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<ncx xmlns='http://www.daisy.org/z3986/2005/ncx/' version='2005-1'>"
        f"<head><meta name='dtb:uid' content='urn:uuid:{uid}'/></head>"
        f"<docTitle><text>{title_esc}</text></docTitle>"
        "<navMap><navPoint id='navPoint-1' playOrder='1'>"
        f"<navLabel><text>{title_esc}</text></navLabel>"
        "<content src='index.xhtml'/>"
        "</navPoint></navMap></ncx>"
    )

    container_xml = (
        "<?xml version='1.0'?>\n"
        "<container version='1.0' xmlns='urn:oasis:names:tc:opendocument:xmlns:container'>"
        "<rootfiles><rootfile full-path='OEBPS/content.opf' media-type='application/oebps-package+xml'/></rootfiles>"
        "</container>"
    )

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        z.writestr("META-INF/container.xml", container_xml)
        z.writestr("OEBPS/content.opf", opf)
        z.writestr("OEBPS/toc.ncx", ncx)
        z.writestr("OEBPS/index.xhtml", index_xhtml)
    return out.getvalue()


def _pdf_editor_watermark_blocking(pdf_bytes: bytes, watermark_text: str) -> bytes:
    if PdfReader is None or PdfWriter is None:
        raise RuntimeError("pypdf-missing")
    if rl_canvas is None:
        raise RuntimeError("watermark-tools-missing")

    rd = PdfReader(io.BytesIO(pdf_bytes))
    if getattr(rd, "is_encrypted", False):
        raise RuntimeError("encrypted-pdf")
    wr = PdfWriter()

    overlay_cache: dict[tuple[int, int], Any] = {}

    def make_overlay(width: float, height: float):
        key = (int(width), int(height))
        if key in overlay_cache:
            return overlay_cache[key]

        text = str(watermark_text or "")[:140]
        page_span = max(1.0, min(float(width), float(height)))
        # One centered watermark per page, sized larger than the screenshot reference.
        font_size = max(30.0, min(52.0, page_span / 6.0))
        if pdfmetrics is not None:
            try:
                text_width = float(pdfmetrics.stringWidth(text, "Helvetica-Bold", 1.0) or 0.0)
            except Exception:
                text_width = 0.0
            if text_width > 0:
                target_width = float(width) * 0.80
                font_size = target_width / max(text_width, 1.0)
        font_size = max(30.0, min(52.0, font_size))

        packet = io.BytesIO()
        c = rl_canvas.Canvas(packet, pagesize=(float(width), float(height)))
        c.saveState()
        try:
            c.setFillAlpha(0.19)
        except Exception:
            pass
        c.setFont("Helvetica-Bold", font_size)
        c.setFillColorRGB(0.52, 0.52, 0.52)
        c.translate(float(width) / 2, float(height) / 2)
        c.rotate(30)
        c.drawCentredString(0, -page_span * 0.04, text)
        c.restoreState()
        c.save()
        packet.seek(0)

        ov = PdfReader(packet).pages[0]
        overlay_cache[key] = ov
        return ov

    for page in rd.pages:
        w = float(page.mediabox.width)
        h = float(page.mediabox.height)
        ov = make_overlay(w, h)
        page.merge_page(ov)
        wr.add_page(page)

    out = io.BytesIO()
    wr.write(out)
    return out.getvalue()


async def _pdf_editor_send_pdf(update: Update, pdf_bytes: bytes, file_name: str, caption: str):
    target = _pdf_editor_target_message(update)
    if not target:
        return None
    bio = io.BytesIO(pdf_bytes)
    bio.name = file_name
    return await _send_with_retry(lambda: target.reply_document(document=bio, caption=caption))


async def _pdf_editor_send_text_file(update: Update, text: str, file_name: str, caption: str):
    target = _pdf_editor_target_message(update)
    if not target:
        return None
    bio = io.BytesIO((text or "").encode("utf-8", errors="ignore"))
    bio.name = file_name
    return await _send_with_retry(lambda: target.reply_document(document=bio, caption=caption))


def _pdf_editor_register_sent_pdf(session: dict, sent_msg, fallback_name: str):
    if not sent_msg:
        return
    doc = getattr(sent_msg, "document", None)
    if not doc:
        return
    _pdf_editor_set_current_file(
        session,
        {
            "file_id": getattr(doc, "file_id", None),
            "file_unique_id": getattr(doc, "file_unique_id", None),
            "file_name": getattr(doc, "file_name", None) or fallback_name,
            "file_size": int(getattr(doc, "file_size", 0) or 0),
        },
    )


async def _pdf_editor_start_session_from_message(
    target_message,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    lang: str,
):
    _pdf_editor_clear_session(context)
    session = {
        "active": True,
        "user_id": update.effective_user.id if update.effective_user else None,
        "phase": "ready",
        "file": None,
        "expires_at": time.time() + 3600,
    }
    _pdf_editor_save_session(context, session)

    uid = update.effective_user.id if update.effective_user else None
    sent = await _send_with_retry(
        lambda: target_message.reply_text(
            _pdf_editor_texts(lang)["start"],
            reply_markup=_main_menu_keyboard(lang, "other", uid),
        )
    )
    if sent:
        session["prompt_chat_id"] = sent.chat_id
        session["prompt_message_id"] = sent.message_id
        _pdf_editor_save_session(context, session)


async def _pdf_editor_handle_media_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    session = _pdf_editor_get_session(context)
    if not session or not session.get("active"):
        return False
    if not update.message:
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    t = _pdf_editor_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _pdf_editor_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    info = _pdf_editor_extract_pdf_info(update.message)
    if not info:
        if getattr(update.message, "document", None):
            await update.message.reply_text(t["not_pdf"])
            return True
        return False

    size = int(info.get("file_size") or 0)
    if size > _pdf_editor_max_bytes():
        await update.message.reply_text(t["too_large"].format(size_mb=max(1, round(size / (1024 * 1024)))))
        return True

    _pdf_editor_set_current_file(session, info)
    _pdf_editor_touch_session(session)
    _pdf_editor_save_session(context, session)

    await update.message.reply_text(t["source_set"].format(name=info.get("file_name") or "file.pdf"))

    await _pdf_editor_send_actions(update, context, lang)
    return True


async def _pdf_editor_op_compress(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    session = _pdf_editor_get_session(context)
    if not session:
        return False
    t = _pdf_editor_texts(lang)
    pdf_bytes, src = await _pdf_editor_get_current_bytes(context, session)
    if not pdf_bytes:
        await _pdf_editor_target_message(update).reply_text(t["no_source"])
        return False

    # Enqueue background job
    user_id = _pdf_editor_target_message(update).chat_id
    job_data = {
        "operation": "compress",
        "pdf_bytes": pdf_bytes,  # Store bytes since it's needed
        "src_name": str(src.get("name") or "pdf"),
        "lang": lang,
    }
    job_id = db_enqueue_background_job("pdf_editor", user_id, job_data)
    if not job_id:
        await _pdf_editor_target_message(update).reply_text(t["failed"])
        return False

    await _pdf_editor_target_message(update).reply_text("✅ PDF compression queued! You'll receive the result soon.")
    return True


async def _pdf_editor_op_to_txt(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    session = _pdf_editor_get_session(context)
    if not session:
        return False
    t = _pdf_editor_texts(lang)
    pdf_bytes, src = await _pdf_editor_get_current_bytes(context, session)
    if not pdf_bytes:
        await _pdf_editor_target_message(update).reply_text(t["no_source"])
        return False

    # Enqueue background job
    user_id = _pdf_editor_target_message(update).chat_id
    job_data = {
        "operation": "to_txt",
        "pdf_bytes": pdf_bytes,
        "src_name": str(src.get("name") or "pdf"),
        "lang": lang,
    }
    job_id = db_enqueue_background_job("pdf_editor", user_id, job_data)
    if not job_id:
        await _pdf_editor_target_message(update).reply_text(t["failed"])
        return False

    await _pdf_editor_target_message(update).reply_text("✅ PDF to TXT conversion queued! You'll receive the result soon.")
    return True


async def _pdf_editor_op_to_epub(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    session = _pdf_editor_get_session(context)
    if not session:
        return False
    t = _pdf_editor_texts(lang)
    pdf_bytes, src = await _pdf_editor_get_current_bytes(context, session)
    if not pdf_bytes:
        await _pdf_editor_target_message(update).reply_text(t["no_source"])
        return False

    # Enqueue background job
    user_id = _pdf_editor_target_message(update).chat_id
    job_data = {
        "operation": "to_epub",
        "pdf_bytes": pdf_bytes,
        "src_name": str(src.get("name") or "book"),
        "lang": lang,
    }
    job_id = db_enqueue_background_job("pdf_editor", user_id, job_data)
    if not job_id:
        await _pdf_editor_target_message(update).reply_text(t["failed"])
        return False

    await _pdf_editor_target_message(update).reply_text("✅ PDF to EPUB conversion queued! You'll receive the result soon.")
    return True


async def _pdf_editor_op_ocr(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str):
    session = _pdf_editor_get_session(context)
    if not session:
        return False
    t = _pdf_editor_texts(lang)
    pdf_bytes, src = await _pdf_editor_get_current_bytes(context, session)
    if not pdf_bytes:
        await _pdf_editor_target_message(update).reply_text(t["no_source"])
        return False

    # Enqueue background job
    user_id = _pdf_editor_target_message(update).chat_id
    job_data = {
        "operation": "ocr",
        "pdf_bytes": pdf_bytes,
        "src_name": str(src.get("name") or "pdf"),
        "lang": lang,
    }
    job_id = db_enqueue_background_job("pdf_editor", user_id, job_data)
    if not job_id:
        await _pdf_editor_target_message(update).reply_text(t["failed"])
        return False

    await _pdf_editor_target_message(update).reply_text("✅ PDF OCR processing queued! You'll receive the result soon.")
    return True


async def _pdf_editor_op_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str, watermark_text: str):
    session = _pdf_editor_get_session(context)
    if not session:
        return False
    t = _pdf_editor_texts(lang)
    if rl_canvas is None:
        await _pdf_editor_target_message(update).reply_text(t["watermark_missing"])
        return False

    pdf_bytes, src = await _pdf_editor_get_current_bytes(context, session)
    if not pdf_bytes:
        await _pdf_editor_target_message(update).reply_text(t["no_source"])
        return False

    status = await _send_with_retry(lambda: _pdf_editor_target_message(update).reply_text(t["working"]))
    try:
        out_bytes = await run_blocking(_pdf_editor_watermark_blocking, pdf_bytes, watermark_text)
        base = _pdf_editor_sanitize_name(str(src.get("name") or "pdf"), fallback="pdf")
        fname = f"{base}_wm_{_pdf_editor_now_stamp()}.pdf"
        sent = await _pdf_editor_send_pdf(update, out_bytes, fname, t["caption_pdf"])
        if sent:
            _pdf_editor_register_sent_pdf(session, sent, fname)
            _pdf_editor_touch_session(session)
            _pdf_editor_save_session(context, session)

        if status:
            try:
                await status.edit_text(t["watermark_done"])
            except Exception:
                pass
        await _pdf_editor_send_actions(update, context, lang)
        return bool(sent)
    except Exception as e:
        logger.warning("pdf editor watermark failed: %s", e, exc_info=True)
        if status:
            try:
                await status.edit_text(t["failed"])
            except Exception:
                pass
        return False


async def _pdf_editor_handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, lang: str) -> bool:
    if not update.message or not update.message.text:
        return False

    session = _pdf_editor_get_session(context)
    if not session or not session.get("active"):
        return False
    if update.effective_user and session.get("user_id") and int(session.get("user_id")) != int(update.effective_user.id):
        return False

    t = _pdf_editor_texts(lang)
    if time.time() > float(session.get("expires_at", 0) or 0):
        _pdf_editor_clear_session(context)
        await update.message.reply_text(t["expired"])
        return True

    raw = (update.message.text or "").strip()
    if raw.lower() in {"cancel", "stop", "/cancel"}:
        _pdf_editor_clear_session(context)
        uid = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        return True

    phase = str(session.get("phase") or "ready")
    if phase == "awaiting_watermark":
        if len(raw) < 2:
            await update.message.reply_text(t["ask_watermark"])
            return True
        session["phase"] = "ready"
        _pdf_editor_touch_session(session)
        _pdf_editor_save_session(context, session)
        await _pdf_editor_op_watermark(update, context, lang, raw)
        return True

    if not _pdf_editor_current_file(session):
        await update.message.reply_text(t["prompt_send_pdf"])
    else:
        await _pdf_editor_send_actions(update, context, lang)
    return True


async def pdf_editor_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    if PdfReader is None or PdfWriter is None:
        t = _pdf_editor_texts(lang)
        await update.message.reply_text(t["tools_missing"])
        return

    await update_user_info(update, context)
    await _pdf_editor_start_session_from_message(update.message, update, context, lang)


async def handle_pdf_editor_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    lang = ensure_user_language(update, context)
    t = _pdf_editor_texts(lang)

    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return

    session = _pdf_editor_get_session(context)
    if not session or not session.get("active"):
        await safe_answer(query, t["expired"], show_alert=True)
        return
    if time.time() > float(session.get("expires_at", 0) or 0):
        _pdf_editor_clear_session(context)
        await safe_answer(query, t["expired"], show_alert=True)
        return
    if query.from_user and session.get("user_id") and int(session.get("user_id")) != int(query.from_user.id):
        await safe_answer(query, t["session_other"], show_alert=True)
        return

    data = str(query.data or "")

    if data == "pdfed:cancel":
        _pdf_editor_clear_session(context)
        await safe_answer(query, t["cancelled"])
        uid = query.from_user.id if query.from_user else None
        try:
            await query.message.reply_text(t["cancelled"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        except Exception:
            pass
        return

    if data == "pdfed:complete":
        _pdf_editor_clear_session(context)
        await safe_answer(query, t["completed"])
        uid = query.from_user.id if query.from_user else None
        try:
            await query.message.reply_text(t["completed"], reply_markup=_main_menu_keyboard(lang, "other", uid))
        except Exception:
            pass
        return

    if data == "pdfed:clr":
        session["file"] = None
        session["phase"] = "ready"
        _pdf_editor_touch_session(session)
        _pdf_editor_save_session(context, session)
        await safe_answer(query)
        try:
            await query.message.reply_text(t["clear_done"])
            await _pdf_editor_send_actions(update, context, lang)
        except Exception:
            pass
        return

    if not _pdf_editor_current_file(session):
        await safe_answer(query, t["no_source"], show_alert=True)
        return

    await safe_answer(query)

    if data == "pdfed:cmp":
        await _pdf_editor_op_compress(update, context, lang)
        return

    if data == "pdfed:txt":
        await _pdf_editor_op_to_txt(update, context, lang)
        return

    if data == "pdfed:epub":
        await _pdf_editor_op_to_epub(update, context, lang)
        return

    if data == "pdfed:ocr":
        await _pdf_editor_op_ocr(update, context, lang)
        return

    if data == "pdfed:wm":
        session["phase"] = "awaiting_watermark"
        _pdf_editor_touch_session(session)
        _pdf_editor_save_session(context, session)
        try:
            await query.message.reply_text(t["ask_watermark"])
        except Exception:
            pass
        return
