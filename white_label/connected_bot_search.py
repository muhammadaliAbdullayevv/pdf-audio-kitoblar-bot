from __future__ import annotations

import logging
import os
import re

from elasticsearch import Elasticsearch

from db import get_book_by_id as db_get_book_by_id, list_books as db_list_books

logger = logging.getLogger(__name__)

ES_INDEX = "books"
_ES_CLIENT = None

_CYRILLIC_TO_LATIN = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
    "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
    "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
}

_ARABIC_TO_LATIN = {
    "ا": "a", "أ": "a", "إ": "i", "آ": "a", "ب": "b", "ت": "t",
    "ث": "th", "ج": "j", "ح": "h", "خ": "kh", "د": "d", "ذ": "dh",
    "ر": "r", "ز": "z", "س": "s", "ش": "sh", "ص": "s", "ض": "d",
    "ط": "t", "ظ": "z", "ع": "a", "غ": "gh", "ف": "f", "ق": "q",
    "ك": "k", "ل": "l", "م": "m", "ن": "n", "ه": "h", "و": "w",
    "ؤ": "w", "ي": "y", "ئ": "y", "ى": "a", "ة": "h",
    "پ": "p", "چ": "ch", "ژ": "zh", "گ": "g", "ک": "k", "ی": "y",
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _normalize_uzbek_apostrophes(text: str) -> str:
    if not text:
        return ""
    s = str(text)
    s = s.replace("'", "ʻ").replace("’", "ʻ").replace("ʼ", "ʻ")
    s = re.sub(r"\b([og])ʻ\s+([^\W\d_])", r"\1ʻ\2", s, flags=re.UNICODE)
    s = re.sub(r"\b([og])\s+([^\W\d_])", r"\1ʻ\2", s, flags=re.UNICODE)
    return s


def normalize(text: str) -> str:
    clean = _normalize_uzbek_apostrophes(str(text or "").lower())
    clean = re.sub(r"@[\w]+", " ", clean)
    clean = re.sub(r"https?://\S+|www\.\S+", " ", clean)
    clean = clean.replace("_", " ")
    clean = re.sub(r"[^\w\sʻ]+", " ", clean, flags=re.UNICODE)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def latinize_text(text: str) -> str:
    out: list[str] = []
    for ch in str(text or "").lower():
        if ch in _CYRILLIC_TO_LATIN:
            out.append(_CYRILLIC_TO_LATIN[ch])
        elif ch in _ARABIC_TO_LATIN:
            out.append(_ARABIC_TO_LATIN[ch])
        else:
            out.append(ch)
    return normalize("".join(out))


def _title_for_book(book: dict) -> str:
    return str(book.get("display_name") or book.get("book_name") or book.get("id") or "Book").strip()


def _is_pdf_accessible_book(book: dict) -> bool:
    path = str(book.get("path") or "").strip().lower()
    file_id = str(book.get("file_id") or "").strip()
    if not path and not file_id:
        return False
    if path and not path.endswith(".pdf"):
        return False
    return True


def _search_es(query: str, size: int) -> list[tuple[dict, float, str]]:
    global _ES_CLIENT
    if not _env_bool("ENABLE_ELASTICSEARCH", True):
        return []
    es_url = str(os.getenv("ES_URL", "") or "").strip()
    if not es_url:
        return []
    if _ES_CLIENT is None:
        kwargs: dict[str, object] = {"request_timeout": max(1, int(os.getenv("ES_TIMEOUT_SECONDS", "3") or "3"))}
        es_ca_cert = str(os.getenv("ES_CA_CERT", "") or "").strip()
        es_user = str(os.getenv("ES_USER", "") or "").strip()
        es_pass = str(os.getenv("ES_PASS", "") or "").strip()
        if es_ca_cert:
            kwargs["ca_certs"] = es_ca_cert
        if es_user and es_pass:
            kwargs["basic_auth"] = (es_user, es_pass)
        _ES_CLIENT = Elasticsearch(es_url, **kwargs)
    try:
        res = _ES_CLIENT.search(  # type: ignore[union-attr]
            index=ES_INDEX,
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["book_name^2", "display_name^2"],
                    "fuzziness": "AUTO",
                }
            },
            size=size,
            track_total_hits=False,
            source_includes=["id", "book_name", "display_name"],
        )
        return [(hit["_source"], float(hit.get("_score") or 0.0), str(hit.get("_id") or "")) for hit in res["hits"]["hits"]]
    except Exception as exc:
        logger.debug("White-label ES search failed for %r: %s", query, exc, exc_info=True)
        return []


def search_connected_books(query: str, limit: int = 5) -> list[dict]:
    cleaned = normalize(query)
    if not cleaned:
        return []
    limit = max(1, min(10, int(limit or 5)))
    variants: list[str] = []
    for candidate in (cleaned, cleaned.replace("ʻ", ""), latinize_text(cleaned)):
        candidate = str(candidate or "").strip()
        if candidate and candidate not in variants:
            variants.append(candidate)

    results: list[dict] = []
    seen_ids: set[str] = set()

    for variant in variants:
        for source, _score, book_id in _search_es(variant, size=max(limit, 10)):
            bid = str(book_id or source.get("id") or "").strip()
            if not bid or bid in seen_ids:
                continue
            book = dict(db_get_book_by_id(bid) or source or {})
            book["id"] = bid
            if not _is_pdf_accessible_book(book):
                continue
            results.append(book)
            seen_ids.add(bid)
            if len(results) >= limit:
                return results

    for book in list(db_list_books() or []):
        bid = str(book.get("id") or "").strip()
        if not bid or bid in seen_ids or not _is_pdf_accessible_book(book):
            continue
        haystacks = [
            normalize(str(book.get("book_name") or "")),
            normalize(str(book.get("display_name") or "")),
            normalize(str(book.get("path") or "")),
            latinize_text(str(book.get("book_name") or "")),
            latinize_text(str(book.get("display_name") or "")),
        ]
        if any(cleaned in text for text in haystacks if text):
            results.append(book)
            seen_ids.add(bid)
            if len(results) >= limit:
                break

    return results[:limit]


def build_results_message(query: str, books: list[dict], *, page: int = 1, pages: int = 1, lang: str = "uz") -> str:
    labels = {
        "uz": {
            "found": f"📚 {len(books)} ta natija topildi",
            "page": f"Sahifa {page}/{pages}",
            "hint": "👇 Pastdagi raqamli tugmalardan birini bosing.",
        },
        "en": {
            "found": f"📚 {len(books)} results found",
            "page": f"Page {page}/{pages}",
            "hint": "👇 Tap one of the numbered buttons below.",
        },
        "ru": {
            "found": f"📚 Найдено результатов: {len(books)}",
            "page": f"Страница {page}/{pages}",
            "hint": "👇 Нажмите одну из цифровых кнопок ниже.",
        },
    }.get(str(lang or "").strip().lower(), {
        "found": f"📚 {len(books)} results found",
        "page": f"Page {page}/{pages}",
        "hint": "👇 Tap one of the numbered buttons below.",
    })
    lines = [labels["found"], f"{query}", labels["page"], ""]
    for idx, book in enumerate(books, start=1):
        lines.append(f"{idx}. {_title_for_book(book)}")
    lines.extend(["", labels["hint"]])
    return "\n".join(lines)
