from __future__ import annotations

import logging
import os
import re

from elasticsearch import Elasticsearch
from rapidfuzz import fuzz

from db import get_book_by_id as db_get_book_by_id, search_books_for_white_label_fallback as db_search_books_for_white_label_fallback

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
    if book.get("white_label_enabled") is False:
        return False
    path = str(book.get("path") or "").strip().lower()
    file_id = str(book.get("file_id") or "").strip()
    if not path and not file_id:
        return False
    if path and not path.endswith(".pdf"):
        return False
    return True


def _search_es(query: str, size: int) -> tuple[list[tuple[dict, float, str]], str | None]:
    global _ES_CLIENT
    if not _env_bool("ENABLE_ELASTICSEARCH", True):
        return [], "elasticsearch disabled"
    es_url = str(os.getenv("ES_URL", "") or "").strip()
    if not es_url:
        return [], "ES_URL is not configured"
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
        return [(hit["_source"], float(hit.get("_score") or 0.0), str(hit.get("_id") or "")) for hit in res["hits"]["hits"]], None
    except Exception as exc:
        logger.warning("White-label ES search failed for %r: %s", query, exc, exc_info=True)
        return [], str(exc)


def _search_pg_fallback(query: str, size: int) -> list[dict]:
    try:
        return list(db_search_books_for_white_label_fallback(query, limit=max(1, min(200, int(size or 50)))) or [])
    except Exception as exc:
        logger.warning("White-label PostgreSQL fallback search failed for %r: %s", query, exc, exc_info=True)
        return []


def _book_popularity_score(book: dict) -> float:
    score = 0.0
    for key, weight in (
        ("search_count", 0.08),
        ("searches", 0.08),
        ("download_count", 0.10),
        ("downloads", 0.10),
        ("favorite_count", 0.12),
        ("favorites", 0.12),
    ):
        try:
            value = float(book.get(key) or 0)
        except Exception:
            value = 0.0
        score += min(8.0, value * weight)
    return score


def _book_search_score(book: dict, query_variants: list[str]) -> float:
    names = [
        str(book.get("book_name") or ""),
        str(book.get("display_name") or ""),
        str(book.get("path") or ""),
    ]
    haystacks: list[str] = []
    for name in names:
        norm = normalize(name)
        latin = latinize_text(name)
        if norm:
            haystacks.append(norm)
        if latin and latin not in haystacks:
            haystacks.append(latin)
    best = 0.0
    for query in query_variants:
        for text in haystacks:
            if not query or not text:
                continue
            score = max(
                fuzz.WRatio(query, text),
                fuzz.token_set_ratio(query, text),
                fuzz.partial_ratio(query, text) * 0.92,
            )
            if text == query:
                score += 20
            elif text.startswith(query):
                score += 12
            elif query in text:
                score += 8
            best = max(best, float(score))
    title_len_penalty = min(6.0, max(0, len(_title_for_book(book)) - 60) / 20)
    return max(0.0, best + _book_popularity_score(book) - title_len_penalty)


def search_connected_books_page(query: str, *, limit: int = 10, offset: int = 0) -> dict:
    cleaned = normalize(query)
    if not cleaned:
        return {"books": [], "total": 0}
    limit = max(1, min(10, int(limit or 10)))
    offset = max(0, int(offset or 0))
    desired = min(100, max(limit + offset, limit, 20))
    variants: list[str] = []
    for candidate in (cleaned, cleaned.replace("ʻ", ""), latinize_text(cleaned)):
        candidate = str(candidate or "").strip()
        if candidate and candidate not in variants:
            variants.append(candidate)

    scored: dict[str, tuple[dict, float]] = {}

    es_failed = False
    for variant in variants:
        es_rows, es_error = _search_es(variant, size=desired)
        if es_error:
            es_failed = True
        for source, es_score, book_id in es_rows:
            bid = str(book_id or source.get("id") or "").strip()
            if not bid:
                continue
            book = dict(db_get_book_by_id(bid) or source or {})
            book["id"] = bid
            if not _is_pdf_accessible_book(book):
                continue
            score = max(_book_search_score(book, variants), 75.0 + min(25.0, float(es_score or 0.0)))
            current = scored.get(bid)
            if not current or score > current[1]:
                scored[bid] = (book, score)

    if es_failed or not scored:
        for book in _search_pg_fallback(query, desired):
            bid = str(book.get("id") or "").strip()
            if not bid or not _is_pdf_accessible_book(book):
                continue
            score = _book_search_score(book, variants)
            if score < 35:
                continue
            current = scored.get(bid)
            if not current or score > current[1]:
                scored[bid] = (dict(book), score)

    ranked = sorted(
        scored.values(),
        key=lambda pair: (-pair[1], len(_title_for_book(pair[0])), _title_for_book(pair[0]).lower()),
    )
    books = [book for book, _score in ranked]
    return {"books": books[offset:offset + limit], "total": len(books)}


def search_connected_books(query: str, limit: int = 10) -> list[dict]:
    result = search_connected_books_page(query, limit=limit, offset=0)
    return list(result.get("books") or [])


def build_results_message(
    query: str,
    books: list[dict],
    *,
    page: int = 1,
    pages: int = 1,
    total: int | None = None,
    start_index: int = 1,
    lang: str = "uz",
) -> str:
    total_count = len(books) if total is None else int(total or 0)
    labels = {
        "uz": {
            "found": f"📚 {total_count} ta natija topildi",
            "page": f"Sahifa {page}/{pages}",
            "hint": "👇 Pastdagi raqamli tugmalardan birini bosing.",
        },
        "en": {
            "found": f"📚 {total_count} results found",
            "page": f"Page {page}/{pages}",
            "hint": "👇 Tap one of the numbered buttons below.",
        },
        "ru": {
            "found": f"📚 Найдено результатов: {total_count}",
            "page": f"Страница {page}/{pages}",
            "hint": "👇 Нажмите одну из цифровых кнопок ниже.",
        },
    }.get(str(lang or "").strip().lower(), {
        "found": f"📚 {total_count} results found",
        "page": f"Page {page}/{pages}",
        "hint": "👇 Tap one of the numbered buttons below.",
    })
    lines = [labels["found"], f"{query}", labels["page"], ""]
    for idx, book in enumerate(books, start=1):
        lines.append(f"{start_index + idx - 1}. {_title_for_book(book)}")
    lines.extend(["", labels["hint"]])
    return "\n".join(lines)
