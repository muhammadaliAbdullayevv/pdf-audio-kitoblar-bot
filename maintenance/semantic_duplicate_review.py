#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any

from rapidfuzz import fuzz

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None  # type: ignore


def _silence_pypdf_noise() -> None:
    for logger_name in ("pypdf", "pypdf._cmap"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.CRITICAL)
        logger.propagate = False
        logger.disabled = True

    try:
        import pypdf._cmap as pypdf_cmap
    except Exception:
        return

    # Some encoding notices are emitted through direct helper references inside
    # pypdf._cmap, so silence them at the call site too.
    pypdf_cmap.logger_error = lambda *args, **kwargs: None
    pypdf_cmap.logger_warning = lambda *args, **kwargs: None


_silence_pypdf_noise()


FORMAT_TOKENS = {
    "pdf", "epub", "djvu", "fb2", "mobi", "doc", "docx", "txt", "rtf", "azw3",
    "kitob", "book", "roman", "qissa", "hikoya", "skan", "scan",
}
NOISE_TOKENS = {
    "muallif", "author", "tom", "jild", "qism", "part", "volume",
}
MIN_SHARED_TOKENS = 2
MIN_WRATIO = 90
MIN_TOKEN_SORT = 93
MAX_CLUSTER_PREVIEW = 50

_APOSTROPHE_RE = re.compile(r"[`´‘’'ʼʹʻʼ]+")
_NONWORD_RE = re.compile(r"[^\w\sʻ]+", flags=re.UNICODE)
_SPACE_RE = re.compile(r"\s+")
_MULTI_DASH_RE = re.compile(r"\s*[-–—:]\s*")

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


@dataclass
class BookRow:
    id: str
    raw: dict[str, Any]
    title: str
    display_name: str
    book_name: str
    path: str
    file_id: str
    file_unique_id: str
    indexed: bool
    downloads: int
    searches: int
    created_at: str
    norm_title: str
    latin_title: str
    token_list: list[str]
    token_set: set[str]
    core_title: str
    core_latin_title: str
    core_tokens: list[str]
    core_token_set: set[str]
    tail_signatures: set[str]
    content_signature: str | None


class DSU:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, value: str) -> None:
        if value not in self.parent:
            self.parent[value] = value

    def find(self, value: str) -> str:
        root = self.parent.setdefault(value, value)
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[value] != value:
            nxt = self.parent[value]
            self.parent[value] = root
            value = nxt
        return root

    def union(self, left: str, right: str) -> None:
        rl = self.find(left)
        rr = self.find(right)
        if rl != rr:
            self.parent[rr] = rl


def _normalize_uzbek_apostrophes(text: str) -> str:
    return _APOSTROPHE_RE.sub("ʻ", str(text or ""))


def normalize(text: str) -> str:
    text = _normalize_uzbek_apostrophes(str(text or "").lower())
    text = re.sub(r"@[\w]+", " ", text)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)
    text = text.replace("_", " ")
    text = _NONWORD_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text


def latinize_text(text: str) -> str:
    if not text:
        return ""
    out: list[str] = []
    for ch in str(text or "").lower():
        if ch in _CYRILLIC_TO_LATIN:
            out.append(_CYRILLIC_TO_LATIN[ch])
        elif ch in _ARABIC_TO_LATIN:
            out.append(_ARABIC_TO_LATIN[ch])
        else:
            out.append(ch)
    return normalize("".join(out))


def _tokenize(text: str) -> list[str]:
    return [tok for tok in normalize(text).split() if tok]


def _strip_format_and_noise(tokens: list[str]) -> list[str]:
    out: list[str] = []
    for tok in tokens:
        if tok in FORMAT_TOKENS:
            continue
        if tok in NOISE_TOKENS:
            continue
        if tok.isdigit() and len(tokens) > 2:
            out.append(tok)
            continue
        out.append(tok)
    return out


def _core_title_from_tokens(tokens: list[str]) -> str:
    clean_tokens = _strip_format_and_noise(tokens)
    if not clean_tokens:
        clean_tokens = tokens[:]
    return " ".join(clean_tokens).strip()


def _tail_signatures(tokens: list[str]) -> set[str]:
    clean_tokens = _strip_format_and_noise(tokens)
    if len(clean_tokens) < 2:
        return set()
    result: set[str] = set()
    max_width = min(5, len(clean_tokens))
    for width in range(2, max_width + 1):
        tail = clean_tokens[-width:]
        if sum(len(tok) for tok in tail) < 8:
            continue
        result.add(" ".join(tail))
    return result


def _created_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value or "")


def _clean_title_penalty(title: str) -> tuple[int, int]:
    tokens = _tokenize(title)
    noise_count = sum(1 for tok in tokens if tok in FORMAT_TOKENS or tok in NOISE_TOKENS)
    return (-noise_count, -len(str(title or "").strip()))


def keeper_sort_key(item: BookRow) -> tuple[Any, ...]:
    noise_penalty = _clean_title_penalty(item.title)
    return (
        1 if item.file_id else 0,
        1 if item.file_unique_id else 0,
        1 if item.path else 0,
        1 if item.indexed else 0,
        item.downloads + item.searches,
        noise_penalty[0],
        noise_penalty[1],
        _created_key(item.created_at),
        item.id,
    )


def _preview_title(item: BookRow, max_len: int = 80) -> str:
    title = item.title or "Untitled"
    if len(title) > max_len:
        return title[: max_len - 1] + "…"
    return title


def _extract_pdf_signature(path: str, max_chars: int = 800) -> str | None:
    if not PdfReader or not path or not os.path.exists(path):
        return None
    try:
        reader = PdfReader(path)
        chunks: list[str] = []
        for page in reader.pages[:3]:
            text = page.extract_text() or ""
            text = normalize(text)
            if text:
                chunks.append(text)
            if sum(len(chunk) for chunk in chunks) >= max_chars:
                break
        merged = " ".join(chunks).strip()
        if not merged:
            return None
        return sha1(merged[:max_chars].encode("utf-8", errors="ignore")).hexdigest()[:16]
    except Exception:
        return None


def _extract_text_signature(path: str, max_chars: int = 800) -> str | None:
    if not path or not os.path.exists(path):
        return None
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".pdf":
            return _extract_pdf_signature(path, max_chars=max_chars)
        if ext in {".txt", ".md", ".rtf"}:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                text = normalize(fh.read(max_chars * 2))
            if not text:
                return None
            return sha1(text[:max_chars].encode("utf-8", errors="ignore")).hexdigest()[:16]
    except Exception:
        return None
    return None


def _book_title(book: dict[str, Any]) -> str:
    return str(book.get("display_name") or book.get("book_name") or "").strip()


def _make_book_row(book: dict[str, Any], *, with_content: bool) -> BookRow | None:
    book_id = str(book.get("id") or "").strip()
    if not book_id:
        return None
    title = _book_title(book)
    norm_title = normalize(title)
    latin_title = latinize_text(title)
    token_list = _tokenize(title)
    core_title = _core_title_from_tokens(token_list)
    core_tokens = _tokenize(core_title)
    path = str(book.get("path") or "").strip()
    return BookRow(
        id=book_id,
        raw=dict(book),
        title=title,
        display_name=str(book.get("display_name") or "").strip(),
        book_name=str(book.get("book_name") or "").strip(),
        path=path,
        file_id=str(book.get("file_id") or "").strip(),
        file_unique_id=str(book.get("file_unique_id") or "").strip(),
        indexed=bool(book.get("indexed")),
        downloads=int(book.get("downloads") or 0),
        searches=int(book.get("searches") or 0),
        created_at=_created_key(book.get("created_at")),
        norm_title=norm_title,
        latin_title=latin_title,
        token_list=token_list,
        token_set=set(token_list),
        core_title=core_title,
        core_latin_title=latinize_text(core_title),
        core_tokens=core_tokens,
        core_token_set=set(core_tokens),
        tail_signatures=_tail_signatures(token_list),
        content_signature=_extract_text_signature(path) if with_content else None,
    )


def _token_overlap(left: set[str], right: set[str]) -> tuple[int, float]:
    if not left or not right:
        return 0, 0.0
    shared = left & right
    union = left | right
    return len(shared), (len(shared) / len(union)) if union else 0.0


def _pair_reasons(left: BookRow, right: BookRow, *, min_wratio: int, min_token_sort: int) -> tuple[bool, list[str], dict[str, float]]:
    reasons: list[str] = []
    metrics: dict[str, float] = {}

    if left.core_title and right.core_title and left.core_title == right.core_title:
        reasons.append("core_title_exact")
    if left.core_latin_title and right.core_latin_title and left.core_latin_title == right.core_latin_title:
        reasons.append("core_latin_exact")
    if left.tail_signatures & right.tail_signatures:
        reasons.append("tail_signature_match")
    if left.content_signature and right.content_signature and left.content_signature == right.content_signature:
        reasons.append("content_signature_match")

    shared_count, jaccard = _token_overlap(left.core_token_set, right.core_token_set)
    metrics["shared_tokens"] = float(shared_count)
    metrics["jaccard"] = round(jaccard, 4)
    if shared_count >= MIN_SHARED_TOKENS and jaccard >= 0.74:
        reasons.append("token_overlap_strong")
    if left.core_token_set and right.core_token_set:
        if left.core_token_set <= right.core_token_set or right.core_token_set <= left.core_token_set:
            if shared_count >= MIN_SHARED_TOKENS:
                reasons.append("token_subset")

    wr = float(fuzz.WRatio(left.core_latin_title or left.latin_title, right.core_latin_title or right.latin_title))
    ts = float(fuzz.token_sort_ratio(left.core_latin_title or left.latin_title, right.core_latin_title or right.latin_title))
    metrics["wratio"] = round(wr, 2)
    metrics["token_sort"] = round(ts, 2)
    if wr >= float(min_wratio):
        reasons.append("wratio_high")
    if ts >= float(min_token_sort):
        reasons.append("token_sort_high")

    strong = (
        "content_signature_match" in reasons
        or "core_title_exact" in reasons
        or "core_latin_exact" in reasons
        or ("tail_signature_match" in reasons and ("wratio_high" in reasons or "token_overlap_strong" in reasons))
        or ("token_subset" in reasons and "wratio_high" in reasons)
        or ("token_overlap_strong" in reasons and "wratio_high" in reasons)
        or ("wratio_high" in reasons and "token_sort_high" in reasons and shared_count >= MIN_SHARED_TOKENS)
    )
    return strong, reasons, metrics


def _bucket_keys(book: BookRow) -> set[str]:
    keys: set[str] = set()
    if book.core_title:
        keys.add(f"core:{book.core_title}")
    if book.core_latin_title:
        keys.add(f"latin:{book.core_latin_title}")
    for sig in book.tail_signatures:
        keys.add(f"tail:{sig}")
    for tok in sorted(book.core_token_set):
        if len(tok) >= 4:
            keys.add(f"tok:{tok}")
    if book.content_signature:
        keys.add(f"content:{book.content_signature}")
    return keys


def _cluster_books(books: list[BookRow], *, min_wratio: int, min_token_sort: int) -> tuple[list[list[BookRow]], dict[tuple[str, str], dict[str, Any]]]:
    dsu = DSU()
    for book in books:
        dsu.add(book.id)

    buckets: dict[str, list[BookRow]] = defaultdict(list)
    for book in books:
        for key in _bucket_keys(book):
            buckets[key].append(book)

    pair_evidence: dict[tuple[str, str], dict[str, Any]] = {}
    for bucket_books in buckets.values():
        if len(bucket_books) < 2:
            continue
        limited = bucket_books[:120]
        for i in range(len(limited)):
            left = limited[i]
            for j in range(i + 1, len(limited)):
                right = limited[j]
                pair_key = tuple(sorted((left.id, right.id)))
                if pair_key in pair_evidence and pair_evidence[pair_key].get("accepted"):
                    continue
                accepted, reasons, metrics = _pair_reasons(left, right, min_wratio=min_wratio, min_token_sort=min_token_sort)
                if not reasons:
                    continue
                pair_evidence[pair_key] = {
                    "left_id": left.id,
                    "right_id": right.id,
                    "accepted": accepted,
                    "reasons": reasons,
                    "metrics": metrics,
                }
                if accepted:
                    dsu.union(left.id, right.id)

    grouped: dict[str, list[BookRow]] = defaultdict(list)
    for book in books:
        grouped[dsu.find(book.id)].append(book)

    clusters = [sorted(rows, key=keeper_sort_key, reverse=True) for rows in grouped.values() if len(rows) > 1]
    clusters.sort(key=lambda rows: (len(rows), sum(item.downloads + item.searches for item in rows)), reverse=True)
    return clusters, pair_evidence


def _cluster_confidence(cluster: list[BookRow], pair_evidence: dict[tuple[str, str], dict[str, Any]]) -> str:
    reasons: set[str] = set()
    wr_values: list[float] = []
    for i in range(len(cluster)):
        for j in range(i + 1, len(cluster)):
            pair = pair_evidence.get(tuple(sorted((cluster[i].id, cluster[j].id))))
            if not pair or not pair.get("accepted"):
                continue
            reasons.update(pair.get("reasons") or [])
            wr = float((pair.get("metrics") or {}).get("wratio") or 0.0)
            if wr:
                wr_values.append(wr)
    if "content_signature_match" in reasons or "core_title_exact" in reasons or "core_latin_exact" in reasons:
        return "high"
    avg_wr = (sum(wr_values) / len(wr_values)) if wr_values else 0.0
    if avg_wr >= 94:
        return "high"
    if avg_wr >= 89:
        return "medium"
    return "review"


def _cluster_report(cluster: list[BookRow], pair_evidence: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    keeper = max(cluster, key=keeper_sort_key)
    title_suggestion = min(
        (item for item in cluster if item.title),
        key=lambda item: (len(item.title), item.title.lower()),
        default=keeper,
    )
    members: list[dict[str, Any]] = []
    for item in sorted(cluster, key=keeper_sort_key, reverse=True):
        reasons_to_keeper: list[str] = []
        metrics_to_keeper: dict[str, Any] = {}
        if item.id != keeper.id:
            pair = pair_evidence.get(tuple(sorted((item.id, keeper.id))))
            if pair:
                reasons_to_keeper = list(pair.get("reasons") or [])
                metrics_to_keeper = dict(pair.get("metrics") or {})
        members.append(
            {
                "id": item.id,
                "title": item.title,
                "book_name": item.book_name,
                "display_name": item.display_name,
                "path": item.path,
                "file_id": bool(item.file_id),
                "file_unique_id": bool(item.file_unique_id),
                "indexed": item.indexed,
                "downloads": item.downloads,
                "searches": item.searches,
                "norm_title": item.norm_title,
                "core_title": item.core_title,
                "content_signature": item.content_signature,
                "keeper_match_reasons": reasons_to_keeper,
                "keeper_match_metrics": metrics_to_keeper,
            }
        )
    return {
        "cluster_size": len(cluster),
        "confidence": _cluster_confidence(cluster, pair_evidence),
        "keeper_id": keeper.id,
        "canonical_title_suggestion": title_suggestion.title or keeper.title,
        "preview": [_preview_title(item) for item in cluster[:5]],
        "members": members,
    }


def _summary(clusters: list[list[BookRow]]) -> dict[str, Any]:
    duplicate_rows = sum(max(0, len(cluster) - 1) for cluster in clusters)
    return {
        "cluster_count": len(clusters),
        "duplicate_rows": duplicate_rows,
        "largest_cluster": max((len(cluster) for cluster in clusters), default=0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a semantic duplicate review report for books in PostgreSQL."
    )
    parser.add_argument("--limit", type=int, default=0, help="Only inspect the first N books (0 = all).")
    parser.add_argument("--min-wratio", type=int, default=MIN_WRATIO)
    parser.add_argument("--min-token-sort", type=int, default=MIN_TOKEN_SORT)
    parser.add_argument("--with-content", action="store_true", help="Also sample local PDF/TXT content signatures when available.")
    parser.add_argument("--json-out", default=os.path.join(BASE_DIR, "tmp", "semantic_duplicate_review.json"))
    parser.add_argument("--max-clusters", type=int, default=MAX_CLUSTER_PREVIEW, help="How many clusters to write to the report preview.")
    args = parser.parse_args()

    try:
        books_raw = list(db.list_books() or [])
    except Exception as e:
        print(f"Failed to load books from DB: {e}", file=sys.stderr)
        return 1
    if args.limit and args.limit > 0:
        books_raw = books_raw[: int(args.limit)]

    books: list[BookRow] = []
    for raw in books_raw:
        row = _make_book_row(dict(raw or {}), with_content=bool(args.with_content))
        if row is not None:
            books.append(row)

    clusters, pair_evidence = _cluster_books(
        books,
        min_wratio=int(args.min_wratio),
        min_token_sort=int(args.min_token_sort),
    )
    report_clusters = [_cluster_report(cluster, pair_evidence) for cluster in clusters[: max(1, int(args.max_clusters or MAX_CLUSTER_PREVIEW))]]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books_scanned": len(books),
        "with_content": bool(args.with_content),
        "thresholds": {
            "min_wratio": int(args.min_wratio),
            "min_token_sort": int(args.min_token_sort),
            "min_shared_tokens": MIN_SHARED_TOKENS,
        },
        "summary": _summary(clusters),
        "clusters": report_clusters,
    }

    out_path = os.path.abspath(str(args.json_out))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    print(f"Books scanned: {len(books)}")
    print(f"Duplicate clusters: {report['summary']['cluster_count']}")
    print(f"Duplicate rows: {report['summary']['duplicate_rows']}")
    print(f"Largest cluster: {report['summary']['largest_cluster']}")
    print(f"Report written to: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
