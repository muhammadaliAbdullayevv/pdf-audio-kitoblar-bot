#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from typing import Any

from dotenv import load_dotenv

from db import RealDictCursor, db_conn
import upload_flow


def _normalize_key(text: str) -> str:
    s = str(text or "").strip().lower()
    s = s.replace("'", "").replace("’", "").replace("ʻ", "").replace("ʼ", "")
    s = re.sub(r"@[\w]+", " ", s)
    s = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+|telegram\.me/\S+", " ", s)
    s = s.replace("_", " ")
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_weak_title(text: str) -> bool:
    title = upload_flow._normalize_title_candidate(text or "")
    if not title:
        return True
    lower = title.lower()
    if upload_flow._is_bad_movie_title_candidate(title):
        return True
    if len(lower) <= 2:
        return True
    if re.fullmatch(r"(.)\1{2,}", lower):
        return True
    return False


def _title_from_filename_like(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    stem, ext = os.path.splitext(raw)
    if ext.lower() in {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}:
        candidate = upload_flow._humanize_movie_filename_stem(stem)
    else:
        candidate = upload_flow._humanize_movie_filename_stem(raw)
    return upload_flow._normalize_title_candidate(candidate)


def _pick_best_title(row: dict[str, Any]) -> tuple[str, str]:
    caption = str(row.get("caption_text") or "").strip()
    parsed = upload_flow._parse_movie_caption(caption) if caption else {}
    parsed_title = upload_flow._normalize_title_candidate(parsed.get("parsed_title") or "")
    if parsed_title and not _is_weak_title(parsed_title):
        return parsed_title, "caption"

    for key in ("display_name", "movie_name"):
        candidate = upload_flow._normalize_title_candidate(row.get(key) or "")
        if candidate and not _is_weak_title(candidate):
            return candidate, key

    for key in ("display_name", "movie_name"):
        candidate = _title_from_filename_like(row.get(key) or "")
        if candidate and not _is_weak_title(candidate):
            return candidate, f"{key}_filename"

    return "", ""


def _build_search_text(row: dict[str, Any], display_name: str) -> str:
    parts = [
        str(display_name or "").strip(),
        str(row.get("release_year") or "").strip(),
        str(row.get("genre") or "").strip(),
        str(row.get("movie_lang") or "").strip(),
        str(row.get("country") or "").strip(),
        str(row.get("rating") or "").strip(),
        str(row.get("caption_text") or "").strip(),
    ]
    return upload_flow._normalize_search_text(" ".join([p for p in parts if p])) or ""


def _fetch_movies(limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT
            id, movie_name, display_name, caption_text, search_text, indexed,
            file_id, file_unique_id, path, mime_type, duration_seconds, file_size,
            channel_id, channel_message_id, release_year, genre, movie_lang, country, rating,
            created_at
        FROM movies
        ORDER BY created_at ASC
    """
    params: tuple[Any, ...] = ()
    if limit and limit > 0:
        sql += " LIMIT %s"
        params = (int(limit),)

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])


def _es_client():
    es_url = str(os.getenv("ES_URL", "") or "").strip()
    if not es_url:
        return None
    try:
        from elasticsearch import Elasticsearch
    except Exception:
        return None

    kwargs: dict[str, Any] = {}
    es_ca_cert = str(os.getenv("ES_CA_CERT", "") or "").strip()
    es_user = str(os.getenv("ES_USER", "") or "").strip()
    es_pass = str(os.getenv("ES_PASS", "") or "").strip()
    if es_ca_cert:
        kwargs["ca_certs"] = es_ca_cert
    if es_user and es_pass:
        kwargs["basic_auth"] = (es_user, es_pass)
    try:
        return Elasticsearch(es_url, **kwargs)
    except Exception:
        return None


def _reindex_movies_es(rows: list[dict[str, Any]]) -> tuple[int, int, list[str]]:
    es = _es_client()
    if not es:
        return 0, len(rows), []

    index_name = str(os.getenv("MOVIES_ES_INDEX", "movies") or "movies").strip() or "movies"
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
    except Exception:
        return 0, len(rows), []

    ok = 0
    failed = 0
    ok_ids: list[str] = []
    for row in rows:
        try:
            movie_id = str(row.get("id") or "").strip()
            if not movie_id:
                failed += 1
                continue
            release_year = row.get("release_year")
            doc = {
                "id": movie_id,
                "movie_name": row.get("movie_name"),
                "display_name": row.get("display_name"),
                "file_id": row.get("file_id"),
                "file_unique_id": row.get("file_unique_id"),
                "path": row.get("path"),
                "mime_type": row.get("mime_type"),
                "duration_seconds": row.get("duration_seconds"),
                "file_size": row.get("file_size"),
                "channel_id": row.get("channel_id"),
                "channel_message_id": row.get("channel_message_id"),
                "release_year": release_year,
                "release_year_text": str(release_year) if release_year else None,
                "genre": row.get("genre"),
                "movie_lang": row.get("movie_lang"),
                "country": row.get("country"),
                "rating": row.get("rating"),
                "caption_text": row.get("caption_text"),
                "search_text": row.get("search_text"),
                "indexed": True,
            }
            es.index(index=index_name, id=movie_id, document=doc, refresh=False)
            ok += 1
            ok_ids.append(movie_id)
        except Exception:
            failed += 1
    try:
        es.indices.refresh(index=index_name)
    except Exception:
        pass
    return ok, failed, ok_ids


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh old/weak movie names from caption metadata.")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for processing.")
    parser.add_argument("--apply", action="store_true", help="Apply DB updates. Without this, runs dry-run.")
    parser.add_argument("--reindex-es", action="store_true", help="Reindex updated movies into Elasticsearch.")
    parser.add_argument("--sample", type=int, default=20, help="How many sample changes to print.")
    args = parser.parse_args()

    load_dotenv(override=False)

    movies = _fetch_movies(limit=args.limit if args.limit > 0 else None)
    total = len(movies)
    updates: list[dict[str, Any]] = []

    for row in movies:
        current_display = upload_flow._normalize_title_candidate(row.get("display_name") or "")
        current_movie = upload_flow._normalize_title_candidate(row.get("movie_name") or "")
        current_best = current_display or current_movie

        weak_now = _is_weak_title(current_best) or _is_weak_title(current_movie)
        if not weak_now:
            continue

        best_title, source = _pick_best_title(row)
        if not best_title:
            continue

        if _normalize_key(best_title) == _normalize_key(current_best):
            continue

        normalized_name = _normalize_key(best_title)
        if not normalized_name:
            continue

        updated = dict(row)
        updated["movie_name"] = normalized_name
        updated["display_name"] = best_title
        updated["search_text"] = _build_search_text(row, best_title)
        updates.append(
            {
                "id": row.get("id"),
                "old_display": row.get("display_name"),
                "old_movie_name": row.get("movie_name"),
                "new_display": updated["display_name"],
                "new_movie_name": updated["movie_name"],
                "source": source,
                "row": updated,
            }
        )

    print(f"Scanned movies: {total}")
    print(f"Candidate updates: {len(updates)}")
    show = max(0, int(args.sample or 0))
    if show:
        print("\nSample changes:")
        for u in updates[:show]:
            print(
                f"- {u['id']}: [{u['old_display']}] / [{u['old_movie_name']}] -> "
                f"[{u['new_display']}] / [{u['new_movie_name']}] (source={u['source']})"
            )

    if not args.apply:
        print("\nDry-run only. Re-run with --apply to write changes.")
        return 0

    es_configured = bool(str(os.getenv("ES_URL", "") or "").strip())
    with db_conn() as conn:
        with conn.cursor() as cur:
            for u in updates:
                row = u["row"]
                indexed_value = False if es_configured else bool(row.get("indexed"))
                cur.execute(
                    """
                    UPDATE movies
                    SET movie_name=%s,
                        display_name=%s,
                        search_text=%s,
                        indexed=%s
                    WHERE id=%s
                    """,
                    (
                        row.get("movie_name"),
                        row.get("display_name"),
                        row.get("search_text"),
                        indexed_value,
                        str(u["id"]),
                    ),
                )
    print(f"\nUpdated DB rows: {len(updates)}")

    if args.reindex_es and updates:
        rows_for_es = [u["row"] for u in updates]
        ok, failed, ok_ids = _reindex_movies_es(rows_for_es)
        print(f"ES reindex: ok={ok}, failed={failed}")
        if ok_ids:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE movies SET indexed=TRUE WHERE id = ANY(%s)",
                        (ok_ids,),
                    )
    elif es_configured:
        print("ES is configured. Updated rows were marked indexed=FALSE for startup/background re-sync.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
