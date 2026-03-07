#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

try:
    import psycopg2
    from psycopg2 import pool
except Exception as e:  # pragma: no cover
    raise SystemExit(f"psycopg2 is required: {e}")

try:
    from elasticsearch import Elasticsearch
except Exception:
    Elasticsearch = None  # type: ignore


@dataclass
class RunStats:
    concurrency: int
    duration_s: float
    total: int
    ok: int
    failed: int
    rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    avg_ms: float
    errors_preview: list[str]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    k = (len(values) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    frac = k - lo
    return float(values[lo] * (1.0 - frac) + values[hi] * frac)


def _make_es_client() -> Any | None:
    if Elasticsearch is None:
        return None
    es_url = os.getenv("ES_URL", "").strip()
    if not es_url:
        return None
    kwargs: dict[str, Any] = {"request_timeout": 8}
    es_user = os.getenv("ES_USER", "").strip()
    es_pass = os.getenv("ES_PASS", "").strip()
    if es_user:
        kwargs["basic_auth"] = (es_user, es_pass)
    es_ca = os.getenv("ES_CA_CERT", "").strip()
    if es_ca:
        kwargs["ca_certs"] = es_ca
    return Elasticsearch(es_url, **kwargs)


def _pick_queries(cur, table: str, name_col: str, display_col: str, limit: int) -> list[str]:
    cur.execute(
        f"""
        SELECT COALESCE(NULLIF({display_col}, ''), NULLIF({name_col}, ''))
        FROM {table}
        WHERE COALESCE(NULLIF({display_col}, ''), NULLIF({name_col}, '')) IS NOT NULL
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (int(limit),),
    )
    items: list[str] = []
    for row in cur.fetchall() or []:
        val = str((row or [""])[0] or "").strip()
        if not val:
            continue
        items.append(val[:80])
    return items


def _seed_queries(db_pool: pool.ThreadedConnectionPool, per_kind_limit: int = 120) -> tuple[list[str], list[str]]:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            books = _pick_queries(cur, "books", "book_name", "display_name", per_kind_limit)
            movies = _pick_queries(cur, "movies", "movie_name", "display_name", per_kind_limit)
    finally:
        db_pool.putconn(conn)

    if not books:
        books = ["python", "english", "history", "science", "novel"]
    if not movies:
        movies = ["action", "comedy", "drama", "adventure", "movie"]
    return books, movies


def _do_db_search(db_pool: pool.ThreadedConnectionPool, kind: str, query_text: str) -> None:
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            like = f"%{query_text}%"
            if kind == "movie":
                cur.execute(
                    """
                    SELECT id
                    FROM movies
                    WHERE movie_name ILIKE %s OR COALESCE(display_name, '') ILIKE %s
                    ORDER BY created_at DESC
                    LIMIT 10
                    """,
                    (like, like),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM books
                    WHERE book_name ILIKE %s OR COALESCE(display_name, '') ILIKE %s
                    ORDER BY created_at DESC
                    LIMIT 10
                    """,
                    (like, like),
                )
            cur.fetchall()
    finally:
        db_pool.putconn(conn)


def _do_es_search(es_client: Any, kind: str, query_text: str) -> None:
    index_name = "movies" if kind == "movie" else "books"
    # Keep query close to real bot behavior while staying lightweight.
    es_client.search(
        index=index_name,
        size=10,
        query={
            "multi_match": {
                "query": query_text,
                "fields": ["display_name^2", "movie_name", "book_name", "search_text", "caption_text"],
            }
        },
    )


def run_once(
    db_pool: pool.ThreadedConnectionPool,
    es_client: Any | None,
    concurrency: int,
    duration_s: int,
    profile: str,
    movie_ratio: float,
    books_q: list[str],
    movies_q: list[str],
) -> RunStats:
    from concurrent.futures import ThreadPoolExecutor

    stop_at = time.perf_counter() + max(1, int(duration_s))

    def worker() -> tuple[int, int, list[float], list[str]]:
        ok = 0
        failed = 0
        lats: list[float] = []
        errs: list[str] = []

        while time.perf_counter() < stop_at:
            kind = "movie" if random.random() < movie_ratio else "book"
            q = random.choice(movies_q if kind == "movie" else books_q)
            t0 = time.perf_counter()
            try:
                if profile in {"mixed", "db"}:
                    _do_db_search(db_pool, kind, q)
                if profile in {"mixed", "es"} and es_client is not None:
                    _do_es_search(es_client, kind, q)
                ok += 1
                lats.append((time.perf_counter() - t0) * 1000.0)
            except Exception as e:
                failed += 1
                if len(errs) < 3:
                    errs.append(f"{type(e).__name__}: {e}")
        return ok, failed, lats, errs

    t_start = time.perf_counter()
    futures = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for _ in range(concurrency):
            futures.append(ex.submit(worker))

    all_lat: list[float] = []
    total_ok = 0
    total_fail = 0
    errors: list[str] = []
    for f in futures:
        ok, failed, lats, errs = f.result()
        total_ok += ok
        total_fail += failed
        all_lat.extend(lats)
        for e in errs:
            if len(errors) < 5:
                errors.append(e)

    elapsed = max(0.001, time.perf_counter() - t_start)
    total = total_ok + total_fail
    all_lat.sort()
    avg_ms = statistics.fmean(all_lat) if all_lat else 0.0

    return RunStats(
        concurrency=concurrency,
        duration_s=elapsed,
        total=total,
        ok=total_ok,
        failed=total_fail,
        rps=(total / elapsed),
        p50_ms=_percentile(all_lat, 50),
        p95_ms=_percentile(all_lat, 95),
        p99_ms=_percentile(all_lat, 99),
        avg_ms=float(avg_ms),
        errors_preview=errors,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Concurrent search load test for SmartAIToolsBot")
    p.add_argument("--env", default="/home/muhammadaliabdullayev/Documents/SmartAIToolsBot/.env", help="Path to .env")
    p.add_argument("--profile", choices=["mixed", "db", "es"], default="mixed", help="Search backend profile")
    p.add_argument("--concurrency", default="10,25,50,75", help="Comma-separated concurrency levels")
    p.add_argument("--duration", type=int, default=12, help="Seconds per concurrency level")
    p.add_argument("--movie-ratio", type=float, default=0.30, help="Share of movie queries [0..1]")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(args.env, override=True)

    dsn = {
        "dbname": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASS", ""),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432") or "5432"),
    }

    minc = 4
    maxc = max(20, int(os.getenv("DB_POOL_MAX", "50") or "50"))
    db_pool = pool.ThreadedConnectionPool(minconn=minc, maxconn=maxc, **dsn)

    es_client = _make_es_client()
    if args.profile in {"mixed", "es"} and es_client is None:
        print("WARN: ES client is not configured/available; switching profile to db")
        args.profile = "db"

    books_q, movies_q = _seed_queries(db_pool)

    levels: list[int] = []
    for part in str(args.concurrency).split(","):
        part = part.strip()
        if not part:
            continue
        levels.append(max(1, int(part)))
    if not levels:
        levels = [10, 25, 50]

    print("\n=== SmartAIToolsBot Search Load Test ===")
    print(f"Profile: {args.profile}")
    print(f"DB pool max: {maxc}")
    print(f"Book query seeds: {len(books_q)} | Movie query seeds: {len(movies_q)}")
    print(f"Levels: {levels} | Duration/level: {args.duration}s | Movie ratio: {args.movie_ratio:.2f}\n")

    results: list[RunStats] = []
    for c in levels:
        print(f"Running concurrency={c} ...")
        stat = run_once(
            db_pool=db_pool,
            es_client=es_client,
            concurrency=c,
            duration_s=args.duration,
            profile=args.profile,
            movie_ratio=max(0.0, min(1.0, float(args.movie_ratio))),
            books_q=books_q,
            movies_q=movies_q,
        )
        results.append(stat)

    db_pool.closeall()

    print("\n=== Results ===")
    print("conc\toks\tfail\trps\tp50\tp95\tp99\tavg(ms)")
    for r in results:
        print(
            f"{r.concurrency}\t{r.ok}\t{r.failed}\t{r.rps:.1f}\t"
            f"{r.p50_ms:.1f}\t{r.p95_ms:.1f}\t{r.p99_ms:.1f}\t{r.avg_ms:.1f}"
        )
        if r.errors_preview:
            print("  errors:")
            for e in r.errors_preview:
                print(f"    - {e}")

    print("\nTip: keep p95 under ~1200ms and failure near 0 for stable UX.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
