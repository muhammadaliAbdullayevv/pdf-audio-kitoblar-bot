#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from telegram.ext import ApplicationBuilder

import bot as bot_runtime


def _build_bot_app():
    builder = (
        ApplicationBuilder()
        .token(bot_runtime.TOKEN)
        .connect_timeout(20)
        .read_timeout(60)
        .write_timeout(1200)
        .pool_timeout(bot_runtime.BOT_POOL_TIMEOUT)
        .connection_pool_size(bot_runtime.BOT_CONNECTION_POOL_SIZE)
        .concurrent_updates(bot_runtime.BOT_CONCURRENT_UPDATES)
    )

    bot_api_base_url = bot_runtime._normalize_bot_api_base_url(
        bot_runtime.os.getenv("TELEGRAM_BOT_API_BASE_URL", "")
    )
    bot_api_base_file_url = bot_runtime._normalize_bot_api_base_file_url(
        bot_runtime.os.getenv("TELEGRAM_BOT_API_BASE_FILE_URL", ""),
        bot_api_base_url,
    )
    bot_api_local_mode = bot_runtime._env_bool("TELEGRAM_BOT_API_LOCAL_MODE", False)

    if bot_api_base_url:
        builder = builder.base_url(bot_api_base_url)
    if bot_api_base_file_url:
        builder = builder.base_file_url(bot_api_base_file_url)
    if bot_api_local_mode:
        builder = builder.local_mode(True)

    return builder.build()


def _pick_title(book: dict[str, Any]) -> str:
    return str(book.get("display_name") or book.get("book_name") or "").strip()


def _build_queries(books: list[dict[str, Any]], max_queries: int = 200) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()
    for book in books:
        title = _pick_title(book)
        if not title:
            continue
        variants = [title]
        parts = [p for p in title.replace("-", " ").split() if len(p) >= 3]
        if parts:
            variants.append(parts[0])
        if len(parts) >= 2:
            variants.append(" ".join(parts[:2]))
        for variant in variants:
            q = str(variant).strip()
            if not q:
                continue
            key = q.lower()
            if key in seen:
                continue
            seen.add(key)
            queries.append(q)
            if len(queries) >= max_queries:
                return queries
    return queries or ["Atomic Habits", "Ikki eshik orasi", "Мастер и Маргарита"]


def _load_sample_books(sample_size: int) -> list[dict[str, Any]]:
    books = list(bot_runtime.db_get_random_books(limit=sample_size, require_accessible=True) or [])
    if not books:
        books = list(bot_runtime.db_list_books() or [])
    return [dict(book) for book in books[:sample_size]]


def _load_live_send_books(books: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for book in books:
        file_id = str(book.get("file_id") or "").strip()
        if not file_id:
            continue
        out.append(book)
        if len(out) >= limit:
            break
    return out


def _load_audiobook_book_ids(books: list[dict[str, Any]], limit: int = 50) -> list[str]:
    ids: list[str] = []
    for book in books:
        book_id = str(book.get("id") or "").strip()
        if not book_id:
            continue
        try:
            if bot_runtime.get_audio_book_for_book(book_id):
                ids.append(book_id)
        except Exception:
            continue
        if len(ids) >= limit:
            break
    return ids


@dataclass
class Metrics:
    started_at: float = field(default_factory=time.perf_counter)
    completed: int = 0
    failed: int = 0
    by_kind: Counter = field(default_factory=Counter)
    failures_by_kind: Counter = field(default_factory=Counter)
    latencies_ms: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))

    def record(self, kind: str, elapsed_ms: float, ok: bool) -> None:
        if ok:
            self.completed += 1
            self.by_kind[kind] += 1
            self.latencies_ms[kind].append(elapsed_ms)
        else:
            self.failed += 1
            self.failures_by_kind[kind] += 1


async def _run_search_once(query: str) -> int:
    results = await asyncio.to_thread(bot_runtime._inline_search_books, query, 10)
    return len(results or [])


async def _run_delivery_once(book: dict[str, Any], user_id: int) -> dict[str, Any] | None:
    book_id = str(book.get("id") or "").strip()
    if not book_id:
        return None
    snapshot = await asyncio.to_thread(bot_runtime.db_get_book_delivery_snapshot, book_id, user_id)
    if not snapshot:
        return None
    counts = {
        "like": int(snapshot.get("like_count") or 0),
        "dislike": int(snapshot.get("dislike_count") or 0),
        "berry": int(snapshot.get("berry_count") or 0),
        "whale": int(snapshot.get("whale_count") or 0),
    }
    await asyncio.to_thread(
        bot_runtime.build_book_keyboard,
        book_id,
        counts,
        bool(snapshot.get("is_favorited")),
        snapshot.get("user_reaction"),
        False,
        False,
        "uz",
        bool(snapshot.get("has_audiobook")),
        False,
    )
    await asyncio.to_thread(
        bot_runtime.build_book_caption,
        snapshot,
        int(snapshot.get("downloads") or 0),
        int(snapshot.get("fav_count") or 0),
        counts,
    )
    return snapshot


async def _run_audiobook_once(book_id: str) -> int:
    abook = await asyncio.to_thread(bot_runtime.get_audio_book_for_book, book_id)
    if not abook:
        return 0
    parts = await asyncio.to_thread(bot_runtime.list_audio_book_parts, str(abook.get("id") or ""))
    return len(parts or [])


async def _run_live_send_once(app, bot_obj, book: dict[str, Any], chat_id: int, reply_to_message_id: int | None) -> None:
    snapshot = await _run_delivery_once(book, chat_id)
    if not snapshot:
        raise RuntimeError("delivery snapshot missing")
    file_id = str(snapshot.get("file_id") or "").strip()
    if not file_id:
        raise RuntimeError("missing file_id")
    counts = {
        "like": int(snapshot.get("like_count") or 0),
        "dislike": int(snapshot.get("dislike_count") or 0),
        "berry": int(snapshot.get("berry_count") or 0),
        "whale": int(snapshot.get("whale_count") or 0),
    }
    caption = await asyncio.to_thread(
        bot_runtime.build_book_caption,
        snapshot,
        int(snapshot.get("downloads") or 0),
        int(snapshot.get("fav_count") or 0),
        counts,
    )
    filename = await asyncio.to_thread(bot_runtime._book_filename, snapshot)
    await bot_obj.send_document(
        chat_id=chat_id,
        document=file_id,
        caption=caption,
        filename=filename,
        reply_to_message_id=reply_to_message_id,
    )


async def _worker(
    worker_id: int,
    args,
    metrics: Metrics,
    queries: list[str],
    books: list[dict[str, Any]],
    audiobook_book_ids: list[str],
    live_books: list[dict[str, Any]],
    stop_at: float,
    app=None,
    bot_obj=None,
) -> None:
    rng = random.Random((worker_id + 1) * 9137)
    op_index = 0
    while True:
        now = time.perf_counter()
        if args.duration and now >= stop_at:
            return
        if args.total_ops and op_index >= args.total_ops:
            return

        if args.mode == "search":
            kind = "search"
        elif args.mode == "delivery":
            kind = "delivery"
        elif args.mode == "audiobook":
            kind = "audiobook"
        elif args.mode == "live-send":
            kind = "live-send"
        else:
            roll = rng.random()
            if roll < 0.55:
                kind = "search"
            elif roll < 0.85:
                kind = "delivery"
            elif roll < 0.95:
                kind = "audiobook"
            else:
                kind = "live-send" if args.chat_id else "delivery"

        started = time.perf_counter()
        ok = False
        try:
            if kind == "search":
                query = rng.choice(queries)
                await _run_search_once(query)
            elif kind == "delivery":
                book = rng.choice(books)
                user_id = 10_000_000 + rng.randrange(max(1, args.simulated_users))
                await _run_delivery_once(book, user_id)
            elif kind == "audiobook":
                if not audiobook_book_ids:
                    raise RuntimeError("no audiobook books in sample")
                await _run_audiobook_once(rng.choice(audiobook_book_ids))
            elif kind == "live-send":
                if not args.chat_id:
                    raise RuntimeError("chat_id required for live-send")
                if not live_books:
                    raise RuntimeError("no cached file_id books available for live-send")
                await _run_live_send_once(app, bot_obj, rng.choice(live_books), args.chat_id, args.reply_to_message_id)
            ok = True
        except Exception as e:
            if args.verbose:
                print(f"[worker {worker_id}] {kind} failed: {e}")
        finally:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            metrics.record(kind, elapsed_ms, ok)
            op_index += 1


def _fmt_ms(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = max(0, min(len(values) - 1, int(round((len(values) - 1) * percentile))))
    return sorted(values)[idx]


def _print_summary(metrics: Metrics) -> None:
    elapsed = max(0.001, time.perf_counter() - metrics.started_at)
    total = metrics.completed + metrics.failed
    print("\n🔥 Shock Test Summary")
    print("────────────────────")
    print(f"elapsed_s={elapsed:.2f}")
    print(f"total_ops={total}")
    print(f"completed={metrics.completed}")
    print(f"failed={metrics.failed}")
    print(f"ops_per_sec={total / elapsed:.2f}")
    print("by_kind:")
    for kind in sorted(set(metrics.by_kind) | set(metrics.failures_by_kind)):
        ok_count = metrics.by_kind.get(kind, 0)
        fail_count = metrics.failures_by_kind.get(kind, 0)
        values = metrics.latencies_ms.get(kind, [])
        p50 = _fmt_ms(values, 0.50)
        p95 = _fmt_ms(values, 0.95)
        mean = statistics.fmean(values) if values else 0.0
        print(
            f"  - {kind}: ok={ok_count} fail={fail_count} "
            f"mean_ms={mean:.1f} p50_ms={p50:.1f} p95_ms={p95:.1f}"
        )


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Controlled high-load / shock test for the bot hot paths."
    )
    parser.add_argument(
        "--mode",
        choices=("search", "delivery", "audiobook", "mixed", "live-send"),
        default="mixed",
        help="Workload mode. 'live-send' sends real books to a test chat.",
    )
    parser.add_argument("--concurrency", type=int, default=25, help="Concurrent workers.")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds.")
    parser.add_argument("--sample-size", type=int, default=200, help="Sample books to load.")
    parser.add_argument("--simulated-users", type=int, default=100, help="Distinct synthetic user IDs.")
    parser.add_argument("--total-ops", type=int, default=0, help="Per-worker operation cap. 0 means duration-only.")
    parser.add_argument("--chat-id", type=int, default=0, help="Required for live-send mode.")
    parser.add_argument("--reply-to-message-id", type=int, default=0, help="Optional reply target for live-send mode.")
    parser.add_argument("--live-book-limit", type=int, default=50, help="Books with valid file_id to use in live-send mode.")
    parser.add_argument("--verbose", action="store_true", help="Print per-failure details.")
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    if args.concurrency < 1:
        raise SystemExit("concurrency must be >= 1")
    if args.mode == "live-send" and not args.chat_id:
        raise SystemExit("--chat-id is required for --mode live-send")

    bot_runtime.init_db()

    books = await asyncio.to_thread(_load_sample_books, max(20, args.sample_size))
    if not books:
        raise SystemExit("No books found in DB to stress-test.")

    queries = await asyncio.to_thread(_build_queries, books, max(50, min(400, args.sample_size * 2)))
    audiobook_book_ids = await asyncio.to_thread(_load_audiobook_book_ids, books, 50)
    live_books = await asyncio.to_thread(_load_live_send_books, books, max(1, args.live_book_limit))

    print("🔥 Starting shock test")
    print("──────────────────────")
    print(f"mode={args.mode}")
    print(f"concurrency={args.concurrency}")
    print(f"duration_s={args.duration}")
    print(f"sample_books={len(books)}")
    print(f"queries={len(queries)}")
    print(f"audiobook_books={len(audiobook_book_ids)}")
    if args.chat_id:
        print(f"live_chat_id={args.chat_id}")
        print(f"live_books={len(live_books)}")

    metrics = Metrics()
    stop_at = time.perf_counter() + max(1, args.duration)

    app = None
    if args.mode == "live-send":
        app = _build_bot_app()
        await app.initialize()
        await app.start()

    try:
        bot_obj = app.bot if app else None
        tasks = [
            asyncio.create_task(
                _worker(
                    worker_id=i,
                    args=args,
                    metrics=metrics,
                    queries=queries,
                    books=books,
                    audiobook_book_ids=audiobook_book_ids,
                    live_books=live_books,
                    stop_at=stop_at,
                    app=app,
                    bot_obj=bot_obj,
                )
            )
            for i in range(args.concurrency)
        ]
        await asyncio.gather(*tasks)
    finally:
        if app:
            await app.stop()
            await app.shutdown()

    _print_summary(metrics)
    return 0


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
