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
from typing import Any, Iterable

from telethon import TelegramClient
from telethon.errors import FloodWaitError

import bot as bot_runtime


LANGUAGE_BUTTON_TEXTS = {
    "uz": "🇺🇿 Uzbek",
    "en": "🇬🇧 English",
    "ru": "🇷🇺 Russian",
}

SEARCH_MENU_TEXTS = {
    "uz": "🔎 Kitob qidirish",
    "en": "🔎 Search Books",
    "ru": "🔎 Поиск книг",
}

SEARCH_RESULT_MARKERS = (
    "ta natija topildi",
    "results found",
    "найдено",
    "sahifa",
    "page",
    "страница",
)

AUDIOBOOK_BUTTON_MARKERS = ("audiobook", "audio", "аудио", "audiokitob", "аудиокнига", "🎧")
PLAY_ALL_MARKERS = ("barchasini", "listen all", "all", "все")


def _pick_title(book: dict[str, Any]) -> str:
    return str(book.get("display_name") or book.get("book_name") or "").strip()


def _load_sample_books(sample_size: int) -> list[dict[str, Any]]:
    books = list(bot_runtime.db_get_random_books(limit=sample_size, require_accessible=True) or [])
    if not books:
        books = list(bot_runtime.db_list_books() or [])
    return [dict(book) for book in books[:sample_size]]


def _load_audiobook_books(books: list[dict[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for book in books:
        book_id = str(book.get("id") or "").strip()
        if not book_id:
            continue
        try:
            if bot_runtime.get_audio_book_for_book(book_id):
                out.append(book)
        except Exception:
            continue
        if len(out) >= limit:
            break
    return out


def _build_queries(books: list[dict[str, Any]], max_queries: int = 150) -> list[str]:
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
    return queries or ["Ikki eshik orasi", "Atomic Habits", "Мастер и Маргарита"]


@dataclass
class Metrics:
    started_at: float = field(default_factory=time.perf_counter)
    completed: int = 0
    failed: int = 0
    by_kind: Counter = field(default_factory=Counter)
    failures_by_kind: Counter = field(default_factory=Counter)
    flood_waits: int = 0
    flood_wait_seconds: float = 0.0
    latencies_ms: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))

    def record(self, kind: str, elapsed_ms: float, ok: bool) -> None:
        if ok:
            self.completed += 1
            self.by_kind[kind] += 1
            self.latencies_ms[kind].append(elapsed_ms)
        else:
            self.failed += 1
            self.failures_by_kind[kind] += 1


def _flatten_button_texts(message) -> list[str]:
    out: list[str] = []
    rows = getattr(message, "buttons", None) or []
    for row in rows:
        for btn in row:
            text = str(getattr(btn, "text", "") or "").strip()
            if text:
                out.append(text)
    return out


def _message_text(message) -> str:
    return str(getattr(message, "raw_text", "") or getattr(message, "message", "") or "").strip()


def _message_has_file(message) -> bool:
    return bool(getattr(message, "file", None) or getattr(message, "document", None))


def _message_has_audio(message) -> bool:
    return bool(getattr(message, "audio", None) or getattr(message, "voice", None))


def _message_looks_like_search_result(message) -> bool:
    text = _message_text(message).lower()
    if any(marker in text for marker in SEARCH_RESULT_MARKERS):
        return True
    return any(btn.isdigit() for btn in _flatten_button_texts(message))


def _find_first_numeric_button_message(messages: Iterable[Any]):
    for msg in messages:
        for text in _flatten_button_texts(msg):
            if text.isdigit():
                return msg, text
    return None, None


def _find_numeric_button_message(messages: Iterable[Any]):
    for msg in messages:
        numeric = [text for text in _flatten_button_texts(msg) if text.isdigit()]
        if numeric:
            return msg, numeric
    return None, []


def _find_keyword_button_message(messages: Iterable[Any], keywords: tuple[str, ...]):
    lowered = tuple(k.lower() for k in keywords)
    for msg in messages:
        for text in _flatten_button_texts(msg):
            raw = text.lower()
            if any(k in raw for k in lowered):
                return msg, text
    return None, None


async def _last_message_id(client: TelegramClient, entity) -> int:
    messages = await client.get_messages(entity, limit=1)
    if not messages:
        return 0
    message = messages[0]
    return int(getattr(message, "id", 0) or 0)


async def _poll_new_messages(
    client: TelegramClient,
    entity,
    *,
    min_id: int,
    timeout_s: float = 10.0,
    limit: int = 20,
    poll_interval_s: float = 0.8,
) -> list[Any]:
    deadline = time.monotonic() + max(0.5, timeout_s)
    while time.monotonic() < deadline:
        messages = await client.get_messages(entity, limit=limit)
        fresh = [
            msg for msg in reversed(list(messages or []))
            if int(getattr(msg, "id", 0) or 0) > int(min_id)
            and not bool(getattr(msg, "out", False))
        ]
        if fresh:
            return fresh
        await asyncio.sleep(max(0.2, float(poll_interval_s or 0.8)))
    return []


async def _ensure_account_ready(client: TelegramClient, entity, lang: str, verbose: bool = False) -> None:
    before_id = await _last_message_id(client, entity)
    await client.send_message(entity, "/start")
    messages = await _poll_new_messages(client, entity, min_id=before_id, timeout_s=8.0, limit=12)
    for message in messages:
        button_texts = _flatten_button_texts(message)
        if LANGUAGE_BUTTON_TEXTS.get(lang) in button_texts:
            if verbose:
                print(f"[setup] clicking language button {LANGUAGE_BUTTON_TEXTS[lang]!r}")
            await message.click(text=LANGUAGE_BUTTON_TEXTS[lang])
            await asyncio.sleep(1.0)
            return


async def _safe_click(message, text: str) -> None:
    await message.click(text=text)


async def _run_search(
    client: TelegramClient,
    entity,
    query: str,
    *,
    use_menu: bool,
    lang: str,
) -> tuple[bool, list[Any]]:
    baseline = await _last_message_id(client, entity)
    if use_menu:
        await client.send_message(entity, SEARCH_MENU_TEXTS.get(lang, SEARCH_MENU_TEXTS["en"]))
        # Do not hammer get_history while waiting for the prompt. The button only
        # toggles search mode, so a short settle delay is enough before sending the query.
        await asyncio.sleep(0.9)
        prompt_messages = await _poll_new_messages(
            client,
            entity,
            min_id=baseline,
            timeout_s=2.5,
            limit=6,
            poll_interval_s=1.0,
        )
        if prompt_messages:
            baseline = int(getattr(prompt_messages[-1], "id", baseline) or baseline)
    await client.send_message(entity, query)
    responses = await _poll_new_messages(
        client,
        entity,
        min_id=baseline,
        timeout_s=10.0,
        limit=12,
        poll_interval_s=0.9,
    )
    ok = any(_message_looks_like_search_result(msg) for msg in responses)
    return ok, responses


async def _run_open_book(
    client: TelegramClient,
    entity,
    query: str,
    *,
    use_menu: bool,
    lang: str,
) -> tuple[bool, list[Any]]:
    search_ok, search_messages = await _run_search(client, entity, query, use_menu=use_menu, lang=lang)
    if not search_ok:
        return False, search_messages
    result_msg, button_text = _find_first_numeric_button_message(search_messages)
    if not result_msg or not button_text:
        return False, search_messages
    baseline = await _last_message_id(client, entity)
    await _safe_click(result_msg, button_text)
    responses = await _poll_new_messages(client, entity, min_id=baseline, timeout_s=15.0, limit=16)
    ok = any(_message_has_file(msg) for msg in responses)
    return ok, responses


async def _run_receive_all_books(
    client: TelegramClient,
    entity,
    query: str,
    *,
    use_menu: bool,
    lang: str,
    verbose: bool = False,
) -> tuple[int, int]:
    search_ok, search_messages = await _run_search(client, entity, query, use_menu=use_menu, lang=lang)
    if not search_ok:
        return 0, 0

    result_msg, button_texts = _find_numeric_button_message(search_messages)
    if not result_msg or not button_texts:
        return 0, 0

    numeric_buttons = sorted({text for text in button_texts if text.isdigit()}, key=lambda x: int(x))
    delivered = 0
    attempted = 0
    for button_text in numeric_buttons:
        attempted += 1
        baseline = await _last_message_id(client, entity)
        await _safe_click(result_msg, button_text)
        responses = await _poll_new_messages(client, entity, min_id=baseline, timeout_s=15.0, limit=16)
        got_file = any(_message_has_file(msg) for msg in responses)
        if got_file:
            delivered += 1
        elif verbose:
            print(f"[receive-all] query={query!r} button={button_text} produced no file")
        await asyncio.sleep(0.25)
    return delivered, attempted


async def _run_audiobook(
    client: TelegramClient,
    entity,
    book: dict[str, Any],
    *,
    use_menu: bool,
    lang: str,
    prefer_play_all: bool,
) -> bool:
    query = _pick_title(book)
    if not query:
        return False
    open_ok, open_messages = await _run_open_book(client, entity, query, use_menu=use_menu, lang=lang)
    if not open_ok:
        return False
    ab_msg, ab_text = _find_keyword_button_message(open_messages, AUDIOBOOK_BUTTON_MARKERS)
    if not ab_msg or not ab_text:
        return False
    baseline = await _last_message_id(client, entity)
    await _safe_click(ab_msg, ab_text)
    part_messages = await _poll_new_messages(client, entity, min_id=baseline, timeout_s=10.0, limit=16)
    if not part_messages:
        return False
    target_msg = None
    target_text = None
    if prefer_play_all:
        target_msg, target_text = _find_keyword_button_message(part_messages, PLAY_ALL_MARKERS)
    if not target_msg or not target_text:
        target_msg, target_text = _find_first_numeric_button_message(part_messages)
    if not target_msg or not target_text:
        return False
    baseline = await _last_message_id(client, entity)
    await _safe_click(target_msg, target_text)
    media_messages = await _poll_new_messages(client, entity, min_id=baseline, timeout_s=20.0, limit=20)
    return any(_message_has_audio(msg) or _message_has_file(msg) for msg in media_messages)


async def _run_inline(client: TelegramClient, bot_username: str, query: str) -> bool:
    results = await client.inline_query(bot_username, query)
    return len(results or []) > 0


async def _session_worker(session_name: str, args, metrics: Metrics, queries: list[str], audiobook_books: list[dict[str, Any]]) -> None:
    client = TelegramClient(session_name, args.api_id, args.api_hash)
    await client.start()
    try:
        entity = await client.get_entity(args.bot_username)
        await _ensure_account_ready(client, entity, args.lang, verbose=args.verbose)
        rng = random.Random(hash(session_name) & 0xFFFFFFFF)
        await asyncio.sleep(rng.uniform(0.3, 1.2))
        stop_at = time.perf_counter() + max(1, args.duration)
        while time.perf_counter() < stop_at:
            if args.mode == "search":
                kind = "search"
            elif args.mode == "receive-book":
                kind = "receive-book"
            elif args.mode == "open-book":
                kind = "open-book"
            elif args.mode == "audiobook":
                kind = "audiobook"
            elif args.mode == "inline":
                kind = "inline"
            else:
                roll = rng.random()
                if roll < 0.50:
                    kind = "search"
                elif roll < 0.80:
                    kind = "receive-book"
                elif roll < 0.95:
                    kind = "audiobook"
                else:
                    kind = "inline"

            started = time.perf_counter()
            ok = False
            try:
                if kind == "search":
                    ok, _ = await _run_search(
                        client, entity, rng.choice(queries), use_menu=args.use_search_button, lang=args.lang
                    )
                elif kind == "open-book":
                    ok, _ = await _run_open_book(
                        client, entity, rng.choice(queries), use_menu=args.use_search_button, lang=args.lang
                    )
                elif kind == "receive-book":
                    delivered, attempted = await _run_receive_all_books(
                        client,
                        entity,
                        rng.choice(queries),
                        use_menu=args.use_search_button,
                        lang=args.lang,
                        verbose=args.verbose,
                    )
                    ok = attempted > 0 and delivered == attempted
                elif kind == "audiobook":
                    if audiobook_books:
                        ok = await _run_audiobook(
                            client,
                            entity,
                            rng.choice(audiobook_books),
                            use_menu=args.use_search_button,
                            lang=args.lang,
                            prefer_play_all=args.prefer_play_all,
                        )
                else:
                    ok = await _run_inline(client, args.bot_username, rng.choice(queries))
            except FloodWaitError as e:
                metrics.flood_waits += 1
                metrics.flood_wait_seconds += float(getattr(e, "seconds", 0) or 0)
                if args.verbose:
                    print(f"[{session_name}] flood wait {e.seconds}s during {kind}")
                await asyncio.sleep(min(int(getattr(e, "seconds", 1) or 1), args.max_flood_sleep))
            except Exception as e:
                if args.verbose:
                    print(f"[{session_name}] {kind} failed: {e}")
            finally:
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                metrics.record(kind, elapsed_ms, ok)
            await asyncio.sleep(max(0.0, args.pause_between_ops) + rng.uniform(0.1, 0.5))
    finally:
        await client.disconnect()


def _fmt_ms(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    idx = max(0, min(len(values) - 1, int(round((len(values) - 1) * percentile))))
    return sorted(values)[idx]


def _print_summary(metrics: Metrics, sessions: list[str]) -> None:
    elapsed = max(0.001, time.perf_counter() - metrics.started_at)
    total = metrics.completed + metrics.failed
    print("\n📲 Real Account Shock Summary")
    print("────────────────────────────")
    print(f"sessions={len(sessions)}")
    print(f"elapsed_s={elapsed:.2f}")
    print(f"total_ops={total}")
    print(f"completed={metrics.completed}")
    print(f"failed={metrics.failed}")
    print(f"ops_per_sec={total / elapsed:.2f}")
    print(f"flood_waits={metrics.flood_waits}")
    print(f"flood_wait_seconds={metrics.flood_wait_seconds:.1f}")
    print("by_kind:")
    for kind in sorted(set(metrics.by_kind) | set(metrics.failures_by_kind)):
        ok_count = metrics.by_kind.get(kind, 0)
        fail_count = metrics.failures_by_kind.get(kind, 0)
        values = metrics.latencies_ms.get(kind, [])
        mean = statistics.fmean(values) if values else 0.0
        p50 = _fmt_ms(values, 0.50)
        p95 = _fmt_ms(values, 0.95)
        print(
            f"  - {kind}: ok={ok_count} fail={fail_count} "
            f"mean_ms={mean:.1f} p50_ms={p50:.1f} p95_ms={p95:.1f}"
        )


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Real Telegram user-account shock test using Telethon sessions."
    )
    parser.add_argument(
        "--session",
        action="append",
        dest="sessions",
        help="Telethon session name/path. Repeat to simulate multiple real accounts.",
    )
    parser.add_argument("--api-id", type=int, default=int(os.getenv("TELEGRAM_API_ID", "0") or "0"))
    parser.add_argument("--api-hash", default=os.getenv("TELEGRAM_API_HASH", ""))
    parser.add_argument("--bot-username", default=os.getenv("BOT_PUBLIC_USERNAME", "@pdf_audio_kitoblar_bot"))
    parser.add_argument("--lang", choices=("uz", "en", "ru"), default="uz")
    parser.add_argument(
        "--mode",
        choices=("search", "open-book", "receive-book", "audiobook", "inline", "mixed"),
        default="mixed",
    )
    parser.add_argument("--duration", type=int, default=120, help="Duration in seconds.")
    parser.add_argument("--sample-size", type=int, default=200, help="Sample books to load from DB.")
    parser.add_argument("--use-search-button", action="store_true", help="Tap/send the search button text before each query.")
    parser.add_argument("--prefer-play-all", action="store_true", help="Use the audiobook play-all button when available.")
    parser.add_argument("--pause-between-ops", type=float, default=0.2, help="Delay between actions per account.")
    parser.add_argument("--max-flood-sleep", type=int, default=30, help="Max seconds to sleep on FloodWait.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    if not args.sessions:
        raise SystemExit("At least one --session is required.")
    if not args.api_id or not args.api_hash:
        raise SystemExit("TELEGRAM_API_ID / TELEGRAM_API_HASH (or --api-id / --api-hash) are required.")

    bot_runtime.init_db()
    books = await asyncio.to_thread(_load_sample_books, max(30, args.sample_size))
    if not books:
        raise SystemExit("No books found in DB.")
    audiobook_books = await asyncio.to_thread(_load_audiobook_books, books, 50)
    queries = await asyncio.to_thread(_build_queries, books, 200)

    print("📲 Starting real-account shock test")
    print("───────────────────────────────────")
    print(f"sessions={len(args.sessions)}")
    print(f"mode={args.mode}")
    print(f"duration_s={args.duration}")
    print(f"sample_books={len(books)}")
    print(f"audiobook_books={len(audiobook_books)}")
    print(f"bot={args.bot_username}")

    metrics = Metrics()
    tasks = [
        asyncio.create_task(_session_worker(session_name, args, metrics, queries, audiobook_books))
        for session_name in args.sessions
    ]
    await asyncio.gather(*tasks)
    _print_summary(metrics, args.sessions)
    return 0


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
