from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import shutil
import socket
import subprocess
import time
from datetime import date, datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
UI_DIR = BASE_DIR / "dashboard_ui"
LOG_DIR = BASE_DIR / "logs"
LOG_MAIN = LOG_DIR / "errors.log"
_SERVER_START_TS = time.time()
_NET_SNAPSHOT: dict[str, float] | None = None

load_dotenv(dotenv_path=BASE_DIR / ".env", override=False)

from db import (  # noqa: E402
    db_conn,
    get_audio_book_stats,
    get_book_totals,
    get_counters,
    get_db_stats,
    get_favorites_total,
    get_reaction_totals,
    get_storage_stats,
    get_user_status_counts,
)
from elasticsearch import Elasticsearch  # noqa: E402


logger = logging.getLogger("dashboard_server")
logging.basicConfig(
    level=os.getenv("DASHBOARD_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("elastic_transport").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_call(name: str, fn: Callable[..., Any], default: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.warning("%s failed: %s", name, e)
        return default


def _fmt_compact(n: int) -> str:
    abs_n = abs(int(n or 0))
    if abs_n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if abs_n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def _pct_delta(now_val: int, prev_val: int) -> str:
    n = float(now_val or 0)
    p = float(prev_val or 0)
    if p <= 0:
        if n <= 0:
            return "0.0%"
        return "+100.0%"
    pct = ((n - p) / p) * 100.0
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _ratio_pct(part: int | float, total: int | float) -> float:
    p = max(0.0, _safe_float(part, 0.0))
    t = max(0.0, _safe_float(total, 0.0))
    if t <= 0:
        return 0.0
    return round((p / t) * 100.0, 1)


def _query_scalar(sql: str, params: tuple[Any, ...] = ()) -> int:
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if not row:
                    return 0
                return _safe_int(row[0], 0)
    except Exception:
        return 0


def _day_bounds(start_day: date, end_day: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_day, datetime.min.time())
    end_dt = datetime.combine(end_day + timedelta(days=1), datetime.min.time())
    return start_dt, end_dt


def _previous_window(start_day: date, end_day: date) -> tuple[date, date]:
    days = max(1, (end_day - start_day).days + 1)
    prev_end = start_day - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end


def _detect_earliest_day(default_day: date) -> date:
    sql = """
    SELECT MIN(d)
    FROM (
      SELECT MIN(day)::date AS d FROM analytics_daily
      UNION ALL SELECT MIN(joined_date)::date FROM users
      UNION ALL SELECT MIN(DATE(created_at)) FROM books
      UNION ALL SELECT MIN(DATE(created_at)) FROM audio_books
      UNION ALL SELECT MIN(DATE(created_at)) FROM book_requests
      UNION ALL SELECT MIN(DATE(created_at)) FROM upload_requests
    ) q
    """
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
    except Exception as e:
        logger.debug("earliest day query failed: %s", e)
    return default_day


def _resolve_time_range(range_key: str | None, days_override: int | None = None) -> dict[str, Any]:
    today = date.today()

    if days_override and int(days_override) > 0:
        days = max(1, min(int(days_override), 3650))
        start_day = today - timedelta(days=days - 1)
        return {
            "key": f"{days}d",
            "label": f"Last {days} days",
            "start_day": start_day,
            "end_day": today,
            "days": days,
        }

    key = str(range_key or "week").strip().lower()
    presets = {
        "today": (1, "Today"),
        "week": (7, "This week"),
        "month": (30, "This month"),
        "7d": (7, "Last 7 days"),
        "30d": (30, "Last 30 days"),
        "90d": (90, "Last 90 days"),
    }

    if key in presets:
        days, label = presets[key]
        start_day = today - timedelta(days=days - 1)
        return {
            "key": key,
            "label": label,
            "start_day": start_day,
            "end_day": today,
            "days": days,
        }

    if key == "all":
        fallback = today - timedelta(days=29)
        start_day = _detect_earliest_day(fallback)
        if start_day > today:
            start_day = fallback
        days = max(1, (today - start_day).days + 1)
        return {
            "key": "all",
            "label": "All time",
            "start_day": start_day,
            "end_day": today,
            "days": days,
        }

    # default
    start_day = today - timedelta(days=6)
    return {
        "key": "week",
        "label": "This week",
        "start_day": start_day,
        "end_day": today,
        "days": 7,
    }


def _downsample_series(labels: list[str], series: list[list[int]], max_points: int = 60) -> tuple[list[str], list[list[int]]]:
    if len(labels) <= max_points:
        return labels, series
    step = max(1, int(math.ceil(len(labels) / float(max_points))))
    idxs = list(range(0, len(labels), step))
    if idxs[-1] != len(labels) - 1:
        idxs.append(len(labels) - 1)
    out_labels = [labels[i] for i in idxs]
    out_series: list[list[int]] = []
    for points in series:
        out_series.append([_safe_int(points[i], 0) for i in idxs])
    return out_labels, out_series


def _fetch_daily_traffic_series(start_day: date, end_day: date, max_points: int = 60) -> dict[str, Any]:
    req_by_day: dict[date, int] = {}
    dau_by_day: dict[date, int] = {}

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT day, searches, buttons
                FROM analytics_daily
                WHERE day >= %s AND day <= %s
                ORDER BY day ASC
                """,
                (start_day, end_day),
            )
            for day_v, searches, buttons in cur.fetchall() or []:
                req_by_day[day_v] = _safe_int(searches) + _safe_int(buttons)

            cur.execute(
                """
                SELECT day, COUNT(*)
                FROM analytics_daily_users
                WHERE day >= %s AND day <= %s
                GROUP BY day
                ORDER BY day ASC
                """,
                (start_day, end_day),
            )
            for day_v, dau in cur.fetchall() or []:
                dau_by_day[day_v] = _safe_int(dau)

    labels: list[str] = []
    requests_points: list[int] = []
    dau_points: list[int] = []
    d = start_day
    while d <= end_day:
        labels.append(d.strftime("%b %d"))
        requests_points.append(_safe_int(req_by_day.get(d, 0)))
        dau_points.append(_safe_int(dau_by_day.get(d, 0)))
        d += timedelta(days=1)

    labels, series = _downsample_series(labels, [requests_points, dau_points], max_points=max_points)
    return {
        "labels": labels,
        "points": series[0],
        "dau_points": series[1],
    }


def _fetch_catalog_growth_series(start_day: date, end_day: date, max_points: int = 60) -> dict[str, Any]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    books_by_day: dict[date, int] = {}
    audio_by_day: dict[date, int] = {}
    unindexed_by_day: dict[date, int] = {}

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DATE(created_at) AS d, COUNT(*)
                FROM books
                WHERE created_at >= %s AND created_at < %s
                GROUP BY d
                ORDER BY d ASC
                """,
                (start_dt, end_dt),
            )
            for d, cnt in cur.fetchall() or []:
                books_by_day[d] = _safe_int(cnt)

            cur.execute(
                """
                SELECT DATE(created_at) AS d, COUNT(*)
                FROM books
                WHERE created_at >= %s AND created_at < %s AND indexed = FALSE
                GROUP BY d
                ORDER BY d ASC
                """,
                (start_dt, end_dt),
            )
            for d, cnt in cur.fetchall() or []:
                unindexed_by_day[d] = _safe_int(cnt)

            cur.execute(
                """
                SELECT DATE(created_at) AS d, COUNT(*)
                FROM audio_books
                WHERE created_at >= %s AND created_at < %s
                GROUP BY d
                ORDER BY d ASC
                """,
                (start_dt, end_dt),
            )
            for d, cnt in cur.fetchall() or []:
                audio_by_day[d] = _safe_int(cnt)

    labels: list[str] = []
    books_new: list[int] = []
    audio_new: list[int] = []
    unindexed_new: list[int] = []

    d = start_day
    while d <= end_day:
        labels.append(d.strftime("%b %d"))
        books_new.append(_safe_int(books_by_day.get(d, 0)))
        audio_new.append(_safe_int(audio_by_day.get(d, 0)))
        unindexed_new.append(_safe_int(unindexed_by_day.get(d, 0)))
        d += timedelta(days=1)

    labels, series = _downsample_series(labels, [books_new, audio_new, unindexed_new], max_points=max_points)
    return {
        "labels": labels,
        "books_new": series[0],
        "audio_new": series[1],
        "unindexed_new": series[2],
    }


def _distinct_active_users(cur, start_day: date, end_day: date) -> int:
    if start_day > end_day:
        return 0
    cur.execute(
        """
        SELECT COUNT(DISTINCT user_id)
        FROM analytics_daily_users
        WHERE day >= %s AND day <= %s
        """,
        (start_day, end_day),
    )
    return _safe_int((cur.fetchone() or [0])[0], 0)


def _fetch_retention(start_day: date, end_day: date) -> dict[str, Any]:
    range_days = max(1, (end_day - start_day).days + 1)

    with db_conn() as conn:
        with conn.cursor() as cur:
            dau = _distinct_active_users(cur, end_day, end_day)
            dau_prev = _distinct_active_users(cur, end_day - timedelta(days=1), end_day - timedelta(days=1))

            wau_start = max(start_day, end_day - timedelta(days=6))
            wau_days = max(1, (end_day - wau_start).days + 1)
            wau = _distinct_active_users(cur, wau_start, end_day)
            wau_prev_end = wau_start - timedelta(days=1)
            wau_prev_start = wau_prev_end - timedelta(days=wau_days - 1)
            wau_prev = _distinct_active_users(cur, wau_prev_start, wau_prev_end)

            mau_start = max(start_day, end_day - timedelta(days=29))
            mau_days = max(1, (end_day - mau_start).days + 1)
            mau = _distinct_active_users(cur, mau_start, end_day)
            mau_prev_end = mau_start - timedelta(days=1)
            mau_prev_start = mau_prev_end - timedelta(days=mau_days - 1)
            mau_prev = _distinct_active_users(cur, mau_prev_start, mau_prev_end)

            active_in_range = _distinct_active_users(cur, start_day, end_day)
            prev_end = start_day - timedelta(days=1)
            prev_start = prev_end - timedelta(days=range_days - 1)
            active_prev = _distinct_active_users(cur, prev_start, prev_end)

    return {
        "dau": dau,
        "wau": wau,
        "mau": mau,
        "active_in_range": active_in_range,
        "active_prev_in_range": active_prev,
        "stickiness_pct": _ratio_pct(dau, mau),
        "dau_change": _pct_delta(dau, dau_prev),
        "wau_change": _pct_delta(wau, wau_prev),
        "mau_change": _pct_delta(mau, mau_prev),
        "active_change": _pct_delta(active_in_range, active_prev),
        "window_days": {
            "range": range_days,
            "wau": wau_days,
            "mau": mau_days,
        },
    }


def _funnel_step(label: str, value: int, base: int) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "pct": _ratio_pct(value, max(1, base)),
    }


def _fetch_funnel(start_day: date, end_day: date) -> dict[str, Any]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM users WHERE joined_date >= %s AND joined_date <= %s",
                (start_day, end_day),
            )
            new_users = _safe_int((cur.fetchone() or [0])[0])

            cur.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM analytics_daily_users
                WHERE day >= %s AND day <= %s
                """,
                (start_day, end_day),
            )
            active_users = _safe_int((cur.fetchone() or [0])[0])

            cur.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM analytics_daily_users
                WHERE day >= %s AND day <= %s AND COALESCE(searches, 0) > 0
                """,
                (start_day, end_day),
            )
            search_users = _safe_int((cur.fetchone() or [0])[0])

            cur.execute(
                """
                SELECT COUNT(DISTINCT user_id)
                FROM user_recents
                WHERE COALESCE(last_ts, ts) >= %s AND COALESCE(last_ts, ts) < %s
                  AND COALESCE(count, 0) > 0
                """,
                (start_dt, end_dt),
            )
            download_users = _safe_int((cur.fetchone() or [0])[0])

    return {
        "new_users": new_users,
        "active_users": active_users,
        "search_users": search_users,
        "download_users": download_users,
        "steps": [
            _funnel_step("New users", new_users, new_users or 1),
            _funnel_step("Active users", active_users, new_users or 1),
            _funnel_step("Search users", search_users, active_users or 1),
            _funnel_step("Download users", download_users, search_users or 1),
        ],
        "new_to_active_pct": _ratio_pct(active_users, new_users or 1),
        "active_to_search_pct": _ratio_pct(search_users, active_users or 1),
        "search_to_download_pct": _ratio_pct(download_users, search_users or 1),
    }


def _fetch_user_join_leave_counts(start_day: date, end_day: date) -> dict[str, int]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN joined_date >= %s AND joined_date <= %s THEN 1 ELSE 0 END) AS joined_in_range,
                    SUM(CASE WHEN left_date >= %s AND left_date <= %s THEN 1 ELSE 0 END) AS left_in_range
                FROM users
                """,
                (start_day, end_day, start_day, end_day),
            )
            row = cur.fetchone() or (0, 0)
            return {
                "joined": _safe_int(row[0], 0),
                "left": _safe_int(row[1], 0),
            }


def _fetch_catalog_additions(start_day: date, end_day: date) -> dict[str, int]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM books WHERE created_at >= %s AND created_at < %s) AS books_added,
                    (SELECT COUNT(*) FROM audio_books WHERE created_at >= %s AND created_at < %s) AS audios_added
                """,
                (start_dt, end_dt, start_dt, end_dt),
            )
            row = cur.fetchone() or (0, 0)
            return {
                "books": _safe_int(row[0], 0),
                "audios": _safe_int(row[1], 0),
            }


def _top_query_rows(cur, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    out: list[dict[str, Any]] = []
    for q, cnt in cur.fetchall() or []:
        query = str(q or "").strip()
        if not query:
            continue
        out.append({"query": query[:120], "count": _safe_int(cnt)})
    return out


def _fetch_search_quality(start_day: date, end_day: date) -> dict[str, Any]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(searches), 0), COALESCE(SUM(buttons), 0) FROM analytics_daily WHERE day >= %s AND day <= %s",
                (start_day, end_day),
            )
            sums_row = cur.fetchone() or (0, 0)
            searches_total = _safe_int(sums_row[0])
            downloads_total = _safe_int(sums_row[1])

            cur.execute(
                "SELECT COUNT(*) FROM book_requests WHERE created_at >= %s AND created_at < %s",
                (start_dt, end_dt),
            )
            request_queries_total = _safe_int((cur.fetchone() or [0])[0])

            cur.execute(
                """
                SELECT COUNT(*)
                FROM book_requests
                WHERE created_at >= %s AND created_at < %s
                  AND LOWER(COALESCE(status, '')) IN ('no', 'open')
                """,
                (start_dt, end_dt),
            )
            zero_result_total = _safe_int((cur.fetchone() or [0])[0])

            top_queries = _top_query_rows(
                cur,
                """
                SELECT COALESCE(NULLIF(TRIM(query_norm), ''), NULLIF(TRIM(query), ''), 'unknown') AS q, COUNT(*) AS c
                FROM book_requests
                WHERE created_at >= %s AND created_at < %s
                GROUP BY q
                ORDER BY c DESC
                LIMIT 8
                """,
                (start_dt, end_dt),
            )

            zero_result_queries = _top_query_rows(
                cur,
                """
                SELECT COALESCE(NULLIF(TRIM(query_norm), ''), NULLIF(TRIM(query), ''), 'unknown') AS q, COUNT(*) AS c
                FROM book_requests
                WHERE created_at >= %s AND created_at < %s
                  AND LOWER(COALESCE(status, '')) IN ('no', 'open')
                GROUP BY q
                ORDER BY c DESC
                LIMIT 8
                """,
                (start_dt, end_dt),
            )

    return {
        "searches_total": searches_total,
        "downloads_total": downloads_total,
        "book_searches_total": searches_total,
        "book_downloads_total": downloads_total,
        "conversion_pct": _ratio_pct(downloads_total, searches_total or 1),
        "request_queries_total": request_queries_total,
        "zero_result_total": zero_result_total,
        "zero_result_rate_pct": _ratio_pct(zero_result_total, request_queries_total or 1),
        "top_queries": top_queries,
        "zero_result_queries": zero_result_queries,
    }


def _fetch_queue_sla(start_day: date, end_day: date) -> dict[str, Any]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    now = datetime.now()
    terminal = ["done", "accept", "reject", "cancelled", "no", "fulfilled", "complete"]

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM upload_requests WHERE LOWER(COALESCE(status, '')) <> ALL(%s)",
                (terminal,),
            )
            pending_upload_count = _safe_int((cur.fetchone() or [0])[0])

            cur.execute(
                "SELECT MIN(created_at) FROM upload_requests WHERE LOWER(COALESCE(status, '')) <> ALL(%s)",
                (terminal,),
            )
            oldest_pending = (cur.fetchone() or [None])[0]

            cur.execute(
                """
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                FROM upload_requests
                WHERE LOWER(COALESCE(status, '')) = ANY(%s)
                  AND created_at >= %s AND created_at < %s
                  AND created_at IS NOT NULL AND updated_at IS NOT NULL
                  AND updated_at >= created_at
                """,
                (terminal, start_dt, end_dt),
            )
            avg_resolve_sec = _safe_int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                FROM upload_requests
                WHERE LOWER(COALESCE(status, '')) = 'accept'
                  AND created_at >= %s AND created_at < %s
                  AND created_at IS NOT NULL AND updated_at IS NOT NULL
                  AND updated_at >= created_at
                """,
                (start_dt, end_dt),
            )
            avg_accept_sec = _safe_int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                FROM upload_requests
                WHERE LOWER(COALESCE(status, '')) = 'reject'
                  AND created_at >= %s AND created_at < %s
                  AND created_at IS NOT NULL AND updated_at IS NOT NULL
                  AND updated_at >= created_at
                """,
                (start_dt, end_dt),
            )
            avg_reject_sec = _safe_int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)))
                FROM book_requests
                WHERE LOWER(COALESCE(status, '')) IN ('done', 'no', 'cancelled')
                  AND created_at >= %s AND created_at < %s
                  AND created_at IS NOT NULL AND updated_at IS NOT NULL
                  AND updated_at >= created_at
                """,
                (start_dt, end_dt),
            )
            avg_book_resolve_sec = _safe_int((cur.fetchone() or [0])[0] or 0)

    oldest_pending_age = 0
    if oldest_pending and isinstance(oldest_pending, datetime):
        oldest_pending_age = max(0, _safe_int((now - oldest_pending).total_seconds(), 0))

    return {
        "pending_upload_count": pending_upload_count,
        "oldest_pending_age_sec": oldest_pending_age,
        "avg_resolve_sec": avg_resolve_sec,
        "avg_accept_sec": avg_accept_sec,
        "avg_reject_sec": avg_reject_sec,
        "avg_book_request_resolve_sec": avg_book_resolve_sec,
    }


def _lang_name(code: str) -> str:
    c = str(code or "").strip().lower()
    mapping = {
        "en": "English",
        "uz": "Uzbek",
        "ru": "Russian",
        "uz_latn": "Uzbek (Latin)",
        "uz_cyrl": "Uzbek (Cyrillic)",
        "unknown": "Unknown",
    }
    return mapping.get(c, c.upper() if c else "Unknown")


def _fetch_language_usage(start_day: date, end_day: date) -> list[dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH active AS (
                    SELECT DISTINCT user_id
                    FROM analytics_daily_users
                    WHERE day >= %s AND day <= %s
                )
                SELECT
                    COALESCE(NULLIF(LOWER(TRIM(u.language)), ''), 'unknown') AS lang,
                    COUNT(*) AS total_users,
                    SUM(CASE WHEN a.user_id IS NOT NULL THEN 1 ELSE 0 END) AS active_users
                FROM users u
                LEFT JOIN active a ON a.user_id = u.id
                GROUP BY lang
                ORDER BY active_users DESC, total_users DESC
                LIMIT 12
                """,
                (start_day, end_day),
            )
            rows = cur.fetchall() or []

    grand_total = sum(_safe_int(r[1]) for r in rows) or 1
    active_total = sum(_safe_int(r[2]) for r in rows) or 1
    out: list[dict[str, Any]] = []
    for lang, total_users, active_users in rows:
        out.append(
            {
                "language": str(lang or "unknown"),
                "label": _lang_name(str(lang or "unknown")),
                "total_users": _safe_int(total_users),
                "active_users": _safe_int(active_users),
                "share_pct": _ratio_pct(_safe_int(total_users), grand_total),
                "active_share_pct": _ratio_pct(_safe_int(active_users), active_total),
                "active_pct": _ratio_pct(_safe_int(active_users), _safe_int(total_users) or 1),
            }
        )
    return out


def _fetch_status_counts_in_range(table_name: str, start_dt: datetime, end_dt: datetime) -> dict[str, int]:
    allowed = {"book_requests", "upload_requests"}
    table = str(table_name or "").strip().lower()
    if table not in allowed:
        return {}

    sql = f"""
    SELECT LOWER(COALESCE(NULLIF(TRIM(status), ''), 'unknown')) AS status, COUNT(*)
    FROM {table}
    WHERE created_at >= %s AND created_at < %s
    GROUP BY 1
    """
    out: dict[str, int] = {}
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_dt, end_dt))
                for status, cnt in cur.fetchall() or []:
                    key = str(status or "unknown").strip().lower() or "unknown"
                    out[key] = _safe_int(cnt, 0)
    except Exception as e:
        logger.debug("status count query failed for %s: %s", table, e)
    return out


def _fetch_feature_usage(start_day: date, end_day: date) -> list[dict[str, Any]]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    metrics = {
        "Book Search": 0,
        "Book Downloads": 0,
        "Book Requests": 0,
        "Upload Requests": 0,
        "Favorites Added": 0,
        "Book Reactions": 0,
        "Downloader Users": 0,
    }

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(searches), 0), COALESCE(SUM(buttons), 0)
                    FROM analytics_daily
                    WHERE day >= %s AND day <= %s
                    """,
                    (start_day, end_day),
                )
                row = cur.fetchone() or (0, 0)
                metrics["Book Search"] = _safe_int(row[0], 0)
                metrics["Book Downloads"] = _safe_int(row[1], 0)

                cur.execute("SELECT COUNT(*) FROM book_requests WHERE created_at >= %s AND created_at < %s", (start_dt, end_dt))
                metrics["Book Requests"] = _safe_int((cur.fetchone() or [0])[0], 0)

                cur.execute("SELECT COUNT(*) FROM upload_requests WHERE created_at >= %s AND created_at < %s", (start_dt, end_dt))
                metrics["Upload Requests"] = _safe_int((cur.fetchone() or [0])[0], 0)

                cur.execute("SELECT COUNT(*) FROM user_favorites WHERE ts >= %s AND ts < %s", (start_dt, end_dt))
                metrics["Favorites Added"] = _safe_int((cur.fetchone() or [0])[0], 0)

                cur.execute("SELECT COUNT(*) FROM book_reactions WHERE ts >= %s AND ts < %s", (start_dt, end_dt))
                metrics["Book Reactions"] = _safe_int((cur.fetchone() or [0])[0], 0)

                cur.execute(
                    """
                    SELECT COUNT(DISTINCT user_id)
                    FROM user_recents
                    WHERE COALESCE(last_ts, ts) >= %s AND COALESCE(last_ts, ts) < %s
                    """,
                    (start_dt, end_dt),
                )
                metrics["Downloader Users"] = _safe_int((cur.fetchone() or [0])[0], 0)
    except Exception as e:
        logger.debug("feature usage query failed: %s", e)

    total = sum(max(0, _safe_int(v, 0)) for v in metrics.values())
    rows: list[dict[str, Any]] = []
    for name, count in metrics.items():
        c = max(0, _safe_int(count, 0))
        if c <= 0:
            continue
        rows.append(
            {
                "name": name,
                "count": c,
                "pct": _ratio_pct(c, total or 1),
            }
        )
    rows.sort(key=lambda x: _safe_int(x.get("count", 0)), reverse=True)

    if rows:
        return rows[:10]
    return [
        {"name": "Book Search", "count": 0, "pct": 0.0},
        {"name": "Book Downloads", "count": 0, "pct": 0.0},
        {"name": "Book Requests", "count": 0, "pct": 0.0},
        {"name": "Upload Requests", "count": 0, "pct": 0.0},
    ]


def _fetch_peak_hours(start_day: date, end_day: date) -> list[dict[str, Any]]:
    start_dt, end_dt = _day_bounds(start_day, end_day)
    sql = """
    SELECT hr, COUNT(*) AS cnt
    FROM (
      SELECT EXTRACT(HOUR FROM created_at)::int AS hr
      FROM book_requests
      WHERE created_at >= %s AND created_at < %s
      UNION ALL
      SELECT EXTRACT(HOUR FROM created_at)::int AS hr
      FROM upload_requests
      WHERE created_at >= %s AND created_at < %s
      UNION ALL
      SELECT EXTRACT(HOUR FROM ts)::int AS hr
      FROM book_reactions
      WHERE ts >= %s AND ts < %s
      UNION ALL
      SELECT EXTRACT(HOUR FROM COALESCE(last_ts, ts))::int AS hr
      FROM user_recents
      WHERE COALESCE(last_ts, ts) >= %s AND COALESCE(last_ts, ts) < %s
    ) q
    GROUP BY hr
    ORDER BY hr ASC
    """

    counts = {h: 0 for h in range(24)}
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (start_dt, end_dt, start_dt, end_dt, start_dt, end_dt, start_dt, end_dt))
                for hr, cnt in cur.fetchall() or []:
                    if hr is None:
                        continue
                    h = _safe_int(hr)
                    if 0 <= h <= 23:
                        counts[h] = _safe_int(cnt)
    except Exception as e:
        logger.debug("peak hour query failed: %s", e)

    return [{"hour": h, "count": _safe_int(counts.get(h, 0))} for h in range(24)]


def _read_proc_uptime() -> float:
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as f:
            first = f.read().strip().split()[0]
            return _safe_float(first, 0.0)
    except Exception:
        return 0.0


def _read_meminfo() -> dict[str, int]:
    data: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if ":" not in line:
                    continue
                k, v = line.split(":", 1)
                parts = v.strip().split()
                if not parts:
                    continue
                data[k.strip()] = _safe_int(parts[0]) * 1024
    except Exception:
        return {"total": 0, "available": 0, "used": 0, "used_pct": 0.0}

    total = _safe_int(data.get("MemTotal", 0))
    available = _safe_int(data.get("MemAvailable", 0))
    used = max(0, total - available)
    return {
        "total": total,
        "available": available,
        "used": used,
        "used_pct": _ratio_pct(used, total or 1),
    }


def _read_net_totals() -> tuple[int, int]:
    rx_total = 0
    tx_total = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8") as f:
            for line in f.readlines()[2:]:
                if ":" not in line:
                    continue
                iface, rest = line.split(":", 1)
                iface = iface.strip()
                if iface == "lo":
                    continue
                cols = rest.split()
                if len(cols) < 16:
                    continue
                rx_total += _safe_int(cols[0])
                tx_total += _safe_int(cols[8])
    except Exception:
        return 0, 0
    return rx_total, tx_total


def _network_metrics() -> dict[str, Any]:
    global _NET_SNAPSHOT

    now = time.time()
    rx, tx = _read_net_totals()
    rx_rate = 0.0
    tx_rate = 0.0

    if _NET_SNAPSHOT is not None:
        dt = max(0.001, now - _safe_float(_NET_SNAPSHOT.get("ts", now), 0.001))
        rx_prev = _safe_float(_NET_SNAPSHOT.get("rx", 0), 0.0)
        tx_prev = _safe_float(_NET_SNAPSHOT.get("tx", 0), 0.0)
        rx_rate = max(0.0, (rx - rx_prev) / dt)
        tx_rate = max(0.0, (tx - tx_prev) / dt)

    _NET_SNAPSHOT = {"ts": now, "rx": float(rx), "tx": float(tx)}

    return {
        "rx_bytes": _safe_int(rx),
        "tx_bytes": _safe_int(tx),
        "rx_rate_bps": round(rx_rate, 1),
        "tx_rate_bps": round(tx_rate, 1),
    }


def _service_runtime_snapshot(unit_name: str, system_uptime_s: float) -> dict[str, Any]:
    out = {
        "unit": unit_name,
        "status": "unknown",
        "substatus": "unknown",
        "restarts": 0,
        "uptime_sec": 0,
        "pid": 0,
        "active_enter_epoch": 0,
        "active_enter_iso": "",
    }
    try:
        proc = subprocess.run(
            [
                "systemctl",
                "show",
                unit_name,
                "--property=ActiveState,SubState,NRestarts,MainPID,ActiveEnterTimestampMonotonic,ActiveEnterTimestampUSec",
            ],
            capture_output=True,
            text=True,
            timeout=1.6,
            check=False,
        )
    except Exception:
        return out

    if proc.returncode != 0:
        return out

    raw_map: dict[str, str] = {}
    for line in (proc.stdout or "").splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        raw_map[k.strip()] = v.strip()

    active_state = raw_map.get("ActiveState", "unknown")
    sub_state = raw_map.get("SubState", "unknown")
    restarts = _safe_int(raw_map.get("NRestarts", "0"), 0)
    pid = _safe_int(raw_map.get("MainPID", "0"), 0)
    active_enter_mono_us = _safe_int(raw_map.get("ActiveEnterTimestampMonotonic", "0"), 0)
    active_enter_us = _safe_int(raw_map.get("ActiveEnterTimestampUSec", "0"), 0)

    uptime_sec = 0
    if active_enter_mono_us > 0 and system_uptime_s > 0:
        uptime_sec = max(0, _safe_int(system_uptime_s - (active_enter_mono_us / 1_000_000.0), 0))

    active_enter_epoch = 0
    active_enter_iso = ""
    if active_enter_us > 0:
        active_enter_epoch = _safe_int(active_enter_us / 1_000_000, 0)
        try:
            active_enter_iso = datetime.fromtimestamp(active_enter_epoch, tz=timezone.utc).isoformat()
        except Exception:
            active_enter_iso = ""

    out.update(
        {
            "status": active_state,
            "substatus": sub_state,
            "restarts": restarts,
            "uptime_sec": uptime_sec,
            "pid": pid,
            "active_enter_epoch": active_enter_epoch,
            "active_enter_iso": active_enter_iso,
        }
    )
    return out


def _fetch_infra_metrics() -> dict[str, Any]:
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1, load5, load15 = 0.0, 0.0, 0.0

    cores = max(1, _safe_int(os.cpu_count() or 1, 1))
    mem = _read_meminfo()
    disk_raw = shutil.disk_usage(BASE_DIR)
    disk = {
        "total": _safe_int(disk_raw.total),
        "used": _safe_int(disk_raw.used),
        "free": _safe_int(disk_raw.free),
        "used_pct": _ratio_pct(_safe_int(disk_raw.used), _safe_int(disk_raw.total) or 1),
    }

    system_uptime_s = _read_proc_uptime()
    services = [
        _service_runtime_snapshot("SmartAIToolsBot.service", system_uptime_s),
        _service_runtime_snapshot("SmartAIToolsBot-bot.service", system_uptime_s),
        _service_runtime_snapshot("SmartAIToolsBot-dashboard.service", system_uptime_s),
    ]

    return {
        "cpu": {
            "cores": cores,
            "load1": round(load1, 2),
            "load5": round(load5, 2),
            "load15": round(load15, 2),
            "load_pct": _ratio_pct(load1, cores),
        },
        "memory": mem,
        "disk": disk,
        "network": _network_metrics(),
        "services": services,
        "process_uptime_sec": max(0, _safe_int(time.time() - _SERVER_START_TS, 0)),
        "system_uptime_sec": max(0, _safe_int(system_uptime_s, 0)),
    }


def _service_reachable(host: str, port: int, timeout_s: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except Exception:
        return False


def _bot_api_status() -> dict[str, Any]:
    base_url = str(os.getenv("TELEGRAM_BOT_API_BASE_URL", "http://127.0.0.1:8081") or "").strip()
    local_mode = str(os.getenv("TELEGRAM_BOT_API_LOCAL_MODE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    parsed = urlparse(base_url)
    host = parsed.hostname or "127.0.0.1"
    port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
    ok = _service_reachable(host, port)
    if ok:
        return {"status": "ok", "label": "Bot API Online", "detail": f"{host}:{port}"}
    if local_mode:
        return {"status": "err", "label": "Bot API Offline", "detail": f"unreachable {host}:{port}"}
    return {"status": "warn", "label": "Bot API Unknown", "detail": f"not reachable {host}:{port}"}


def _db_status(db_stats: dict[str, Any]) -> dict[str, Any]:
    if db_stats.get("ok"):
        counts = db_stats.get("counts", {}) or {}
        detail = f"users={counts.get('users', 0)} books={counts.get('books', 0)}"
        return {"status": "ok", "label": "PostgreSQL Healthy", "detail": detail}
    return {"status": "err", "label": "PostgreSQL Error", "detail": str(db_stats.get("error") or "unknown error")}


def _es_status() -> dict[str, Any]:
    es_url = str(os.getenv("ES_URL", "") or "").strip()
    if not es_url:
        return {"status": "warn", "label": "ES Disabled", "detail": "ES_URL is not set"}

    es_index = str(os.getenv("ES_INDEX", "books") or "books").strip() or "books"
    kwargs: dict[str, Any] = {}
    es_ca = str(os.getenv("ES_CA_CERT", "") or "").strip()
    es_user = str(os.getenv("ES_USER", "") or "").strip()
    es_pass = str(os.getenv("ES_PASS", "") or "").strip()
    if es_ca:
        kwargs["ca_certs"] = es_ca
    if es_user and es_pass:
        kwargs["basic_auth"] = (es_user, es_pass)

    try:
        es = Elasticsearch(es_url, **kwargs)
        es.info()
        health = es.cluster.health(index=es_index)
        status = str((health or {}).get("status") or "unknown").lower()
        count = _safe_int(es.count(index=es_index).get("count", 0), 0)
        sev = "ok" if status == "green" else "warn" if status == "yellow" else "err"
        return {
            "status": sev,
            "label": f"ES {status.title()}",
            "detail": f"{es_index} indexed={_fmt_compact(count)}",
        }
    except Exception as e:
        return {"status": "err", "label": "ES Error", "detail": str(e)[:140]}


LOG_RE = re.compile(
    r"^(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+\|\s+(?P<level>[A-Z]+)\s+\|\s+(?P<svc>[^|]+)\s+\|\s+(?P<msg>.*)$"
)


def _tail_lines(path: Path, limit: int) -> list[str]:
    if limit <= 0 or not path.exists():
        return []
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        chunk = 4096
        data = b""
        while size > 0 and data.count(b"\n") <= limit * 3:
            step = min(chunk, size)
            size -= step
            f.seek(size)
            data = f.read(step) + data
            if size == 0:
                break
    lines = data.decode("utf-8", errors="replace").splitlines()
    return lines[-limit * 3 :]


def _event_status_from_level(level: str) -> str:
    lv = (level or "").upper()
    if lv in {"ERROR", "CRITICAL"}:
        return "err"
    if lv in {"WARNING", "WARN"}:
        return "warn"
    return "ok"


def _detect_platform_text(text: str) -> str:
    low = str(text or "").lower()
    if any(x in low for x in ("youtube", "youtu.be", "yt")):
        return "youtube"
    if "instagram" in low or "insta" in low:
        return "instagram"
    if "tiktok" in low:
        return "tiktok"
    if any(x in low for x in ("facebook", "fb.watch")):
        return "facebook"
    if any(x in low for x in ("twitter", "x.com")):
        return "x"
    return "generic"


def _detect_fail_reason_text(text: str) -> str:
    low = str(text or "").lower()
    if any(x in low for x in ("timeout", "timed out", "took too long")):
        return "timeout"
    if any(x in low for x in ("invalid", "not found", "unsupported")):
        return "invalid"
    if any(x in low for x in ("too large", "file-too-large")):
        return "too_large"
    if any(x in low for x in ("rate limit", "retry after", "too many requests")):
        return "rate_limit"
    if any(x in low for x in ("network", "connection", "tls", "dns", "socket")):
        return "network"
    return "other"


def _collect_recent_events(
    limit: int = 8,
    since_dt: datetime | None = None,
    until_dt: datetime | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in reversed(_tail_lines(LOG_MAIN, max(limit, 16))):
        m = LOG_RE.match(line.strip())
        if not m:
            continue

        dt_raw = m.group("dt")
        level = m.group("level")
        svc = m.group("svc").strip()
        msg = m.group("msg").strip()
        dt_obj: datetime | None = None

        try:
            dt_obj = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S")
            if since_dt and dt_obj < since_dt:
                continue
            if until_dt and dt_obj >= until_dt:
                continue
            t = dt_obj.strftime("%H:%M:%S")
        except Exception:
            t = dt_raw[-8:]

        out.append(
            {
                "time": t,
                "service": svc[:32] or "app",
                "message": msg[:120] if msg else "(no message)",
                "status": _event_status_from_level(level),
            }
        )
        if len(out) >= limit:
            break

    if out:
        return out

    now = datetime.now().strftime("%H:%M:%S")
    return [{"time": now, "service": "dashboard", "message": "No recent logs found.", "status": "ok"}]


def _collect_log_signals(limit: int = 300, since_dt: datetime | None = None, until_dt: datetime | None = None) -> dict[str, Any]:
    signals: dict[str, Any] = {
        "errors": 0,
        "warns": 0,
        "video_errors": 0,
        "video_warns": 0,
        "recent_video_issues": [],
        "platform_warn_err": {},
        "reason_err": {},
    }

    for line in reversed(_tail_lines(LOG_MAIN, max(limit, 120))):
        m = LOG_RE.match(line.strip())
        if not m:
            continue

        dt_raw = m.group("dt")
        try:
            dt_obj = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt_obj = None

        if since_dt and dt_obj and dt_obj < since_dt:
            continue
        if until_dt and dt_obj and dt_obj >= until_dt:
            continue

        level = m.group("level")
        svc = m.group("svc").strip()
        msg = m.group("msg").strip()
        status = _event_status_from_level(level)

        if status == "err":
            signals["errors"] = _safe_int(signals.get("errors", 0)) + 1
        elif status == "warn":
            signals["warns"] = _safe_int(signals.get("warns", 0)) + 1

        combined = f"{svc} {msg}"
        is_video_related = any(x in combined.lower() for x in ("video", "downloader", "youtube", "instagram", "tiktok", "vdl"))

        if is_video_related and status in {"err", "warn"}:
            bucket = "video_errors" if status == "err" else "video_warns"
            signals[bucket] = _safe_int(signals.get(bucket, 0)) + 1

            platform = _detect_platform_text(combined)
            platform_map = signals.get("platform_warn_err")
            if isinstance(platform_map, dict):
                platform_map[platform] = _safe_int(platform_map.get(platform, 0)) + 1

            reason = _detect_fail_reason_text(msg)
            if status == "err":
                reason_map = signals.get("reason_err")
                if isinstance(reason_map, dict):
                    reason_map[reason] = _safe_int(reason_map.get(reason, 0)) + 1

            recent = signals.get("recent_video_issues")
            if isinstance(recent, list) and len(recent) < 4:
                t = dt_obj.strftime("%H:%M:%S") if dt_obj else dt_raw[-8:]
                recent.append(
                    {
                        "time": t,
                        "service": svc[:28] or "video",
                        "message": msg[:96] if msg else "(no message)",
                        "status": status,
                    }
                )

    return signals


def _normalize_status_map(raw: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, value in (raw or {}).items():
        out[str(key or "unknown").strip().lower() or "unknown"] = max(0, _safe_int(value))
    return out


def _segments_from_counts(
    counts: dict[str, int],
    order: list[str],
    labels: dict[str, str],
    colors: dict[str, str],
) -> list[dict[str, Any]]:
    total = sum(max(0, _safe_int(counts.get(key, 0))) for key in order)
    segments: list[dict[str, Any]] = []
    for key in order:
        value = max(0, _safe_int(counts.get(key, 0)))
        pct = round((value / total) * 100.0, 1) if total > 0 else 0.0
        segments.append(
            {
                "key": key,
                "label": labels.get(key, key.title()),
                "value": value,
                "pct": pct,
                "color": colors.get(key, "#7f8c9f"),
            }
        )
    return segments


def _feature_mix_from_counters(
    counters: dict[str, int],
    searches_total: int,
    downloads_total: int,
    feature_usage: list[dict[str, Any]] | None = None,
    range_key: str = "week",
) -> list[dict[str, Any]]:
    del counters, range_key
    usage_map: dict[str, int] = {}
    for row in feature_usage or []:
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        usage_map[name] = usage_map.get(name, 0) + max(0, _safe_int(row.get("count", 0), 0))

    book_search = max(
        0,
        _safe_int(usage_map.get("Book Search", 0), 0)
        + _safe_int(usage_map.get("Book Downloads", 0), 0)
        + _safe_int(usage_map.get("Result Clicks", 0), 0),
    )
    video_dl = max(0, _safe_int(usage_map.get("Downloader Users", 0), 0))
    other = max(
        0,
        _safe_int(usage_map.get("Book Requests", 0), 0)
        + _safe_int(usage_map.get("Upload Requests", 0), 0)
        + _safe_int(usage_map.get("Favorites Added", 0), 0)
        + _safe_int(usage_map.get("Book Reactions", 0), 0),
    )

    if book_search + video_dl + other <= 0:
        book_search = max(0, _safe_int(searches_total))
        video_dl = max(0, _safe_int(downloads_total))
        other = 0

    raw = [
        {"name": "Book Search", "value": max(0, book_search), "color": "#2ca58d"},
        {"name": "Video Downloader", "value": max(0, video_dl), "color": "#3f88c5"},
        {"name": "Other", "value": max(0, other), "color": "#7f8c9f"},
    ]

    total = sum(item["value"] for item in raw)
    if total <= 0:
        return [{"name": item["name"], "value": 0, "color": item["color"]} for item in raw]

    mix: list[dict[str, Any]] = []
    for item in raw:
        pct = round((item["value"] / total) * 100.0)
        mix.append({"name": item["name"], "value": int(pct), "color": item["color"]})

    diff = 100 - sum(int(m["value"]) for m in mix)
    if diff != 0 and mix:
        mix[0]["value"] = int(mix[0]["value"]) + diff
    return mix


def _humanize_counter_key(key: str) -> str:
    pretty = str(key or "").replace("_", " ").strip()
    if not pretty:
        return "unknown"
    if pretty.startswith("video dl test user "):
        return ""
    return pretty.title()


def _command_load_from_counters(counters: dict[str, int]) -> list[dict[str, Any]]:
    filtered: list[tuple[str, int]] = []
    for key, val in counters.items():
        v = _safe_int(val)
        if v <= 0:
            continue
        label = _humanize_counter_key(key)
        if not label:
            continue
        filtered.append((label, v))
    filtered.sort(key=lambda x: x[1], reverse=True)
    top = filtered[:8]
    if not top:
        return [
            {"name": "/start", "count": 0},
            {"name": "Search Books", "count": 0},
            {"name": "Video Downloader", "count": 0},
            {"name": "Other Functions", "count": 0},
        ]
    return [{"name": name, "count": count} for name, count in top]


def _counter_prefix_map(counters: dict[str, int], prefix: str) -> dict[str, int]:
    out: dict[str, int] = {}
    p = prefix.lower()
    for key, val in (counters or {}).items():
        k = str(key or "").lower()
        if not k.startswith(p):
            continue
        item_key = k[len(p):].strip("_") or "unknown"
        out[item_key] = out.get(item_key, 0) + max(0, _safe_int(val))
    return out


def _build_downloader_quality(counters: dict[str, int], log_signals: dict[str, Any]) -> dict[str, Any]:
    success_by_platform = _counter_prefix_map(counters, "video_dl_success_platform_")
    fail_by_platform = _counter_prefix_map(counters, "video_dl_fail_platform_")
    fail_reason_counts = _counter_prefix_map(counters, "video_dl_fail_reason_")

    if not success_by_platform and isinstance(log_signals.get("platform_warn_err"), dict):
        for platform, cnt in (log_signals.get("platform_warn_err") or {}).items():
            fail_by_platform[str(platform)] = _safe_int(cnt)

    if not fail_reason_counts and isinstance(log_signals.get("reason_err"), dict):
        for reason, cnt in (log_signals.get("reason_err") or {}).items():
            fail_reason_counts[str(reason)] = _safe_int(cnt)

    success_total = max(
        _safe_int(counters.get("video_dl_success_total", 0)),
        sum(_safe_int(v) for v in success_by_platform.values()),
    )
    fail_total = max(
        _safe_int(counters.get("video_dl_fail_total", 0)),
        sum(_safe_int(v) for v in fail_by_platform.values()),
        sum(_safe_int(v) for v in fail_reason_counts.values()),
    )
    jobs_total = max(_safe_int(counters.get("video_dl_jobs_total", 0)), success_total + fail_total)
    duration_total_ms = _safe_int(counters.get("video_dl_duration_total_ms", 0))
    avg_processing_sec = round((duration_total_ms / jobs_total) / 1000.0, 2) if jobs_total > 0 else 0.0

    platforms = sorted(set(list(success_by_platform.keys()) + list(fail_by_platform.keys())))
    if not platforms:
        platforms = ["generic"]

    platform_rows: list[dict[str, Any]] = []
    for platform in platforms:
        succ = _safe_int(success_by_platform.get(platform, 0))
        fail = _safe_int(fail_by_platform.get(platform, 0))
        total = succ + fail
        platform_rows.append(
            {
                "platform": platform,
                "success": succ,
                "fail": fail,
                "total": total,
                "success_rate": _ratio_pct(succ, total or 1),
            }
        )
    platform_rows.sort(key=lambda x: x.get("total", 0), reverse=True)

    fail_reason_rows = [
        {"reason": reason, "count": _safe_int(cnt)}
        for reason, cnt in fail_reason_counts.items()
        if _safe_int(cnt) > 0
    ]
    fail_reason_rows.sort(key=lambda x: x["count"], reverse=True)

    return {
        "success_total": success_total,
        "fail_total": fail_total,
        "attempts_total": jobs_total,
        "success_rate": _ratio_pct(success_total, jobs_total or 1),
        "avg_processing_sec": avg_processing_sec,
        "platform": platform_rows[:8],
        "fail_reasons": fail_reason_rows[:8],
        "recent_video_errors": _safe_int(log_signals.get("video_errors", 0)),
        "recent_video_warns": _safe_int(log_signals.get("video_warns", 0)),
        "recent_issues": list(log_signals.get("recent_video_issues", []))[:4],
    }


def build_dashboard_payload(range_key: str = "week", days_override: int | None = None) -> dict[str, Any]:
    range_info = _resolve_time_range(range_key=range_key, days_override=days_override)
    start_day = range_info["start_day"]
    end_day = range_info["end_day"]
    range_days = max(1, _safe_int(range_info.get("days", 1), 1))
    range_key_norm = str(range_info.get("key", "week") or "week").lower()
    prev_start_day, prev_end_day = _previous_window(start_day, end_day)
    start_dt, end_dt = _day_bounds(start_day, end_day)

    db_stats = get_db_stats()
    db_ok = bool(db_stats.get("ok"))
    db_counts = db_stats.get("counts", {}) if db_ok else {}

    counters = _safe_call("get_counters", get_counters, {}) if db_ok else {}
    user_status = _safe_call("get_user_status_counts", get_user_status_counts, {"total": 0, "blocked": 0, "allowed": 0}) if db_ok else {"total": 0, "blocked": 0, "allowed": 0}
    request_status = _fetch_status_counts_in_range("book_requests", start_dt, end_dt) if db_ok else {}
    upload_status = _fetch_status_counts_in_range("upload_requests", start_dt, end_dt) if db_ok else {}
    reactions = _safe_call("get_reaction_totals", get_reaction_totals, {"like": 0, "dislike": 0, "berry": 0, "whale": 0}) if db_ok else {"like": 0, "dislike": 0, "berry": 0, "whale": 0}
    book_totals = _safe_call("get_book_totals", get_book_totals, {"total": 0, "indexed": 0, "downloads": 0, "searches": 0}) if db_ok else {"total": 0, "indexed": 0, "downloads": 0, "searches": 0}
    audio_stats = _safe_call(
        "get_audio_book_stats",
        get_audio_book_stats,
        {
            "total_audiobooks": 0,
            "books_with_audiobooks": 0,
            "total_parts": 0,
            "total_downloads": 0,
            "total_searches": 0,
            "total_duration_seconds": 0,
        },
    ) if db_ok else {
        "total_audiobooks": 0,
        "books_with_audiobooks": 0,
        "total_parts": 0,
        "total_downloads": 0,
        "total_searches": 0,
        "total_duration_seconds": 0,
    }
    storage_stats = _safe_call(
        "get_storage_stats",
        get_storage_stats,
        {
            "total_files": 0,
            "total_size": 0,
            "book_count": 0,
            "total_book_size": 0,
            "audio_count": 0,
            "total_audio_size": 0,
            "avg_book_size": 0,
            "avg_audio_size": 0,
        },
    ) if db_ok else {
        "total_files": 0,
        "total_size": 0,
        "book_count": 0,
        "total_book_size": 0,
        "audio_count": 0,
        "total_audio_size": 0,
        "avg_book_size": 0,
        "avg_audio_size": 0,
    }
    favorites_total = _safe_call("get_favorites_total", get_favorites_total, 0) if db_ok else 0

    retention = _fetch_retention(start_day, end_day) if db_ok else {
        "dau": 0,
        "wau": 0,
        "mau": 0,
        "active_in_range": 0,
        "active_prev_in_range": 0,
        "stickiness_pct": 0.0,
        "dau_change": "0.0%",
        "wau_change": "0.0%",
        "mau_change": "0.0%",
        "active_change": "0.0%",
        "window_days": {"range": 0, "wau": 0, "mau": 0},
    }
    funnel = _fetch_funnel(start_day, end_day) if db_ok else {"new_users": 0, "active_users": 0, "search_users": 0, "download_users": 0, "steps": []}
    search_quality = _fetch_search_quality(start_day, end_day) if db_ok else {
        "searches_total": 0,
        "downloads_total": 0,
        "book_searches_total": 0,
        "book_downloads_total": 0,
        "conversion_pct": 0.0,
        "request_queries_total": 0,
        "zero_result_total": 0,
        "zero_result_rate_pct": 0.0,
        "top_queries": [],
        "zero_result_queries": [],
    }
    search_quality_prev = _fetch_search_quality(prev_start_day, prev_end_day) if db_ok else {
        "searches_total": 0,
        "downloads_total": 0,
        "book_searches_total": 0,
        "book_downloads_total": 0,
    }
    user_changes = _fetch_user_join_leave_counts(start_day, end_day) if db_ok else {"joined": 0, "left": 0}
    user_changes_prev = _fetch_user_join_leave_counts(prev_start_day, prev_end_day) if db_ok else {"joined": 0, "left": 0}
    catalog_additions = _fetch_catalog_additions(start_day, end_day) if db_ok else {"books": 0, "audios": 0}
    catalog_additions_prev = _fetch_catalog_additions(prev_start_day, prev_end_day) if db_ok else {"books": 0, "audios": 0}
    queue_sla = _fetch_queue_sla(start_day, end_day) if db_ok else {
        "pending_upload_count": 0,
        "oldest_pending_age_sec": 0,
        "avg_resolve_sec": 0,
        "avg_accept_sec": 0,
        "avg_reject_sec": 0,
        "avg_book_request_resolve_sec": 0,
    }
    languages = _fetch_language_usage(start_day, end_day) if db_ok else []
    feature_usage = _fetch_feature_usage(start_day, end_day) if db_ok else []
    peak_hours = _fetch_peak_hours(start_day, end_day) if db_ok else [{"hour": h, "count": 0} for h in range(24)]

    books_total = _safe_int(book_totals.get("total", db_counts.get("books", 0)))
    books_indexed = _safe_int(book_totals.get("indexed", db_counts.get("books_indexed", 0)))
    books_unindexed = max(0, books_total - books_indexed)
    books_index_ratio = _ratio_pct(books_indexed, books_total)

    audio_total = _safe_int(audio_stats.get("total_audiobooks", 0))
    audio_books_with_books = _safe_int(audio_stats.get("books_with_audiobooks", 0))
    audio_parts_total = _safe_int(audio_stats.get("total_parts", 0))
    audio_duration_s = _safe_int(audio_stats.get("total_duration_seconds", 0))

    request_map = _normalize_status_map(request_status)
    upload_map = _normalize_status_map(upload_status)

    known_book_states = {"open", "seen", "done", "no", "cancelled"}
    book_other = sum(v for k, v in request_map.items() if k not in known_book_states)
    book_counts = {
        "open": _safe_int(request_map.get("open", 0)),
        "seen": _safe_int(request_map.get("seen", 0)),
        "done": _safe_int(request_map.get("done", 0)),
        "no": _safe_int(request_map.get("no", 0)),
        "cancelled": _safe_int(request_map.get("cancelled", 0)),
        "other": _safe_int(book_other),
    }
    book_total_requests = sum(book_counts.values())

    known_upload_states = {"open", "accept", "reject", "done", "cancelled", "fulfilled", "complete"}
    upload_other = sum(v for k, v in upload_map.items() if k not in known_upload_states)
    upload_counts = {
        "open": _safe_int(upload_map.get("open", 0)),
        "accept": _safe_int(upload_map.get("accept", 0)),
        "reject": _safe_int(upload_map.get("reject", 0)),
        "done": _safe_int(upload_map.get("done", 0)),
        "cancelled": _safe_int(upload_map.get("cancelled", 0)),
        "other": _safe_int(upload_other),
    }
    terminal_upload_states = {"done", "accept", "reject", "cancelled", "no", "fulfilled", "complete"}
    queue_pending = sum(v for k, v in upload_map.items() if k not in terminal_upload_states)
    upload_total_requests = sum(upload_counts.values())

    start_since_logs, end_since_logs = start_dt, end_dt
    log_signals = _collect_log_signals(limit=320, since_dt=start_since_logs, until_dt=end_since_logs)
    downloader_quality = _build_downloader_quality(counters, log_signals)

    trend = _fetch_daily_traffic_series(start_day, end_day, max_points=60) if db_ok else {
        "labels": ["-", "-", "-", "-", "-", "-", "-"],
        "points": [0, 0, 0, 0, 0, 0, 0],
        "dau_points": [0, 0, 0, 0, 0, 0, 0],
    }
    catalog_growth = _fetch_catalog_growth_series(start_day, end_day, max_points=60) if db_ok else {
        "labels": [],
        "books_new": [],
        "audio_new": [],
        "unindexed_new": [],
    }

    users_total = _safe_int(user_status.get("total", 0))
    users_blocked = _safe_int(user_status.get("blocked", 0))
    users_allowed = _safe_int(user_status.get("allowed", 0))

    searches_total_range = _safe_int(search_quality.get("searches_total", 0))
    downloads_total_range = _safe_int(search_quality.get("downloads_total", 0))
    searches_total_prev = _safe_int(search_quality_prev.get("searches_total", 0))
    downloads_total_prev = _safe_int(search_quality_prev.get("downloads_total", 0))
    book_searches_range = _safe_int(search_quality.get("book_searches_total", searches_total_range))
    book_downloads_range = _safe_int(search_quality.get("book_downloads_total", downloads_total_range))
    book_searches_prev = _safe_int(search_quality_prev.get("book_searches_total", searches_total_prev))
    book_downloads_prev = _safe_int(search_quality_prev.get("book_downloads_total", downloads_total_prev))
    joined_users_range = _safe_int(user_changes.get("joined", 0))
    left_users_range = _safe_int(user_changes.get("left", 0))
    joined_users_prev = _safe_int(user_changes_prev.get("joined", 0))
    left_users_prev = _safe_int(user_changes_prev.get("left", 0))
    added_books_range = _safe_int(catalog_additions.get("books", 0))
    added_audios_range = _safe_int(catalog_additions.get("audios", 0))
    added_books_prev = _safe_int(catalog_additions_prev.get("books", 0))
    added_audios_prev = _safe_int(catalog_additions_prev.get("audios", 0))
    active_users_range = _safe_int(funnel.get("active_users", 0))
    active_users_prev = _safe_int(retention.get("active_prev_in_range", 0))

    current_book_reaction_total = sum(_safe_int(v) for v in reactions.values())

    # In all-time mode, "new/added" metrics should represent lifetime totals.
    if range_key_norm == "all":
        added_books_range = books_total
        added_audios_range = audio_total
        added_books_prev = 0
        added_audios_prev = 0

    def _kpi_change(now_val: int, prev_val: int) -> str:
        if range_key_norm == "all":
            return "lifetime total"
        return f"{_pct_delta(now_val, prev_val)} vs previous {range_days}d"

    lifetime_book_reactions = {
        "like": max(_safe_int(counters.get("reaction_like", 0)), _safe_int(reactions.get("like", 0))),
        "dislike": max(_safe_int(counters.get("reaction_dislike", 0)), _safe_int(reactions.get("dislike", 0))),
        "berry": max(_safe_int(counters.get("reaction_berry", 0)), _safe_int(reactions.get("berry", 0))),
        "whale": max(_safe_int(counters.get("reaction_whale", 0)), _safe_int(reactions.get("whale", 0))),
    }
    reaction_total_current = current_book_reaction_total
    reaction_total_range = _safe_int(next((x.get("count", 0) for x in feature_usage if x.get("name") == "Book Reactions"), 0))
    favorites_added_range = _safe_int(next((x.get("count", 0) for x in feature_usage if x.get("name") == "Favorites Added"), 0))

    command_rows = [
        {
            "name": str(row.get("name", "unknown")),
            "count": _safe_int(row.get("count", 0)),
            "pct": _safe_float(row.get("pct", 0.0), 0.0),
        }
        for row in feature_usage[:8]
    ]
    if not command_rows and range_key_norm == "all":
        command_rows = _command_load_from_counters(counters)

    reliability = {
        "log_errors": _safe_int(log_signals.get("errors", 0)),
        "log_warns": _safe_int(log_signals.get("warns", 0)),
        "video_errors": _safe_int(log_signals.get("video_errors", 0)),
        "video_warns": _safe_int(log_signals.get("video_warns", 0)),
    }

    services = {
        "bot_api": _bot_api_status(),
        "db": _db_status(db_stats),
        "es": _es_status(),
    }
    infra_data = _fetch_infra_metrics()
    infra_services = infra_data.get("services", []) if isinstance(infra_data, dict) else []
    bot_runtime = {"last_restart_epoch": 0, "last_restart_iso": "", "uptime_sec": 0, "status": "unknown", "pid": 0}
    if isinstance(infra_services, list):
        for svc in infra_services:
            if not isinstance(svc, dict):
                continue
            unit = str(svc.get("unit", ""))
            if unit.endswith("SmartAIToolsBot-bot.service"):
                bot_runtime = {
                    "last_restart_epoch": _safe_int(svc.get("active_enter_epoch", 0), 0),
                    "last_restart_iso": str(svc.get("active_enter_iso", "") or ""),
                    "uptime_sec": _safe_int(svc.get("uptime_sec", 0), 0),
                    "status": str(svc.get("status", "unknown") or "unknown"),
                    "pid": _safe_int(svc.get("pid", 0), 0),
                }
                break
    if _safe_int(bot_runtime.get("last_restart_epoch", 0), 0) <= 0 and _safe_int(bot_runtime.get("uptime_sec", 0), 0) > 0:
        guessed_epoch = max(0, _safe_int(time.time(), 0) - _safe_int(bot_runtime.get("uptime_sec", 0), 0))
        bot_runtime["last_restart_epoch"] = guessed_epoch
        if not str(bot_runtime.get("last_restart_iso", "") or ""):
            try:
                bot_runtime["last_restart_iso"] = datetime.fromtimestamp(guessed_epoch, tz=timezone.utc).isoformat()
            except Exception:
                bot_runtime["last_restart_iso"] = ""

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "range": {
            "key": range_info.get("key", "week"),
            "label": range_info.get("label", "This week"),
            "start_date": str(start_day),
            "end_date": str(end_day),
            "days": range_days,
        },
        "kpis": {
            "users_current": {
                "value": users_total,
                "change": f"{users_allowed} allowed, {users_blocked} blocked",
                "scope": "lifetime",
            },
            "users_new": {
                "value": joined_users_range,
                "change": _kpi_change(joined_users_range, joined_users_prev),
                "scope": "range",
            },
            "users_left": {
                "value": left_users_range,
                "change": _kpi_change(left_users_range, left_users_prev),
                "scope": "range",
            },
            "books_total": {
                "value": books_total,
                "change": f"{books_unindexed} not indexed",
                "scope": "lifetime",
            },
            "books_new": {
                "value": added_books_range,
                "change": _kpi_change(added_books_range, added_books_prev),
                "scope": "range",
            },
            "book_searches": {
                "value": book_searches_range,
                "change": _kpi_change(book_searches_range, book_searches_prev),
                "scope": "range",
            },
            "book_downloads": {
                "value": book_downloads_range,
                "change": _kpi_change(book_downloads_range, book_downloads_prev),
                "scope": "range",
            },
            "searches": {
                "value": searches_total_range,
                "change": _kpi_change(searches_total_range, searches_total_prev),
                "scope": "range",
            },
            "downloads_total": {
                "value": downloads_total_range,
                "change": _kpi_change(downloads_total_range, downloads_total_prev),
                "scope": "range",
            },
            "audios_total": {
                "value": audio_total,
                "change": f"{audio_parts_total} audio parts",
                "scope": "lifetime",
            },
            "audios_new": {
                "value": added_audios_range,
                "change": _kpi_change(added_audios_range, added_audios_prev),
                "scope": "range",
            },
        },
        "trend": trend,
        "catalog_growth": catalog_growth,
        "mix": _feature_mix_from_counters(
            counters,
            searches_total_range,
            downloads_total_range,
            feature_usage=feature_usage,
            range_key=range_key_norm,
        ),
        "retention": retention,
        "funnel": funnel,
        "search_quality": search_quality,
        "queue_sla": queue_sla,
        "events": _collect_recent_events(limit=8, since_dt=start_since_logs, until_dt=end_since_logs),
        "commands": command_rows,
        "feature_usage": feature_usage,
        "services": services,
        "bot_runtime": bot_runtime,
        "catalog": {
            "books_total": books_total,
            "books_indexed": books_indexed,
            "books_unindexed": books_unindexed,
            "books_index_ratio": books_index_ratio,
            "books_downloads_total": _safe_int(book_totals.get("downloads", 0)),
            "books_searches_total": _safe_int(book_totals.get("searches", 0)),
            "audio_books_total": audio_total,
            "audio_books_with_source_books": audio_books_with_books,
            "audio_parts_total": audio_parts_total,
            "audio_duration_seconds": audio_duration_s,
            "audio_searches_total": _safe_int(audio_stats.get("total_searches", 0)),
            "audio_downloads_total": _safe_int(audio_stats.get("total_downloads", 0)),
        },
        "users": {
            "total": users_total,
            "blocked": users_blocked,
            "allowed": users_allowed,
            "active_ratio": _ratio_pct(users_allowed, users_total or 1),
            "new_in_range": _safe_int(funnel.get("new_users", 0)),
            "active_in_range": _safe_int(funnel.get("active_users", 0)),
        },
        "requests": {
            "book": {
                "counts": book_counts,
                "total": book_total_requests,
                "segments": _segments_from_counts(
                    counts=book_counts,
                    order=["open", "seen", "done", "no", "cancelled", "other"],
                    labels={
                        "open": "Open",
                        "seen": "Seen",
                        "done": "Done",
                        "no": "No",
                        "cancelled": "Cancelled",
                        "other": "Other",
                    },
                    colors={
                        "open": "#3f88c5",
                        "seen": "#9b8de5",
                        "done": "#2ca58d",
                        "no": "#ef6f6c",
                        "cancelled": "#f4b942",
                        "other": "#7f8c9f",
                    },
                ),
            },
            "upload": {
                "counts": upload_counts,
                "total": upload_total_requests,
                "pending": _safe_int(queue_pending),
                "segments": _segments_from_counts(
                    counts=upload_counts,
                    order=["open", "accept", "reject", "done", "cancelled", "other"],
                    labels={
                        "open": "Open",
                        "accept": "Accepted",
                        "reject": "Rejected",
                        "done": "Done",
                        "cancelled": "Cancelled",
                        "other": "Other",
                    },
                    colors={
                        "open": "#3f88c5",
                        "accept": "#2ca58d",
                        "reject": "#ef6f6c",
                        "done": "#1f8f7d",
                        "cancelled": "#f4b942",
                        "other": "#7f8c9f",
                    },
                ),
            },
            "queue_pending": _safe_int(queue_pending),
        },
        "storage": {
            "total_files": _safe_int(storage_stats.get("total_files", 0)),
            "total_size": _safe_int(storage_stats.get("total_size", 0)),
            "book_count": _safe_int(storage_stats.get("book_count", 0)),
            "total_book_size": _safe_int(storage_stats.get("total_book_size", 0)),
            "audio_count": _safe_int(storage_stats.get("audio_count", 0)),
            "total_audio_size": _safe_int(storage_stats.get("total_audio_size", 0)),
            "avg_book_size": _safe_int(storage_stats.get("avg_book_size", 0)),
            "avg_audio_size": _safe_int(storage_stats.get("avg_audio_size", 0)),
            "book_size_ratio": _ratio_pct(
                _safe_int(storage_stats.get("total_book_size", 0)),
                _safe_int(storage_stats.get("total_size", 0)) or 1,
            ),
        },
        "engagement": {
            "favorites_total": _safe_int(favorites_total),
            "favorites_added_total": favorites_added_range,
            "favorites_removed_total": _safe_int(counters.get("favorite_removed", 0)),
            "book_reactions_current": reactions,
            "book_reactions_lifetime": lifetime_book_reactions,
            "reaction_total_current": reaction_total_range if reaction_total_range > 0 else reaction_total_current,
            "reaction_total_lifetime": sum(lifetime_book_reactions.values()),
            "favorites_per_active_user": round(_safe_float(favorites_total) / max(1, active_users_range), 2),
            "reactions_per_active_user": round(_safe_float(reaction_total_current) / max(1, active_users_range), 2),
        },
        "downloader": downloader_quality,
        "audience": {
            "languages": languages,
            "peak_hours": peak_hours,
            "geo_available": False,
            "geo_note": "Geo analytics are not tracked yet. Language split is available.",
        },
        "infra": infra_data,
        "reliability": reliability,
        "summary": {
            "users_total": users_total,
            "users_blocked": users_blocked,
            "users_allowed": users_allowed,
            "books_total": books_total,
            "books_indexed": books_indexed,
            "reactions_current_total": reaction_total_current,
            "queue_pending": _safe_int(queue_pending),
        },
    }

    return payload


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(UI_DIR), **kwargs)

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json({"ok": True, "time": datetime.now(timezone.utc).isoformat()})
            return

        if parsed.path == "/api/dashboard":
            qs = parse_qs(parsed.query)
            range_key = str((qs.get("range") or ["week"])[0] or "week")
            days_override = _safe_int((qs.get("days") or ["0"])[0], 0)
            if days_override <= 0:
                days_override = None
            try:
                payload = build_dashboard_payload(range_key=range_key, days_override=days_override)
                self._send_json(payload)
            except Exception as e:
                logger.exception("dashboard api failed")
                self._send_json({"ok": False, "error": str(e)}, status=500)
            return

        super().do_GET()

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pdf va audio kitoblar local dashboard server")
    parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "127.0.0.1"), help="Bind host")
    parser.add_argument(
        "--port",
        type=int,
        default=_safe_int(os.getenv("DASHBOARD_PORT", "8090"), 8090),
        help="Bind port",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not UI_DIR.exists():
        raise RuntimeError(f"dashboard ui directory not found: {UI_DIR}")

    try:
        get_db_stats()
    except Exception as e:
        logger.warning("DB warm-up failed: %s", e)

    server = ThreadingHTTPServer((args.host, int(args.port)), DashboardHandler)
    logger.info("Dashboard server listening at http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down dashboard server")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
