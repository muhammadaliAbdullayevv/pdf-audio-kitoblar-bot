from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from psycopg2.extras import RealDictCursor

from config import (
    WHITE_LABEL_DEFAULT_DAILY_SEARCH_LIMIT,
    WHITE_LABEL_DEFAULT_DAILY_SEND_LIMIT,
    WHITE_LABEL_DEFAULT_PER_MINUTE_SEND_LIMIT,
)
from db import db_conn
from . import (
    WL_CACHE_STATUS_FAILED,
    WL_CACHE_STATUS_INVALID,
    WL_CACHE_STATUS_VALID,
    WL_PLAN_MANUAL,
    WL_PLAN_TRIAL,
    WL_REQUEST_STATUS_ACCEPTED,
    WL_REQUEST_STATUS_PENDING,
    WL_REQUEST_STATUS_REJECTED,
    WL_SEED_STATUS_CACHED,
    WL_SEED_STATUS_EXPIRED,
    WL_SEED_STATUS_FAILED,
    WL_SEED_STATUS_PENDING,
    WL_SEED_STATUS_SENT_TO_CACHE,
    WL_SUBSCRIPTION_TRIALING,
    WL_STATUS_ACTIVE,
    WL_STATUS_ERROR,
    WL_STATUS_SUSPENDED,
)


def _now_utc() -> datetime:
    return datetime.utcnow()


def _normalize_identifier(raw: str | None) -> str:
    text = str(raw or "").strip()
    return text[1:] if text.startswith("@") else text


def _normalize_bot_status(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if text in {WL_STATUS_ACTIVE, WL_STATUS_SUSPENDED, WL_STATUS_ERROR}:
        return text
    return WL_STATUS_SUSPENDED


def _normalize_request_status(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if text in {WL_REQUEST_STATUS_PENDING, WL_REQUEST_STATUS_ACCEPTED, WL_REQUEST_STATUS_REJECTED, "CANCELLED"}:
        return text
    return WL_REQUEST_STATUS_PENDING


def _normalize_cache_status(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if text in {WL_CACHE_STATUS_VALID, WL_CACHE_STATUS_INVALID, WL_CACHE_STATUS_FAILED}:
        return text
    return WL_CACHE_STATUS_VALID


def _normalize_seed_status(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if text in {WL_SEED_STATUS_PENDING, WL_SEED_STATUS_SENT_TO_CACHE, WL_SEED_STATUS_CACHED, WL_SEED_STATUS_FAILED, WL_SEED_STATUS_EXPIRED}:
        return text
    return WL_SEED_STATUS_PENDING


def get_connected_bot_by_id(connected_bot_id: str) -> dict | None:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (clean_id,))
            return cur.fetchone()


def get_connected_bot_by_cache_channel_id(cache_channel_id: int) -> dict | None:
    if not cache_channel_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bots WHERE cache_channel_id=%s LIMIT 1", (int(cache_channel_id),))
            return cur.fetchone()


def get_connected_bot_by_identifier(identifier: str) -> dict | None:
    clean = _normalize_identifier(identifier)
    if not clean:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (clean,))
            row = cur.fetchone()
            if row:
                return row
            cur.execute(
                "SELECT * FROM connected_bots WHERE LOWER(bot_username)=LOWER(%s) LIMIT 1",
                (clean,),
            )
            row = cur.fetchone()
            if row:
                return row
            if clean.isdigit():
                cur.execute("SELECT * FROM connected_bots WHERE bot_telegram_id=%s LIMIT 1", (int(clean),))
                return cur.fetchone()
            return None


def list_connected_bots() -> list[dict]:
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bots ORDER BY created_at DESC, bot_username ASC")
            return cur.fetchall()


def count_connected_bots() -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM connected_bots")
            return int(cur.fetchone()[0] or 0)


def list_connected_bots_page(*, limit: int = 10, offset: int = 0) -> list[dict]:
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM connected_bots
                ORDER BY created_at DESC, bot_username ASC
                LIMIT %s OFFSET %s
                """,
                (max(1, int(limit or 10)), max(0, int(offset or 0))),
            )
            return cur.fetchall()


def upsert_connected_bot(
    *,
    owner_telegram_id: int,
    bot_telegram_id: int,
    bot_username: str,
    bot_first_name: str | None,
    bot_token_encrypted: str,
    bot_token_fingerprint: str,
    status: str | None = None,
    plan: str | None = None,
) -> dict:
    normalized_status = _normalize_bot_status(status)
    normalized_plan = str(plan or WL_PLAN_MANUAL).strip().upper() or WL_PLAN_MANUAL
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM connected_bots
                WHERE bot_telegram_id=%s OR bot_token_fingerprint=%s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (int(bot_telegram_id), str(bot_token_fingerprint)),
            )
            existing = cur.fetchone()
            if existing:
                connected_bot_id = str(existing.get("id") or "")
                effective_status = normalized_status
                if not int(existing.get("cache_channel_id") or 0) and effective_status == WL_STATUS_ACTIVE:
                    effective_status = WL_STATUS_SUSPENDED
                cur.execute(
                    """
                    UPDATE connected_bots
                    SET owner_telegram_id=%s,
                        bot_telegram_id=%s,
                        bot_username=%s,
                        bot_first_name=%s,
                        bot_token_encrypted=%s,
                        bot_token_fingerprint=%s,
                        status=%s,
                        plan=%s,
                        last_verified_at=NOW(),
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        int(owner_telegram_id),
                        int(bot_telegram_id),
                        str(bot_username or "").strip(),
                        str(bot_first_name or "").strip() or None,
                        str(bot_token_encrypted),
                        str(bot_token_fingerprint),
                        effective_status,
                        normalized_plan,
                        connected_bot_id,
                    ),
                )
                cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (connected_bot_id,))
                return cur.fetchone()

            connected_bot_id = str(uuid.uuid4())
            effective_status = normalized_status if normalized_status != WL_STATUS_ACTIVE else WL_STATUS_SUSPENDED
            cur.execute(
                """
                INSERT INTO connected_bots (
                    id,
                    owner_telegram_id,
                    bot_telegram_id,
                    bot_username,
                    bot_first_name,
                    bot_token_encrypted,
                    bot_token_fingerprint,
                    status,
                    plan,
                    daily_search_limit,
                    daily_send_limit,
                    per_minute_send_limit,
                    last_verified_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), NOW())
                """,
                (
                    connected_bot_id,
                    int(owner_telegram_id),
                    int(bot_telegram_id),
                    str(bot_username or "").strip(),
                    str(bot_first_name or "").strip() or None,
                    str(bot_token_encrypted),
                    str(bot_token_fingerprint),
                    effective_status,
                    normalized_plan,
                    int(WHITE_LABEL_DEFAULT_DAILY_SEARCH_LIMIT),
                    int(WHITE_LABEL_DEFAULT_DAILY_SEND_LIMIT),
                    int(WHITE_LABEL_DEFAULT_PER_MINUTE_SEND_LIMIT),
                ),
            )
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (connected_bot_id,))
            return cur.fetchone()


def update_connected_bot_status(connected_bot_id: str, status: str, *, last_error: str | None = None, clear_error: bool = False) -> dict | None:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            set_parts = ["status=%s", "updated_at=NOW()"]
            values: list[object] = [_normalize_bot_status(status)]
            if last_error is not None:
                set_parts.append("last_error=%s")
                values.append(str(last_error)[:2000] or None)
            elif clear_error:
                set_parts.append("last_error=NULL")
            values.append(clean_id)
            cur.execute(f"UPDATE connected_bots SET {', '.join(set_parts)} WHERE id=%s", values)
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (clean_id,))
            return cur.fetchone()


def record_connected_bot_verification(connected_bot_id: str, *, last_error: str | None = None) -> int:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE connected_bots
                SET last_verified_at=NOW(),
                    last_error=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (str(last_error)[:2000] if last_error else None, clean_id),
            )
            return cur.rowcount


def update_connected_bot_cache_channel(connected_bot_id: str, cache_channel_id: int, cache_channel_username: str | None = None) -> dict | None:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id or not cache_channel_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE connected_bots
                SET cache_channel_id=%s,
                    cache_channel_username=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (int(cache_channel_id), str(cache_channel_username or "").strip() or None, clean_id),
            )
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (clean_id,))
            return cur.fetchone()


def delete_connected_bot(connected_bot_id: str) -> int:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM connected_bots WHERE id=%s", (clean_id,))
            return cur.rowcount


def get_connected_bot_request_by_id(request_id: str) -> dict | None:
    clean_id = str(request_id or "").strip()
    if not clean_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bot_requests WHERE id=%s LIMIT 1", (clean_id,))
            return cur.fetchone()


def find_existing_connected_bot_request_or_bot(*, token_fingerprint: str, bot_telegram_id: int | None = None) -> dict | None:
    fingerprint = str(token_fingerprint or "").strip()
    bot_id = int(bot_telegram_id or 0)
    if not fingerprint and not bot_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 'request' AS source, id, status, bot_username, bot_first_name, created_at
                FROM connected_bot_requests
                WHERE status=%s
                  AND (bot_token_fingerprint=%s OR bot_telegram_id=%s)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (WL_REQUEST_STATUS_PENDING, fingerprint, bot_id),
            )
            row = cur.fetchone()
            if row:
                return row
            cur.execute(
                """
                SELECT 'connected_bot' AS source, id, status, bot_username, bot_first_name, created_at
                FROM connected_bots
                WHERE bot_token_fingerprint=%s OR bot_telegram_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (fingerprint, bot_id),
            )
            return cur.fetchone()


def create_connected_bot_request(
    *,
    requesting_user_id: int,
    requesting_username: str | None,
    requesting_first_name: str | None,
    bot_telegram_id: int,
    bot_username: str,
    bot_first_name: str | None,
    bot_token_encrypted: str,
    bot_token_fingerprint: str,
    token_masked: str | None,
) -> dict:
    request_id = str(uuid.uuid4())
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO connected_bot_requests (
                    id,
                    requesting_user_id,
                    requesting_username,
                    requesting_first_name,
                    bot_telegram_id,
                    bot_username,
                    bot_first_name,
                    bot_token_encrypted,
                    bot_token_fingerprint,
                    token_masked,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING *
                """,
                (
                    request_id,
                    int(requesting_user_id),
                    str(requesting_username or "").strip() or None,
                    str(requesting_first_name or "").strip() or None,
                    int(bot_telegram_id),
                    str(bot_username or "").strip().lstrip("@"),
                    str(bot_first_name or "").strip() or None,
                    str(bot_token_encrypted),
                    str(bot_token_fingerprint),
                    str(token_masked or "").strip() or None,
                    WL_REQUEST_STATUS_PENDING,
                ),
            )
            return cur.fetchone()


def count_connected_bot_requests(status: str = WL_REQUEST_STATUS_PENDING) -> int:
    normalized = _normalize_request_status(status)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM connected_bot_requests WHERE status=%s", (normalized,))
            return int(cur.fetchone()[0] or 0)


def list_connected_bot_requests(
    *,
    status: str = WL_REQUEST_STATUS_PENDING,
    limit: int = 10,
    offset: int = 0,
) -> list[dict]:
    normalized = _normalize_request_status(status)
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM connected_bot_requests
                WHERE status=%s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (normalized, max(1, int(limit or 10)), max(0, int(offset or 0))),
            )
            return cur.fetchall()


def accept_connected_bot_request(
    request_id: str,
    *,
    accepted_by_owner_id: int,
    cache_channel_id: int,
    cache_channel_username: str | None = None,
    trial_days: int = 3,
    daily_search_limit: int = 100,
    daily_send_limit: int = 20,
    per_minute_send_limit: int = 10,
) -> dict | None:
    clean_id = str(request_id or "").strip()
    if not clean_id:
        return None
    trial_days = max(1, int(trial_days or 3))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bot_requests WHERE id=%s FOR UPDATE", (clean_id,))
            req = cur.fetchone()
            if not req or str(req.get("status") or "").upper() != WL_REQUEST_STATUS_PENDING:
                return None
            trial_ends_at = _now_utc() + timedelta(days=trial_days)
            cur.execute(
                """
                SELECT *
                FROM connected_bots
                WHERE bot_telegram_id=%s OR bot_token_fingerprint=%s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (int(req.get("bot_telegram_id") or 0), str(req.get("bot_token_fingerprint") or "")),
            )
            existing = cur.fetchone()
            if existing:
                connected_bot_id = str(existing.get("id") or "")
                cur.execute(
                    """
                    UPDATE connected_bots
                    SET owner_telegram_id=%s,
                        requested_by_user_id=%s,
                        requested_by_username=%s,
                        requested_by_first_name=%s,
                        accepted_request_id=%s,
                        bot_telegram_id=%s,
                        bot_username=%s,
                        bot_first_name=%s,
                        bot_token_encrypted=%s,
                        bot_token_fingerprint=%s,
                        cache_channel_id=%s,
                        cache_channel_username=%s,
                        status=%s,
                        plan=%s,
                        subscription_status=%s,
                        daily_search_limit=%s,
                        daily_send_limit=%s,
                        per_minute_send_limit=%s,
                        last_verified_at=NOW(),
                        last_error=NULL,
                        trial_started_at=NOW(),
                        trial_ends_at=%s,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (
                        int(req.get("requesting_user_id") or 0),
                        int(req.get("requesting_user_id") or 0),
                        req.get("requesting_username"),
                        req.get("requesting_first_name"),
                        clean_id,
                        int(req.get("bot_telegram_id") or 0),
                        str(req.get("bot_username") or "").strip(),
                        req.get("bot_first_name"),
                        str(req.get("bot_token_encrypted") or ""),
                        str(req.get("bot_token_fingerprint") or ""),
                        int(cache_channel_id),
                        str(cache_channel_username or "").strip() or None,
                        WL_STATUS_SUSPENDED,
                        WL_PLAN_TRIAL,
                        WL_SUBSCRIPTION_TRIALING,
                        max(1, int(daily_search_limit or 100)),
                        max(1, int(daily_send_limit or 20)),
                        max(1, int(per_minute_send_limit or 10)),
                        trial_ends_at,
                        connected_bot_id,
                    ),
                )
            else:
                connected_bot_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO connected_bots (
                        id,
                        owner_telegram_id,
                        requested_by_user_id,
                        requested_by_username,
                        requested_by_first_name,
                        accepted_request_id,
                        bot_telegram_id,
                        bot_username,
                        bot_first_name,
                        bot_token_encrypted,
                        bot_token_fingerprint,
                        cache_channel_id,
                        cache_channel_username,
                        status,
                        plan,
                        subscription_status,
                        daily_search_limit,
                        daily_send_limit,
                        per_minute_send_limit,
                        last_verified_at,
                        trial_started_at,
                        trial_ends_at,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, NOW(), NOW())
                    """,
                    (
                        connected_bot_id,
                        int(req.get("requesting_user_id") or 0),
                        int(req.get("requesting_user_id") or 0),
                        req.get("requesting_username"),
                        req.get("requesting_first_name"),
                        clean_id,
                        int(req.get("bot_telegram_id") or 0),
                        str(req.get("bot_username") or "").strip(),
                        req.get("bot_first_name"),
                        str(req.get("bot_token_encrypted") or ""),
                        str(req.get("bot_token_fingerprint") or ""),
                        int(cache_channel_id),
                        str(cache_channel_username or "").strip() or None,
                        WL_STATUS_SUSPENDED,
                        WL_PLAN_TRIAL,
                        WL_SUBSCRIPTION_TRIALING,
                        max(1, int(daily_search_limit or 100)),
                        max(1, int(daily_send_limit or 20)),
                        max(1, int(per_minute_send_limit or 10)),
                        trial_ends_at,
                    ),
                )
            cur.execute(
                """
                UPDATE connected_bot_requests
                SET status=%s,
                    accepted_by_owner_id=%s,
                    accepted_at=NOW(),
                    accepted_connected_bot_id=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (WL_REQUEST_STATUS_ACCEPTED, int(accepted_by_owner_id), connected_bot_id, clean_id),
            )
            cur.execute("SELECT * FROM connected_bots WHERE id=%s LIMIT 1", (connected_bot_id,))
            return cur.fetchone()


def reject_connected_bot_request(request_id: str, *, rejected_by_owner_id: int, reason: str | None = None) -> dict | None:
    clean_id = str(request_id or "").strip()
    if not clean_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE connected_bot_requests
                SET status=%s,
                    rejection_reason=%s,
                    rejected_by_owner_id=%s,
                    rejected_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s AND status=%s
                RETURNING *
                """,
                (
                    WL_REQUEST_STATUS_REJECTED,
                    str(reason or "").strip()[:1000] or None,
                    int(rejected_by_owner_id),
                    clean_id,
                    WL_REQUEST_STATUS_PENDING,
                ),
            )
            return cur.fetchone()


def get_connected_bot_file_cache(connected_bot_id: str, book_id: str, *, only_valid: bool = False) -> dict | None:
    clean_bot_id = str(connected_bot_id or "").strip()
    clean_book_id = str(book_id or "").strip()
    if not clean_bot_id or not clean_book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if only_valid:
                cur.execute(
                    """
                    SELECT *
                    FROM connected_bot_file_cache
                    WHERE connected_bot_id=%s AND book_id=%s AND status=%s
                    LIMIT 1
                    """,
                    (clean_bot_id, clean_book_id, WL_CACHE_STATUS_VALID),
                )
            else:
                cur.execute(
                    "SELECT * FROM connected_bot_file_cache WHERE connected_bot_id=%s AND book_id=%s LIMIT 1",
                    (clean_bot_id, clean_book_id),
                )
            return cur.fetchone()


def upsert_connected_bot_file_cache(
    *,
    connected_bot_id: str,
    book_id: str,
    telegram_file_id: str,
    telegram_file_unique_id: str | None,
    cache_channel_id: int | None,
    cache_message_id: int | None,
    status: str = WL_CACHE_STATUS_VALID,
    last_error: str | None = None,
) -> dict:
    clean_bot_id = str(connected_bot_id or "").strip()
    clean_book_id = str(book_id or "").strip()
    if not clean_bot_id or not clean_book_id:
        raise ValueError("connected_bot_id and book_id are required")
    cache_id = str(uuid.uuid4())
    normalized_status = _normalize_cache_status(status)
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO connected_bot_file_cache (
                    id,
                    connected_bot_id,
                    book_id,
                    telegram_file_id,
                    telegram_file_unique_id,
                    cache_channel_id,
                    cache_message_id,
                    status,
                    failure_count,
                    last_error,
                    created_at,
                    last_used_at,
                    last_validated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, NOW(), NOW(), NOW())
                ON CONFLICT (connected_bot_id, book_id) DO UPDATE SET
                    telegram_file_id = EXCLUDED.telegram_file_id,
                    telegram_file_unique_id = EXCLUDED.telegram_file_unique_id,
                    cache_channel_id = EXCLUDED.cache_channel_id,
                    cache_message_id = EXCLUDED.cache_message_id,
                    status = EXCLUDED.status,
                    failure_count = 0,
                    last_error = EXCLUDED.last_error,
                    last_used_at = NOW(),
                    last_validated_at = NOW()
                RETURNING *
                """,
                (
                    cache_id,
                    clean_bot_id,
                    clean_book_id,
                    str(telegram_file_id or "").strip(),
                    str(telegram_file_unique_id or "").strip() or None,
                    int(cache_channel_id) if cache_channel_id else None,
                    int(cache_message_id) if cache_message_id else None,
                    normalized_status,
                    str(last_error)[:2000] if last_error else None,
                ),
            )
            return cur.fetchone()


def touch_connected_bot_file_cache_usage(connected_bot_id: str, book_id: str) -> int:
    clean_bot_id = str(connected_bot_id or "").strip()
    clean_book_id = str(book_id or "").strip()
    if not clean_bot_id or not clean_book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE connected_bot_file_cache
                SET last_used_at=NOW(),
                    last_validated_at=NOW()
                WHERE connected_bot_id=%s AND book_id=%s
                """,
                (clean_bot_id, clean_book_id),
            )
            return cur.rowcount


def mark_connected_bot_file_cache_invalid(connected_bot_id: str, book_id: str, error_text: str | None = None) -> int:
    clean_bot_id = str(connected_bot_id or "").strip()
    clean_book_id = str(book_id or "").strip()
    if not clean_bot_id or not clean_book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE connected_bot_file_cache
                SET status=%s,
                    failure_count=COALESCE(failure_count, 0) + 1,
                    last_error=%s,
                    last_validated_at=NOW()
                WHERE connected_bot_id=%s AND book_id=%s
                """,
                (
                    WL_CACHE_STATUS_INVALID,
                    str(error_text)[:2000] if error_text else None,
                    clean_bot_id,
                    clean_book_id,
                ),
            )
            return cur.rowcount


def get_active_connected_bot_cache_seed_job(connected_bot_id: str, book_id: str) -> dict | None:
    clean_bot_id = str(connected_bot_id or "").strip()
    clean_book_id = str(book_id or "").strip()
    if not clean_bot_id or not clean_book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM connected_bot_cache_seed_jobs
                WHERE connected_bot_id=%s
                  AND book_id=%s
                  AND status = ANY(%s)
                  AND expires_at > NOW()
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (clean_bot_id, clean_book_id, [WL_SEED_STATUS_PENDING, WL_SEED_STATUS_SENT_TO_CACHE]),
            )
            return cur.fetchone()


def get_connected_bot_cache_seed_job_by_token(seed_token: str) -> dict | None:
    clean_token = str(seed_token or "").strip()
    if not clean_token:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM connected_bot_cache_seed_jobs WHERE seed_token=%s LIMIT 1", (clean_token,))
            return cur.fetchone()


def create_connected_bot_cache_seed_job(
    *,
    connected_bot_id: str,
    book_id: str,
    requesting_chat_id: int,
    requesting_user_id: int,
    requesting_message_id: int | None,
    cache_channel_id: int,
    seed_token: str,
    expires_at: datetime | None = None,
) -> dict:
    if not expires_at:
        expires_at = _now_utc() + timedelta(minutes=10)
    job_id = str(uuid.uuid4())
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO connected_bot_cache_seed_jobs (
                    id,
                    connected_bot_id,
                    book_id,
                    requesting_chat_id,
                    requesting_user_id,
                    requesting_message_id,
                    cache_channel_id,
                    status,
                    seed_token,
                    created_at,
                    updated_at,
                    expires_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                RETURNING *
                """,
                (
                    job_id,
                    str(connected_bot_id or "").strip(),
                    str(book_id or "").strip(),
                    int(requesting_chat_id),
                    int(requesting_user_id),
                    int(requesting_message_id) if requesting_message_id else None,
                    int(cache_channel_id),
                    WL_SEED_STATUS_PENDING,
                    str(seed_token or "").strip(),
                    expires_at,
                ),
            )
            return cur.fetchone()


def update_connected_bot_cache_seed_job(job_id: str, **fields) -> dict | None:
    clean_id = str(job_id or "").strip()
    if not clean_id:
        return None
    set_parts: list[str] = ["updated_at=NOW()"]
    values: list[object] = []
    mapping = {
        "status": ("status", _normalize_seed_status),
        "main_bot_cache_message_id": ("main_bot_cache_message_id", lambda v: int(v) if v is not None else None),
        "connected_bot_cache_message_id": ("connected_bot_cache_message_id", lambda v: int(v) if v is not None else None),
        "error_message": ("error_message", lambda v: str(v)[:2000] if v else None),
        "expires_at": ("expires_at", lambda v: v),
    }
    for field_name, value in fields.items():
        if field_name not in mapping:
            continue
        column, transformer = mapping[field_name]
        set_parts.append(f"{column}=%s")
        values.append(transformer(value))
    values.append(clean_id)
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"UPDATE connected_bot_cache_seed_jobs SET {', '.join(set_parts)} WHERE id=%s", values)
            cur.execute("SELECT * FROM connected_bot_cache_seed_jobs WHERE id=%s LIMIT 1", (clean_id,))
            return cur.fetchone()


def expire_stale_connected_bot_cache_seed_jobs() -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE connected_bot_cache_seed_jobs
                SET status=%s,
                    updated_at=NOW()
                WHERE status = ANY(%s)
                  AND expires_at <= NOW()
                """,
                (WL_SEED_STATUS_EXPIRED, [WL_SEED_STATUS_PENDING, WL_SEED_STATUS_SENT_TO_CACHE]),
            )
            return cur.rowcount


def increment_connected_bot_usage(
    connected_bot_id: str,
    *,
    day_value: date | None = None,
    searches: int = 0,
    sends: int = 0,
    cache_misses: int = 0,
    cache_hits: int = 0,
    errors: int = 0,
) -> dict:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        raise ValueError("connected_bot_id is required")
    day_value = day_value or date.today()
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO connected_bot_usage (
                    connected_bot_id,
                    day,
                    searches,
                    sends,
                    cache_misses,
                    cache_hits,
                    errors
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (connected_bot_id, day) DO UPDATE SET
                    searches = connected_bot_usage.searches + EXCLUDED.searches,
                    sends = connected_bot_usage.sends + EXCLUDED.sends,
                    cache_misses = connected_bot_usage.cache_misses + EXCLUDED.cache_misses,
                    cache_hits = connected_bot_usage.cache_hits + EXCLUDED.cache_hits,
                    errors = connected_bot_usage.errors + EXCLUDED.errors
                RETURNING *
                """,
                (
                    clean_id,
                    day_value,
                    int(searches),
                    int(sends),
                    int(cache_misses),
                    int(cache_hits),
                    int(errors),
                ),
            )
            return cur.fetchone()


def get_connected_bot_usage(connected_bot_id: str, *, day_value: date | None = None) -> dict | None:
    clean_id = str(connected_bot_id or "").strip()
    if not clean_id:
        return None
    day_value = day_value or date.today()
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM connected_bot_usage WHERE connected_bot_id=%s AND day=%s LIMIT 1",
                (clean_id, day_value),
            )
            return cur.fetchone()
