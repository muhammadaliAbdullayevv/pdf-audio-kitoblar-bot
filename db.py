import os
import logging
import re
import json
import uuid
import base64
import random
from contextlib import contextmanager
from datetime import datetime, date
from typing import Any, Iterable
try:
    from dotenv import dotenv_values
except Exception:
    dotenv_values = None  # type: ignore

try:
    import psycopg2
    from psycopg2 import pool
    from psycopg2.extras import RealDictCursor, execute_values
except ImportError:
    psycopg2 = None  # type: ignore
    pool = None  # type: ignore
    RealDictCursor = None  # type: ignore
    execute_values = None  # type: ignore

logger = logging.getLogger(__name__)

BG_STATUS_PENDING = "PENDING"
BG_STATUS_RUNNING = "RUNNING"
BG_STATUS_DONE = "DONE"
BG_STATUS_FAILED = "FAILED"
BG_STATUS_CANCELLED = "CANCELLED"

_BG_PENDING_ALIASES = ("queued", BG_STATUS_PENDING)
_BG_RUNNING_ALIASES = ("processing", BG_STATUS_RUNNING)
_BG_DONE_ALIASES = ("completed", BG_STATUS_DONE)
_BG_FAILED_ALIASES = ("failed", BG_STATUS_FAILED)
_BG_CANCELLED_ALIASES = ("cancelled", BG_STATUS_CANCELLED)
_BG_ACTIVE_ALIASES = _BG_PENDING_ALIASES + _BG_RUNNING_ALIASES

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_BASE_DIR, ".env")
_PROJECT_DOTENV = dotenv_values(_ENV_PATH) if callable(dotenv_values) and os.path.exists(_ENV_PATH) else {}


def _env_project_first(name: str, default: str = "") -> str:
    value = _PROJECT_DOTENV.get(name, None) if isinstance(_PROJECT_DOTENV, dict) else None
    if value is not None:
        text = str(value).strip()
        if text:
            return text
    return str(os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_project_first(name, str(default)) or str(default))
    except Exception:
        return int(default)


def _background_job_owner_id() -> int:
    try:
        return int(_env_project_first("TELEGRAM_OWNER_ID", "0") or "0")
    except Exception:
        return 0


def _background_job_limit_running() -> int:
    return max(1, _env_int("MAX_RUNNING_JOBS_PER_USER", 2))


def _background_job_limit_pending() -> int:
    return max(1, _env_int("MAX_PENDING_JOBS_PER_USER", 5))


def _guest_docs_forbidden_threshold() -> int:
    return max(1, _env_int("GUEST_DOCS_FORBIDDEN_THRESHOLD", 2))


def _guest_docs_skip_minutes() -> int:
    return max(5, _env_int("GUEST_DOCS_SKIP_MINUTES", 720))


def _background_job_user_has_limit_bypass(user_id: int) -> bool:
    owner_id = _background_job_owner_id()
    return bool(owner_id and int(user_id or 0) == owner_id)


def _normalize_background_job_status(raw: str | None) -> str:
    status = str(raw or "").strip().upper()
    if status == BG_STATUS_PENDING or status == "QUEUED":
        return BG_STATUS_PENDING
    if status == BG_STATUS_RUNNING or status == "PROCESSING":
        return BG_STATUS_RUNNING
    if status == BG_STATUS_DONE or status == "COMPLETED":
        return BG_STATUS_DONE
    if status == BG_STATUS_FAILED:
        return BG_STATUS_FAILED
    if status == BG_STATUS_CANCELLED:
        return BG_STATUS_CANCELLED
    return status or BG_STATUS_PENDING


def _bg_payload_to_jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__kind__": "bytes_b64", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, bytearray):
        return {"__kind__": "bytes_b64", "data": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _bg_payload_to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_bg_payload_to_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def serialize_background_job_payload(payload: dict | None) -> tuple[str, str]:
    clean = _bg_payload_to_jsonable(payload or {})
    text = json.dumps(clean, ensure_ascii=False)
    return text, text


def deserialize_background_job_payload(payload: Any) -> Any:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload or "{}")
        except Exception:
            return {}
    if isinstance(payload, dict):
        kind = str(payload.get("__kind__") or "").strip()
        if kind == "bytes_b64":
            try:
                return base64.b64decode(str(payload.get("data") or "").encode("ascii"))
            except Exception:
                return b""
        return {k: deserialize_background_job_payload(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return [deserialize_background_job_payload(v) for v in payload]
    return payload


def _ensure_schema_migrations(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
            note TEXT
        );
        """
    )


def _applied_schema_versions(cur) -> set[int]:
    try:
        cur.execute("SELECT version FROM schema_migrations")
        return {int(r[0]) for r in (cur.fetchall() or []) if r and r[0] is not None}
    except Exception:
        return set()


def _apply_schema_migrations(cur) -> None:
    """
    Apply one-time schema changes in a controlled way.
    Keep table creation idempotent (CREATE TABLE IF NOT EXISTS) in init_db(),
    but track index/constraint changes here to avoid repeated startup mutations.
    """
    _ensure_schema_migrations(cur)
    applied = _applied_schema_versions(cur)
    migrations: list[tuple[int, str, list[str]]] = [
        (
            1,
            "audiobooks: drop old unique index on audio_books(book_id); add non-unique index",
            [
                "DROP INDEX IF EXISTS uniq_audio_books_book_id;",
                "CREATE INDEX IF NOT EXISTS idx_audio_books_book_id ON audio_books (book_id);",
            ],
        ),
        (
            2,
            "audiobooks: allow reusing file_unique_id across audiobooks",
            [
                "DROP INDEX IF EXISTS uniq_audio_parts_file_unique_id;",
                """
                CREATE INDEX IF NOT EXISTS idx_audio_parts_audio_book_file_unique
                ON audio_book_parts (audio_book_id, file_unique_id)
                WHERE file_unique_id IS NOT NULL;
                """,
            ],
        ),
        (
            3,
            "audiobooks: drop legacy unique constraint/index on audio_books(book_id)",
            [
                "ALTER TABLE audio_books DROP CONSTRAINT IF EXISTS uniq_audio_books_book_id;",
                "DROP INDEX IF EXISTS uniq_audio_books_book_id;",
                "DROP INDEX IF EXISTS audio_books_book_id_key;",
                "CREATE INDEX IF NOT EXISTS idx_audio_books_book_id ON audio_books (book_id);",
            ],
        ),
        (
            4,
            "audiobooks: persist storage source metadata for parts",
            [
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_id BIGINT;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_message_id BIGINT;",
                "CREATE INDEX IF NOT EXISTS idx_audio_book_parts_channel_msg ON audio_book_parts (channel_id, channel_message_id);",
            ],
        ),
        (
            7,
            "users: store separate group language preference",
            [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS group_language TEXT;",
            ],
        ),
        (
            8,
            "movies: remove movie tables after feature deprecation",
            [
                "DROP TABLE IF EXISTS movie_reactions;",
                "DROP TABLE IF EXISTS movies;",
            ],
        ),
        (
            9,
            "remove legacy movie analytics columns and quiz storage",
            [
                "ALTER TABLE analytics_daily DROP COLUMN IF EXISTS movie_searches;",
                "ALTER TABLE analytics_daily DROP COLUMN IF EXISTS movie_downloads;",
                "ALTER TABLE analytics_daily_users DROP COLUMN IF EXISTS movie_searches;",
                "ALTER TABLE analytics_daily_users DROP COLUMN IF EXISTS movie_downloads;",
                "DROP TABLE IF EXISTS user_quizzes;",
            ],
        ),
        (
            10,
            "remove deprecated name meanings dataset table",
            [
                "DROP TABLE IF EXISTS name_meanings;",
            ],
        ),
        (
            11,
            "books: persist local backup jobs for restart-safe downloads",
            [
                """
                CREATE TABLE IF NOT EXISTS book_local_download_jobs (
                    id TEXT PRIMARY KEY,
                    book_id TEXT NOT NULL UNIQUE,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    file_name TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 12,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    worker_id TEXT,
                    last_error TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_local_download_jobs_status_next ON book_local_download_jobs (status, next_attempt_at, created_at);",
                "CREATE INDEX IF NOT EXISTS idx_book_local_download_jobs_book_id ON book_local_download_jobs (book_id);",
            ],
        ),
        (
            12,
            "books: drop legacy storage channel metadata",
            [
                "ALTER TABLE books DROP COLUMN IF EXISTS storage_chat_id;",
                "ALTER TABLE books DROP COLUMN IF EXISTS storage_message_id;",
                "ALTER TABLE books DROP COLUMN IF EXISTS storage_updated_at;",
            ],
        ),
        (
            13,
            "users: add rename permission flag",
            [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS rename_allowed BOOLEAN NOT NULL DEFAULT FALSE;",
            ],
        ),
        (
            14,
            "groups: persist private-start prompt messages for editable follow-up",
            [
                """
                CREATE TABLE IF NOT EXISTS group_private_start_prompts (
                    token TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    prompt_lang TEXT NOT NULL DEFAULT 'en',
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    resolved_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_group_private_start_prompts_user_status ON group_private_start_prompts (user_id, status, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_group_private_start_prompts_chat_msg ON group_private_start_prompts (chat_id, message_id);",
            ],
        ),
        (
            17,
            "bots: persist runtime key/value settings",
            [
                """
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_bot_settings_updated_at ON bot_settings (updated_at DESC);",
            ],
        ),
        (
            18,
            "audiobooks: persist local backup jobs for restart-safe downloads",
            [
                """
                CREATE TABLE IF NOT EXISTS audio_book_local_download_jobs (
                    id TEXT PRIMARY KEY,
                    audio_book_id TEXT NOT NULL REFERENCES audio_books(id) ON DELETE CASCADE,
                    audio_book_part_id TEXT NOT NULL UNIQUE REFERENCES audio_book_parts(id) ON DELETE CASCADE,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    file_name TEXT,
                    media_kind TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 12,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    worker_id TEXT,
                    last_error TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_status_next ON audio_book_local_download_jobs (status, next_attempt_at, created_at);",
                "CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_audio_book_id ON audio_book_local_download_jobs (audio_book_id);",
                "CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_part_id ON audio_book_local_download_jobs (audio_book_part_id);",
            ],
        ),
        (
            19,
            "audiobooks: add missing part metadata columns for older databases",
            [
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS media_kind TEXT;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS path TEXT;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS duration_seconds INTEGER;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_id BIGINT;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_message_id BIGINT;",
                "ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS display_order BIGINT;",
            ],
        ),
        (
            22,
            "background jobs: production worker fields, statuses, and indexes",
            [
                """
                CREATE TABLE IF NOT EXISTS background_jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT,
                    message_id BIGINT,
                    data_json TEXT NOT NULL DEFAULT '{}',
                    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    result_json JSONB,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    priority INTEGER NOT NULL DEFAULT 100,
                    progress INTEGER NOT NULL DEFAULT 0,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    idempotency_key TEXT,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    locked_by TEXT,
                    worker_id TEXT,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS chat_id BIGINT;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS message_id BIGINT;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 100;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS payload_json JSONB;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS result_json JSONB;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS progress INTEGER NOT NULL DEFAULT 0;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS locked_by TEXT;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS error_message TEXT;",
                "ALTER TABLE background_jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;",
                "UPDATE background_jobs SET payload_json = COALESCE(payload_json, CASE WHEN data_json IS NULL OR BTRIM(data_json) = '' THEN '{}'::jsonb ELSE data_json::jsonb END) WHERE payload_json IS NULL;",
                "UPDATE background_jobs SET status = 'PENDING' WHERE UPPER(COALESCE(status, '')) = 'QUEUED';",
                "UPDATE background_jobs SET status = 'RUNNING' WHERE UPPER(COALESCE(status, '')) = 'PROCESSING';",
                "UPDATE background_jobs SET status = 'DONE' WHERE UPPER(COALESCE(status, '')) = 'COMPLETED';",
                "UPDATE background_jobs SET status = 'FAILED' WHERE UPPER(COALESCE(status, '')) = 'FAILED';",
                "UPDATE background_jobs SET status = 'CANCELLED' WHERE UPPER(COALESCE(status, '')) = 'CANCELLED';",
                "UPDATE background_jobs SET locked_by = COALESCE(locked_by, worker_id) WHERE locked_by IS NULL AND worker_id IS NOT NULL;",
                "UPDATE background_jobs SET error_message = COALESCE(error_message, last_error) WHERE error_message IS NULL AND last_error IS NOT NULL;",
                "UPDATE background_jobs SET finished_at = COALESCE(finished_at, completed_at) WHERE finished_at IS NULL AND completed_at IS NOT NULL;",
                "UPDATE background_jobs SET started_at = COALESCE(started_at, locked_at) WHERE started_at IS NULL AND locked_at IS NOT NULL;",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_user_id ON background_jobs (user_id);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_status ON background_jobs (status);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_next_attempt_at ON background_jobs (next_attempt_at);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_job_type ON background_jobs (job_type);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_claim ON background_jobs (status, priority, next_attempt_at, created_at);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_idempotency ON background_jobs (user_id, idempotency_key, status);",
                "CREATE INDEX IF NOT EXISTS idx_background_jobs_locked_at ON background_jobs (locked_at) WHERE locked_at IS NOT NULL;",
            ],
        ),
        (
            23,
            "remove deprecated media/pdf/tts/sticker tool job rows and counters",
            [
                """
                DELETE FROM background_jobs
                WHERE job_type IN (
                    'pdf_maker',
                    'pdf_editor',
                    'tts_generate',
                    'audio_convert',
                    'sticker_convert',
                    'PDF_CREATE',
                    'PDF_MERGE',
                    'PDF_SPLIT',
                    'PDF_COMPRESS',
                    'PDF_EDIT',
                    'TEXT_TO_VOICE',
                    'AUDIO_TRIM',
                    'STICKER_CONVERT'
                );
                """,
                "DELETE FROM analytics_counters WHERE key IN ('ai_pdf_created');",
            ],
        ),
        (
            24,
            "remove deprecated video downloader jobs and counters",
            [
                "DELETE FROM background_jobs WHERE job_type IN ('video_download', 'VIDEO_DOWNLOAD');",
                "DELETE FROM analytics_counters WHERE key = 'video_downloads' OR key LIKE 'video_dl_%';",
            ],
        ),
        (
            25,
            "add delivery-path indexes",
            [
                "CREATE INDEX IF NOT EXISTS idx_books_display_name ON books (display_name);",
                "CREATE INDEX IF NOT EXISTS idx_upload_receipts_book_id ON upload_receipts (book_id);",
            ],
        ),
        (
            26,
            "guest mode analytics: persist guest group usage",
            [
                """
                CREATE TABLE IF NOT EXISTS guest_groups (
                    chat_id BIGINT PRIMARY KEY,
                    chat_type TEXT,
                    title TEXT,
                    username TEXT,
                    public_link TEXT,
                    last_query TEXT,
                    searches INTEGER NOT NULL DEFAULT 0,
                    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_guest_groups_last_seen ON guest_groups (last_seen_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_guest_groups_username ON guest_groups (username);",
            ],
        ),
        (
            27,
            "guest mode analytics: add missing first_seen_at to existing guest_groups tables",
            [
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP NOT NULL DEFAULT NOW();",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP NOT NULL DEFAULT NOW();",
            ],
        ),
        (
            28,
            "guest mode: persist private-chat handoff payloads",
            [
                """
                CREATE TABLE IF NOT EXISTS guest_private_handoffs (
                    token TEXT PRIMARY KEY,
                    handoff_type TEXT NOT NULL DEFAULT 'query',
                    creator_user_id BIGINT,
                    query_text TEXT,
                    book_id TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMP,
                    use_count INTEGER NOT NULL DEFAULT 0
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_created_at ON guest_private_handoffs (created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_book_id ON guest_private_handoffs (book_id);",
            ],
        ),
        (
            29,
            "guest mode analytics: persist guest user searches",
            [
                """
                CREATE TABLE IF NOT EXISTS guest_user_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    chat_id BIGINT,
                    chat_type TEXT,
                    group_title TEXT,
                    group_username TEXT,
                    public_link TEXT,
                    query_text TEXT,
                    searched_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_guest_user_activity_ts ON guest_user_activity (searched_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_guest_user_activity_user ON guest_user_activity (user_id, searched_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_guest_user_activity_chat ON guest_user_activity (chat_id, searched_at DESC);",
            ],
        ),
        (
            30,
            "inline analytics: persist inline searches and chosen results",
            [
                """
                CREATE TABLE IF NOT EXISTS inline_search_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    query_text TEXT,
                    searched_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_inline_search_activity_ts ON inline_search_activity (searched_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_inline_search_activity_user ON inline_search_activity (user_id, searched_at DESC);",
                """
                CREATE TABLE IF NOT EXISTS inline_chosen_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    query_text TEXT,
                    result_id TEXT,
                    chosen_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_ts ON inline_chosen_activity (chosen_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_user ON inline_chosen_activity (user_id, chosen_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_result ON inline_chosen_activity (result_id, chosen_at DESC);",
            ],
        ),
        (
            31,
            "guest mode: persist source chat and delivery capability memory",
            [
                "ALTER TABLE guest_private_handoffs ADD COLUMN IF NOT EXISTS source_chat_id BIGINT;",
                "ALTER TABLE guest_private_handoffs ADD COLUMN IF NOT EXISTS source_chat_type TEXT;",
                "ALTER TABLE guest_private_handoffs ADD COLUMN IF NOT EXISTS source_chat_title TEXT;",
                "ALTER TABLE guest_private_handoffs ADD COLUMN IF NOT EXISTS source_chat_username TEXT;",
                "ALTER TABLE guest_private_handoffs ADD COLUMN IF NOT EXISTS source_public_link TEXT;",
                "CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_source_chat_id ON guest_private_handoffs (source_chat_id);",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS guest_doc_forbidden_count INTEGER NOT NULL DEFAULT 0;",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS guest_doc_success_count INTEGER NOT NULL DEFAULT 0;",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS guest_doc_skip_until TIMESTAMP;",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS guest_doc_last_forbidden_at TIMESTAMP;",
                "ALTER TABLE guest_groups ADD COLUMN IF NOT EXISTS guest_doc_last_success_at TIMESTAMP;",
            ],
        ),
        (
            32,
            "books: persist forbidden/copyright-restricted titles",
            [
                """
                CREATE TABLE IF NOT EXISTS forbidden_books (
                    normalized_title TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    created_by_user_id BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_forbidden_books_updated_at ON forbidden_books (updated_at DESC);",
            ],
        ),
        (
            33,
            "books: persist owner reaction display adjustments",
            [
                """
                CREATE TABLE IF NOT EXISTS book_reaction_adjustments (
                    book_id TEXT PRIMARY KEY,
                    like_offset INTEGER NOT NULL DEFAULT 0,
                    dislike_offset INTEGER NOT NULL DEFAULT 0,
                    berry_offset INTEGER NOT NULL DEFAULT 0,
                    whale_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
            ],
        ),
        (
            34,
            "books: persist negative reaction alert state",
            [
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS negative_reaction_alert_active BOOLEAN NOT NULL DEFAULT FALSE;",
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS negative_reaction_alert_trigger_dislikes INTEGER;",
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS negative_reaction_alerted_at TIMESTAMP;",
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS negative_reaction_alert_chat_id BIGINT;",
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS negative_reaction_alert_message_id BIGINT;",
            ],
        ),
        (
            35,
            "books: persist display adjustments for downloads and favorites",
            [
                """
                CREATE TABLE IF NOT EXISTS book_counter_adjustments (
                    book_id TEXT PRIMARY KEY,
                    downloads_offset INTEGER NOT NULL DEFAULT 0,
                    favorite_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
            ],
        ),
        (
            36,
            "books: persist per-book reaction moderation policy",
            [
                """
                CREATE TABLE IF NOT EXISTS book_reaction_policies (
                    book_id TEXT PRIMARY KEY,
                    reactions_locked BOOLEAN NOT NULL DEFAULT FALSE,
                    dislikes_disabled BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
            ],
        ),
        (
            37,
            "books: store threaded comments",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comments (
                    id TEXT PRIMARY KEY,
                    book_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    parent_comment_id TEXT,
                    root_comment_id TEXT,
                    text TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    moderated_by_user_id BIGINT,
                    moderated_at TIMESTAMP,
                    deleted_reason TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comments_book_status_created ON book_comments (book_id, status, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comments_parent_created ON book_comments (parent_comment_id, created_at ASC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comments_root_created ON book_comments (root_comment_id, created_at ASC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comments_user_created ON book_comments (user_id, created_at DESC);",
            ],
        ),
        (
            38,
            "books: store stable anonymous aliases for commenters per book",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_aliases (
                    book_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    alias_number INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (book_id, user_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_aliases_book_alias ON book_comment_aliases (book_id, alias_number ASC);",
            ],
        ),
        (
            39,
            "books: store identity reveal requests for comments",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_identity_requests (
                    id TEXT PRIMARY KEY,
                    comment_id TEXT NOT NULL,
                    requester_user_id BIGINT NOT NULL,
                    commenter_user_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    resolved_at TIMESTAMP
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_identity_requests_comment_requester ON book_comment_identity_requests (comment_id, requester_user_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_identity_requests_commenter_status ON book_comment_identity_requests (commenter_user_id, status, created_at DESC);",
            ],
        ),
        (
            40,
            "books: store comment reports",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_reports (
                    id TEXT PRIMARY KEY,
                    comment_id TEXT NOT NULL,
                    reporter_user_id BIGINT NOT NULL,
                    reason TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE UNIQUE INDEX IF NOT EXISTS uniq_book_comment_reports_comment_reporter ON book_comment_reports (comment_id, reporter_user_id);",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_reports_status_created ON book_comment_reports (status, created_at DESC);",
            ],
        ),
        (
            41,
            "books: store comment bans",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_bans (
                    user_id BIGINT PRIMARY KEY,
                    banned_by_user_id BIGINT,
                    reason TEXT,
                    until_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
            ],
        ),
        (
            42,
            "books: store anonymous relay conversations for comment replies",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_relay_conversations (
                    id TEXT PRIMARY KEY,
                    book_id TEXT NOT NULL,
                    comment_id TEXT NOT NULL,
                    comment_owner_user_id BIGINT NOT NULL,
                    peer_user_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    UNIQUE (comment_id, peer_user_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_conversations_owner_updated ON book_comment_relay_conversations (comment_owner_user_id, updated_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_conversations_peer_updated ON book_comment_relay_conversations (peer_user_id, updated_at DESC);",
            ],
        ),
        (
            43,
            "books: store anonymous relay messages for comment replies",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_relay_messages (
                    id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    book_id TEXT NOT NULL,
                    comment_id TEXT NOT NULL,
                    sender_user_id BIGINT NOT NULL,
                    recipient_user_id BIGINT NOT NULL,
                    message_type TEXT NOT NULL,
                    text TEXT,
                    caption TEXT,
                    media_file_id TEXT,
                    media_file_unique_id TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_messages_conversation_created ON book_comment_relay_messages (conversation_id, created_at ASC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_messages_recipient_created ON book_comment_relay_messages (recipient_user_id, created_at DESC);",
            ],
        ),
        (
            44,
            "books: store recipient-specific blocks for anonymous relay replies",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_peer_blocks (
                    blocker_user_id BIGINT NOT NULL,
                    blocked_user_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (blocker_user_id, blocked_user_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_peer_blocks_blocker_updated ON book_comment_peer_blocks (blocker_user_id, updated_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_peer_blocks_blocked_updated ON book_comment_peer_blocks (blocked_user_id, updated_at DESC);",
            ],
        ),
        (
            45,
            "books: store participant state for anonymous relay conversations",
            [
                """
                CREATE TABLE IF NOT EXISTS book_comment_relay_participants (
                    conversation_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    muted BOOLEAN NOT NULL DEFAULT FALSE,
                    last_seen_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (conversation_id, user_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_participants_user_updated ON book_comment_relay_participants (user_id, updated_at DESC);",
            ],
        ),
        (
            46,
            "books: support one-sided closure for comment relay conversations",
            [
                "ALTER TABLE book_comment_relay_conversations ADD COLUMN IF NOT EXISTS closed_by_user_id BIGINT;",
                "ALTER TABLE book_comment_relay_conversations ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP;",
                "ALTER TABLE book_comment_relay_conversations ADD COLUMN IF NOT EXISTS closed_notified_at TIMESTAMP;",
                "CREATE INDEX IF NOT EXISTS idx_book_comment_relay_conversations_closed_by ON book_comment_relay_conversations (closed_by_user_id, updated_at DESC);",
            ],
        ),
        (
            47,
            "white-label connected bot mvp tables",
            [
                """
                CREATE TABLE IF NOT EXISTS connected_bots (
                    id TEXT PRIMARY KEY,
                    owner_telegram_id BIGINT NOT NULL,
                    bot_telegram_id BIGINT NOT NULL UNIQUE,
                    bot_username TEXT NOT NULL,
                    bot_first_name TEXT,
                    bot_token_encrypted TEXT NOT NULL,
                    bot_token_fingerprint TEXT NOT NULL UNIQUE,
                    cache_channel_id BIGINT,
                    cache_channel_username TEXT,
                    status TEXT NOT NULL DEFAULT 'SUSPENDED',
                    plan TEXT NOT NULL DEFAULT 'MANUAL',
                    daily_search_limit INTEGER NOT NULL DEFAULT 1000,
                    daily_send_limit INTEGER NOT NULL DEFAULT 100,
                    per_minute_send_limit INTEGER NOT NULL DEFAULT 10,
                    last_verified_at TIMESTAMP,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_connected_bots_owner ON connected_bots (owner_telegram_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bots_status ON connected_bots (status, updated_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bots_username ON connected_bots (LOWER(bot_username));",
                """
                CREATE TABLE IF NOT EXISTS connected_bot_file_cache (
                    id TEXT PRIMARY KEY,
                    connected_bot_id TEXT NOT NULL REFERENCES connected_bots(id) ON DELETE CASCADE,
                    book_id TEXT NOT NULL REFERENCES books(id) ON DELETE CASCADE,
                    telegram_file_id TEXT,
                    telegram_file_unique_id TEXT,
                    cache_channel_id BIGINT,
                    cache_message_id BIGINT,
                    status TEXT NOT NULL DEFAULT 'VALID',
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMP,
                    last_validated_at TIMESTAMP,
                    UNIQUE (connected_bot_id, book_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_file_cache_status ON connected_bot_file_cache (connected_bot_id, status, last_validated_at DESC);",
                """
                CREATE TABLE IF NOT EXISTS connected_bot_cache_seed_jobs (
                    id TEXT PRIMARY KEY,
                    connected_bot_id TEXT NOT NULL REFERENCES connected_bots(id) ON DELETE CASCADE,
                    book_id TEXT NOT NULL REFERENCES books(id) ON DELETE CASCADE,
                    requesting_chat_id BIGINT NOT NULL,
                    requesting_user_id BIGINT NOT NULL,
                    requesting_message_id BIGINT,
                    cache_channel_id BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    seed_token TEXT NOT NULL UNIQUE,
                    main_bot_cache_message_id BIGINT,
                    connected_bot_cache_message_id BIGINT,
                    error_message TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMP NOT NULL
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_cache_seed_jobs_lookup ON connected_bot_cache_seed_jobs (connected_bot_id, book_id, status, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_cache_seed_jobs_expiry ON connected_bot_cache_seed_jobs (status, expires_at);",
                """
                CREATE TABLE IF NOT EXISTS connected_bot_usage (
                    connected_bot_id TEXT NOT NULL REFERENCES connected_bots(id) ON DELETE CASCADE,
                    day DATE NOT NULL,
                    searches INTEGER NOT NULL DEFAULT 0,
                    sends INTEGER NOT NULL DEFAULT 0,
                    cache_misses INTEGER NOT NULL DEFAULT 0,
                    cache_hits INTEGER NOT NULL DEFAULT 0,
                    errors INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (connected_bot_id, day)
                );
                """,
            ],
        ),
        (
            48,
            "white-label public connected bot request workflow",
            [
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_user_id BIGINT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_username TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_first_name TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS accepted_request_id TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS subscription_status TEXT DEFAULT 'MANUAL';",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMP;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_ends_at TIMESTAMP;",
                "CREATE INDEX IF NOT EXISTS idx_connected_bots_requested_by ON connected_bots (requested_by_user_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bots_trial ON connected_bots (plan, subscription_status, trial_ends_at);",
                """
                CREATE TABLE IF NOT EXISTS connected_bot_requests (
                    id TEXT PRIMARY KEY,
                    requesting_user_id BIGINT NOT NULL,
                    requesting_username TEXT,
                    requesting_first_name TEXT,
                    bot_telegram_id BIGINT NOT NULL,
                    bot_username TEXT NOT NULL,
                    bot_first_name TEXT,
                    bot_token_encrypted TEXT NOT NULL,
                    bot_token_fingerprint TEXT NOT NULL,
                    token_masked TEXT,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    rejection_reason TEXT,
                    accepted_by_owner_id BIGINT,
                    accepted_at TIMESTAMP,
                    rejected_by_owner_id BIGINT,
                    rejected_at TIMESTAMP,
                    accepted_connected_bot_id TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_status_created ON connected_bot_requests (status, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_user ON connected_bot_requests (requesting_user_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_username ON connected_bot_requests (LOWER(bot_username));",
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_token_fp ON connected_bot_requests (bot_token_fingerprint);",
            ],
        ),
        (
            49,
            "white-label connected bot user language and public settings",
            [
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS branding_title TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_uz TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_en TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_ru TEXT;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS search_results_limit INTEGER NOT NULL DEFAULT 10;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_expired_notified_at TIMESTAMP;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMP;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_pid BIGINT;",
                """
                CREATE TABLE IF NOT EXISTS connected_bot_users (
                    connected_bot_id TEXT NOT NULL REFERENCES connected_bots(id) ON DELETE CASCADE,
                    telegram_user_id BIGINT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    language_code TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (connected_bot_id, telegram_user_id)
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_users_updated ON connected_bot_users (connected_bot_id, updated_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_connected_bot_users_user ON connected_bot_users (telegram_user_id, updated_at DESC);",
            ],
        ),
        (
            50,
            "white-label safety controls",
            [
                "ALTER TABLE connected_bot_requests ALTER COLUMN bot_token_encrypted DROP NOT NULL;",
                """
                UPDATE connected_bot_requests
                SET bot_token_encrypted=NULL,
                    updated_at=NOW()
                WHERE status IN ('ACCEPTED', 'REJECTED', 'CANCELLED')
                  AND bot_token_encrypted IS NOT NULL;
                """,
                "ALTER TABLE books ADD COLUMN IF NOT EXISTS white_label_enabled BOOLEAN NOT NULL DEFAULT TRUE;",
                "CREATE INDEX IF NOT EXISTS idx_books_white_label_enabled ON books (white_label_enabled);",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMP;",
                "ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_pid BIGINT;",
                "UPDATE connected_bots SET plan='PRO' WHERE UPPER(plan)='PLUS';",
                """
                CREATE TABLE IF NOT EXISTS white_label_audit_logs (
                    id TEXT PRIMARY KEY,
                    connected_bot_id TEXT REFERENCES connected_bots(id) ON DELETE SET NULL,
                    request_id TEXT,
                    actor_user_id BIGINT,
                    action TEXT NOT NULL,
                    target_bot_username TEXT,
                    details_json JSONB,
                    error_message TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """,
                "CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_bot_created ON white_label_audit_logs (connected_bot_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_request_created ON white_label_audit_logs (request_id, created_at DESC);",
                "CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_action_created ON white_label_audit_logs (action, created_at DESC);",
            ],
        ),
    ]
    for version, note, stmts in migrations:
        if version in applied:
            continue
        for sql in stmts:
            cur.execute(sql)
        cur.execute(
            "INSERT INTO schema_migrations (version, note) VALUES (%s, %s) ON CONFLICT (version) DO NOTHING",
            (int(version), str(note)),
        )

_pool: pool.ThreadedConnectionPool | None = None
try:
    _DB_POOL_MIN = max(1, int(os.getenv("DB_POOL_MIN", "10")))
except Exception:
    _DB_POOL_MIN = 10
try:
    _DB_POOL_MAX = max(_DB_POOL_MIN, int(os.getenv("DB_POOL_MAX", "50")))
except Exception:
    _DB_POOL_MAX = max(_DB_POOL_MIN, 50)


def _dsn():
    return {
        "dbname": _env_project_first("DB_NAME", ""),
        "user": _env_project_first("DB_USER", ""),
        "password": _env_project_first("DB_PASS", ""),
        "host": _env_project_first("DB_HOST", "localhost"),
        "port": int(_env_project_first("DB_PORT", "5432") or "5432"),
        "application_name": _env_project_first("PGAPPNAME", "pdf_audio_kitoblar_bot"),
    }


def _create_pool() -> None:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(minconn=_DB_POOL_MIN, maxconn=_DB_POOL_MAX, **_dsn())
        logger.info("DB pool initialized: minconn=%s maxconn=%s", _DB_POOL_MIN, _DB_POOL_MAX)


def _reset_pool() -> None:
    global _pool
    if _pool is None:
        return
    try:
        _pool.closeall()
    except Exception:
        pass
    finally:
        _pool = None


def _table_exists(cur, table_name: str) -> bool:
    def _first_value(row):
        if row is None:
            return None
        # RealDictCursor rows are mapping-like; regular cursor rows are tuple-like.
        if isinstance(row, dict):
            for v in row.values():
                return v
            return None
        try:
            return row[0]
        except Exception:
            try:
                values = row.values()  # type: ignore[attr-defined]
                for v in values:
                    return v
            except Exception:
                return None
            return None

    try:
        # Resolve against current search_path first, then explicit public schema.
        # This avoids false negatives when deployments use non-public schemas.
        cur.execute("SELECT to_regclass(%s)", (str(table_name),))
        row = cur.fetchone()
        if _first_value(row):
            return True
        if "." not in str(table_name):
            cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
            row = cur.fetchone()
            if _first_value(row):
                return True
        return False
    except Exception:
        return False


def _runtime_schema_mode() -> str:
    mode = str(os.getenv("DB_RUNTIME_SCHEMA_MODE", "auto") or "auto").strip().lower()
    return mode if mode in {"auto", "always", "never"} else "auto"


def init_db():
    """Initialize connection pool and ensure schema exists."""
    _create_pool()
    with db_conn() as conn:
        with conn.cursor() as cur:
            _ensure_schema_migrations(cur)
            mode = _runtime_schema_mode()
            users_exists = _table_exists(cur, "users")
            # Fast path for Alembic-managed environments:
            # avoid replaying large CREATE/ALTER bootstrap on every startup.
            if mode in {"auto", "never"} and users_exists:
                try:
                    _apply_schema_migrations(cur)
                    logger.debug("Runtime schema bootstrap skipped (mode=%s, users_exists=%s)", mode, users_exists)
                    return
                except Exception as e:
                    if mode == "never":
                        raise
                    logger.warning("Fast schema migration path failed, falling back to runtime bootstrap: %s", e)
            if mode == "never" and not users_exists:
                raise RuntimeError(
                    "DB_RUNTIME_SCHEMA_MODE=never but required tables are missing. "
                    "Run Alembic migrations or set DB_RUNTIME_SCHEMA_MODE=auto."
                )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    allowed BOOLEAN NOT NULL DEFAULT FALSE,
                    rename_allowed BOOLEAN NOT NULL DEFAULT FALSE,
                    joined_date DATE,
                    left_date DATE,
                    language TEXT,
                    group_language TEXT,
                    language_selected BOOLEAN
                );
                """
            )
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS group_language TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS delete_allowed BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS rename_allowed BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS stopped BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS audio_allowed BOOLEAN NOT NULL DEFAULT FALSE;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS coin_adjustment INTEGER NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referrer_id BIGINT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_at TIMESTAMP;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS language_selected BOOLEAN;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_referrer_id ON users (referrer_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    id TEXT PRIMARY KEY,
                    book_name TEXT,
                    display_name TEXT,
                    file_id TEXT,
                    path TEXT,
                    indexed BOOLEAN NOT NULL DEFAULT FALSE,
                    downloads INTEGER NOT NULL DEFAULT 0,
                    searches INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_books_name ON books (book_name);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_books_display_name ON books (display_name);")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS file_unique_id TEXT;")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS downloads INTEGER NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS searches INTEGER NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT NOW();")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS uploaded_by_user_id BIGINT;")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS upload_source TEXT;")
            cur.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS white_label_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_books_white_label_enabled ON books (white_label_enabled);")
            cur.execute("ALTER TABLE books DROP COLUMN IF EXISTS storage_chat_id;")
            cur.execute("ALTER TABLE books DROP COLUMN IF EXISTS storage_message_id;")
            cur.execute("ALTER TABLE books DROP COLUMN IF EXISTS storage_updated_at;")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_books_created_at ON books (created_at DESC);")
            try:
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_books_file_unique_id ON books (file_unique_id) WHERE file_unique_id IS NOT NULL;"
                )
            except Exception as e:
                # In case duplicates already exist, skip to avoid init failure
                logger.warning("Could not create uniq_books_file_unique_id (skipping): %s", e)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_local_download_jobs (
                    id TEXT PRIMARY KEY,
                    book_id TEXT NOT NULL UNIQUE,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    file_name TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 12,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    worker_id TEXT,
                    last_error TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_local_download_jobs_status_next ON book_local_download_jobs (status, next_attempt_at, created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_local_download_jobs_book_id ON book_local_download_jobs (book_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS group_private_start_prompts (
                    token TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    prompt_lang TEXT NOT NULL DEFAULT 'en',
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    resolved_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_group_private_start_prompts_user_status ON group_private_start_prompts (user_id, status, created_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_group_private_start_prompts_chat_msg ON group_private_start_prompts (chat_id, message_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guest_private_handoffs (
                    token TEXT PRIMARY KEY,
                    handoff_type TEXT NOT NULL DEFAULT 'query',
                    creator_user_id BIGINT,
                    query_text TEXT,
                    book_id TEXT,
                    source_chat_id BIGINT,
                    source_chat_type TEXT,
                    source_chat_title TEXT,
                    source_chat_username TEXT,
                    source_public_link TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMP,
                    use_count INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_created_at ON guest_private_handoffs (created_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_book_id ON guest_private_handoffs (book_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_private_handoffs_source_chat_id ON guest_private_handoffs (source_chat_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_settings_updated_at ON bot_settings (updated_at DESC);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forbidden_books (
                    normalized_title TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    created_by_user_id BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_forbidden_books_updated_at ON forbidden_books (updated_at DESC);")
            cur.execute("DROP TABLE IF EXISTS movie_reactions;")
            cur.execute("DROP TABLE IF EXISTS movies;")
            cur.execute("DROP TABLE IF EXISTS name_meanings;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_receipts (
                    id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    file_id TEXT,
                    file_unique_id TEXT,
                    file_name TEXT,
                    status TEXT NOT NULL DEFAULT 'received',
                    error TEXT,
                    book_id TEXT,
                    saved_to_db BOOLEAN NOT NULL DEFAULT FALSE,
                    saved_to_es BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_receipts_created_at ON upload_receipts (created_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_receipts_status ON upload_receipts (status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_receipts_file_unique_id ON upload_receipts (file_unique_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_receipts_book_id ON upload_receipts (book_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_summaries (
                    book_id TEXT NOT NULL,
                    lang TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    summary_text TEXT NOT NULL,
                    model_name TEXT,
                    source_hash TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (book_id, lang, mode)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_summaries_book_id ON book_summaries (book_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_reactions (
                    book_id TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    reaction TEXT NOT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (book_id, user_id)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_reactions_book ON book_reactions (book_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_reaction_adjustments (
                    book_id TEXT PRIMARY KEY,
                    like_offset INTEGER NOT NULL DEFAULT 0,
                    dislike_offset INTEGER NOT NULL DEFAULT 0,
                    berry_offset INTEGER NOT NULL DEFAULT 0,
                    whale_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_favorites (
                    user_id BIGINT NOT NULL,
                    book_id TEXT NOT NULL,
                    title TEXT,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, book_id)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_favs_user_ts ON user_favorites (user_id, ts DESC);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_favorite_awards (
                    user_id BIGINT NOT NULL,
                    book_id TEXT NOT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, book_id)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_fav_awards_user ON user_favorite_awards (user_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_recents (
                    user_id BIGINT NOT NULL,
                    book_id TEXT NOT NULL,
                    title TEXT,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, book_id)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_recents_user_ts ON user_recents (user_id, ts DESC);")
            cur.execute("ALTER TABLE user_recents ADD COLUMN IF NOT EXISTS count INTEGER NOT NULL DEFAULT 0;")
            cur.execute("ALTER TABLE user_recents ADD COLUMN IF NOT EXISTS last_ts TIMESTAMP;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_reaction_awards (
                    user_id BIGINT NOT NULL,
                    book_id TEXT NOT NULL,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, book_id)
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_react_awards_user ON user_reaction_awards (user_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_daily (
                    day DATE PRIMARY KEY,
                    searches INTEGER NOT NULL DEFAULT 0,
                    buttons INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_daily_users (
                    day DATE NOT NULL,
                    user_id BIGINT NOT NULL,
                    searches INTEGER NOT NULL DEFAULT 0,
                    buttons INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (day, user_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_counters (
                    key TEXT PRIMARY KEY,
                    value BIGINT NOT NULL DEFAULT 0
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guest_groups (
                    chat_id BIGINT PRIMARY KEY,
                    chat_type TEXT,
                    title TEXT,
                    username TEXT,
                    public_link TEXT,
                    last_query TEXT,
                    searches BIGINT NOT NULL DEFAULT 0,
                    first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    guest_doc_forbidden_count INTEGER NOT NULL DEFAULT 0,
                    guest_doc_success_count INTEGER NOT NULL DEFAULT 0,
                    guest_doc_skip_until TIMESTAMP,
                    guest_doc_last_forbidden_at TIMESTAMP,
                    guest_doc_last_success_at TIMESTAMP
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_groups_last_seen ON guest_groups (last_seen_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_groups_username ON guest_groups (username);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS guest_user_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    chat_id BIGINT,
                    chat_type TEXT,
                    group_title TEXT,
                    group_username TEXT,
                    public_link TEXT,
                    query_text TEXT,
                    searched_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_user_activity_ts ON guest_user_activity (searched_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_user_activity_user ON guest_user_activity (user_id, searched_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_guest_user_activity_chat ON guest_user_activity (chat_id, searched_at DESC);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS inline_search_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    query_text TEXT,
                    searched_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inline_search_activity_ts ON inline_search_activity (searched_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inline_search_activity_user ON inline_search_activity (user_id, searched_at DESC);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS inline_chosen_activity (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    query_text TEXT,
                    result_id TEXT,
                    chosen_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_ts ON inline_chosen_activity (chosen_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_user ON inline_chosen_activity (user_id, chosen_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inline_chosen_activity_result ON inline_chosen_activity (result_id, chosen_at DESC);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_requests (
                    id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    query TEXT,
                    query_norm TEXT,
                    language TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    created_ts DOUBLE PRECISION,
                    updated_at TIMESTAMP,
                    status_by BIGINT,
                    status_by_name TEXT,
                    admin_chat_id BIGINT,
                    admin_message_id BIGINT,
                    admin_note TEXT,
                    fulfilled_at TIMESTAMP,
                    book_id TEXT
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_requests_user ON book_requests (user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_book_requests_status ON book_requests (status);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS upload_requests (
                    id TEXT PRIMARY KEY,
                    user_id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language TEXT,
                    status TEXT,
                    created_at TIMESTAMP,
                    created_ts DOUBLE PRECISION,
                    updated_at TIMESTAMP,
                    status_by BIGINT,
                    status_by_name TEXT,
                    admin_chat_id BIGINT,
                    admin_message_id BIGINT,
                    admin_note TEXT
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_requests_user ON upload_requests (user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_upload_requests_status ON upload_requests (status);")
            # --- Audiobook support ---
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audio_books (
                    id TEXT PRIMARY KEY,
                    book_id TEXT REFERENCES books(id),
                    title TEXT,
                    display_title TEXT,
                    language TEXT,
                    performer TEXT,
                    total_duration_seconds INTEGER,
                    part_count INTEGER NOT NULL DEFAULT 0,
                    downloads INTEGER NOT NULL DEFAULT 0,
                    searches INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    uploaded_by_user_id BIGINT,
                    upload_source TEXT,
                    display_order BIGINT
                );
                """
            )
            # Note: Removed unique constraint to allow multiple audiobooks per book
            # Previously: CREATE UNIQUE INDEX IF NOT EXISTS uniq_audio_books_book_id ON audio_books (book_id) WHERE book_id IS NOT NULL;
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audio_book_parts (
                    id TEXT PRIMARY KEY,
                    audio_book_id TEXT NOT NULL REFERENCES audio_books(id) ON DELETE CASCADE,
                    part_index INTEGER NOT NULL,
                    title TEXT,
                    media_kind TEXT,
                    file_id TEXT,
                    file_unique_id TEXT,
                    path TEXT,
                    duration_seconds INTEGER,
                    channel_id BIGINT,
                    channel_message_id BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    display_order BIGINT
                );
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uniq_audio_book_part_idx
                ON audio_book_parts (audio_book_id, part_index);
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audio_book_local_download_jobs (
                    id TEXT PRIMARY KEY,
                    audio_book_id TEXT NOT NULL REFERENCES audio_books(id) ON DELETE CASCADE,
                    audio_book_part_id TEXT NOT NULL UNIQUE REFERENCES audio_book_parts(id) ON DELETE CASCADE,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    file_name TEXT,
                    media_kind TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 12,
                    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    worker_id TEXT,
                    last_error TEXT,
                    completed_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_status_next ON audio_book_local_download_jobs (status, next_attempt_at, created_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_audio_book_id ON audio_book_local_download_jobs (audio_book_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_audio_book_local_download_jobs_part_id ON audio_book_local_download_jobs (audio_book_part_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_audiobook_progress (
                    user_id BIGINT NOT NULL,
                    audio_book_id TEXT NOT NULL REFERENCES audio_books(id) ON DELETE CASCADE,
                    audio_book_part_id TEXT REFERENCES audio_book_parts(id) ON DELETE SET NULL,
                    part_index INTEGER NOT NULL DEFAULT 0,
                    completed BOOLEAN NOT NULL DEFAULT FALSE,
                    last_listened_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, audio_book_id)
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_audiobook_progress_audio ON user_audiobook_progress (audio_book_id, last_listened_at DESC);"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_audiobook_part_history (
                    user_id BIGINT NOT NULL,
                    audio_book_id TEXT NOT NULL REFERENCES audio_books(id) ON DELETE CASCADE,
                    audio_book_part_id TEXT NOT NULL REFERENCES audio_book_parts(id) ON DELETE CASCADE,
                    part_index INTEGER NOT NULL DEFAULT 0,
                    listen_count INTEGER NOT NULL DEFAULT 0,
                    last_listened_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, audio_book_part_id)
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_audiobook_part_history_user_audio ON user_audiobook_part_history (user_id, audio_book_id, last_listened_at DESC);"
            )
            # One-time index/constraint migrations
            _apply_schema_migrations(cur)
            cur.execute("ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS media_kind TEXT;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS removed_users (
                    id BIGINT,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    blocked BOOLEAN,
                    allowed BOOLEAN,
                    removed_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_task_runs (
                    id TEXT PRIMARY KEY,
                    task_key TEXT NOT NULL,
                    task_kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_by BIGINT,
                    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    summary TEXT,
                    error TEXT,
                    metadata_json TEXT
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_task_runs_started_at ON admin_task_runs (started_at DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_task_runs_task_kind ON admin_task_runs (task_kind);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_admin_task_runs_status ON admin_task_runs (status);")
            # --- Migrations: add display_order columns if they don't exist ---
            try:
                cur.execute("ALTER TABLE audio_books ADD COLUMN IF NOT EXISTS display_order BIGINT;")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS display_order BIGINT;")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_id BIGINT;")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE audio_book_parts ADD COLUMN IF NOT EXISTS channel_message_id BIGINT;")
            except Exception:
                pass


@contextmanager
def db_conn():
    global _pool
    if _pool is None:
        _create_pool()
    conn = _pool.getconn()
    try:
        # PostgreSQL may restart while the app is still running. A pooled
        # connection can look alive but fail on the first query, so ping it
        # once here and recreate the pool if needed.
        try:
            if getattr(conn, "closed", 1):
                raise psycopg2.OperationalError("connection already closed")
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
        except Exception:
            try:
                _pool.putconn(conn, close=True)
            except Exception:
                pass
            _reset_pool()
            _create_pool()
            conn = _pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
        yield conn
        conn.commit()
    except Exception:
        try:
            if getattr(conn, "closed", 1) == 0:
                conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            _pool.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass


# --- Users ---

def get_user(user_id: int):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            return cur.fetchone()


def list_users():
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users ORDER BY id")
            return cur.fetchall()


def search_users_by_name(query: str, limit: int = 30):
    query = (query or "").strip()
    if not query:
        return []
    q = f"%{query}%"
    q_prefix = f"{query}%"
    q_low = query.lower()
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if query.isdigit():
                cur.execute(
                    """
                    SELECT * FROM users
                    WHERE id = %s
                       OR username ILIKE %s
                       OR first_name ILIKE %s
                       OR last_name ILIKE %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (int(query), q, q, q, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM users
                    WHERE username ILIKE %s
                       OR first_name ILIKE %s
                       OR last_name ILIKE %s
                       OR (COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) ILIKE %s
                       OR (COALESCE(last_name,'') || ' ' || COALESCE(first_name,'')) ILIKE %s
                    ORDER BY
                        CASE
                            WHEN LOWER(username) = %s THEN 0
                            WHEN LOWER(first_name) = %s THEN 0
                            WHEN LOWER(last_name) = %s THEN 0
                            WHEN LOWER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) = %s THEN 0
                            WHEN LOWER(COALESCE(last_name,'') || ' ' || COALESCE(first_name,'')) = %s THEN 0
                            WHEN LOWER(username) LIKE %s THEN 1
                            WHEN LOWER(first_name) LIKE %s THEN 1
                            WHEN LOWER(last_name) LIKE %s THEN 1
                            WHEN LOWER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE %s THEN 1
                            WHEN LOWER(COALESCE(last_name,'') || ' ' || COALESCE(first_name,'')) LIKE %s THEN 1
                            ELSE 2
                        END,
                        id
                    LIMIT %s
                    """,
                    (
                        q,
                        q,
                        q,
                        q,
                        q,
                        q_low,
                        q_low,
                        q_low,
                        q_low,
                        q_low,
                        q_prefix.lower(),
                        q_prefix.lower(),
                        q_prefix.lower(),
                        q_prefix.lower(),
                        q_prefix.lower(),
                        limit,
                    ),
                )
            return cur.fetchall()


def upsert_user(user_id: int, username: str | None, first_name: str | None, last_name: str | None,
                blocked: bool, allowed: bool, joined_date: date | None, left_date: date | None,
                language: str | None, delete_allowed: bool = False, stopped: bool = False,
                audio_allowed: bool = False, rename_allowed: bool = False,
                language_selected: bool | None = None, group_language: str | None = None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, username, first_name, last_name, blocked, allowed, rename_allowed, joined_date, left_date, language, delete_allowed, stopped, audio_allowed, language_selected, group_language)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    blocked=EXCLUDED.blocked,
                    allowed=EXCLUDED.allowed,
                    rename_allowed=EXCLUDED.rename_allowed,
                    joined_date=EXCLUDED.joined_date,
                    left_date=EXCLUDED.left_date,
                    language=COALESCE(EXCLUDED.language, users.language),
                    delete_allowed=EXCLUDED.delete_allowed,
                    stopped=EXCLUDED.stopped,
                    audio_allowed=EXCLUDED.audio_allowed,
                    language_selected=COALESCE(EXCLUDED.language_selected, users.language_selected),
                    group_language=COALESCE(EXCLUDED.group_language, users.group_language)
                """,
                (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    blocked,
                    allowed,
                    rename_allowed,
                    joined_date,
                    left_date,
                    language,
                    delete_allowed,
                    stopped,
                    audio_allowed,
                    language_selected,
                    group_language,
                ),
            )


def update_user_language(user_id: int, lang: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET language=%s, language_selected=TRUE WHERE id=%s", (lang, user_id))


def update_user_group_language(user_id: int, lang: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET group_language=%s WHERE id=%s", (lang, user_id))


def update_user_left_date(user_id: int, left_date: date | None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET left_date=%s WHERE id=%s", (left_date, user_id))


def set_user_blocked(user_id: int, blocked: bool):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET blocked=%s WHERE id=%s", (blocked, user_id))


def set_user_allowed(user_id: int, allowed: bool):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET allowed=%s WHERE id=%s", (allowed, user_id))


def set_user_delete_allowed(user_id: int, allowed: bool):
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, delete_allowed, stopped, audio_allowed)
                VALUES (%s, FALSE, FALSE, %s, %s, FALSE, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    delete_allowed = EXCLUDED.delete_allowed
                """,
                (user_id, today, allowed),
            )


def is_user_delete_allowed(user_id: int) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT delete_allowed FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def set_user_rename_allowed(user_id: int, allowed: bool):
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, rename_allowed, delete_allowed, stopped, audio_allowed)
                VALUES (%s, FALSE, FALSE, %s, %s, FALSE, FALSE, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    rename_allowed = EXCLUDED.rename_allowed
                """,
                (user_id, today, allowed),
            )


def is_user_rename_allowed(user_id: int) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT rename_allowed FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def set_user_audio_allowed(user_id: int, allowed: bool):
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, delete_allowed, stopped, audio_allowed)
                VALUES (%s, FALSE, FALSE, %s, FALSE, FALSE, %s)
                ON CONFLICT (id) DO UPDATE SET
                    audio_allowed = EXCLUDED.audio_allowed
                """,
                (user_id, today, allowed),
            )


def is_user_audio_allowed(user_id: int) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT audio_allowed FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def set_user_stopped(user_id: int, stopped: bool):
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, stopped, delete_allowed, audio_allowed)
                VALUES (%s, FALSE, FALSE, %s, %s, FALSE, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    stopped = EXCLUDED.stopped
                """,
                (user_id, today, stopped),
            )


def is_user_stopped(user_id: int) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT stopped FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def add_user_coin_adjustment(user_id: int, delta: int) -> int:
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, coin_adjustment, delete_allowed, stopped)
                VALUES (%s, FALSE, FALSE, %s, %s, FALSE, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    coin_adjustment = COALESCE(users.coin_adjustment, 0) + EXCLUDED.coin_adjustment
                RETURNING coin_adjustment
                """,
                (user_id, today, delta),
            )
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def set_user_coin_adjustment(user_id: int, value: int) -> int:
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (id, blocked, allowed, joined_date, coin_adjustment, delete_allowed, stopped)
                VALUES (%s, FALSE, FALSE, %s, %s, FALSE, FALSE)
                ON CONFLICT (id) DO UPDATE SET
                    coin_adjustment = EXCLUDED.coin_adjustment
                RETURNING coin_adjustment
                """,
                (user_id, today, value),
            )
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_user_coin_adjustment(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT coin_adjustment FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def set_user_referrer(user_id: int, referrer_id: int) -> bool:
    if not user_id or not referrer_id or user_id == referrer_id:
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT referrer_id FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return False
            if row[0] is not None:
                return False
            cur.execute("SELECT 1 FROM users WHERE id=%s", (referrer_id,))
            if cur.fetchone() is None:
                return False
            cur.execute(
                """
                SELECT
                    COALESCE((SELECT SUM(searches) FROM analytics_daily_users WHERE user_id=%s), 0) AS searches,
                    COALESCE((SELECT SUM(count) FROM user_recents WHERE user_id=%s), 0) AS downloads,
                    COALESCE((SELECT COUNT(*) FROM user_favorites WHERE user_id=%s), 0) AS favorites,
                    COALESCE((SELECT COUNT(*) FROM book_reactions WHERE user_id=%s), 0) AS reactions
                """,
                (user_id, user_id, user_id, user_id),
            )
            stats = cur.fetchone()
            if stats and any(int(v or 0) > 0 for v in stats):
                return False
            cur.execute(
                "UPDATE users SET referrer_id=%s, referred_at=NOW() WHERE id=%s",
                (referrer_id, user_id),
            )
            return cur.rowcount > 0


def get_user_reaction_count(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM book_reactions WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_user_referrals_count(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users WHERE referrer_id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def delete_users_by_ids(user_ids: Iterable[int]):
    ids = list(user_ids)
    if not ids:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = ANY(%s)", (ids,))
            return cur.rowcount


def insert_removed_users(rows: list[dict]):
    if not rows:
        return 0
    values = [
        (r.get("id"), r.get("username"), r.get("first_name"), r.get("last_name"), r.get("blocked"), r.get("allowed"))
        for r in rows
    ]
    with db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO removed_users (id, username, first_name, last_name, blocked, allowed)
                VALUES %s
                """,
                values,
            )
            return len(values)


def insert_admin_task_run(
    task_key: str,
    task_kind: str,
    started_by: int | None = None,
    status: str = "running",
    metadata: dict | None = None,
    task_id: str | None = None,
):
    run_id = str(task_id or uuid.uuid4().hex)
    meta_json = None
    if metadata is not None:
        try:
            meta_json = json.dumps(metadata, ensure_ascii=False)
        except Exception:
            meta_json = None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin_task_runs (
                    id, task_key, task_kind, status, started_by, started_at, metadata_json
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (run_id, str(task_key), str(task_kind), str(status or "running"), started_by, meta_json),
            )
    return run_id


def update_admin_task_run(
    task_id: str,
    *,
    status: str | None = None,
    summary: str | None = None,
    error: str | None = None,
    metadata: dict | None = None,
    finished_at: datetime | None = None,
):
    if not task_id:
        return 0
    set_parts: list[str] = []
    values: list = []
    if status is not None:
        set_parts.append("status=%s")
        values.append(str(status))
    if summary is not None:
        set_parts.append("summary=%s")
        values.append(str(summary)[:4000])
    if error is not None:
        set_parts.append("error=%s")
        values.append(str(error)[:4000])
    if metadata is not None:
        try:
            meta_json = json.dumps(metadata, ensure_ascii=False)
        except Exception:
            meta_json = None
        set_parts.append("metadata_json=%s")
        values.append(meta_json)
    if finished_at is not None:
        set_parts.append("finished_at=%s")
        values.append(finished_at)
    if not set_parts:
        return 0
    values.append(str(task_id))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE admin_task_runs SET {', '.join(set_parts)} WHERE id=%s",
                values,
            )
            return cur.rowcount


def list_admin_task_runs(limit: int = 30, task_kind: str | None = None):
    limit = max(1, min(int(limit or 30), 200))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if task_kind:
                cur.execute(
                    """
                    SELECT *
                    FROM admin_task_runs
                    WHERE task_kind=%s
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (str(task_kind), limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM admin_task_runs
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall() or []

    out = []
    for row in rows:
        item = dict(row)
        meta = item.get("metadata_json")
        if isinstance(meta, str) and meta:
            try:
                item["metadata"] = json.loads(meta)
            except Exception:
                item["metadata"] = None
        else:
            item["metadata"] = None
        out.append(item)
    return out


# --- Favorites & Recents ---

def add_favorite(user_id: int, book_id: str, title: str, max_favorites: int):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_favorites (user_id, book_id, title, ts)
                VALUES (%s,%s,%s,NOW())
                ON CONFLICT (user_id, book_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    ts=EXCLUDED.ts
                """,
                (user_id, book_id, title),
            )
            # trim older
            cur.execute(
                """
                DELETE FROM user_favorites
                WHERE user_id=%s AND book_id IN (
                    SELECT book_id FROM user_favorites
                    WHERE user_id=%s
                    ORDER BY ts DESC
                    OFFSET %s
                )
                """,
                (user_id, user_id, max_favorites),
            )
    return True


def remove_favorite(user_id: int, book_id: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_favorites WHERE user_id=%s AND book_id=%s", (user_id, book_id))
            return cur.rowcount > 0


def is_favorited(user_id: int, book_id: str) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM user_favorites WHERE user_id=%s AND book_id=%s", (user_id, book_id))
            return cur.fetchone() is not None


def list_favorites(user_id: int):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    f.book_id as id,
                    COALESCE(b.book_name, f.title) AS title,
                    f.ts
                FROM user_favorites f
                LEFT JOIN books b ON b.id = f.book_id
                WHERE f.user_id=%s
                ORDER BY f.ts DESC
                """,
                (user_id,),
            )
            return cur.fetchall()


def award_favorite_action(user_id: int, book_id: str) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_favorite_awards (user_id, book_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id, book_id) DO NOTHING
                """,
                (user_id, book_id),
            )
            return cur.rowcount > 0


def get_user_favorite_awards_count(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_favorite_awards WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def add_recent(user_id: int, book_id: str, title: str, max_recents: int):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_recents (user_id, book_id, title, ts, count, last_ts)
                VALUES (%s,%s,%s,NOW(),1,NOW())
                ON CONFLICT (user_id, book_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    ts=EXCLUDED.ts,
                    count=user_recents.count + 1,
                    last_ts=NOW()
                """,
                (user_id, book_id, title),
            )
            cur.execute(
                """
                DELETE FROM user_recents
                WHERE user_id=%s AND book_id IN (
                    SELECT book_id FROM user_recents
                    WHERE user_id=%s
                    ORDER BY ts DESC
                    OFFSET %s
                )
                """,
                (user_id, user_id, max_recents),
            )
    return True


# --- Analytics ---

def increment_analytics(key: str, amount: int = 1):
    valid_keys = {"searches", "buttons"}
    if key not in valid_keys:
        return 0
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_daily (day, searches, buttons)
                VALUES (%s, %s, %s)
                ON CONFLICT (day) DO UPDATE SET
                    searches = analytics_daily.searches + EXCLUDED.searches,
                    buttons = analytics_daily.buttons + EXCLUDED.buttons
                """,
                (today, amount if key == "searches" else 0, amount if key == "buttons" else 0),
            )
            cur.execute("SELECT searches, buttons FROM analytics_daily WHERE day=%s", (today,))
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0) if key == "searches" else int(row[1] or 0)


def increment_user_analytics(user_id: int, key: str, amount: int = 1):
    valid_keys = {"searches", "buttons"}
    if key not in valid_keys:
        return 0
    today = date.today()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_daily_users (day, user_id, searches, buttons)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (day, user_id) DO UPDATE SET
                    searches = analytics_daily_users.searches + EXCLUDED.searches,
                    buttons = analytics_daily_users.buttons + EXCLUDED.buttons
                """,
                (today, user_id, amount if key == "searches" else 0, amount if key == "buttons" else 0),
            )
            cur.execute(
                "SELECT searches, buttons FROM analytics_daily_users WHERE day=%s AND user_id=%s",
                (today, user_id),
            )
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0) if key == "searches" else int(row[1] or 0)


def get_analytics_map():
    data: dict[str, dict] = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT day, searches, buttons FROM analytics_daily")
            for day, searches, buttons in cur.fetchall():
                total_searches = int(searches or 0)
                total_downloads = int(buttons or 0)
                data[str(day)] = {
                    "searches": total_searches,
                    "buttons": total_downloads,
                    "book_searches": total_searches,
                    "book_downloads": total_downloads,
                }
    return data


def increment_counter(key: str, amount: int = 1) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analytics_counters (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    value = analytics_counters.value + EXCLUDED.value
                RETURNING value
                """,
                (key, amount),
            )
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_counters(keys: list[str] | None = None) -> dict[str, int]:
    data: dict[str, int] = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            if keys:
                cur.execute(
                    "SELECT key, value FROM analytics_counters WHERE key = ANY(%s)",
                    (keys,),
                )
            else:
                cur.execute("SELECT key, value FROM analytics_counters")
            for key, value in cur.fetchall():
                data[str(key)] = int(value or 0)
    return data


def get_daily_analytics(day: date):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT searches, buttons FROM analytics_daily WHERE day=%s", (day,))
            row = cur.fetchone()
            if not row:
                return {"searches": 0, "downloads": 0, "book_searches": 0, "book_downloads": 0}
            total_searches = int(row[0] or 0)
            total_downloads = int(row[1] or 0)
            return {
                "searches": total_searches,
                "downloads": total_downloads,
                "book_searches": total_searches,
                "book_downloads": total_downloads,
            }


def upsert_guest_group(
    chat_id: int,
    chat_type: str | None = None,
    title: str | None = None,
    username: str | None = None,
    public_link: str | None = None,
    last_query: str | None = None,
    increment_searches: bool = False,
) -> dict[str, Any]:
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        safe_chat_id = 0
    if not safe_chat_id:
        return {}

    safe_chat_type = str(chat_type or "").strip()
    safe_title = str(title or "").strip()
    safe_username = str(username or "").strip().lstrip("@")
    safe_public_link = str(public_link or "").strip()
    safe_last_query = str(last_query or "").strip()
    inc = 1 if increment_searches else 0

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO guest_groups (
                    chat_id,
                    chat_type,
                    title,
                    username,
                    public_link,
                    last_query,
                    searches,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (chat_id) DO UPDATE SET
                    chat_type = COALESCE(NULLIF(EXCLUDED.chat_type, ''), guest_groups.chat_type),
                    title = COALESCE(NULLIF(EXCLUDED.title, ''), guest_groups.title),
                    username = COALESCE(NULLIF(EXCLUDED.username, ''), guest_groups.username),
                    public_link = COALESCE(NULLIF(EXCLUDED.public_link, ''), guest_groups.public_link),
                    last_query = COALESCE(NULLIF(EXCLUDED.last_query, ''), guest_groups.last_query),
                    searches = guest_groups.searches + %s,
                    last_seen_at = NOW()
                RETURNING
                    chat_id,
                    chat_type,
                    title,
                    username,
                    public_link,
                    last_query,
                    searches,
                    first_seen_at,
                    last_seen_at
                """,
                (
                    safe_chat_id,
                    safe_chat_type,
                    safe_title,
                    safe_username,
                    safe_public_link,
                    safe_last_query,
                    inc,
                    inc,
                ),
            )
            row = cur.fetchone()
            return dict(row or {})


def get_guest_group_audit_stats(limit: int = 5) -> dict[str, Any]:
    safe_limit = max(1, min(20, int(limit or 5)))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_groups,
                    COALESCE(SUM(searches), 0) AS total_group_searches
                FROM guest_groups
                WHERE chat_type IN ('group', 'supergroup')
                """
            )
            summary = cur.fetchone() or {}
            cur.execute(
                """
                SELECT
                    chat_id,
                    chat_type,
                    title,
                    username,
                    public_link,
                    last_query,
                    searches,
                    first_seen_at,
                    last_seen_at
                FROM guest_groups
                WHERE chat_type IN ('group', 'supergroup')
                ORDER BY last_seen_at DESC, searches DESC, chat_id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            groups = [dict(row or {}) for row in (cur.fetchall() or [])]
            return {
                "total_groups": int((summary or {}).get("total_groups") or 0),
                "total_group_searches": int((summary or {}).get("total_group_searches") or 0),
                "groups": groups,
            }


def record_guest_user_activity(
    user_id: int | None,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    chat_id: int | None = None,
    chat_type: str | None = None,
    group_title: str | None = None,
    group_username: str | None = None,
    public_link: str | None = None,
    query_text: str | None = None,
) -> dict[str, Any]:
    try:
        safe_user_id = int(user_id or 0) or None
    except Exception:
        safe_user_id = None
    try:
        safe_chat_id = int(chat_id or 0) or None
    except Exception:
        safe_chat_id = None

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO guest_user_activity (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    chat_id,
                    chat_type,
                    group_title,
                    group_username,
                    public_link,
                    query_text,
                    searched_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING
                    id,
                    user_id,
                    username,
                    first_name,
                    last_name,
                    chat_id,
                    chat_type,
                    group_title,
                    group_username,
                    public_link,
                    query_text,
                    searched_at
                """,
                (
                    safe_user_id,
                    str(username or "").strip().lstrip("@"),
                    str(first_name or "").strip(),
                    str(last_name or "").strip(),
                    safe_chat_id,
                    str(chat_type or "").strip(),
                    str(group_title or "").strip(),
                    str(group_username or "").strip().lstrip("@"),
                    str(public_link or "").strip(),
                    str(query_text or "").strip(),
                ),
            )
            row = cur.fetchone()
            return dict(row or {})


def get_guest_user_audit_stats(limit: int = 10) -> dict[str, Any]:
    safe_limit = max(1, min(25, int(limit or 10)))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_searches,
                    COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS total_users
                FROM guest_user_activity
                """
            )
            summary = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    first_name,
                    last_name,
                    COUNT(*) AS searches,
                    MAX(searched_at) AS last_seen_at
                FROM guest_user_activity
                WHERE user_id IS NOT NULL
                GROUP BY user_id, username, first_name, last_name
                ORDER BY searches DESC, last_seen_at DESC, user_id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            top_users = [dict(row or {}) for row in (cur.fetchall() or [])]
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    first_name,
                    last_name,
                    chat_id,
                    chat_type,
                    group_title,
                    group_username,
                    public_link,
                    query_text,
                    searched_at
                FROM guest_user_activity
                ORDER BY searched_at DESC, id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            recent_searches = [dict(row or {}) for row in (cur.fetchall() or [])]
            return {
                "total_searches": int(summary.get("total_searches") or 0),
                "total_users": int(summary.get("total_users") or 0),
                "top_users": top_users,
                "recent_searches": recent_searches,
            }


def record_inline_search_activity(
    user_id: int | None,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    query_text: str | None = None,
) -> dict[str, Any]:
    try:
        safe_user_id = int(user_id or 0) or None
    except Exception:
        safe_user_id = None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO inline_search_activity (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    query_text,
                    searched_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING id, user_id, username, first_name, last_name, query_text, searched_at
                """,
                (
                    safe_user_id,
                    str(username or "").strip().lstrip("@"),
                    str(first_name or "").strip(),
                    str(last_name or "").strip(),
                    str(query_text or "").strip(),
                ),
            )
            row = cur.fetchone()
            return dict(row or {})


def record_inline_chosen_activity(
    user_id: int | None,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    query_text: str | None = None,
    result_id: str | None = None,
) -> dict[str, Any]:
    try:
        safe_user_id = int(user_id or 0) or None
    except Exception:
        safe_user_id = None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO inline_chosen_activity (
                    user_id,
                    username,
                    first_name,
                    last_name,
                    query_text,
                    result_id,
                    chosen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING id, user_id, username, first_name, last_name, query_text, result_id, chosen_at
                """,
                (
                    safe_user_id,
                    str(username or "").strip().lstrip("@"),
                    str(first_name or "").strip(),
                    str(last_name or "").strip(),
                    str(query_text or "").strip(),
                    str(result_id or "").strip(),
                ),
            )
            row = cur.fetchone()
            return dict(row or {})


def get_inline_audit_stats(limit: int = 10) -> dict[str, Any]:
    safe_limit = max(1, min(25, int(limit or 10)))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM inline_search_activity) AS total_searches,
                    (SELECT COUNT(DISTINCT user_id) FROM inline_search_activity WHERE user_id IS NOT NULL) AS total_users,
                    (SELECT COUNT(*) FROM inline_chosen_activity) AS total_choices
                """
            )
            summary = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    first_name,
                    last_name,
                    COUNT(*) AS searches,
                    MAX(searched_at) AS last_seen_at
                FROM inline_search_activity
                WHERE user_id IS NOT NULL
                GROUP BY user_id, username, first_name, last_name
                ORDER BY searches DESC, last_seen_at DESC, user_id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            top_users = [dict(row or {}) for row in (cur.fetchall() or [])]
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    first_name,
                    last_name,
                    query_text,
                    searched_at
                FROM inline_search_activity
                ORDER BY searched_at DESC, id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            recent_searches = [dict(row or {}) for row in (cur.fetchall() or [])]
            cur.execute(
                """
                SELECT
                    user_id,
                    username,
                    first_name,
                    last_name,
                    query_text,
                    result_id,
                    chosen_at
                FROM inline_chosen_activity
                ORDER BY chosen_at DESC, id DESC
                LIMIT %s
                """,
                (safe_limit,),
            )
            recent_choices = [dict(row or {}) for row in (cur.fetchall() or [])]
            return {
                "total_searches": int(summary.get("total_searches") or 0),
                "total_users": int(summary.get("total_users") or 0),
                "total_choices": int(summary.get("total_choices") or 0),
                "top_users": top_users,
                "recent_searches": recent_searches,
                "recent_choices": recent_choices,
            }


def create_guest_private_handoff(
    handoff_type: str,
    creator_user_id: int | None = None,
    query_text: str | None = None,
    book_id: str | None = None,
    source_chat_id: int | None = None,
    source_chat_type: str | None = None,
    source_chat_title: str | None = None,
    source_chat_username: str | None = None,
    source_public_link: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    safe_type = str(handoff_type or "query").strip().lower() or "query"
    if safe_type not in {"query", "book"}:
        safe_type = "query"
    safe_token = str(token or uuid.uuid4().hex[:12]).strip()
    if not safe_token:
        safe_token = uuid.uuid4().hex[:12]
    try:
        safe_creator_user_id = int(creator_user_id or 0) or None
    except Exception:
        safe_creator_user_id = None
    try:
        safe_source_chat_id = int(source_chat_id or 0) or None
    except Exception:
        safe_source_chat_id = None
    safe_query_text = str(query_text or "").strip()
    safe_book_id = str(book_id or "").strip()
    safe_source_chat_type = str(source_chat_type or "").strip().lower()
    safe_source_chat_title = str(source_chat_title or "").strip()
    safe_source_chat_username = str(source_chat_username or "").strip().lstrip("@")
    safe_source_public_link = str(source_public_link or "").strip()

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO guest_private_handoffs (
                    token,
                    handoff_type,
                    creator_user_id,
                    query_text,
                    book_id,
                    source_chat_id,
                    source_chat_type,
                    source_chat_title,
                    source_chat_username,
                    source_public_link,
                    created_at,
                    last_used_at,
                    use_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NULL, 0)
                ON CONFLICT (token) DO UPDATE SET
                    handoff_type = EXCLUDED.handoff_type,
                    creator_user_id = COALESCE(EXCLUDED.creator_user_id, guest_private_handoffs.creator_user_id),
                    query_text = COALESCE(NULLIF(EXCLUDED.query_text, ''), guest_private_handoffs.query_text),
                    book_id = COALESCE(NULLIF(EXCLUDED.book_id, ''), guest_private_handoffs.book_id),
                    source_chat_id = COALESCE(EXCLUDED.source_chat_id, guest_private_handoffs.source_chat_id),
                    source_chat_type = COALESCE(NULLIF(EXCLUDED.source_chat_type, ''), guest_private_handoffs.source_chat_type),
                    source_chat_title = COALESCE(NULLIF(EXCLUDED.source_chat_title, ''), guest_private_handoffs.source_chat_title),
                    source_chat_username = COALESCE(NULLIF(EXCLUDED.source_chat_username, ''), guest_private_handoffs.source_chat_username),
                    source_public_link = COALESCE(NULLIF(EXCLUDED.source_public_link, ''), guest_private_handoffs.source_public_link)
                RETURNING token, handoff_type, creator_user_id, query_text, book_id, source_chat_id, source_chat_type, source_chat_title, source_chat_username, source_public_link, created_at, last_used_at, use_count
                """,
                (
                    safe_token,
                    safe_type,
                    safe_creator_user_id,
                    safe_query_text,
                    safe_book_id,
                    safe_source_chat_id,
                    safe_source_chat_type,
                    safe_source_chat_title,
                    safe_source_chat_username,
                    safe_source_public_link,
                ),
            )
            row = cur.fetchone()
            return dict(row or {})


def get_guest_private_handoff_by_token(token: str) -> dict[str, Any] | None:
    safe_token = str(token or "").strip()
    if not safe_token:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT token, handoff_type, creator_user_id, query_text, book_id, source_chat_id, source_chat_type, source_chat_title, source_chat_username, source_public_link, created_at, last_used_at, use_count
                FROM guest_private_handoffs
                WHERE token = %s
                LIMIT 1
                """,
                (safe_token,),
            )
            row = cur.fetchone()
            return dict(row or {}) if row else None


def touch_guest_private_handoff(token: str) -> dict[str, Any] | None:
    safe_token = str(token or "").strip()
    if not safe_token:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE guest_private_handoffs
                SET last_used_at = NOW(),
                    use_count = use_count + 1
                WHERE token = %s
                RETURNING token, handoff_type, creator_user_id, query_text, book_id, source_chat_id, source_chat_type, source_chat_title, source_chat_username, source_public_link, created_at, last_used_at, use_count
                """,
                (safe_token,),
            )
            row = cur.fetchone()
            return dict(row or {}) if row else None


def get_guest_group_delivery_capability(chat_id: int) -> dict[str, Any]:
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        safe_chat_id = 0
    if not safe_chat_id:
        return {
            "chat_id": 0,
            "guest_doc_forbidden_count": 0,
            "guest_doc_success_count": 0,
            "guest_doc_skip_until": None,
            "guest_doc_last_forbidden_at": None,
            "guest_doc_last_success_at": None,
            "skip_same_chat_delivery": False,
        }
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    chat_id,
                    guest_doc_forbidden_count,
                    guest_doc_success_count,
                    guest_doc_skip_until,
                    guest_doc_last_forbidden_at,
                    guest_doc_last_success_at,
                    CASE
                        WHEN guest_doc_skip_until IS NOT NULL AND guest_doc_skip_until > NOW() THEN TRUE
                        ELSE FALSE
                    END AS skip_same_chat_delivery
                FROM guest_groups
                WHERE chat_id = %s
                LIMIT 1
                """,
                (safe_chat_id,),
            )
            row = cur.fetchone()
            return dict(row or {
                "chat_id": safe_chat_id,
                "guest_doc_forbidden_count": 0,
                "guest_doc_success_count": 0,
                "guest_doc_skip_until": None,
                "guest_doc_last_forbidden_at": None,
                "guest_doc_last_success_at": None,
                "skip_same_chat_delivery": False,
            })


def mark_guest_group_delivery_forbidden(chat_id: int) -> dict[str, Any]:
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        safe_chat_id = 0
    if not safe_chat_id:
        return {}
    threshold = _guest_docs_forbidden_threshold()
    skip_minutes = _guest_docs_skip_minutes()
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO guest_groups (
                    chat_id,
                    first_seen_at,
                    last_seen_at,
                    searches
                )
                VALUES (%s, NOW(), NOW(), 0)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                (safe_chat_id,),
            )
            cur.execute(
                """
                UPDATE guest_groups
                SET guest_doc_forbidden_count = guest_doc_forbidden_count + 1,
                    guest_doc_last_forbidden_at = NOW(),
                    guest_doc_skip_until = CASE
                        WHEN guest_doc_forbidden_count + 1 >= %s
                            THEN NOW() + (%s * INTERVAL '1 minute')
                        ELSE guest_doc_skip_until
                    END,
                    last_seen_at = NOW()
                WHERE chat_id = %s
                RETURNING
                    chat_id,
                    guest_doc_forbidden_count,
                    guest_doc_success_count,
                    guest_doc_skip_until,
                    guest_doc_last_forbidden_at,
                    guest_doc_last_success_at,
                    CASE
                        WHEN guest_doc_skip_until IS NOT NULL AND guest_doc_skip_until > NOW() THEN TRUE
                        ELSE FALSE
                    END AS skip_same_chat_delivery
                """,
                (threshold, skip_minutes, safe_chat_id),
            )
            row = cur.fetchone()
            return dict(row or {})


def mark_guest_group_delivery_success(chat_id: int) -> dict[str, Any]:
    try:
        safe_chat_id = int(chat_id or 0)
    except Exception:
        safe_chat_id = 0
    if not safe_chat_id:
        return {}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO guest_groups (
                    chat_id,
                    first_seen_at,
                    last_seen_at,
                    searches
                )
                VALUES (%s, NOW(), NOW(), 0)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                (safe_chat_id,),
            )
            cur.execute(
                """
                UPDATE guest_groups
                SET guest_doc_success_count = guest_doc_success_count + 1,
                    guest_doc_last_success_at = NOW(),
                    guest_doc_forbidden_count = 0,
                    guest_doc_skip_until = NULL,
                    last_seen_at = NOW()
                WHERE chat_id = %s
                RETURNING
                    chat_id,
                    guest_doc_forbidden_count,
                    guest_doc_success_count,
                    guest_doc_skip_until,
                    guest_doc_last_forbidden_at,
                    guest_doc_last_success_at,
                    FALSE AS skip_same_chat_delivery
                """,
                (safe_chat_id,),
            )
            row = cur.fetchone()
            return dict(row or {})


def backfill_counters_if_empty() -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM analytics_counters")
            row = cur.fetchone()
            if row and int(row[0] or 0) > 0:
                return False

            cur.execute("SELECT COALESCE(SUM(searches),0), COALESCE(SUM(downloads),0) FROM books")
            search_total, download_total = cur.fetchone() or (0, 0)

            cur.execute("SELECT COUNT(*) FROM user_favorites")
            fav_total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute("SELECT COUNT(*) FROM book_requests")
            req_total = int((cur.fetchone() or [0])[0] or 0)

            cur.execute("SELECT status, COUNT(*) FROM book_requests GROUP BY status")
            req_status = {str(s or "unknown"): int(c or 0) for s, c in cur.fetchall()}

            cur.execute("SELECT status, COUNT(*) FROM upload_requests GROUP BY status")
            upload_status = {str(s or "unknown"): int(c or 0) for s, c in cur.fetchall()}

            cur.execute("SELECT reaction, COUNT(*) FROM book_reactions GROUP BY reaction")
            react_counts = {str(r): int(c or 0) for r, c in cur.fetchall()}

            counters = {
                "search_total": int(search_total or 0),
                "download_total": int(download_total or 0),
                "favorite_added": fav_total,
                "favorite_removed": 0,
                "request_created": req_total,
                "request_cancelled": 0,
                "request_seen": req_status.get("seen", 0),
                "request_done": req_status.get("done", 0),
                "request_no": req_status.get("no", 0),
                "upload_accept": upload_status.get("accept", 0),
                "upload_reject": upload_status.get("reject", 0),
                "reaction_like": react_counts.get("like", 0),
                "reaction_dislike": react_counts.get("dislike", 0),
                "reaction_berry": react_counts.get("berry", 0),
                "reaction_whale": react_counts.get("whale", 0),
            }

            for key, value in counters.items():
                cur.execute(
                    "INSERT INTO analytics_counters (key, value) VALUES (%s, %s)",
                    (key, int(value)),
                )
    return True


def backfill_user_awards_if_empty() -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_favorite_awards")
            fav_awards = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM user_reaction_awards")
            react_awards = int((cur.fetchone() or [0])[0] or 0)
            if fav_awards > 0 or react_awards > 0:
                return False
            cur.execute(
                """
                INSERT INTO user_favorite_awards (user_id, book_id, ts)
                SELECT user_id, book_id, MIN(ts)
                FROM user_favorites
                GROUP BY user_id, book_id
                ON CONFLICT (user_id, book_id) DO NOTHING
                """
            )
            cur.execute(
                """
                INSERT INTO user_reaction_awards (user_id, book_id, ts)
                SELECT user_id, book_id, MIN(ts)
                FROM book_reactions
                GROUP BY user_id, book_id
                ON CONFLICT (user_id, book_id) DO NOTHING
                """
            )
            return True


def get_db_stats():
    stats = {"ok": False, "error": None, "counts": {}}
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                stats["ok"] = True
                tables = [
                    "users",
                    "books",
                    "user_favorites",
                    "user_recents",
                    "analytics_daily",
                    "book_requests",
                    "upload_requests",
                    "removed_users",
                ]
                for t in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    stats["counts"][t] = int(cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM books WHERE indexed = TRUE")
                stats["counts"]["books_indexed"] = int(cur.fetchone()[0])
    except Exception as e:
        stats["error"] = str(e)
    return stats


def ping_db():
    stats = {"ok": False, "error": None, "database": None}
    if psycopg2 is None:
        stats["error"] = "psycopg2 unavailable"
        return stats
    conn = None
    try:
        params = dict(_dsn())
        params["connect_timeout"] = 3
        conn = psycopg2.connect(**params)
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            row = cur.fetchone()
            stats["database"] = row[0] if row else None
        stats["ok"] = True
    except Exception as e:
        stats["error"] = str(e)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
    return stats


# --- Books ---

def list_books():
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM books")
            return cur.fetchall()


def search_books_for_white_label_fallback(query: str, limit: int = 50):
    """Limited DB fallback for connected bots; do not scan the full catalog in Python."""
    text = re.sub(r"\s+", " ", str(query or "").strip())
    if not text:
        return []
    try:
        safe_limit = max(1, min(200, int(limit or 50)))
    except Exception:
        safe_limit = 50
    tokens: list[str] = []
    seen: set[str] = set()
    for candidate in [text, *re.split(r"\s+", text)]:
        clean = str(candidate or "").strip()
        if len(clean) < 2 or clean.lower() in seen:
            continue
        seen.add(clean.lower())
        tokens.append(clean)
        if len(tokens) >= 6:
            break
    if not tokens:
        return []
    where_parts: list[str] = []
    params: list[object] = []
    for token in tokens:
        pattern = f"%{token}%"
        where_parts.append("(book_name ILIKE %s OR display_name ILIKE %s OR path ILIKE %s)")
        params.extend([pattern, pattern, pattern])
    params.append(safe_limit)
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT *
                FROM books
                WHERE COALESCE(white_label_enabled, TRUE) = TRUE
                  AND ((file_id IS NOT NULL AND file_id <> '') OR (path IS NOT NULL AND path <> ''))
                  AND (path IS NULL OR path = '' OR LOWER(path) LIKE '%%.pdf')
                  AND ({' OR '.join(where_parts)})
                ORDER BY searches DESC, downloads DESC, created_at DESC NULLS LAST, display_name ASC NULLS LAST
                LIMIT %s
                """,
                params,
            )
            return cur.fetchall()


def update_book_white_label_enabled(book_id: str, enabled: bool):
    clean_id = str(book_id or "").strip()
    if not clean_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE books SET white_label_enabled=%s WHERE id=%s", (bool(enabled), clean_id))
            return cur.rowcount


def get_random_book(require_accessible: bool = True):
    rows = get_random_books(limit=1, require_accessible=require_accessible)
    return rows[0] if rows else None


def get_random_books(limit: int = 10, require_accessible: bool = True):
    try:
        safe_limit = max(1, min(50, int(limit or 10)))
    except Exception:
        safe_limit = 10
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if require_accessible:
                cur.execute(
                    """
                    SELECT *
                    FROM books
                    WHERE (file_id IS NOT NULL AND file_id <> '')
                       OR (path IS NOT NULL AND path <> '')
                    ORDER BY RANDOM()
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM books
                    ORDER BY RANDOM()
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
            return cur.fetchall()

def get_book_by_name(book_name: str):
    def _name_variants(raw_name: str) -> list[str]:
        name = str(raw_name or "").strip()
        if not name:
            return []
        variants: list[str] = []
        for candidate in (
            name,
            name.replace("ʻ", ""),
            name.replace("’", ""),
            name.replace("ʼ", ""),
            name.replace("'", ""),
        ):
            candidate = str(candidate or "").strip()
            if candidate and candidate not in variants:
                variants.append(candidate)
        return variants

    variants = _name_variants(book_name)
    if not variants:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for candidate in variants:
                cur.execute(
                    """
                    SELECT
                        b.*,
                        j.status AS local_backup_status,
                        j.attempts AS local_backup_attempts,
                        j.max_attempts AS local_backup_max_attempts,
                        j.last_error AS local_backup_error,
                        j.next_attempt_at AS local_backup_next_attempt_at,
                        j.updated_at AS local_backup_updated_at,
                        j.completed_at AS local_backup_completed_at
                    FROM books b
                    LEFT JOIN book_local_download_jobs j ON j.book_id = b.id
                    WHERE b.book_name=%s
                    """,
                    (candidate,),
                )
                row = cur.fetchone()
                if row:
                    return row
            return None

def get_book_by_file_unique_id(file_unique_id: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.*,
                    j.status AS local_backup_status,
                    j.attempts AS local_backup_attempts,
                    j.max_attempts AS local_backup_max_attempts,
                    j.last_error AS local_backup_error,
                    j.next_attempt_at AS local_backup_next_attempt_at,
                    j.updated_at AS local_backup_updated_at,
                    j.completed_at AS local_backup_completed_at
                FROM books b
                LEFT JOIN book_local_download_jobs j ON j.book_id = b.id
                WHERE b.file_unique_id=%s
                """,
                (file_unique_id,),
            )
            return cur.fetchone()

VARIANT_TOKENS = {
    "english", "eng", "en",
    "russian", "rus", "russia",
    "uzbek", "ozbek", "uzbekcha", "ozbekcha", "uzb", "ozb",
    "first", "second", "third", "fourth", "fifth", "fivth", "sixth",
}


_BOOK_ADULT_FILTER_PATTERNS = (
    re.compile(r"(?<!\d)18\s*\+"),
    re.compile(r"\b18\s*yosh\b"),
    re.compile(r"\b18\s*plus\b"),
    re.compile(r"\badult\b"),
    re.compile(r"\bnsfw\b"),
    re.compile(r"\bporn\w*\b"),
    re.compile(r"\berot\w*\b"),
    re.compile(r"\bsex\b"),
    re.compile(r"\bseks\b"),
    re.compile(r"\bxxx\b"),
    re.compile(r"\bhentai\b"),
    re.compile(r"\bonlyfans\b"),
    re.compile(r"\bэрот\w*\b"),
    re.compile(r"\bпорн\w*\b"),
    re.compile(r"\bсекс\w*\b"),
    re.compile(r"\bjinsiy\b"),
    re.compile(r"\bvoyaga\s+yetgan\w*\b"),
)


def _extra_book_adult_keywords() -> list[str]:
    raw = str(os.getenv("BOOK_ADULT_FILTER_KEYWORDS", "") or "").strip()
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        token = str(item or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _book_filter_haystack(parts: list[Any]) -> str:
    base = " ".join(str(p or "") for p in parts).lower()
    base = re.sub(r"[\s_\-./|\\:;,(){}\[\]<>]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def is_book_adult_marked(book: dict | None = None, text: str | None = None) -> bool:
    parts: list[Any] = []
    if text:
        parts.append(text)
    if isinstance(book, dict):
        parts.extend(
            [
                book.get("book_name"),
                book.get("display_name"),
                book.get("file_name"),
                book.get("caption_text"),
                book.get("search_text"),
                book.get("path"),
            ]
        )
    haystack = _book_filter_haystack(parts)
    if not haystack:
        return False
    for pattern in _BOOK_ADULT_FILTER_PATTERNS:
        if pattern.search(haystack):
            return True
    for token in _extra_book_adult_keywords():
        if token in haystack:
            return True
    return False


def _name_allows_duplicates(book_name: str) -> bool:
    if not book_name:
        return False
    if re.search(r"\d", book_name):
        return True
    words = set(book_name.split())
    return not VARIANT_TOKENS.isdisjoint(words)

def find_duplicate_book(book_name: str | None, path: str | None = None, file_unique_id: str | None = None):
    # Highest confidence keys first
    if file_unique_id:
        existing = get_book_by_file_unique_id(file_unique_id)
        if existing:
            return existing
    if path:
        existing = get_book_by_path(path)
        if existing:
            return existing
    if book_name and not _name_allows_duplicates(book_name):
        existing = get_book_by_name(book_name)
        if existing:
            return existing
    return None

def list_books_missing_file_unique_id(limit: int = 100):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, file_id
                FROM books
                WHERE file_id IS NOT NULL
                  AND (file_unique_id IS NULL OR file_unique_id = '')
                ORDER BY id
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def list_books_missing_file_unique_id_with_path(limit: int = 100):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, file_id, path, display_name, book_name
                FROM books
                WHERE (file_unique_id IS NULL OR file_unique_id = '')
                  AND path IS NOT NULL
                ORDER BY id
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


def set_book_file_unique_id(book_id: str, file_unique_id: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE books SET file_unique_id=%s WHERE id=%s",
                (file_unique_id, book_id),
            )
            return cur.rowcount


def get_duplicate_counts_file_unique_id() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dup AS (
                    SELECT file_unique_id, COUNT(*) AS cnt
                    FROM books
                    WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''
                    GROUP BY file_unique_id
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COALESCE(SUM(cnt), 0) AS rows,
                    COUNT(*) AS groups,
                    COALESCE(SUM(cnt) - COUNT(*), 0) AS extra
                FROM dup
                """
            )
            row = cur.fetchone()
            return {"rows": int(row[0] or 0), "groups": int(row[1] or 0), "extra": int(row[2] or 0)}


def get_duplicate_counts_path() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dup AS (
                    SELECT path, COUNT(*) AS cnt
                    FROM books
                    WHERE path IS NOT NULL AND path <> ''
                    GROUP BY path
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COALESCE(SUM(cnt), 0) AS rows,
                    COUNT(*) AS groups,
                    COALESCE(SUM(cnt) - COUNT(*), 0) AS extra
                FROM dup
                """
            )
            row = cur.fetchone()
            return {"rows": int(row[0] or 0), "groups": int(row[1] or 0), "extra": int(row[2] or 0)}


def get_duplicate_counts_name() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dup AS (
                    SELECT book_name, COUNT(*) AS cnt
                    FROM books
                    WHERE book_name IS NOT NULL AND book_name <> ''
                    GROUP BY book_name
                    HAVING COUNT(*) > 1
                )
                SELECT
                    COALESCE(SUM(cnt), 0) AS rows,
                    COUNT(*) AS groups,
                    COALESCE(SUM(cnt) - COUNT(*), 0) AS extra
                FROM dup
                """
            )
            row = cur.fetchone()
            return {"rows": int(row[0] or 0), "groups": int(row[1] or 0), "extra": int(row[2] or 0)}


def get_book_storage_counts() -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN path IS NOT NULL AND path <> '' THEN 1 ELSE 0 END) AS local_count,
                    SUM(CASE WHEN file_id IS NOT NULL AND file_id <> '' THEN 1 ELSE 0 END) AS cached_count,
                    SUM(CASE WHEN (path IS NOT NULL AND path <> '') AND (file_id IS NOT NULL AND file_id <> '') THEN 1 ELSE 0 END) AS both_count,
                    COUNT(*) AS total
                FROM books
                """
            )
            row = cur.fetchone()
            return {
                "local": int(row[0] or 0),
                "cached": int(row[1] or 0),
                "both": int(row[2] or 0),
                "total": int(row[3] or 0),
            }


def get_book_by_id(book_id: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.*,
                    j.status AS local_backup_status,
                    j.attempts AS local_backup_attempts,
                    j.max_attempts AS local_backup_max_attempts,
                    j.last_error AS local_backup_error,
                    j.next_attempt_at AS local_backup_next_attempt_at,
                    j.updated_at AS local_backup_updated_at,
                    j.completed_at AS local_backup_completed_at
                FROM books b
                LEFT JOIN book_local_download_jobs j ON j.book_id = b.id
                WHERE b.id=%s
                """,
                (book_id,),
            )
            return cur.fetchone()


def get_book_delivery_snapshot(book_id: str, user_id: int | None = None):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.*,
                    j.status AS local_backup_status,
                    j.attempts AS local_backup_attempts,
                    j.max_attempts AS local_backup_max_attempts,
                    j.last_error AS local_backup_error,
                    j.next_attempt_at AS local_backup_next_attempt_at,
                    j.updated_at AS local_backup_updated_at,
                    j.completed_at AS local_backup_completed_at,
                    COALESCE(
                        (SELECT COUNT(*) FROM user_favorites uf WHERE uf.book_id = b.id),
                        0
                    ) AS fav_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'like'),
                        0
                    ) AS like_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'dislike'),
                        0
                    ) AS dislike_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'berry'),
                        0
                    ) AS berry_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'whale'),
                        0
                    ) AS whale_count,
                    EXISTS(
                        SELECT 1
                        FROM user_favorites uf
                        WHERE uf.book_id = b.id AND uf.user_id = %s
                    ) AS is_favorited,
                    (
                        SELECT br.reaction
                        FROM book_reactions br
                        WHERE br.book_id = b.id AND br.user_id = %s
                        LIMIT 1
                    ) AS user_reaction,
                    EXISTS(
                        SELECT 1
                        FROM audio_books ab
                        WHERE ab.book_id = b.id
                    ) AS has_audiobook
                FROM books b
                LEFT JOIN book_local_download_jobs j ON j.book_id = b.id
                WHERE b.id = %s
                LIMIT 1
                """,
                (user_id, user_id, book_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            adjusted_downloads, adjusted_favorites = _apply_book_counter_adjustments(
                int(row.get("downloads") or 0),
                int(row.get("fav_count") or 0),
                get_book_counter_adjustments(book_id),
            )
            row["downloads"] = adjusted_downloads
            row["fav_count"] = adjusted_favorites
            base_counts = {
                "like": int(row.get("like_count") or 0),
                "dislike": int(row.get("dislike_count") or 0),
                "berry": int(row.get("berry_count") or 0),
                "whale": int(row.get("whale_count") or 0),
            }
            adjusted_counts = _apply_book_reaction_adjustments(base_counts, get_book_reaction_adjustments(book_id))
            row["like_count"] = adjusted_counts["like"]
            row["dislike_count"] = adjusted_counts["dislike"]
            row["berry_count"] = adjusted_counts["berry"]
            row["whale_count"] = adjusted_counts["whale"]
            return row


def get_book_summary(book_id: str, lang: str, mode: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT book_id, lang, mode, summary_text, model_name, source_hash, created_at, updated_at
                FROM book_summaries
                WHERE book_id=%s AND lang=%s AND mode=%s
                """,
                (book_id, lang, mode),
            )
            return cur.fetchone()


def upsert_book_summary(
    book_id: str,
    lang: str,
    mode: str,
    summary_text: str,
    model_name: str | None = None,
    source_hash: str | None = None,
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO book_summaries (
                    book_id, lang, mode, summary_text, model_name, source_hash, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (book_id, lang, mode)
                DO UPDATE SET
                    summary_text = EXCLUDED.summary_text,
                    model_name = EXCLUDED.model_name,
                    source_hash = EXCLUDED.source_hash,
                    updated_at = NOW()
                """,
                (book_id, lang, mode, summary_text, model_name, source_hash),
            )


def get_book_by_path(path: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.*,
                    j.status AS local_backup_status,
                    j.attempts AS local_backup_attempts,
                    j.max_attempts AS local_backup_max_attempts,
                    j.last_error AS local_backup_error,
                    j.next_attempt_at AS local_backup_next_attempt_at,
                    j.updated_at AS local_backup_updated_at,
                    j.completed_at AS local_backup_completed_at
                FROM books b
                LEFT JOIN book_local_download_jobs j ON j.book_id = b.id
                WHERE b.path=%s
                """,
                (path,),
            )
            return cur.fetchone()


def get_book_totals():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN indexed THEN 1 ELSE 0 END) AS indexed,
                    SUM(COALESCE(downloads,0)) AS downloads,
                    SUM(COALESCE(searches,0)) AS searches
                FROM books
                """
            )
            row = cur.fetchone()
            if not row:
                return {"total": 0, "indexed": 0, "downloads": 0, "searches": 0}
            return {
                "total": int(row[0] or 0),
                "indexed": int(row[1] or 0),
                "downloads": int(row[2] or 0),
                "searches": int(row[3] or 0),
            }


def get_book_downloads(book_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT downloads FROM books WHERE id=%s", (book_id,))
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0)


def increment_book_download(book_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE books SET downloads = COALESCE(downloads,0) + 1 WHERE id=%s RETURNING downloads",
                (book_id,),
            )
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0)


def increment_book_searches(book_ids: list[str]):
    if not book_ids:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE books SET searches = COALESCE(searches,0) + 1 WHERE id = ANY(%s)",
                (book_ids,),
            )


def increment_audio_book_download(audio_book_id: str) -> int:
    """Increase downloads counter for an audiobook and return new value."""
    if not audio_book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE audio_books SET downloads = COALESCE(downloads,0) + 1 WHERE id=%s RETURNING downloads",
                (audio_book_id,),
            )
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0)


def increment_audio_book_searches(audio_book_ids: list[str]):
    """Bump search counter for a list of audiobooks."""
    if not audio_book_ids:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE audio_books SET searches = COALESCE(searches,0) + 1 WHERE id = ANY(%s)",
                (audio_book_ids,),
            )


def record_user_audiobook_progress(
    user_id: int,
    audio_book_id: str,
    audio_book_part_id: str,
    part_index: int,
    total_parts: int | None = None,
) -> int:
    if not user_id or not audio_book_id or not audio_book_part_id:
        return 0
    completed = bool(total_parts and int(part_index or 0) >= int(total_parts or 0))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_audiobook_progress (
                    user_id, audio_book_id, audio_book_part_id,
                    part_index, completed, last_listened_at, created_at
                )
                VALUES (%s,%s,%s,%s,%s,NOW(),NOW())
                ON CONFLICT (user_id, audio_book_id)
                DO UPDATE SET
                    audio_book_part_id = EXCLUDED.audio_book_part_id,
                    part_index = EXCLUDED.part_index,
                    completed = CASE
                        WHEN EXCLUDED.completed THEN TRUE
                        ELSE user_audiobook_progress.completed
                    END,
                    last_listened_at = NOW()
                """,
                (
                    int(user_id),
                    str(audio_book_id),
                    str(audio_book_part_id),
                    int(part_index or 0),
                    completed,
                ),
            )
            cur.execute(
                """
                INSERT INTO user_audiobook_part_history (
                    user_id, audio_book_id, audio_book_part_id,
                    part_index, listen_count, last_listened_at, created_at
                )
                VALUES (%s,%s,%s,%s,1,NOW(),NOW())
                ON CONFLICT (user_id, audio_book_part_id)
                DO UPDATE SET
                    audio_book_id = EXCLUDED.audio_book_id,
                    part_index = EXCLUDED.part_index,
                    listen_count = user_audiobook_part_history.listen_count + 1,
                    last_listened_at = NOW()
                """,
                (
                    int(user_id),
                    str(audio_book_id),
                    str(audio_book_part_id),
                    int(part_index or 0),
                ),
            )
            return cur.rowcount


def get_user_audiobook_progress(user_id: int, audio_book_id: str) -> dict | None:
    if not user_id or not audio_book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM user_audiobook_progress
                WHERE user_id=%s AND audio_book_id=%s
                LIMIT 1
                """,
                (int(user_id), str(audio_book_id)),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def list_user_audiobook_listened_part_ids(user_id: int, audio_book_id: str) -> list[str]:
    if not user_id or not audio_book_id:
        return []
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT audio_book_part_id
                FROM user_audiobook_part_history
                WHERE user_id=%s AND audio_book_id=%s
                ORDER BY part_index ASC, last_listened_at DESC
                """,
                (int(user_id), str(audio_book_id)),
            )
            rows = cur.fetchall() or []
            return [str(row[0] or "").strip() for row in rows if row and str(row[0] or "").strip()]


def set_book_reaction(user_id: int, book_id: str, reaction: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO book_reactions (book_id, user_id, reaction)
                VALUES (%s, %s, %s)
                ON CONFLICT (book_id, user_id) DO UPDATE SET
                    reaction = EXCLUDED.reaction,
                    ts = NOW()
                """,
                (book_id, user_id, reaction),
            )


def get_book_negative_reaction_alert_state(book_id: str) -> dict[str, Any] | None:
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    negative_reaction_alert_active,
                    negative_reaction_alert_trigger_dislikes,
                    negative_reaction_alerted_at,
                    negative_reaction_alert_chat_id,
                    negative_reaction_alert_message_id
                FROM books
                WHERE id=%s
                LIMIT 1
                """,
                (book_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def mark_book_negative_reaction_alert_sent(
    book_id: str,
    dislike_count: int,
    chat_id: int | None = None,
    message_id: int | None = None,
) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE books
                SET
                    negative_reaction_alert_active = TRUE,
                    negative_reaction_alert_trigger_dislikes = %s,
                    negative_reaction_alerted_at = NOW(),
                    negative_reaction_alert_chat_id = %s,
                    negative_reaction_alert_message_id = %s
                WHERE id=%s
                """,
                (
                    int(dislike_count or 0),
                    int(chat_id) if chat_id else None,
                    int(message_id) if message_id else None,
                    book_id,
                ),
            )
            return int(cur.rowcount or 0)


def clear_book_negative_reaction_alert(book_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE books
                SET
                    negative_reaction_alert_active = FALSE,
                    negative_reaction_alert_trigger_dislikes = NULL,
                    negative_reaction_alerted_at = NULL,
                    negative_reaction_alert_chat_id = NULL,
                    negative_reaction_alert_message_id = NULL
                WHERE id=%s
                """,
                (book_id,),
            )
            return int(cur.rowcount or 0)


def get_book_counter_adjustments(book_id: str) -> dict[str, int]:
    adjustments = {"downloads": 0, "favorite": 0}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT downloads_offset, favorite_offset
                    FROM book_counter_adjustments
                    WHERE book_id=%s
                    """,
                    (book_id,),
                )
                row = cur.fetchone()
                if not row:
                    return adjustments
                adjustments["downloads"] = int(row.get("downloads_offset") or 0)
                adjustments["favorite"] = int(row.get("favorite_offset") or 0)
            except Exception as e:
                if "book_counter_adjustments" in str(e):
                    return adjustments
                raise
    return adjustments


def _apply_book_counter_adjustments(
    downloads: int,
    favorite_count: int,
    adjustments: dict[str, int] | None,
) -> tuple[int, int]:
    if not adjustments:
        return max(0, int(downloads or 0)), max(0, int(favorite_count or 0))
    adj_downloads = max(0, int(downloads or 0) + int(adjustments.get("downloads", 0) or 0))
    adj_favorites = max(0, int(favorite_count or 0) + int(adjustments.get("favorite", 0) or 0))
    return adj_downloads, adj_favorites


def set_book_counter_display_counts(
    book_id: str,
    target_downloads: int,
    target_favorites: int,
    editor_user_id: int | None = None,
) -> dict[str, int]:
    raw_downloads = get_book_downloads(book_id)
    raw_favorites = get_book_favorite_count(book_id)
    download_offset = max(0, int(target_downloads or 0)) - int(raw_downloads or 0)
    favorite_offset = max(0, int(target_favorites or 0)) - int(raw_favorites or 0)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_counter_adjustments (
                    book_id TEXT PRIMARY KEY,
                    downloads_offset INTEGER NOT NULL DEFAULT 0,
                    favorite_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            if int(download_offset or 0) == 0 and int(favorite_offset or 0) == 0:
                cur.execute("DELETE FROM book_counter_adjustments WHERE book_id=%s", (book_id,))
            else:
                cur.execute(
                    """
                    INSERT INTO book_counter_adjustments (
                        book_id, downloads_offset, favorite_offset, updated_by, updated_at
                    )
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (book_id) DO UPDATE SET
                        downloads_offset = EXCLUDED.downloads_offset,
                        favorite_offset = EXCLUDED.favorite_offset,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    (
                        book_id,
                        int(download_offset),
                        int(favorite_offset),
                        int(editor_user_id) if editor_user_id else None,
                    ),
                )
    adj_downloads, adj_favorites = _apply_book_counter_adjustments(
        raw_downloads,
        raw_favorites,
        {"downloads": download_offset, "favorite": favorite_offset},
    )
    return {"downloads": adj_downloads, "favorite": adj_favorites}


def get_book_reaction_adjustments(book_id: str) -> dict[str, int]:
    adjustments = {"like": 0, "dislike": 0, "berry": 0, "whale": 0}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT like_offset, dislike_offset, berry_offset, whale_offset
                    FROM book_reaction_adjustments
                    WHERE book_id=%s
                    """,
                    (book_id,),
                )
                row = cur.fetchone()
                if not row:
                    return adjustments
                adjustments["like"] = int(row.get("like_offset") or 0)
                adjustments["dislike"] = int(row.get("dislike_offset") or 0)
                adjustments["berry"] = int(row.get("berry_offset") or 0)
                adjustments["whale"] = int(row.get("whale_offset") or 0)
            except Exception as e:
                if "book_reaction_adjustments" in str(e):
                    return adjustments
                raise
    return adjustments


def get_book_reaction_policy(book_id: str) -> dict[str, bool | int | None]:
    policy: dict[str, bool | int | None] = {
        "reactions_locked": False,
        "dislikes_disabled": False,
        "updated_by": None,
        "updated_at": None,
    }
    book_key = str(book_id or "").strip()
    if not book_key:
        return policy
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT reactions_locked, dislikes_disabled, updated_by, updated_at
                    FROM book_reaction_policies
                    WHERE book_id=%s
                    """,
                    (book_key,),
                )
                row = cur.fetchone()
                if not row:
                    return policy
                policy["reactions_locked"] = bool(row.get("reactions_locked"))
                policy["dislikes_disabled"] = bool(row.get("dislikes_disabled"))
                policy["updated_by"] = row.get("updated_by")
                policy["updated_at"] = row.get("updated_at")
            except Exception as e:
                if "book_reaction_policies" in str(e):
                    return policy
                raise
    return policy


def set_book_reaction_policy(
    book_id: str,
    *,
    reactions_locked: bool | None = None,
    dislikes_disabled: bool | None = None,
    updated_by: int | None = None,
) -> dict[str, bool | int | None]:
    book_key = str(book_id or "").strip()
    if not book_key:
        return {
            "reactions_locked": False,
            "dislikes_disabled": False,
            "updated_by": None,
            "updated_at": None,
        }
    current = get_book_reaction_policy(book_key)
    next_locked = bool(current.get("reactions_locked")) if reactions_locked is None else bool(reactions_locked)
    next_dislikes_disabled = bool(current.get("dislikes_disabled")) if dislikes_disabled is None else bool(dislikes_disabled)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_reaction_policies (
                    book_id TEXT PRIMARY KEY,
                    reactions_locked BOOLEAN NOT NULL DEFAULT FALSE,
                    dislikes_disabled BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            if not next_locked and not next_dislikes_disabled:
                cur.execute("DELETE FROM book_reaction_policies WHERE book_id=%s", (book_key,))
            else:
                cur.execute(
                    """
                    INSERT INTO book_reaction_policies (
                        book_id, reactions_locked, dislikes_disabled, updated_by, updated_at
                    )
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (book_id) DO UPDATE SET
                        reactions_locked = EXCLUDED.reactions_locked,
                        dislikes_disabled = EXCLUDED.dislikes_disabled,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    (
                        book_key,
                        next_locked,
                        next_dislikes_disabled,
                        int(updated_by) if updated_by is not None else None,
                    ),
                )
    updated = get_book_reaction_policy(book_key)
    updated["reactions_locked"] = bool(updated.get("reactions_locked"))
    updated["dislikes_disabled"] = bool(updated.get("dislikes_disabled"))
    return updated


def _book_comment_alias_number(cur, book_id: str, user_id: int) -> int:
    cur.execute(
        """
        SELECT alias_number
        FROM book_comment_aliases
        WHERE book_id=%s AND user_id=%s
        LIMIT 1
        """,
        (book_id, int(user_id)),
    )
    row = cur.fetchone()
    if row:
        try:
            if isinstance(row, dict):
                return int(row.get("alias_number") or 0) or 1
            return int(row[0] or 0) or 1
        except Exception:
            return 1
    cur.execute(
        """
        SELECT COALESCE(MAX(alias_number), 0) + 1
        FROM book_comment_aliases
        WHERE book_id=%s
        """,
        (book_id,),
    )
    row = cur.fetchone()
    try:
        if isinstance(row, dict):
            alias_number = int(next(iter(row.values())) or 0)
        else:
            alias_number = int(row[0] or 0)
    except Exception:
        alias_number = 0
    alias_number = max(1, alias_number)
    cur.execute(
        """
        INSERT INTO book_comment_aliases (book_id, user_id, alias_number, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (book_id, user_id) DO UPDATE SET user_id = EXCLUDED.user_id
        RETURNING alias_number
        """,
        (book_id, int(user_id), alias_number),
    )
    row = cur.fetchone()
    try:
        if isinstance(row, dict):
            return int(row.get("alias_number") or alias_number)
        return int(row[0] or alias_number)
    except Exception:
        return alias_number


def is_book_comment_banned(user_id: int) -> bool:
    if not user_id:
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM book_comment_bans
                    WHERE user_id=%s
                      AND (until_at IS NULL OR until_at > NOW())
                    LIMIT 1
                    """,
                    (int(user_id),),
                )
                return bool(cur.fetchone())
            except Exception as e:
                if "book_comment_bans" in str(e):
                    return False
                raise


def set_book_comment_ban(user_id: int, banned_by_user_id: int | None = None, reason: str | None = None) -> dict[str, Any]:
    safe_user_id = int(user_id or 0)
    if not safe_user_id:
        return {}
    safe_banned_by = int(banned_by_user_id or 0) or None
    safe_reason = str(reason or "").strip() or None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO book_comment_bans (
                    user_id,
                    banned_by_user_id,
                    reason,
                    until_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, NULL, NOW(), NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    banned_by_user_id = EXCLUDED.banned_by_user_id,
                    reason = EXCLUDED.reason,
                    until_at = NULL,
                    updated_at = NOW()
                RETURNING
                    user_id,
                    banned_by_user_id,
                    reason,
                    until_at,
                    created_at,
                    updated_at
                """,
                (safe_user_id, safe_banned_by, safe_reason),
            )
            row = cur.fetchone()
            return dict(row or {})


def clear_book_comment_ban(user_id: int) -> bool:
    safe_user_id = int(user_id or 0)
    if not safe_user_id:
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    DELETE FROM book_comment_bans
                    WHERE user_id=%s
                    """,
                    (safe_user_id,),
                )
                return cur.rowcount > 0
            except Exception as e:
                if "book_comment_bans" in str(e):
                    return False
                raise


def is_book_comment_peer_blocked(blocker_user_id: int, blocked_user_id: int) -> bool:
    safe_blocker = int(blocker_user_id or 0)
    safe_blocked = int(blocked_user_id or 0)
    if not safe_blocker or not safe_blocked or safe_blocker == safe_blocked:
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM book_comment_peer_blocks
                    WHERE blocker_user_id=%s AND blocked_user_id=%s
                    LIMIT 1
                    """,
                    (safe_blocker, safe_blocked),
                )
                return bool(cur.fetchone())
            except Exception as e:
                if "book_comment_peer_blocks" in str(e):
                    return False
                raise


def set_book_comment_peer_block(blocker_user_id: int, blocked_user_id: int) -> dict[str, Any]:
    safe_blocker = int(blocker_user_id or 0)
    safe_blocked = int(blocked_user_id or 0)
    if not safe_blocker or not safe_blocked or safe_blocker == safe_blocked:
        return {}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_peer_blocks (
                        blocker_user_id,
                        blocked_user_id,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, NOW(), NOW())
                    ON CONFLICT (blocker_user_id, blocked_user_id) DO UPDATE
                    SET updated_at = NOW()
                    RETURNING blocker_user_id, blocked_user_id, created_at, updated_at
                    """,
                    (safe_blocker, safe_blocked),
                )
                row = cur.fetchone()
                return dict(row or {})
            except Exception as e:
                if "book_comment_peer_blocks" in str(e):
                    return {}
                raise


def clear_book_comment_peer_block(blocker_user_id: int, blocked_user_id: int) -> bool:
    safe_blocker = int(blocker_user_id or 0)
    safe_blocked = int(blocked_user_id or 0)
    if not safe_blocker or not safe_blocked or safe_blocker == safe_blocked:
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    DELETE FROM book_comment_peer_blocks
                    WHERE blocker_user_id=%s AND blocked_user_id=%s
                    """,
                    (safe_blocker, safe_blocked),
                )
                return cur.rowcount > 0
            except Exception as e:
                if "book_comment_peer_blocks" in str(e):
                    return False
                raise


def get_book_comment_count(book_id: str) -> int:
    book_key = str(book_id or "").strip()
    if not book_key:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM book_comments
                    WHERE book_id=%s AND status='active'
                    """,
                    (book_key,),
                )
                row = cur.fetchone()
                return int((row[0] if row else 0) or 0)
            except Exception as e:
                if "book_comments" in str(e):
                    return 0
                raise


def get_user_book_comment_summary(user_id: int) -> dict[str, int]:
    safe_user_id = int(user_id or 0)
    if not safe_user_id:
        return {
            "total_comments": 0,
            "top_level_comments": 0,
            "reply_comments": 0,
            "distinct_books": 0,
            "relay_people": 0,
            "relay_conversations": 0,
        }
    default = {
        "total_comments": 0,
        "top_level_comments": 0,
        "reply_comments": 0,
        "distinct_books": 0,
        "relay_people": 0,
        "relay_conversations": 0,
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::INT AS total_comments,
                        COUNT(*) FILTER (WHERE parent_comment_id IS NULL)::INT AS top_level_comments,
                        COUNT(*) FILTER (WHERE parent_comment_id IS NOT NULL)::INT AS reply_comments,
                        COUNT(DISTINCT book_id)::INT AS distinct_books
                    FROM book_comments
                    WHERE user_id=%s AND status='active'
                    """,
                    (safe_user_id,),
                )
                row = cur.fetchone()
                if row:
                    default["total_comments"] = int(row[0] or 0)
                    default["top_level_comments"] = int(row[1] or 0)
                    default["reply_comments"] = int(row[2] or 0)
                    default["distinct_books"] = int(row[3] or 0)
                cur.execute(
                    """
                    SELECT
                        COUNT(*)::INT AS relay_conversations,
                        COUNT(DISTINCT counterpart_user_id)::INT AS relay_people
                    FROM (
                        SELECT
                            id,
                            CASE
                                WHEN comment_owner_user_id=%s THEN peer_user_id
                                WHEN peer_user_id=%s THEN comment_owner_user_id
                                ELSE NULL
                            END AS counterpart_user_id
                        FROM book_comment_relay_conversations
                        WHERE comment_owner_user_id=%s OR peer_user_id=%s
                          AND (
                              closed_by_user_id IS NULL
                              OR (closed_notified_at IS NULL AND closed_by_user_id <> %s)
                          )
                    ) q
                    WHERE counterpart_user_id IS NOT NULL
                    """,
                    (safe_user_id, safe_user_id, safe_user_id, safe_user_id, safe_user_id),
                )
                relay_row = cur.fetchone()
                if relay_row:
                    default["relay_conversations"] = int(relay_row[0] or 0)
                    default["relay_people"] = int(relay_row[1] or 0)
                return default
            except Exception as e:
                text = str(e)
                if "book_comments" in text or "book_comment_relay_conversations" in text:
                    return default
                raise


def get_book_comment_thread_count(book_id: str) -> int:
    book_key = str(book_id or "").strip()
    if not book_key:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM book_comments
                    WHERE book_id=%s
                      AND status='active'
                      AND parent_comment_id IS NULL
                    """,
                    (book_key,),
                )
                row = cur.fetchone()
                return int((row[0] if row else 0) or 0)
            except Exception as e:
                if "book_comments" in str(e):
                    return 0
                raise


def get_book_comment_by_id(comment_id: str) -> dict | None:
    comment_key = str(comment_id or "").strip()
    if not comment_key:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.*,
                        a.alias_number,
                        u.username,
                        u.first_name,
                        u.last_name,
                        COALESCE(
                            (
                                SELECT COUNT(*)
                                FROM book_comments r
                                WHERE r.root_comment_id = c.id
                                  AND r.status = 'active'
                            ),
                            0
                        ) AS reply_count
                    FROM book_comments c
                    LEFT JOIN book_comment_aliases a
                        ON a.book_id = c.book_id AND a.user_id = c.user_id
                    LEFT JOIN users u
                        ON u.id = c.user_id
                    WHERE c.id=%s
                    LIMIT 1
                    """,
                    (comment_key,),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comments" in str(e):
                    return None
                raise


def add_book_comment(book_id: str, user_id: int, text: str, parent_comment_id: str | None = None) -> dict | None:
    book_key = str(book_id or "").strip()
    clean_text = str(text or "").strip()
    if not book_key or not user_id or not clean_text:
        return None
    parent_key = str(parent_comment_id or "").strip() or None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if is_book_comment_banned(int(user_id)):
                return {"error": "banned"}
            parent_row: dict | None = None
            if parent_key:
                cur.execute(
                    """
                    SELECT id, book_id, user_id, parent_comment_id, root_comment_id, status
                    FROM book_comments
                    WHERE id=%s
                    LIMIT 1
                    """,
                    (parent_key,),
                )
                parent_row = cur.fetchone()
                if not parent_row or str(parent_row.get("book_id") or "") != book_key or str(parent_row.get("status") or "") != "active":
                    return None
            alias_number = _book_comment_alias_number(cur, book_key, int(user_id))
            comment_uuid = uuid.uuid4().hex
            root_comment_id = None
            if parent_row:
                root_comment_id = str(parent_row.get("root_comment_id") or parent_row.get("id") or "").strip() or None
            cur.execute(
                """
                INSERT INTO book_comments (
                    id, book_id, user_id, parent_comment_id, root_comment_id, text, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'active', NOW(), NOW())
                RETURNING id, book_id, user_id, parent_comment_id, root_comment_id, text, status, created_at, updated_at
                """,
                (
                    comment_uuid,
                    book_key,
                    int(user_id),
                    parent_key,
                    root_comment_id,
                    clean_text,
                ),
            )
            row = cur.fetchone()
            if row:
                row["alias_number"] = alias_number
            return row


def list_book_comment_threads(book_id: str, limit: int = 5, offset: int = 0) -> list[dict]:
    book_key = str(book_id or "").strip()
    if not book_key:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.*,
                        a.alias_number,
                        u.username,
                        u.first_name,
                        u.last_name,
                        COALESCE(
                            (
                                SELECT COUNT(*)
                                FROM book_comments r
                                WHERE r.root_comment_id = c.id
                                  AND r.status = 'active'
                            ),
                            0
                        ) AS reply_count
                    FROM book_comments c
                    LEFT JOIN book_comment_aliases a
                        ON a.book_id = c.book_id AND a.user_id = c.user_id
                    LEFT JOIN users u
                        ON u.id = c.user_id
                    WHERE c.book_id=%s
                      AND c.status='active'
                      AND c.parent_comment_id IS NULL
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (book_key, max(1, int(limit)), max(0, int(offset))),
                )
                return cur.fetchall() or []
            except Exception as e:
                if "book_comments" in str(e):
                    return []
                raise


def list_book_comment_replies(parent_comment_id: str, limit: int = 3) -> list[dict]:
    parent_key = str(parent_comment_id or "").strip()
    if not parent_key:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.*,
                        a.alias_number,
                        u.username,
                        u.first_name,
                        u.last_name
                    FROM book_comments c
                    LEFT JOIN book_comment_aliases a
                        ON a.book_id = c.book_id AND a.user_id = c.user_id
                    LEFT JOIN users u
                        ON u.id = c.user_id
                    WHERE c.parent_comment_id=%s
                      AND c.status='active'
                    ORDER BY c.created_at ASC
                    LIMIT %s
                    """,
                    (parent_key, max(1, int(limit))),
                )
                return cur.fetchall() or []
            except Exception as e:
                if "book_comments" in str(e):
                    return []
                raise


def list_user_book_comments(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    safe_user_id = int(user_id or 0)
    safe_limit = max(1, min(int(limit or 10), 30))
    safe_offset = max(0, int(offset or 0))
    if not safe_user_id:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.id,
                        c.book_id,
                        c.parent_comment_id,
                        c.root_comment_id,
                        c.text,
                        c.created_at,
                        COALESCE(NULLIF(b.display_name, ''), NULLIF(b.book_name, ''), c.book_id) AS book_title,
                        (
                            SELECT COUNT(*)
                            FROM book_comments r
                            WHERE r.parent_comment_id = c.id AND r.status='active'
                        )::INT AS reply_count
                    FROM book_comments c
                    LEFT JOIN books b
                        ON b.id = c.book_id
                    WHERE c.user_id=%s AND c.status='active'
                    ORDER BY c.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (safe_user_id, safe_limit, safe_offset),
                )
                return list(cur.fetchall() or [])
            except Exception as e:
                if "book_comments" in str(e):
                    return []
                raise


def update_book_comment_text(comment_id: str, user_id: int, text: str) -> dict | None:
    comment_key = str(comment_id or "").strip()
    clean_text = str(text or "").strip()
    safe_user_id = int(user_id or 0)
    if not comment_key or not clean_text or not safe_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    UPDATE book_comments
                    SET text=%s,
                        updated_at=NOW()
                    WHERE id=%s
                      AND user_id=%s
                      AND status='active'
                    RETURNING id, book_id, user_id, parent_comment_id, root_comment_id, text, status, created_at, updated_at
                    """,
                    (clean_text, comment_key, safe_user_id),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comments" in str(e):
                    return None
                raise


def list_book_comment_thread_messages(root_comment_id: str, limit: int = 20) -> list[dict]:
    root_key = str(root_comment_id or "").strip()
    if not root_key:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.*,
                        a.alias_number,
                        u.username,
                        u.first_name,
                        u.last_name
                    FROM book_comments c
                    LEFT JOIN book_comment_aliases a
                        ON a.book_id = c.book_id AND a.user_id = c.user_id
                    LEFT JOIN users u
                        ON u.id = c.user_id
                    WHERE (c.id=%s OR c.root_comment_id=%s)
                      AND c.status='active'
                    ORDER BY c.created_at ASC
                    LIMIT %s
                    """,
                    (root_key, root_key, max(1, int(limit))),
                )
                return cur.fetchall() or []
            except Exception as e:
                if "book_comments" in str(e):
                    return []
                raise


def viewer_can_see_book_comment_identity(comment_id: str, viewer_user_id: int | None) -> bool:
    if not viewer_user_id:
        return False
    comment = get_book_comment_by_id(comment_id)
    if not comment:
        return False
    if int(comment.get("user_id") or 0) == int(viewer_user_id):
        return True
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM book_comment_identity_requests
                    WHERE comment_id=%s
                      AND requester_user_id=%s
                      AND status='approved'
                    LIMIT 1
                    """,
                    (str(comment_id), int(viewer_user_id)),
                )
                return bool(cur.fetchone())
            except Exception as e:
                if "book_comment_identity_requests" in str(e):
                    return False
                raise


def create_book_comment_identity_request(comment_id: str, requester_user_id: int) -> dict | None:
    comment = get_book_comment_by_id(comment_id)
    if not comment:
        return None
    commenter_user_id = int(comment.get("user_id") or 0)
    if not commenter_user_id or commenter_user_id == int(requester_user_id or 0):
        return {"status": "self", "comment_id": str(comment_id), "commenter_user_id": commenter_user_id}
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT *
                    FROM book_comment_identity_requests
                    WHERE comment_id=%s AND requester_user_id=%s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (str(comment_id), int(requester_user_id)),
                )
                existing = cur.fetchone()
                if existing and str(existing.get("status") or "") in {"pending", "approved"}:
                    existing["is_existing"] = True
                    return existing
                request_id = uuid.uuid4().hex
                cur.execute(
                    """
                    INSERT INTO book_comment_identity_requests (
                        id, comment_id, requester_user_id, commenter_user_id, status, created_at
                    )
                    VALUES (%s, %s, %s, %s, 'pending', NOW())
                    RETURNING *
                    """,
                    (request_id, str(comment_id), int(requester_user_id), commenter_user_id),
                )
                row = cur.fetchone()
                if row:
                    row["is_existing"] = False
                return row
            except Exception as e:
                if "book_comment_identity_requests" in str(e):
                    return None
                raise


def resolve_book_comment_identity_request(request_id: str, commenter_user_id: int, approve: bool) -> dict | None:
    request_key = str(request_id or "").strip()
    if not request_key or not commenter_user_id:
        return None
    next_status = "approved" if approve else "rejected"
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    UPDATE book_comment_identity_requests
                    SET status=%s, resolved_at=NOW()
                    WHERE id=%s
                      AND commenter_user_id=%s
                      AND status='pending'
                    RETURNING *
                    """,
                    (next_status, request_key, int(commenter_user_id)),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_identity_requests" in str(e):
                    return None
                raise


def create_book_comment_report(comment_id: str, reporter_user_id: int, reason: str | None = None) -> dict | None:
    comment_key = str(comment_id or "").strip()
    if not comment_key or not reporter_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_reports (id, comment_id, reporter_user_id, reason, status, created_at)
                    VALUES (%s, %s, %s, %s, 'open', NOW())
                    ON CONFLICT (comment_id, reporter_user_id) DO UPDATE SET
                        reason = COALESCE(EXCLUDED.reason, book_comment_reports.reason),
                        status = 'open'
                    RETURNING *
                    """,
                    (uuid.uuid4().hex, comment_key, int(reporter_user_id), str(reason or "").strip() or None),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_reports" in str(e):
                    return None
                raise


def get_or_create_book_comment_relay_conversation(comment_id: str, peer_user_id: int) -> dict | None:
    comment = get_book_comment_by_id(comment_id)
    if not comment:
        return None
    comment_key = str(comment.get("id") or "").strip()
    book_id = str(comment.get("book_id") or "").strip()
    owner_user_id = int(comment.get("user_id") or 0)
    peer_id = int(peer_user_id or 0)
    if not comment_key or not book_id or not owner_user_id or not peer_id or owner_user_id == peer_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_conversations (
                        id, book_id, comment_id, comment_owner_user_id, peer_user_id, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (comment_id, peer_user_id) DO UPDATE
                    SET updated_at = NOW()
                    RETURNING *
                    """,
                    (uuid.uuid4().hex, book_id, comment_key, owner_user_id, peer_id),
                )
                row = cur.fetchone()
                if row:
                    cur.execute(
                        """
                        INSERT INTO book_comment_relay_participants (
                            conversation_id,
                            user_id,
                            muted,
                            created_at,
                            updated_at
                        )
                        VALUES
                            (%s, %s, FALSE, NOW(), NOW()),
                            (%s, %s, FALSE, NOW(), NOW())
                        ON CONFLICT (conversation_id, user_id) DO NOTHING
                        """,
                        (
                            str(row.get("id") or ""),
                            owner_user_id,
                            str(row.get("id") or ""),
                            peer_id,
                        ),
                    )
                return row
            except Exception as e:
                if "book_comment_relay_conversations" in str(e):
                    return None
                raise


def get_book_comment_relay_conversation(conversation_id: str) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    if not conv_key:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT *
                    FROM book_comment_relay_conversations
                    WHERE id=%s
                    LIMIT 1
                    """,
                    (conv_key,),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_conversations" in str(e):
                    return None
                raise


def close_book_comment_relay_conversation(conversation_id: str, user_id: int) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    if not conv_key or not safe_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    UPDATE book_comment_relay_conversations
                    SET closed_by_user_id=%s,
                        closed_at=NOW(),
                        closed_notified_at=NULL,
                        updated_at=NOW()
                    WHERE id=%s
                      AND (comment_owner_user_id=%s OR peer_user_id=%s)
                    RETURNING *
                    """,
                    (safe_user_id, conv_key, safe_user_id, safe_user_id),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_conversations" in str(e):
                    return None
                raise


def acknowledge_book_comment_relay_closure(conversation_id: str, user_id: int) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    if not conv_key or not safe_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    UPDATE book_comment_relay_conversations
                    SET closed_notified_at=NOW(),
                        updated_at=NOW()
                    WHERE id=%s
                      AND closed_by_user_id IS NOT NULL
                      AND closed_by_user_id <> %s
                      AND closed_notified_at IS NULL
                      AND (comment_owner_user_id=%s OR peer_user_id=%s)
                    RETURNING *
                    """,
                    (conv_key, safe_user_id, safe_user_id, safe_user_id),
                )
                row = cur.fetchone()
                if row:
                    return row
                cur.execute(
                    """
                    SELECT *
                    FROM book_comment_relay_conversations
                    WHERE id=%s
                      AND (comment_owner_user_id=%s OR peer_user_id=%s)
                    LIMIT 1
                    """,
                    (conv_key, safe_user_id, safe_user_id),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_conversations" in str(e):
                    return None
                raise


def get_book_comment_relay_participant_state(conversation_id: str, user_id: int) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    if not conv_key or not safe_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_participants (
                        conversation_id,
                        user_id,
                        muted,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, FALSE, NOW(), NOW())
                    ON CONFLICT (conversation_id, user_id) DO NOTHING
                    """,
                    (conv_key, safe_user_id),
                )
                cur.execute(
                    """
                    SELECT *
                    FROM book_comment_relay_participants
                    WHERE conversation_id=%s AND user_id=%s
                    LIMIT 1
                    """,
                    (conv_key, safe_user_id),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_participants" in str(e):
                    return None
                raise


def touch_book_comment_relay_last_seen(conversation_id: str, user_id: int, seen_at: datetime | None = None) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    if not conv_key or not safe_user_id:
        return None
    ts = seen_at if hasattr(seen_at, "strftime") else datetime.utcnow()
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_participants (
                        conversation_id,
                        user_id,
                        muted,
                        last_seen_at,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, FALSE, %s, NOW(), NOW())
                    ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = NOW()
                    RETURNING *
                    """,
                    (conv_key, safe_user_id, ts),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_participants" in str(e):
                    return None
                raise


def set_book_comment_relay_muted(conversation_id: str, user_id: int, muted: bool) -> dict | None:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    if not conv_key or not safe_user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_participants (
                        conversation_id,
                        user_id,
                        muted,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                        muted = EXCLUDED.muted,
                        updated_at = NOW()
                    RETURNING *
                    """,
                    (conv_key, safe_user_id, bool(muted)),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_participants" in str(e):
                    return None
                raise


def get_book_comment_relay_unread_summary(user_id: int) -> dict[str, int]:
    safe_user_id = int(user_id or 0)
    default = {"unread_messages": 0, "unread_conversations": 0}
    if not safe_user_id:
        return default
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        COALESCE(SUM(unread_count), 0)::INT AS unread_messages,
                        COUNT(*) FILTER (WHERE unread_count > 0)::INT AS unread_conversations
                    FROM (
                        SELECT
                            c.id,
                            COUNT(m.id)::INT AS unread_count
                        FROM book_comment_relay_conversations c
                        LEFT JOIN book_comment_relay_participants p
                            ON p.conversation_id = c.id AND p.user_id = %s
                        LEFT JOIN book_comment_relay_messages m
                            ON m.conversation_id = c.id
                           AND m.recipient_user_id = %s
                           AND (p.last_seen_at IS NULL OR m.created_at > p.last_seen_at)
                        WHERE c.comment_owner_user_id = %s OR c.peer_user_id = %s
                          AND (
                              c.closed_by_user_id IS NULL
                              OR (c.closed_notified_at IS NULL AND c.closed_by_user_id <> %s)
                          )
                        GROUP BY c.id
                    ) q
                    """,
                    (safe_user_id, safe_user_id, safe_user_id, safe_user_id, safe_user_id),
                )
                row = cur.fetchone()
                if row:
                    return {
                        "unread_messages": int(row[0] or 0),
                        "unread_conversations": int(row[1] or 0),
                    }
                return default
            except Exception as e:
                text = str(e)
                if "book_comment_relay_conversations" in text or "book_comment_relay_participants" in text or "book_comment_relay_messages" in text:
                    return default
                raise


def list_book_comment_relay_conversations_for_user(user_id: int, limit: int = 10, offset: int = 0) -> list[dict]:
    safe_user_id = int(user_id or 0)
    safe_limit = max(1, min(int(limit or 10), 30))
    safe_offset = max(0, int(offset or 0))
    if not safe_user_id:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        c.*,
                        COALESCE(NULLIF(b.display_name, ''), NULLIF(b.book_name, ''), c.book_id) AS book_title,
                        CASE
                            WHEN c.comment_owner_user_id = %s THEN c.peer_user_id
                            ELSE c.comment_owner_user_id
                        END AS counterpart_user_id,
                        counterpart.username AS counterpart_username,
                        counterpart.first_name AS counterpart_first_name,
                        counterpart.last_name AS counterpart_last_name,
                        COALESCE(p.muted, FALSE) AS muted,
                        p.last_seen_at,
                        last_msg.id AS last_message_id,
                        last_msg.message_type AS last_message_type,
                        last_msg.text AS last_message_text,
                        last_msg.caption AS last_message_caption,
                        last_msg.created_at AS last_message_at,
                        last_msg.sender_user_id AS last_sender_user_id,
                        (
                            SELECT COUNT(*)
                            FROM book_comment_relay_messages unread
                            WHERE unread.conversation_id = c.id
                              AND unread.recipient_user_id = %s
                              AND (p.last_seen_at IS NULL OR unread.created_at > p.last_seen_at)
                        )::INT AS unread_count
                    FROM book_comment_relay_conversations c
                    LEFT JOIN books b
                        ON b.id = c.book_id
                    LEFT JOIN book_comment_relay_participants p
                        ON p.conversation_id = c.id AND p.user_id = %s
                    LEFT JOIN LATERAL (
                        SELECT m.*
                        FROM book_comment_relay_messages m
                        WHERE m.conversation_id = c.id
                        ORDER BY m.created_at DESC
                        LIMIT 1
                    ) last_msg ON TRUE
                    LEFT JOIN users counterpart
                        ON counterpart.id = CASE
                            WHEN c.comment_owner_user_id = %s THEN c.peer_user_id
                            ELSE c.comment_owner_user_id
                        END
                    WHERE c.comment_owner_user_id = %s OR c.peer_user_id = %s
                      AND (
                          c.closed_by_user_id IS NULL
                          OR (c.closed_notified_at IS NULL AND c.closed_by_user_id <> %s)
                      )
                    ORDER BY COALESCE(last_msg.created_at, c.updated_at, c.created_at) DESC
                    LIMIT %s OFFSET %s
                    """,
                    (
                        safe_user_id,
                        safe_user_id,
                        safe_user_id,
                        safe_user_id,
                        safe_user_id,
                        safe_user_id,
                        safe_user_id,
                        safe_limit,
                        safe_offset,
                    ),
                )
                return list(cur.fetchall() or [])
            except Exception as e:
                text = str(e)
                if "book_comment_relay_conversations" in text or "book_comment_relay_messages" in text or "book_comment_relay_participants" in text:
                    return []
                raise


def count_book_comment_relay_conversations_for_user(user_id: int) -> int:
    safe_user_id = int(user_id or 0)
    if not safe_user_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT COUNT(*)::INT
                    FROM book_comment_relay_conversations
                    WHERE comment_owner_user_id = %s OR peer_user_id = %s
                      AND (
                          closed_by_user_id IS NULL
                          OR (closed_notified_at IS NULL AND closed_by_user_id <> %s)
                      )
                    """,
                    (safe_user_id, safe_user_id, safe_user_id),
                )
                row = cur.fetchone()
                return int((row[0] if row else 0) or 0)
            except Exception as e:
                if "book_comment_relay_conversations" in str(e):
                    return 0
                raise


def create_book_comment_relay_message(
    conversation_id: str,
    sender_user_id: int,
    recipient_user_id: int,
    message_type: str,
    *,
    text: str | None = None,
    caption: str | None = None,
    media_file_id: str | None = None,
    media_file_unique_id: str | None = None,
) -> dict | None:
    conversation = get_book_comment_relay_conversation(conversation_id)
    if not conversation:
        return None
    conv_key = str(conversation.get("id") or "").strip()
    book_id = str(conversation.get("book_id") or "").strip()
    comment_id = str(conversation.get("comment_id") or "").strip()
    sender_id = int(sender_user_id or 0)
    recipient_id = int(recipient_user_id or 0)
    msg_type = str(message_type or "").strip().lower()
    if (
        not conv_key
        or not book_id
        or not comment_id
        or not sender_id
        or not recipient_id
        or not msg_type
        or int(conversation.get("closed_by_user_id") or 0) != 0
    ):
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                message_id = uuid.uuid4().hex
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_messages (
                        id, conversation_id, book_id, comment_id, sender_user_id, recipient_user_id,
                        message_type, text, caption, media_file_id, media_file_unique_id, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING *
                    """,
                    (
                        message_id,
                        conv_key,
                        book_id,
                        comment_id,
                        sender_id,
                        recipient_id,
                        msg_type,
                        str(text or "").strip() or None,
                        str(caption or "").strip() or None,
                        str(media_file_id or "").strip() or None,
                        str(media_file_unique_id or "").strip() or None,
                    ),
                )
                row = cur.fetchone()
                cur.execute(
                    """
                    UPDATE book_comment_relay_conversations
                    SET updated_at=NOW()
                    WHERE id=%s
                    """,
                    (conv_key,),
                )
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_participants (
                        conversation_id,
                        user_id,
                        muted,
                        last_seen_at,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, FALSE, NOW(), NOW(), NOW())
                    ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                        last_seen_at = NOW(),
                        updated_at = NOW()
                    """,
                    (conv_key, sender_id),
                )
                cur.execute(
                    """
                    INSERT INTO book_comment_relay_participants (
                        conversation_id,
                        user_id,
                        muted,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, FALSE, NOW(), NOW())
                    ON CONFLICT (conversation_id, user_id) DO NOTHING
                    """,
                    (conv_key, recipient_id),
                )
                return row
            except Exception as e:
                if "book_comment_relay_messages" in str(e) or "book_comment_relay_conversations" in str(e):
                    return None
                raise


def get_book_comment_relay_message(message_id: str) -> dict | None:
    message_key = str(message_id or "").strip()
    if not message_key:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        m.*,
                        c.comment_owner_user_id,
                        c.peer_user_id
                    FROM book_comment_relay_messages m
                    LEFT JOIN book_comment_relay_conversations c
                        ON c.id = m.conversation_id
                    WHERE m.id=%s
                    LIMIT 1
                    """,
                    (message_key,),
                )
                return cur.fetchone()
            except Exception as e:
                if "book_comment_relay_messages" in str(e):
                    return None
                raise


def list_book_comment_relay_messages_for_user(
    conversation_id: str,
    user_id: int,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    conv_key = str(conversation_id or "").strip()
    safe_user_id = int(user_id or 0)
    safe_limit = max(1, min(int(limit or 20), 50))
    safe_offset = max(0, int(offset or 0))
    if not conv_key or not safe_user_id:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM book_comment_relay_conversations
                    WHERE id=%s
                      AND (comment_owner_user_id=%s OR peer_user_id=%s)
                    LIMIT 1
                    """,
                    (conv_key, safe_user_id, safe_user_id),
                )
                if not cur.fetchone():
                    return []
                cur.execute(
                    """
                    SELECT
                        m.*,
                        CASE
                            WHEN m.sender_user_id = %s THEN 'outgoing'
                            ELSE 'incoming'
                        END AS direction
                    FROM book_comment_relay_messages m
                    WHERE m.conversation_id=%s
                    ORDER BY m.created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (safe_user_id, conv_key, safe_limit, safe_offset),
                )
                rows = list(cur.fetchall() or [])
                rows.reverse()
                return rows
            except Exception as e:
                text = str(e)
                if "book_comment_relay_conversations" in text or "book_comment_relay_messages" in text:
                    return []
                raise


def delete_book_comment(comment_id: str, moderator_user_id: int | None = None, reason: str | None = None) -> int:
    comment = get_book_comment_by_id(comment_id)
    if not comment:
        return 0
    comment_key = str(comment.get("id") or "").strip()
    parent_comment_id = str(comment.get("parent_comment_id") or "").strip() or None
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                if parent_comment_id:
                    cur.execute(
                        """
                        WITH RECURSIVE subtree AS (
                            SELECT id
                            FROM book_comments
                            WHERE id=%s
                            UNION ALL
                            SELECT c.id
                            FROM book_comments c
                            JOIN subtree s ON c.parent_comment_id = s.id
                        )
                        UPDATE book_comments
                        SET status='deleted',
                            moderated_by_user_id=%s,
                            moderated_at=NOW(),
                            deleted_reason=%s,
                            updated_at=NOW()
                        WHERE id IN (SELECT id FROM subtree)
                        """,
                        (
                            comment_key,
                            int(moderator_user_id) if moderator_user_id else None,
                            str(reason or "").strip() or "deleted",
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE book_comments
                        SET status='deleted',
                            moderated_by_user_id=%s,
                            moderated_at=NOW(),
                            deleted_reason=%s,
                            updated_at=NOW()
                        WHERE id=%s OR root_comment_id=%s
                        """,
                        (
                            int(moderator_user_id) if moderator_user_id else None,
                            str(reason or "").strip() or "deleted",
                            comment_key,
                            comment_key,
                        ),
                    )
                return int(cur.rowcount or 0)
            except Exception as e:
                if "book_comments" in str(e):
                    return 0
                raise


def _apply_book_reaction_adjustments(counts: dict[str, int], adjustments: dict[str, int] | None) -> dict[str, int]:
    if not adjustments:
        return {
            "like": int(counts.get("like", 0) or 0),
            "dislike": int(counts.get("dislike", 0) or 0),
            "berry": int(counts.get("berry", 0) or 0),
            "whale": int(counts.get("whale", 0) or 0),
        }
    adjusted: dict[str, int] = {}
    for key in ("like", "dislike", "berry", "whale"):
        base = int(counts.get(key, 0) or 0)
        offset = int(adjustments.get(key, 0) or 0)
        adjusted[key] = max(0, base + offset)
    return adjusted


def set_book_reaction_display_counts(book_id: str, target_counts: dict[str, int], editor_user_id: int | None = None) -> dict[str, int]:
    raw_counts = get_book_reaction_counts(book_id, include_adjustments=False)
    offsets = {}
    for key in ("like", "dislike", "berry", "whale"):
        target = max(0, int(target_counts.get(key, 0) or 0))
        offsets[key] = target - int(raw_counts.get(key, 0) or 0)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_reaction_adjustments (
                    book_id TEXT PRIMARY KEY,
                    like_offset INTEGER NOT NULL DEFAULT 0,
                    dislike_offset INTEGER NOT NULL DEFAULT 0,
                    berry_offset INTEGER NOT NULL DEFAULT 0,
                    whale_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            if all(int(offsets.get(key, 0) or 0) == 0 for key in ("like", "dislike", "berry", "whale")):
                cur.execute("DELETE FROM book_reaction_adjustments WHERE book_id=%s", (book_id,))
            else:
                cur.execute(
                    """
                    INSERT INTO book_reaction_adjustments (
                        book_id, like_offset, dislike_offset, berry_offset, whale_offset, updated_by, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (book_id) DO UPDATE SET
                        like_offset = EXCLUDED.like_offset,
                        dislike_offset = EXCLUDED.dislike_offset,
                        berry_offset = EXCLUDED.berry_offset,
                        whale_offset = EXCLUDED.whale_offset,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    (
                        book_id,
                        int(offsets["like"]),
                        int(offsets["dislike"]),
                        int(offsets["berry"]),
                        int(offsets["whale"]),
                        int(editor_user_id) if editor_user_id else None,
                    ),
                )
    return _apply_book_reaction_adjustments(raw_counts, offsets)


def clear_all_book_display_adjustments() -> dict[str, int]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM book_counter_adjustments")
            counter_rows = int(cur.rowcount or 0)
            cur.execute("DELETE FROM book_reaction_adjustments")
            reaction_rows = int(cur.rowcount or 0)
    return {"counter_rows": counter_rows, "reaction_rows": reaction_rows}


def seed_all_book_display_stats_randomly(
    *,
    download_min: int = 20,
    download_max: int = 50,
    favorite_min: int = 10,
    favorite_max: int = 60,
    positive_min: int = 10,
    positive_max: int = 60,
    negative_min: int = 0,
    negative_max: int = 10,
    editor_user_id: int | None = None,
) -> dict[str, int]:
    download_low, download_high = sorted((int(download_min), int(download_max)))
    favorite_low, favorite_high = sorted((int(favorite_min), int(favorite_max)))
    positive_low, positive_high = sorted((int(positive_min), int(positive_max)))
    negative_low, negative_high = sorted((int(negative_min), int(negative_max)))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.id,
                    COALESCE(b.downloads, 0) AS downloads,
                    COALESCE(f.fav_count, 0) AS fav_count,
                    COALESCE(r.like_count, 0) AS like_count,
                    COALESCE(r.dislike_count, 0) AS dislike_count,
                    COALESCE(r.berry_count, 0) AS berry_count,
                    COALESCE(r.whale_count, 0) AS whale_count
                FROM books b
                LEFT JOIN (
                    SELECT book_id, COUNT(*) AS fav_count
                    FROM user_favorites
                    GROUP BY book_id
                ) f ON f.book_id = b.id
                LEFT JOIN (
                    SELECT
                        book_id,
                        SUM(CASE WHEN reaction='like' THEN 1 ELSE 0 END) AS like_count,
                        SUM(CASE WHEN reaction='dislike' THEN 1 ELSE 0 END) AS dislike_count,
                        SUM(CASE WHEN reaction='berry' THEN 1 ELSE 0 END) AS berry_count,
                        SUM(CASE WHEN reaction='whale' THEN 1 ELSE 0 END) AS whale_count
                    FROM book_reactions
                    GROUP BY book_id
                ) r ON r.book_id = b.id
                """
            )
            rows = cur.fetchall() or []

        counter_payload: list[tuple[str, int, int, int | None]] = []
        reaction_payload: list[tuple[str, int, int, int, int, int | None]] = []
        randomized_books = 0

        for row in rows:
            book_id = str(row.get("id") or "").strip()
            if not book_id:
                continue
            randomized_books += 1
            target_downloads = random.randint(download_low, download_high)
            target_favorites = random.randint(favorite_low, favorite_high)
            target_like = random.randint(positive_low, positive_high)
            target_berry = random.randint(positive_low, positive_high)
            target_whale = random.randint(positive_low, positive_high)
            target_dislike = random.randint(negative_low, negative_high)

            downloads_offset = int(target_downloads) - int(row.get("downloads") or 0)
            favorite_offset = int(target_favorites) - int(row.get("fav_count") or 0)
            like_offset = int(target_like) - int(row.get("like_count") or 0)
            dislike_offset = int(target_dislike) - int(row.get("dislike_count") or 0)
            berry_offset = int(target_berry) - int(row.get("berry_count") or 0)
            whale_offset = int(target_whale) - int(row.get("whale_count") or 0)

            if downloads_offset or favorite_offset:
                counter_payload.append(
                    (
                        book_id,
                        int(downloads_offset),
                        int(favorite_offset),
                        int(editor_user_id) if editor_user_id else None,
                    )
                )
            if like_offset or dislike_offset or berry_offset or whale_offset:
                reaction_payload.append(
                    (
                        book_id,
                        int(like_offset),
                        int(dislike_offset),
                        int(berry_offset),
                        int(whale_offset),
                        int(editor_user_id) if editor_user_id else None,
                    )
                )

        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_counter_adjustments (
                    book_id TEXT PRIMARY KEY,
                    downloads_offset INTEGER NOT NULL DEFAULT 0,
                    favorite_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS book_reaction_adjustments (
                    book_id TEXT PRIMARY KEY,
                    like_offset INTEGER NOT NULL DEFAULT 0,
                    dislike_offset INTEGER NOT NULL DEFAULT 0,
                    berry_offset INTEGER NOT NULL DEFAULT 0,
                    whale_offset INTEGER NOT NULL DEFAULT 0,
                    updated_by BIGINT,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("DELETE FROM book_counter_adjustments")
            cur.execute("DELETE FROM book_reaction_adjustments")
            if counter_payload and execute_values is not None:
                execute_values(
                    cur,
                    """
                    INSERT INTO book_counter_adjustments (
                        book_id, downloads_offset, favorite_offset, updated_by, updated_at
                    ) VALUES %s
                    ON CONFLICT (book_id) DO UPDATE SET
                        downloads_offset = EXCLUDED.downloads_offset,
                        favorite_offset = EXCLUDED.favorite_offset,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    counter_payload,
                    template="(%s, %s, %s, %s, NOW())",
                )
            if reaction_payload and execute_values is not None:
                execute_values(
                    cur,
                    """
                    INSERT INTO book_reaction_adjustments (
                        book_id, like_offset, dislike_offset, berry_offset, whale_offset, updated_by, updated_at
                    ) VALUES %s
                    ON CONFLICT (book_id) DO UPDATE SET
                        like_offset = EXCLUDED.like_offset,
                        dislike_offset = EXCLUDED.dislike_offset,
                        berry_offset = EXCLUDED.berry_offset,
                        whale_offset = EXCLUDED.whale_offset,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    reaction_payload,
                    template="(%s, %s, %s, %s, %s, %s, NOW())",
                )

    return {
        "books": int(randomized_books),
        "counter_rows": int(len(counter_payload)),
        "reaction_rows": int(len(reaction_payload)),
        "download_min": int(download_low),
        "download_max": int(download_high),
        "favorite_min": int(favorite_low),
        "favorite_max": int(favorite_high),
        "positive_min": int(positive_low),
        "positive_max": int(positive_high),
        "negative_min": int(negative_low),
        "negative_max": int(negative_high),
    }


def get_book_reaction_counts(book_id: str, include_adjustments: bool = True) -> dict[str, int]:
    counts = {"like": 0, "dislike": 0, "berry": 0, "whale": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reaction, COUNT(*) FROM book_reactions WHERE book_id=%s GROUP BY reaction",
                (book_id,),
            )
            for reaction, count in cur.fetchall():
                if reaction in counts:
                    counts[reaction] = int(count)
    if include_adjustments:
        return _apply_book_reaction_adjustments(counts, get_book_reaction_adjustments(book_id))
    return counts

def get_book_favorite_count(book_id: str) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_favorites WHERE book_id=%s", (book_id,))
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0] or 0)


def get_book_stats(book_id: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(b.downloads, 0) AS downloads,
                    COALESCE(
                        (SELECT COUNT(*) FROM user_favorites uf WHERE uf.book_id = b.id),
                        0
                    ) AS fav_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'like'),
                        0
                    ) AS like_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'dislike'),
                        0
                    ) AS dislike_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'berry'),
                        0
                    ) AS berry_count,
                    COALESCE(
                        (SELECT COUNT(*) FROM book_reactions br WHERE br.book_id = b.id AND br.reaction = 'whale'),
                        0
                    ) AS whale_count
                FROM books b
                WHERE b.id = %s
                """,
                (book_id,),
            )
            row = cur.fetchone()
            if not row:
                return {
                    "downloads": 0,
                    "fav_count": 0,
                    "like": 0,
                    "dislike": 0,
                    "berry": 0,
                    "whale": 0,
                }
            base_counts = {
                "like": int(row.get("like_count") or 0),
                "dislike": int(row.get("dislike_count") or 0),
                "berry": int(row.get("berry_count") or 0),
                "whale": int(row.get("whale_count") or 0),
            }
            adjusted_downloads, adjusted_favorites = _apply_book_counter_adjustments(
                int(row.get("downloads") or 0),
                int(row.get("fav_count") or 0),
                get_book_counter_adjustments(book_id),
            )
            adjusted_counts = _apply_book_reaction_adjustments(base_counts, get_book_reaction_adjustments(book_id))
            return {
                "downloads": adjusted_downloads,
                "fav_count": adjusted_favorites,
                "like": adjusted_counts["like"],
                "dislike": adjusted_counts["dislike"],
                "berry": adjusted_counts["berry"],
                "whale": adjusted_counts["whale"],
            }


def get_favorites_total() -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_favorites")
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_user_favorites_count(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_favorites WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_user_usage_stats(user_id: int) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT joined_date FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            joined_date = row[0] if row else None
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(count),0) AS downloads,
                    COUNT(*) FILTER (WHERE count > 0) AS active_days
                FROM user_recents
                WHERE user_id=%s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            downloads = int(row[0] or 0)
            active_days = int(row[1] or 0)
            cur.execute(
                """
                SELECT COALESCE(SUM(searches),0)
                FROM analytics_daily_users
                WHERE user_id=%s
                """,
                (user_id,),
            )
            row = cur.fetchone()
            searches = int(row[0] or 0) if row else 0
            return {
                "joined_date": joined_date,
                "searches": searches,
                "downloads": downloads,
                "active_days": active_days,
            }


def get_request_status_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM book_requests GROUP BY status")
            for status, count in cur.fetchall():
                key = str(status or "unknown")
                counts[key] = int(count or 0)
    return counts


def get_upload_request_status_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM upload_requests GROUP BY status")
            for status, count in cur.fetchall():
                key = str(status or "unknown")
                counts[key] = int(count or 0)
    return counts


def get_user_status_counts() -> dict[str, int]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN blocked THEN 1 ELSE 0 END) AS blocked,
                    SUM(CASE WHEN allowed THEN 1 ELSE 0 END) AS allowed
                FROM users
                """
            )
            row = cur.fetchone()
            if not row:
                return {"total": 0, "blocked": 0, "allowed": 0}
            return {
                "total": int(row[0] or 0),
                "blocked": int(row[1] or 0),
                "allowed": int(row[2] or 0),
            }


def get_user_daily_counts(day: date):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN joined_date=%s THEN 1 ELSE 0 END) AS joined_today,
                    SUM(CASE WHEN left_date=%s THEN 1 ELSE 0 END) AS left_today
                FROM users
                """,
                (day, day),
            )
            row = cur.fetchone()
            if not row:
                return {"joined": 0, "left": 0}
            return {"joined": int(row[0] or 0), "left": int(row[1] or 0)}


def get_reaction_totals() -> dict[str, int]:
    counts = {"like": 0, "dislike": 0, "berry": 0, "whale": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT reaction, COUNT(*) FROM book_reactions GROUP BY reaction")
            for reaction, count in cur.fetchall():
                if reaction in counts:
                    counts[reaction] = int(count or 0)
    return counts

def get_user_reaction(book_id: str, user_id: int) -> str | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reaction FROM book_reactions WHERE book_id=%s AND user_id=%s",
                (book_id, user_id),
            )
            row = cur.fetchone()
            return row[0] if row else None

def award_reaction_action(user_id: int, book_id: str) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_reaction_awards (user_id, book_id)
                VALUES (%s, %s)
                ON CONFLICT (user_id, book_id) DO NOTHING
                """,
                (user_id, book_id),
            )
            return cur.rowcount > 0


def get_user_reaction_awards_count(user_id: int) -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM user_reaction_awards WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def get_top_users(limit: int, coin_search: int, coin_download: int, coin_reaction: int, coin_favorite: int, coin_referral: int):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    u.id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    COALESCE(u.coin_adjustment, 0) AS coin_adjustment,
                    COALESCE(s.searches, 0) AS searches,
                    COALESCE(d.downloads, 0) AS downloads,
                    COALESCE(f.favorites, 0) AS favorites,
                    COALESCE(r.reactions, 0) AS reactions,
                    COALESCE(ref.referrals, 0) AS referrals
                FROM users u
                LEFT JOIN (
                    SELECT user_id, COALESCE(SUM(searches), 0) AS searches
                    FROM analytics_daily_users
                    GROUP BY user_id
                ) s ON s.user_id = u.id
                LEFT JOIN (
                    SELECT user_id, COALESCE(SUM(count), 0) AS downloads
                    FROM user_recents
                    GROUP BY user_id
                ) d ON d.user_id = u.id
                LEFT JOIN (
                    SELECT user_id, COUNT(*) AS favorites
                    FROM user_favorite_awards
                    GROUP BY user_id
                ) f ON f.user_id = u.id
                LEFT JOIN (
                    SELECT user_id, COUNT(*) AS reactions
                    FROM user_reaction_awards
                    GROUP BY user_id
                ) r ON r.user_id = u.id
                LEFT JOIN (
                    SELECT referrer_id AS user_id, COUNT(*) AS referrals
                    FROM users
                    WHERE referrer_id IS NOT NULL
                    GROUP BY referrer_id
                ) ref ON ref.user_id = u.id
                WHERE u.blocked = FALSE AND u.stopped = FALSE AND u.left_date IS NULL
                ORDER BY
                    (
                        COALESCE(s.searches, 0) * %s
                        + COALESCE(d.downloads, 0) * %s
                        + COALESCE(r.reactions, 0) * %s
                        + COALESCE(f.favorites, 0) * %s
                        + COALESCE(ref.referrals, 0) * %s
                        + COALESCE(u.coin_adjustment, 0)
                    ) DESC,
                    COALESCE(s.searches, 0) DESC,
                    COALESCE(d.downloads, 0) DESC,
                    u.id ASC
                LIMIT %s
                """,
                (coin_search, coin_download, coin_reaction, coin_favorite, coin_referral, limit),
            )
            return cur.fetchall()


def get_top_books(limit: int = 20, offset: int = 0):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.id,
                    b.book_name,
                    b.display_name,
                    COALESCE(b.downloads, 0) AS downloads,
                    COALESCE(b.searches, 0) AS searches,
                    COALESCE(f.fav_count, 0) AS fav_count,
                    COALESCE(r.like_count, 0) AS like_count,
                    COALESCE(r.dislike_count, 0) AS dislike_count,
                    COALESCE(r.berry_count, 0) AS berry_count,
                    COALESCE(r.whale_count, 0) AS whale_count
                FROM books b
                LEFT JOIN (
                    SELECT book_id, COUNT(*) AS fav_count
                    FROM user_favorites
                    GROUP BY book_id
                ) f ON f.book_id = b.id
                LEFT JOIN (
                    SELECT
                        book_id,
                        SUM(CASE WHEN reaction='like' THEN 1 ELSE 0 END) AS like_count,
                        SUM(CASE WHEN reaction='dislike' THEN 1 ELSE 0 END) AS dislike_count,
                        SUM(CASE WHEN reaction='berry' THEN 1 ELSE 0 END) AS berry_count,
                        SUM(CASE WHEN reaction='whale' THEN 1 ELSE 0 END) AS whale_count
                    FROM book_reactions
                    GROUP BY book_id
                ) r ON r.book_id = b.id
                ORDER BY
                    (
                        COALESCE(b.searches, 0) * 3
                        + COALESCE(b.downloads, 0) * 2
                        + COALESCE(f.fav_count, 0) * 2
                        + COALESCE(r.like_count, 0)
                        + COALESCE(r.berry_count, 0)
                        + COALESCE(r.whale_count, 0)
                        - COALESCE(r.dislike_count, 0) * 2
                    ) DESC,
                    COALESCE(b.searches, 0) DESC,
                    COALESCE(b.downloads, 0) DESC,
                    COALESCE(f.fav_count, 0) DESC,
                    COALESCE(r.like_count, 0) DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            return cur.fetchall()


def insert_book(book: dict):
    if is_book_adult_marked(book):
        try:
            logger.info(
                "insert_book: skipped by adult filter (display=%s, normalized=%s)",
                str((book or {}).get("display_name") or "")[:120],
                str((book or {}).get("book_name") or "")[:120],
            )
        except Exception:
            pass
        return False
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO books (id, book_name, display_name, file_id, file_unique_id, path, indexed)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET
                        book_name=EXCLUDED.book_name,
                        display_name=EXCLUDED.display_name,
                        file_id=EXCLUDED.file_id,
                        file_unique_id=EXCLUDED.file_unique_id,
                        path=EXCLUDED.path,
                        indexed=EXCLUDED.indexed
                    """,
                    (
                        book.get("id"),
                        book.get("book_name"),
                        book.get("display_name"),
                        book.get("file_id"),
                        book.get("file_unique_id"),
                        book.get("path"),
                        bool(book.get("indexed", False)),
                    ),
                )
                return True
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                return False


def bulk_upsert_books(books: list[dict]):
    if not books:
        return 0
    values = []
    skipped_adult = 0
    for b in books:
        if not b.get("id"):
            continue
        if is_book_adult_marked(b):
            skipped_adult += 1
            continue
        values.append(
            (
                b.get("id"),
                b.get("book_name"),
                b.get("display_name"),
                b.get("file_id"),
                b.get("file_unique_id"),
                b.get("path"),
                bool(b.get("indexed", False)),
            )
        )
    if not values:
        if skipped_adult:
            logger.info("bulk_upsert_books: skipped %s adult-marked book rows", skipped_adult)
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO books (id, book_name, display_name, file_id, file_unique_id, path, indexed)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    book_name=EXCLUDED.book_name,
                    display_name=EXCLUDED.display_name,
                    file_id=EXCLUDED.file_id,
                    file_unique_id=EXCLUDED.file_unique_id,
                    path=EXCLUDED.path,
                    indexed=EXCLUDED.indexed
                """,
                values,
            )
    if skipped_adult:
        logger.info("bulk_upsert_books: upserted %s rows, skipped %s adult-marked rows", len(values), skipped_adult)
    return len(values)


def update_book_file_id(book_id: str, file_id: str, indexed: bool = True, file_unique_id: str | None = None):
    fields = ["file_id=%s", "indexed=%s"]
    values = [file_id, indexed]
    if file_unique_id:
        fields.append("file_unique_id=%s")
        values.append(file_unique_id)
    values.append(book_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE books SET {', '.join(fields)} WHERE id=%s",
                values,
            )
            return cur.rowcount


def update_book_path(book_id: str, path: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE books SET path=%s WHERE id=%s", (path, book_id))
            return cur.rowcount


def update_book_names(book_id: str, book_name: str, display_name: str | None = None):
    book_name = str(book_name or "").strip()
    display_name = str(display_name or book_name or "").strip() or book_name
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE books SET book_name=%s, display_name=%s WHERE id=%s",
                (book_name, display_name, book_id),
            )
            return cur.rowcount


def update_book_rename_meta(
    book_id: str,
    book_name: str,
    display_name: str | None = None,
    path: str | None = None,
    file_id: str | None = None,
    file_unique_id: str | None = None,
    indexed: bool = True,
):
    book_name = str(book_name or "").strip()
    display_name = str(display_name or book_name or "").strip() or book_name
    fields = ["book_name=%s", "display_name=%s", "indexed=%s"]
    values = [book_name, display_name, indexed]
    if path is not None:
        fields.append("path=%s")
        values.append(str(path))
    if file_id is not None:
        fields.append("file_id=%s")
        values.append(str(file_id))
    if file_unique_id is not None:
        fields.append("file_unique_id=%s")
        values.append(str(file_unique_id))
    values.append(book_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE books SET {', '.join(fields)} WHERE id=%s",
                values,
            )
            return cur.rowcount


def enqueue_book_local_download_job(
    book_id: str,
    file_id: str,
    file_name: str,
    file_unique_id: str | None = None,
) -> str | None:
    book_id = str(book_id or "").strip()
    file_id = str(file_id or "").strip()
    file_name = str(file_name or "").strip()
    file_unique_id = str(file_unique_id or "").strip() or None
    if not book_id or not file_id or not file_name:
        return None
    job_id = uuid.uuid4().hex
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO book_local_download_jobs (
                    id, book_id, file_id, file_unique_id, file_name,
                    status, attempts, max_attempts, next_attempt_at,
                    locked_at, worker_id, last_error, completed_at,
                    created_at, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,'queued',0,12,NOW(),NULL,NULL,NULL,NULL,NOW(),NOW())
                ON CONFLICT (book_id) DO UPDATE SET
                    file_id=EXCLUDED.file_id,
                    file_unique_id=COALESCE(EXCLUDED.file_unique_id, book_local_download_jobs.file_unique_id),
                    file_name=EXCLUDED.file_name,
                    status='queued',
                    attempts=0,
                    next_attempt_at=NOW(),
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=NULL,
                    completed_at=NULL,
                    updated_at=NOW()
                RETURNING id
                """,
                (job_id, book_id, file_id, file_unique_id, file_name),
            )
            row = cur.fetchone()
            if not row:
                return job_id
            try:
                return str(row[0])
            except Exception:
                return job_id


def get_book_local_download_job(book_id: str) -> dict | None:
    book_id = str(book_id or "").strip()
    if not book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM book_local_download_jobs
                WHERE book_id=%s
                LIMIT 1
                """,
                (book_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def claim_book_local_download_job(worker_id: str, stale_after_seconds: int = 1800) -> dict | None:
    worker_id = str(worker_id or "").strip() or "worker"
    stale_after_seconds = max(60, int(stale_after_seconds or 1800))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH candidate AS (
                    SELECT id
                    FROM book_local_download_jobs
                    WHERE next_attempt_at <= NOW()
                      AND (
                          status = 'queued'
                          OR (
                              status = 'downloading'
                              AND locked_at IS NOT NULL
                              AND locked_at < NOW() - (%s * INTERVAL '1 second')
                          )
                      )
                    ORDER BY next_attempt_at ASC, created_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE book_local_download_jobs j
                SET
                    status='downloading',
                    attempts=attempts + 1,
                    locked_at=NOW(),
                    worker_id=%s,
                    updated_at=NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.*
                """,
                (stale_after_seconds, worker_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def complete_book_local_download_job(job_id: str) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE book_local_download_jobs
                SET status='done',
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=NULL,
                    completed_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (job_id,),
            )
            return cur.rowcount


def retry_book_local_download_job(job_id: str, error: str, retry_after_seconds: float = 60.0) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    retry_after_seconds = max(1.0, float(retry_after_seconds or 60.0))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE book_local_download_jobs
                SET status='queued',
                    next_attempt_at=NOW() + (%s * INTERVAL '1 second'),
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (retry_after_seconds, str(error or "")[:2000], job_id),
            )
            return cur.rowcount


def fail_book_local_download_job(job_id: str, error: str) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE book_local_download_jobs
                SET status='failed',
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (str(error or "")[:2000], job_id),
            )
            return cur.rowcount


def get_book_local_download_job_status_counts() -> dict[str, int]:
    counts = {
        "queued": 0,
        "downloading": 0,
        "done": 0,
        "failed": 0,
        "total": 0,
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(status, 'unknown') AS status, COUNT(*)::int
                FROM book_local_download_jobs
                GROUP BY COALESCE(status, 'unknown')
                """
            )
            for status, count in (cur.fetchall() or []):
                key = str(status or "").strip().lower()
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if key in counts:
                    counts[key] = value
                counts["total"] += value
    counts["pending"] = counts["queued"] + counts["downloading"]
    return counts


def enqueue_audio_book_part_local_download_job(
    audio_book_id: str,
    audio_book_part_id: str,
    file_id: str,
    file_name: str,
    file_unique_id: str | None = None,
    media_kind: str | None = None,
) -> str | None:
    audio_book_id = str(audio_book_id or "").strip()
    audio_book_part_id = str(audio_book_part_id or "").strip()
    file_id = str(file_id or "").strip()
    file_name = str(file_name or "").strip()
    file_unique_id = str(file_unique_id or "").strip() or None
    media_kind = str(media_kind or "").strip().lower() or None
    if not audio_book_id or not audio_book_part_id or not file_id or not file_name:
        return None
    job_id = uuid.uuid4().hex
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audio_book_local_download_jobs (
                    id, audio_book_id, audio_book_part_id, file_id, file_unique_id, file_name, media_kind,
                    status, attempts, max_attempts, next_attempt_at,
                    locked_at, worker_id, last_error, completed_at,
                    created_at, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,'queued',0,12,NOW(),NULL,NULL,NULL,NULL,NOW(),NOW())
                ON CONFLICT (audio_book_part_id) DO UPDATE SET
                    audio_book_id=EXCLUDED.audio_book_id,
                    file_id=EXCLUDED.file_id,
                    file_unique_id=COALESCE(EXCLUDED.file_unique_id, audio_book_local_download_jobs.file_unique_id),
                    file_name=EXCLUDED.file_name,
                    media_kind=COALESCE(EXCLUDED.media_kind, audio_book_local_download_jobs.media_kind),
                    status='queued',
                    attempts=0,
                    next_attempt_at=NOW(),
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=NULL,
                    completed_at=NULL,
                    updated_at=NOW()
                RETURNING id
                """,
                (job_id, audio_book_id, audio_book_part_id, file_id, file_unique_id, file_name, media_kind),
            )
            row = cur.fetchone()
            if not row:
                return job_id
            try:
                return str(row[0])
            except Exception:
                return job_id


def claim_audio_book_part_local_download_job(worker_id: str, stale_after_seconds: int = 1800) -> dict | None:
    worker_id = str(worker_id or "").strip() or "worker"
    stale_after_seconds = max(60, int(stale_after_seconds or 1800))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH candidate AS (
                    SELECT id
                    FROM audio_book_local_download_jobs
                    WHERE next_attempt_at <= NOW()
                      AND (
                          status = 'queued'
                          OR (
                              status = 'downloading'
                              AND locked_at IS NOT NULL
                              AND locked_at < NOW() - (%s * INTERVAL '1 second')
                          )
                      )
                    ORDER BY next_attempt_at ASC, created_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE audio_book_local_download_jobs j
                SET
                    status='downloading',
                    attempts=attempts + 1,
                    locked_at=NOW(),
                    worker_id=%s,
                    updated_at=NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.*
                """,
                (stale_after_seconds, worker_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def complete_audio_book_part_local_download_job(job_id: str) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audio_book_local_download_jobs
                SET status='done',
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=NULL,
                    completed_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (job_id,),
            )
            return cur.rowcount


def retry_audio_book_part_local_download_job(job_id: str, error: str, retry_after_seconds: float = 60.0) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    retry_after_seconds = max(1.0, float(retry_after_seconds or 60.0))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audio_book_local_download_jobs
                SET status='queued',
                    next_attempt_at=NOW() + (%s * INTERVAL '1 second'),
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (retry_after_seconds, str(error or "")[:2000], job_id),
            )
            return cur.rowcount


def fail_audio_book_part_local_download_job(job_id: str, error: str) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audio_book_local_download_jobs
                SET status='failed',
                    locked_at=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (str(error or "")[:2000], job_id),
            )
            return cur.rowcount


def get_audio_book_part_local_download_job_status_counts() -> dict[str, int]:
    counts = {
        "queued": 0,
        "downloading": 0,
        "done": 0,
        "failed": 0,
        "total": 0,
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(status, 'unknown') AS status, COUNT(*)::int
                FROM audio_book_local_download_jobs
                GROUP BY COALESCE(status, 'unknown')
                """
            )
            for status, count in (cur.fetchall() or []):
                key = str(status or "").strip().lower()
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if key in counts:
                    counts[key] = value
                counts["total"] += value
    counts["pending"] = counts["queued"] + counts["downloading"]
    return counts


def upsert_group_private_start_prompt(
    token: str,
    user_id: int,
    chat_id: int,
    message_id: int,
    prompt_lang: str,
    status: str = "pending",
) -> str | None:
    token = str(token or "").strip()
    prompt_lang = str(prompt_lang or "en").strip() or "en"
    status = str(status or "pending").strip().lower() or "pending"
    try:
        user_id = int(user_id)
        chat_id = int(chat_id)
        message_id = int(message_id)
    except Exception:
        return None
    if not token or not user_id or not chat_id or not message_id:
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO group_private_start_prompts (
                    token, user_id, chat_id, message_id, prompt_lang, status, last_error, resolved_at, created_at, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,NULL,NULL,NOW(),NOW())
                ON CONFLICT (token) DO UPDATE SET
                    user_id=EXCLUDED.user_id,
                    chat_id=EXCLUDED.chat_id,
                    message_id=EXCLUDED.message_id,
                    prompt_lang=EXCLUDED.prompt_lang,
                    status=EXCLUDED.status,
                    last_error=NULL,
                    resolved_at=NULL,
                    updated_at=NOW()
                RETURNING token
                """,
                (token, user_id, chat_id, message_id, prompt_lang, status),
            )
            row = cur.fetchone()
            return str(row[0]) if row and row[0] is not None else token


def get_group_private_start_prompt_by_token(token: str) -> dict | None:
    token = str(token or "").strip()
    if not token:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM group_private_start_prompts
                WHERE token=%s
                LIMIT 1
                """,
                (token,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_latest_pending_group_private_start_prompt(user_id: int) -> dict | None:
    try:
        user_id = int(user_id)
    except Exception:
        return None
    if not user_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM group_private_start_prompts
                WHERE user_id=%s AND status='pending'
                ORDER BY created_at DESC, updated_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def set_group_private_start_prompt_status(token: str, status: str, error: str | None = None) -> int:
    token = str(token or "").strip()
    status = str(status or "").strip().lower() or "pending"
    if not token:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE group_private_start_prompts
                SET status=%s,
                    last_error=%s,
                    resolved_at=CASE
                        WHEN %s IN ('resolved', 'done', 'completed') THEN COALESCE(resolved_at, NOW())
                        ELSE resolved_at
                    END,
                    updated_at=NOW()
                WHERE token=%s
                """,
                (status, str(error or "")[:2000] if error else None, status, token),
            )
            return cur.rowcount


def update_book_indexed(book_id: str, indexed: bool):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE books SET indexed=%s WHERE id=%s", (indexed, book_id))


def update_book_upload_meta(book_id: str, uploaded_by_user_id: int | None = None, upload_source: str | None = None):
    fields = []
    values = []
    if uploaded_by_user_id is not None:
        fields.append("uploaded_by_user_id=%s")
        values.append(int(uploaded_by_user_id))
    if upload_source is not None:
        fields.append("upload_source=%s")
        values.append(upload_source)
    if not fields:
        return 0
    values.append(book_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE books SET {', '.join(fields)} WHERE id=%s", values)
            return cur.rowcount


def set_bot_setting(key: str, value: str) -> int:
    key = str(key or "").strip()
    value = str(value or "").strip()
    if not key:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                (key, value),
            )
            return cur.rowcount


def get_bot_setting(key: str) -> str | None:
    key = str(key or "").strip()
    if not key:
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM bot_settings WHERE key=%s LIMIT 1", (key,))
            row = cur.fetchone()
            if not row:
                return None
            try:
                value = row[0]
            except Exception:
                value = None
            text = str(value or "").strip()
            return text or None


def delete_bot_setting(key: str) -> int:
    key = str(key or "").strip()
    if not key:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bot_settings WHERE key=%s", (key,))
            return cur.rowcount


def upsert_forbidden_books(entries: list[tuple[str, str]] | tuple[tuple[str, str], ...], created_by_user_id: int | None = None) -> int:
    clean_rows: list[tuple[str, str, int | None]] = []
    seen: set[str] = set()
    for normalized_title, title in entries or []:
        norm = str(normalized_title or "").strip()
        raw = str(title or "").strip()
        if not norm or not raw or norm in seen:
            continue
        seen.add(norm)
        clean_rows.append((norm, raw, int(created_by_user_id) if created_by_user_id is not None else None))
    if not clean_rows:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO forbidden_books (normalized_title, title, created_by_user_id, created_at, updated_at)
                VALUES %s
                ON CONFLICT (normalized_title) DO UPDATE SET
                    title = EXCLUDED.title,
                    created_by_user_id = EXCLUDED.created_by_user_id,
                    updated_at = NOW()
                """,
                clean_rows,
                template="(%s, %s, %s, NOW(), NOW())",
            )
            return len(clean_rows)


def get_forbidden_book_title(normalized_title: str) -> str | None:
    key = str(normalized_title or "").strip()
    if not key:
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title FROM forbidden_books WHERE normalized_title=%s LIMIT 1",
                (key,),
            )
            row = cur.fetchone()
            if not row:
                return None
            try:
                value = row[0]
            except Exception:
                value = None
            text = str(value or "").strip()
            return text or None


def list_forbidden_book_titles() -> list[str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT normalized_title FROM forbidden_books")
            rows = cur.fetchall() or []
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        try:
            value = row[0]
        except Exception:
            value = None
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def list_forbidden_books(limit: int | None = None) -> list[dict]:
    sql = """
        SELECT normalized_title, title, created_by_user_id, created_at, updated_at
        FROM forbidden_books
        ORDER BY LOWER(title) ASC, updated_at DESC
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (max(1, int(limit)),)
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    items: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        normalized_title = str((row or {}).get("normalized_title") or "").strip()
        title = str((row or {}).get("title") or "").strip()
        if not normalized_title or not title or normalized_title in seen:
            continue
        seen.add(normalized_title)
        items.append(
            {
                "normalized_title": normalized_title,
                "title": title,
                "created_by_user_id": (row or {}).get("created_by_user_id"),
                "created_at": (row or {}).get("created_at"),
                "updated_at": (row or {}).get("updated_at"),
            }
        )
    return items


def remove_forbidden_books(normalized_titles: list[str] | tuple[str, ...]) -> int:
    clean_titles: list[str] = []
    seen: set[str] = set()
    for item in normalized_titles or []:
        title = str(item or "").strip()
        if not title or title in seen:
            continue
        seen.add(title)
        clean_titles.append(title)
    if not clean_titles:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM forbidden_books WHERE normalized_title = ANY(%s)",
                (clean_titles,),
            )
            return int(cur.rowcount or 0)


def insert_upload_receipt(record: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO upload_receipts (
                    id, user_id, file_id, file_unique_id, file_name,
                    status, error, book_id, saved_to_db, saved_to_es
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                    user_id=EXCLUDED.user_id,
                    file_id=EXCLUDED.file_id,
                    file_unique_id=EXCLUDED.file_unique_id,
                    file_name=EXCLUDED.file_name,
                    status=EXCLUDED.status,
                    error=EXCLUDED.error,
                    book_id=EXCLUDED.book_id,
                    saved_to_db=EXCLUDED.saved_to_db,
                    saved_to_es=EXCLUDED.saved_to_es,
                    updated_at=NOW()
                """,
                (
                    record.get("id"),
                    record.get("user_id"),
                    record.get("file_id"),
                    record.get("file_unique_id"),
                    record.get("file_name"),
                    record.get("status") or "received",
                    record.get("error"),
                    record.get("book_id"),
                    bool(record.get("saved_to_db", False)),
                    bool(record.get("saved_to_es", False)),
                ),
            )
            return True


def update_upload_receipt(receipt_id: str, **fields):
    if not receipt_id:
        return 0
    set_parts = []
    values = []
    allowed = {
        "status",
        "error",
        "book_id",
        "saved_to_db",
        "saved_to_es",
        "file_id",
        "file_unique_id",
        "file_name",
        "user_id",
    }
    for key, value in fields.items():
        if key not in allowed:
            continue
        set_parts.append(f"{key}=%s")
        values.append(value)
    if not set_parts:
        return 0
    set_parts.append("updated_at=NOW()")
    values.append(receipt_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE upload_receipts SET {', '.join(set_parts)} WHERE id=%s", values)
            return cur.rowcount


def list_recent_upload_receipts(limit: int = 20, pending_only: bool = False):
    limit = max(1, min(int(limit or 20), 100))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if pending_only:
                cur.execute(
                    """
                    SELECT * FROM upload_receipts
                    WHERE NOT (COALESCE(saved_to_db, FALSE) = TRUE AND COALESCE(saved_to_es, FALSE) = TRUE)
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            else:
                cur.execute(
                    "SELECT * FROM upload_receipts ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
            return cur.fetchall()


def get_upload_receipt_by_id(receipt_id: str):
    receipt_id = str(receipt_id or "").strip()
    if not receipt_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM upload_receipts WHERE id=%s LIMIT 1", (receipt_id,))
            return cur.fetchone()


def update_book_by_path(path: str, file_id: str | None = None, indexed: bool | None = None):
    fields = []
    values = []
    if file_id is not None:
        fields.append("file_id=%s")
        values.append(file_id)
    if indexed is not None:
        fields.append("indexed=%s")
        values.append(indexed)
    if not fields:
        return 0
    values.append(path)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE books SET {', '.join(fields)} WHERE path=%s", values)
            return cur.rowcount


def get_audio_book_for_book(book_id: str) -> dict | None:
    if not book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM audio_books
                WHERE book_id=%s
                ORDER BY
                    display_order DESC NULLS LAST,
                    created_at DESC
                LIMIT 1
                """,
                (book_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def list_audio_books_by_book_id(book_id: str) -> list[dict]:
    if not book_id:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM audio_books
                WHERE book_id=%s
                ORDER BY
                    display_order DESC NULLS LAST,
                    created_at DESC
                """,
                (book_id,),
            )
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]


def get_audio_book_by_id(audio_book_id: str) -> dict | None:
    if not audio_book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM audio_books WHERE id=%s LIMIT 1",
                (audio_book_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def create_audio_book_for_book(
    *,
    book_id: str,
    title: str | None = None,
    display_title: str | None = None,
    language: str | None = None,
    performer: str | None = None,
    uploaded_by_user_id: int | None = None,
    upload_source: str | None = None,
) -> str:
    # Reuse existing audiobook for this book when one is already present.
    existing = get_audio_book_for_book(book_id)
    if existing and existing.get("id"):
        return str(existing["id"])

    audio_book_id = uuid.uuid4().hex
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Simple display_order - just use timestamp to ensure uniqueness
                cur.execute(
                    """
                    INSERT INTO audio_books (
                        id, book_id, title, display_title, language, performer,
                        total_duration_seconds, part_count, downloads, searches,
                        uploaded_by_user_id, upload_source, display_order
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,0,0,0,0,%s,%s,EXTRACT(EPOCH FROM NOW())::bigint)
                    """,
                    (
                        audio_book_id,
                        book_id,
                        title,
                        display_title,
                        language,
                        performer,
                        uploaded_by_user_id,
                        upload_source,
                    ),
                )
                return audio_book_id
    except Exception as e:
        # Legacy DBs may still have a unique rule on book_id. Reuse existing row on conflict.
        msg = str(e).lower()
        unique_book_conflict = (
            "uniq_audio_books_book_id" in msg
            or ("duplicate" in msg and "audio_books" in msg and "book_id" in msg)
        )
        if unique_book_conflict:
            existing = get_audio_book_for_book(book_id)
            if existing and existing.get("id"):
                return str(existing["id"])
        raise


def insert_audio_book_part(
    *,
    audio_book_id: str,
    part_index: int,
    title: str | None,
    media_kind: str | None = None,
    file_id: str,
    file_unique_id: str | None,
    path: str | None,
    duration_seconds: int | None,
    channel_id: int | None = None,
    channel_message_id: int | None = None,
) -> str:
    part_id = uuid.uuid4().hex
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audio_book_parts (
                    id, audio_book_id, part_index, title, media_kind,
                    file_id, file_unique_id, path, duration_seconds,
                    channel_id, channel_message_id, display_order
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    (SELECT display_order FROM audio_books WHERE id=%s) + %s
                )
                """,
                (
                    part_id,
                    audio_book_id,
                    int(part_index),
                    title,
                    media_kind,
                    file_id,
                    file_unique_id,
                    path,
                    duration_seconds,
                    channel_id,
                    channel_message_id,
                    audio_book_id,
                    int(part_index),
                ),
            )
            # Update aggregate stats on parent audiobook
            cur.execute(
                """
                UPDATE audio_books
                SET
                    part_count = (
                        SELECT COUNT(*) FROM audio_book_parts WHERE audio_book_id=%s
                    ),
                    total_duration_seconds = (
                        SELECT COALESCE(SUM(duration_seconds), 0)
                        FROM audio_book_parts
                        WHERE audio_book_id=%s
                    )
                WHERE id=%s
                """,
                (audio_book_id, audio_book_id, audio_book_id),
            )
    return part_id


def list_audio_book_parts(audio_book_id: str) -> list[dict]:
    if not audio_book_id:
        return []
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT *
                FROM audio_book_parts
                WHERE audio_book_id=%s
                ORDER BY part_index ASC, created_at ASC
                """,
                (audio_book_id,),
            )
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]


def get_audio_book_part(part_id: str) -> dict | None:
    if not part_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM audio_book_parts WHERE id=%s LIMIT 1",
                (part_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def update_audio_book_part_media(
    part_id: str,
    file_id: str | None = None,
    file_unique_id: str | None = None,
    path: str | None = None,
    title: str | None = None,
    media_kind: str | None = None,
    duration_seconds: int | None = None,
    channel_id: int | None = None,
    channel_message_id: int | None = None,
):
    if not part_id:
        return 0
    fields: list[str] = []
    values: list[Any] = []
    if file_id is not None:
        fields.append("file_id=%s")
        values.append(str(file_id))
    if file_unique_id is not None:
        fields.append("file_unique_id=%s")
        values.append(str(file_unique_id))
    if path is not None:
        fields.append("path=%s")
        values.append(str(path))
    if title is not None:
        fields.append("title=%s")
        values.append(str(title))
    if media_kind is not None:
        fields.append("media_kind=%s")
        values.append(str(media_kind))
    if duration_seconds is not None:
        fields.append("duration_seconds=%s")
        values.append(int(duration_seconds))
    if channel_id is not None:
        fields.append("channel_id=%s")
        values.append(int(channel_id))
    if channel_message_id is not None:
        fields.append("channel_message_id=%s")
        values.append(int(channel_message_id))
    if not fields:
        return 0
    values.append(str(part_id))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE audio_book_parts SET {', '.join(fields)} WHERE id=%s",
                values,
            )
            return cur.rowcount


def get_audio_book_part_by_file_unique_id(file_unique_id: str) -> dict | None:
    """Get audio part by file_unique_id to detect duplicates."""
    if not file_unique_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM audio_book_parts WHERE file_unique_id=%s LIMIT 1",
                (file_unique_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def get_audio_book_part_by_file_unique_id_and_audio_book(file_unique_id: str, audio_book_id: str) -> dict | None:
    """Get audio part by file_unique_id and audio_book_id to detect duplicates within same audiobook only."""
    if not file_unique_id or not audio_book_id:
        return None
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM audio_book_parts WHERE file_unique_id=%s AND audio_book_id=%s LIMIT 1",
                (file_unique_id, audio_book_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def delete_audio_book_part(part_id: str) -> int:
    """Delete a single audiobook part. Returns number of rows deleted."""
    if not part_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT audio_book_id FROM audio_book_parts WHERE id=%s", (part_id,))
            row = cur.fetchone()
            audio_book_id = row[0] if row else None
            cur.execute("DELETE FROM audio_book_parts WHERE id=%s", (part_id,))
            deleted = cur.rowcount
            if deleted and audio_book_id:
                # Recompute aggregates on parent audiobook
                cur.execute(
                    """
                    UPDATE audio_books
                    SET
                        part_count = (
                            SELECT COUNT(*) FROM audio_book_parts WHERE audio_book_id=%s
                        ),
                        total_duration_seconds = (
                            SELECT COALESCE(SUM(duration_seconds), 0)
                            FROM audio_book_parts
                            WHERE audio_book_id=%s
                        )
                    WHERE id=%s
                    """,
                    (audio_book_id, audio_book_id, audio_book_id),
                )
            return deleted


def shift_audio_book_parts_from(audio_book_id: str, from_index: int) -> int:
    """Increment part_index by 1 for all parts with part_index >= from_index.
    Used to make room for a new part inserted at a specific position."""
    if not audio_book_id or from_index < 1:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Avoid unique index collisions on (audio_book_id, part_index) by doing a two-phase shift.
            # Phase 1: move the range up by a dynamic offset above current max(part_index).
            cur.execute(
                "SELECT COALESCE(MAX(part_index), 0) FROM audio_book_parts WHERE audio_book_id=%s",
                (audio_book_id,),
            )
            row = cur.fetchone()
            max_index = int((row[0] if row else 0) or 0)
            offset = max(1000000, max_index + 1)
            cur.execute(
                """
                UPDATE audio_book_parts
                SET
                    part_index = part_index + %s,
                    display_order = COALESCE(display_order, 0) + %s
                WHERE audio_book_id = %s AND part_index >= %s
                """,
                (offset, offset, audio_book_id, from_index),
            )
            moved = cur.rowcount
            if not moved:
                return 0
            # Phase 2: bring it back down to net +1.
            cur.execute(
                """
                UPDATE audio_book_parts
                SET
                    part_index = part_index - %s,
                    display_order = COALESCE(display_order, 0) - %s
                WHERE audio_book_id = %s AND part_index >= %s
                """,
                (offset - 1, offset - 1, audio_book_id, from_index + offset),
            )
            return moved


def backfill_audio_display_orders() -> dict:
    """Populate display_order for existing audiobooks and parts.
    Returns counts of updated records."""
    result = {"audiobooks_updated": 0, "parts_updated": 0}
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Backfill audio_books display_order using book creation time + book ID hash
            cur.execute(
                """
                UPDATE audio_books ab
                SET display_order = (
                    SELECT 
                        FLOOR(EXTRACT(EPOCH FROM b.created_at) * 1000) * 1000000 + 
                        (CAST(('x' || SUBSTRING(md5(b.id), 1, 8)) AS BIGINT) % 1000000)
                    FROM books b WHERE b.id = ab.book_id
                )
                WHERE ab.display_order IS NULL
                """
            )
            result["audiobooks_updated"] = cur.rowcount

            # Backfill audio_book_parts display_order using book creation time + book ID hash + part_index
            cur.execute(
                """
                UPDATE audio_book_parts p
                SET display_order = (
                    SELECT 
                        FLOOR(EXTRACT(EPOCH FROM b.created_at) * 1000) * 1000000 + 
                        (CAST(('x' || SUBSTRING(md5(b.id), 1, 8)) AS BIGINT) % 1000000)
                    FROM books b 
                    JOIN audio_books ab ON b.id = ab.book_id 
                    WHERE ab.id = p.audio_book_id
                ) * 1000000 + p.part_index
                WHERE p.display_order IS NULL
                """
            )
            result["parts_updated"] = cur.rowcount
    return result


def delete_audio_book(audio_book_id: str) -> int:
    """Delete entire audiobook and all its parts. Returns rows deleted."""
    if not audio_book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Cascade delete is handled via FK ON DELETE CASCADE
            cur.execute("DELETE FROM audio_books WHERE id=%s", (audio_book_id,))
            return cur.rowcount


def delete_audio_books_by_book_id(book_id: str) -> int:
    """Delete all audiobooks for a given book (cascades to parts)."""
    if not book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Cascade delete is handled via FK ON DELETE CASCADE
            cur.execute("DELETE FROM audio_books WHERE book_id=%s", (book_id,))
            return cur.rowcount


def get_audio_book_stats() -> dict:
    """Get comprehensive audiobook statistics."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Total audiobooks and parts
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT ab.id) as total_audiobooks,
                    COUNT(abp.id) as total_parts,
                    COALESCE(SUM(ab.part_count), 0) as total_parts_in_books,
                    COALESCE(SUM(ab.downloads), 0) as total_downloads,
                    COALESCE(SUM(ab.searches), 0) as total_searches,
                    COALESCE(SUM(ab.total_duration_seconds), 0) as total_duration_seconds
                FROM audio_books ab
                LEFT JOIN audio_book_parts abp ON ab.id = abp.audio_book_id
            """)
            row = cur.fetchone()
            
            stats = {
                'total_audiobooks': row[0] or 0,
                'total_parts': row[1] or 0,
                'total_parts_in_books': row[2] or 0,
                'total_downloads': row[3] or 0,
                'total_searches': row[4] or 0,
                'total_duration_seconds': row[5] or 0,
            }
            
            # Books with audiobooks
            cur.execute("SELECT COUNT(DISTINCT book_id) FROM audio_books")
            stats['books_with_audiobooks'] = cur.fetchone()[0] or 0
            
            return stats


def delete_books_by_ids(ids: list[str]):
    if not ids:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM books WHERE id = ANY(%s)", (ids,))
            return cur.rowcount


def delete_book_and_related(book_id: str) -> int:
    if not book_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Delete audiobook references first (foreign key constraint)
            cur.execute("DELETE FROM audio_books WHERE book_id=%s", (book_id,))
            # Delete other related records
            cur.execute("DELETE FROM book_local_download_jobs WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM upload_receipts WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_summaries WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM user_favorites WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM user_favorite_awards WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM user_recents WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_counter_adjustments WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_reaction_adjustments WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_reaction_policies WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_comment_aliases WHERE book_id=%s", (book_id,))
            cur.execute(
                """
                DELETE FROM book_comment_relay_messages
                WHERE book_id=%s
                """,
                (book_id,),
            )
            cur.execute(
                """
                DELETE FROM book_comment_relay_participants
                WHERE conversation_id IN (
                    SELECT id FROM book_comment_relay_conversations WHERE book_id=%s
                )
                """,
                (book_id,),
            )
            cur.execute(
                """
                DELETE FROM book_comment_relay_conversations
                WHERE book_id=%s
                """,
                (book_id,),
            )
            cur.execute(
                """
                DELETE FROM book_comment_identity_requests
                WHERE comment_id IN (SELECT id FROM book_comments WHERE book_id=%s)
                """,
                (book_id,),
            )
            cur.execute(
                """
                DELETE FROM book_comment_reports
                WHERE comment_id IN (SELECT id FROM book_comments WHERE book_id=%s)
                """,
                (book_id,),
            )
            cur.execute("DELETE FROM book_comments WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM book_reactions WHERE book_id=%s", (book_id,))
            cur.execute("DELETE FROM user_reaction_awards WHERE book_id=%s", (book_id,))
            cur.execute("UPDATE book_requests SET book_id=NULL WHERE book_id=%s", (book_id,))
            # Finally delete the book
            cur.execute("DELETE FROM books WHERE id=%s", (book_id,))
            return cur.rowcount


# --- Book Requests ---

def list_requests():
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM book_requests")
            return cur.fetchall()


def list_requests_for_user(user_id: int):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM book_requests WHERE user_id=%s", (user_id,))
            return cur.fetchall()


def get_request_by_id(request_id: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM book_requests WHERE id=%s", (request_id,))
            return cur.fetchone()


def insert_request(record: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO book_requests (
                    id, user_id, username, first_name, last_name, query, query_norm, language,
                    status, created_at, created_ts, updated_at, status_by, status_by_name,
                    admin_chat_id, admin_message_id, admin_note, fulfilled_at, book_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    record.get("id"),
                    record.get("user_id"),
                    record.get("username"),
                    record.get("first_name"),
                    record.get("last_name"),
                    record.get("query"),
                    record.get("query_norm"),
                    record.get("language"),
                    record.get("status"),
                    record.get("created_at"),
                    record.get("created_ts"),
                    record.get("updated_at"),
                    record.get("status_by"),
                    record.get("status_by_name"),
                    record.get("admin_chat_id"),
                    record.get("admin_message_id"),
                    record.get("admin_note"),
                    record.get("fulfilled_at"),
                    record.get("book_id"),
                ),
            )


def update_request(record: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE book_requests SET
                    user_id=%s, username=%s, first_name=%s, last_name=%s, query=%s, query_norm=%s,
                    language=%s, status=%s, created_at=%s, created_ts=%s, updated_at=%s,
                    status_by=%s, status_by_name=%s, admin_chat_id=%s, admin_message_id=%s,
                    admin_note=%s, fulfilled_at=%s, book_id=%s
                WHERE id=%s
                """,
                (
                    record.get("user_id"),
                    record.get("username"),
                    record.get("first_name"),
                    record.get("last_name"),
                    record.get("query"),
                    record.get("query_norm"),
                    record.get("language"),
                    record.get("status"),
                    record.get("created_at"),
                    record.get("created_ts"),
                    record.get("updated_at"),
                    record.get("status_by"),
                    record.get("status_by_name"),
                    record.get("admin_chat_id"),
                    record.get("admin_message_id"),
                    record.get("admin_note"),
                    record.get("fulfilled_at"),
                    record.get("book_id"),
                    record.get("id"),
                ),
            )


def delete_request(request_id: str):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM book_requests WHERE id=%s", (request_id,))
            return cur.rowcount


def set_request_status(request_id: str, status: str, updated_at: str | None, status_by: int | None,
                       status_by_name: str | None, admin_chat_id: int | None,
                       admin_message_id: int | None, admin_note: str | None,
                       fulfilled_at: str | None, book_id: str | None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE book_requests SET
                    status=%s, updated_at=%s, status_by=%s, status_by_name=%s,
                    admin_chat_id=%s, admin_message_id=%s, admin_note=%s,
                    fulfilled_at=%s, book_id=%s
                WHERE id=%s
                """,
                (
                    status, updated_at, status_by, status_by_name,
                    admin_chat_id, admin_message_id, admin_note,
                    fulfilled_at, book_id, request_id,
                ),
            )


# --- Upload Requests ---

def list_upload_requests():
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM upload_requests")
            return cur.fetchall()


def get_upload_request_by_id(request_id: str):
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM upload_requests WHERE id=%s", (request_id,))
            return cur.fetchone()


def insert_upload_request(record: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO upload_requests (
                    id, user_id, username, first_name, last_name, language, status,
                    created_at, created_ts, updated_at, status_by, status_by_name,
                    admin_chat_id, admin_message_id, admin_note
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    record.get("id"),
                    record.get("user_id"),
                    record.get("username"),
                    record.get("first_name"),
                    record.get("last_name"),
                    record.get("language"),
                    record.get("status"),
                    record.get("created_at"),
                    record.get("created_ts"),
                    record.get("updated_at"),
                    record.get("status_by"),
                    record.get("status_by_name"),
                    record.get("admin_chat_id"),
                    record.get("admin_message_id"),
                    record.get("admin_note"),
                ),
            )


def update_upload_request(record: dict):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE upload_requests SET
                    user_id=%s, username=%s, first_name=%s, last_name=%s, language=%s,
                    status=%s, created_at=%s, created_ts=%s, updated_at=%s,
                    status_by=%s, status_by_name=%s, admin_chat_id=%s,
                    admin_message_id=%s, admin_note=%s
                WHERE id=%s
                """,
                (
                    record.get("user_id"),
                    record.get("username"),
                    record.get("first_name"),
                    record.get("last_name"),
                    record.get("language"),
                    record.get("status"),
                    record.get("created_at"),
                    record.get("created_ts"),
                    record.get("updated_at"),
                    record.get("status_by"),
                    record.get("status_by_name"),
                    record.get("admin_chat_id"),
                    record.get("admin_message_id"),
                    record.get("admin_note"),
                    record.get("id"),
                ),
            )


def set_upload_request_status(request_id: str, status: str, updated_at: str | None,
                              status_by: int | None, status_by_name: str | None,
                              admin_chat_id: int | None, admin_message_id: int | None,
                              admin_note: str | None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE upload_requests SET
                    status=%s, updated_at=%s, status_by=%s, status_by_name=%s,
                    admin_chat_id=%s, admin_message_id=%s, admin_note=%s
                WHERE id=%s
                """,
                (status, updated_at, status_by, status_by_name, admin_chat_id, admin_message_id, admin_note, request_id),
            )
            return cur.rowcount


def get_storage_stats() -> dict:
    """Get storage usage statistics from local files and DB metadata."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            def _sum_local_file_sizes(table_name: str) -> tuple[int, int]:
                cur.execute(f"SELECT path FROM {table_name} WHERE path IS NOT NULL AND path <> ''")
                local_count = 0
                total_size = 0
                while True:
                    rows = cur.fetchmany(2000)
                    if not rows:
                        break
                    for row in rows:
                        try:
                            path = str(row[0] or "").strip()
                        except Exception:
                            path = ""
                        if not path or not os.path.isfile(path):
                            continue
                        local_count += 1
                        try:
                            total_size += int(os.path.getsize(path))
                        except Exception:
                            continue
                return local_count, total_size

            cur.execute("SELECT COUNT(*) FROM books")
            book_db_count = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(*) FROM audio_book_parts")
            audio_db_count = int((cur.fetchone() or [0])[0] or 0)

            local_book_count, local_book_size = _sum_local_file_sizes("books")
            local_audio_count, local_audio_size = _sum_local_file_sizes("audio_book_parts")

            stats = {
                "book_count": int(local_book_count),
                "book_db_count": book_db_count,
                "total_book_size": int(local_book_size),
                "avg_book_size": int(local_book_size / local_book_count) if local_book_count > 0 else 0,
                "audio_count": int(local_audio_count),
                "audio_db_count": audio_db_count,
                "total_audio_size": int(local_audio_size),
                "avg_audio_size": int(local_audio_size / local_audio_count) if local_audio_count > 0 else 0,
            }

            stats["total_size"] = stats["total_book_size"] + stats["total_audio_size"]
            stats["total_files"] = stats["book_count"] + stats["audio_count"]
            return stats


# Background Jobs

def get_background_job_user_usage(user_id: int) -> dict[str, int]:
    usage = {"pending": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0, "total": 0}
    try:
        user_id = int(user_id or 0)
    except Exception:
        user_id = 0
    if not user_id:
        return usage
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT UPPER(COALESCE(status, 'PENDING')) AS status_key, COUNT(*)::int
                FROM background_jobs
                WHERE user_id = %s
                GROUP BY UPPER(COALESCE(status, 'PENDING'))
                """,
                (user_id,),
            )
            for status_key, count in (cur.fetchall() or []):
                key = _normalize_background_job_status(status_key)
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if key == BG_STATUS_PENDING:
                    usage["pending"] += value
                elif key == BG_STATUS_RUNNING:
                    usage["running"] += value
                elif key == BG_STATUS_DONE:
                    usage["done"] += value
                elif key == BG_STATUS_FAILED:
                    usage["failed"] += value
                elif key == BG_STATUS_CANCELLED:
                    usage["cancelled"] += value
                usage["total"] += value
    return usage


def create_background_job(
    job_type: str,
    user_id: int,
    data: dict | None,
    *,
    chat_id: int | None = None,
    message_id: int | None = None,
    priority: int = 100,
    max_attempts: int = 3,
    idempotency_key: str | None = None,
    ignore_limits: bool = False,
) -> dict[str, Any]:
    job_type = str(job_type or "").strip()
    try:
        user_id = int(user_id or 0)
    except Exception:
        user_id = 0
    if not job_type or not user_id:
        return {"ok": False, "job_id": None, "reason": "invalid"}

    payload = data or {}
    data_json, payload_json = serialize_background_job_payload(payload)
    priority = max(1, int(priority or 100))
    max_attempts = max(1, int(max_attempts or 3))
    idem = str(idempotency_key or "").strip() or None
    bypass_limits = bool(ignore_limits) or _background_job_user_has_limit_bypass(user_id)
    pending_limit = _background_job_limit_pending()
    running_limit = _background_job_limit_running()

    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if idem:
                cur.execute(
                    """
                    SELECT id, status
                    FROM background_jobs
                    WHERE user_id = %s
                      AND idempotency_key = %s
                      AND UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'RUNNING', 'QUEUED', 'PROCESSING')
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (user_id, idem),
                )
                existing = cur.fetchone()
                if existing:
                    return {
                        "ok": True,
                        "job_id": str(existing.get("id") or "").strip() or None,
                        "reason": "existing",
                    }

            if not bypass_limits:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'QUEUED')
                        )::int AS pending_count,
                        COUNT(*) FILTER (
                            WHERE UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING')
                        )::int AS running_count
                    FROM background_jobs
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                row = cur.fetchone() or {}
                pending_count = int(row.get("pending_count") or 0)
                running_count = int(row.get("running_count") or 0)
                if running_count >= running_limit:
                    return {
                        "ok": False,
                        "job_id": None,
                        "reason": "running_limit",
                        "pending_count": pending_count,
                        "running_count": running_count,
                    }
                if pending_count >= pending_limit:
                    return {
                        "ok": False,
                        "job_id": None,
                        "reason": "pending_limit",
                        "pending_count": pending_count,
                        "running_count": running_count,
                    }

            job_id = uuid.uuid4().hex
            cur.execute(
                """
                INSERT INTO background_jobs (
                    id,
                    job_type,
                    user_id,
                    chat_id,
                    message_id,
                    data_json,
                    payload_json,
                    status,
                    priority,
                    progress,
                    attempts,
                    max_attempts,
                    idempotency_key,
                    next_attempt_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s::jsonb,
                    %s, %s, 0, 0, %s, %s, NOW(), NOW(), NOW()
                )
                """,
                (
                    job_id,
                    job_type,
                    user_id,
                    int(chat_id) if chat_id is not None else None,
                    int(message_id) if message_id is not None else None,
                    data_json,
                    payload_json,
                    BG_STATUS_PENDING,
                    priority,
                    max_attempts,
                    idem,
                ),
            )
    logger.info(
        "background job created: job_id=%s job_type=%s user_id=%s chat_id=%s priority=%s",
        job_id,
        job_type,
        user_id,
        chat_id,
        priority,
    )
    return {"ok": True, "job_id": job_id, "reason": None}


def enqueue_background_job(
    job_type: str,
    user_id: int,
    data: dict,
    *,
    chat_id: int | None = None,
    message_id: int | None = None,
    priority: int = 100,
    max_attempts: int = 3,
    idempotency_key: str | None = None,
    ignore_limits: bool = False,
    return_meta: bool = False,
) -> str | dict[str, Any] | None:
    meta = create_background_job(
        job_type,
        user_id,
        data,
        chat_id=chat_id,
        message_id=message_id,
        priority=priority,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        ignore_limits=ignore_limits,
    )
    if return_meta:
        return meta
    if meta.get("ok"):
        return str(meta.get("job_id") or "").strip() or None
    return None


def claim_background_job(
    worker_id: str,
    stale_after_seconds: int = 1800,
    allowed_job_types: list[str] | tuple[str, ...] | None = None,
    excluded_job_types: list[str] | tuple[str, ...] | None = None,
) -> dict | None:
    worker_id = str(worker_id or "").strip() or "worker"
    stale_after_seconds = max(60, int(stale_after_seconds or 1800))
    allowed_job_types = [str(item or "").strip() for item in (allowed_job_types or []) if str(item or "").strip()]
    excluded_job_types = [str(item or "").strip() for item in (excluded_job_types or []) if str(item or "").strip()]
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH candidate AS (
                    SELECT id
                    FROM background_jobs
                    WHERE COALESCE(next_attempt_at, NOW()) <= NOW()
                      AND (
                          COALESCE(array_length(%s::text[], 1), 0) = 0
                          OR job_type = ANY(%s)
                      )
                      AND (
                          COALESCE(array_length(%s::text[], 1), 0) = 0
                          OR NOT (job_type = ANY(%s))
                      )
                      AND (
                          UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'QUEUED')
                          OR (
                              UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING')
                              AND locked_at IS NOT NULL
                              AND locked_at < NOW() - (%s * INTERVAL '1 second')
                          )
                      )
                    ORDER BY priority ASC, next_attempt_at ASC, created_at ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE background_jobs j
                SET
                    status=%s,
                    attempts=attempts + 1,
                    progress=GREATEST(COALESCE(progress, 0), 1),
                    locked_at=NOW(),
                    locked_by=%s,
                    worker_id=%s,
                    started_at=COALESCE(j.started_at, NOW()),
                    updated_at=NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.*
                """,
                (
                    allowed_job_types,
                    allowed_job_types,
                    excluded_job_types,
                    excluded_job_types,
                    stale_after_seconds,
                    BG_STATUS_RUNNING,
                    worker_id,
                    worker_id,
                ),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def complete_background_job(job_id: str, result: dict | None = None) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    result_json, _ = serialize_background_job_payload(result or {})
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    result_json=CASE
                        WHEN %s::jsonb = '{}'::jsonb AND result_json IS NOT NULL THEN result_json
                        ELSE %s::jsonb
                    END,
                    progress=100,
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    started_at=COALESCE(started_at, NOW()),
                    last_error=NULL,
                    error_message=NULL,
                    finished_at=NOW(),
                    completed_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (BG_STATUS_DONE, result_json, result_json, job_id),
            )
            return cur.rowcount


def update_background_job_progress(job_id: str, progress: int, result: dict | None = None) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    progress = max(0, min(100, int(progress or 0)))
    result_json, _ = serialize_background_job_payload(result or {})
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET progress=%s,
                    result_json=CASE
                        WHEN %s::jsonb = '{}'::jsonb AND result_json IS NOT NULL THEN result_json
                        ELSE %s::jsonb
                    END,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (progress, result_json, result_json, job_id),
            )
            return cur.rowcount


def retry_background_job(job_id: str, error: str, retry_after_seconds: float = 60.0) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    retry_after_seconds = max(1.0, float(retry_after_seconds or 60.0))
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    next_attempt_at=NOW() + (%s * INTERVAL '1 second'),
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    error_message=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (
                    BG_STATUS_PENDING,
                    retry_after_seconds,
                    str(error or "")[:2000],
                    str(error or "")[:2000],
                    job_id,
                ),
            )
            return cur.rowcount


def fail_background_job(job_id: str, error: str) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    error_message=%s,
                    finished_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (BG_STATUS_FAILED, str(error or "")[:2000], str(error or "")[:2000], job_id),
            )
            return cur.rowcount


def cancel_background_job(job_id: str, error: str | None = None) -> int:
    job_id = str(job_id or "").strip()
    if not job_id:
        return 0
    text = str(error or "cancelled")[:2000]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    last_error=%s,
                    error_message=%s,
                    finished_at=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (BG_STATUS_CANCELLED, text, text, job_id),
            )
            return cur.rowcount


def recover_stale_background_jobs(lock_timeout_minutes: int = 30) -> dict[str, int]:
    lock_timeout_minutes = max(1, int(lock_timeout_minutes or 30))
    recovered = 0
    failed = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    next_attempt_at=NOW(),
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    last_error=COALESCE(last_error, 'stale worker lock recovered'),
                    error_message=COALESCE(error_message, 'stale worker lock recovered'),
                    updated_at=NOW()
                WHERE UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING')
                  AND locked_at IS NOT NULL
                  AND locked_at < NOW() - (%s * INTERVAL '1 minute')
                  AND attempts < max_attempts
                """,
                (BG_STATUS_PENDING, lock_timeout_minutes),
            )
            recovered = int(cur.rowcount or 0)
            cur.execute(
                """
                UPDATE background_jobs
                SET status=%s,
                    locked_at=NULL,
                    locked_by=NULL,
                    worker_id=NULL,
                    last_error=COALESCE(last_error, 'stale worker lock exceeded max attempts'),
                    error_message=COALESCE(error_message, 'stale worker lock exceeded max attempts'),
                    finished_at=NOW(),
                    updated_at=NOW()
                WHERE UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING')
                  AND locked_at IS NOT NULL
                  AND locked_at < NOW() - (%s * INTERVAL '1 minute')
                  AND attempts >= max_attempts
                """,
                (BG_STATUS_FAILED, lock_timeout_minutes),
            )
            failed = int(cur.rowcount or 0)
    return {"recovered": recovered, "failed": failed}


def get_background_job_admin_summary(lock_timeout_minutes: int = 30) -> dict[str, Any]:
    summary = {
        "pending": 0,
        "running": 0,
        "failed": 0,
        "oldest_pending_seconds": None,
        "stuck_jobs": 0,
    }
    lock_timeout_minutes = max(1, int(lock_timeout_minutes or 30))
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'QUEUED'))::int AS pending_count,
                    COUNT(*) FILTER (WHERE UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING'))::int AS running_count,
                    COUNT(*) FILTER (WHERE UPPER(COALESCE(status, 'PENDING')) = 'FAILED')::int AS failed_count,
                    COUNT(*) FILTER (
                        WHERE UPPER(COALESCE(status, 'PENDING')) IN ('RUNNING', 'PROCESSING')
                          AND locked_at IS NOT NULL
                          AND locked_at < NOW() - (%s * INTERVAL '1 minute')
                    )::int AS stuck_count,
                    EXTRACT(EPOCH FROM (
                        NOW() - MIN(created_at) FILTER (
                            WHERE UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'QUEUED')
                        )
                    ))::bigint AS oldest_pending_seconds
                FROM background_jobs
                WHERE UPPER(COALESCE(status, 'PENDING')) IN ('PENDING', 'QUEUED', 'RUNNING', 'PROCESSING', 'FAILED')
                """,
                (lock_timeout_minutes,),
            )
            row = cur.fetchone() or {}
            summary["pending"] = int(row.get("pending_count") or 0)
            summary["running"] = int(row.get("running_count") or 0)
            summary["failed"] = int(row.get("failed_count") or 0)
            summary["stuck_jobs"] = int(row.get("stuck_count") or 0)
            oldest = row.get("oldest_pending_seconds")
            summary["oldest_pending_seconds"] = int(oldest) if oldest is not None else None
    return summary


def get_background_job_status_counts() -> dict[str, int]:
    counts = {
        "queued": 0,
        "processing": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
        "total": 0,
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(status, 'unknown') AS status, COUNT(*)::int
                FROM background_jobs
                GROUP BY COALESCE(status, 'unknown')
                """
            )
            for status, count in (cur.fetchall() or []):
                key = _normalize_background_job_status(status)
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if key == BG_STATUS_PENDING:
                    counts["queued"] += value
                elif key == BG_STATUS_RUNNING:
                    counts["processing"] += value
                elif key == BG_STATUS_DONE:
                    counts["completed"] += value
                elif key == BG_STATUS_FAILED:
                    counts["failed"] += value
                elif key == BG_STATUS_CANCELLED:
                    counts["cancelled"] += value
                counts["total"] += value
    counts["pending"] = counts["queued"] + counts["processing"]
    counts["running"] = counts["processing"]
    counts["done"] = counts["completed"]
    return counts


def get_background_job_status_counts_by_type() -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(job_type, 'unknown') AS job_type,
                    COALESCE(status, 'unknown') AS status,
                    COUNT(*)::int
                FROM background_jobs
                GROUP BY COALESCE(job_type, 'unknown'), COALESCE(status, 'unknown')
                """
            )
            for job_type, status, count in (cur.fetchall() or []):
                key = str(job_type or "").strip() or "unknown"
                status_key = str(status or "").strip().lower() or "unknown"
                bucket = summary.setdefault(
                    key,
                    {
                        "queued": 0,
                        "processing": 0,
                        "completed": 0,
                        "failed": 0,
                        "cancelled": 0,
                        "total": 0,
                    },
                )
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                normalized = _normalize_background_job_status(status_key)
                if normalized == BG_STATUS_PENDING:
                    bucket["queued"] += value
                elif normalized == BG_STATUS_RUNNING:
                    bucket["processing"] += value
                elif normalized == BG_STATUS_DONE:
                    bucket["completed"] += value
                elif normalized == BG_STATUS_FAILED:
                    bucket["failed"] += value
                elif normalized == BG_STATUS_CANCELLED:
                    bucket["cancelled"] += value
                bucket["total"] += value
    for bucket in summary.values():
        bucket["pending"] = bucket.get("queued", 0) + bucket.get("processing", 0)
        bucket["running"] = bucket.get("processing", 0)
        bucket["done"] = bucket.get("completed", 0)
    return summary
