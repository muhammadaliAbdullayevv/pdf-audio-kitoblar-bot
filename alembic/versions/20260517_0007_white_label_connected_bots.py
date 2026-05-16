"""white-label connected bot mvp tables

Revision ID: 20260517_0007
Revises: 20260506_0006
Create Date: 2026-05-17 01:30:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260517_0007"
down_revision = "20260506_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bots_owner ON connected_bots (owner_telegram_id, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bots_status ON connected_bots (status, updated_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bots_username ON connected_bots (LOWER(bot_username));")
    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_file_cache_status ON connected_bot_file_cache (connected_bot_id, status, last_validated_at DESC);")
    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_cache_seed_jobs_lookup ON connected_bot_cache_seed_jobs (connected_bot_id, book_id, status, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_cache_seed_jobs_expiry ON connected_bot_cache_seed_jobs (status, expires_at);")
    op.execute(
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
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS connected_bot_usage;")
    op.execute("DROP TABLE IF EXISTS connected_bot_cache_seed_jobs;")
    op.execute("DROP TABLE IF EXISTS connected_bot_file_cache;")
    op.execute("DROP TABLE IF EXISTS connected_bots;")
