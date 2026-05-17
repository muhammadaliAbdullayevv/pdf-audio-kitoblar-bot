"""connected bot public request workflow

Revision ID: 20260517_0008
Revises: 20260517_0007
Create Date: 2026-05-17 16:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260517_0008"
down_revision = "20260517_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_user_id BIGINT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_username TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS requested_by_first_name TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS accepted_request_id TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS subscription_status TEXT DEFAULT 'MANUAL';")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMP;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_ends_at TIMESTAMP;")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bots_requested_by ON connected_bots (requested_by_user_id, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bots_trial ON connected_bots (plan, subscription_status, trial_ends_at);")

    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_status_created ON connected_bot_requests (status, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_user ON connected_bot_requests (requesting_user_id, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_username ON connected_bot_requests (LOWER(bot_username));")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_requests_token_fp ON connected_bot_requests (bot_token_fingerprint);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS connected_bot_requests;")
    op.execute("DROP INDEX IF EXISTS idx_connected_bots_trial;")
    op.execute("DROP INDEX IF EXISTS idx_connected_bots_requested_by;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS trial_ends_at;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS trial_started_at;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS subscription_status;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS accepted_request_id;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS requested_by_first_name;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS requested_by_username;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS requested_by_user_id;")
