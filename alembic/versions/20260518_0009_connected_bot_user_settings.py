"""connected bot user language and public settings

Revision ID: 20260518_0009
Revises: 20260517_0008
Create Date: 2026-05-18 12:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260518_0009"
down_revision = "20260517_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS branding_title TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_uz TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_en TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS welcome_text_ru TEXT;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS search_results_limit INTEGER NOT NULL DEFAULT 10;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS trial_expired_notified_at TIMESTAMP;")
    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_users_updated ON connected_bot_users (connected_bot_id, updated_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_connected_bot_users_user ON connected_bot_users (telegram_user_id, updated_at DESC);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS connected_bot_users;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS trial_expired_notified_at;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS search_results_limit;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS welcome_text_ru;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS welcome_text_en;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS welcome_text_uz;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS branding_title;")
