"""white-label safety controls

Revision ID: 20260519_0010
Revises: 20260518_0009
Create Date: 2026-05-19 10:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260519_0010"
down_revision = "20260518_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE connected_bot_requests ALTER COLUMN bot_token_encrypted DROP NOT NULL;")
    op.execute(
        """
        UPDATE connected_bot_requests
        SET bot_token_encrypted=NULL,
            updated_at=NOW()
        WHERE status IN ('ACCEPTED', 'REJECTED', 'CANCELLED')
          AND bot_token_encrypted IS NOT NULL;
        """
    )
    op.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS white_label_enabled BOOLEAN NOT NULL DEFAULT TRUE;")
    op.execute("CREATE INDEX IF NOT EXISTS idx_books_white_label_enabled ON books (white_label_enabled);")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_last_heartbeat_at TIMESTAMP;")
    op.execute("ALTER TABLE connected_bots ADD COLUMN IF NOT EXISTS runtime_pid BIGINT;")
    op.execute("UPDATE connected_bots SET plan='PRO' WHERE UPPER(plan)='PLUS';")
    op.execute(
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
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_bot_created ON white_label_audit_logs (connected_bot_id, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_request_created ON white_label_audit_logs (request_id, created_at DESC);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_white_label_audit_logs_action_created ON white_label_audit_logs (action, created_at DESC);")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS white_label_audit_logs;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS runtime_pid;")
    op.execute("ALTER TABLE connected_bots DROP COLUMN IF EXISTS runtime_last_heartbeat_at;")
    op.execute("DROP INDEX IF EXISTS idx_books_white_label_enabled;")
    op.execute("ALTER TABLE books DROP COLUMN IF EXISTS white_label_enabled;")
