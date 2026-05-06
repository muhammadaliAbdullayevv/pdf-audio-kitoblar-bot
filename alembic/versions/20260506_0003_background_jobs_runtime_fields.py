"""expand background_jobs runtime fields

Revision ID: 20260506_0003
Revises: 20260420_0002
Create Date: 2026-05-06 00:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260506_0003"
down_revision = "20260420_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    statements = [
        """
        ALTER TABLE background_jobs
        ADD COLUMN IF NOT EXISTS chat_id BIGINT,
        ADD COLUMN IF NOT EXISTS message_id BIGINT,
        ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 100,
        ADD COLUMN IF NOT EXISTS payload_json JSONB,
        ADD COLUMN IF NOT EXISTS result_json JSONB,
        ADD COLUMN IF NOT EXISTS progress INTEGER NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS locked_by TEXT,
        ADD COLUMN IF NOT EXISTS started_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP,
        ADD COLUMN IF NOT EXISTS error_message TEXT,
        ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
        """,
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
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_claim ON background_jobs (status, priority, next_attempt_at, created_at);",
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_idempotency ON background_jobs (user_id, idempotency_key, status);",
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_locked_at ON background_jobs (locked_at) WHERE locked_at IS NOT NULL;",
    ]
    for sql in statements:
        op.execute(sql)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_locked_at;")
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_idempotency;")
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_claim;")
