"""create background_jobs table

Revision ID: 20260420_0002
Revises: 20260309_0001
Create Date: 2026-04-20 00:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260420_0002"
down_revision = "20260309_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS background_jobs (
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            data_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
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
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_user_id ON background_jobs (user_id);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_status ON background_jobs (status);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_next_attempt_at ON background_jobs (next_attempt_at);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_background_jobs_job_type ON background_jobs (job_type);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_job_type;")
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_next_attempt_at;")
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_status;")
    op.execute("DROP INDEX IF EXISTS idx_background_jobs_user_id;")
    op.execute("DROP TABLE IF EXISTS background_jobs;")