"""create admin_task_runs table

Revision ID: 20260309_0001
Revises:
Create Date: 2026-03-09 00:00:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260309_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
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
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_task_runs_started_at ON admin_task_runs (started_at DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_task_runs_task_kind ON admin_task_runs (task_kind);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_task_runs_status ON admin_task_runs (status);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_admin_task_runs_status;")
    op.execute("DROP INDEX IF EXISTS idx_admin_task_runs_task_kind;")
    op.execute("DROP INDEX IF EXISTS idx_admin_task_runs_started_at;")
    op.execute("DROP TABLE IF EXISTS admin_task_runs;")
