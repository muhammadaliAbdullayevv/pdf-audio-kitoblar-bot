"""remove deprecated video downloader jobs and counters

Revision ID: 20260506_0005
Revises: 20260506_0004
Create Date: 2026-05-06 18:20:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260506_0005"
down_revision = "20260506_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DELETE FROM background_jobs WHERE job_type IN ('video_download', 'VIDEO_DOWNLOAD');")
    op.execute("DELETE FROM analytics_counters WHERE key = 'video_downloads' OR key LIKE 'video_dl_%';")


def downgrade() -> None:
    pass
