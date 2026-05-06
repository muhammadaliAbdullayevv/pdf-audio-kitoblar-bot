"""remove deprecated media/pdf/tts/sticker tool jobs and counters

Revision ID: 20260506_0004
Revises: 20260506_0003
Create Date: 2026-05-06 00:30:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260506_0004"
down_revision = "20260506_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
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
        """
    )
    op.execute("DELETE FROM analytics_counters WHERE key IN ('ai_pdf_created');")


def downgrade() -> None:
    pass
