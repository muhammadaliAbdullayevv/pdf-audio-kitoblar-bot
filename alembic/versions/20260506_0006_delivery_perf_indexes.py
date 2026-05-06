"""add delivery performance indexes

Revision ID: 20260506_0006
Revises: 20260506_0005
Create Date: 2026-05-06 21:55:00
"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260506_0006"
down_revision = "20260506_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_books_display_name ON books (display_name);")
    op.execute("CREATE INDEX IF NOT EXISTS idx_upload_receipts_book_id ON upload_receipts (book_id);")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_upload_receipts_book_id;")
    op.execute("DROP INDEX IF EXISTS idx_books_display_name;")
