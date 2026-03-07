#!/usr/bin/env python3
"""Recalculate display_order for all audiobooks and parts using book creation time + ID hash."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from db import init_db, db_conn

def main():
    print("Initializing database connection...")
    init_db()
    
    with db_conn() as conn:
        with conn.cursor() as cur:
            # First, assign row numbers to books with audiobooks for consistent ordering
            cur.execute(
                """
                WITH ranked_books AS (
                    SELECT b.id, ROW_NUMBER() OVER (ORDER BY b.created_at, b.id) as rn
                    FROM books b
                    WHERE EXISTS (SELECT 1 FROM audio_books ab WHERE ab.book_id = b.id)
                )
                UPDATE audio_books ab
                SET display_order = rn * 1000000
                FROM books b
                JOIN ranked_books rb ON b.id = rb.id
                WHERE b.id = ab.book_id
                """
            )
            ab_updated = cur.rowcount
            
            # Recalculate all audio_book_parts display_order
            cur.execute(
                """
                UPDATE audio_book_parts p
                SET display_order = ab.display_order + p.part_index
                FROM audio_books ab
                WHERE ab.id = p.audio_book_id
                """
            )
            parts_updated = cur.rowcount
    
    print(f"✅ Recalculation complete:")
    print(f"   - Audiobooks updated: {ab_updated}")
    print(f"   - Parts updated: {parts_updated}")

if __name__ == "__main__":
    main()
