#!/usr/bin/env python3
"""One-time script to backfill display_order for existing audiobooks and parts."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from db import backfill_audio_display_orders, init_db

def main():
    print("Initializing database connection...")
    init_db()
    
    print("Running backfill for display_order columns...")
    result = backfill_audio_display_orders()
    
    print(f"✅ Backfill complete:")
    print(f"   - Audiobooks updated: {result['audiobooks_updated']}")
    print(f"   - Parts updated: {result['parts_updated']}")
    
    if result['audiobooks_updated'] == 0 and result['parts_updated'] == 0:
        print("   (All records already had display_order set)")

if __name__ == "__main__":
    main()
