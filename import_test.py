#!/usr/bin/env python3
"""Test imports and write results to file"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

with open("import_test_results.txt", "w") as f:
    f.write("Import Test Results\n")
    f.write("=" * 30 + "\n")
    
    try:
        import upload_flow
        f.write("✅ upload_flow: SUCCESS\n")
    except Exception as e:
        f.write(f"❌ upload_flow: {e}\n")
        import traceback
        f.write(f"Traceback: {traceback.format_exc()}\n")
    
    try:
        import bot
        f.write("✅ bot: SUCCESS\n")
    except Exception as e:
        f.write(f"❌ bot: {e}\n")
        import traceback
        f.write(f"Traceback: {traceback.format_exc()}\n")
    
    try:
        import db
        f.write("✅ db: SUCCESS\n")
    except Exception as e:
        f.write(f"❌ db: {e}\n")
        import traceback
        f.write(f"Traceback: {traceback.format_exc()}\n")
    
    f.write("\nTest complete\n")
