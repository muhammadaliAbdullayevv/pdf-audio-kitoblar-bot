#!/usr/bin/env python3
"""Test search_flow imports"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

with open("import_test_results.txt", "w") as f:
    f.write("Testing search_flow imports\n")
    f.write("=" * 40 + "\n")
    
    try:
        import search_flow
        f.write("✅ search_flow imported successfully\n")
        
        # Check required functions
        required_functions = [
            'handle_audiobook_delete_callback',
            'handle_audiobook_part_delete_callback',
            'handle_audiobook_listen_callback',
            'handle_audiobook_part_play_callback',
            'handle_audiobook_add_callback',
            'handle_abook_audio'
        ]
        
        for func_name in required_functions:
            if hasattr(search_flow, func_name):
                f.write(f"✅ {func_name} exists\n")
            else:
                f.write(f"❌ {func_name} missing\n")
                
        f.write("\n✅ Test completed\n")
        
    except Exception as e:
        f.write(f"❌ Import failed: {e}\n")
        import traceback
        f.write(f"Traceback: {traceback.format_exc()}\n")

print("Import test completed - check import_test_results.txt")
