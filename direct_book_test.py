#!/usr/bin/env python3
"""Direct book insertion test to bypass upload issues"""

import sys
import os
import uuid
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("direct_book_test.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🧪 Direct Book Insertion Test")
write_log("=" * 40)

try:
    import db
    import bot
    
    write_log("✅ Modules imported successfully")
    
    # Test 1: Direct database insertion
    write_log("\n📚 Test 1: Direct Database Insertion")
    
    test_book = {
        "id": f"test-direct-{uuid.uuid4().hex[:8]}",
        "book_name": "test book direct insertion",
        "display_name": "Test Book Direct Insertion",
        "file_id": "test-file-id-direct",
        "file_unique_id": f"test-unique-{uuid.uuid4().hex[:8]}",
        "path": None,
        "indexed": False
    }
    
    write_log(f"Test book: {test_book}")
    
    # Insert the book
    result = db.insert_book(test_book)
    write_log(f"Insert result: {result}")
    
    if result is not False:
        write_log("✅ Book inserted successfully")
        
        # Retrieve the book
        retrieved = db.get_book_by_id(test_book["id"])
        if retrieved:
            write_log("✅ Book retrieved successfully")
            write_log(f"Retrieved: {retrieved}")
            
            # Clean up
            db.delete_books_by_ids([test_book["id"]])
            write_log("✅ Test book cleaned up")
        else:
            write_log("❌ Could not retrieve test book")
    else:
        write_log("❌ Book insertion failed")
    
    # Test 2: Test the upload flow functions
    write_log("\n📤 Test 2: Upload Flow Functions")
    
    try:
        # Test clean_query
        if hasattr(bot, 'clean_query'):
            cleaned = bot.clean_query("Test Book PDF")
            write_log(f"✅ clean_query: 'Test Book PDF' -> '{cleaned}'")
        
        # Test normalize
        if hasattr(bot, 'normalize'):
            normalized = bot.normalize("Test Book PDF")
            write_log(f"✅ normalize: 'Test Book PDF' -> '{normalized}'")
        
        # Test duplicate detection
        if hasattr(bot, 'db_find_duplicate_book'):
            duplicate = bot.db_find_duplicate_book(None, None, test_book["file_unique_id"])
            write_log(f"✅ db_find_duplicate_book by file_unique_id: {duplicate}")
        
    except Exception as e:
        write_log(f"❌ Upload flow test failed: {e}")
    
    # Test 3: Check database stats
    write_log("\n📊 Test 3: Database Stats")
    
    try:
        stats = db.get_db_stats()
        if stats.get('ok'):
            write_log("✅ Database connection OK")
            counts = stats.get('counts', {})
            write_log(f"   Books: {counts.get('books', 'Unknown')}")
            write_log(f"   Users: {counts.get('users', 'Unknown')}")
        else:
            write_log(f"❌ Database connection failed: {stats.get('error')}")
    except Exception as e:
        write_log(f"❌ Stats test failed: {e}")
    
    write_log("\n🎯 Test Summary:")
    write_log("If Test 1 passes, database insertion works.")
    write_log("If Test 2 passes, upload functions work.")
    write_log("If Test 3 passes, database is accessible.")
    
    write_log("\n🔧 If all tests pass but upload still fails:")
    write_log("1. The issue is in the upload flow logic")
    write_log("2. Check the debug logs I added")
    write_log("3. The problem is likely in handle_file or _process_upload")
    
except Exception as e:
    write_log(f"❌ Critical error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Direct test complete")
