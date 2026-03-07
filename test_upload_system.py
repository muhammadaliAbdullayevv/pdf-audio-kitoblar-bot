#!/usr/bin/env python3
"""Test upload system components"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("upload_test_result.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🧪 Upload System Test")
write_log("=" * 40)

try:
    import bot
    import upload_flow
    
    # Test Elasticsearch availability
    write_log("\n🔍 Checking Elasticsearch...")
    try:
        if hasattr(bot, 'es_available'):
            es_status = bot.es_available()
            write_log(f"✅ Elasticsearch available: {es_status}")
        else:
            write_log("❌ es_available function not found")
    except Exception as e:
        write_log(f"❌ Elasticsearch check error: {e}")
    
    # Test upload_mode
    write_log("\n📊 Checking upload mode...")
    try:
        if hasattr(upload_flow, 'upload_mode'):
            write_log(f"✅ upload_mode: {upload_flow.upload_mode}")
        else:
            write_log("❌ upload_mode not found")
    except Exception as e:
        write_log(f"❌ Upload mode check error: {e}")
    
    # Test message constants
    write_log("\n📝 Checking upload messages...")
    try:
        if hasattr(bot, 'MESSAGES'):
            messages = bot.MESSAGES
            en_messages = messages.get('en', {})
            
            upload_messages = {
                'upload_activated': en_messages.get('upload_activated', 'NOT FOUND'),
                'upload_processing': en_messages.get('upload_processing', 'NOT FOUND'),
                'saved': en_messages.get('saved', 'NOT FOUND'),
                'saved_indexing': en_messages.get('saved_indexing', 'NOT FOUND'),
                'duplicate': en_messages.get('duplicate', 'NOT FOUND'),
                'error': en_messages.get('error', 'NOT FOUND'),
            }
            
            write_log("Upload message constants:")
            for key, value in upload_messages.items():
                write_log(f"  {key}: {value}")
        else:
            write_log("❌ MESSAGES not found")
    except Exception as e:
        write_log(f"❌ Message check error: {e}")
    
    # Test database functions
    write_log("\n💾 Testing database functions...")
    try:
        if hasattr(bot, 'db_insert_book'):
            write_log("✅ db_insert_book exists")
        else:
            write_log("❌ db_insert_book missing")
            
        if hasattr(bot, 'db_find_duplicate_book'):
            write_log("✅ db_find_duplicate_book exists")
        else:
            write_log("❌ db_find_duplicate_book missing")
            
    except Exception as e:
        write_log(f"❌ Database function test error: {e}")
    
    write_log("\n🎯 Upload Flow Test Summary:")
    write_log("1. Check if Elasticsearch is running")
    write_log("2. Verify upload_mode is activated")
    write_log("3. Ensure message constants exist")
    write_log("4. Check database connectivity")
    
    write_log("\n📋 Expected Upload Flow:")
    write_log("1. /upload → upload_mode = True")
    write_log("2. Send file → _process_upload starts")
    write_log("3. Check duplicates → save if new")
    write_log("4. Send confirmation:")
    write_log("   - If ES available: 'Saving and indexing...'")
    write_log("   - If ES unavailable: 'Saved (not indexed)'")
    write_log("   - If duplicate: 'Duplicate book'")
    
except Exception as e:
    write_log(f"❌ Critical error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Test complete")
