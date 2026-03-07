#!/usr/bin/env python3
"""Test upload functionality step by step"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("upload_issue_diagnosis.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🔍 Upload Issue Diagnosis")
write_log("=" * 50)

try:
    # Test 1: Import modules
    write_log("\n📦 Testing imports...")
    import upload_flow
    import bot
    import db
    write_log("✅ All modules imported successfully")

    # Test 2: Check upload_mode
    write_log("\n📊 Testing upload mode...")
    if hasattr(upload_flow, 'upload_mode'):
        write_log(f"✅ upload_mode exists: {upload_flow.upload_mode}")
        write_log("   (This should be False initially)")
    else:
        write_log("❌ upload_mode not found")

    # Test 3: Check is_allowed function
    write_log("\n👤 Testing user permissions...")
    if hasattr(bot, 'ADMIN_ID'):
        admin_id = bot.ADMIN_ID
        write_log(f"✅ ADMIN_ID: {admin_id}")

        if hasattr(bot, 'is_allowed'):
            allowed = bot.is_allowed(admin_id)
            write_log(f"✅ Admin is_allowed: {allowed}")

            if hasattr(bot, 'get_user'):
                user = bot.get_user(admin_id)
                write_log(f"✅ Admin user record: {user}")

                if user:
                    allowed_field = user.get('allowed', False)
                    write_log(f"✅ User allowed field: {allowed_field}")
                    write_log(f"✅ User blocked field: {user.get('blocked', False)}")
                    write_log(f"✅ User status: {user.get('status', 'unknown')}")
                else:
                    write_log("❌ Admin user not found in database")
        else:
            write_log("❌ is_allowed function not found")
    else:
        write_log("❌ ADMIN_ID not found")

    # Test 4: Check database functions
    write_log("\n💾 Testing database functions...")
    try:
        db_stats = db.get_db_stats()
        if db_stats.get('ok'):
            write_log("✅ Database connection OK")
            counts = db_stats.get('counts', {})
            write_log(f"   Books: {counts.get('books', 0)}")
            write_log(f"   Users: {counts.get('users', 0)}")
        else:
            write_log(f"❌ Database connection failed: {db_stats.get('error')}")
    except Exception as e:
        write_log(f"❌ Database test failed: {e}")

    # Test 5: Simulate upload command
    write_log("\n📝 Testing upload command simulation...")
    try:
        # This simulates what happens when /upload is called
        if hasattr(bot, 'ADMIN_ID'):
            admin_id = bot.ADMIN_ID
            if hasattr(bot, 'is_allowed'):
                allowed = bot.is_allowed(admin_id)
                if allowed:
                    write_log("✅ Upload command would activate upload_mode")
                    write_log("   (This means /upload command logic is working)")
                else:
                    write_log("❌ Upload command would fail - user not allowed")
                    write_log("   SOLUTION: Check user permissions in database")
            else:
                write_log("❌ is_allowed function missing")
        else:
            write_log("❌ ADMIN_ID missing")
    except Exception as e:
        write_log(f"❌ Upload simulation failed: {e}")

    # Test 6: Check file handler registration
    write_log("\n📁 Testing file handler...")
    try:
        if hasattr(upload_flow, 'handle_file'):
            write_log("✅ handle_file function exists")
        else:
            write_log("❌ handle_file function missing")

        if hasattr(upload_flow, '_process_upload'):
            write_log("✅ _process_upload function exists")
        else:
            write_log("❌ _process_upload function missing")
    except Exception as e:
        write_log(f"❌ File handler test failed: {e}")

    # Test 7: Check message constants
    write_log("\n💬 Testing message constants...")
    try:
        if hasattr(bot, 'MESSAGES'):
            messages = bot.MESSAGES
            en = messages.get('en', {})
            required_messages = [
                'upload_activated',
                'upload_processing',
                'saved',
                'duplicate',
                'upload_inactive'
            ]

            for msg_key in required_messages:
                if msg_key in en:
                    write_log(f"✅ {msg_key}: '{en[msg_key]}'")
                else:
                    write_log(f"❌ {msg_key}: MISSING")
        else:
            write_log("❌ MESSAGES not found")
    except Exception as e:
        write_log(f"❌ Message constants test failed: {e}")

    write_log("\n🎯 Diagnosis Summary:")
    write_log("1. Check if user has 'allowed = true' in database")
    write_log("2. Check if upload_mode gets set when /upload is run")
    write_log("3. Check if handle_file gets called when files are sent")
    write_log("4. Check debug logs for 'DEBUG:' messages")
    write_log("5. Verify database and Elasticsearch are accessible")

    write_log("\n🔧 Most Likely Issues:")
    write_log("• User not allowed in database (check 'allowed' field)")
    write_log("• upload_mode not being set properly")
    write_log("• handle_file not being called")
    write_log("• Database insertion failing")

    write_log("\n📋 Next Steps:")
    write_log("1. Check the debug logs I added")
    write_log("2. Run /upload and see if upload_mode changes")
    write_log("3. Send a file and check for DEBUG messages")

except Exception as e:
    write_log(f"❌ Critical error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Diagnosis complete")
