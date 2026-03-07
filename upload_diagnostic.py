#!/usr/bin/env python3
"""Diagnose upload command issues"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("upload_diagnostic_result.txt", "a") as f:
        f.write(message + "\n")

write_log("🔍 Upload Command Diagnostic")
write_log("=" * 50)

try:
    import bot
    import db
    
    # Check upload_mode variable
    write_log("\n📊 Checking upload system...")
    
    # Check if upload_mode exists
    if hasattr(bot, 'upload_mode'):
        write_log(f"✅ upload_mode exists: {bot.upload_mode}")
    else:
        write_log("❌ upload_mode not found")
    
    # Test is_allowed function
    write_log("\n👤 Testing user permissions...")
    
    # Get ADMIN_ID for testing
    if hasattr(bot, 'ADMIN_ID'):
        admin_id = bot.ADMIN_ID
        write_log(f"✅ ADMIN_ID: {admin_id}")
        
        # Test is_allowed for admin
        try:
            admin_allowed = bot.is_allowed(admin_id)
            write_log(f"✅ Admin is_allowed: {admin_allowed}")
        except Exception as e:
            write_log(f"❌ Admin is_allowed error: {e}")
        
        # Check user record in database
        try:
            admin_user = bot.get_user(admin_id)
            write_log(f"✅ Admin user record: {admin_user}")
        except Exception as e:
            write_log(f"❌ Admin user record error: {e}")
            
    else:
        write_log("❌ ADMIN_ID not found")
    
    # Test database functions
    write_log("\n💾 Testing database functions...")
    
    try:
        # Test get_user function
        if hasattr(bot, 'get_user'):
            write_log("✅ get_user function exists")
        else:
            write_log("❌ get_user function missing")
            
        # Test is_allowed function
        if hasattr(bot, 'is_allowed'):
            write_log("✅ is_allowed function exists")
        else:
            write_log("❌ is_allowed function missing")
            
    except Exception as e:
        write_log(f"❌ Database function test error: {e}")
    
    # Check upload flow
    write_log("\n📤 Checking upload flow...")
    
    try:
        import upload_flow
        write_log("✅ upload_flow module imported")
        
        if hasattr(upload_flow, 'upload_command'):
            write_log("✅ upload_command function exists")
        else:
            write_log("❌ upload_command function missing")
            
        if hasattr(upload_flow, 'handle_file'):
            write_log("✅ handle_file function exists")
        else:
            write_log("❌ handle_file function missing")
            
        if hasattr(upload_flow, '_process_upload'):
            write_log("✅ _process_upload function exists")
        else:
            write_log("❌ _process_upload function missing")
            
    except Exception as e:
        write_log(f"❌ Upload flow test error: {e}")
    
    write_log("\n🎯 Diagnostic Summary:")
    write_log("Upload requires BOTH conditions:")
    write_log("1. upload_mode = True (set by /upload command)")
    write_log("2. is_allowed(user_id) = True (user has 'allowed' field in database)")
    
    write_log("\n📋 Troubleshooting Steps:")
    write_log("1. Run /upload command first to activate upload_mode")
    write_log("2. Check if your user has 'allowed' = True in database")
    write_log("3. Verify you're not blocked or stopped")
    write_log("4. Check for spam protection limits")
    
except Exception as e:
    write_log(f"❌ Critical error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Diagnostic complete")
