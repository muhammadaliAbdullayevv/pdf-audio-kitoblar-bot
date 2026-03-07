#!/usr/bin/env python3
"""Check upload permissions for current user"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    import bot
    import db
    
    print("🔍 Upload Permission Check")
    print("=" * 40)
    
    # Check ADMIN_ID
    if hasattr(bot, 'ADMIN_ID'):
        admin_id = bot.ADMIN_ID
        print(f"ADMIN_ID: {admin_id}")
        
        # Check user record
        user = bot.get_user(admin_id)
        print(f"User record: {user}")
        
        if user:
            allowed = user.get('allowed', False)
            blocked = user.get('blocked', False)
            status = user.get('status', 'unknown')
            
            print(f"User allowed: {allowed}")
            print(f"User blocked: {blocked}")
            print(f"User status: {status}")
            
            # Test is_allowed function
            is_allowed_result = bot.is_allowed(admin_id)
            print(f"is_allowed() result: {is_allowed_result}")
            
            print("\n📋 Upload Requirements:")
            print(f"✓ upload_mode must be True (run /upload first)")
            print(f"✓ is_allowed() must be True (currently: {is_allowed_result})")
            
            if not is_allowed_result:
                print("\n❌ ISSUE: User is not allowed to upload!")
                print("🔧 SOLUTION: Update user record in database:")
                print(f"UPDATE users SET allowed = true WHERE id = {admin_id};")
            else:
                print("\n✅ User has upload permissions!")
                
        else:
            print("❌ User not found in database!")
            print("🔧 SOLUTION: Create user record or check bot initialization")
    else:
        print("❌ ADMIN_ID not found!")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
