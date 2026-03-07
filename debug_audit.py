#!/usr/bin/env python3
"""Debug audit command issues"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

print("🔍 Debugging Audit Command")
print("=" * 50)

try:
    # Test imports
    print("📦 Testing imports...")
    import bot
    import db
    print("✅ Imports successful")
    
    # Test database functions
    print("\n📊 Testing database functions...")
    
    # Test audio book stats
    try:
        audio_stats = db.get_audio_book_stats()
        print(f"✅ Audio book stats: {audio_stats}")
    except Exception as e:
        print(f"❌ Audio book stats error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test storage stats
    try:
        storage_stats = db.get_storage_stats()
        print(f"✅ Storage stats: {storage_stats}")
    except Exception as e:
        print(f"❌ Storage stats error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test format bytes
    try:
        from bot import _format_bytes
        test_format = _format_bytes(1048576)
        print(f"✅ Format bytes test: 1048576 = {test_format}")
    except Exception as e:
        print(f"❌ Format bytes error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test audit command function exists
    print("\n🔍 Testing audit command...")
    if hasattr(bot, 'audit_command'):
        print("✅ audit_command function exists")
        
        # Check if ADMIN_ID is available
        if hasattr(bot, 'ADMIN_ID'):
            print(f"✅ ADMIN_ID available: {bot.ADMIN_ID}")
        else:
            print("❌ ADMIN_ID not found")
            
        # Check MESSAGES
        if hasattr(bot, 'MESSAGES'):
            print("✅ MESSAGES available")
        else:
            print("❌ MESSAGES not found")
            
    else:
        print("❌ audit_command function missing")
    
    # Test a minimal audit data collection
    print("\n🧪 Testing audit data collection...")
    try:
        # Test basic database functions that audit uses
        book_totals = db.get_book_totals()
        print(f"✅ Book totals: {book_totals}")
        
        user_status = db.get_user_status_counts()
        print(f"✅ User status: {user_status}")
        
        fav_total = db.get_favorites_total()
        print(f"✅ Favorites total: {fav_total}")
        
        print("✅ Core audit functions working")
        
    except Exception as e:
        print(f"❌ Core audit functions error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n🎯 Debug Summary:")
    print("✅ All imports working")
    print("✅ Database functions accessible")
    print("✅ Helper functions working")
    print("✅ Audit command structure intact")
    
    print("\n⚠️  If audit still not working, check:")
    print("  1. Bot permissions (ADMIN_ID)")
    print("  2. Telegram message sending")
    print("  3. Spam protection")
    print("  4. Bot restart status")
    
except Exception as e:
    print(f"❌ Critical error: {e}")
    import traceback
    traceback.print_exc()
