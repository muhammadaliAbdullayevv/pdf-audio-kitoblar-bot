#!/usr/bin/env python3
"""Test if bot can start without errors"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

print("🚀 Testing Bot Startup")
print("=" * 40)

try:
    # Test basic imports
    print("📦 Testing imports...")
    import config
    print("✅ config.py imported")
    
    import language
    print("✅ language.py imported")
    
    import menu_ui
    print("✅ menu_ui.py imported")
    
    import menus
    print("✅ menus.py imported")
    
    # Test bot import (this will fail if there are syntax errors)
    print("🤖 Testing bot.py import...")
    import bot
    print("✅ bot.py imported successfully")
    
    # Test key functions
    print("🔧 Testing key functions...")
    
    # Test admin check function
    test_user_id = 12345
    is_admin = bot._is_admin_user(test_user_id)
    print(f"✅ _is_admin_user works: {is_admin}")
    
    # Test menu building
    from language import MESSAGES
    keyboard = menus.build_main_menu_keyboard(
        lang="en",
        section="other", 
        user_id=test_user_id,
        messages=MESSAGES,
        is_admin_user_fn=bot._is_admin_user
    )
    print("✅ build_main_menu_keyboard works")
    
    # Test menu action mapping
    action = bot._menu_ui_main_menu_text_action("❓ Help", MESSAGES, bot._ADMIN_MENU_LABELS)
    print(f"✅ main_menu_text_action works: {action}")
    
    print("\n🎉 All tests passed! No errors found.")
    print("✅ Bot should be working correctly.")
    
except ImportError as e:
    print(f"❌ Import Error: {e}")
    print("This suggests a missing dependency or syntax error.")
    
except SyntaxError as e:
    print(f"❌ Syntax Error: {e}")
    print("This is a code syntax error that needs to be fixed.")
    
except Exception as e:
    print(f"❌ Other Error: {e}")
    print("This could be a runtime error or configuration issue.")
    
    import traceback
    traceback.print_exc()
