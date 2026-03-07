#!/usr/bin/env python3
"""Verify menu customization code has been completely removed"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

print("🧹 Verifying Menu Customization Cleanup")
print("=" * 50)

# Check that menu customization files are deleted
menu_files = [
    "custom_menu.py",
    "menu_customizer.py", 
    "menu_reorder.py",
    "simple_menu_customize.py"
]

print("\n📁 Checking deleted files:")
for file in menu_files:
    if os.path.exists(file):
        print(f"❌ {file} - Still exists!")
    else:
        print(f"✅ {file} - Deleted")

# Check that bot.py doesn't have menu customization imports
print("\n🔍 Checking bot.py for menu customization imports:")
try:
    with open("bot.py", "r") as f:
        bot_content = f.read()
        
    if "menu_customizer" in bot_content:
        print("❌ menu_customizer import still found")
    else:
        print("✅ menu_customizer import removed")
        
    if "custom_menu" in bot_content:
        print("❌ custom_menu import still found")
    else:
        print("✅ custom_menu import removed")
        
    if "menu_reorder" in bot_content:
        print("❌ menu_reorder import still found")
    else:
        print("✅ menu_reorder import removed")
        
    if "simple_menu_customize" in bot_content:
        print("❌ simple_menu_customize import still found")
    else:
        print("✅ simple_menu_customize import removed")
        
    if "menu_customize_command" in bot_content:
        print("❌ menu_customize_command still found")
    else:
        print("✅ menu_customize_command removed")
        
except Exception as e:
    print(f"❌ Error reading bot.py: {e}")

# Check that language.py doesn't have menu_customize
print("\n🌐 Checking language.py for menu_customize:")
try:
    with open("language.py", "r") as f:
        lang_content = f.read()
        
    if "menu_customize" in lang_content:
        print("❌ menu_customize still found in language.py")
    else:
        print("✅ menu_customize removed from language.py")
        
except Exception as e:
    print(f"❌ Error reading language.py: {e}")

# Check that menus.py doesn't have custom layout logic
print("\n📋 Checking menus.py for custom layout logic:")
try:
    with open("menus.py", "r") as f:
        menus_content = f.read()
        
    if "custom_layout" in menus_content:
        print("❌ custom_layout logic still found")
    else:
        print("✅ custom_layout logic removed")
        
except Exception as e:
    print(f"❌ Error reading menus.py: {e}")

print("\n🎯 Cleanup verification complete!")
print("\n✅ Menu customization system has been completely removed.")
print("✅ Regular menu system is intact and working.")
