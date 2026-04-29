#!/usr/bin/env python3
"""Check for errors in the bot after menu customization removal"""

import sys
import os
import ast
import subprocess
sys.path.insert(0, os.path.dirname(__file__))

print("🔍 Checking for Bot Errors")
print("=" * 50)

errors_found = []

# Test 1: Check Python syntax errors in main files
print("\n📝 Checking Python syntax:")
python_files = [
    "bot.py",
    "menus.py", 
    "menu_ui.py",
    "language.py",
    "config.py"
]

for file in python_files:
    if os.path.exists(file):
        try:
            with open(file, 'r') as f:
                content = f.read()
            ast.parse(content)
            print(f"✅ {file} - Syntax OK")
        except SyntaxError as e:
            errors_found.append(f"Syntax error in {file}: {e}")
            print(f"❌ {file} - Syntax Error: {e}")
        except Exception as e:
            errors_found.append(f"Error reading {file}: {e}")
            print(f"❌ {file} - Error: {e}")
    else:
        print(f"⚠️ {file} - Not found")

# Test 2: Check imports
print("\n📦 Testing imports:")
try:
    import bot
    print("✅ bot.py imports successfully")
except Exception as e:
    errors_found.append(f"Import error in bot.py: {e}")
    print(f"❌ bot.py import error: {e}")

try:
    import menus
    print("✅ menus.py imports successfully")
except Exception as e:
    errors_found.append(f"Import error in menus.py: {e}")
    print(f"❌ menus.py import error: {e}")

try:
    import menu_ui
    print("✅ menu_ui.py imports successfully")
except Exception as e:
    errors_found.append(f"Import error in menu_ui.py: {e}")
    print(f"❌ menu_ui.py import error: {e}")

try:
    import language
    print("✅ language.py imports successfully")
except Exception as e:
    errors_found.append(f"Import error in language.py: {e}")
    print(f"❌ language.py import error: {e}")

# Test 3: Check for missing menu customization references
print("\n🧹 Checking for leftover menu customization code:")
files_to_check = ["bot.py", "menus.py", "menu_ui.py", "language.py"]

for file in files_to_check:
    if os.path.exists(file):
        try:
            with open(file, 'r') as f:
                content = f.read()
            
            leftover_refs = []
            if "menu_customize" in content:
                leftover_refs.append("menu_customize")
            if "custom_menu" in content:
                leftover_refs.append("custom_menu")
            if "menu_customizer" in content:
                leftover_refs.append("menu_customizer")
            if "menu_reorder" in content:
                leftover_refs.append("menu_reorder")
            if "simple_menu_customize" in content:
                leftover_refs.append("simple_menu_customize")
            
            if leftover_refs:
                errors_found.append(f"Leftover references in {file}: {leftover_refs}")
                print(f"❌ {file} - Leftover references: {leftover_refs}")
            else:
                print(f"✅ {file} - Clean of menu customization code")
                
        except Exception as e:
            errors_found.append(f"Error checking {file}: {e}")
            print(f"❌ {file} - Error: {e}")

# Test 4: Check bot service status
print("\n🤖 Checking bot service status:")
try:
    result = subprocess.run(["systemctl", "is-active", "pdf_audio_kitoblar_bot-bot.service"], 
                          capture_output=True, text=True, timeout=5)
    if result.returncode == 0:
        status = result.stdout.strip()
        if status == "active":
            print("✅ Bot service is running")
        else:
            errors_found.append(f"Bot service status: {status}")
            print(f"⚠️ Bot service status: {status}")
    else:
        errors_found.append("Could not check bot service status")
        print("❌ Could not check bot service status")
except Exception as e:
    errors_found.append(f"Error checking service: {e}")
    print(f"❌ Error checking service: {e}")

# Test 5: Check for deleted files
print("\n🗑️ Checking deleted menu customization files:")
deleted_files = [
    "custom_menu.py",
    "menu_customizer.py", 
    "menu_reorder.py",
    "simple_menu_customize.py"
]

for file in deleted_files:
    if os.path.exists(file):
        errors_found.append(f"File still exists: {file}")
        print(f"❌ {file} - Still exists (should be deleted)")
    else:
        print(f"✅ {file} - Successfully deleted")

# Summary
print("\n" + "=" * 50)
if errors_found:
    print(f"❌ Found {len(errors_found)} issues:")
    for i, error in enumerate(errors_found, 1):
        print(f"  {i}. {error}")
else:
    print("✅ No errors found! Bot should be working correctly.")

print(f"\n📊 Summary: {len(errors_found)} errors found")
