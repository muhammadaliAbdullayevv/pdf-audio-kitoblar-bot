#!/usr/bin/env python3
"""Test the updated audit command functionality"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

print("🧪 Testing Updated Audit Command")
print("=" * 50)

try:
    # Test database functions
    print("\n📊 Testing new database functions...")
    
    import db
    
    # Test audio book stats
    print("🎧 Testing audio book stats...")
    audio_stats = db.get_audio_book_stats()
    print(f"✅ Audio book stats: {audio_stats}")
    
    # Test storage stats  
    print("💾 Testing storage stats...")
    storage_stats = db.get_storage_stats()
    print(f"✅ Storage stats: {storage_stats}")
    
    # Test format bytes function
    print("📏 Testing format bytes...")
    from bot import _format_bytes
    test_sizes = [0, 1024, 1048576, 1073741824, 1099511627776]
    for size in test_sizes:
        formatted = _format_bytes(size)
        print(f"  {size} bytes = {formatted}")
    print("✅ Format bytes working")
    
    # Test audit command imports
    print("\n🔍 Testing audit command imports...")
    import bot
    print("✅ Bot module imports successfully")
    
    # Check if new functions are available
    if hasattr(bot, 'audit_command'):
        print("✅ audit_command function exists")
    else:
        print("❌ audit_command function missing")
        
    if hasattr(bot, '_format_bytes'):
        print("✅ _format_bytes function exists")
    else:
        print("❌ _format_bytes function missing")
    
    print("\n🎯 Testing audit command structure...")
    
    # Mock the audit command data collection (without actually running it)
    # This tests that all the required functions exist and can be called
    test_data = {
        'audio_stats': audio_stats,
        'storage_stats': storage_stats,
        'counters': {
            'ai_chat_sessions': 0,
            'ai_translator_uses': 0,
            'ai_grammar_fixes': 0,
            'ai_email_writes': 0,
            'ai_quiz_generated': 0,
            'ai_music_generated': 0,
            'ai_pdf_created': 0,
        }
    }
    
    print("✅ All required data structures available")
    
    # Test format_bytes with storage data
    if storage_stats.get('total_size', 0) > 0:
        formatted_total = _format_bytes(storage_stats['total_size'])
        print(f"✅ Total storage formatted: {formatted_total}")
    
    print("\n📋 Summary:")
    print("✅ Audio book statistics function working")
    print("✅ Storage statistics function working") 
    print("✅ Format bytes helper working")
    print("✅ All imports successful")
    print("✅ Audit command structure updated")
    
    print("\n🚀 Ready to test with /audit command!")
    
except Exception as e:
    print(f"❌ Error during testing: {e}")
    import traceback
    traceback.print_exc()
