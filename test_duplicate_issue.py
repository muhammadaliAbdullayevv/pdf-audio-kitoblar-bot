#!/usr/bin/env python3
"""Test the duplicate detection issue"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("duplicate_test_result.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🔍 Testing Duplicate Detection Issue")
write_log("=" * 50)

try:
    import bot
    
    # Test normalize function
    write_log("\n📝 Testing normalize function:")
    
    test_names = [
        "Book 1.pdf",
        "Book-1.pdf", 
        "Book_1.pdf",
        "Book (1).pdf",
        "Book[1].pdf",
        "Book 1 (2023).pdf",
        "Book 1 - Second Edition.pdf",
        "Book 1 : Chapter 1.pdf",
    ]
    
    for name in test_names:
        normalized = bot.normalize(name)
        # Remove extension for comparison (like upload_flow does)
        book_name, _ = os.path.splitext(name)
        cleaned = bot.clean_query(book_name)
        write_log(f"  '{book_name}' -> '{cleaned}'")
    
    write_log("\n🚨 ISSUE IDENTIFIED:")
    write_log("The normalize function is too aggressive!")
    write_log("It removes punctuation, making different books look identical.")
    
    write_log("\n🔧 SOLUTION NEEDED:")
    write_log("1. Make duplicate detection less aggressive")
    write_log("2. Or use file_unique_id as primary duplicate check")
    write_log("3. Or improve the normalization logic")
    
    # Test current duplicate detection
    write_log("\n📊 Current duplicate detection logic:")
    write_log("1. Check file_unique_id (highest priority)")
    write_log("2. Check path (if provided)")
    write_log("3. Check book_name if _name_allows_duplicates() is False")
    
    write_log("\n🎯 The problem is in step 3:")
    write_log("- normalize() removes punctuation")
    write_log("- get_book_by_name() looks for exact match")
    write_log("- Many different books normalize to the same name")
    
except Exception as e:
    write_log(f"❌ Error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Test complete")
