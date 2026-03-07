#!/usr/bin/env python3
"""Debug database and Elasticsearch connectivity"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("db_es_debug.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🔍 Database & Elasticsearch Debug")
write_log("=" * 50)

try:
    import db
    import bot
    
    # Test database connection
    write_log("\n💾 Testing Database Connection...")
    try:
        db_stats = db.get_db_stats()
        if db_stats.get('ok'):
            write_log("✅ Database connection: SUCCESS")
            write_log(f"   Books count: {db_stats.get('counts', {}).get('books', 'Unknown')}")
            write_log(f"   Users count: {db_stats.get('counts', {}).get('users', 'Unknown')}")
        else:
            write_log("❌ Database connection: FAILED")
            write_log(f"   Error: {db_stats.get('error', 'Unknown')}")
    except Exception as e:
        write_log(f"❌ Database test failed: {e}")
        import traceback
        write_log(f"Traceback: {traceback.format_exc()}")
    
    # Test Elasticsearch
    write_log("\n🔍 Testing Elasticsearch...")
    try:
        if hasattr(bot, 'es_available'):
            es_available = bot.es_available()
            write_log(f"✅ Elasticsearch available: {es_available}")
            
            if es_available:
                try:
                    es = bot.get_es()
                    if es:
                        # Test basic ES operation
                        health = es.cluster.health()
                        write_log(f"✅ ES Health: {health.get('status', 'Unknown')}")
                        write_log(f"   Nodes: {health.get('number_of_nodes', 'Unknown')}")
                        
                        # Check index
                        if hasattr(bot, 'ES_INDEX'):
                            try:
                                count = es.count(index=bot.ES_INDEX).get('count', 0)
                                write_log(f"✅ ES Index '{bot.ES_INDEX}': {count} documents")
                            except Exception as index_e:
                                write_log(f"❌ ES Index check failed: {index_e}")
                    else:
                        write_log("❌ get_es() returned None")
                except Exception as es_e:
                    write_log(f"❌ ES operations failed: {es_e}")
            else:
                write_log("❌ Elasticsearch not available")
        else:
            write_log("❌ es_available function not found")
    except Exception as e:
        write_log(f"❌ Elasticsearch test failed: {e}")
    
    # Test book insertion
    write_log("\n📚 Testing Book Insertion...")
    try:
        # Create a test book
        test_book = {
            "id": "test-book-123",
            "book_name": "test book insertion",
            "display_name": "Test Book Insertion",
            "file_id": "test-file-id",
            "file_unique_id": "test-unique-id",
            "path": None,
            "indexed": False
        }
        
        # Try to insert
        result = db.insert_book(test_book)
        write_log(f"✅ Book insertion result: {result}")
        
        if result is not False:
            write_log("✅ Book inserted successfully")
            
            # Try to retrieve
            retrieved = db.get_book_by_id("test-book-123")
            if retrieved:
                write_log("✅ Book retrieved successfully")
                
                # Clean up test book
                db.delete_books_by_ids(["test-book-123"])
                write_log("✅ Test book cleaned up")
            else:
                write_log("❌ Could not retrieve test book")
        else:
            write_log("❌ Book insertion failed")
            
    except Exception as e:
        write_log(f"❌ Book insertion test failed: {e}")
        import traceback
        write_log(f"Traceback: {traceback.format_exc()}")
    
    # Test Elasticsearch indexing
    write_log("\n🔍 Testing Elasticsearch Indexing...")
    try:
        if hasattr(bot, 'index_book') and bot.es_available():
            write_log("Testing index_book function...")
            # This would require a real book, so we'll just check if function exists
            write_log("✅ index_book function exists")
        else:
            write_log("❌ index_book function missing or ES not available")
    except Exception as e:
        write_log(f"❌ ES indexing test failed: {e}")
    
    write_log("\n🎯 Debug Summary:")
    write_log("1. Check if database is accessible and working")
    write_log("2. Check if Elasticsearch is running and accessible")
    write_log("3. Check if book insertion works")
    write_log("4. Check if indexing functions are available")
    
except Exception as e:
    write_log(f"❌ Critical error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Debug complete")
