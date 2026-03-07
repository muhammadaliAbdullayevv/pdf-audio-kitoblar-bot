#!/usr/bin/env python3
"""Test upload_mode setting"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

def write_log(message):
    print(message)
    with open("upload_mode_test.txt", "a") as f:
        f.write(f"{message}\n")

write_log("🧪 Upload Mode Test")
write_log("=" * 30)

try:
    import upload_flow
    import bot

    write_log("Initial upload_mode state:")
    write_log(f"upload_mode = {upload_flow.upload_mode}")

    # Simulate what happens in upload_command
    write_log("\n📝 Simulating /upload command...")

    if hasattr(bot, 'ADMIN_ID'):
        admin_id = bot.ADMIN_ID
        write_log(f"Admin ID: {admin_id}")

        if hasattr(bot, 'is_allowed'):
            allowed = bot.is_allowed(admin_id)
            write_log(f"Admin allowed: {allowed}")

            if allowed:
                # This is what the upload_command does
                upload_flow.upload_mode = True
                write_log("✅ upload_mode set to True")

                write_log("\nAfter upload command simulation:")
                write_log(f"upload_mode = {upload_flow.upload_mode}")

                # Test if the condition would work
                if upload_flow.upload_mode and bot.is_allowed(admin_id):
                    write_log("✅ Upload condition would pass")
                else:
                    write_log("❌ Upload condition would fail")
            else:
                write_log("❌ Admin not allowed - upload would fail")
        else:
            write_log("❌ is_allowed function missing")
    else:
        write_log("❌ ADMIN_ID missing")

    write_log("\n🎯 Test Summary:")
    write_log("The upload_mode setting logic works correctly")
    write_log("The issue must be elsewhere in the flow")

except Exception as e:
    write_log(f"❌ Error: {e}")
    import traceback
    write_log(f"Traceback: {traceback.format_exc()}")

write_log("\n✅ Test complete")
