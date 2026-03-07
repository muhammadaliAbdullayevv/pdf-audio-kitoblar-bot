#!/usr/bin/env python3
"""Check recent logs for upload issues"""

import os
import subprocess
from datetime import datetime

def write_log(message):
    print(message)
    with open("log_analysis.txt", "a") as f:
        f.write(f"{datetime.now()}: {message}\n")

write_log("🔍 Log Analysis for Upload Issues")
write_log("=" * 50)

# Check error logs
write_log("\n📋 Checking error logs...")
try:
    if os.path.exists("logs/errors.log"):
        with open("logs/errors.log", "r") as f:
            lines = f.readlines()
            recent_errors = lines[-20:]  # Last 20 lines
            if recent_errors:
                write_log("Recent error log entries:")
                for i, line in enumerate(recent_errors):
                    write_log(f"{i+1}: {line.strip()}")
            else:
                write_log("No recent error log entries")
    else:
        write_log("Error log file not found")
except Exception as e:
    write_log(f"Error reading error logs: {e}")

# Check bot logs via journalctl
write_log("\n📋 Checking systemd logs...")
try:
    result = subprocess.run(
        ["sudo", "journalctl", "-u", "SmartAIToolsBot-bot.service", "--no-pager", "-n", "30"],
        capture_output=True,
        text=True,
        timeout=10
    )
    if result.stdout:
        write_log("Recent systemd log entries:")
        for line in result.stdout.split('\n')[-20:]:
            if line.strip():
                write_log(f"  {line}")
    else:
        write_log("No systemd log output")
except Exception as e:
    write_log(f"Error reading systemd logs: {e}")

# Check if bot is running
write_log("\n📋 Checking bot status...")
try:
    result = subprocess.run(
        ["sudo", "systemctl", "status", "SmartAIToolsBot-bot.service"],
        capture_output=True,
        text=True,
        timeout=5
    )
    if result.stdout:
        write_log("Bot status:")
        for line in result.stdout.split('\n'):
            if "Active:" in line or "Main PID:" in line or "Tasks:" in line:
                write_log(f"  {line.strip()}")
    else:
        write_log("No status output")
except Exception as e:
    write_log(f"Error checking bot status: {e}")

# Check for any upload-related log entries
write_log("\n📋 Looking for upload-related entries...")
try:
    if os.path.exists("logs/errors.log"):
        with open("logs/errors.log", "r") as f:
            content = f.read()
            upload_keywords = ["upload", "handle_file", "_process_upload", "duplicate"]
            found_lines = []
            for line_num, line in enumerate(content.split('\n'), 1):
                if any(keyword.lower() in line.lower() for keyword in upload_keywords):
                    found_lines.append(f"Line {line_num}: {line.strip()}")
            
            if found_lines:
                write_log("Upload-related log entries:")
                for line in found_lines[-10:]:  # Last 10 matches
                    write_log(f"  {line}")
            else:
                write_log("No upload-related entries found in error logs")
except Exception as e:
    write_log(f"Error searching upload logs: {e}")

write_log("\n✅ Log analysis complete")
