#!/usr/bin/env python3
"""
Bot Log Viewer - Real-time log monitoring script
Usage: python3 view_logs.py [--tail=50] [--follow] [--service=pdf_audio_kitoblar_bot-bot]
"""

import subprocess
import sys
import argparse
import time
from datetime import datetime

DEFAULT_SERVICE = "pdf_audio_kitoblar_bot-bot"


def _unit_name(service: str) -> str:
    return service if service.endswith(".service") else f"{service}.service"


def get_logs(tail_lines=50, follow=False, service=DEFAULT_SERVICE):
    """Get logs from systemd service"""
    unit = _unit_name(service)
    cmd = ["sudo", "journalctl", "-u", unit, "--no-pager"]
    
    if tail_lines > 0:
        cmd.extend(["-n", str(tail_lines)])
    
    if follow:
        cmd.append("-f")
    
    try:
        # Run the command and stream output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        print(f"🔍 Monitoring logs for {unit}...")
        print(f"📊 Showing last {tail_lines} lines" + (" (following)" if follow else ""))
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        for line in process.stdout:
            # Add timestamp for each line if following
            if follow:
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(f"[{timestamp}] {line.rstrip()}")
            else:
                print(line.rstrip())
                
        process.wait()
        
    except KeyboardInterrupt:
        print("\n👋 Stopped log monitoring")
        process.terminate()
    except Exception as e:
        print(f"❌ Error getting logs: {e}")

def show_recent_errors(service=DEFAULT_SERVICE):
    """Show only recent error logs"""
    print("🚨 Recent Error Logs (last 20):")
    print("=" * 50)
    
    cmd = ["sudo", "journalctl", "-u", _unit_name(service), "--no-pager", "-n", "50"]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        lines = result.stdout.split('\n')
        
        for line in lines:
            if any(keyword in line.lower() for keyword in ['error', 'exception', 'failed', 'critical']):
                print(line)
                
    except Exception as e:
        print(f"❌ Error getting error logs: {e}")

def show_service_status(service=DEFAULT_SERVICE):
    """Show current service status"""
    print("📊 Service Status:")
    print("=" * 30)
    
    try:
        # Get status
        result = subprocess.run(
            ["sudo", "systemctl", "status", _unit_name(service)], 
            capture_output=True, 
            text=True
        )
        
        # Get recent logs
        log_result = subprocess.run(
            ["sudo", "journalctl", "-u", _unit_name(service), "--no-pager", "-n", "5"],
            capture_output=True,
            text=True
        )
        
        print(result.stdout)
        if log_result.stdout.strip():
            print("\n📝 Recent Logs:")
            print("-" * 30)
            print(log_result.stdout)
            
    except Exception as e:
        print(f"❌ Error getting status: {e}")

def main():
    parser = argparse.ArgumentParser(description="View pdf_audio_kitoblar_bot logs")
    parser.add_argument("--tail", type=int, default=50, help="Number of lines to show (default: 50)")
    parser.add_argument("--follow", action="store_true", help="Follow logs in real-time")
    parser.add_argument("--errors", action="store_true", help="Show only recent errors")
    parser.add_argument("--status", action="store_true", help="Show service status")
    parser.add_argument("--service", default=DEFAULT_SERVICE, help=f"Service name (default: {DEFAULT_SERVICE})")
    
    args = parser.parse_args()
    
    if args.status:
        show_service_status(args.service)
    elif args.errors:
        show_recent_errors(args.service)
    else:
        get_logs(args.tail, args.follow, args.service)

if __name__ == "__main__":
    main()
