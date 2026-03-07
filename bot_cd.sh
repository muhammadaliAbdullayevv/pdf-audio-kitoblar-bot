#!/bin/bash

# Bot Navigation Script - Quick access to bot directory
# Usage: source bot_cd.sh or . bot_cd.sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the bot directory
cd "$SCRIPT_DIR"

# Show current directory and available commands
echo "🤖 Navigated to: $(pwd)"
echo ""
echo "📋 Available Commands:"
echo "  python3 view_logs.py --follow     # Monitor logs in real-time"
echo "  python3 view_logs.py --status      # Check service status"
echo "  python3 view_logs.py --errors      # Show recent errors"
echo "  sudo systemctl restart SmartAIToolsBot-bot.service  # Restart bot"
echo "  sudo systemctl status SmartAIToolsBot-bot.service   # Check status"
echo ""
echo "🔧 Quick Start:"
echo "  1. Monitor logs: python3 view_logs.py --follow"
echo "  2. Restart bot:  sudo systemctl restart SmartAIToolsBot-bot.service"
echo ""
