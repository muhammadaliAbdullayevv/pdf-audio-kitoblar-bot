#!/bin/bash
# Check recent bot logs and write to file
echo "Checking bot logs..." > upload_debug_logs.txt
sudo journalctl -u pdf_audio_kitoblar_bot-bot.service -n 50 --no-pager >> upload_debug_logs.txt 2>&1
echo "Logs saved to upload_debug_logs.txt" >> upload_debug_logs.txt
