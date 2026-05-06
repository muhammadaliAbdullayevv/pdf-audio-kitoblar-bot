#!/usr/bin/env bash
set -u

LOG_FILE="$HOME/.local/state/pdf_audio_kitoblar_bot/hotkey-restart.log"
mkdir -p "$(dirname "$LOG_FILE")"
LOCK_FILE="$HOME/.local/state/pdf_audio_kitoblar_bot/hotkey-restart.lock"
exec 9>"$LOCK_FILE"

BOT_SERVICE="pdf_audio_kitoblar_bot-bot.service"
WORKER_SERVICE="pdf_audio_kitoblar_bot-worker.service"
CLEANUP_SERVICE="pdf_audio_kitoblar_bot-cleanup.service"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" >> "$LOG_FILE"; }
notify() { /usr/bin/notify-send "pdf_audio_kitoblar_bot" "$1"; }

if ! flock -n 9; then
  log "hotkey ignored: restart already in progress"
  notify "Restart already in progress"
  exit 0
fi

restart_unit() {
  local unit="$1"
  if sudo -n /usr/bin/systemctl restart "$unit"; then
    log "$unit restart ok"
    return 0
  fi
  log "$unit restart failed"
  return 1
}

restart_bot_last() {
  restart_unit "$CLEANUP_SERVICE" && cleanup_ok=1
  restart_unit "$WORKER_SERVICE" && worker_ok=1
  if sudo -n /usr/bin/systemctl restart "$BOT_SERVICE"; then
    log "$BOT_SERVICE restart ok"
    bot_ok=1
    return 0
  fi
  log "$BOT_SERVICE restart failed"
  return 1
}

log "hotkey triggered"

bot_ok=0
worker_ok=0
cleanup_ok=0

restart_bot_last

if [ "$bot_ok" -eq 1 ] && [ "$worker_ok" -eq 1 ] && [ "$cleanup_ok" -eq 1 ]; then
  notify "Bot + worker + cleanup restarted"
  exit 0
fi

if [ "$bot_ok" -eq 1 ]; then
  notify "Bot restarted, but worker/cleanup had issues"
  exit 0
fi

notify "Restart failed (sudo). Update hotkey sudoers rule."
exit 1
