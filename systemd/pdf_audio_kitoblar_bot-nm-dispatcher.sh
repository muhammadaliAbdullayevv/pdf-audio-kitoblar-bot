#!/usr/bin/env bash
set -euo pipefail

# NetworkManager dispatcher hook for controlling pdf_audio_kitoblar_bot services
# based on Wi-Fi connectivity changes.
# Behavior:
# - Wi-Fi disconnected: stop services once
# - Wi-Fi reconnected (after disconnect): restart services once
# - Repeated events in same state: no action

BOT_API_SERVICE="pdf_audio_kitoblar_bot.service"
BOT_SERVICE="pdf_audio_kitoblar_bot-bot.service"
STATE_FILE="/run/pdf_audio_kitoblar_bot-dispatcher.state"
LOCK_FILE="/run/pdf_audio_kitoblar_bot-dispatcher.lock"
LOGGER_TAG="pdf_audio_kitoblar_bot-dispatcher"

log() {
  /usr/bin/logger -t "${LOGGER_TAG}" "$*"
}

wifi_connected() {
  /usr/bin/nmcli -t -f TYPE,STATE device status | /usr/bin/awk -F: '$1=="wifi" && $2=="connected" {ok=1} END{exit ok?0:1}'
}

read_prev_state() {
  [[ -f "${STATE_FILE}" ]] || return 0
  /usr/bin/cat "${STATE_FILE}" 2>/dev/null || true
}

write_state() {
  printf '%s\n' "$1" > "${STATE_FILE}"
}

stack_is_active() {
  /usr/bin/systemctl is-active --quiet "${BOT_API_SERVICE}" && /usr/bin/systemctl is-active --quiet "${BOT_SERVICE}"
}

start_stack() {
  /usr/bin/systemctl start "${BOT_API_SERVICE}" "${BOT_SERVICE}" >/dev/null 2>&1 || true
}

stop_stack() {
  /usr/bin/systemctl stop "${BOT_SERVICE}" "${BOT_API_SERVICE}" >/dev/null 2>&1 || true
}

restart_stack() {
  /usr/bin/systemctl restart "${BOT_API_SERVICE}" "${BOT_SERVICE}" >/dev/null 2>&1 || true
}

main() {
  local iface="${1:-unknown}"
  local action="${2:-unknown}"
  local current_state="disconnected"
  local prev_state=""

  # Prevent duplicate parallel actions from rapid NM events.
  exec 9>"${LOCK_FILE}"
  if ! /usr/bin/flock -n 9; then
    exit 0
  fi

  if wifi_connected; then
    current_state="connected"
  fi
  prev_state="$(read_prev_state)"

  # Ignore duplicate NetworkManager events while staying in same state.
  if [[ "${prev_state}" == "${current_state}" ]]; then
    exit 0
  fi
  write_state "${current_state}"

  if [[ "${current_state}" == "connected" ]]; then
    # Reconnect case: exactly one restart per disconnected -> connected transition.
    if [[ "${prev_state}" == "disconnected" ]]; then
      restart_stack
      log "wifi connected (${iface}/${action}) -> restart ${BOT_API_SERVICE} + ${BOT_SERVICE}"
      exit 0
    fi

    # First seen event after boot: start only if not running.
    if ! stack_is_active; then
      start_stack
      log "wifi connected (${iface}/${action}) -> start ${BOT_API_SERVICE} + ${BOT_SERVICE}"
    else
      log "wifi connected (${iface}/${action}) -> no action (already active)"
    fi
    exit 0
  fi

  # Disconnected: stop once and do not trigger restart loops.
  stop_stack
  log "wifi disconnected (${iface}/${action}) -> stop ${BOT_API_SERVICE} + ${BOT_SERVICE}"
}

main "$@"
