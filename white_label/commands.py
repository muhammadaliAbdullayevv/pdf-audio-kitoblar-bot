from __future__ import annotations

from typing import Iterable

from .crypto import mask_bot_token


def normalize_connected_bot_identifier(raw: str | None) -> str:
    text = str(raw or "").strip()
    return text[1:] if text.startswith("@") else text


def format_connected_bot_reference(row: dict | None) -> str:
    if not row:
        return "-"
    username = str((row or {}).get("bot_username") or "").strip()
    if username:
        return f"@{username.lstrip('@')}"
    return str((row or {}).get("id") or "-")


def build_connected_bot_list_text(rows: Iterable[dict] | None) -> str:
    items = list(rows or [])
    if not items:
        return "No connected bots yet."
    lines = ["🤖 Connected bots"]
    for idx, row in enumerate(items, start=1):
        cache_channel_id = row.get("cache_channel_id")
        cache_channel_text = str(cache_channel_id) if cache_channel_id else "not set"
        lines.extend(
            [
                "",
                f"{idx}. {format_connected_bot_reference(row)}",
                f"ID: {row.get('id')}",
                f"Status: {row.get('status')}",
                f"Plan: {row.get('plan')}",
                f"Cache channel: {cache_channel_text}",
                f"Daily search/send: {row.get('daily_search_limit')}/{row.get('daily_send_limit')}",
            ]
        )
    return "\n".join(lines)


def describe_token_for_owner(token: str) -> str:
    return mask_bot_token(token)

