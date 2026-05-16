from __future__ import annotations

import os

from telegram import Bot
from telegram.ext import ApplicationBuilder


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def normalize_bot_api_base_url(raw: str) -> str:
    text = str(raw or "").strip().rstrip("/")
    if not text:
        return ""
    return text if text.endswith("/bot") else f"{text}/bot"


def normalize_bot_api_base_file_url(raw: str, fallback_base_url: str) -> str:
    text = str(raw or "").strip().rstrip("/")
    if text:
        return text if text.endswith("/file/bot") else f"{text}/file/bot"
    if fallback_base_url.endswith("/bot"):
        return f"{fallback_base_url[:-4]}/file/bot"
    return ""


def bot_api_settings() -> dict[str, object]:
    base_url = normalize_bot_api_base_url(os.getenv("TELEGRAM_BOT_API_BASE_URL", ""))
    base_file_url = normalize_bot_api_base_file_url(
        os.getenv("TELEGRAM_BOT_API_BASE_FILE_URL", ""),
        base_url,
    )
    local_mode = _env_bool("TELEGRAM_BOT_API_LOCAL_MODE", False)
    return {
        "base_url": base_url,
        "base_file_url": base_file_url,
        "local_mode": local_mode,
    }


def build_bot_client(token: str) -> Bot:
    settings = bot_api_settings()
    kwargs: dict[str, object] = {}
    if settings["base_url"]:
        kwargs["base_url"] = settings["base_url"]
    if settings["base_file_url"]:
        kwargs["base_file_url"] = settings["base_file_url"]
    if settings["local_mode"]:
        kwargs["local_mode"] = True
    return Bot(token=str(token or "").strip(), **kwargs)


def configure_application_builder(builder: ApplicationBuilder) -> ApplicationBuilder:
    settings = bot_api_settings()
    try:
        pool_timeout = max(5, int(os.getenv("BOT_POOL_TIMEOUT", "30") or "30"))
    except Exception:
        pool_timeout = 30
    try:
        connection_pool_size = max(2, int(os.getenv("BOT_CONNECTION_POOL_SIZE", "16") or "16"))
    except Exception:
        connection_pool_size = 16
    try:
        concurrent_updates = max(1, int(os.getenv("WHITE_LABEL_BOT_CONCURRENT_UPDATES", os.getenv("BOT_CONCURRENT_UPDATES", "4")) or "4"))
    except Exception:
        concurrent_updates = 4

    builder = (
        builder.connect_timeout(20)
        .read_timeout(60)
        .write_timeout(1200)
        .pool_timeout(pool_timeout)
        .connection_pool_size(connection_pool_size)
        .concurrent_updates(concurrent_updates)
    )
    if settings["base_url"]:
        builder = builder.base_url(str(settings["base_url"]))
    if settings["base_file_url"]:
        builder = builder.base_file_url(str(settings["base_file_url"]))
    if settings["local_mode"]:
        builder = builder.local_mode(True)
    return builder

