import os
from dotenv import dotenv_values, load_dotenv

_CONFIG_ERRORS: list[str] = []


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int = 0) -> int:
    raw = str(os.getenv(name, str(default)) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        _CONFIG_ERRORS.append(f"{name} must be an integer value")
        return int(default)


# Load variables from project .env using an absolute path so systemd cwd does not matter.
# By default, do not override existing process env vars (safer for systemd EnvironmentFile).
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_BASE_DIR, ".env")
_DOTENV_OVERRIDE = _env_bool("DOTENV_OVERRIDE", False)
load_dotenv(dotenv_path=_ENV_PATH, override=_DOTENV_OVERRIDE)
_PROJECT_DOTENV = dotenv_values(_ENV_PATH) if os.path.exists(_ENV_PATH) else {}


def _env_project_first(name: str, default: str = "") -> str:
    value = _PROJECT_DOTENV.get(name, None)
    if value is not None:
        text = str(value).strip()
        if text:
            return text
    return str(os.getenv(name, default) or "").strip()

# Load secrets/config from environment to avoid committing credentials.
TOKEN = _env_project_first("TELEGRAM_BOT_TOKEN", "")
OWNER_ID = _env_int("TELEGRAM_OWNER_ID", 0)
# Legacy compatibility only. Runtime authorization is owner-only.
ADMIN_ID = _env_int("TELEGRAM_ADMIN_ID", 0)
REQUEST_CHAT_ID = _env_int("REQUEST_CHAT_ID", 0)
try:
    _req_project = _env_project_first("REQUEST_CHAT_ID", "")
    if _req_project:
        REQUEST_CHAT_ID = int(_req_project)
except Exception:
    pass
_BOOK_STORAGE_CHANNEL_ID_RAW = _env_project_first("BOOK_STORAGE_CHANNEL_ID", "")
try:
    BOOK_STORAGE_CHANNEL_ID = int(_BOOK_STORAGE_CHANNEL_ID_RAW) if _BOOK_STORAGE_CHANNEL_ID_RAW else 0
except Exception:
    BOOK_STORAGE_CHANNEL_ID = 0
_AUDIO_UPLOAD_CHANNEL_ID_RAW = _env_project_first("AUDIO_UPLOAD_CHANNEL_ID", "")
_AUDIO_UPLOAD_CHANNEL_IDS_RAW = _env_project_first("AUDIO_UPLOAD_CHANNEL_IDS", "")
_VIDEO_UPLOAD_CHANNEL_ID_RAW = _env_project_first("VIDEO_UPLOAD_CHANNEL_ID", "")
_VIDEO_UPLOAD_CHANNEL_IDS_RAW = _env_project_first("VIDEO_UPLOAD_CHANNEL_IDS", "")

def _parse_id_list(raw: str) -> list[int]:
    items = []
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except ValueError:
            continue
    return items

_audio_ids = _parse_id_list(_AUDIO_UPLOAD_CHANNEL_IDS_RAW)
if not _audio_ids:
    # Allow comma-separated IDs in AUDIO_UPLOAD_CHANNEL_ID as well.
    _audio_ids = _parse_id_list(_AUDIO_UPLOAD_CHANNEL_ID_RAW)

AUDIO_UPLOAD_CHANNEL_IDS = _audio_ids
AUDIO_UPLOAD_CHANNEL_ID = _audio_ids[0] if _audio_ids else _env_int("AUDIO_UPLOAD_CHANNEL_ID", 0)

_video_ids = _parse_id_list(_VIDEO_UPLOAD_CHANNEL_IDS_RAW)
if not _video_ids:
    # Allow comma-separated IDs in VIDEO_UPLOAD_CHANNEL_ID as well.
    _video_ids = _parse_id_list(_VIDEO_UPLOAD_CHANNEL_ID_RAW)

VIDEO_UPLOAD_CHANNEL_IDS = _video_ids
VIDEO_UPLOAD_CHANNEL_ID = _video_ids[0] if _video_ids else _env_int("VIDEO_UPLOAD_CHANNEL_ID", 0)

# Coins / leaderboard settings
COIN_SEARCH = _env_int("COIN_SEARCH", 1)
COIN_DOWNLOAD = _env_int("COIN_DOWNLOAD", 1)
COIN_REACTION = _env_int("COIN_REACTION", 1)
COIN_FAVORITE = _env_int("COIN_FAVORITE", 1)
COIN_REFERRAL = _env_int("COIN_REFERRAL", 15)
TOP_USERS_LIMIT = _env_int("TOP_USERS_LIMIT", 10)


def validate_runtime_config() -> list[str]:
    errors = list(_CONFIG_ERRORS)
    if not TOKEN or not isinstance(TOKEN, str) or len(TOKEN) < 10:
        errors.append("TELEGRAM_BOT_TOKEN is missing or invalid")
    if OWNER_ID <= 0:
        errors.append("TELEGRAM_OWNER_ID must be a positive integer")

    for key in ("DB_NAME", "DB_USER", "DB_PASS", "DB_HOST"):
        if not str(os.getenv(key, "") or "").strip():
            errors.append(f"{key} is required")
    db_port_raw = str(os.getenv("DB_PORT", "5432") or "").strip()
    if not db_port_raw:
        errors.append("DB_PORT is required")
    else:
        try:
            if int(db_port_raw) <= 0:
                errors.append("DB_PORT must be a positive integer")
        except Exception:
            errors.append("DB_PORT must be a valid integer")

    return errors
