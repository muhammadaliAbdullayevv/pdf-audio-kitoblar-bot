import os
from dotenv import load_dotenv

# Load variables from project .env using an absolute path so systemd cwd does not matter.
# Use override=True so .env values win over existing shell env vars.
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_PATH = os.path.join(_BASE_DIR, ".env")
load_dotenv(dotenv_path=_ENV_PATH, override=True)

# Load secrets/config from environment to avoid committing credentials.
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0"))
# Admin must be explicitly configured (or falls back to OWNER_ID).
# Never hardcode a real user ID as a fallback: it can accidentally grant admin powers.
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0")) or OWNER_ID
REQUEST_CHAT_ID = int(os.getenv("REQUEST_CHAT_ID", "0"))
_UPLOAD_CHANNEL_ID_RAW = os.getenv("UPLOAD_CHANNEL_ID", "").strip()
_UPLOAD_CHANNEL_IDS_RAW = os.getenv("UPLOAD_CHANNEL_IDS", "").strip()

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

_ids = _parse_id_list(_UPLOAD_CHANNEL_IDS_RAW)
if not _ids:
    # Allow comma-separated IDs in UPLOAD_CHANNEL_ID as well
    _ids = _parse_id_list(_UPLOAD_CHANNEL_ID_RAW)

UPLOAD_CHANNEL_IDS = _ids
UPLOAD_CHANNEL_ID = _ids[0] if _ids else int(_UPLOAD_CHANNEL_ID_RAW or "0")
AUDIO_UPLOAD_CHANNEL_ID = int(os.getenv("AUDIO_UPLOAD_CHANNEL_ID", "0") or "0")
VIDEO_UPLOAD_CHANNEL_ID = int(os.getenv("VIDEO_UPLOAD_CHANNEL_ID", "0") or "0")

# Coins / leaderboard settings
COIN_SEARCH = int(os.getenv("COIN_SEARCH", "1"))
COIN_DOWNLOAD = int(os.getenv("COIN_DOWNLOAD", "1"))
COIN_REACTION = int(os.getenv("COIN_REACTION", "1"))
COIN_FAVORITE = int(os.getenv("COIN_FAVORITE", "1"))
COIN_REFERRAL = int(os.getenv("COIN_REFERRAL", "15"))
TOP_USERS_LIMIT = int(os.getenv("TOP_USERS_LIMIT", "10"))
