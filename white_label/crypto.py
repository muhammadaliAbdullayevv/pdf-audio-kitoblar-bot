from __future__ import annotations

import hashlib
import hmac
import re

try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:  # pragma: no cover - optional dependency at runtime
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore


_TOKEN_RE = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")


def is_crypto_available() -> bool:
    return Fernet is not None


def require_crypto() -> None:
    if Fernet is None:
        raise RuntimeError("cryptography is required for white-label connected bot tokens")


def validate_encryption_key(secret: str) -> bytes:
    require_crypto()
    clean = str(secret or "").strip()
    if not clean:
        raise ValueError("CONNECTED_BOT_TOKEN_ENCRYPTION_KEY is missing")
    key_bytes = clean.encode("utf-8")
    Fernet(key_bytes)  # type: ignore[arg-type]
    return key_bytes


def mask_bot_token(token: str) -> str:
    clean = str(token or "").strip()
    if not clean:
        return "<empty>"
    head, sep, tail = clean.partition(":")
    if not sep:
        if len(clean) <= 4:
            return "***"
        return f"{clean[:2]}***{clean[-2:]}"
    suffix = tail[-4:] if len(tail) >= 4 else tail
    return f"{head}:***{suffix}"


def redact_token_like_strings(text: str) -> str:
    raw = str(text or "")
    if not raw:
        return raw
    return _TOKEN_RE.sub(lambda m: mask_bot_token(m.group(0)), raw)


def fingerprint_bot_token(token: str, secret: str) -> str:
    key = validate_encryption_key(secret)
    return hmac.new(key, str(token or "").encode("utf-8"), hashlib.sha256).hexdigest()


def encrypt_bot_token(token: str, secret: str) -> str:
    key = validate_encryption_key(secret)
    clean = str(token or "").strip()
    if not clean:
        raise ValueError("bot token is empty")
    return Fernet(key).encrypt(clean.encode("utf-8")).decode("utf-8")  # type: ignore[arg-type]


def decrypt_bot_token(token_encrypted: str, secret: str) -> str:
    key = validate_encryption_key(secret)
    clean = str(token_encrypted or "").strip()
    if not clean:
        raise ValueError("encrypted bot token is empty")
    try:
        return Fernet(key).decrypt(clean.encode("utf-8")).decode("utf-8")  # type: ignore[arg-type]
    except InvalidToken as exc:  # type: ignore[misc]
        raise RuntimeError("connected bot token could not be decrypted") from exc

