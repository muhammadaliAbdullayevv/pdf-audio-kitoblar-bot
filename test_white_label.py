#!/usr/bin/env python3
from __future__ import annotations

import base64
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(__file__))

import db
from white_label import WL_CACHE_STATUS_INVALID, WL_CACHE_STATUS_VALID, WL_SEED_STATUS_PENDING
from white_label.cache_seeding import build_cache_seed_caption, parse_cache_seed_caption
from white_label.crypto import (
    decrypt_bot_token,
    encrypt_bot_token,
    fingerprint_bot_token,
    is_crypto_available,
    mask_bot_token,
    redact_token_like_strings,
)
from white_label.db_helpers import (
    accept_connected_bot_request,
    create_connected_bot_cache_seed_job,
    create_connected_bot_request,
    delete_connected_bot,
    find_existing_connected_bot_request_or_bot,
    get_active_connected_bot_cache_seed_job,
    get_connected_bot_file_cache,
    get_connected_bot_usage,
    increment_connected_bot_usage,
    mark_connected_bot_file_cache_invalid,
    reject_connected_bot_request,
    upsert_connected_bot,
    upsert_connected_bot_file_cache,
)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _test_crypto() -> None:
    key = base64.urlsafe_b64encode(b"1" * 32).decode()
    token = "123456:ABCdefGhIJKlmnOP_qrstUVwxyz123456"

    masked = mask_bot_token(token)
    _assert(masked.startswith("123456:"), "masked token should keep the prefix")
    _assert("***" in masked, "masked token should hide the middle")

    redacted = redact_token_like_strings(f"token={token}")
    _assert(token not in redacted, "redacted string must not contain the raw token")
    _assert("***" in redacted, "redacted string should contain a masked token")

    fingerprint = fingerprint_bot_token(token, key)
    _assert(len(fingerprint) == 64, "token fingerprint should be a sha256 hex digest")

    if not is_crypto_available():
        print("⚠️ cryptography is not installed; encrypt/decrypt test skipped")
        return

    encrypted = encrypt_bot_token(token, key)
    decrypted = decrypt_bot_token(encrypted, key)
    _assert(decrypted == token, "encrypted token should decrypt back to the original token")


def _test_cache_caption() -> None:
    token = uuid.uuid4().hex
    caption = build_cache_seed_caption(token, "book-123", "bot-456")
    parsed = parse_cache_seed_caption(caption)
    _assert(parsed is not None, "cache seed caption should parse")
    _assert(parsed["seed_token"] == token, "parsed seed token should match")
    _assert(parsed["book_id"] == "book-123", "parsed book id should match")
    _assert(parsed["connected_bot_id"] == "bot-456", "parsed connected bot id should match")


def _cleanup_db_rows(connected_bot_id: str) -> None:
    with db.db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM connected_bot_usage WHERE connected_bot_id=%s", (connected_bot_id,))
            cur.execute("DELETE FROM connected_bot_cache_seed_jobs WHERE connected_bot_id=%s", (connected_bot_id,))
            cur.execute("DELETE FROM connected_bot_file_cache WHERE connected_bot_id=%s", (connected_bot_id,))
            cur.execute("DELETE FROM connected_bots WHERE id=%s", (connected_bot_id,))


def _cleanup_db_request(request_id: str, connected_bot_id: str | None = None) -> None:
    with db.db_conn() as conn:
        with conn.cursor() as cur:
            if connected_bot_id:
                cur.execute("DELETE FROM connected_bots WHERE id=%s", (connected_bot_id,))
            cur.execute("DELETE FROM connected_bot_requests WHERE id=%s", (request_id,))


def _test_db_helpers() -> None:
    key = base64.urlsafe_b64encode(b"2" * 32).decode()
    token = f"654321:{uuid.uuid4().hex}"
    bot_telegram_id = 910000000 + (uuid.uuid4().int % 100000)
    bot_username = f"wltestbot_{uuid.uuid4().hex[:8]}"
    encrypted = encrypt_bot_token(token, key) if is_crypto_available() else "encrypted-token-placeholder"
    fingerprint = fingerprint_bot_token(token, key)

    request_token = f"777777:{uuid.uuid4().hex}"
    request_encrypted = encrypt_bot_token(request_token, key) if is_crypto_available() else "encrypted-request-token-placeholder"
    request_fingerprint = fingerprint_bot_token(request_token, key)
    request_bot_id = 920000000 + (uuid.uuid4().int % 100000)
    request_row = create_connected_bot_request(
        requesting_user_id=999999002,
        requesting_username="requester",
        requesting_first_name="Requester",
        bot_telegram_id=request_bot_id,
        bot_username=f"wlrequestbot_{uuid.uuid4().hex[:8]}",
        bot_first_name="WL Request Bot",
        bot_token_encrypted=request_encrypted,
        bot_token_fingerprint=request_fingerprint,
        token_masked="777777:***test",
    )
    request_id = str(request_row.get("id") or "")
    accepted_bot_id = ""
    try:
        duplicate = find_existing_connected_bot_request_or_bot(
            token_fingerprint=request_fingerprint,
            bot_telegram_id=request_bot_id,
        )
        _assert(duplicate is not None and str(duplicate.get("source") or "") == "request", "pending request duplicate should be found")
        accepted = accept_connected_bot_request(
            request_id,
            accepted_by_owner_id=999999000,
            cache_channel_id=-1001234567890,
            trial_days=3,
            daily_search_limit=100,
            daily_send_limit=20,
            per_minute_send_limit=10,
        )
        accepted_bot_id = str((accepted or {}).get("id") or "")
        _assert(accepted_bot_id, "accepting request should create a connected bot")
        _assert(str((accepted or {}).get("plan") or "") == "TRIAL", "accepted bot should start on TRIAL plan")
        _assert(int((accepted or {}).get("daily_send_limit") or 0) == 20, "accepted bot should use trial send limit")
    finally:
        _cleanup_db_request(request_id, accepted_bot_id or None)

    reject_token = f"888888:{uuid.uuid4().hex}"
    reject_fp = fingerprint_bot_token(reject_token, key)
    reject_req = create_connected_bot_request(
        requesting_user_id=999999003,
        requesting_username=None,
        requesting_first_name="Rejecter",
        bot_telegram_id=930000000 + (uuid.uuid4().int % 100000),
        bot_username=f"wlrejectbot_{uuid.uuid4().hex[:8]}",
        bot_first_name="WL Reject Bot",
        bot_token_encrypted=encrypt_bot_token(reject_token, key) if is_crypto_available() else "encrypted-reject-token-placeholder",
        bot_token_fingerprint=reject_fp,
        token_masked="888888:***test",
    )
    reject_request_id = str(reject_req.get("id") or "")
    try:
        rejected = reject_connected_bot_request(reject_request_id, rejected_by_owner_id=999999000, reason="test")
        _assert(str((rejected or {}).get("status") or "") == "REJECTED", "reject helper should mark request rejected")
    finally:
        _cleanup_db_request(reject_request_id)

    books = db.list_books()
    if not books:
        print("⚠️ No books in the catalog, skipping DB-backed white-label cache tests")
        return
    book_id = str((books[0] or {}).get("id") or "").strip()
    _assert(book_id, "test catalog book id is required")

    row = upsert_connected_bot(
        owner_telegram_id=999999001,
        bot_telegram_id=bot_telegram_id,
        bot_username=bot_username,
        bot_first_name="WL Test Bot",
        bot_token_encrypted=encrypted,
        bot_token_fingerprint=fingerprint,
        status="SUSPENDED",
        plan="MANUAL",
    )
    connected_bot_id = str(row.get("id") or "")
    _assert(connected_bot_id, "connected bot id should be created")
    try:
        seed_job = create_connected_bot_cache_seed_job(
            connected_bot_id=connected_bot_id,
            book_id=book_id,
            requesting_chat_id=123456789,
            requesting_user_id=123456789,
            requesting_message_id=101,
            cache_channel_id=-1001234567890,
            seed_token=uuid.uuid4().hex,
        )
        _assert(str(seed_job.get("status") or "") == WL_SEED_STATUS_PENDING, "new seed job should start as PENDING")

        active_job = get_active_connected_bot_cache_seed_job(connected_bot_id, book_id)
        _assert(active_job is not None, "active seed job should be discoverable")

        cache_row = upsert_connected_bot_file_cache(
            connected_bot_id=connected_bot_id,
            book_id=book_id,
            telegram_file_id="BQACAgIAAxkBAAIBAWexamplefileid",
            telegram_file_unique_id="AgADexampleuniqueid",
            cache_channel_id=-1001234567890,
            cache_message_id=555,
            status=WL_CACHE_STATUS_VALID,
        )
        _assert(str(cache_row.get("status") or "") == WL_CACHE_STATUS_VALID, "cache row should be valid after upsert")

        fetched_valid = get_connected_bot_file_cache(connected_bot_id, book_id, only_valid=True)
        _assert(fetched_valid is not None, "valid cache lookup should find the seeded row")

        invalidated = mark_connected_bot_file_cache_invalid(connected_bot_id, book_id, "wrong file identifier")
        _assert(invalidated == 1, "mark invalid should update exactly one cache row")
        fetched_any = get_connected_bot_file_cache(connected_bot_id, book_id, only_valid=False)
        _assert(str(fetched_any.get("status") or "") == WL_CACHE_STATUS_INVALID, "cache row should become INVALID")

        usage = increment_connected_bot_usage(connected_bot_id, searches=2, sends=1, cache_hits=1, cache_misses=1)
        _assert(int(usage.get("searches") or 0) == 2, "usage should accumulate searches")
        _assert(int(usage.get("sends") or 0) == 1, "usage should accumulate sends")
        usage_after = get_connected_bot_usage(connected_bot_id)
        _assert(int(usage_after.get("cache_hits") or 0) == 1, "usage should persist cache hits")
    finally:
        _cleanup_db_rows(connected_bot_id)
        delete_connected_bot(connected_bot_id)


def main() -> None:
    print("🧪 White-label MVP tests")
    _test_crypto()
    _test_cache_caption()

    try:
        db.init_db()
    except Exception as exc:
        print(f"⚠️ DB unavailable, skipping DB-backed white-label tests: {exc}")
        print("✅ white-label non-DB tests passed")
        return

    _test_db_helpers()
    print("✅ white-label tests passed")


if __name__ == "__main__":
    main()
