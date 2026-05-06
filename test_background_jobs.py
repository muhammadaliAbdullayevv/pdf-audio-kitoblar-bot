#!/usr/bin/env python3
from __future__ import annotations

import importlib
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(__file__))

import db


def _cleanup_test_jobs() -> None:
    with db.db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM background_jobs WHERE job_type LIKE 'TEST_%' OR idempotency_key LIKE 'test:%'")


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    print("🧪 Background job tests")
    db.init_db()
    _cleanup_test_jobs()

    # create + claim + single-claim safety
    test_user = 910000001
    meta = db.create_background_job(
        "TEST_CREATE",
        test_user,
        {"value": "ok", "pdf_bytes": b"%PDF-test"},
        idempotency_key=f"test:create:{uuid.uuid4().hex}",
        ignore_limits=True,
    )
    _assert(bool(meta.get("ok")), "create_background_job should succeed")
    job_id = str(meta.get("job_id") or "")
    job = db.claim_background_job(
        f"test-worker-{uuid.uuid4().hex[:6]}",
        stale_after_seconds=1800,
        allowed_job_types=["TEST_CREATE"],
    )
    _assert(job is not None and str(job.get("id")) == job_id, "first worker should claim the created job")
    second = db.claim_background_job(
        f"test-worker-{uuid.uuid4().hex[:6]}",
        stale_after_seconds=1800,
        allowed_job_types=["TEST_CREATE"],
    )
    _assert(second is None, "second worker must not claim the same running job")
    db.complete_background_job(job_id, {"ok": True})

    # stale recovery
    stale_meta = db.create_background_job(
        "TEST_STALE",
        test_user,
        {"value": "stale"},
        idempotency_key=f"test:stale:{uuid.uuid4().hex}",
        ignore_limits=True,
    )
    stale_id = str(stale_meta.get("job_id") or "")
    claimed = db.claim_background_job(
        f"test-worker-{uuid.uuid4().hex[:6]}",
        stale_after_seconds=1800,
        allowed_job_types=["TEST_STALE"],
    )
    _assert(claimed is not None and str(claimed.get("id")) == stale_id, "stale test job should claim")
    with db.db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE background_jobs
                SET status='RUNNING',
                    locked_at=NOW() - INTERVAL '2 hours',
                    attempts=1,
                    max_attempts=3
                WHERE id=%s
                """,
                (stale_id,),
            )
    recovered = db.recover_stale_background_jobs(1)
    _assert(int(recovered.get("recovered") or 0) >= 1, "stale job should be recovered")
    reclaimed = db.claim_background_job(
        f"test-worker-{uuid.uuid4().hex[:6]}",
        stale_after_seconds=1800,
        allowed_job_types=["TEST_STALE"],
    )
    _assert(reclaimed is not None and str(reclaimed.get("id")) == stale_id, "recovered stale job should be claimable again")
    db.complete_background_job(stale_id, {"ok": True})

    # user limits
    os.environ["MAX_PENDING_JOBS_PER_USER"] = "1"
    os.environ["MAX_RUNNING_JOBS_PER_USER"] = "1"
    limit_user = 910000002
    first = db.create_background_job("TEST_LIMIT_PENDING", limit_user, {"v": 1}, idempotency_key=f"test:limitp:{uuid.uuid4().hex}")
    _assert(bool(first.get("ok")), "first pending job should be accepted")
    second = db.create_background_job("TEST_LIMIT_PENDING", limit_user, {"v": 2}, idempotency_key=f"test:limitp2:{uuid.uuid4().hex}")
    _assert(second.get("reason") == "pending_limit", "second pending job should hit pending limit")
    first_id = str(first.get("job_id") or "")
    claimed_limit = db.claim_background_job(
        f"test-worker-{uuid.uuid4().hex[:6]}",
        stale_after_seconds=1800,
        allowed_job_types=["TEST_LIMIT_PENDING", "TEST_LIMIT_RUNNING"],
    )
    _assert(claimed_limit is not None and str(claimed_limit.get("id")) == first_id, "limit test job should claim")
    third = db.create_background_job("TEST_LIMIT_RUNNING", limit_user, {"v": 3}, idempotency_key=f"test:limitr:{uuid.uuid4().hex}")
    _assert(third.get("reason") == "running_limit", "running limit should be enforced")
    db.complete_background_job(first_id, {"ok": True})

    # feature flag / ES fallback sanity
    os.environ["ENABLE_ELASTICSEARCH"] = "0"
    import bot
    importlib.reload(bot)
    _assert(bot.ENABLE_ELASTICSEARCH is False, "ENABLE_ELASTICSEARCH flag should reload as false")
    _assert(bot.get_es() is None, "get_es should be disabled when feature flag is false")
    _assert(bot.search_es("anything") == [], "search_es should fall back cleanly when ES is disabled")

    _cleanup_test_jobs()
    print("✅ background job tests passed")


if __name__ == "__main__":
    main()
