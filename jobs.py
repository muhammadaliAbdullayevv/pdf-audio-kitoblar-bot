from __future__ import annotations

import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


_BASE_DIR = Path(__file__).resolve().parent
_JOB_ROOT = _BASE_DIR / "tmp" / "jobs"
_FAILED_MARKER = ".failed"


def job_root_dir() -> Path:
    _JOB_ROOT.mkdir(parents=True, exist_ok=True)
    return _JOB_ROOT


def job_dir(job_id: str) -> Path:
    safe_id = str(job_id or "").strip() or "unknown"
    root = job_root_dir() / safe_id
    root.mkdir(parents=True, exist_ok=True)
    (root / "input").mkdir(parents=True, exist_ok=True)
    (root / "output").mkdir(parents=True, exist_ok=True)
    return root


def mark_job_failed(job_id: str) -> None:
    root = job_dir(job_id)
    marker = root / _FAILED_MARKER
    marker.write_text(str(int(time.time())), encoding="utf-8")


def clear_job_failed_marker(job_id: str) -> None:
    marker = job_dir(job_id) / _FAILED_MARKER
    try:
        if marker.exists():
            marker.unlink()
    except Exception:
        pass


def remove_job_dir(job_id: str) -> None:
    root = job_root_dir() / (str(job_id or "").strip() or "unknown")
    shutil.rmtree(root, ignore_errors=True)


@contextmanager
def job_temp_environment(job_id: str) -> Iterator[Path]:
    root = job_dir(job_id)
    tmp_path = root / "input"
    tmp_path.mkdir(parents=True, exist_ok=True)
    old_tmp = tempfile.tempdir
    old_env = {key: os.environ.get(key) for key in ("TMPDIR", "TEMP", "TMP")}
    try:
        tempfile.tempdir = str(tmp_path)
        os.environ["TMPDIR"] = str(tmp_path)
        os.environ["TEMP"] = str(tmp_path)
        os.environ["TMP"] = str(tmp_path)
        yield root
    finally:
        tempfile.tempdir = old_tmp
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def prune_job_dirs(temp_job_ttl_hours: int = 24, failed_job_ttl_hours: int = 48) -> dict[str, int]:
    root = job_root_dir()
    now = time.time()
    normal_ttl = max(1, int(temp_job_ttl_hours or 24)) * 3600
    failed_ttl = max(1, int(failed_job_ttl_hours or 48)) * 3600
    deleted = 0
    failed_deleted = 0
    kept = 0

    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            marker = child / _FAILED_MARKER
            ttl = failed_ttl if marker.exists() else normal_ttl
            ref_ts = marker.stat().st_mtime if marker.exists() else child.stat().st_mtime
            if now - ref_ts >= ttl:
                shutil.rmtree(child, ignore_errors=True)
                if marker.exists():
                    failed_deleted += 1
                else:
                    deleted += 1
            else:
                kept += 1
        except Exception:
            kept += 1

    return {
        "deleted": deleted,
        "failed_deleted": failed_deleted,
        "kept": kept,
    }
