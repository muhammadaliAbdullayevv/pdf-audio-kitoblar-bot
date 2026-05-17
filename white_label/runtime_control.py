from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, MutableMapping

from .commands import format_connected_bot_reference

_REGISTRY_KEY = "white_label_runtime_processes"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _runtime_registry(bot_data: MutableMapping[str, Any]) -> dict[str, dict[str, Any]]:
    registry = bot_data.setdefault(_REGISTRY_KEY, {})
    if not isinstance(registry, dict):
        registry = {}
        bot_data[_REGISTRY_KEY] = registry
    return registry


def _runtime_python() -> str:
    return str(os.getenv("WHITE_LABEL_RUNTIME_PYTHON") or sys.executable or "python3").strip()


def _stop_timeout() -> float:
    try:
        return max(2.0, float(os.getenv("WHITE_LABEL_RUNTIME_STOP_TIMEOUT", "10") or "10"))
    except Exception:
        return 10.0


def _start_grace_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("WHITE_LABEL_RUNTIME_START_GRACE_SECONDS", "0.5") or "0.5"))
    except Exception:
        return 0.5


def _lock_path(connected_bot_id: str) -> str:
    template = os.getenv("WHITE_LABEL_LOCK_FILE_TEMPLATE", "/tmp/pdf_audio_kitoblar_connected_bot.{id}.lock")
    return str(template).format(id=str(connected_bot_id or "").strip())


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _pid_cmdline(pid: int) -> str:
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
        return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _pid_matches_connected_runtime(pid: int, connected_bot_id: str) -> bool:
    cmdline = _pid_cmdline(pid)
    return (
        bool(cmdline)
        and "white_label.connected_bot_runtime" in cmdline
        and str(connected_bot_id or "").strip() in cmdline
    )


def _external_runtime_pid(connected_bot_id: str) -> int | None:
    try:
        text = Path(_lock_path(connected_bot_id)).read_text(errors="ignore").strip()
        pid = int(text) if text else 0
    except Exception:
        return None
    if pid > 0 and _pid_is_alive(pid) and _pid_matches_connected_runtime(pid, connected_bot_id):
        return pid
    return None


async def _watch_process(registry: dict[str, dict[str, Any]], connected_bot_id: str, process: asyncio.subprocess.Process) -> None:
    returncode = await process.wait()
    record = registry.get(connected_bot_id)
    if record and record.get("process") is process:
        record["returncode"] = int(returncode)
        record["ended_at"] = time.time()


def _managed_record_status(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    process = record.get("process")
    if not isinstance(process, asyncio.subprocess.Process):
        return None
    returncode = process.returncode
    if returncode is None:
        return {
            "state": "RUNNING",
            "managed": True,
            "pid": int(process.pid or 0),
            "started_at": record.get("started_at"),
            "returncode": None,
        }
    return {
        "state": "STOPPED",
        "managed": True,
        "pid": int(process.pid or 0),
        "started_at": record.get("started_at"),
        "ended_at": record.get("ended_at"),
        "returncode": int(returncode),
    }


async def get_connected_bot_runtime_status(
    bot_data: MutableMapping[str, Any],
    connected_bot_id: str,
) -> dict[str, Any]:
    clean_id = str(connected_bot_id or "").strip()
    registry = _runtime_registry(bot_data)
    status = _managed_record_status(registry.get(clean_id))
    if status and status.get("state") == "RUNNING":
        return status
    external_pid = _external_runtime_pid(clean_id)
    if external_pid:
        return {"state": "RUNNING", "managed": False, "pid": int(external_pid), "returncode": None}
    if status:
        return status
    return {"state": "STOPPED", "managed": False, "pid": None, "returncode": None}


async def start_connected_bot_runtime(
    bot_data: MutableMapping[str, Any],
    connected_bot: dict[str, Any],
) -> dict[str, Any]:
    connected_bot_id = str((connected_bot or {}).get("id") or "").strip()
    if not connected_bot_id:
        return {"ok": False, "state": "ERROR", "error": "connected bot id is missing"}

    current = await get_connected_bot_runtime_status(bot_data, connected_bot_id)
    if current.get("state") == "RUNNING":
        current["ok"] = True
        current["already_running"] = True
        return current

    command = [
        _runtime_python(),
        "-m",
        "white_label.connected_bot_runtime",
        "--connected-bot-id",
        connected_bot_id,
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(_PROJECT_ROOT),
        env=env,
    )
    registry = _runtime_registry(bot_data)
    record = {
        "process": process,
        "started_at": time.time(),
        "command": command,
        "returncode": None,
    }
    registry[connected_bot_id] = record
    record["watch_task"] = asyncio.create_task(_watch_process(registry, connected_bot_id, process))
    grace = _start_grace_seconds()
    if grace:
        await asyncio.sleep(grace)
        if process.returncode is not None:
            record["returncode"] = int(process.returncode)
            record["ended_at"] = time.time()
            return {
                "ok": False,
                "state": "STOPPED",
                "managed": True,
                "pid": int(process.pid or 0),
                "returncode": int(process.returncode),
                "error": f"connected bot runtime exited immediately with code {int(process.returncode)}",
            }
    return {
        "ok": True,
        "state": "RUNNING",
        "managed": True,
        "pid": int(process.pid or 0),
        "already_running": False,
        "command": command,
    }


async def _stop_process(process: asyncio.subprocess.Process, timeout: float) -> dict[str, Any]:
    if process.returncode is not None:
        return {"ok": True, "state": "STOPPED", "returncode": int(process.returncode)}
    process.terminate()
    try:
        returncode = await asyncio.wait_for(process.wait(), timeout=timeout)
        return {"ok": True, "state": "STOPPED", "returncode": int(returncode)}
    except asyncio.TimeoutError:
        process.kill()
        returncode = await process.wait()
        return {"ok": True, "state": "KILLED", "returncode": int(returncode)}


async def _stop_external_pid(pid: int, timeout: float) -> dict[str, Any]:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return {"ok": True, "state": "STOPPED", "pid": pid}
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_is_alive(pid):
            return {"ok": True, "state": "STOPPED", "pid": pid}
        await asyncio.sleep(0.25)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return {"ok": True, "state": "STOPPED", "pid": pid}
    return {"ok": True, "state": "KILLED", "pid": pid}


async def stop_connected_bot_runtime(
    bot_data: MutableMapping[str, Any],
    connected_bot_id: str,
) -> dict[str, Any]:
    clean_id = str(connected_bot_id or "").strip()
    registry = _runtime_registry(bot_data)
    record = registry.get(clean_id)
    process = record.get("process") if isinstance(record, dict) else None
    if isinstance(process, asyncio.subprocess.Process):
        result = await _stop_process(process, _stop_timeout())
        if isinstance(record, dict):
            record["returncode"] = result.get("returncode")
            record["ended_at"] = time.time()
        registry.pop(clean_id, None)
        result.update({"managed": True, "pid": int(process.pid or 0)})
        return result

    external_pid = _external_runtime_pid(clean_id)
    if external_pid:
        result = await _stop_external_pid(external_pid, _stop_timeout())
        result["managed"] = False
        return result

    return {"ok": True, "state": "STOPPED", "managed": False, "pid": None, "already_stopped": True}


def format_runtime_status(connected_bot: dict[str, Any] | None, status: dict[str, Any]) -> str:
    pid = status.get("pid") or "-"
    state = str(status.get("state") or "UNKNOWN")
    managed = "main bot" if status.get("managed") else "external/none"
    lines = [
        f"Bot: {format_connected_bot_reference(connected_bot)}",
        f"Runtime: {state}",
        f"Managed by: {managed}",
        f"PID: {pid}",
    ]
    if status.get("returncode") is not None:
        lines.append(f"Return code: {status.get('returncode')}")
    return "\n".join(lines)
