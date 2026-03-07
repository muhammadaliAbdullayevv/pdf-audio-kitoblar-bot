import logging
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SafeCompleted:
    returncode: int
    stdout: str
    stderr: str


def _trim(s: str, max_chars: int) -> str:
    s = s or ""
    if max_chars <= 0:
        return ""
    if len(s) <= max_chars:
        return s
    half = max_chars // 2
    return f"{s[:half]}\n…(truncated)…\n{s[-half:]}"


def _to_text(x) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            return bytes(x).decode("utf-8", errors="replace")
        except Exception:
            return ""
    return str(x)


def run(
    cmd: list[str],
    *,
    timeout_s: float = 60.0,
    max_output_chars: int = 20000,
    text: bool = True,
    check: bool = False,
    cwd: str | None = None,
    env: dict | None = None,
    stdin_text: str | None = None,
) -> SafeCompleted:
    """
    Run a subprocess safely:
    - no shell
    - bounded output
    - timeout
    """
    if not isinstance(cmd, list) or not all(isinstance(x, str) and x for x in cmd):
        raise ValueError("cmd must be list[str] with non-empty items")
    try:
        p = subprocess.run(
            cmd,
            input=stdin_text,
            capture_output=True,
            text=text,
            timeout=timeout_s,
            cwd=cwd,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        stdout = _to_text(getattr(e, "stdout", None))
        stderr = _to_text(getattr(e, "stderr", None))
        raise RuntimeError(
            f"command-timeout after {timeout_s}s: {cmd[0]}"
            + (f"\nstdout:\n{_trim(stdout, 2000)}" if stdout else "")
            + (f"\nstderr:\n{_trim(stderr, 2000)}" if stderr else "")
        ) from None

    out = _to_text(p.stdout)
    err = _to_text(p.stderr)
    completed = SafeCompleted(
        returncode=int(p.returncode),
        stdout=_trim(out, max_output_chars),
        stderr=_trim(err, max_output_chars),
    )
    if check and completed.returncode != 0:
        raise RuntimeError(f"command-failed rc={completed.returncode}: {cmd[0]}\n{completed.stderr or completed.stdout}")
    return completed

