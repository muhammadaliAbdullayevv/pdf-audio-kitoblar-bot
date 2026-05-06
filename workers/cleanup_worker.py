from __future__ import annotations

import asyncio
import signal

import bot as bot_runtime
from jobs import prune_job_dirs


async def main() -> None:
    bot_runtime.init_db()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    while not stop_event.is_set():
        try:
            recovered = await bot_runtime.run_blocking(
                bot_runtime.db_recover_stale_background_jobs,
                bot_runtime.JOB_LOCK_TIMEOUT_MINUTES,
            )
            if recovered and (recovered.get("recovered") or recovered.get("failed")):
                bot_runtime.logger.info("Cleanup worker recovered stale jobs: %s", recovered)
        except Exception as e:
            bot_runtime.logger.error("Cleanup worker stale job recovery failed: %s", e, exc_info=True)

        try:
            cleaned = await asyncio.to_thread(
                prune_job_dirs,
                bot_runtime.TEMP_JOB_TTL_HOURS,
                bot_runtime.FAILED_JOB_TTL_HOURS,
            )
            if cleaned and (cleaned.get("deleted") or cleaned.get("failed_deleted")):
                bot_runtime.logger.info("Cleanup worker removed job temp dirs: %s", cleaned)
        except Exception as e:
            bot_runtime.logger.error("Cleanup worker temp dir prune failed: %s", e, exc_info=True)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
