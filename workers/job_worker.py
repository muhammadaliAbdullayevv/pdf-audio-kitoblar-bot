from __future__ import annotations

import asyncio
import signal

from telegram.ext import ApplicationBuilder

import bot as bot_runtime


async def main() -> None:
    bot_runtime.init_db()

    builder = (
        ApplicationBuilder()
        .token(bot_runtime.TOKEN)
        .connect_timeout(20)
        .read_timeout(60)
        .write_timeout(1200)
        .pool_timeout(bot_runtime.BOT_POOL_TIMEOUT)
        .connection_pool_size(bot_runtime.BOT_CONNECTION_POOL_SIZE)
        .concurrent_updates(bot_runtime.BOT_CONCURRENT_UPDATES)
    )

    bot_api_base_url = bot_runtime._normalize_bot_api_base_url(
        bot_runtime.os.getenv("TELEGRAM_BOT_API_BASE_URL", "")
    )
    bot_api_base_file_url = bot_runtime._normalize_bot_api_base_file_url(
        bot_runtime.os.getenv("TELEGRAM_BOT_API_BASE_FILE_URL", ""),
        bot_api_base_url,
    )
    bot_api_local_mode = bot_runtime._env_bool("TELEGRAM_BOT_API_LOCAL_MODE", False)

    if bot_api_base_url:
        builder = builder.base_url(bot_api_base_url)
    if bot_api_base_file_url:
        builder = builder.base_file_url(bot_api_base_file_url)
    if bot_api_local_mode:
        builder = builder.local_mode(True)

    app = builder.build()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    await app.initialize()
    await app.start()
    try:
        recovered = await bot_runtime.run_blocking(
            bot_runtime.db_recover_stale_background_jobs,
            bot_runtime.JOB_LOCK_TIMEOUT_MINUTES,
        )
        if recovered and (recovered.get("recovered") or recovered.get("failed")):
            bot_runtime.logger.info("Standalone job worker recovered stale jobs: %s", recovered)
        bot_runtime.start_background_job_workers(app)
        while not stop_event.is_set():
            bot_runtime.start_background_job_workers(app)
            await asyncio.sleep(30)
    finally:
        workers = list(app.bot_data.get(bot_runtime._BACKGROUND_JOB_WORKERS_KEY) or [])
        for task in workers:
            try:
                task.cancel()
            except Exception:
                pass
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
