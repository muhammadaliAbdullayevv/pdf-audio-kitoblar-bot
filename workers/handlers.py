from __future__ import annotations

from typing import Any


SUPPORTED_JOB_TYPES = {
    "SEARCH_INDEX_BOOK",
    "SEARCH_REINDEX_ALL",
}

LEGACY_DISABLED_JOB_TYPES = {
    "book_summary",
}

TODO_JOB_TYPES = {
    "BOOK_DOWNLOAD",
    "BOOK_UPLOAD",
    "BOOK_DELIVERY_REFRESH",
    "AUDIO_DOWNLOAD",
    "AUDIO_UPLOAD",
    "ADMIN_DUPLICATE_SCAN",
    "ADMIN_MISSING_FILE_SCAN",
    "CLEANUP_TEMP_FILES",
}


async def process_background_job(job_type: str, payload: dict[str, Any], app) -> dict | None:
    import bot as bot_runtime

    if job_type in LEGACY_DISABLED_JOB_TYPES:
        bot_runtime.logger.info("Ignoring legacy disabled background job type: %s", job_type)
        return {"suppress_send": True, "disabled": True}
    if job_type == "SEARCH_INDEX_BOOK":
        return await bot_runtime._process_search_index_book_job(payload)
    if job_type == "SEARCH_REINDEX_ALL":
        return await bot_runtime._process_search_reindex_all_job(payload)
    raise RuntimeError(f"Unknown job type: {job_type}")
