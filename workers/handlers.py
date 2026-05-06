from __future__ import annotations

from typing import Any


SUPPORTED_JOB_TYPES = {
    "book_summary",
    "SEARCH_INDEX_BOOK",
    "SEARCH_REINDEX_ALL",
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

    if job_type == "book_summary":
        return await bot_runtime._process_book_summary_job(payload, app)
    if job_type == "SEARCH_INDEX_BOOK":
        return await bot_runtime._process_search_index_book_job(payload)
    if job_type == "SEARCH_REINDEX_ALL":
        return await bot_runtime._process_search_reindex_all_job(payload)
    raise RuntimeError(f"Unknown job type: {job_type}")
