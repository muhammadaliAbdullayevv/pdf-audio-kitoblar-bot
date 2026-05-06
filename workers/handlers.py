from __future__ import annotations

from typing import Any


SUPPORTED_JOB_TYPES = {
    "pdf_maker",
    "pdf_editor",
    "tts_generate",
    "audio_convert",
    "sticker_convert",
    "book_summary",
    "video_download",
    "SEARCH_INDEX_BOOK",
    "SEARCH_REINDEX_ALL",
}

TODO_JOB_TYPES = {
    "BOOK_DOWNLOAD",
    "BOOK_UPLOAD",
    "BOOK_DELIVERY_REFRESH",
    "AUDIO_DOWNLOAD",
    "AUDIO_UPLOAD",
    "PDF_CREATE",
    "PDF_MERGE",
    "PDF_SPLIT",
    "PDF_COMPRESS",
    "PDF_EDIT",
    "TEXT_TO_VOICE",
    "AUDIO_TRIM",
    "STICKER_CONVERT",
    "ADMIN_DUPLICATE_SCAN",
    "ADMIN_MISSING_FILE_SCAN",
    "CLEANUP_TEMP_FILES",
}


async def process_background_job(job_type: str, payload: dict[str, Any], app) -> dict | None:
    import bot as bot_runtime

    if job_type == "pdf_maker":
        return await bot_runtime._process_pdf_maker_job(payload)
    if job_type == "pdf_editor":
        return await bot_runtime._process_pdf_editor_job(payload)
    if job_type == "tts_generate":
        return await bot_runtime._process_tts_job(payload)
    if job_type == "audio_convert":
        return await bot_runtime._process_audio_convert_job(payload, app)
    if job_type == "sticker_convert":
        return await bot_runtime._process_sticker_convert_job(payload, app)
    if job_type == "book_summary":
        return await bot_runtime._process_book_summary_job(payload, app)
    if job_type == "video_download":
        return await bot_runtime._process_video_download_job(payload, app)
    if job_type == "SEARCH_INDEX_BOOK":
        return await bot_runtime._process_search_index_book_job(payload)
    if job_type == "SEARCH_REINDEX_ALL":
        return await bot_runtime._process_search_reindex_all_job(payload)
    raise RuntimeError(f"Unknown job type: {job_type}")
