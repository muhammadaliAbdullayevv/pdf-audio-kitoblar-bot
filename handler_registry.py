from __future__ import annotations

from typing import Any, Mapping

from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    InlineQueryHandler,
    MessageHandler,
    PollAnswerHandler,
    filters,
)


REQUIRED_DEP_KEYS = (
    "paused_guard",
    "paused_callback_guard",
    "_touch_user_activity_message",
    "_touch_user_activity_callback",
    "start",
    "upload_command_wrapper",
    "handle_channel_post",
    "handle_photo_message",
    "handle_abook_audio",
    "handle_movie_video",
    "handle_file",
    "search_books",
    "language_command_handler",
    "pdf_maker_command",
    "pdf_editor_command",
    "text_to_voice_command",
    "sticker_tools_command",
    "handle_pdf_maker_callback",
    "handle_pdf_editor_callback",
    "handle_tts_callback",
    "handle_video_downloader_callback",
    "handle_audio_converter_callback",
    "handle_sticker_tools_callback",
    "handle_ai_tools_callback",
    "handle_my_quiz_callback",
    "handle_ai_quiz_poll_answer",
    "handle_language_callback",
    "handle_page_callback",
    "handle_movie_page_callback",
    "handle_user_page_callback",
    "handle_top_page_callback",
    "handle_top_users_toggle_callback",
    "handle_favorite_callback",
    "handle_audiobook_listen_callback",
    "handle_audiobook_page_callback",
    "handle_audiobook_part_play_callback",
    "handle_reaction_callback",
    "handle_movie_reaction_callback",
    "handle_movie_share_callback",
    "handle_summary_placeholder_callback",
    "handle_admin_panel_callback",
    "handle_background_task_callback",
    "handle_dupes_confirm_callback",
    "handle_user_select_callback",
    "handle_user_action_callback",
    "handle_request_callback",
    "broadcast_command_wrapper",
    "admin_panel_command",
    "smoke_command_wrapper",
    "favorites_command",
    "random_command",
    "top_command",
    "top_users_command",
    "help_command",
    "request_command",
    "requests_command_wrapper",
    "my_quiz_command",
    "mystats_command",
    "myprofile_command",
    "inlinequery",
    "audit_command",
    "prune_command",
    "missing_command",
    "db_dupes_command",
    "es_dupes_command",
    "dupes_status_command",
    "cancel_task_command",
    "user_search_command",
    "pause_bot_command",
    "resume_bot_command",
    "upload_local_books_command",
    "handle_request_status_callback",
    "handle_requests_page_callback",
    "handle_requests_view_callback",
    "handle_request_cancel_callback",
    "handle_upload_help_callback",
    "handle_upload_request_status_callback",
    "handle_delete_book_callback",
    "handle_audiobook_part_delete_callback",
    "handle_audiobook_delete_by_book_callback",
    "handle_audiobook_delete_callback",
    "handle_audiobook_add_callback",
    "handle_movie_selection",
    "handle_book_selection",
    "handle_error",
)


def _resolve_deps(deps: Mapping[str, Any]) -> dict[str, Any]:
    missing = [key for key in REQUIRED_DEP_KEYS if key not in deps]
    if missing:
        raise RuntimeError(f"handler_registry missing dependencies: {', '.join(missing)}")
    return {key: deps[key] for key in REQUIRED_DEP_KEYS}


def register_handlers(app, deps: Mapping[str, Any]) -> None:
    d = _resolve_deps(deps)
    app.add_handler(MessageHandler(filters.ALL, d["paused_guard"]), group=-1)
    app.add_handler(CallbackQueryHandler(d["paused_callback_guard"]), group=-1)
    app.add_handler(MessageHandler(filters.ALL, d["_touch_user_activity_message"]), group=-1)
    app.add_handler(CallbackQueryHandler(d["_touch_user_activity_callback"]), group=-1)
    app.add_handler(CommandHandler("start", d["start"]))
    app.add_handler(CommandHandler("upload", d["upload_command_wrapper"]))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, d["handle_channel_post"]))
    app.add_handler(MessageHandler(filters.PHOTO, d["handle_photo_message"]))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO | (filters.Document.ALL & filters.Document.MimeType("audio/")), d["handle_abook_audio"]))
    app.add_handler(MessageHandler(filters.VIDEO | (filters.Document.ALL & filters.Document.MimeType("video/")), d["handle_movie_video"]))
    app.add_handler(MessageHandler(filters.ANIMATION, d["handle_movie_video"]))
    app.add_handler(MessageHandler(filters.Document.ALL, d["handle_file"]))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, d["search_books"]))
    app.add_handler(CommandHandler("language", d["language_command_handler"]))
    app.add_handler(CommandHandler("pdf_maker", d["pdf_maker_command"]))
    app.add_handler(CommandHandler("pdf_editor", d["pdf_editor_command"]))
    app.add_handler(CommandHandler("text_to_voice", d["text_to_voice_command"]))
    app.add_handler(CommandHandler("sticker_tools", d["sticker_tools_command"]))
    app.add_handler(CallbackQueryHandler(d["handle_pdf_maker_callback"], pattern="^pdfmk:"))
    app.add_handler(CallbackQueryHandler(d["handle_pdf_editor_callback"], pattern="^pdfed:"))
    app.add_handler(CallbackQueryHandler(d["handle_tts_callback"], pattern="^tts:"))
    app.add_handler(CallbackQueryHandler(d["handle_video_downloader_callback"], pattern="^vdl:"))
    app.add_handler(CallbackQueryHandler(d["handle_audio_converter_callback"], pattern="^atool:"))
    app.add_handler(CallbackQueryHandler(d["handle_sticker_tools_callback"], pattern="^stkr:"))
    app.add_handler(CallbackQueryHandler(d["handle_ai_tools_callback"], pattern="^aitool:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_quiz_callback"], pattern="^myquiz:"))
    app.add_handler(PollAnswerHandler(d["handle_ai_quiz_poll_answer"]))
    app.add_handler(CallbackQueryHandler(d["handle_language_callback"], pattern="^lang_"))
    app.add_handler(CallbackQueryHandler(d["handle_page_callback"], pattern="^page:"))
    app.add_handler(CallbackQueryHandler(d["handle_movie_page_callback"], pattern="^mpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_page_callback"], pattern="^userpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_top_page_callback"], pattern="^top:"))
    app.add_handler(CallbackQueryHandler(d["handle_top_users_toggle_callback"], pattern="^topusers:"))
    app.add_handler(CallbackQueryHandler(d["handle_favorite_callback"], pattern="^fav:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_listen_callback"], pattern="^abook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_page_callback"], pattern="^abpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_part_play_callback"], pattern="^abplay:"))
    app.add_handler(CallbackQueryHandler(d["handle_reaction_callback"], pattern="^react:"))
    app.add_handler(CallbackQueryHandler(d["handle_movie_reaction_callback"], pattern="^mreact:"))
    app.add_handler(CallbackQueryHandler(d["handle_movie_share_callback"], pattern="^mshare:"))
    app.add_handler(CallbackQueryHandler(d["handle_summary_placeholder_callback"], pattern="^summary:"))
    app.add_handler(CallbackQueryHandler(d["handle_admin_panel_callback"], pattern="^adminp:"))
    app.add_handler(CallbackQueryHandler(d["handle_background_task_callback"], pattern="^bgtask:"))
    app.add_handler(CallbackQueryHandler(d["handle_dupes_confirm_callback"], pattern="^dupesop:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_select_callback"], pattern="^user:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_action_callback"], pattern="^uact:"))
    app.add_handler(CallbackQueryHandler(d["handle_request_callback"], pattern="^request:"))
    app.add_handler(CommandHandler("broadcast", d["broadcast_command_wrapper"]))
    app.add_handler(CommandHandler("admin", d["admin_panel_command"]))
    app.add_handler(CommandHandler("smoke", d["smoke_command_wrapper"]))
    app.add_handler(CommandHandler("favorite", d["favorites_command"]))
    app.add_handler(CommandHandler("random", d["random_command"]))
    app.add_handler(CommandHandler("top", d["top_command"]))
    app.add_handler(CommandHandler("top_users", d["top_users_command"]))
    app.add_handler(CommandHandler("help", d["help_command"]))
    app.add_handler(CommandHandler("request", d["request_command"]))
    app.add_handler(CommandHandler("requests", d["requests_command_wrapper"]))
    app.add_handler(CommandHandler("my_quiz", d["my_quiz_command"]))
    app.add_handler(CommandHandler("mystats", d["mystats_command"]))
    app.add_handler(CommandHandler("myprofile", d["myprofile_command"]))
    app.add_handler(InlineQueryHandler(d["inlinequery"]))
    app.add_handler(CommandHandler("audit", d["audit_command"]))
    app.add_handler(CommandHandler("prune", d["prune_command"]))
    app.add_handler(CommandHandler("missing", d["missing_command"]))
    app.add_handler(CommandHandler("db_dupes", d["db_dupes_command"]))
    app.add_handler(CommandHandler("es_dupes", d["es_dupes_command"]))
    app.add_handler(CommandHandler("dupes_status", d["dupes_status_command"]))
    app.add_handler(CommandHandler("cancel_task", d["cancel_task_command"]))
    app.add_handler(CommandHandler("user", d["user_search_command"]))
    app.add_handler(CommandHandler("pause_bot", d["pause_bot_command"]))
    app.add_handler(CommandHandler("resume_bot", d["resume_bot_command"]))
    app.add_handler(CommandHandler("upload_local_books", d["upload_local_books_command"]))
    app.add_handler(CallbackQueryHandler(d["handle_request_status_callback"], pattern="^reqstatus:"))
    app.add_handler(CallbackQueryHandler(d["handle_requests_page_callback"], pattern="^reqpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_requests_view_callback"], pattern="^reqview:"))
    app.add_handler(CallbackQueryHandler(d["handle_request_cancel_callback"], pattern="^reqcancel:"))
    app.add_handler(CallbackQueryHandler(d["handle_upload_help_callback"], pattern="^upload_help_"))
    app.add_handler(CallbackQueryHandler(d["handle_upload_request_status_callback"], pattern="^uploadreqstatus:"))
    app.add_handler(CallbackQueryHandler(d["handle_delete_book_callback"], pattern="^delbook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_part_delete_callback"], pattern=r"^apdel:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_delete_by_book_callback"], pattern=r"^abdelbook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_delete_callback"], pattern=r"^abdel:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_add_callback"], pattern=r"^abadd:"))
    app.add_handler(CallbackQueryHandler(d["handle_movie_selection"], pattern=r"^movie:[0-9a-fA-F-]{32,36}$"))
    app.add_handler(CallbackQueryHandler(d["handle_book_selection"], pattern=r"^(book:)?[0-9a-fA-F-]{32,36}$"))
    app.add_error_handler(d["handle_error"])
