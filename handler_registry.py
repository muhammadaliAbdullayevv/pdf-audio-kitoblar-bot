from __future__ import annotations

from typing import Any, Mapping

from telegram import Update
from telegram.ext import (
    CallbackQueryHandler,
    ChosenInlineResultHandler,
    CommandHandler,
    InlineQueryHandler,
    MessageHandler,
    TypeHandler,
    filters,
)


REQUIRED_DEP_KEYS = (
    "paused_guard",
    "paused_callback_guard",
    "_touch_user_activity_message",
    "_touch_user_activity_callback",
    "start",
    "upload_command_wrapper",
    "settings_command",
    "handle_guest_message_update",
    "handle_channel_post",
    "handle_photo_message",
    "handle_abook_audio",
    "handle_video_media",
    "handle_file",
    "search_books",
    "language_command_handler",
    "handle_language_callback",
    "handle_page_callback",
    "handle_user_page_callback",
    "handle_top_page_callback",
    "handle_top_users_toggle_callback",
    "handle_favorite_callback",
    "handle_request_callback",
    "handle_request_status_callback",
    "handle_requests_page_callback",
    "handle_requests_view_callback",
    "handle_request_cancel_callback",
    "handle_audiobook_listen_callback",
    "handle_audiobook_play_all_callback",
    "handle_audiobook_page_callback",
    "handle_audiobook_part_play_callback",
    "handle_reaction_callback",
    "handle_summary_placeholder_callback",
    "handle_admin_panel_callback",
    "handle_background_task_callback",
    "handle_dupes_confirm_callback",
    "handle_user_select_callback",
    "handle_user_action_callback",
    "broadcast_command_wrapper",
    "admin_panel_command",
    "smoke_command_wrapper",
    "favorites_command",
    "random_command",
    "top_command",
    "top_users_command",
    "contact_admin_command",
    "help_command",
    "request_command",
    "requests_command",
    "chatid_command",
    "mystats_command",
    "myprofile_command",
    "my_comments_command",
    "my_chats_command",
    "comment_inbox_command",
    "inlinequery",
    "chosen_inline_result",
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
    "drafttime_command",
    "negalert_command",
    "seedbookstats_command",
    "forbidden_books_command",
    "wl_add_bot_command",
    "wl_set_cache_channel_command",
    "wl_list_bots_command",
    "wl_suspend_bot_command",
    "wl_activate_bot_command",
    "wl_start_bot_command",
    "wl_stop_bot_command",
    "wl_runtime_status_command",
    "wl_delete_bot_command",
    "wl_test_bot_command",
    "wl_test_cache_command",
    "botconnectreq_command",
    "connected_bots_command",
    "handle_white_label_public_request_callback",
    "handle_white_label_owner_menu_callback",
    "handle_white_label_request_callback",
    "handle_white_label_connected_bot_callback",
    "handle_upload_help_callback",
    "handle_upload_request_status_callback",
    "handle_delete_book_callback",
    "handle_audiobook_part_delete_callback",
    "handle_audiobook_delete_by_book_callback",
    "handle_audiobook_delete_callback",
    "handle_audiobook_add_callback",
    "handle_book_rename_callback",
    "handle_book_reaction_edit_callback",
    "handle_book_reaction_policy_callback",
    "handle_book_comments_callback",
    "handle_book_comment_thread_callback",
    "handle_book_comment_add_callback",
    "handle_book_comment_reply_callback",
    "handle_my_comments_page_callback",
    "handle_my_comment_view_callback",
    "handle_my_comment_edit_callback",
    "handle_my_comment_delete_callback",
    "handle_my_chats_page_callback",
    "handle_my_chat_view_callback",
    "handle_my_chat_delete_callback",
    "handle_comment_inbox_callback",
    "handle_comment_conversation_callback",
    "handle_comment_conversation_mute_callback",
    "handle_book_comment_relay_reply_callback",
    "handle_book_comment_identity_request_callback",
    "handle_book_comment_identity_resolve_callback",
    "handle_book_comment_report_callback",
    "handle_book_comment_user_ban_toggle_callback",
    "handle_book_comment_relay_block_callback",
    "handle_book_comment_relay_report_callback",
    "handle_book_comment_moderation_callback",
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
    app.add_handler(CommandHandler("settings", d["settings_command"]))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, d["handle_channel_post"]))
    app.add_handler(MessageHandler(filters.PHOTO, d["handle_photo_message"]))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO | (filters.Document.ALL & filters.Document.MimeType("audio/")), d["handle_abook_audio"]))
    app.add_handler(MessageHandler(filters.VIDEO | (filters.Document.ALL & filters.Document.MimeType("video/")), d["handle_video_media"]))
    app.add_handler(MessageHandler(filters.ANIMATION, d["handle_video_media"]))
    app.add_handler(MessageHandler(filters.Document.ALL, d["handle_file"]))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, d["search_books"]))
    app.add_handler(CommandHandler("language", d["language_command_handler"]))
    app.add_handler(CallbackQueryHandler(d["handle_language_callback"], pattern="^lang_"))
    app.add_handler(CallbackQueryHandler(d["handle_page_callback"], pattern="^page:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_page_callback"], pattern="^userpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_top_page_callback"], pattern="^top:"))
    app.add_handler(CallbackQueryHandler(d["handle_top_users_toggle_callback"], pattern="^topusers:"))
    app.add_handler(CallbackQueryHandler(d["handle_favorite_callback"], pattern="^fav:"))
    app.add_handler(CallbackQueryHandler(d["handle_request_callback"], pattern="^req:"))
    app.add_handler(CallbackQueryHandler(d["handle_request_status_callback"], pattern="^reqstatus:"))
    app.add_handler(CallbackQueryHandler(d["handle_requests_page_callback"], pattern="^reqpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_requests_view_callback"], pattern="^reqview:"))
    app.add_handler(CallbackQueryHandler(d["handle_request_cancel_callback"], pattern="^reqcancel:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_listen_callback"], pattern="^abook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_play_all_callback"], pattern="^abplayall:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_page_callback"], pattern="^abpage:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_part_play_callback"], pattern="^abplay:"))
    app.add_handler(CallbackQueryHandler(d["handle_reaction_callback"], pattern="^react:"))
    app.add_handler(CallbackQueryHandler(d["handle_summary_placeholder_callback"], pattern="^summary:"))
    app.add_handler(CallbackQueryHandler(d["handle_admin_panel_callback"], pattern="^adminp:"))
    app.add_handler(CallbackQueryHandler(d["handle_background_task_callback"], pattern="^bgtask:"))
    app.add_handler(CallbackQueryHandler(d["handle_dupes_confirm_callback"], pattern="^dupesop:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_select_callback"], pattern="^user:"))
    app.add_handler(CallbackQueryHandler(d["handle_user_action_callback"], pattern="^uact:"))
    app.add_handler(CommandHandler("broadcast", d["broadcast_command_wrapper"]))
    app.add_handler(CommandHandler("admin", d["admin_panel_command"]))
    app.add_handler(CommandHandler("smoke", d["smoke_command_wrapper"]))
    app.add_handler(CommandHandler("favorite", d["favorites_command"]))
    app.add_handler(CommandHandler("random", d["random_command"]))
    app.add_handler(CommandHandler("top", d["top_command"]))
    app.add_handler(CommandHandler("top_users", d["top_users_command"]))
    app.add_handler(CommandHandler("contact_admin", d["contact_admin_command"]))
    app.add_handler(CommandHandler("help", d["help_command"]))
    app.add_handler(CommandHandler("request", d["request_command"]))
    app.add_handler(CommandHandler("requests", d["requests_command"]))
    app.add_handler(CommandHandler("chatid", d["chatid_command"]))
    app.add_handler(CommandHandler("mystats", d["mystats_command"]))
    app.add_handler(CommandHandler("myprofile", d["myprofile_command"]))
    app.add_handler(CommandHandler("my_comments", d["my_comments_command"]))
    app.add_handler(CommandHandler("mycomments", d["my_comments_command"]))
    app.add_handler(CommandHandler("my_commands", d["my_comments_command"]))
    app.add_handler(CommandHandler("my_chats", d["my_chats_command"]))
    app.add_handler(CommandHandler("mychats", d["my_chats_command"]))
    app.add_handler(CommandHandler("comment_inbox", d["comment_inbox_command"]))
    app.add_handler(CommandHandler("commentinbox", d["comment_inbox_command"]))
    app.add_handler(InlineQueryHandler(d["inlinequery"]))
    app.add_handler(ChosenInlineResultHandler(d["chosen_inline_result"]))
    app.add_handler(CommandHandler("audit", d["audit_command"]))
    app.add_handler(CommandHandler("drafttime", d["drafttime_command"]))
    app.add_handler(CommandHandler("negalert", d["negalert_command"]))
    app.add_handler(CommandHandler("seedbookstats", d["seedbookstats_command"]))
    app.add_handler(CommandHandler("randbookstats", d["seedbookstats_command"]))
    app.add_handler(CommandHandler("forbidden_books", d["forbidden_books_command"]))
    app.add_handler(CommandHandler("forbiddenbooks", d["forbidden_books_command"]))
    app.add_handler(CommandHandler("wl_add_bot", d["wl_add_bot_command"]))
    app.add_handler(CommandHandler("wl_set_cache_channel", d["wl_set_cache_channel_command"]))
    app.add_handler(CommandHandler("wl_list_bots", d["wl_list_bots_command"]))
    app.add_handler(CommandHandler("wl_suspend_bot", d["wl_suspend_bot_command"]))
    app.add_handler(CommandHandler("wl_activate_bot", d["wl_activate_bot_command"]))
    app.add_handler(CommandHandler("wl_start_bot", d["wl_start_bot_command"]))
    app.add_handler(CommandHandler("wl_stop_bot", d["wl_stop_bot_command"]))
    app.add_handler(CommandHandler("wl_runtime_status", d["wl_runtime_status_command"]))
    app.add_handler(CommandHandler("wl_delete_bot", d["wl_delete_bot_command"]))
    app.add_handler(CommandHandler("wl_test_bot", d["wl_test_bot_command"]))
    app.add_handler(CommandHandler("wl_test_cache", d["wl_test_cache_command"]))
    app.add_handler(CommandHandler("botconnectreq", d["botconnectreq_command"]))
    app.add_handler(CommandHandler("connected_bots", d["connected_bots_command"]))
    app.add_handler(CommandHandler("prune", d["prune_command"]))
    app.add_handler(CommandHandler("missing", d["missing_command"]))
    app.add_handler(CommandHandler("db_dupes", d["db_dupes_command"]))
    app.add_handler(CommandHandler("es_dupes", d["es_dupes_command"]))
    app.add_handler(CommandHandler("dupes_status", d["dupes_status_command"]))
    app.add_handler(CommandHandler("cancel_task", d["cancel_task_command"]))
    app.add_handler(CommandHandler("user", d["user_search_command"]))
    app.add_handler(CommandHandler("pause_bot", d["pause_bot_command"]))
    app.add_handler(CommandHandler("resume_bot", d["resume_bot_command"]))
    app.add_handler(CallbackQueryHandler(d["handle_upload_help_callback"], pattern="^upload_help_"))
    app.add_handler(CallbackQueryHandler(d["handle_upload_request_status_callback"], pattern="^uploadreqstatus:"))
    app.add_handler(CallbackQueryHandler(d["handle_delete_book_callback"], pattern="^delbook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_part_delete_callback"], pattern=r"^apdel:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_delete_by_book_callback"], pattern=r"^abdelbook:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_delete_callback"], pattern=r"^abdel:"))
    app.add_handler(CallbackQueryHandler(d["handle_audiobook_add_callback"], pattern=r"^abadd:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_rename_callback"], pattern=r"^bookrename:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_reaction_edit_callback"], pattern=r"^bookreactedit:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_reaction_policy_callback"], pattern=r"^bookreactpolicy:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comments_callback"], pattern=r"^bookcomments:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_thread_callback"], pattern=r"^commentthread:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_add_callback"], pattern=r"^commentadd:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_reply_callback"], pattern=r"^commentreply:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_comments_page_callback"], pattern=r"^mycomments:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_comment_view_callback"], pattern=r"^mycommentview:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_comment_edit_callback"], pattern=r"^mycommentedit:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_comment_delete_callback"], pattern=r"^mycommentdelete:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_chats_page_callback"], pattern=r"^mychats:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_chat_view_callback"], pattern=r"^mychatview:"))
    app.add_handler(CallbackQueryHandler(d["handle_my_chat_delete_callback"], pattern=r"^mychatdelete:"))
    app.add_handler(CallbackQueryHandler(d["handle_comment_inbox_callback"], pattern=r"^commentinbox:"))
    app.add_handler(CallbackQueryHandler(d["handle_comment_conversation_callback"], pattern=r"^commentconv:"))
    app.add_handler(CallbackQueryHandler(d["handle_comment_conversation_mute_callback"], pattern=r"^commentmute:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_relay_reply_callback"], pattern=r"^commentrelayreply:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_identity_request_callback"], pattern=r"^commentwho:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_identity_resolve_callback"], pattern=r"^commentreveal:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_report_callback"], pattern=r"^commentreport:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_user_ban_toggle_callback"], pattern=r"^commentuserban:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_relay_block_callback"], pattern=r"^commentrelayblock:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_relay_report_callback"], pattern=r"^commentrelayreport:"))
    app.add_handler(CallbackQueryHandler(d["handle_book_comment_moderation_callback"], pattern=r"^commentmod:"))
    app.add_handler(CallbackQueryHandler(d["handle_white_label_public_request_callback"], pattern=r"^wlreq:(?:sendtoken|cancel)$"))
    app.add_handler(CallbackQueryHandler(d["handle_white_label_owner_menu_callback"], pattern=r"^wlmenu$"))
    app.add_handler(CallbackQueryHandler(d["handle_white_label_request_callback"], pattern=r"^wlreq(?:page|view|accept|reject):"))
    app.add_handler(CallbackQueryHandler(d["handle_white_label_connected_bot_callback"], pattern=r"^wlbot(?:page|view|start|stop|restart|test|suspend|resume|delete|deleteconfirm):"))
    app.add_handler(
        CallbackQueryHandler(
            d["handle_book_selection"],
            pattern=r"^(?:(book:)?[0-9a-fA-F-]{32,36}|gbook:[0-9a-fA-F-]{32,36}:[0-9a-f]{8,32})$",
        )
    )
    # PTB 20.x does not expose guest_message as a native update type.
    # Keep the raw Update catch-all in a very late group so it only handles
    # guest-mode traffic and does not swallow normal bot updates.
    app.add_handler(TypeHandler(Update, d["handle_guest_message_update"]), group=100)
    app.add_error_handler(d["handle_error"])
