import time


async def handle_admin_menu_action(
    *,
    update,
    context,
    lang: str,
    action: str,
    user_id: int | None,
    messages: dict,
    is_admin_user_fn,
    main_menu_keyboard_fn,
    send_main_menu_fn,
    upload_command_fn,
    pause_bot_command_fn,
    resume_bot_command_fn,
    audit_command_fn,
    prune_command_fn,
    missing_command_fn,
    db_dupes_command_fn,
    es_dupes_command_fn,
    dupes_status_command_fn,
    cancel_task_command_fn,
) -> bool:
    if not update.message:
        return False

    def admin_only() -> bool:
        return bool(user_id and is_admin_user_fn(user_id))

    if action == "admin_panel":
        if not admin_only():
            await update.message.reply_text(messages[lang]["admin_only"])
            return True
        await send_main_menu_fn(update, context, lang, "admin")
        return True

    if action in {"admin_system", "admin_maintenance", "admin_duplicates", "admin_tasks"}:
        if not admin_only():
            await update.message.reply_text(messages[lang]["admin_only"])
            return True
        section_map = {
            "admin_system": "admin",
            "admin_maintenance": "admin_maintenance",
            "admin_duplicates": "admin_duplicates",
            "admin_tasks": "admin_tasks",
        }
        await send_main_menu_fn(update, context, lang, section_map[action])
        return True

    if action in {"admin_broadcast", "admin_user_search"}:
        if not admin_only():
            await update.message.reply_text(messages[lang]["admin_only"])
            return True
        prompt_text = (
            "📣 Broadcast: send the message to send to all users.\nCancel: cancel"
            if action == "admin_broadcast"
            else "👤 User search: send name, username, or user ID (full/partial).\nCancel: cancel"
        )
        context.user_data["admin_menu_prompt"] = {
            "type": "broadcast" if action == "admin_broadcast" else "user_search",
            "expires_at": time.time() + 300,
            "section": "admin",
        }
        await update.message.reply_text(prompt_text, reply_markup=main_menu_keyboard_fn(lang, "admin", user_id))
        context.user_data["main_menu_section"] = "admin"
        return True

    if action == "admin_upload":
        if not admin_only():
            await update.message.reply_text(messages[lang]["admin_only"])
            return True
        context.user_data["_skip_spam_check_once"] = True
        context.user_data["main_menu_section"] = "admin"
        await upload_command_fn(update, context)
        return True

    if action in {
        "admin_pause", "admin_resume", "admin_audit", "admin_prune", "admin_missing", "admin_missing_confirm",
        "admin_db_dupes", "admin_es_dupes", "admin_dupes_status", "admin_cancel_task",
    }:
        if not admin_only():
            await update.message.reply_text(messages[lang]["admin_only"])
            return True
        if action in {"admin_audit", "admin_db_dupes", "admin_es_dupes", "admin_dupes_status", "admin_cancel_task"}:
            context.user_data["_skip_spam_check_once"] = True
        if action == "admin_pause":
            await pause_bot_command_fn(update, context)
        elif action == "admin_resume":
            await resume_bot_command_fn(update, context)
        elif action == "admin_audit":
            await audit_command_fn(update, context)
        elif action == "admin_prune":
            await prune_command_fn(update, context)
        elif action == "admin_missing":
            prev_args = list(context.args or [])
            try:
                context.args = []
                await missing_command_fn(update, context)
            finally:
                context.args = prev_args
        elif action == "admin_missing_confirm":
            prev_args = list(context.args or [])
            try:
                context.args = ["confirm"]
                await missing_command_fn(update, context)
            finally:
                context.args = prev_args
        elif action == "admin_db_dupes":
            await db_dupes_command_fn(update, context)
        elif action == "admin_es_dupes":
            await es_dupes_command_fn(update, context)
        elif action == "admin_dupes_status":
            await dupes_status_command_fn(update, context)
        elif action == "admin_cancel_task":
            await cancel_task_command_fn(update, context)
        return True

    return False


async def handle_admin_menu_prompt_input(
    *,
    update,
    context,
    lang: str,
    messages: dict,
    is_admin_user_fn,
    main_menu_keyboard_fn,
    broadcast_fn,
    user_search_command_fn,
) -> bool:
    if not update.message or not update.effective_user:
        return False

    admin_menu_prompt = context.user_data.get("admin_menu_prompt")
    if not admin_menu_prompt or not is_admin_user_fn(update.effective_user.id):
        return False

    if time.time() > float(admin_menu_prompt.get("expires_at", 0) or 0):
        context.user_data.pop("admin_menu_prompt", None)
        return False

    admin_text = (update.message.text or "").strip()
    if admin_text.lower() in {"cancel", "stop"}:
        context.user_data.pop("admin_menu_prompt", None)
        await update.message.reply_text(
            messages.get(lang, messages["en"]).get("menu_flow_cancelled", "❌ Previous process was cancelled."),
            reply_markup=main_menu_keyboard_fn(lang, str(admin_menu_prompt.get("section") or "admin"), update.effective_user.id),
        )
        return True

    prompt_type = str(admin_menu_prompt.get("type") or "")
    if prompt_type == "broadcast":
        context.user_data.pop("admin_menu_prompt", None)
        prev_args = list(context.args or [])
        try:
            context.user_data["_skip_spam_check_once"] = True
            context.args = [admin_text]
            await broadcast_fn(update, context)
        finally:
            context.args = prev_args
        return True

    if prompt_type == "user_search":
        prev_args = list(context.args or [])
        try:
            context.user_data["_skip_spam_check_once"] = True
            context.args = [admin_text]
            await user_search_command_fn(update, context)
            admin_menu_prompt["expires_at"] = time.time() + 3600
            context.user_data["admin_menu_prompt"] = admin_menu_prompt
        finally:
            context.args = prev_args
        return True

    context.user_data.pop("admin_menu_prompt", None)
    return False
