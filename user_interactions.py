from __future__ import annotations

import logging
from typing import Any

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)


def configure(deps: dict[str, Any]) -> None:
    for k, v in deps.items():
        if k.startswith('__') and k.endswith('__'):
            continue
        globals()[k] = v


async def handle_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    req_id = data.split(":", 1)[1] if ":" in data else ""
    req = context.user_data.get("requests", {}).pop(req_id, None)
    if not req:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    await send_request_to_admin(context, update.effective_user, req.get("query", ""), lang)
    await safe_answer(query, MESSAGES[lang]["request_sent"])
    try:
        await query.message.reply_text(MESSAGES[lang]["request_sent"])
    except Exception:
        pass


async def handle_request_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    try:
        _, status, req_id = data.split(":", 2)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    if not _is_admin_user(user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    record = await run_blocking(get_request_by_id, req_id)
    if not record:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    current = record.get("status", "open")
    # Enforce order: open -> seen -> done/no
    if status in {"done", "no"} and current != "seen":
        await safe_answer(query, MESSAGES[lang]["request_status_order"], show_alert=True)
        return
    if status == "seen" and current != "open":
        await safe_answer(query, MESSAGES[lang]["request_status_already"], show_alert=True)
        return
    if current in {"done", "no"}:
        await safe_answer(query, MESSAGES[lang]["request_status_already"], show_alert=True)
        return

    # If seen: update immediately + auto notify user
    if status == "seen":
        record = await run_blocking(update_request_status, req_id, status, user, None)
        if not record:
            await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
            return
        req_lang = record.get("language", "en")
        msg = MESSAGES[req_lang]["request_reply_seen_auto"].format(query=record.get("query"))
        try:
            await context.bot.send_message(chat_id=record["user_id"], text=msg)
        except Exception:
            pass
        # Update admin message
        try:
            keyboard = build_request_admin_keyboard(record.get("status", "open"), record.get("id"))
            await context.bot.edit_message_text(
                chat_id=query.message.chat_id,
                message_id=query.message.message_id,
                text=format_request_admin_text(record),
                reply_markup=keyboard
            )
        except Exception:
            pass
        await safe_answer(query, MESSAGES[lang]["request_status_updated_admin"].format(status=status))
        return

    # done / no -> ask admin for message
    context.user_data["pending_request_reply"] = {
        "request_id": req_id,
        "status": status,
        "admin_chat_id": query.message.chat_id,
        "admin_message_id": query.message.message_id,
        "expires_at": time.time() + 300
    }
    status_label = MESSAGES[lang].get(f"request_status_{status}", status)
    await safe_answer(query)
    await query.message.reply_text(
        MESSAGES[lang]["request_admin_prompt"].format(status=status_label)
    )


async def handle_requests_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    try:
        _, user_id_str, page_str = data.split(":", 2)
        user_id = int(user_id_str)
        page = int(page_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    if query.from_user.id != user_id:
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    requests = await run_blocking(db_list_requests_for_user, user_id)
    if not requests:
        await safe_answer(query, MESSAGES[lang]["requests_empty"], show_alert=True)
        return

    requests.sort(key=lambda r: r.get("created_ts") or 0, reverse=True)
    total = len(requests)
    pages = max(1, int(math.ceil(total / REQUESTS_PAGE_SIZE)))
    page = max(0, min(page, pages - 1))
    start = page * REQUESTS_PAGE_SIZE
    end = start + REQUESTS_PAGE_SIZE
    page_items = requests[start:end]

    def status_label(status: str) -> str:
        return MESSAGES[lang].get(f"request_status_{status}", status)

    lines = [
        f"{i + 1}. {item.get('query')} — {status_label(item.get('status', 'open'))}"
        for i, item in enumerate(page_items)
    ]
    text = MESSAGES[lang]["requests_title"].format(page=page + 1, pages=pages, total=total) + "\n\n" + "\n".join(lines)
    reply_markup = build_requests_keyboard(page_items, user_id, page, pages)
    context.user_data["requests_page"] = page
    context.user_data["requests_list_message_id"] = query.message.message_id
    context.user_data["requests_list_chat_id"] = query.message.chat_id
    await query.edit_message_text(text, reply_markup=reply_markup)
    await safe_answer(query)


async def handle_requests_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    try:
        _, user_id_str, req_id = data.split(":", 2)
        user_id = int(user_id_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    if query.from_user.id != user_id:
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    record = await run_blocking(db_get_request_by_id, req_id)
    if record and record.get("user_id") != user_id:
        record = None
    if not record:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    status_label = MESSAGES[lang].get(f"request_status_{record.get('status','open')}", record.get("status"))
    text = MESSAGES[lang]["request_detail"].format(
        query=record.get("query"),
        status=status_label,
        created=record.get("created_at"),
        updated=record.get("updated_at") or "-"
    )
    cancel_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(MESSAGES[lang]["request_cancel"], callback_data=f"reqcancel:{user_id}:{req_id}")]
    ])
    await context.bot.send_message(chat_id=user_id, text=text, reply_markup=cancel_kb)
    await safe_answer(query)


async def handle_request_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    try:
        _, user_id_str, req_id = data.split(":", 2)
        user_id = int(user_id_str)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    if query.from_user.id != user_id:
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    record = await run_blocking(db_get_request_by_id, req_id)
    if record and record.get("user_id") != user_id:
        record = None
    if not record:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    # Remove from DB
    await run_blocking(db_delete_request, req_id)
    await run_blocking(db_increment_counter, "request_cancelled", 1)

    # Notify admin group only if still open (not seen/done/no)
    if record.get("status") == "open":
        admin_chat_id = record.get("admin_chat_id")
        admin_message_id = record.get("admin_message_id")
        notify_text = MESSAGES["en"]["request_cancelled_admin"].format(
            user_id=user_id, query=record.get("query")
        )
        if admin_chat_id and admin_message_id:
            try:
                await context.bot.send_message(
                    chat_id=admin_chat_id,
                    text=notify_text,
                    reply_to_message_id=admin_message_id
                )
            except Exception:
                pass
        elif admin_chat_id:
            await context.bot.send_message(chat_id=admin_chat_id, text=notify_text)

    await query.edit_message_text(MESSAGES[lang]["request_cancelled_user"])
    await refresh_requests_list(context, user_id, lang)
    await safe_answer(query)


async def handle_upload_request_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    data = query.data or ""
    try:
        _, status, req_id = data.split(":", 2)
    except Exception:
        await safe_answer(query, MESSAGES[lang]["error"], show_alert=True)
        return

    if not _is_admin_user(user.id):
        await safe_answer(query, MESSAGES[lang]["admin_only"], show_alert=True)
        return

    record = await run_blocking(get_upload_request_by_id, req_id)
    if not record:
        await safe_answer(query, MESSAGES[lang]["page_expired"], show_alert=True)
        return

    # Ask admin for message to user
    context.user_data["pending_upload_reply"] = {
        "request_id": req_id,
        "status": status,
        "admin_chat_id": query.message.chat_id,
        "admin_message_id": query.message.message_id,
        "expires_at": time.time() + 300
    }
    status_label = MESSAGES[lang].get(f"upload_status_{status}", status)
    await safe_answer(query)
    await query.message.reply_text(
        MESSAGES[lang]["upload_admin_prompt"].format(status=status_label)
    )


async def handle_upload_help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        if query:
            await safe_answer(query)
        return
    lang = ensure_user_language(update, context)
    limited, wait_s = spam_check_callback(update, context)
    if limited:
        await safe_answer(query, MESSAGES[lang]["spam_wait"].format(seconds=wait_s), show_alert=True)
        return
    if query.data in {"upload_help_yes", "upload_help_movie_yes"}:
        await safe_answer(query)
        await send_upload_request_to_admin(context, query.from_user, lang)
        await query.edit_message_text(MESSAGES[lang]["upload_help_sent"])
    elif query.data in {"upload_help_no", "upload_help_movie_no"}:
        await safe_answer(query)
        await query.edit_message_text(MESSAGES[lang]["upload_help_no_reply"])
    else:
        await safe_answer(query)


async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    user_id = update.effective_user.id
    favs = await run_blocking(db_list_favorites, user_id)

    if not favs:
        await target_message.reply_text(MESSAGES[lang]["favorites_empty"])
        return

    items = favs[:10]
    lines = [
        MESSAGES[lang]["favorites_item"].format(
            title=item.get("title", "")
        )
        for item in items
    ]
    text = MESSAGES[lang]["favorites_title"] + "\n\n" + "\n".join(lines)
    keyboard = build_simple_book_keyboard(items)
    msg = await target_message.reply_text(text, reply_markup=keyboard)
    context.user_data["requests_list_message_id"] = msg.message_id
    context.user_data["requests_list_chat_id"] = msg.chat_id


async def random_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    books = await run_blocking(db_get_random_books, 10, True)
    if not books:
        await target_message.reply_text(MESSAGES[lang]["book_not_found"])
        return

    entries: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for book in books:
        book_id = str(book.get("id") or "").strip()
        if not book_id or book_id in seen_ids:
            continue
        seen_ids.add(book_id)
        title = str(get_result_title(book) or "").strip() or "Untitled"
        entries.append({"id": book_id, "title": title})

    if not entries:
        await target_message.reply_text(MESSAGES[lang]["book_not_found"])
        return

    query_label = MESSAGES[lang].get("random_results_query", "🎲 Random books")
    query_id = cache_search_results(context, query_label, entries)
    result_text, page_entries, pages = build_results_text(query_label, entries, 0, lang)
    reply_markup = build_results_keyboard(page_entries, 0, pages, query_id)

    try:
        page_ids = [str(e.get("id") or "").strip() for e in page_entries if str(e.get("id") or "").strip()]
        if page_ids:
            await run_blocking(db_increment_book_searches, page_ids)
    except Exception as e:
        logger.warning("random_command search count increment failed: %s", e)

    await target_message.reply_text(result_text, reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    uid = update.effective_user.id if update.effective_user else None
    await target_message.reply_text(_build_help_text(lang, user_id=uid))


async def request_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if update.effective_user and is_blocked(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    if not context.args:
        context.user_data["awaiting_request"] = True
        context.user_data["awaiting_request_until"] = time.time() + 30
        await update.message.reply_text(MESSAGES[lang]["request_prompt"])
        return
    query_text = " ".join(context.args).strip()
    await send_request_to_admin(context, update.effective_user, query_text, lang)
    await update.message.reply_text(MESSAGES[lang]["request_sent"])


async def requests_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    if update.effective_user and is_blocked(update.effective_user.id):
        await update.message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await update.message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    user_id = update.effective_user.id

    requests = await run_blocking(db_list_requests_for_user, user_id)
    if not requests:
        await update.message.reply_text(MESSAGES[lang]["requests_empty"])
        return

    requests.sort(key=lambda r: r.get("created_ts") or 0, reverse=True)
    page = 0
    total = len(requests)
    pages = max(1, int(math.ceil(total / REQUESTS_PAGE_SIZE)))
    start = page * REQUESTS_PAGE_SIZE
    end = start + REQUESTS_PAGE_SIZE
    page_items = requests[start:end]

    def status_label(status: str) -> str:
        return MESSAGES[lang].get(f"request_status_{status}", status)

    lines = [
        f"{i + 1}. {item.get('query')} — {status_label(item.get('status', 'open'))}"
        for i, item in enumerate(page_items)
    ]
    text = MESSAGES[lang]["requests_title"].format(page=page + 1, pages=pages, total=total) + "\n\n" + "\n".join(lines)
    keyboard = build_requests_keyboard(page_items, user_id, page, pages)
    context.user_data["requests_page"] = page
    await update.message.reply_text(text, reply_markup=keyboard)


async def myprofile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = ensure_user_language(update, context)
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return
    if update.effective_user and is_blocked(update.effective_user.id):
        await target_message.reply_text(MESSAGES[lang]["blocked"])
        return
    if update.effective_user and await is_stopped_user(update.effective_user.id):
        return
    limited, wait_s = spam_check_message(update, context)
    if limited:
        await target_message.reply_text(MESSAGES[lang]["spam_wait"].format(seconds=wait_s))
        return
    await update_user_info(update, context)
    user_id = update.effective_user.id

    stats = await run_blocking(db_get_user_usage_stats, user_id)
    fav_count = await run_blocking(db_get_user_favorites_count, user_id)
    reaction_count = await run_blocking(db_get_user_reaction_count, user_id)
    fav_awards = await run_blocking(db_get_user_favorite_awards_count, user_id)
    reaction_awards = await run_blocking(db_get_user_reaction_awards_count, user_id)
    referral_count = await run_blocking(db_get_user_referrals_count, user_id)
    coin_bonus = await run_blocking(db_get_user_coin_adjustment, user_id)
    searches = stats.get("searches", 0)
    downloads = stats.get("downloads", 0)
    active_days = stats.get("active_days", 0)
    joined = stats.get("joined_date") or "—"
    avg_per_day = 0
    if active_days > 0:
        avg_per_day = round((searches + downloads) / active_days, 2)

    coins = compute_coin_breakdown(searches, downloads, reaction_awards, fav_awards, referral_count, coin_bonus)
    bonus_line = MESSAGES[lang]["coin_bonus_line"].format(bonus=coin_bonus)
    ref_link = await build_referral_link(context, user_id)
    ref_info = MESSAGES[lang]["myprofile_ref_info"].format(coins_per_referral=COIN_REFERRAL)
    share_text = MESSAGES[lang]["share_referral_text"].format(ref_link=ref_link)
    share_url = f"https://t.me/share/url?url={quote_plus(ref_link)}&text={quote_plus(share_text)}"
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(MESSAGES[lang]["share_referral_button"], url=share_url)]]
    )

    text = MESSAGES[lang]["myprofile_text"].format(
        searches=searches,
        downloads=downloads,
        favorites=fav_count,
        reactions=reaction_count,
        referrals=referral_count,
        active_days=active_days,
        avg_per_day=avg_per_day,
        joined=joined,
        coins_searches=coins["searches"],
        coins_downloads=coins["downloads"],
        coins_favorites=coins["favorites"],
        coins_reactions=coins["reactions"],
        coins_referrals=coins["referrals"],
        coins_total=coins["total"],
        bonus_line=bonus_line,
        ref_link=ref_link,
        ref_info=ref_info,
    )
    await target_message.reply_text(text, reply_markup=keyboard)


async def mystats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await myprofile_command(update, context)
