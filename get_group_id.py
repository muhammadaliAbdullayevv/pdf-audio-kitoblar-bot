from dotenv import load_dotenv

load_dotenv()

import logging

from telegram import Update
from telegram.ext import ApplicationBuilder, ChatMemberHandler, ContextTypes

from config import TOKEN, OWNER_ID

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


async def my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.my_chat_member:
        return

    chat = update.effective_chat
    new_status = update.my_chat_member.new_chat_member.status

    if not chat or chat.type not in ("group", "supergroup"):
        return

    if new_status == "administrator":
        title = chat.title or "Unknown"
        msg = f"✅ Bot is admin in group: {title}\nID: {chat.id}"
        # Send to the group
        await context.bot.send_message(chat_id=chat.id, text=msg)
        # Also send to owner if configured
        if OWNER_ID:
            await context.bot.send_message(chat_id=OWNER_ID, text=msg)
        logger.info(msg)


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing in .env")
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(ChatMemberHandler(my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    logger.info("Listening for admin promotions. Add/promote the bot in a group...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
