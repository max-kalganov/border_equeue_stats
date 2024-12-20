import logging
logger = logging.getLogger("app_main")

import os
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters
from border_equeue_stats.telegram_bot.stats_interface import plot_queue


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"user_id={update.effective_user.id}, chat_id={update.effective_chat.id}")
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


def app_main():
    bot_token = os.environ.get('BOT_TOKEN', None)

    application = ApplicationBuilder().token(bot_token).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters=filters.TEXT & (~filters.COMMAND), callback=plot_queue))

    application.run_polling()
