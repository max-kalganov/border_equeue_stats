import logging
from border_equeue_stats.telegram_bot.app_logging import set_root_logger
set_root_logger()
logger = logging.getLogger("app_main")

import os
from telegram.ext import ApplicationBuilder


def app_main():
    bot_token = os.environ.get('BOT_TOKEN', None)

    application = ApplicationBuilder().token(bot_token).build()

    # application.add_handler(CommandHandler('start', start))
    # application.add_handler(MessageHandler(filters=filters.TEXT & (~filters.COMMAND), callback=echo))

    application.run_polling()
