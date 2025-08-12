import os
import logging

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from border_equeue_stats.telegram_bot.logging_utils import UserLogger
from border_equeue_stats.constants import MAIN_LOGGER_NAME
from border_equeue_stats.telegram_bot.periodic_tasks import set_dumper, unset_dumper

logger = logging.getLogger(MAIN_LOGGER_NAME)

CHOOSING, TYPING_REPLY, TYPING_CHOICE = range(3)

reply_keyboard = [
    ["Age", "Favourite colour"],
    ["Number of siblings", "Something else..."],
    ["Done"],
]
markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True)


def facts_to_str(user_data: dict[str, str]) -> str:
    """Helper function for formatting the gathered user info."""
    facts = [f"{key} - {value}" for key, value in user_data.items()]
    return "\n".join(facts).join(["\n", "\n"])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the conversation and ask user for input."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running start")

    await update.message.reply_text(
        "Hi! My name is Doctor Botter. I will hold a more complex conversation with you. "
        "Why don't you tell me something about yourself?",
        reply_markup=markup,
    )

    return CHOOSING


async def regular_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ask the user for info about the selected predefined choice."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running regular_choice")

    text = update.message.text
    context.user_data["choice"] = text
    await update.message.reply_text(f"Your {text.lower()}? Yes, I would love to hear about that!")

    return TYPING_REPLY


async def custom_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Ask the user for a description of a custom category."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running custom_choice")

    await update.message.reply_text(
        'Alright, please send me the category first, for example "Most impressive skill"'
    )

    return TYPING_CHOICE


async def received_information(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Store info provided by user and ask for the next category."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running received_information")

    user_data = context.user_data
    text = update.message.text
    category = user_data["choice"]
    user_data[category] = text
    del user_data["choice"]

    await update.message.reply_text(
        "Neat! Just so you know, this is what you already told me:"
        f"{facts_to_str(user_data)}You can tell me more, or change your opinion"
        " on something.",
        reply_markup=markup,
    )

    return CHOOSING


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Display the gathered info and end the conversation."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running done")

    user_data = context.user_data
    if "choice" in user_data:
        del user_data["choice"]

    await update.message.reply_text(
        f"I learned these facts about you: {facts_to_str(user_data)}Until next time!",
        reply_markup=ReplyKeyboardRemove(),
    )

    user_data.clear()
    return ConversationHandler.END


def app_main() -> None:
    """Run the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(os.environ.get('BOT_TOKEN', None)).build()

    # Add conversation handler with the states CHOOSING, TYPING_CHOICE and TYPING_REPLY
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CHOOSING: [
                MessageHandler(
                    filters.Regex("^(Age|Favourite colour|Number of siblings)$"), regular_choice
                ),
                MessageHandler(filters.Regex("^Something else...$"), custom_choice),
            ],
            TYPING_CHOICE: [
                # MessageHandler(
                #     filters.TEXT & ~(filters.COMMAND | filters.Regex("^Done$")), regular_choice
                # ),
                MessageHandler(
                    filters.Regex("^New option$"), regular_choice
                ),
                MessageHandler(
                    filters.Regex("^New option2$"), regular_choice
                ),
            ],
            TYPING_REPLY: [
                MessageHandler(
                    filters.TEXT & ~(filters.COMMAND | filters.Regex("^Done$")),
                    received_information,
                )
            ],
        },
        fallbacks=[MessageHandler(filters.Regex("^Done$"), done)],
    )

    application.add_handler(conv_handler)

    # # Start the queue processor as a background task
    application.add_handler(CommandHandler('set_dumper', set_dumper))
    application.add_handler(CommandHandler('unset_dumper', unset_dumper))
    # asyncio.create_task(start_queue_processor())

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)

