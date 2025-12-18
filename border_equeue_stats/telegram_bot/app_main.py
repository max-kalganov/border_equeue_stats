import os
import logging

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters, CallbackQueryHandler,
)

from border_equeue_stats.telegram_bot.logging_utils import UserLogger
from border_equeue_stats.constants import MAIN_LOGGER_NAME
from border_equeue_stats.telegram_bot.periodic_tasks import set_dumper, unset_dumper

logger = logging.getLogger(MAIN_LOGGER_NAME)

CHOOSING, TYPING_REPLY, TYPING_CHOICE = range(3)

reply_keyboard = [
    ["Age"], ["Favourite colour"], ["Option 1"], ["Option 2"], ["Option 3"], ["Option 4"], ["Option 5"], ["Option 6"],
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
        reply_markup=markup, #build_multi_select_keyboard(["Option1", "Option2", "Option3"]),
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

def build_multi_select_keyboard(options):
    keyboard = []
    for option in options:
        # Add checkmark if selected
        keyboard.append([InlineKeyboardButton(
            f"{option}",
            callback_data=f"toggle_{option}"
        )])
    keyboard.append([InlineKeyboardButton("Submit", callback_data="submit")])
    return InlineKeyboardMarkup(keyboard)


async def handle_selection(update, context):
    query = update.callback_query
    data = query.data

    if data.startswith("toggle_"):
        option = data.split("_")[1]
        # Toggle selection
        if option in context.user_data.get('selected_options', set()):
            context.user_data['selected_options'].remove(option)
        else:
            if 'selected_options' not in context.user_data:
                context.user_data['selected_options'] = set()
            context.user_data['selected_options'].add(option)

        # Update keyboard with new selection
        await query.edit_message_reply_markup(
            reply_markup=build_multi_select_keyboard(["Option1", "Option2", "Option3"])
        )

    elif data == "submit":
        selected = context.user_data.get('selected_options', set())
        await query.edit_message_text(f"Selected: {', '.join(selected)}")
        context.user_data.pop('selected_options', None)  # Clear selection

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Create a message with multiple lines + buttons
    message_text = (
        "🔹 *Item 1*: Description here\n"
        "🔹 *Item 2*: Another description\n"
        "🔹 *Item 3*: More details"
    )

    # Create inline buttons (one per row)
    keyboard = [
        # Row 1: Text + Button
        [
            InlineKeyboardButton("📝 Action for Item 1", callback_data="action_1")
        ],
        # Row 2: Text + Button
        [
            InlineKeyboardButton("⚙️ Action for Item 2", callback_data="action_2")
        ],
        # Row 3: Text + Button
        [
            InlineKeyboardButton("🔍 Action for Item 3", callback_data="action_3")
        ],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"  # For bold/formatting
    )

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
                    filters.Regex("^(Age|Favourite colour|Option1)$"), regular_choice
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

    application.add_handler(CommandHandler('help', help))
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(handle_selection))
    # Add handlers for dumper commands (only for admin)
    admin_id = os.environ.get('ADMIN_ID', None)
    assert admin_id is not None and admin_id.isdigit(), \
        (f"ADMIN_ID is not set. Is None: {admin_id is None}, Is not None, "
         f"but not digit: {admin_id is not None and not admin_id.isdigit()}")
    admin_id = int(admin_id)

    application.add_handler(CommandHandler('set_dumper', set_dumper, filters=filters.User(admin_id)))
    application.add_handler(CommandHandler('unset_dumper', unset_dumper, filters=filters.User(admin_id)))

    # Example of how to integrate the ConversationHandler for chart selection
    """
    To integrate the chart selection ConversationHandler, add this to your main bot setup:

    from telegram.ext import ConversationHandler, CommandHandler, MessageHandler, filters
    from border_equeue_stats.telegram_bot.stats_interface import (
        start_chart_conversation, 
        select_chart_type, 
        select_vehicles, 
        select_queues, 
        select_aggregation,
        select_time_range,
        select_aggregation_method,
        cancel_chart_conversation,
        SELECTING_CHART_TYPE, 
        SELECTING_VEHICLES, 
        SELECTING_QUEUES, 
        SELECTING_AGGREGATION,
        SELECTING_TIME_RANGE,
        SELECTING_AGG_METHOD
    )

    def setup_chart_conversation_handler():
        chart_conv_handler = ConversationHandler(
            entry_points=[CommandHandler('chart', start_chart_conversation)],
            states={
                SELECTING_CHART_TYPE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_chart_type)
                ],
                SELECTING_VEHICLES: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_vehicles)
                ],
                SELECTING_QUEUES: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_queues)
                ],
                SELECTING_AGGREGATION: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_aggregation)
                ],
                SELECTING_TIME_RANGE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_time_range)
                ],
                SELECTING_AGG_METHOD: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, select_aggregation_method)
                ],
            },
            fallbacks=[
                CommandHandler('cancel', cancel_chart_conversation),
                MessageHandler(filters.Regex('^❌ Cancel$'), cancel_chart_conversation)
            ],
        )
        return chart_conv_handler

    # Usage in main():
    # application.add_handler(setup_chart_conversation_handler())

    # New Features Summary:
    # ✅ 5-minute aggregation support
    # ✅ Monthly aggregation support  
    # ✅ Smart time range recommendations based on aggregation choice
    # ✅ Multiple aggregation methods (mean, max, min, drop)
    # ✅ Comprehensive docstrings for all queue_stats functions
    # ✅ Centralized datetime aggregation function in parquet_storage.py
    # ✅ Enhanced conversation flow with 7 steps for complete customization

    # Example user flow:
    # /chart → "Waiting Time by Load" → "Car" + "Bus" → "Live Queue" → "5-Minute" → "Last 3 Days" → "Mean" → Chart!
    """
    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)

