import logging
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import ContextTypes

from border_equeue_stats.constants import PERIODIC_TASKS_LOGGER_NAME
from border_equeue_stats.telegram_bot.logging_utils import hash_user_id

from border_equeue_stats.equeue_parser import parse_equeue
from border_equeue_stats.data_storage.parquet_storage import dump_to_parquet
from border_equeue_stats.constants import EQUEUE_JSON_PATH


logger = logging.getLogger(PERIODIC_TASKS_LOGGER_NAME)


def remove_job_if_exists(name: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Removes job with specified name. Returns True if job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


async def set_dumper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds job to the queue."""
    logger.info(f"Running set_dumper -- user: {hash_user_id(update.effective_user.id)}")
    chat_id = update.effective_message.chat_id
    try:
        # args[0] should contain time in seconds.
        due = float(context.args[0])
        if due < 0:
            await update.effective_message.reply_text("Incorrect time in seconds!")
            return

        job_removed = remove_job_if_exists(str(chat_id), context)
        context.job_queue.run_repeating(dump_equeue_stats_task, interval=timedelta(seconds=due), chat_id=chat_id,
                                        name=str(chat_id), data=None)

        text = "Timer successfully set!"
        if job_removed:
            text += " Old one was removed."
        await update.effective_message.reply_text(text)

    except (IndexError, ValueError):
        await update.effective_message.reply_text("Use: /set_dumper <seconds>")


async def dump_equeue_stats_task(context):
    """Background task that processes queue data periodically."""
    try:
        logger.info(f"Dumping equeue stats at {datetime.now()}")
        dump_to_parquet(parse_equeue(url=EQUEUE_JSON_PATH))
        logger.info("Dumping equeue stats completed successfully")

    except Exception as e:
        logger.error(f"Error in dumping equeue stats: {e}")
    await context.bot.send_message(context.job.chat_id, text="Complete dumping equeue stats")


async def unset_dumper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Removes job if user wants to stop it."""
    logger.info(f"Running unset_dumper -- user: {hash_user_id(update.effective_user.id)}")

    chat_id = update.message.chat_id
    job_removed = remove_job_if_exists(str(chat_id), context)
    text = "Timer successfully removed!" if job_removed else "No active timer."
    await update.message.reply_text(text)
