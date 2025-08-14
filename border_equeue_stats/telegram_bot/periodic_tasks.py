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
    """Удаляет задание с указанным именем. Возвращает, было ли задание удалено."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


async def set_dumper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Добавляет задание в очередь."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running set_dumper")
    chat_id = update.effective_message.chat_id
    try:
        # TODO: check user
        # args[0] должен содержать время таймера в секундах.
        due = float(context.args[0])
        if due < 0:
            await update.effective_message.reply_text("Извините, нельзя вернуться в будущее!")
            return

        # args[0] должен содержать время таймера в секундах.
        job_removed = remove_job_if_exists(str(chat_id), context)
        context.job_queue.run_repeating(periodic_processor, interval=timedelta(seconds=due), chat_id=chat_id,
                                        name=str(chat_id), data=None)

        text = "Timer successfully set!"
        if job_removed:
            text += " Old one was removed."
        await update.effective_message.reply_text(text)

    except (IndexError, ValueError):
        await update.effective_message.reply_text("Используйте: /set_dumper <seconds>")


async def periodic_processor(context):
    """Background task that processes queue data periodically."""
    try:
        logger.info(f"Running periodic processor at {datetime.now()}")

        # Add your queue processing logic here
        # For example:
        # - Parse equeue data
        # - Update statistics
        # - Send notifications
        # - Clean up old data

        # Example processing (replace with your actual logic):
        # from border_equeue_stats.equeue_parser import parse_equeue
        # from border_equeue_stats.data_storage.parquet_storage import dump_to_parquet
        # from border_equeue_stats.constants import EQUEUE_JSON_PATH
        #
        # data = parse_equeue(url=EQUEUE_JSON_PATH)
        # dump_to_parquet(data)

        logger.info("Periodic processor completed successfully")

    except Exception as e:
        logger.error(f"Error in periodic processor: {e}")
    await context.bot.send_message(context.job.chat_id, text="Complete peridic processor")


async def unset_dumper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет задание, если пользователь передумал."""
    user_logger = UserLogger(update.effective_user.id)
    user_logger.info("Running unset_dumper")

    chat_id = update.message.chat_id
    job_removed = remove_job_if_exists(str(chat_id), context)
    text = "Таймер успешно отменен!" if job_removed else "Нет активного таймера."
    await update.message.reply_text(text)
