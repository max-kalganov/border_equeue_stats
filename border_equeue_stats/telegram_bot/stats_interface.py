
import logging
import os

from plotly import express as px
from telegram import Update
from telegram.ext import ContextTypes

from border_equeue_stats import constants as ct
from border_equeue_stats.queue_stats import get_waiting_time

logger = logging.getLogger(ct.STAT_INTERFACE_LOGGER_NAME)


async def plot_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"user_id={update.effective_user.id}, chat_id={update.effective_chat.id}, msg={update.message.text}")
    # reply_message = update.message.reply_text(text='Generating image...')
    message = await context.bot.send_message(chat_id=update.effective_chat.id, text='Generating image...')

    df = get_waiting_time(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY],
                          relative_time='load')
    fig = px.line(df,
                  x='relative_time',
                  y='hours_waited',
                  labels=dict(relative_time="First in queue registration date",
                              hours_waited="Hours waited",
                              queue_name="Queue types"),
                  hover_name='first_vehicle_number',
                  color='queue_name')
    os.makedirs('data/images', exist_ok=True)
    fig.write_image("data/images/tmp_image.png", scale=6)
    im = open("data/images/tmp_image.png", 'rb')

    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=message.message_id)
    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=im, caption=update.message.text)
