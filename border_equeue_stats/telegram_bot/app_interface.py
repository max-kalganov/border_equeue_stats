import os
import logging
from uuid import uuid4
import plotly.express as px

from telegram import Update, InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

from border_equeue_stats.queue_stats import get_waiting_time
from border_equeue_stats import constants as ct

BOT_TOKEN = os.environ.get('BOT_TOKEN', None)


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=im, caption=update.message.text)


if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    start_handler = CommandHandler('start', start)
    message_handler = MessageHandler(filters=filters.TEXT & (~filters.COMMAND), callback=echo)

    application.add_handler(start_handler)
    application.add_handler(message_handler)

    application.run_polling()
