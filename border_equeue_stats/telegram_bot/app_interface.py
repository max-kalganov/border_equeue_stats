import os

import telebot
import plotly
import plotly.express as px

from border_equeue_stats.queue_stats import get_waiting_time
from border_equeue_stats import constants as ct

BOT_TOKEN = os.environ.get('BOT_TOKEN', None)
bot = telebot.TeleBot(BOT_TOKEN)


@bot.message_handler(commands=['start', 'hello'])
def send_welcome(message):
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
    bot.send_photo(chat_id=message.chat.id, photo=im)
    bot.reply_to(message, "send a photo")


@bot.message_handler(func=lambda msg: True)
def echo_all(message):
    bot.reply_to(message, message.text)


if __name__ == '__main__':
    bot.infinity_polling()
