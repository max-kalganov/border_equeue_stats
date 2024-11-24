from datetime import datetime
import json
import plotly.express as px
import typing as tp
from border_equeue_stats import constants as ct
from border_equeue_stats.queue_stats import get_waiting_time


def plot_waiting_hours(queues_names, relative_time):
    df = get_waiting_time(queues_names=queues_names, relative_time=relative_time)
    fig = px.line(df,
                  x='relative_time',
                  y='hours_waited',
                  labels=dict(relative_time="First in queue registration date",
                              hours_waited="Hours waited",
                              queue_name="Queue types"),
                  hover_name='first_vehicle_number',
                  color='queue_name')
    fig.show()


def plot_waiting_hours_by_reg(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY)):
    plot_waiting_hours(queues_names, relative_time='reg')


def plot_waiting_hours_by_load(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY)):
    plot_waiting_hours(queues_names, relative_time='load')


def plot_cars_cnt():
    with open('data/brest_border_equeue.txt', 'r') as f:
        lines = f.readlines()

    all_dt, all_cnt = [], []
    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', 'null'))
        que_cnt = len(line_dict['carLiveQueue'])
        all_dt.append(datetime.strptime(line_dict['datetime'], '%Y-%m-%d %H:%M:%S.%f'))
        all_cnt.append(que_cnt)

    fig = px.line(x=all_dt, y=all_cnt, labels=dict(x="Date time", y="Cars count"))
    fig.update_traces(mode="markers+lines")
    fig.show()
