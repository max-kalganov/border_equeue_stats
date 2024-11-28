from datetime import datetime
import json
import plotly.express as px
import typing as tp
from border_equeue_stats import constants as ct
from border_equeue_stats.queue_stats import get_waiting_time, get_count, get_count_by_regions, \
    get_single_vehicle_registrations_count, get_called_vehicles_waiting_time, get_number_of_declined_vehicles, \
    get_registered_per_hour


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


def plot_vehicle_counts(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY)):
    df = get_count(queues_names=queues_names)
    fig = px.line(df,
                  x='relative_time',
                  y='vehicle_count',
                  labels=dict(relative_time="Queue dump date",
                              vehicle_count="Number of vehicles",
                              queue_name="Queue types"),
                  color='queue_name')
    fig.show()


def plot_vehicle_count_per_regions(queue_name: str = ct.CAR_LIVE_QUEUE_KEY, plot_type: str = 'bar'):
    """
    :param plot_type: str - 'bar' - regions are stacked on top of each other
                            'line' - separate lines for each region
    """
    assert plot_type in ('bar', 'line'), 'incorrect plot type'
    if plot_type == 'line':
        df = get_count_by_regions(queue_name=queue_name, floor_value=None)
        fig = px.line(df,
                      x='relative_time',
                      y='vehicle_count',
                      labels=dict(relative_time="Queue dump date",
                                  vehicle_count="Number of vehicles",
                                  region="Regions"),
                      color='region')
    else:
        df = get_count_by_regions(queue_name=queue_name)  # , floor_value='d')
        fig = px.area(df,
                      x='relative_time',
                      y='vehicle_count',
                      labels=dict(relative_time="Queue dump date",
                                  vehicle_count="Number of vehicles",
                                  region="Regions"),
                      color='region',
                      line_group='region')
    fig.show()


def plot_frequent_vehicles_registrations_count(queue_name: str = ct.CAR_LIVE_QUEUE_KEY, has_been_called: bool = False):
    filtering_date = '2024-09-01'
    reg_freq_df = get_single_vehicle_registrations_count(queue_name=queue_name,
                                                         has_been_called=has_been_called,
                                                         filters=[(ct.LOAD_DATE_COLUMN, '>=',
                                                                   datetime.strptime(filtering_date,
                                                                                     '%Y-%m-%d'))])
    reg_freq_df['count_names'] = reg_freq_df['count_of_registrations'].apply(
        lambda c: f"{c} time(s) were in queue ({filtering_date} - {datetime.today().date()})"
    )
    px.pie(reg_freq_df, values='vehicle_count', names='count_names').show()
    # px.bar(reg_freq_df, x='count_of_registrations', y='vehicle_count').show()


def plot_called_status_waiting_time(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY),
                                    aggregation_type: str = 'min'):
    waiting_df = get_called_vehicles_waiting_time(queues_names=queues_names, aggregation_type=aggregation_type)
    px.line(waiting_df,
            x='relative_time',
            y='waiting_after_called',
            labels=dict(relative_time="Called to the border",
                        waiting_after_called="Minutes waited after called",
                        queue_name="Queue types"),
            color='queue_name').show()


def plot_declined_vehicles(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY)):
    declined_df = get_number_of_declined_vehicles(queues_names=queues_names)
    px.line(declined_df,
            x='relative_time',
            y='vehicle_count',
            labels=dict(relative_time="Queue dump date",
                        vehicle_count="Number of declined vehicles",
                        queue_name="Queue types"),
            color='queue_name').show()


def plot_registered_vehicles(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY)):
    registered_df = get_registered_per_hour(queues_names=queues_names)
    px.line(registered_df,
            x='relative_time',
            y='vehicle_count',
            labels=dict(relative_time="Registered time to hours",
                        vehicle_count="Number of registered vehicles",
                        queue_name="Queue types"),
            color='queue_name').show()


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
