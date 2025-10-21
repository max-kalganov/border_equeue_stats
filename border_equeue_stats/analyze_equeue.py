from datetime import datetime, timedelta
import json
import plotly.express as px
import plotly.graph_objects as go
import typing as tp
from border_equeue_stats import constants as ct
from border_equeue_stats.queue_stats import get_waiting_time, get_count, get_count_by_regions, \
    get_single_vehicle_registrations_count, get_called_vehicles_waiting_time, get_number_of_declined_vehicles, \
    get_registered_count, get_called_count


def _optimize_figure_for_chat(fig: go.Figure) -> go.Figure:
    """Optimize figure size and layout for Telegram chat interface"""
    fig.update_layout(
        width=800,
        height=500,
        font=dict(size=12),
        margin=dict(l=50, r=50, t=50, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig


def get_figure_waiting_hours(queues_names, relative_time, floor_value: tp.Optional[str] = None,
                             aggregation_method: str = 'mean', time_range: tp.Optional[timedelta] = None):
    """Returns figure of waiting hours chart

    Args:
        queues_names: List of queue names
        relative_time: 'reg' or 'load'
        floor_value: Optional time aggregation ('5min', 'h', 'd', 'M', None)
        aggregation_method: How to aggregate values ('mean', 'max', 'min', 'drop')
        time_range: Optional time window to limit analysis
    """
    df = get_waiting_time(queues_names=queues_names, relative_time=relative_time,
                          floor_value=floor_value, aggregation_method=aggregation_method,
                          time_range=time_range)
    fig = px.line(df,
                  x='relative_time',
                  y='hours_waited',
                  labels=dict(relative_time="First in queue registration date",
                              hours_waited="Hours waited",
                              queue_name="Queue types"),
                  hover_name='first_vehicle_number',
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_waiting_hours_by_reg(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY),
                                    floor_value: tp.Optional[str] = None,
                                    aggregation_method: str = 'mean',
                                    time_range: tp.Optional[timedelta] = None):
    """Returns figure of waiting hours by registration time"""
    return get_figure_waiting_hours(queues_names, relative_time='reg', floor_value=floor_value,
                                    aggregation_method=aggregation_method, time_range=time_range)


def get_figure_waiting_hours_by_load(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY),
                                     floor_value: tp.Optional[str] = None,
                                     aggregation_method: str = 'mean',
                                     time_range: tp.Optional[timedelta] = None):
    """Returns figure of waiting hours by load time"""
    return get_figure_waiting_hours(queues_names, relative_time='load', floor_value=floor_value,
                                    aggregation_method=aggregation_method, time_range=time_range)


def get_figure_vehicle_counts(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY),
                              floor_value: tp.Optional[str] = None,
                              aggregation_method: str = 'max',
                              time_range: tp.Optional[timedelta] = None):
    """Returns figure of vehicle counts over time"""
    df = get_count(queues_names=queues_names, floor_value=floor_value,
                   aggregation_method=aggregation_method, time_range=time_range)
    fig = px.line(df,
                  x='relative_time',
                  y='vehicle_count',
                  labels=dict(relative_time="Queue dump date",
                              vehicle_count="Number of vehicles",
                              queue_name="Queue types"),
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_vehicle_count_per_regions(queue_name: str = ct.CAR_LIVE_QUEUE_KEY, plot_type: str = 'bar',
                                         floor_value: tp.Optional[str] = None,
                                         aggregation_method: str = 'sum',
                                         time_range: tp.Optional[timedelta] = None):
    """
    Returns figure of vehicle count per regions

    :param plot_type: str - 'bar' - regions are stacked on top of each other
                            'line' - separate lines for each region
    :param floor_value: Optional time aggregation ('5min', 'h', 'd', 'M', None)
    :param aggregation_method: How to aggregate values ('sum', 'mean', 'max', 'min', 'drop')
    :param time_range: Optional time window to limit analysis
    """
    assert plot_type in ('bar', 'line'), 'incorrect plot type'
    if plot_type == 'line':
        df = get_count_by_regions(queue_name=queue_name, floor_value=floor_value,
                                  aggregation_method=aggregation_method, time_range=time_range)
        fig = px.line(df,
                      x='relative_time',
                      y='vehicle_count',
                      labels=dict(relative_time="Queue dump date",
                                  vehicle_count="Number of vehicles",
                                  region="Regions"),
                      color='region')
    else:
        df = get_count_by_regions(queue_name=queue_name, floor_value=floor_value or 'd',
                                  aggregation_method=aggregation_method, time_range=time_range)
        fig = px.area(df,
                      x='relative_time',
                      y='vehicle_count',
                      labels=dict(relative_time="Queue dump date",
                                  vehicle_count="Number of vehicles",
                                  region="Regions"),
                      color='region',
                      line_group='region')
    return _optimize_figure_for_chat(fig)


def get_figure_frequent_vehicles_registrations_count(queue_name: str = ct.CAR_LIVE_QUEUE_KEY, has_been_called: bool = False):
    """Returns figure of frequent vehicle registrations count as pie chart"""
    filtering_date = '2024-09-01'
    reg_freq_df = get_single_vehicle_registrations_count(queue_name=queue_name,
                                                         has_been_called=has_been_called,
                                                         filters=[(ct.LOAD_DATE_COLUMN, '>=',
                                                                   datetime.strptime(filtering_date,
                                                                                     '%Y-%m-%d'))])
    reg_freq_df['count_names'] = reg_freq_df['count_of_registrations'].apply(
        lambda c: f"{c} time(s) were in queue ({filtering_date} - {datetime.today().date()})"
    )
    fig = px.pie(reg_freq_df, values='vehicle_count', names='count_names')
    return _optimize_figure_for_chat(fig)


def get_figure_called_status_waiting_time(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.CAR_PRIORITY_KEY),
                                          aggregation_type: str = 'min',
                                          floor_value: tp.Optional[str] = None,
                                          time_range: tp.Optional[timedelta] = None):
    """Returns figure of called status waiting time"""
    waiting_df = get_called_vehicles_waiting_time(queues_names=queues_names,
                                                  aggregation_type=aggregation_type,
                                                  floor_value=floor_value,
                                                  time_range=time_range)
    fig = px.line(waiting_df,
                  x='relative_time',
                  y='waiting_after_called',
                  labels=dict(relative_time="Called to the border",
                              waiting_after_called="Minutes waited after called",
                              queue_name="Queue types"),
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_declined_vehicles(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY),
                                 floor_value: tp.Optional[str] = None,
                                 aggregation_method: str = 'sum',
                                 time_range: tp.Optional[timedelta] = None):
    """Returns figure of declined vehicles count"""
    declined_df = get_number_of_declined_vehicles(queues_names=queues_names,
                                                  floor_value=floor_value,
                                                  aggregation_method=aggregation_method,
                                                  time_range=time_range)
    fig = px.line(declined_df,
                  x='relative_time',
                  y='vehicle_count',
                  labels=dict(relative_time="Queue dump date",
                              vehicle_count="Number of declined vehicles",
                              queue_name="Queue types"),
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_registered_vehicles(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY),
                                   floor_value: str = 'h'):
    """Returns figure of registered vehicles count"""
    registered_df = get_registered_count(
        queues_names=queues_names, floor_value=floor_value)
    fig = px.line(registered_df,
                  x='relative_time',
                  y='vehicle_count',
                  labels=dict(relative_time="Registered time to hours",
                              vehicle_count="Number of registered vehicles",
                              queue_name="Queue types"),
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_called_vehicles(queues_names: tp.List[str] = (ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY),
                               floor_value: str = 'h'):
    """Returns figure of called vehicles count"""
    called_df = get_called_count(
        queues_names=queues_names, floor_value=floor_value)
    fig = px.line(called_df,
                  x='relative_time',
                  y='vehicle_count',
                  labels=dict(relative_time="Called time to hours",
                              vehicle_count="Number of called vehicles",
                              queue_name="Queue types"),
                  color='queue_name')
    return _optimize_figure_for_chat(fig)


def get_figure_cars_cnt():
    """Returns figure of cars count from raw data file"""
    with open('data/brest_border_equeue.txt', 'r') as f:
        lines = f.readlines()

    all_dt, all_cnt = [], []
    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', 'null'))
        que_cnt = len(line_dict['carLiveQueue'])
        all_dt.append(datetime.strptime(
            line_dict['datetime'], '%Y-%m-%d %H:%M:%S.%f'))
        all_cnt.append(que_cnt)

    fig = px.line(x=all_dt, y=all_cnt, labels=dict(
        x="Date time", y="Cars count"))
    fig.update_traces(mode="markers+lines")
    return _optimize_figure_for_chat(fig)
