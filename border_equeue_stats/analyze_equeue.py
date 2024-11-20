from datetime import datetime
import json
import plotly.express as px


def plot_waiting_hours():
    with open('data/brest_border_equeue.txt', 'r') as f:
        lines = f.readlines()

    all_dt, all_td, all_regnum = [], [], []
    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', 'null'))
        first_in_queue = line_dict['carLiveQueue'][0]
        all_regnum.append(f"{first_in_queue['regnum']}-{line_dict['datetime']}")
        all_dt.append(datetime.strptime(first_in_queue['registration_date'],
                                        '%H:%M:%S %d.%m.%Y'))
        all_td.append(round((datetime.strptime(line_dict['datetime'],
                                               '%Y-%m-%d %H:%M:%S.%f')
                             - datetime.strptime(first_in_queue['registration_date'],
                                                 '%H:%M:%S %d.%m.%Y')).total_seconds() / 3600, 2))

    fig = px.line(x=all_dt, y=all_td, labels=dict(x="First in queue", y="Hours waited"), hover_name=all_regnum)
    fig.update_traces(mode="markers+lines")
    fig.show()


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
