from border_equeue_stats import constants as ct
from border_equeue_stats.analyze_equeue import *


if __name__ == '__main__':
    # plot_waiting_hours_by_reg(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    # plot_waiting_hours_by_load(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    plot_vehicle_counts()
