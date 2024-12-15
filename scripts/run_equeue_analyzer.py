from border_equeue_stats import constants as ct
from border_equeue_stats.analyze_equeue import *


if __name__ == '__main__':
    plot_waiting_hours_by_reg(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    # plot_waiting_hours_by_load(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    # plot_vehicle_counts()
    # plot_vehicle_count_per_regions(plot_type='line')
    # plot_frequent_vehicles_registrations_count(has_been_called=True)
    # plot_called_status_waiting_time(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY,
    #                                               ct.MOTORCYCLE_LIVE_QUEUE_KEY], aggregation_type='mean')
    # plot_declined_vehicles()
    # plot_registered_vehicles()
    # plot_called_vehicles()
