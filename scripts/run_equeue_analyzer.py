from border_equeue_stats import constants as ct
# ct.PARQUET_STORAGE_PATH = 'data/parquet_dataset_tmp'

from border_equeue_stats.analyze_equeue import *


if __name__ == '__main__':
    fig = get_figure_vehicle_counts()
    # fig = get_figure_waiting_hours_by_load(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    
    # fig = get_figure_waiting_hours_by_reg(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY])
    fig.show()
    # get_figure_vehicle_counts()
    # get_figure_vehicle_count_per_regions(get_figure_type='line')
    # get_figure_called_status_waiting_time(queues_names=[ct.CAR_LIVE_QUEUE_KEY, ct.BUS_LIVE_QUEUE_KEY,
    #                                               ct.MOTORCYCLE_LIVE_QUEUE_KEY], aggregation_type='mean')
    # get_figure_declined_vehicles()
    # get_figure_registered_vehicles()
    # get_figure_called_vehicles()
