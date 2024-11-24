import pandas as pd
import typing as tp
from border_equeue_stats import constants as ct
from border_equeue_stats.data_storage.parquet_storage import read_from_parquet


def get_waiting_time(queues_names: tp.List[str],
                     relative_time: str = 'reg',
                     filters: tp.Optional[tp.List] = None) -> pd.DataFrame:
    """
    Returns DataFrame with waiting time values.

    Helps to understand, how many hours are waiting people in the queue.

    Notes:
        The following details are using 'car' in description. Car is given as an example,
        if queue_names contains 'carLiveQueue' or 'carPriority', otherwise it could be replaced with
        truck/bus/motorcycle.

    :param queues_names: List[str] - list of queues which will be in the
        dataframe separated by queue_name column value
    :param relative_time: str - 'reg' or 'load'
        'reg' - sets relative_time column values as the first car in queue registration time
        'load' - sets relative_time column values as a time, when queue dump was made
    :param filters: Optional[List[str]] - parquet data filters
    :return:
        DataFrame columns:
        (1) relative_time - depending on relative_time parameter
        (2) hours_waited - how much time the first car has waited in queue already
        (3) first_vehicle_number - car number of the first vehicle in queue
        (4) queue_name - name of the queue for the specific row
    """

    def read_queue(name):
        read_filters = filters if filters is not None else []
        read_filters.append((ct.QUEUE_POS_COLUMN, '==', 1))
        queue_df = list(read_from_parquet(
            name,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.REGISTRATION_DATE_COLUMN, ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN]
        ))[0]
        queue_df['hours_waited'] = queue_df[ct.LOAD_DATE_COLUMN] - queue_df[ct.REGISTRATION_DATE_COLUMN]
        queue_df['hours_waited'] = queue_df['hours_waited'].apply(lambda hw: round(hw.total_seconds()/3600, 2))
        queue_df['queue_name'] = name
        relative_time_column = ct.LOAD_DATE_COLUMN if relative_time == 'load' else ct.REGISTRATION_DATE_COLUMN
        queue_df = queue_df.rename(columns={relative_time_column: 'relative_time',
                                            ct.CAR_NUMBER_COLUMN: 'first_vehicle_number'})
        return queue_df[['relative_time', 'hours_waited',
                         'first_vehicle_number', 'queue_name']].sort_values('relative_time')

    unq_queue_names = set(queues_names)
    assert (len(queues_names) > 0
            and len(ct.ALL_EQUEUE_KEYS) > len(queues_names) == len(unq_queue_names)
            and ct.INFO_KEY not in unq_queue_names
            and unq_queue_names < set(ct.ALL_EQUEUE_KEYS)), \
        'incorrect queue names for get_waiting_time'
    assert relative_time in {'reg', 'load'}

    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)
