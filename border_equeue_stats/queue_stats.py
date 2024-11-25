import numpy as np
import pandas as pd
import typing as tp

import pyarrow.compute as pc
from pyarrow.parquet import filters_to_expression

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
        queue_df['hours_waited'] = queue_df['hours_waited'].apply(lambda hw: round(hw.total_seconds() / 3600, 2))
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


def get_count(queues_names: tp.List[str],
              filters: tp.Optional[tp.List] = None) -> pd.DataFrame:
    """
    Returns DataFrame with vehicle counts over time.

    :param queues_names: List[str] - list of queues which will be in the
        dataframe separated by queue_name column value
    :param filters: Optional[List[str]] - parquet data filters
    :return:
        DataFrame columns:
        (1) relative_time - data load date
        (2) vehicle_count - number of ordered vehicles in queue at a specific load date
        (3) queue_name - name of the queue for the specific row
    """

    def read_queue(name):
        read_filters = filters if filters is not None else []
        read_filters.append((ct.QUEUE_POS_COLUMN, '!=', np.nan))
        queue_df = list(read_from_parquet(
            name,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.QUEUE_POS_COLUMN, ct.LOAD_DATE_COLUMN]
        ))[0]
        queue_df = queue_df.groupby(ct.LOAD_DATE_COLUMN).aggregate({ct.QUEUE_POS_COLUMN: 'max'}).reset_index()
        queue_df = queue_df.rename(columns={ct.LOAD_DATE_COLUMN: 'relative_time',
                                            ct.QUEUE_POS_COLUMN: 'vehicle_count'})
        queue_df['queue_name'] = name
        return queue_df[['relative_time', 'vehicle_count', 'queue_name']].sort_values('relative_time')

    unq_queue_names = set(queues_names)
    assert (len(queues_names) > 0
            and len(ct.ALL_EQUEUE_KEYS) > len(queues_names) == len(unq_queue_names)
            and ct.INFO_KEY not in unq_queue_names
            and unq_queue_names < set(ct.ALL_EQUEUE_KEYS)), \
        'incorrect queue names for get_count'

    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_count_by_regions(queue_name: str,
                         filters: tp.Optional[tp.List] = None,
                         floor_value: tp.Optional[str] = None) -> pd.DataFrame:
    """
    Returns DataFrame with vehicle counts over time per regions.

    :param queue_name: str - queue which will be stored into the dataframe
    :param filters: Optional[List[str]] - parquet data filters
    :param floor_value: Optional[str] - sets larger buckets for counting
    :return:
        DataFrame columns:
        (1) relative_time - data load date
        (2) vehicle_count - number of ordered vehicles in queue at a specific load date
        (3) region - region label
    """
    assert (isinstance(queue_name, str)
            and queue_name in ct.ALL_EQUEUE_KEYS
            and queue_name != ct.INFO_KEY), \
        'incorrect queue names for get_count_by_regions'

    read_filters = filters if filters is not None else []
    read_filters.append((ct.QUEUE_POS_COLUMN, '!=', np.nan))
    queue_df = list(read_from_parquet(
        queue_name,
        filters=read_filters,
        parquet_storage_path=ct.PARQUET_STORAGE_PATH,
        in_batches=False,
        columns=[ct.CAR_NUMBER_COLUMN, ct.LOAD_DATE_COLUMN]
    ))[0]
    # queue_df = queue_df.groupby(ct.CAR_NUMBER_COLUMN).aggregate({ct.LOAD_DATE_COLUMN: 'min'}).reset_index()
    queue_df['region'] = queue_df[ct.CAR_NUMBER_COLUMN].apply(
        lambda cn: cn[-1] if ct.BELARUS_CAR_NUMBER_FORMAT.match(cn) else None
    )
    if floor_value is not None:
        queue_df[ct.LOAD_DATE_COLUMN] = queue_df[ct.LOAD_DATE_COLUMN].apply(lambda t: t.floor(floor_value))
        queue_df = queue_df.drop_duplicates([ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN])
    queue_df = queue_df \
        .groupby(by=[ct.LOAD_DATE_COLUMN, 'region']) \
        .aggregate({ct.CAR_NUMBER_COLUMN: 'count'}) \
        .reset_index()
    queue_df = queue_df.rename(columns={ct.LOAD_DATE_COLUMN: 'relative_time',
                                        ct.CAR_NUMBER_COLUMN: 'vehicle_count'})
    queue_df['region'] = queue_df['region'].apply(lambda r: ct.BELARUS_REGIONS_MAP.get(r, 'other'))
    return queue_df[['relative_time', 'vehicle_count', 'region']].sort_values('relative_time')


def get_single_vehicle_registrations_count(queue_name: str,
                                           filters: tp.Optional[tp.List] = None,
                                           has_been_called: bool = False) -> pd.DataFrame:
    """
    Returns DataFrame with vehicles grouped by registration counts where count >1.

    :param queue_name: str - queue which will be stored into the dataframe
    :param filters: Optional[List[str]] - parquet data filters
    :param has_been_called: bool - True - select only vehicles registrations, which were successfully completed
    :return:
        DataFrame columns:
        (1) count_of_registrations - number of registration of a vehicle in queue
        (2) vehicle_count - number of ordered vehicles in queue at a specific load date
    """
    if has_been_called:
        queue_pos_filter_expr = pc.field(ct.QUEUE_POS_COLUMN).is_null()
        if filters is not None:
            read_filters = filters_to_expression(filters)
            read_filters = read_filters & queue_pos_filter_expr
        else:
            read_filters = queue_pos_filter_expr
        queue_df = list(read_from_parquet(
            queue_name,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN, ct.STATUS_COLUMN]
        ))[0]
        queue_df = queue_df.drop_duplicates()
        queue_df = queue_df\
            .groupby([ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN])\
            .apply(lambda gr: pd.Series({'is_canceled': 9 in gr[ct.STATUS_COLUMN]})).reset_index()
        queue_df = queue_df[queue_df['is_canceled'] == False].reset_index()
    else:
        queue_df = list(read_from_parquet(
            queue_name,
            filters=filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN]
        ))[0]
        queue_df = queue_df.drop_duplicates()
    queue_df = queue_df.groupby(ct.CAR_NUMBER_COLUMN).count().reset_index()
    queue_df = queue_df.groupby(ct.REGISTRATION_DATE_COLUMN).count().reset_index()
    queue_df = queue_df.rename(columns={ct.REGISTRATION_DATE_COLUMN: 'count_of_registrations',
                                        ct.CAR_NUMBER_COLUMN: 'vehicle_count'})
    return queue_df[['vehicle_count', 'count_of_registrations']]
