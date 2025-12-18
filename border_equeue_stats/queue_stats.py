import numpy as np
import pandas as pd
import typing as tp
from datetime import datetime, timedelta

import pyarrow.compute as pc
from pyarrow.parquet import filters_to_expression

from border_equeue_stats import constants as ct
from border_equeue_stats.data_storage.parquet_storage import read_from_parquet
from border_equeue_stats.data_processing import apply_datetime_aggregation


def check_queue_names(queues_names: tp.List[str]) -> None:
    """
    Validate that queue names are correct and unique.
    
    Args:
        queues_names: List of queue names to validate
        
    Raises:
        AssertionError: If queue names are invalid, empty, or contain duplicates
    """
    unq_queue_names = set(queues_names)
    assert (len(queues_names) > 0
            and len(queues_names) == len(unq_queue_names)
            and all(check_single_queue_name(q) for q in queues_names)), \
        f'incorrect queue names - {queues_names}'


def check_single_queue_name(queue_name: str) -> bool:
    """
    Validate a single queue name.
    
    Args:
        queue_name: Queue name to validate
        
    Returns:
        True if queue name is valid, False otherwise
    """
    return (isinstance(queue_name, str)
            and queue_name in ct.ALL_EQUEUE_KEYS
            and queue_name != ct.INFO_KEY), \
        f'incorrect queue name - {queue_name}'


def get_waiting_time(queues_names: tp.List[str],
                     relative_time: str = 'reg',
                     filters: tp.Optional[tp.List] = None,
                     floor_value: tp.Optional[str] = None,
                     aggregation_method: str = 'mean',
                     time_range: tp.Optional[timedelta] = None) -> pd.DataFrame:
    """
    Returns DataFrame with waiting time values showing how long vehicles wait in queue.

    Helps to understand, how many hours are waiting people in the queue.

    Args:
        queues_names: List of queue names to include in analysis. Must be valid queue keys
                     from constants (e.g., ['carLiveQueue', 'busLiveQueue'])
        relative_time: Time reference for analysis. Options:
                      - 'reg': Use vehicle registration time as x-axis
                      - 'load': Use queue data collection time as x-axis
        filters: Optional parquet filters to limit data scope. Format: [(column, operator, value)]
                Example: [('load_date', '>=', datetime(2024, 1, 1))]
        floor_value: Time aggregation period for grouping data points:
                    - None: All individual data points (no aggregation)
                    - '5min': 5-minute intervals
                    - 'h': Hourly aggregation
                    - 'd': Daily aggregation  
                    - 'M': Monthly aggregation
        aggregation_method: How to combine values within each time bucket:
                           - 'mean': Average waiting time in each period
                           - 'max': Maximum waiting time in each period
                           - 'min': Minimum waiting time in each period
                           - 'drop': Just remove intermediate points (keep first)
        time_range: Optional time window to limit analysis (e.g., timedelta(days=30))
                   Data will be filtered to this range from the most recent date

    Returns:
        DataFrame with columns:
        - relative_time: Time reference (registration or load date based on relative_time param)
        - hours_waited: Waiting time in hours for first vehicle in queue
        - first_vehicle_number: License plate of first vehicle in queue
        - queue_name: Name of the queue for this data row

    Raises:
        AssertionError: If queues_names are invalid or relative_time not in {'reg', 'load'}
        
    Example:
        >>> df = get_waiting_time(['carLiveQueue'], relative_time='load', 
                                 floor_value='h', aggregation_method='mean')
        >>> print(df.head())
    """
    def read_queue(name):
        read_filters = filters if filters is not None else []
        read_filters.append((ct.QUEUE_POS_COLUMN, '==', 1))
        
        # Add time range filter if specified
        relative_time_column = ct.LOAD_DATE_COLUMN if relative_time == 'load' else ct.REGISTRATION_DATE_COLUMN
        if time_range is not None:
            cutoff_date = datetime.now() - time_range
            read_filters.append((relative_time_column, '>=', cutoff_date))
        
        queue_df = list(read_from_parquet(
            name,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.REGISTRATION_DATE_COLUMN, ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN]
        ))[0]
        
        if queue_df is None or len(queue_df) == 0:
            return pd.DataFrame(columns=['relative_time', 'hours_waited', 'first_vehicle_number', 'queue_name'])
        
        queue_df['hours_waited'] = queue_df[ct.LOAD_DATE_COLUMN] - queue_df[ct.REGISTRATION_DATE_COLUMN]
        queue_df['hours_waited'] = queue_df['hours_waited'].apply(lambda hw: round(hw.total_seconds() / 3600, 2))
        queue_df['queue_name'] = name
        
        # Apply time aggregation if specified
        if floor_value is not None:
            queue_df = apply_datetime_aggregation(
                df=queue_df,
                time_column=relative_time_column,
                floor_value=floor_value,
                aggregation_method=aggregation_method,
                group_columns=['queue_name'],
                value_columns={'hours_waited': aggregation_method, ct.CAR_NUMBER_COLUMN: 'first'}
            )
        
        queue_df = queue_df.rename(columns={relative_time_column: 'relative_time',
                                            ct.CAR_NUMBER_COLUMN: 'first_vehicle_number'})
        return queue_df[['relative_time', 'hours_waited',
                         'first_vehicle_number', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    assert relative_time in {'reg', 'load'}, f"relative_time must be 'reg' or 'load', got {relative_time}"
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_count(queues_names: tp.List[str],
              filters: tp.Optional[tp.List] = None,
              floor_value: tp.Optional[str] = None,
              aggregation_method: str = 'max',
              time_range: tp.Optional[timedelta] = None) -> pd.DataFrame:
    """
    Returns DataFrame with vehicle counts over time for specified queues.

    This function analyzes queue capacity by tracking the number of vehicles
    in each queue at different time points.

    Args:
        queues_names: List of queue names to analyze. Must be valid queue keys
                     from constants (e.g., ['carLiveQueue', 'busLiveQueue'])
        filters: Optional parquet filters to limit data scope. Format: [(column, operator, value)]
                Example: [('load_date', '>=', datetime(2024, 1, 1))]
        floor_value: Time aggregation period for grouping data points:
                    - None: All individual data points (no aggregation)
                    - '5min': 5-minute intervals  
                    - 'h': Hourly aggregation
                    - 'd': Daily aggregation
                    - 'M': Monthly aggregation
        aggregation_method: How to combine vehicle counts within each time bucket:
                           - 'max': Peak vehicle count in each period (default, most meaningful)
                           - 'mean': Average vehicle count in each period
                           - 'min': Minimum vehicle count in each period
                           - 'drop': Just remove intermediate points (keep first)
        time_range: Optional time window to limit analysis (e.g., timedelta(days=30))
                   Data will be filtered to this range from the most recent date

    Returns:
        DataFrame with columns:
        - relative_time: Data collection timestamp
        - vehicle_count: Number of vehicles in queue at that time
        - queue_name: Name of the queue for this data row

    Raises:
        AssertionError: If queues_names are invalid
        
    Example:
        >>> df = get_count(['carLiveQueue', 'busLiveQueue'], floor_value='d', 
                          aggregation_method='max')
        >>> print(df.head())
    """
    def read_queue(name):
        read_filters = filters if filters is not None else []
        read_filters.append((ct.QUEUE_POS_COLUMN, '!=', np.nan))
        
        # Add time range filter if specified
        if time_range is not None:
            cutoff_date = datetime.now() - time_range
            read_filters.append((ct.LOAD_DATE_COLUMN, '>=', cutoff_date))
        
        queue_df = list(read_from_parquet(
            name,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.QUEUE_POS_COLUMN, ct.LOAD_DATE_COLUMN]
        ))[0]
        
        if queue_df is None or len(queue_df) == 0:
            return pd.DataFrame(columns=['relative_time', 'vehicle_count', 'queue_name'])
        
        # First get max position per load date (represents queue length)
        queue_df = queue_df.groupby(ct.LOAD_DATE_COLUMN).aggregate({ct.QUEUE_POS_COLUMN: 'max'}).reset_index()
        queue_df['queue_name'] = name
        
        # Apply time aggregation if specified
        if floor_value is not None:
            queue_df = apply_datetime_aggregation(
                df=queue_df,
                time_column=ct.LOAD_DATE_COLUMN,
                floor_value=floor_value,
                aggregation_method=aggregation_method,
                group_columns=['queue_name'],
                value_columns={ct.QUEUE_POS_COLUMN: aggregation_method}
            )
            
        queue_df = queue_df.rename(columns={ct.LOAD_DATE_COLUMN: 'relative_time',
                                            ct.QUEUE_POS_COLUMN: 'vehicle_count'})
        return queue_df[['relative_time', 'vehicle_count', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_count_by_regions(queue_name: str,
                         filters: tp.Optional[tp.List] = None,
                         floor_value: tp.Optional[str] = None,
                         aggregation_method: str = 'sum',
                         time_range: tp.Optional[timedelta] = None) -> pd.DataFrame:
    """
    Returns DataFrame with vehicle counts over time per regions for a single queue.

    This function analyzes regional distribution of vehicles in a specific queue,
    showing how different regions contribute to queue composition over time.

    Args:
        queue_name: Single queue name to analyze. Must be a valid queue key from constants
                   (e.g., 'carLiveQueue', 'busLiveQueue')
        filters: Optional parquet filters to limit data scope. Format: [(column, operator, value)]
                Example: [('load_date', '>=', datetime(2024, 1, 1))]
        floor_value: Time aggregation period for grouping data points:
                    - None: All individual data points (no aggregation)
                    - '5min': 5-minute intervals
                    - 'h': Hourly aggregation
                    - 'd': Daily aggregation
                    - 'M': Monthly aggregation
        aggregation_method: How to combine vehicle counts within each time bucket:
                           - 'sum': Total vehicles from each region in each period (default)
                           - 'mean': Average vehicle count per region in each period
                           - 'max': Peak vehicle count per region in each period
                           - 'min': Minimum vehicle count per region in each period
                           - 'drop': Just remove intermediate points (keep first)
        time_range: Optional time window to limit analysis (e.g., timedelta(days=30))
                   Data will be filtered to this range from the most recent date

    Returns:
        DataFrame with columns:
        - relative_time: Data collection timestamp
        - vehicle_count: Number of vehicles from this region at that time
        - region: Region identifier (based on license plate patterns)

    Raises:
        AssertionError: If queue_name is invalid

    Example:
        >>> df = get_count_by_regions('carLiveQueue', floor_value='d', 
                                     aggregation_method='sum')
        >>> print(df.head())
    """
    assert check_single_queue_name(queue_name), f'incorrect queue_name: {queue_name}'

    read_filters = filters if filters is not None else []
    read_filters.append((ct.QUEUE_POS_COLUMN, '!=', np.nan))
    
    # Add time range filter if specified
    if time_range is not None:
        cutoff_date = datetime.now() - time_range
        read_filters.append((ct.LOAD_DATE_COLUMN, '>=', cutoff_date))
    
    queue_df = list(read_from_parquet(
        queue_name,
        filters=read_filters,
        parquet_storage_path=ct.PARQUET_STORAGE_PATH,
        in_batches=False,
        columns=[ct.CAR_NUMBER_COLUMN, ct.LOAD_DATE_COLUMN]
    ))[0]
    
    if queue_df is None or len(queue_df) == 0:
        return pd.DataFrame(columns=['relative_time', 'vehicle_count', 'region'])
    
    # Extract region from license plate
    queue_df['region'] = queue_df[ct.CAR_NUMBER_COLUMN].apply(
        lambda cn: cn[-1] if ct.BELARUS_CAR_NUMBER_FORMAT.match(cn) else None
    )
    # TODO: correct aggregation if floor_value is not None - drop with grouping by car number, then aggregate
    # Apply time aggregation if specified
    if floor_value is not None:
        
        # Remove duplicates first, then aggregate
        queue_df = queue_df.drop_duplicates([ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN])
        queue_df = apply_datetime_aggregation(
            df=queue_df,
            time_column=ct.LOAD_DATE_COLUMN,
            floor_value=floor_value,
            aggregation_method='drop',  # Just floor the time, don't aggregate counts yet
            group_columns=['region'],
            value_columns={ct.CAR_NUMBER_COLUMN: 'count'}
        )
        
        # Now group by time and region to get counts
        queue_df = queue_df \
            .groupby(by=[ct.LOAD_DATE_COLUMN, 'region']) \
            .aggregate({ct.CAR_NUMBER_COLUMN: aggregation_method}) \
            .reset_index()
    else:
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
    Returns DataFrame with vehicles grouped by registration counts.

    Args:
        queue_name: str - queue which will be stored into the dataframe
        filters: Optional[List[str]] - parquet data filters
        has_been_called: bool - True means selecting only vehicles registrations, which were successfully completed
    
    Returns:
        DataFrame with columns:
        - count_of_registrations: Number of registration of a vehicle in queue
        - vehicle_count: Number of ordered vehicles in queue at a specific load date

    Raises:
        AssertionError: If queue_name is invalid

    Example:
        >>> df = get_single_vehicle_registrations_count('carLiveQueue', has_been_called=True)
        >>> print(df.head())
    """
    assert check_single_queue_name(queue_name) is True, f'incorrect queue_name: {queue_name}'
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
        queue_df = queue_df \
            .groupby([ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN]) \
            .apply(lambda gr: pd.Series({'is_canceled': any(gr[ct.STATUS_COLUMN] == 9)})).reset_index()
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


def get_called_vehicles_waiting_time(queues_names: tp.List[str],
                                     filters: tp.Optional[tp.List] = None,
                                     aggregation_type: str = 'min',
                                     floor_value: tp.Optional[str] = None,
                                     time_range: tp.Optional[timedelta] = None) -> pd.DataFrame:
    """
    Returns DataFrame with waiting minutes for vehicles after being called to the border.

    This function analyzes the time vehicles wait after receiving a "called" status
    until they are removed from the queue (presumably processed at the border).

    Args:
        queues_names: List of queue names to analyze. Must be valid queue keys
                     from constants (e.g., ['carLiveQueue', 'busLiveQueue'])
        filters: Optional parquet filters to limit data scope. Format: [(column, operator, value)]
                Example: [('load_date', '>=', datetime(2024, 1, 1))]
        aggregation_type: How to aggregate waiting times for vehicles called at the same time:
                         - 'min': Shortest waiting time (default, most optimistic)
                         - 'max': Longest waiting time (worst case scenario)
                         - 'mean': Average waiting time (typical experience)
        floor_value: Time aggregation period for grouping data points:
                    - None: All individual data points (no aggregation)
                    - '5min': 5-minute intervals
                    - 'h': Hourly aggregation
                    - 'd': Daily aggregation
                    - 'M': Monthly aggregation
        time_range: Optional time window to limit analysis (e.g., timedelta(days=30))
                   Data will be filtered to this range from the most recent date

    Returns:
        DataFrame with columns:
        - relative_time: Vehicle call timestamp
        - waiting_after_called: Minutes waited after being called before queue removal
        - queue_name: Name of the queue for this data row

    Raises:
        AssertionError: If queues_names are invalid or aggregation_type not in {'max', 'min', 'mean'}

    Example:
        >>> df = get_called_vehicles_waiting_time(['carLiveQueue'], 
                                                 aggregation_type='mean', floor_value='h')
        >>> print(df.head())
    """
    def read_queue(qname):
        queue_pos_filter_expr = pc.field(ct.QUEUE_POS_COLUMN).is_null()
        
        read_filters = queue_pos_filter_expr
        if filters is not None:
            filters_expr = filters_to_expression(filters)
            read_filters = filters_expr & queue_pos_filter_expr
        
        # Add time range filter if specified
        if time_range is not None:
            cutoff_date = datetime.now() - time_range
            time_filter_expr = pc.field(ct.CHANGED_DATE_COLUMN) >= cutoff_date
            read_filters = read_filters & time_filter_expr
            
        queue_df = list(read_from_parquet(
            qname,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN, ct.STATUS_COLUMN,
                     ct.CHANGED_DATE_COLUMN, ct.LOAD_DATE_COLUMN]
        ))[0]
        
        if queue_df is None or len(queue_df) == 0:
            return pd.DataFrame(columns=['relative_time', 'waiting_after_called', 'queue_name'])
        
        # Apply time aggregation to datetime columns if specified
        if floor_value is not None:
            queue_df = apply_datetime_aggregation(
                df=queue_df,
                time_column=ct.CHANGED_DATE_COLUMN,
                floor_value=floor_value,
                aggregation_method='drop',  # Don't aggregate yet, just floor times
                group_columns=[ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN],
                value_columns={ct.LOAD_DATE_COLUMN: 'first', ct.STATUS_COLUMN: 'first'}
            )

        queue_df = queue_df \
            .groupby([ct.CAR_NUMBER_COLUMN, ct.REGISTRATION_DATE_COLUMN]) \
            .aggregate({
            ct.STATUS_COLUMN: lambda st: any(st == 9),
            ct.CHANGED_DATE_COLUMN: 'min',
            ct.LOAD_DATE_COLUMN: aggregation_type}) \
            .reset_index()

        queue_df = queue_df[queue_df[ct.STATUS_COLUMN] == False]
        queue_df = queue_df.groupby(ct.CHANGED_DATE_COLUMN).aggregate(
            {ct.LOAD_DATE_COLUMN: aggregation_type}).reset_index()
        queue_df['waiting_after_called'] = queue_df[ct.LOAD_DATE_COLUMN] - queue_df[ct.CHANGED_DATE_COLUMN]
        queue_df['waiting_after_called'] = queue_df['waiting_after_called'].apply(
            lambda mw: round(mw.total_seconds() / 60, 2))

        queue_df['queue_name'] = qname
        return queue_df.rename(columns={
            ct.CHANGED_DATE_COLUMN: 'relative_time'
        })[['relative_time', 'waiting_after_called', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    assert aggregation_type in {'max', 'min', 'mean'}, f"aggregation_type must be 'max', 'min', or 'mean', got {aggregation_type}"
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_number_of_declined_vehicles(queues_names: tp.List[str],
                                    filters: tp.Optional[tp.List] = None,
                                    floor_value: tp.Optional[str] = None,
                                    aggregation_method: str = 'sum',
                                    time_range: tp.Optional[timedelta] = None) -> pd.DataFrame:
    """
    Returns DataFrame with number of declined vehicles over time.

    This function tracks vehicles that were declined/rejected from queues,
    helping understand rejection patterns and queue management efficiency.

    Args:
        queues_names: List of queue names to analyze. Must be valid queue keys
                     from constants (e.g., ['carLiveQueue', 'busLiveQueue'])
        filters: Optional parquet filters to limit data scope. Format: [(column, operator, value)]
                Example: [('load_date', '>=', datetime(2024, 1, 1))]
        floor_value: Time aggregation period for grouping data points:
                    - None: All individual data points (no aggregation)
                    - '5min': 5-minute intervals
                    - 'h': Hourly aggregation
                    - 'd': Daily aggregation
                    - 'M': Monthly aggregation
        aggregation_method: How to combine declined vehicle counts within each time bucket:
                           - 'sum': Total declined vehicles in each period (default)
                           - 'mean': Average declined vehicles per time unit
                           - 'max': Peak declined vehicles in each period
                           - 'min': Minimum declined vehicles in each period
                           - 'drop': Just remove intermediate points (keep first)
        time_range: Optional time window to limit analysis (e.g., timedelta(days=30))
                   Data will be filtered to this range from the most recent date

    Returns:
        DataFrame with columns:
        - relative_time: Data collection timestamp
        - vehicle_count: Number of declined vehicles at that time
        - queue_name: Name of the queue for this data row

    Raises:
        AssertionError: If queues_names are invalid

    Example:
        >>> df = get_number_of_declined_vehicles(['carLiveQueue', 'busLiveQueue'], 
                                               floor_value='d', aggregation_method='sum')
        >>> print(df.head())
    """
    def read_queue(qname):
        vehicle_type_filter_expr = pc.field(ct.STATUS_COLUMN) == 9

        read_filters = vehicle_type_filter_expr
        if filters is not None:
            filters_expr = filters_to_expression(filters)
            read_filters = filters_expr & vehicle_type_filter_expr
        
        # Add time range filter if specified
        if time_range is not None:
            cutoff_date = datetime.now() - time_range
            time_filter_expr = pc.field(ct.LOAD_DATE_COLUMN) >= cutoff_date
            read_filters = read_filters & time_filter_expr
            
        queue_df = list(read_from_parquet(
            qname,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN]
        ))[0]
        
        if queue_df is None or len(queue_df) == 0:
            return pd.DataFrame(columns=['relative_time', 'vehicle_count', 'queue_name'])
        
        # Remove duplicates and count by load date
        queue_df = queue_df.drop_duplicates([ct.LOAD_DATE_COLUMN, ct.CAR_NUMBER_COLUMN])
        queue_df['queue_name'] = qname
        
        # Apply time aggregation if specified
        if floor_value is not None:
            queue_df = apply_datetime_aggregation(
                df=queue_df,
                time_column=ct.LOAD_DATE_COLUMN,
                floor_value=floor_value,
                aggregation_method='drop',  # Don't aggregate yet, just floor times
                group_columns=['queue_name'],
                value_columns={ct.CAR_NUMBER_COLUMN: 'count'}
            )
            
            # Now group and aggregate
            queue_df = queue_df \
                .groupby([ct.LOAD_DATE_COLUMN, 'queue_name']) \
                .aggregate({ct.CAR_NUMBER_COLUMN: aggregation_method}) \
                .reset_index()
        else:
            queue_df = queue_df \
                .groupby([ct.LOAD_DATE_COLUMN, 'queue_name']) \
                .count().reset_index()
        
        return queue_df.rename(columns={
            ct.LOAD_DATE_COLUMN: 'relative_time',
            ct.CAR_NUMBER_COLUMN: 'vehicle_count'
        })[['relative_time', 'vehicle_count', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_registered_count(queues_names: tp.List[str],
                         filters: tp.Optional[tp.List] = None,
                         floor_value: str = 'h') -> pd.DataFrame:
    """
    Returns DataFrame with number of registered vehicles.

    Args:
        queues_names: List[str] - queues which will be stored into the dataframe
        filters: Optional[List[str]] - parquet data filters
        floor_value: str - sets larger buckets for counting

    Notes:
        The trick is in column selection and dropping duplicates. When dropping duplicates by
        car number and registration date, we get the number of registered vehicles at a specific date.

    Returns:
        DataFrame with columns:
        - relative_time: Registered date
        - vehicle_count: Number of registered vehicles
        - queue_name: Name of the queue for the specific row
    """

    def read_queue(qname):
        vehicle_type_filter_expr = pc.field(ct.STATUS_COLUMN) == 2

        if filters is not None:
            read_filters = filters_to_expression(filters)
            read_filters = read_filters & vehicle_type_filter_expr
        else:
            read_filters = vehicle_type_filter_expr
        queue_df = list(read_from_parquet(
            qname,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.REGISTRATION_DATE_COLUMN, ct.CAR_NUMBER_COLUMN]
        ))[0]
        queue_df = queue_df.drop_duplicates()
        queue_df[ct.REGISTRATION_DATE_COLUMN] = queue_df[ct.REGISTRATION_DATE_COLUMN] \
            .apply(lambda t: t.floor(floor_value))

        queue_df = queue_df \
            .groupby(ct.REGISTRATION_DATE_COLUMN) \
            .count().reset_index()
        queue_df['queue_name'] = qname
        return queue_df.rename(columns={
            ct.REGISTRATION_DATE_COLUMN: 'relative_time',
            ct.CAR_NUMBER_COLUMN: 'vehicle_count'
        })[['relative_time', 'vehicle_count', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)


def get_called_count(queues_names: tp.List[str],
                     filters: tp.Optional[tp.List] = None,
                     floor_value: str = 'h') -> pd.DataFrame:
    """
    Returns DataFrame with number of called vehicles (time when vehicle status changed to 'called').

    :param queues_names: List[str] - queues which will be stored into the dataframe
    :param filters: Optional[List[str]] - parquet data filters
    :param floor_value: str - sets larger buckets for counting
    :return:
        DataFrame columns:
        (1) relative_time - changed status date
        (2) vehicle_count - number of called vehicles
        (3) queue_name - name of the queue for the specific row
    """

    def read_queue(qname):
        vehicle_type_filter_expr = pc.field(ct.STATUS_COLUMN) == 3

        if filters is not None:
            read_filters = filters_to_expression(filters)
            read_filters = read_filters & vehicle_type_filter_expr
        else:
            read_filters = vehicle_type_filter_expr
        queue_df = list(read_from_parquet(
            qname,
            filters=read_filters,
            parquet_storage_path=ct.PARQUET_STORAGE_PATH,
            in_batches=False,
            columns=[ct.CHANGED_DATE_COLUMN, ct.CAR_NUMBER_COLUMN]
        ))[0]
        queue_df = queue_df.drop_duplicates()
        queue_df[ct.CHANGED_DATE_COLUMN] = queue_df[ct.CHANGED_DATE_COLUMN] \
            .apply(lambda t: t.floor(floor_value))

        queue_df = queue_df \
            .groupby(ct.CHANGED_DATE_COLUMN) \
            .count().reset_index()
        queue_df['queue_name'] = qname
        return queue_df.rename(columns={
            ct.CHANGED_DATE_COLUMN: 'relative_time',
            ct.CAR_NUMBER_COLUMN: 'vehicle_count'
        })[['relative_time', 'vehicle_count', 'queue_name']].sort_values('relative_time')

    check_queue_names(queues_names)
    return pd.concat([read_queue(qname) for qname in queues_names], axis=0)
