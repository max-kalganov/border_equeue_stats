import json
import os
import shutil
import typing as tp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from datetime import datetime, timedelta

from border_equeue_stats import constants as ct
from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue


def apply_datetime_aggregation(df: pd.DataFrame,
                               time_column: str,
                               floor_value: tp.Optional[str] = None,
                               aggregation_method: str = 'mean',
                               group_columns: tp.Optional[tp.List[str]] = None,
                               value_columns: tp.Optional[tp.Dict[str, str]] = None) -> pd.DataFrame:
    """
    Apply datetime aggregation to a DataFrame with flexible aggregation methods.

    Args:
        df: Input DataFrame
        time_column: Name of the datetime column to aggregate by
        floor_value: Time aggregation period ('5min', 'h', 'd', 'M', None)
            - '5min': 5-minute intervals
            - 'h': hourly aggregation  
            - 'd': daily aggregation
            - 'M': monthly aggregation
            - None: no aggregation (return original data)
        aggregation_method: How to aggregate values ('mean', 'max', 'min', 'drop')
            - 'mean': calculate mean of values in each time bucket
            - 'max': take maximum value in each time bucket
            - 'min': take minimum value in each time bucket  
            - 'drop': just drop intermediate points (take first value)
        group_columns: Additional columns to group by (e.g., ['queue_name'])
        value_columns: Dict mapping column names to aggregation methods
                      e.g., {'hours_waited': 'mean', 'vehicle_count': 'max'}

    Returns:
        Aggregated DataFrame
    """
    if df is None or len(df) == 0:
        return df

    if floor_value is None:
        return df

    # Make a copy to avoid modifying original
    result_df = df.copy().sort_values(time_column).reset_index(drop=True)

    # Apply time floor
    result_df[time_column] = result_df[time_column].dt.floor(floor_value)

    # Prepare grouping columns
    group_cols = [time_column]
    if group_columns:
        group_cols.extend(group_columns)

    # Handle different aggregation methods
    if aggregation_method == 'drop':
        # Just drop duplicates, keeping first occurrence
        result_df = result_df.drop_duplicates(subset=group_cols, keep='first')
    else:
        # Prepare aggregation dictionary
        if value_columns:
            agg_dict = value_columns.copy()
        else:
            # Default: aggregate all numeric columns with the specified method
            numeric_cols = result_df.select_dtypes(include=['number']).columns
            agg_dict = {col: aggregation_method for col in numeric_cols}

        # Handle non-numeric columns (take first value)
        non_numeric_cols = []
        for col in result_df.columns:
            if col not in group_cols \
                    and col not in agg_dict \
                    and (result_df[col].dtype == 'object'
                         or pd.api.types.is_categorical_dtype(result_df[col])):
                non_numeric_cols.append(col)
                agg_dict[col] = 'first'

        # Apply aggregation
        if agg_dict:
            result_df = result_df.groupby(
                group_cols).agg(agg_dict).reset_index()

    return result_df


# TODO: move to constants

def get_recommended_time_ranges(floor_value: tp.Optional[str]) -> tp.Dict[str, timedelta]:
    """
    Get recommended time ranges for different aggregation periods.

    Args:
        floor_value: Time aggregation period ('5min', 'h', 'd', 'M', None)

    Returns:
        Dictionary with time range options and their timedelta values
    """
    if floor_value == '5min':
        return {
            "📅 Last 3 Days": timedelta(days=3),
            "📅 Last Week": timedelta(days=7),
            "📅 Last 2 Weeks": timedelta(days=14),
        }
    elif floor_value == 'h':
        return {
            "📅 Last Week": timedelta(days=7),
            "📅 Last Month": timedelta(days=30),
            "📅 Last 3 Months": timedelta(days=90),
        }
    elif floor_value == 'd':
        return {
            "📅 Last Month": timedelta(days=30),
            "📅 Last 3 Months": timedelta(days=90),
            "📅 Last 6 Months": timedelta(days=180),
            "📅 Last Year": timedelta(days=365),
        }
    elif floor_value == 'M':
        return {
            "📅 Last Year": timedelta(days=365),
            "📅 Last 2 Years": timedelta(days=730),
            "📅 Last 3 Years": timedelta(days=1095),
        }
    else:  # None
        return {
            "📅 Last Day": timedelta(days=1),
            "📅 Last 3 Days": timedelta(days=3),
            "📅 Last Week": timedelta(days=7),
        }


def read_from_parquet(name, filters: tp.Optional = None, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH,
                      in_batches: bool = False, columns=None, **batching_kwargs) -> tp.Iterable[tp.Optional[pd.DataFrame]]:
    data_dir = os.path.join(parquet_storage_path, name)
    os.makedirs(data_dir, exist_ok=True)
    dataset = pq.ParquetDataset(data_dir, filters=filters)
    if len(dataset.files) > 0:
        if in_batches:
            for pq_file_name in dataset.files:
                pq_file = pq.ParquetFile(pq_file_name)
                # TODO: partitioning keys are not added even with explicit columns -> find a way to add them.
                # if 'columns' not in batching_kwargs:
                #     batching_kwargs['columns'] = ct.EQUEUE_COLUMNS
                for batch in pq_file.iter_batches(columns=columns, **batching_kwargs):
                    batch_df = batch.to_pandas()
                    batch_df[ct.MONTH_COLUMN] = batch_df[ct.LOAD_DATE_COLUMN].apply(lambda l: l.month)
                    batch_df[ct.YEAR_COLUMN] = batch_df[ct.LOAD_DATE_COLUMN].apply(lambda l: l.year)
                    yield batch_df
        else:
            yield dataset.read(columns=columns).to_pandas()
    else:
        yield None


def read_all_from_parquet(filters: tp.Optional = None,
                          parquet_storage_path: str = ct.PARQUET_STORAGE_PATH,
                          apply_filter_to_info: bool = False) -> EqueueData:
    return EqueueData(
        info=list(read_from_parquet(ct.INFO_KEY, parquet_storage_path=parquet_storage_path,
                                    filters=filters if apply_filter_to_info else None, in_batches=False))[0],
        truck_queue=list(read_from_parquet(name=ct.TRUCK_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                           filters=filters, in_batches=False))[0],
        truck_priority=list(read_from_parquet(name=ct.TRUCK_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                              filters=filters, in_batches=False))[0],
        truck_gpk=list(read_from_parquet(name=ct.TRUCK_GPK_KEY, parquet_storage_path=parquet_storage_path,
                                         filters=filters, in_batches=False))[0],
        bus_queue=list(read_from_parquet(name=ct.BUS_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                         filters=filters, in_batches=False))[0],
        bus_priority=list(read_from_parquet(name=ct.BUS_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                            filters=filters, in_batches=False))[0],
        car_queue=list(read_from_parquet(name=ct.CAR_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                         filters=filters, in_batches=False))[0],
        car_priority=list(read_from_parquet(name=ct.CAR_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                            filters=filters, in_batches=False))[0],
        motorcycle_queue=list(read_from_parquet(name=ct.MOTORCYCLE_LIVE_QUEUE_KEY,
                                                parquet_storage_path=parquet_storage_path,
                                                filters=filters, in_batches=False))[0],
        motorcycle_priority=list(read_from_parquet(name=ct.MOTORCYCLE_PRIORITY_KEY,
                                                   parquet_storage_path=parquet_storage_path,
                                                   filters=filters, in_batches=False))[0]
    )


def read_parquet_info_data(filter_hash: tp.Optional[str] = None,
                           parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> tp.Optional[pd.DataFrame]:
    filters = None if filter_hash is None else [(ct.INFO_HASH_COLUMN, '==', filter_hash)]
    df = list(read_from_parquet(name=ct.INFO_KEY,
                                parquet_storage_path=parquet_storage_path,
                                filters=filters,
                                in_batches=False))[0]
    return df if df is not None and len(df) > 0 else None


def is_info_stored(filter_hash: str, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> bool:
    stored_info_df = read_parquet_info_data(filter_hash, parquet_storage_path=parquet_storage_path)
    return stored_info_df is not None and len(stored_info_df) > 0


def file_visitor(written_file):
    print(f"path={written_file.path}")
    print(f"size={written_file.size} bytes")
    print(f"metadata={written_file.metadata}")


def dump_to_parquet(data: dict, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> None:
    def dump_single_df(df: tp.Optional[pd.DataFrame], name: str):
        if df is not None and len(df) > 0:
            # TODO: check None values in queue_pos column
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=os.path.join(parquet_storage_path, name),
                                partition_cols=ct.PARTITION_COLUMNS, file_visitor=file_visitor)
        else:
            print(f"Skipping empty {name}..")

    def dump_info(info: tp.Union[pd.Series, pd.DataFrame]):
        if isinstance(info, pd.Series):
            info = info.to_frame().T
        info['is_stored'] = info[ct.INFO_HASH_COLUMN].apply(lambda h: is_info_stored(
            filter_hash=h,
            parquet_storage_path=parquet_storage_path
        ))
        not_stored_info = info[~info['is_stored']].drop('is_stored', axis=1)
        dump_single_df(not_stored_info, name=ct.INFO_KEY)

    single_equeue_dataframes = convert_to_pandas_equeue(data)

    dump_info(single_equeue_dataframes.info)
    dump_single_df(single_equeue_dataframes.truck_queue, ct.TRUCK_LIVE_QUEUE_KEY)
    dump_single_df(single_equeue_dataframes.truck_priority, ct.TRUCK_PRIORITY_KEY)
    dump_single_df(single_equeue_dataframes.truck_gpk, ct.TRUCK_GPK_KEY)
    dump_single_df(single_equeue_dataframes.bus_queue, ct.BUS_LIVE_QUEUE_KEY)
    dump_single_df(single_equeue_dataframes.bus_priority, ct.BUS_PRIORITY_KEY)
    dump_single_df(single_equeue_dataframes.car_queue, ct.CAR_LIVE_QUEUE_KEY)
    dump_single_df(single_equeue_dataframes.car_priority, ct.CAR_PRIORITY_KEY)
    dump_single_df(single_equeue_dataframes.motorcycle_queue, ct.MOTORCYCLE_LIVE_QUEUE_KEY)
    dump_single_df(single_equeue_dataframes.motorcycle_priority, ct.MOTORCYCLE_PRIORITY_KEY)


def dump_all_stored_json_to_parquet(json_storage_path: str = ct.JSON_STORAGE_PATH,
                                    parquet_storage_path: str = ct.PARQUET_STORAGE_PATH):
    with open(json_storage_path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', 'null'))
        dump_to_parquet(line_dict, parquet_storage_path=parquet_storage_path)


def coalesce_parquet_data(parquet_storage_path: str = ct.PARQUET_STORAGE_PATH):
    def has_many_files(dt, path: str):
        all_files = dt.files
        unq_partitions = set()
        has_unq_partitions = True
        for f in all_files:
            assert f.startswith(path)
            partition = os.path.dirname(f)[len(path):].strip(os.sep)
            if partition in unq_partitions:
                has_unq_partitions = False
                break
            unq_partitions.add(partition)
        return not has_unq_partitions

    tmp_parquet_storage_path = parquet_storage_path.rstrip('/') + '_tmp'
    os.makedirs(tmp_parquet_storage_path, exist_ok=True)

    for name in tqdm(ct.ALL_EQUEUE_KEYS, desc='Processing equeue folders'):
        cur_path = os.path.join(parquet_storage_path, name)
        if not os.path.exists(cur_path):
            continue
        dataset = pq.ParquetDataset(cur_path)
        if not has_many_files(dataset, cur_path):
            shutil.move(cur_path, os.path.join(tmp_parquet_storage_path, name))
        else:
            df = list(read_from_parquet(name=name, parquet_storage_path=parquet_storage_path, in_batches=False))[0]
            if df is not None:
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=os.path.join(tmp_parquet_storage_path, name),
                                    partition_cols=ct.PARTITION_COLUMNS, file_visitor=file_visitor)

    shutil.rmtree(parquet_storage_path)
    # os.rename(parquet_storage_path, parquet_storage_path + '_backup')
    os.rename(tmp_parquet_storage_path, parquet_storage_path)
