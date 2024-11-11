import json
import os
import typing as tp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from border_equeue_stats.constants import JSON_STORAGE_PATH, PARQUET_STORAGE_PATH
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue


def dump_all_stored_json_to_parquet():
    with open(JSON_STORAGE_PATH, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', '"None"'))
        dump_to_parquet(line_dict)


def read_parquet_info_data(filter_hash: tp.Optional[str] = None) -> tp.Optional[pd.DataFrame]:
    os.makedirs(os.path.join(PARQUET_STORAGE_PATH, 'info'), exist_ok=True)
    filters = None if filter_hash is None else [('hash', '==', filter_hash)]
    info_dataset = pq.ParquetDataset(os.path.join(PARQUET_STORAGE_PATH, 'info'), filters=filters)
    return info_dataset.read().to_pandas() if len(info_dataset.files) > 0 else None


def is_info_stored(filter_hash: str) -> bool:
    stored_info_df = read_parquet_info_data(filter_hash)
    return stored_info_df is not None and len(stored_info_df) > 0


def dump_to_parquet(data: dict) -> None:
    def file_visitor(written_file):
        print(f"path={written_file.path}")
        print(f"size={written_file.size} bytes")
        print(f"metadata={written_file.metadata}")

    def dump_single_df(df: tp.Optional[pd.DataFrame], name: str):
        if df is not None and len(df) > 0:
            # TODO: check None values in queue_pos column
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=os.path.join(PARQUET_STORAGE_PATH, name),
                                partition_cols=['year', 'month'], file_visitor=file_visitor)
        else:
            print(f"Skipping empty {name}..")

    def dump_info(info: tp.Union[pd.Series, pd.DataFrame]):
        if isinstance(info, pd.Series):
            info = info.to_frame().T
        info['hash'] = info[['id', 'name', 'address', 'phone', 'is_brest', 'name_ru']]\
            .apply(lambda r: hash('_'.join(map(str, r.values))), axis=1)
        info['is_stored'] = info['hash'].apply(is_info_stored)
        not_stored_info = info[~info['is_stored']].drop('is_stored', axis=1)
        dump_single_df(not_stored_info, name='info')

    single_equeue_dataframes = convert_to_pandas_equeue(data)

    dump_info(single_equeue_dataframes.info)
    dump_single_df(single_equeue_dataframes.truck_queue, 'truck_queue')
    dump_single_df(single_equeue_dataframes.truck_priority, 'truck_priority')
    dump_single_df(single_equeue_dataframes.truck_gpk, 'truck_gpk')
    dump_single_df(single_equeue_dataframes.bus_queue, 'bus_queue')
    dump_single_df(single_equeue_dataframes.bus_priority, 'bus_priority')
    dump_single_df(single_equeue_dataframes.car_queue, 'car_queue')
    dump_single_df(single_equeue_dataframes.car_priority, 'car_priority')
    dump_single_df(single_equeue_dataframes.motorcycle_queue, 'motorcycle_queue')
    dump_single_df(single_equeue_dataframes.motorcycle_priority, 'motorcycle_priority')


def read_from_parquet(name) -> pd.DataFrame:
    return pq.ParquetDataset(os.path.join(PARQUET_STORAGE_PATH), name).read_pandas()


