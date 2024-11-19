import json
import os
import typing as tp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from border_equeue_stats import constants as ct
from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue


def read_from_parquet(name, filters: tp.Optional = None, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH,
                      in_batches: bool = False, **batching_kwargs) -> pd.DataFrame:
    data_dir = os.path.join(parquet_storage_path, name)
    os.makedirs(data_dir, exist_ok=True)
    dataset = pq.ParquetDataset(data_dir, filters=filters)
    if in_batches:
        for pq_file_name in dataset.files:
            pq_file = pq.ParquetFile(pq_file_name)
            for batch in pq_file.iter_batches(**batching_kwargs):
                yield batch.to_pandas()
    else:
        yield dataset.read().to_pandas()


def read_all_from_parquet(filters: tp.Optional = None,
                          parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> EqueueData:
    return EqueueData(
        info=read_from_parquet(ct.INFO_KEY, parquet_storage_path=parquet_storage_path,
                               filters=filters, in_batches=False),
        truck_queue=read_from_parquet(name=ct.TRUCK_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                      filters=filters, in_batches=False),
        truck_priority=read_from_parquet(name=ct.TRUCK_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                         filters=filters, in_batches=False),
        truck_gpk=read_from_parquet(name=ct.TRUCK_GPK_KEY, parquet_storage_path=parquet_storage_path,
                                    filters=filters, in_batches=False),
        bus_queue=read_from_parquet(name=ct.BUS_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                    filters=filters, in_batches=False),
        bus_priority=read_from_parquet(name=ct.BUS_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                       filters=filters, in_batches=False),
        car_queue=read_from_parquet(name=ct.CAR_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                    filters=filters, in_batches=False),
        car_priority=read_from_parquet(name=ct.CAR_PRIORITY_KEY, parquet_storage_path=parquet_storage_path,
                                       filters=filters, in_batches=False),
        motorcycle_queue=read_from_parquet(name=ct.MOTORCYCLE_LIVE_QUEUE_KEY, parquet_storage_path=parquet_storage_path,
                                           filters=filters, in_batches=False),
        motorcycle_priority=read_from_parquet(name=ct.MOTORCYCLE_PRIORITY_KEY,
                                              parquet_storage_path=parquet_storage_path,
                                              filters=filters, in_batches=False)
    )


def read_parquet_info_data(filter_hash: tp.Optional[str] = None,
                           parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> tp.Optional[pd.DataFrame]:
    filters = None if filter_hash is None else [('hash', '==', filter_hash)]
    df = read_from_parquet(name=ct.INFO_KEY, parquet_storage_path=parquet_storage_path, filters=filters)
    return df if len(df) > 0 else None


def is_info_stored(filter_hash: str, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> bool:
    stored_info_df = read_parquet_info_data(filter_hash, parquet_storage_path=parquet_storage_path)
    return stored_info_df is not None and len(stored_info_df) > 0


def dump_to_parquet(data: dict, parquet_storage_path: str = ct.PARQUET_STORAGE_PATH) -> None:
    def file_visitor(written_file):
        print(f"path={written_file.path}")
        print(f"size={written_file.size} bytes")
        print(f"metadata={written_file.metadata}")

    def dump_single_df(df: tp.Optional[pd.DataFrame], name: str):
        if df is not None and len(df) > 0:
            # TODO: check None values in queue_pos column
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=os.path.join(parquet_storage_path, name),
                                partition_cols=['year', 'month'], file_visitor=file_visitor)
        else:
            print(f"Skipping empty {name}..")

    def dump_info(info: tp.Union[pd.Series, pd.DataFrame]):
        if isinstance(info, pd.Series):
            info = info.to_frame().T
        info['hash'] = info[['id', 'name', 'address', 'phone', 'is_brest', 'name_ru']] \
            .apply(lambda r: hash('_'.join(map(str, r.values))), axis=1)
        info['is_stored'] = info['hash'].apply(lambda h: is_info_stored(filter_hash=h,
                                                                        parquet_storage_path=parquet_storage_path))
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


def dump_all_stored_json_to_parquet(json_storage_path: str = ct.JSON_STORAGE_PATH):
    with open(json_storage_path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', '"None"'))
        dump_to_parquet(line_dict)
