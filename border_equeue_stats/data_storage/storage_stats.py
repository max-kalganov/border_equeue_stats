import os
import typing as tp
from itertools import chain

import pyarrow.parquet as pq

from border_equeue_stats import constants as ct


def get_files(queue_name: tp.Optional[str] = None) -> tp.List[str]:
    """Returns queue dataset files

    :param queue_name: Optional[str] - queue name. If queue_name is None, returns files for all datasets
    :return file names
    """
    def get_dataset_files(qname) -> tp.List[str]:
        data_dir = os.path.join(ct.PARQUET_STORAGE_PATH, qname)
        os.makedirs(data_dir, exist_ok=True)
        dataset = pq.ParquetDataset(data_dir)
        return dataset.files
    queues = [queue_name] if queue_name is not None else ct.ALL_EQUEUE_KEYS
    return list(chain(*[get_dataset_files(q) for q in queues]))


def get_files_size(queue_name: tp.Optional[str] = None) -> int:
    """Returns total dataset files size in bytes.

    :param queue_name: Optional[str] - queue name. If queue_name is None, returns size of all datasets
    :return size in bytes
    """
    return sum(os.path.getsize(dataset_file) for dataset_file in get_files(queue_name))









