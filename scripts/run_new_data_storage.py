from border_equeue_stats.constants import EQUEUE_JSON_PATH
from border_equeue_stats.equeue_parser import parse_equeue
from border_equeue_stats.data_storage.json_storage import *
from border_equeue_stats.data_storage.parquet_storage import *


if __name__ == '__main__':
    # record = parse_equeue(url=EQUEUE_JSON_PATH)
    record = read_from_json()

    print(record)
