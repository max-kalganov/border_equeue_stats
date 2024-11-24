import os
import time
from datetime import datetime

from border_equeue_stats.constants import EQUEUE_JSON_PATH
from border_equeue_stats.data_storage.parquet_storage import dump_to_parquet
from border_equeue_stats.equeue_parser import parse_equeue

if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    while True:
        print(f"Parsing .. {datetime.now()}", end='')
        dump_to_parquet(parse_equeue(url=EQUEUE_JSON_PATH))
        print(f"Complete! Waiting...")
        time.sleep(60 * 5)
