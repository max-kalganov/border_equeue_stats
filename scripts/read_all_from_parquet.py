import time
from datetime import datetime, timedelta

from border_equeue_stats.data_storage.parquet_storage import read_all_from_parquet

if __name__ == '__main__':
    start_time = time.time()
    res = read_all_from_parquet()
    end_time = time.time()
    print((end_time - start_time))
    print(f"{res.car_queue.shape=}, {res.bus_queue.shape=}")

    print(res.car_queue[res.car_queue['load_dt'] > '2024-11-22'].sample(5).to_string())