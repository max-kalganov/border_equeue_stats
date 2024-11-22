import time

from border_equeue_stats.data_storage.json_storage import read_from_json

if __name__ == '__main__':
    start_time = time.time()
    res = read_from_json()
    end_time = time.time()
    print((end_time - start_time))
    print(f"{res.car_queue.shape=}, {res.bus_queue.shape=}")
