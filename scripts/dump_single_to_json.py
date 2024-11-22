import time

from border_equeue_stats.constants import EQUEUE_JSON_PATH
from border_equeue_stats.data_storage.json_storage import dump_to_json
from border_equeue_stats.equeue_parser import parse_equeue

if __name__ == '__main__':
    start_time = time.time()
    dump_to_json(parse_equeue(url=EQUEUE_JSON_PATH))
    end_time = time.time()
    print((end_time - start_time))
