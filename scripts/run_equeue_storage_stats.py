from datetime import datetime

from border_equeue_stats import constants as ct
from border_equeue_stats.data_storage.storage_stats import *


if __name__ == '__main__':
    print(f"Car dataset files count: {len(get_files(queue_name=ct.CAR_LIVE_QUEUE_KEY))}")
    print(f"All dataset files count: {len(get_files(queue_name=None))}")
    print(f"Car dataset files size: {get_files_size(queue_name=ct.CAR_LIVE_QUEUE_KEY):,d} bytes")
    print(f"All dataset files size: {get_files_size(queue_name=None):,d} bytes")
