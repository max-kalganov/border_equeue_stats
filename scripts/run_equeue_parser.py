import os
from datetime import datetime

from border_equeue_stats.equeue_parser import direct_parser

if __name__ == '__main__':
    os.makedirs('../data', exist_ok=True)
    import time
    while True:
        print(f"Parsing .. {datetime.now()}")
        direct_parser()
        time.sleep(60 * 5)
