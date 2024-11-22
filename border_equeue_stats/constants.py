EQUEUE_JSON_PATH = "https://belarusborder.by/info/" \
                   "monitoring-new?token=test&checkpointId=a9173a85-3fc0-424c-84f0-defa632481e4"

JSON_STORAGE_PATH = 'data/brest_border_equeue.txt'
PARQUET_STORAGE_PATH = 'data/parquet_dataset'

##################################################################################################
# EqueueData
##################################################################################################
EQUEUE_COLUMNS = ['year', 'month', 'load_dt', 'car_number', 'status', 'queue_pos',
                  'queue_type', 'reg_date', 'changed_date']

##################################################################################################
# Equeue fields
##################################################################################################
INFO_KEY = 'info'

TRUCK_LIVE_QUEUE_KEY = 'truckLiveQueue'
TRUCK_PRIORITY_KEY = 'truckPriority'
TRUCK_GPK_KEY = 'truckGpk'
BUS_LIVE_QUEUE_KEY = 'busLiveQueue'
BUS_PRIORITY_KEY = 'busPriority'
CAR_LIVE_QUEUE_KEY = 'carLiveQueue'
CAR_PRIORITY_KEY = 'carPriority'
MOTORCYCLE_LIVE_QUEUE_KEY = 'motorcycleLiveQueue'
MOTORCYCLE_PRIORITY_KEY = 'motorcyclePriority'

ALL_EQUEUE_KEYS = [INFO_KEY, TRUCK_LIVE_QUEUE_KEY, TRUCK_PRIORITY_KEY, TRUCK_GPK_KEY, BUS_LIVE_QUEUE_KEY,
                   BUS_PRIORITY_KEY, CAR_LIVE_QUEUE_KEY, CAR_PRIORITY_KEY, MOTORCYCLE_LIVE_QUEUE_KEY,
                   MOTORCYCLE_PRIORITY_KEY]
