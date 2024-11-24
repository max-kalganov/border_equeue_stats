EQUEUE_JSON_PATH = "https://belarusborder.by/info/" \
                   "monitoring-new?token=test&checkpointId=a9173a85-3fc0-424c-84f0-defa632481e4"

JSON_STORAGE_PATH = 'data/brest_border_equeue.txt'
PARQUET_STORAGE_PATH = 'data/parquet_dataset'

##################################################################################################
# EqueueData
##################################################################################################
YEAR_COLUMN = 'year'
MONTH_COLUMN = 'month'
LOAD_DATE_COLUMN = 'load_dt'
CAR_NUMBER_COLUMN = 'car_number'
STATUS_COLUMN = 'status'
QUEUE_POS_COLUMN = 'queue_pos'
QUEUE_TYPE_COLUMN = 'queue_type'
REGISTRATION_DATE_COLUMN = 'reg_date'
CHANGED_DATE_COLUMN = 'changed_date'

EQUEUE_COLUMNS = (YEAR_COLUMN, MONTH_COLUMN, LOAD_DATE_COLUMN, CAR_NUMBER_COLUMN, STATUS_COLUMN,
                  QUEUE_POS_COLUMN, QUEUE_TYPE_COLUMN, REGISTRATION_DATE_COLUMN, CHANGED_DATE_COLUMN)

PARTITION_COLUMNS = (YEAR_COLUMN, MONTH_COLUMN)

INFO_ID_COLUMN = 'id'
INFO_NAME_COLUMN = 'name'
INFO_ADDRESS_COLUMN = 'address'
INFO_PHONE_COLUMN = 'phone'
INFO_IS_BREST_COLUMN = 'is_brest'
INFO_NAME_RU_COLUMN = 'name_ru'
INFO_HASH_COLUMN = 'hash'

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

ALL_EQUEUE_KEYS = (INFO_KEY, TRUCK_LIVE_QUEUE_KEY, TRUCK_PRIORITY_KEY, TRUCK_GPK_KEY, BUS_LIVE_QUEUE_KEY,
                   BUS_PRIORITY_KEY, CAR_LIVE_QUEUE_KEY, CAR_PRIORITY_KEY, MOTORCYCLE_LIVE_QUEUE_KEY,
                   MOTORCYCLE_PRIORITY_KEY)
