import json
from collections import defaultdict

import pandas as pd
from border_equeue_stats.constants import JSON_STORAGE_PATH
from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue


def dump_to_json(data: dict) -> None:
    with open(JSON_STORAGE_PATH, 'a') as f:
        f.write(str(data) + '\n')


def read_from_json() -> EqueueData:
    def concat_all_dfs(key):
        dfs = [df for df in all_dfs[key] if df is not None]
        return pd.concat(dfs) if len(dfs) > 0 else None

    with open(JSON_STORAGE_PATH, 'r') as f:
        lines = f.readlines()

    infos = set()
    all_dfs = defaultdict(list)

    for line in lines:
        line_dict = json.loads(line.replace("'", '"').replace('None', '"None"'))
        single_equeue_dataframes = convert_to_pandas_equeue(line_dict)
        info_str = single_equeue_dataframes.info[['id', 'name', 'address', 'phone', 'is_brest', 'name_ru']].to_string()
        if info_str not in infos:
            infos.add(info_str)
            all_dfs['info'].append(single_equeue_dataframes.info)

        all_dfs['truck_queue'].append(single_equeue_dataframes.truck_queue)
        all_dfs['truck_priority'].append(single_equeue_dataframes.truck_priority)
        all_dfs['truck_gpk'].append(single_equeue_dataframes.truck_gpk)
        all_dfs['bus_queue'].append(single_equeue_dataframes.bus_queue)
        all_dfs['bus_priority'].append(single_equeue_dataframes.bus_priority)
        all_dfs['car_queue'].append(single_equeue_dataframes.car_queue)
        all_dfs['car_priority'].append(single_equeue_dataframes.car_priority)
        all_dfs['motorcycle_queue'].append(single_equeue_dataframes.motorcycle_queue)
        all_dfs['motorcycle_priority'].append(single_equeue_dataframes.motorcycle_priority)
    return EqueueData(
        info=pd.concat(all_dfs['info'], axis=1).T,
        truck_queue=concat_all_dfs('truck_queue'),
        truck_priority=concat_all_dfs('truck_priority'),
        truck_gpk=concat_all_dfs('truck_gpk'),
        bus_queue=concat_all_dfs('bus_queue'),
        bus_priority=concat_all_dfs('bus_priority'),
        car_queue=concat_all_dfs('car_queue'),
        car_priority=concat_all_dfs('car_priority'),
        motorcycle_queue=concat_all_dfs('motorcycle_queue'),
        motorcycle_priority=concat_all_dfs('motorcycle_priority'),
    )
