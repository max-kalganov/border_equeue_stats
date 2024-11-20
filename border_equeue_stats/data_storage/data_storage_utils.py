import typing as tp

import pandas as pd
import requests
import json
from datetime import datetime

from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats import constants as ct


def parse_equeue(url: str) -> dict:
    results = requests.get(url)
    data = json.loads(results.text)
    data['datetime'] = str(datetime.now())
    return data


def convert_equeue_entity_to_pandas(equeue_entity: tp.Dict, load_dt: datetime) -> pd.Series:
    return pd.Series({
        'year': load_dt.year,
        'month': load_dt.month,
        # 'week': load_dt.,
        'load_dt': load_dt,
        'car_number': equeue_entity['regnum'],
        'status': equeue_entity['status'],
        'queue_pos': equeue_entity['order_id'],
        'queue_type': equeue_entity['type_queue'],
        'reg_date': datetime.strptime(equeue_entity['registration_date'], '%H:%M:%S %d.%m.%Y'),
        'changed_date': datetime.strptime(equeue_entity['changed_date'], '%H:%M:%S %d.%m.%Y')
    })


def convert_equeue_info_to_pandas(equeue_info: tp.Dict, load_dt: datetime) -> pd.Series:
    return pd.Series({
        'year': load_dt.year,
        'month': load_dt.month,
        'load_dt': load_dt,
        'id': equeue_info['id'],
        'name': equeue_info['nameEn'],
        'address': equeue_info['address'],
        'phone': equeue_info['phone'],
        'is_brest': equeue_info['isBts'],
        'name_ru': equeue_info['name'],
        'hash': hash('_'.join(map(str, [equeue_info['id'], equeue_info['nameEn'], equeue_info['address'],
                                        equeue_info['phone'], equeue_info['isBts'], equeue_info['name']])))
    })


def convert_to_pandas_equeue(equeue_snapshot_dict: tp.Dict) -> EqueueData:
    def convert_and_group_entities(all_entities: tp.List) -> tp.Optional[pd.DataFrame]:
        grouped_entities = None
        if len(all_entities) > 0:
            grouped_entities = pd.concat([convert_equeue_entity_to_pandas(equeue_entity=entity, load_dt=load_dt)
                                          for entity in all_entities], axis=1).T
        return grouped_entities

    load_dt = datetime.strptime(equeue_snapshot_dict['datetime'], '%Y-%m-%d %H:%M:%S.%f')

    return EqueueData(
        info=convert_equeue_info_to_pandas(equeue_snapshot_dict[ct.INFO_KEY], load_dt),
        truck_queue=convert_and_group_entities(equeue_snapshot_dict[ct.TRUCK_LIVE_QUEUE_KEY]),
        truck_priority=convert_and_group_entities(equeue_snapshot_dict[ct.TRUCK_PRIORITY_KEY]),
        truck_gpk=convert_and_group_entities(equeue_snapshot_dict[ct.TRUCK_GPK_KEY]),
        bus_queue=convert_and_group_entities(equeue_snapshot_dict[ct.BUS_LIVE_QUEUE_KEY]),
        bus_priority=convert_and_group_entities(equeue_snapshot_dict[ct.BUS_PRIORITY_KEY]),
        car_queue=convert_and_group_entities(equeue_snapshot_dict[ct.CAR_LIVE_QUEUE_KEY]),
        car_priority=convert_and_group_entities(equeue_snapshot_dict[ct.CAR_PRIORITY_KEY]),
        motorcycle_queue=convert_and_group_entities(equeue_snapshot_dict[ct.MOTORCYCLE_LIVE_QUEUE_KEY]),
        motorcycle_priority=convert_and_group_entities(equeue_snapshot_dict[ct.MOTORCYCLE_PRIORITY_KEY]),
    )
