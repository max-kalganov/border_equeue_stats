import typing as tp

import pandas as pd
from datetime import datetime

from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats import constants as ct


def convert_equeue_entity_to_pandas(equeue_entity: tp.Dict, load_dt: datetime) -> pd.Series:
    equeue_data = {
        ct.YEAR_COLUMN: load_dt.year,
        ct.MONTH_COLUMN: load_dt.month,
        ct.LOAD_DATE_COLUMN: load_dt,
        ct.CAR_NUMBER_COLUMN: equeue_entity['regnum'],
        ct.STATUS_COLUMN: equeue_entity['status'],
        ct.QUEUE_POS_COLUMN: equeue_entity['order_id'],
        ct.QUEUE_TYPE_COLUMN: equeue_entity['type_queue'],
        ct.REGISTRATION_DATE_COLUMN: datetime.strptime(equeue_entity['registration_date'], '%H:%M:%S %d.%m.%Y'),
        ct.CHANGED_DATE_COLUMN: datetime.strptime(equeue_entity['changed_date'], '%H:%M:%S %d.%m.%Y')
    }
    assert sorted(equeue_data.keys()) == sorted(ct.EQUEUE_COLUMNS), 'not all columns are set'
    return pd.Series(equeue_data)


def convert_equeue_info_to_pandas(equeue_info: tp.Dict, load_dt: datetime) -> pd.Series:
    return pd.Series({
        ct.YEAR_COLUMN: load_dt.year,
        ct.MONTH_COLUMN: load_dt.month,
        ct.LOAD_DATE_COLUMN: load_dt,
        ct.INFO_ID_COLUMN: equeue_info['id'],
        ct.INFO_NAME_COLUMN: equeue_info['nameEn'],
        ct.INFO_ADDRESS_COLUMN: equeue_info['address'],
        ct.INFO_PHONE_COLUMN: equeue_info['phone'],
        ct.INFO_IS_BREST_COLUMN: equeue_info['isBts'],
        ct.INFO_NAME_RU_COLUMN: equeue_info['name'],
        ct.INFO_HASH_COLUMN: hash('_'.join(map(str, [equeue_info['id'], equeue_info['nameEn'], equeue_info['address'],
                                                     equeue_info['phone'], equeue_info['isBts'], equeue_info['name']])))
    })


def convert_to_pandas_equeue(equeue_snapshot_dict: tp.Dict) -> EqueueData:
    def convert_and_group_entities(all_entities: tp.List) -> tp.Optional[pd.DataFrame]:
        grouped_entities = None
        if len(all_entities) > 0:
            grouped_entities = pd.concat([convert_equeue_entity_to_pandas(equeue_entity=entity, load_dt=load_dt)
                                          for entity in all_entities], axis=1).T
            grouped_entities = grouped_entities.astype({
                ct.YEAR_COLUMN: 'int32',
                ct.MONTH_COLUMN: 'int32',
                ct.LOAD_DATE_COLUMN: 'datetime64[us]',
                ct.REGISTRATION_DATE_COLUMN: 'datetime64[us]',
                ct.CHANGED_DATE_COLUMN: 'datetime64[us]',
                ct.STATUS_COLUMN: 'int64',
                ct.QUEUE_POS_COLUMN: 'float64',
                ct.QUEUE_TYPE_COLUMN: 'int32'
            })
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
