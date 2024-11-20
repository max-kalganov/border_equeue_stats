import json
import os
import shutil
import unittest
import typing as tp
from datetime import datetime

import pandas as pd

from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue
from border_equeue_stats.data_storage.json_storage import read_from_json
from border_equeue_stats.data_storage.parquet_storage import dump_to_parquet, read_from_parquet, read_all_from_parquet, \
    dump_all_stored_json_to_parquet


class TestParquetStorage(unittest.TestCase):
    @staticmethod
    def _get_all_test_dicts(data_path: str) -> tp.List[tp.Dict]:
        with open(data_path, 'r') as f:
            lines = f.readlines()
        return [json.loads(line) for line in lines]

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_equeue_path = os.path.join('test_data', 'test_equeue.txt')
        cls.test_equeue_pq_path = os.path.join('test_data', 'pq_data')
        assert os.path.exists(cls.test_equeue_path), 'test data file is not found'
        cls.test_all_jsons = cls._get_all_test_dicts(cls.test_equeue_path)

    def setUp(self):
        os.makedirs(self.test_equeue_pq_path, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_equeue_pq_path)

    def test_1_convert(self):
        test_dict = self.test_all_jsons[0]
        true_data = EqueueData(
            info=pd.Series({
                'year': 2024,
                'month': 9,
                'load_dt': datetime.strptime('2024-09-30 15:31:21.954864', '%Y-%m-%d %H:%M:%S.%f'),
                'id': 'a9173a85-3fc0-424c-84f0-defa632481e4',
                'name': 'Brest BTS',
                'address': 'Брест, Варшавское шоссе, 1',
                'phone': '+375 (162) 58-60-44,+375 (162) 58-60-50,+375 (33) 323-75-89',
                'is_brest': 1,
                'name_ru': 'Брест',
                'hash': hash('a9173a85-3fc0-424c-84f0-defa632481e4'
                             '_Brest BTS'
                             '_Брест, Варшавское шоссе, 1'
                             '_+375 (162) 58-60-44,+375 (162) 58-60-50,+375 (33) 323-75-89'
                             '_1'
                             '_Брест')
            }),
            truck_queue=None,
            truck_priority=None,
            truck_gpk=None,
            bus_queue=pd.DataFrame({
                'year': [2024] * 4,
                'month': [9] * 4,
                'car_number': ['AM20354', 'AI32906', 'AH24387', 'AK69103'],
                'status': [2] * 4,
                'queue_pos': [1, 2, 3, 4],
                'queue_type': [3] * 4,
                'reg_date': [datetime.strptime('20:11:32 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('20:38:44 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('21:52:04 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('22:08:37 29.09.2024', '%H:%M:%S %d.%m.%Y')],
                'changed_date': [datetime.strptime('20:11:32 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('20:38:44 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('21:52:04 29.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('22:08:37 29.09.2024', '%H:%M:%S %d.%m.%Y')],
                'load_dt': [datetime.strptime('2024-09-30 15:31:21.954864', '%Y-%m-%d %H:%M:%S.%f')] * 4
            }),
            bus_priority=None,
            car_queue=pd.DataFrame({
                'year': [2024] * 6,
                'month': [9] * 6,
                'car_number': ['0111II1', '7876KK1', '9915EH2', '4021HI4', '9697IA4', '5583AO1'],
                'status': [2] * 6,
                'queue_pos': [1, 2, 3, 4, 5, 6],
                'queue_type': [3] * 6,
                'reg_date': [datetime.strptime('09:20:10 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('09:21:07 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('09:22:09 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('09:24:35 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('09:25:19 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                             datetime.strptime('09:26:09 30.09.2024', '%H:%M:%S %d.%m.%Y')],
                'changed_date': [datetime.strptime('09:20:10 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('09:21:07 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('09:22:09 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('09:24:35 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('09:25:19 30.09.2024', '%H:%M:%S %d.%m.%Y'),
                                 datetime.strptime('09:26:09 30.09.2024', '%H:%M:%S %d.%m.%Y')],
                'load_dt': [datetime.strptime('2024-09-30 15:31:21.954864', '%Y-%m-%d %H:%M:%S.%f')] * 6
            }),
            car_priority=None,
            motorcycle_queue=None,
            motorcycle_priority=None
        )
        tested_data = convert_to_pandas_equeue(equeue_snapshot_dict=test_dict)
        self.assertTrue(true_data == tested_data)

    def test_2_single_dump(self):
        for test_num, test_single_json in enumerate(self.test_all_jsons):
            with self.subTest(test_num=test_num):
                dump_to_parquet(test_single_json, parquet_storage_path=self.test_equeue_pq_path)
                tested_data = read_all_from_parquet(
                    filters=[('load_dt', '=', datetime.strptime(test_single_json['datetime'], '%Y-%m-%d %H:%M:%S.%f'))],
                    parquet_storage_path=self.test_equeue_pq_path,
                    apply_filter_to_info=False
                )
                true_data = convert_to_pandas_equeue(test_single_json)
                self.assertTrue(true_data == tested_data)
                # TODO: fix different types in queue_pos - float and int

    def test_3_full_dump(self):
        dump_all_stored_json_to_parquet(json_storage_path=self.test_equeue_path,
                                        parquet_storage_path=self.test_equeue_pq_path)
        tested_data = read_all_from_parquet(parquet_storage_path=self.test_equeue_pq_path, apply_filter_to_info=False)
        true_data = read_from_json(json_storage_path=self.test_equeue_path)
        self.assertTrue(true_data == tested_data)
    #
    # def test_3_read(self):
    #     "test read single"
    #
    #     "test read all"
    #
    #     "test read in batches"
    #
    #     "test read with filter"
    #
    #
    # def test_4_read_info(self):
    #     # read_parquet_info_data
    #     pass
    #
    # def test_5_is_info_stored(self):
    #     # is_info_stored
    #     pass
    #


if __name__ == '__main__':
    unittest.main()
