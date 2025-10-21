import json
import os
import shutil
import unittest
import typing as tp
from datetime import datetime, timedelta

import pandas as pd

from border_equeue_stats.data_storage.data_models import EqueueData
from border_equeue_stats.constants import CAR_LIVE_QUEUE_KEY
from border_equeue_stats.data_storage.data_storage_utils import convert_to_pandas_equeue
from border_equeue_stats.data_storage.json_storage import read_from_json
from border_equeue_stats.data_storage.parquet_storage import dump_to_parquet, read_from_parquet, read_all_from_parquet, \
    dump_all_stored_json_to_parquet, apply_datetime_aggregation


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
                if test_num == 2:
                    prev_true_data = convert_to_pandas_equeue(self.test_all_jsons[0])
                    true_data.info = pd.concat([prev_true_data.info, true_data.info], axis=1).T
                self.assertTrue(true_data == tested_data)

    def test_3_full_dump(self):
        dump_all_stored_json_to_parquet(json_storage_path=self.test_equeue_path,
                                        parquet_storage_path=self.test_equeue_pq_path)
        tested_data = read_all_from_parquet(parquet_storage_path=self.test_equeue_pq_path, apply_filter_to_info=False)
        true_data = read_from_json(json_storage_path=self.test_equeue_path)

        self.assertTrue(true_data == tested_data)

    def test_4_read_batches(self):
        dump_all_stored_json_to_parquet(json_storage_path=self.test_equeue_path,
                                        parquet_storage_path=self.test_equeue_pq_path)

        all_batches = [batch for batch in read_from_parquet(CAR_LIVE_QUEUE_KEY,
                                                            filters=None,
                                                            parquet_storage_path=self.test_equeue_pq_path,
                                                            in_batches=True,
                                                            batch_size=5)]
        full_tested_data = pd.concat(all_batches).sort_values('load_dt').reset_index(drop=True)

        true_equeue_data = read_from_json(json_storage_path=self.test_equeue_path)
        true_data = true_equeue_data.car_queue.sort_values('load_dt').reset_index(drop=True)
        # TODO: add partitioning keys into batches and remove the following filter
        # true_data = true_data[true_data.columns.difference(['year', 'month'])]
        self.assertTrue(EqueueData._check_dfs(df1=full_tested_data, df2=true_data))


class TestApplyDatetimeAggregation(unittest.TestCase):
    """Test cases for apply_datetime_aggregation function"""

    def setUp(self):
        """Set up test data"""
        # Create base datetime for consistent testing
        self.base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Create test data with various time intervals
        self.test_data = pd.DataFrame({
            'timestamp': [
                self.base_time + timedelta(minutes=i*5) for i in range(12)  # Every 5 minutes
            ],
            'value': [i * 10 for i in range(12)],
            'count': [i + 1 for i in range(12)],
            'category': ['A', 'B'] * 6,
            'region': ['North', 'South'] * 6,
            'text_field': [f'text_{i}' for i in range(12)]
        })

    def test_no_aggregation(self):
        """Test that function returns original data when floor_value is None"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value=None
        )
        
        pd.testing.assert_frame_equal(result, self.test_data)

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        empty_df = pd.DataFrame()
        result = apply_datetime_aggregation(
            df=empty_df,
            time_column='timestamp',
            floor_value='5min'
        )
        
        self.assertTrue(result is None or len(result) == 0)

    def test_none_dataframe(self):
        """Test handling of None DataFrame"""
        result = apply_datetime_aggregation(
            df=None,
            time_column='timestamp',
            floor_value='5min'
        )
        
        self.assertTrue(result is None)

    def test_5min_aggregation_mean(self):
        """Test 5-minute aggregation with mean method"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='5min',
            aggregation_method='mean',
            group_columns=['category'],
            value_columns={'value': 'mean', 'count': 'sum'}
        )
        
        # Should have 2 groups (A, B) with 6 time buckets each
        self.assertEqual(len(result), 12)
        self.assertIn('timestamp', result.columns)
        self.assertIn('category', result.columns)
        self.assertIn('value', result.columns)
        self.assertIn('count', result.columns)

    def test_hourly_aggregation_max(self):
        """Test hourly aggregation with max method"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='h',
            aggregation_method='max',
            group_columns=['category']
        )
        
        # Should have 2 groups (A, B) with 1 time bucket each (all within same hour)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(result['category'].isin(['A', 'B'])))

    def test_daily_aggregation_drop(self):
        """Test daily aggregation with drop method"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='d',
            aggregation_method='drop',
            group_columns=['category']
        )
        
        # Should have 2 groups (A, B) with 1 time bucket each (all within same day)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(result['category'].isin(['A', 'B'])))

    def test_aggregation_without_group_columns(self):
        """Test aggregation without additional group columns"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='15min',
            aggregation_method='mean'
        )
        
        # Should have fewer rows due to 15-minute aggregation
        self.assertLess(len(result), len(self.test_data))
        self.assertIn('timestamp', result.columns)

    def test_aggregation_with_custom_value_columns(self):
        """Test aggregation with custom value column mappings"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='10min',
            aggregation_method='mean',
            group_columns=['category'],
            value_columns={'value': 'max', 'count': 'min'}
        )
        
        self.assertIn('value', result.columns)
        self.assertIn('count', result.columns)
        self.assertIn('category', result.columns)

    def test_non_numeric_columns_handling(self):
        """Test that non-numeric columns are handled with 'first' aggregation"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='15min',
            aggregation_method='mean',
            group_columns=['category']
        )
        
        # Text field should be present and use 'first' aggregation
        self.assertIn('text_field', result.columns)
        self.assertTrue(all(isinstance(val, str) for val in result['text_field']))

    def test_aggregation_preserves_grouping(self):
        """Test that grouping columns are preserved correctly"""
        result = apply_datetime_aggregation(
            df=self.test_data,
            time_column='timestamp',
            floor_value='10min',
            aggregation_method='mean',
            group_columns=['category', 'region']
        )
        
        # Check that all combinations of category and region are preserved
        expected_combinations = set(zip(self.test_data['category'], self.test_data['region']))
        result_combinations = set(zip(result['category'], result['region']))
        
        # All original combinations should be present (though possibly aggregated)
        self.assertTrue(expected_combinations.issubset(result_combinations))

    def test_time_flooring(self):
        """Test that time flooring works correctly"""
        # Create data with irregular timestamps
        irregular_data = pd.DataFrame({
            'timestamp': [
                datetime(2024, 1, 1, 10, 2, 30),
                datetime(2024, 1, 1, 10, 7, 45),
                datetime(2024, 1, 1, 10, 12, 15),
            ],
            'value': [10, 20, 30],
            'category': ['A', 'A', 'A']
        })
        
        result = apply_datetime_aggregation(
            df=irregular_data,
            time_column='timestamp',
            floor_value='5min',
            aggregation_method='mean',
            group_columns=['category']
        )
        
        # All timestamps should be floored to 5-minute boundaries
        for ts in result['timestamp']:
            self.assertEqual(ts.minute % 5, 0)
            self.assertEqual(ts.second, 0)

    def test_large_dataset_aggregation(self):
        """Test aggregation with a larger dataset"""
        # Create larger dataset
        large_data = pd.DataFrame({
            'timestamp': [
                self.base_time + timedelta(minutes=i) for i in range(100)
            ],
            'value': [i for i in range(100)],
            'category': ['A', 'B'] * 50
        })
        
        result = apply_datetime_aggregation(
            df=large_data,
            time_column='timestamp',
            floor_value='10min',
            aggregation_method='mean',
            group_columns=['category']
        )
        
        # Should have fewer rows due to aggregation
        self.assertLess(len(result), len(large_data))
        self.assertGreater(len(result), 0)

    def test_edge_case_single_row(self):
        """Test aggregation with single row data"""
        single_row = pd.DataFrame({
            'timestamp': [self.base_time],
            'value': [42],
            'category': ['A']
        })
        
        result = apply_datetime_aggregation(
            df=single_row,
            time_column='timestamp',
            floor_value='5min',
            aggregation_method='mean',
            group_columns=['category']
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['value'], 42)
        self.assertEqual(result.iloc[0]['category'], 'A')


if __name__ == '__main__':
    unittest.main()
