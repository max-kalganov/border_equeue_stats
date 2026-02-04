from border_equeue_stats.data_storage.parquet_storage import coalesce_parquet_data

if __name__ == '__main__':
    coalesce_parquet_data(parquet_storage_path='data/parquet_dataset')
