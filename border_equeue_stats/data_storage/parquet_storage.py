import pandas as pd


def dump_to_parquet(data: dict) -> None:
    def dump_metadata():
        """
        id -- uuid
        nameEn -- Brest BTS
        address -- address (ru)
        phone -- several numbers
        isBts -- 1
        name -- Брест
        """
        pass

    def dump_truck_queue():
        pass

    def dump_truck_priority():
        pass

    def dump_truck_gpk():
        pass

    def dump_bus_queue():
        """
        regnum -- string
        status -- int
        order_id -- Optional[int]
        type_queue -- int
        registration_date -- str (%H:%M:%S %d.%m.%Y)
        changed_date -- str (%H:%M:%S %d.%m.%Y)
        :return:
        """
        pass

    def dump_bus_priority():
        pass

    def dump_car_queue():
        """
        regnum -- string
        status -- int
        order_id -- Optional[int]
        type_queue -- int
        registration_date -- str (%H:%M:%S %d.%m.%Y)
        changed_date -- str (%H:%M:%S %d.%m.%Y)

        :return:
        """
        pass

    def dump_car_priority():
        pass

    def dump_motorcycle_queue():
        pass

    def dump_motorcycle_priority():
        pass

    import pyarrow as pa
    import pandas as pd
    df = pd.DataFrame({'year': [2020, 2022, 2021, 2022, 2019, 2021],
                       'n_legs': [2, 2, 4, 4, 5, 100],
                       'animal': ["Flamingo", "Parrot", "Dog", "Horse",
                                  "Brittle stars", "Centipede"]})
    table = pa.Table.from_pandas(df)
    import pyarrow.parquet as pq
    pq.write_table(table, 'table_V2.parquet')
    dataset = pq.ParquetDataset('table_V2.parquet')





def read_from_parquet() -> pd.DataFrame:
    pass
