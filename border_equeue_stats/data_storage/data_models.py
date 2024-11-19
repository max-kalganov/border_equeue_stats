from typing import Union, Optional
import pandas as pd
from dataclasses import dataclass


@dataclass
class EqueueData:
    info: Union[pd.Series, pd.DataFrame]
    truck_queue: Optional[pd.DataFrame]
    truck_priority: Optional[pd.DataFrame]
    truck_gpk: Optional[pd.DataFrame]
    bus_queue: Optional[pd.DataFrame]
    bus_priority: Optional[pd.DataFrame]
    car_queue: Optional[pd.DataFrame]
    car_priority: Optional[pd.DataFrame]
    motorcycle_queue: Optional[pd.DataFrame]
    motorcycle_priority: Optional[pd.DataFrame]

    def __eq__(self, other):
        def _check_dfs(df1, df2):
            if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
                cols = df1.columns
                return (sorted(df1.columns) == sorted(df2.columns)) \
                       and df1.shape == df2.shape \
                       and (df1[cols] == df2[cols]).all().all()
            else:
                return df1 is None and df2 is None

        try:
            are_all_passed = (self.info == other.info).all() \
                             and _check_dfs(self.truck_queue, other.truck_queue) \
                             and _check_dfs(self.truck_priority, other.truck_priority) \
                             and _check_dfs(self.truck_gpk, other.truck_gpk) \
                             and _check_dfs(self.bus_queue, other.bus_queue) \
                             and _check_dfs(self.bus_priority, other.bus_priority) \
                             and _check_dfs(self.car_queue, other.car_queue) \
                             and _check_dfs(self.car_priority, other.car_priority) \
                             and _check_dfs(self.motorcycle_queue, other.motorcycle_queue) \
                             and _check_dfs(self.motorcycle_priority, other.motorcycle_priority)
        except Exception as e:
            print(f"Error while comparing EqueueData: {e}")
            are_all_passed = False
        return True if are_all_passed else False  # to convert numpy bool into python bool
