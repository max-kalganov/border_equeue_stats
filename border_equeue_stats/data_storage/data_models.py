from typing import Union, Optional
import pandas as pd
from dataclasses import dataclass
from border_equeue_stats import constants as ct


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

    @staticmethod
    def _check_dfs(df1, df2):
        if isinstance(df1, pd.DataFrame) and isinstance(df2, pd.DataFrame):
            if ct.LOAD_DATE_COLUMN in df1:
                df1 = df1.sort_values(ct.LOAD_DATE_COLUMN).reset_index(drop=True)
            if ct.LOAD_DATE_COLUMN in df2:
                df2 = df2.sort_values(ct.LOAD_DATE_COLUMN).reset_index(drop=True)
            cols = df1.columns
            compared_dfs = (df1[cols] == df2[cols])
            return (sorted(df1.columns) == sorted(df2.columns)) \
                   and df1.shape == df2.shape \
                   and (df1[cols].isna() == df2[cols].isna()).all().all() \
                   and compared_dfs[~df1[cols].isna()].all().all()
        else:
            return df1 is None and df2 is None

    def _check_infos(self, info1, info2):
        if not (isinstance(info1, (pd.Series, pd.DataFrame)) and isinstance(info2, (pd.Series, pd.DataFrame))):
            return False

        if isinstance(info1, pd.Series):
            info1 = info1.to_frame().T

        if isinstance(info2, pd.Series):
            info2 = info2.to_frame().T

        info1 = info1.sort_values(ct.LOAD_DATE_COLUMN).reset_index(drop=True)
        info2 = info2.sort_values(ct.LOAD_DATE_COLUMN).reset_index(drop=True)

        return self._check_dfs(df1=info1[info1.columns.difference([ct.LOAD_DATE_COLUMN,
                                                                   ct.YEAR_COLUMN, ct.MONTH_COLUMN])],
                               df2=info2[info2.columns.difference([ct.LOAD_DATE_COLUMN,
                                                                   ct.YEAR_COLUMN, ct.MONTH_COLUMN])])

    def __eq__(self, other):
        try:
            are_all_passed = self._check_infos(self.info, other.info) \
                             and self._check_dfs(self.truck_queue, other.truck_queue) \
                             and self._check_dfs(self.truck_priority, other.truck_priority) \
                             and self._check_dfs(self.truck_gpk, other.truck_gpk) \
                             and self._check_dfs(self.bus_queue, other.bus_queue) \
                             and self._check_dfs(self.bus_priority, other.bus_priority) \
                             and self._check_dfs(self.car_queue, other.car_queue) \
                             and self._check_dfs(self.car_priority, other.car_priority) \
                             and self._check_dfs(self.motorcycle_queue, other.motorcycle_queue) \
                             and self._check_dfs(self.motorcycle_priority, other.motorcycle_priority)
        except Exception as e:
            print(f"Error while comparing EqueueData: {e}")
            are_all_passed = False
        return True if are_all_passed else False  # to convert numpy bool into python bool
