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
