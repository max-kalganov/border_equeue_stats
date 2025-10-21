import typing as tp
import pandas as pd


def apply_datetime_aggregation(df: pd.DataFrame,
                               time_column: str,
                               floor_value: tp.Optional[str] = None,
                               aggregation_method: str = 'mean',
                               group_columns: tp.Optional[tp.List[str]] = None,
                               value_columns: tp.Optional[tp.Dict[str, str]] = None) -> pd.DataFrame:
    """
    Apply datetime aggregation to a DataFrame with flexible aggregation methods.

    Args:
        df: Input DataFrame
        time_column: Name of the datetime column to aggregate by
        floor_value: Time aggregation period ('5min', 'h', 'd', 'M', None)
            - '5min': 5-minute intervals
            - 'h': hourly aggregation  
            - 'd': daily aggregation
            - 'M': monthly aggregation
            - None: no aggregation (return original data)
        aggregation_method: How to aggregate values ('mean', 'max', 'min', 'drop')
            - 'mean': calculate mean of values in each time bucket
            - 'max': take maximum value in each time bucket
            - 'min': take minimum value in each time bucket  
            - 'drop': just drop intermediate points (take first value)
        group_columns: Additional columns to group by (e.g., ['queue_name'])
        value_columns: Dict mapping column names to aggregation methods
                      e.g., {'hours_waited': 'mean', 'vehicle_count': 'max'}

    Returns:
        Aggregated DataFrame
    """
    if df is None or len(df) == 0:
        return df

    if floor_value is None:
        return df

    # Make a copy to avoid modifying original
    result_df = df.copy().sort_values(time_column).reset_index(drop=True)

    # Apply time floor
    result_df[time_column] = result_df[time_column].dt.floor(floor_value)

    # Prepare grouping columns
    group_cols = [time_column]
    if group_columns:
        group_cols.extend(group_columns)

    # Handle different aggregation methods
    if aggregation_method == 'drop':
        # Just drop duplicates, keeping first occurrence
        result_df = result_df.drop_duplicates(subset=group_cols, keep='first')
    else:
        # Prepare aggregation dictionary
        if value_columns:
            agg_dict = value_columns.copy()
        else:
            # Default: aggregate all numeric columns with the specified method
            numeric_cols = result_df.select_dtypes(include=['number']).columns
            agg_dict = {col: aggregation_method for col in numeric_cols}

        # Handle non-numeric columns (take first value)
        non_numeric_cols = []
        for col in result_df.columns:
            if col not in group_cols \
                    and col not in agg_dict \
                    and (result_df[col].dtype == 'object'
                         or pd.api.types.is_categorical_dtype(result_df[col])):
                non_numeric_cols.append(col)
                agg_dict[col] = 'first'

        # Apply aggregation
        if agg_dict:
            result_df = result_df.groupby(
                group_cols).agg(agg_dict).reset_index()

    return result_df
