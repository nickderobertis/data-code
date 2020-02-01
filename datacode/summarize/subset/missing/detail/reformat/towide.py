import pandas as pd
import pd_utils

def _long_counts_to_wide_df(long_df: pd.DataFrame, col: str, row_byvar: str, col_byvar: str) -> pd.DataFrame:
    return pd_utils.long_to_wide(
        long_df[[row_byvar, col_byvar, col]],
        groupvars=[row_byvar],
        values=[col],
        colindex=[col_byvar],
        colindex_only=True
    ).set_index(row_byvar).fillna(0)