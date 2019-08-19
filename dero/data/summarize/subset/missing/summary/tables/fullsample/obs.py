import pandas as pd
import pyexlatex.table as lt

def obs_count_and_missing_data_table(df: pd.DataFrame, col_with_missings: str,
                                     missing_display_str: str='Missing', pct_format_str: str='.1f') -> lt.DataTable:

    obs_pct_df = _obs_count_and_missing_pct_df(
        df,
        col_with_missings,
        missing_display_str=missing_display_str,
        pct_format_str=pct_format_str
    )

    obs_pct_dt = lt.DataTable.from_df(
        obs_pct_df,
        extra_header='Obs'
    )

    return obs_pct_dt

def _obs_count_and_missing_pct_df(df: pd.DataFrame, col_with_missings: str,
                                  missing_display_str: str='Missing', pct_format_str: str='.1f') -> pd.DataFrame:
    obs_count = len(df)
    non_miss_count = len(df.dropna(subset=[col_with_missings]))
    return pd.DataFrame(
        data=[
            (
                obs_count,
                f'{(1 - (non_miss_count/obs_count)) * 100:{pct_format_str}}'
            )
        ],
        columns=['#', f'% {missing_display_str}']
    )