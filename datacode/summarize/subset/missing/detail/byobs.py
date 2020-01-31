import pandas as pd
from datacode.typing import StrList


def obs_pct_long_df(df: pd.DataFrame, byvars: StrList, obs_count_var: str,
                    count_with_missings_var: str, missing_display_str: str='Missing') -> pd.DataFrame:
    """

    Args:
        df:
        byvars:
        obs_count_var: should never be missing. may be id var, or may have to create a column
        count_with_missings_var:

    Returns:

    """

    long_obs_df = _obs_count_long_df(
        df,
        byvars,
        [obs_count_var, count_with_missings_var]
    )

    long_obs_df[f'Obs {missing_display_str} Percentage'] = \
        (1 - (long_obs_df[count_with_missings_var] / long_obs_df[obs_count_var])) * 100
    long_obs_df.drop(count_with_missings_var, axis=1, inplace=True)
    long_obs_df.rename(columns={obs_count_var: 'Obs'}, inplace=True)

    return long_obs_df.reset_index()

def _obs_count_long_df(df: pd.DataFrame, byvars: StrList, countvars: StrList) -> pd.DataFrame:
    return df.groupby(byvars)[countvars].count()