import pandas as pd
import dero.latex.table as lt

from dero.data.typing import StrList, IntSequence

from dero.data.summarize.subset.missing.detail.byid import by_id_pct_long_df
from dero.data.summarize.subset.missing.detail.textfuncs import missing_more_than_pct_str

def missing_more_than_data_table(df: pd.DataFrame, id_col: str, col_with_missings: str,
                                 missing_tolerances: IntSequence=(0, 1, 10, 50), missing_display_str: str='Missing',
                                 period_display_name: str='Period',
                                 pct_format_str: str = '.1f') -> lt.DataTable:
    missing_more_than_df = _missing_more_than_df(
        df,
        id_col,
        col_with_missings,
        missing_tolerances=missing_tolerances,
        missing_display_str=missing_display_str,
        pct_format_str=pct_format_str
    )

    missing_more_than_dt = lt.DataTable.from_df(
        missing_more_than_df,
        extra_header=f'% {missing_display_str} > # {period_display_name}s'
    )

    return missing_more_than_dt

def _missing_more_than_df(df: pd.DataFrame, id_col: str, col_with_missings: str,
                          missing_tolerances: IntSequence=(0, 1, 10, 50), missing_display_str: str='Missing',
                          pct_format_str: str = '.1f'
                          ) -> pd.DataFrame:

    if '_ones' in df.columns:
        raise ValueError('must not have a column _ones, will be overwritten')
    df['_ones'] = 1

    missing_more_than_df = pd.DataFrame()
    for missing_tolerance in missing_tolerances:
        missing_more_than_name = missing_more_than_pct_str(missing_tolerance, missing_display_str, id_col)
        rename_dict = {missing_more_than_name: missing_tolerance}
        temp_by_id_pct_df = by_id_pct_long_df(
            df,
            ['_ones'],
            id_col,
            col_with_missings,
            missing_tolerance=missing_tolerance,
            missing_display_str=missing_display_str
        ).drop([f'{id_col} Count', '_ones'], axis=1).rename(columns=rename_dict)
        missing_more_than_df = pd.concat([missing_more_than_df, temp_by_id_pct_df], axis=1)
    missing_more_than_df = missing_more_than_df.applymap(lambda x: f'{x:{pct_format_str}}')

    df.drop('_ones', axis=1, inplace=True)

    return missing_more_than_df