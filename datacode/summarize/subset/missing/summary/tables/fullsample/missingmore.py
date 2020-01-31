import pandas as pd
import pyexlatex.table as lt

from datacode.typing import StrList, IntSequenceOrNone, FloatSequenceOrNone

from datacode.summarize.subset.missing.detail.byid import by_id_pct_long_df
from datacode.summarize.subset.missing.detail.textfuncs import missing_more_than_pct_str, num_or_pct

def missing_more_than_data_table(df: pd.DataFrame, id_col: str, col_with_missings: str,
                                 missing_tolerances: IntSequenceOrNone=(0, 1, 10, 50),
                                 missing_quantiles: FloatSequenceOrNone = None,
                                 missing_display_str: str='Missing',
                                 period_display_name: str='Period',
                                 pct_format_str: str = '.1f') -> lt.DataTable:
    missing_more_than_df = _missing_more_than_df(
        df,
        id_col,
        col_with_missings,
        missing_tolerances=missing_tolerances,
        missing_quantiles=missing_quantiles,
        missing_display_str=missing_display_str,
        pct_format_str=pct_format_str
    )

    missing_more_than_dt = lt.DataTable.from_df(
        missing_more_than_df,
        extra_header=f'% {missing_display_str} > {num_or_pct(missing_tolerances, missing_quantiles)} '
                     f'{period_display_name}s'
    )

    return missing_more_than_dt

def _missing_more_than_df(df: pd.DataFrame, id_col: str, col_with_missings: str,
                          missing_tolerances: IntSequenceOrNone=(0, 1, 10, 50),
                          missing_quantiles: FloatSequenceOrNone = None,
                          missing_display_str: str='Missing',
                          pct_format_str: str = '.1f'
                          ) -> pd.DataFrame:

    if '_ones' in df.columns:
        raise ValueError('must not have a column _ones, will be overwritten')
    df['_ones'] = 1

    if missing_quantiles is not None:
        missing_cuts = missing_quantiles
        name_arg_index = 1
        cut_name = 'missing_quantile'
    elif missing_tolerances is not None:
        missing_cuts = missing_tolerances
        name_arg_index = 0
        cut_name = 'missing_tolerance'

    else:
        raise ValueError('must pass missing_tolerances or missing_quantiles, got both None')

    missing_more_than_df = pd.DataFrame()
    name_args = [None, None, missing_display_str, id_col]
    for missing_cut in missing_cuts:
        name_args[name_arg_index] = missing_cut
        missing_more_than_name = missing_more_than_pct_str(*name_args)
        cut_kwargs = {cut_name: missing_cut}
        rename_dict = {missing_more_than_name: f'{missing_cut * 100:g}%'}
        temp_by_id_pct_df = by_id_pct_long_df(
            df,
            ['_ones'],
            id_col,
            col_with_missings,
            missing_display_str=missing_display_str,
            **cut_kwargs
        ).drop([f'{id_col} Count', '_ones'], axis=1).rename(columns=rename_dict)
        missing_more_than_df = pd.concat([missing_more_than_df, temp_by_id_pct_df], axis=1)
    missing_more_than_df = missing_more_than_df.applymap(lambda x: f'{x:{pct_format_str}}')

    df.drop('_ones', axis=1, inplace=True)

    return missing_more_than_df