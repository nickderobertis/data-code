import pandas as pd
import pyexlatex.table as lt

from datacode.typing import SimpleDfDict, StrOrNone, IntSequenceOrNone, IntOrNone, FloatSequenceOrNone, FloatOrNone
from datacode.summarize.subset.missing.detail.byid import by_id_pct_long_df
from datacode.summarize.subset.missing.detail.byobs import obs_pct_long_df
from datacode.summarize.subset.missing.detail.reformat.main import long_counts_to_formatted_wide_df_dict
from datacode.summarize.subset.missing.detail.totex import missing_detail_df_dict_to_table_and_output
from datacode.summarize.subset.missing.summary.tables.fullsample.main import missing_full_sample_summary_panel

def obs_and_id_count_and_missing_pct_table(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                           row_byvar: str, col_byvar: str, datevar: str,
                                           missing_tolerance: IntOrNone=0, missing_quantile: FloatOrNone = None,
                                           summary_missing_tolerances: IntSequenceOrNone = (0, 5, 10),
                                           summary_missing_quantiles: FloatSequenceOrNone = None,
                                           sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                           sort_cols_as_portvar: bool = False, sort_rows_as_portvar: bool = False,
                                           count_format_str: str = '.0f', pct_format_str: str = '.1f',
                                           missing_display_str: str = 'Missing', period_display_name: str = 'Period',
                                           extra_caption: str = '', extra_below_text: str = '',
                                           table_align: str = None,
                                           outfolder: StrOrNone=None) -> lt.Table:

    df_dict = obs_and_id_count_and_missing_pct_df_dict(
        df,
        col_with_missings,
        id_col,
        row_byvar,
        col_byvar,
        missing_tolerance=missing_tolerance,
        missing_quantile=missing_quantile,
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        sort_cols_as_portvar=sort_cols_as_portvar,
        sort_rows_as_portvar=sort_rows_as_portvar,
        count_format_str=count_format_str,
        pct_format_str=pct_format_str,
        missing_display_str=missing_display_str
    )

    summary_panel = missing_full_sample_summary_panel(
        df,
        id_col,
        col_with_missings,
        missing_tolerances=summary_missing_tolerances,
        missing_quantiles=summary_missing_quantiles,
        missing_display_str=missing_display_str,
        datevar=datevar,
        pct_format_str=pct_format_str,
        period_display_name=period_display_name
    )

    table = missing_detail_df_dict_to_table_and_output(
        df_dict,
        summary_panel,
        row_byvar,
        col_byvar,
        id_col,
        col_with_missings,
        missing_tolerance=missing_tolerance,
        missing_quantile=missing_quantile,
        summary_missing_tolerances=summary_missing_tolerances,
        summary_missing_quantiles=summary_missing_quantiles,
        missing_display_str=missing_display_str,
        period_display_name=period_display_name,
        extra_caption=extra_caption,
        extra_below_text=extra_below_text,
        align=table_align,
        outfolder=outfolder
    )

    return table


def obs_and_id_count_and_missing_pct_df_dict(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                             row_byvar: str, col_byvar: str,
                                             missing_tolerance: int=0, missing_quantile: IntOrNone = None,
                                             sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                             sort_cols_as_portvar: bool=False, sort_rows_as_portvar: bool=False,
                                             count_format_str: str = '.0f', pct_format_str: str = '.1f',
                                             missing_display_str: str = 'Missing'
                                             ) -> SimpleDfDict:

    byvars = [row_byvar, col_byvar]

    common_args = (
        df,
        byvars,
        id_col,
        col_with_missings
    )

    obs_pct_df = obs_pct_long_df(
        *common_args,
        missing_display_str=missing_display_str
    )

    by_id_pct_df = by_id_pct_long_df(
        *common_args,
        missing_tolerance=missing_tolerance,
        missing_quantile=missing_quantile,
        missing_display_str=missing_display_str
    )

    format_strs = [count_format_str, pct_format_str]

    common_args = (
        row_byvar,
        col_byvar
    )

    common_kwargs = dict(
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        sort_cols_as_portvar=sort_cols_as_portvar,
        sort_rows_as_portvar=sort_rows_as_portvar,
        format_strs=format_strs
    )

    df_dict = long_counts_to_formatted_wide_df_dict(
        obs_pct_df,
        *common_args,
        **common_kwargs
    )

    df_dict.update(
        long_counts_to_formatted_wide_df_dict(
            by_id_pct_df,
            *common_args,
            **common_kwargs
        )
    )

    return df_dict

