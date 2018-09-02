import pandas as pd
import dero.latex.table as lt

from dero.data.typing import DfDict, StrOrNone
from dero.data.summarize.subset.missing.detail.byid import by_id_pct_long_df
from dero.data.summarize.subset.missing.detail.byobs import obs_pct_long_df
from dero.data.summarize.subset.missing.detail.reformat.main import long_counts_to_formatted_wide_df_dict
from dero.data.summarize.subset.missing.detail.totex import missing_detail_df_dict_to_table_and_output

def obs_and_id_count_and_missing_pct_table(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                           row_byvar: str, col_byvar: str,
                                           missing_tolerance: int=0,
                                           sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                           count_format_str: str = '.0f', pct_format_str: str = '.1f',
                                           extra_caption: str='', extra_below_text: str='',
                                           outfolder: StrOrNone=None) -> lt.Table:

    df_dict = obs_and_id_count_and_missing_pct_df_dict(
        df,
        col_with_missings,
        id_col,
        row_byvar,
        col_byvar,
        missing_tolerance=missing_tolerance,
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        count_format_str=count_format_str,
        pct_format_str=pct_format_str
    )

    table = missing_detail_df_dict_to_table_and_output(
        df_dict,
        row_byvar,
        col_byvar,
        id_col,
        col_with_missings,
        missing_tolerance,
        extra_caption=extra_caption,
        extra_below_text=extra_below_text,
        outfolder=outfolder
    )

    return table


def obs_and_id_count_and_missing_pct_df_dict(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                             row_byvar: str, col_byvar: str,
                                             missing_tolerance: int=0,
                                             sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                             count_format_str: str = '.0f', pct_format_str: str = '.1f'
                                             ) -> DfDict:

    byvars = [row_byvar, col_byvar]

    common_args = (
        df,
        byvars,
        id_col,
        col_with_missings
    )

    obs_pct_df = obs_pct_long_df(
        *common_args
    )

    by_id_pct_df = by_id_pct_long_df(
        *common_args,
        missing_tolerance=missing_tolerance
    )

    format_strs = [count_format_str, pct_format_str]

    common_args = (
        row_byvar,
        col_byvar
    )

    common_kwargs = dict(
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
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

