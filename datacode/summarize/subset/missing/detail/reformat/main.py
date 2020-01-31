import pandas as pd

from datacode.summarize.subset.missing.detail.reformat.towide import _long_counts_to_wide_df
from datacode.summarize.subset.missing.detail.reformat.numformat import sort_and_apply_formatting_to_df
from datacode.typing import DfDict, StrList

def long_counts_to_formatted_wide_df_dict(long_df: pd.DataFrame, row_byvar: str, col_byvar: str,
                                          sort_cols_as_numeric: bool =True, sort_rows_as_numeric: bool =True,
                                          sort_cols_as_portvar: bool=False, sort_rows_as_portvar: bool=False,
                                          format_strs: StrList = None
                                          ) -> DfDict:

    if format_strs is None:
        format_strs = ['.0f', '.1f']

    output_cols = [col for col in long_df.columns if col not in [row_byvar, col_byvar]]

    if len(format_strs) != len(output_cols):
        raise ValueError(f'expected {len(output_cols)} format strs, only got {len(format_strs)}: {format_strs}')

    df_dict = {}
    for i, col in enumerate(output_cols):
        df_dict[col] = long_counts_to_formatted_wide_df(
            long_df,
            col,
            row_byvar,
            col_byvar,
            sort_cols_as_numeric=sort_cols_as_numeric,
            sort_rows_as_numeric=sort_rows_as_numeric,
            sort_cols_as_portvar=sort_cols_as_portvar,
            sort_rows_as_portvar=sort_rows_as_portvar,
            format_str=format_strs[i]
        )

    return df_dict


def long_counts_to_formatted_wide_df(long_df: pd.DataFrame, col: str, row_byvar: str, col_byvar: str,
                                     sort_cols_as_numeric: bool =True, sort_rows_as_numeric: bool =True,
                                     sort_cols_as_portvar: bool=False, sort_rows_as_portvar: bool=False,
                                     format_str: str ='.0f'
                                     ) -> pd.DataFrame:

    wide_df = _long_counts_to_wide_df(
        long_df,
        col,
        row_byvar,
        col_byvar
    )

    return sort_and_apply_formatting_to_df(
        wide_df,
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        sort_cols_as_portvar=sort_cols_as_portvar,
        sort_rows_as_portvar=sort_rows_as_portvar,
        format_str=format_str
    )