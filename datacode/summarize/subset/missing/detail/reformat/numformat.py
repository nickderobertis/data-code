import pandas as pd

from datacode.portfolio.sort import sort_portfolio_index, sort_portfolio_columns

def sort_and_apply_formatting_to_df(counts: pd.DataFrame,
                                  sort_cols_as_numeric: bool=True, sort_rows_as_numeric: bool=True,
                                  sort_cols_as_portvar: bool=False, sort_rows_as_portvar: bool=False,
                                  format_str: str='.0f'
                                  ) -> pd.DataFrame:

    if sort_cols_as_numeric:
        counts.columns = [_to_int_if_possible(col) for col in counts.columns]
        counts.sort_index(axis=1, inplace=True)

    if sort_rows_as_numeric:
        counts.index = [_to_int_if_possible(col) for col in counts.index]
        counts.sort_index(inplace=True)

    if sort_cols_as_portvar:
        assert not sort_cols_as_numeric
        counts = sort_portfolio_columns(counts)

    if sort_rows_as_portvar:
        assert not sort_rows_as_numeric
        counts = sort_portfolio_index(counts)

    return counts.applymap(lambda x: f'{x:{format_str}}')

def _to_int_if_possible(s):
    try:
        return int(float(s))
    except ValueError:
        return s