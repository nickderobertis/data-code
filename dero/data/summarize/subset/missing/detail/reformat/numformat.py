import pandas as pd

def _apply_formatting_to_count_df(counts: pd.DataFrame,
                                  sort_cols_as_numeric: bool=True, sort_rows_as_numeric: bool=True,
                                  format_str: str='.0f'
                                  ) -> pd.DataFrame:

    if sort_cols_as_numeric:
        counts.columns = [_to_int_if_possible(col) for col in counts.columns]
        counts.sort_index(axis=1, inplace=True)

    if sort_rows_as_numeric:
        counts.index = [_to_int_if_possible(col) for col in counts.index]
        counts.sort_index(inplace=True)

    return counts.applymap(lambda x: f'{x:{format_str}}')

def _to_int_if_possible(s):
    try:
        return int(float(s))
    except ValueError:
        return s