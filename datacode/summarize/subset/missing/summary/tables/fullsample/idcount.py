import pandas as pd
import pyexlatex.table as lt

def by_id_count_data_table(df: pd.DataFrame, id_col: str) -> lt.DataTable:
    by_id_count_df = _by_id_count_df(df, id_col)

    by_id_count_dt = lt.DataTable.from_df(
        by_id_count_df,
        extra_header=id_col
    )

    return by_id_count_dt

def _by_id_count_df(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    by_id_count_df = pd.DataFrame(
        data=[
            (len(df[id_col].dropna().unique()),)
        ],
        columns=['#']
    )

    return by_id_count_df
