import pandas as pd
import pyexlatex.table as lt

def periods_data_table(df: pd.DataFrame, id_col: str, datevar: str,
                       period_display_name: str='Period') -> lt.DataTable:

    period_counts_df = _period_counts_df(
        df,
        id_col,
        datevar
    )

    period_counts_dt = lt.DataTable.from_df(
        period_counts_df,
        extra_header=f'{id_col}-{period_display_name}s'
    )

    return period_counts_dt

def _period_counts_df(df: pd.DataFrame, id_col: str, datevar: str) -> pd.DataFrame:
    date_counts = df.dropna().groupby(id_col)[datevar].count()

    period_counts_df = pd.DataFrame(
        data=[
            (
                date_counts.mean(),
                date_counts.median(),
                date_counts.max()
            )
        ],
        columns=['Mean', 'Median', 'Max']
    )

    period_counts_df = period_counts_df.applymap(lambda x: f'{x:.0f}')

    return period_counts_df