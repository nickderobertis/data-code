import pandas as pd
import pyexlatex.table as lt

from datacode.typing import IntSequenceOrNone, FloatSequenceOrNone
from datacode.summarize.subset.missing.summary.tables.fullsample.idcount import by_id_count_data_table
from datacode.summarize.subset.missing.summary.tables.fullsample.missingmore import missing_more_than_data_table
from datacode.summarize.subset.missing.summary.tables.fullsample.obs import obs_count_and_missing_data_table
from datacode.summarize.subset.missing.summary.tables.fullsample.periods import periods_data_table
from pyexlatex.table.models.spacing.columntable import ColumnPadTable

def missing_full_sample_summary_panel(df: pd.DataFrame, id_col: str, col_with_missings: str,
                                      missing_tolerances: IntSequenceOrNone = (0, 5, 10),
                                      missing_quantiles: FloatSequenceOrNone = None,
                                      missing_display_str: str = 'Missing', datevar: str = 'Date',
                                      pct_format_str: str = '.1f', period_display_name: str = 'Period') -> lt.Panel:

    obs_pct_dt = obs_count_and_missing_data_table(
        df,
        col_with_missings,
        missing_display_str=missing_display_str,
        pct_format_str=pct_format_str
    )

    by_id_count_dt = by_id_count_data_table(df, id_col)

    period_counts_dt = periods_data_table(
        df,
        id_col,
        datevar,
        period_display_name=period_display_name
    )

    missing_more_than_dt = missing_more_than_data_table(
        df,
        id_col,
        col_with_missings,
        missing_tolerances=missing_tolerances,
        missing_quantiles=missing_quantiles,
        missing_display_str=missing_display_str,
        period_display_name=period_display_name,
        pct_format_str=pct_format_str
    )

    # Create a single data table from the existing data tables
    combine_items = [
        obs_pct_dt,
        by_id_count_dt,
        period_counts_dt,
        missing_more_than_dt
    ]
    data_table = combine_items[0]
    for item in combine_items[1:]:
        data_table += ColumnPadTable()  # pad with a single column between each
        data_table += item

    panel = lt.Panel.from_data_tables(
        [data_table],
        name=f'{missing_display_str} {col_with_missings} Full Sample Summary'
    )

    return panel