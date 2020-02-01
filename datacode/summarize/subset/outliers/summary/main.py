import pandas as pd

from datacode.summarize.subset.outliers.summary.overview import bad_df_summary_df
from datacode.summarize.subset.outliers.summary.stats import full_df_summary_stats_df
from datacode.summarize.subset.outliers.select import drop_outliers_by_cutoffs
from datacode.summarize.subset.outliers.summary.totex import df_dict_to_table
from datacode.summarize.subset.outliers.typing import (
    MinMaxDict,
    FloatSequence
)
from datacode.typing import DocumentOrTables


def outlier_overview_summary_page_table(df: pd.DataFrame, bad_df: pd.DataFrame,
                                        min_max_dict: MinMaxDict,
                                        bad_column_name: str = '_bad_column',
                                        firm_id_col: str = 'TICKER',
                                        outdir: str = None, as_document=True, author: str = None,
                                        caption: str='Outlier Overview and Summary Statistics',
                                        quants: FloatSequence=(0.01, 0.1, 0.5, 0.9, 0.99)
                                        ) -> DocumentOrTables:

    outlier_cols = list(min_max_dict.keys())
    valid_df = drop_outliers_by_cutoffs(df, min_max_dict)

    overview_df = bad_df_summary_df(
        bad_df,
        min_max_dict,
        firm_id_col=firm_id_col,
        bad_column_name=bad_column_name
    )

    outlier_stats, full_stats, no_outlier_stats = [full_df_summary_stats_df(
        current_df,
        outlier_cols,
        quants=quants
    ) for current_df in (bad_df, df, valid_df)]

    panel_name_df_dict = {
        'Outlier Overview by Variable': overview_df,
        'Summary Statistics - Outlier Observations': outlier_stats,
        'Summary Statistics - Full Sample': full_stats,
        'Summary Statistics - Trimmed Sample': no_outlier_stats
    }

    document_or_table = df_dict_to_table(
        panel_name_df_dict,
        as_document=as_document,
        author=author,
        caption=caption,
    )

    if outdir:
        document_or_table.to_pdf_and_move(
            outdir,
            outname=caption
        )

    return document_or_table



