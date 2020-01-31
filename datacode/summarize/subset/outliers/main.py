import pandas as pd

from pyexlatex import Document
from datacode.summarize.subset.outliers.select import outlier_summary_dicts
from datacode.summarize.subset.outliers.detail.totex import outlier_by_column_summary
from datacode.summarize.subset.outliers.summary.main import outlier_overview_summary_page_table
from datacode.summarize.subset.outliers.typing import (
    AssociatedColDict,
    MinMaxDict,
    BoolDict,
    StrList,
    FloatSequence
)
from datacode.typing import DocumentOrTables


def outlier_summary_tables(df: pd.DataFrame, associated_col_dict: AssociatedColDict,
                           min_max_dict: MinMaxDict,
                           ascending_sort_dict: BoolDict = None,
                           always_associated_cols: StrList = None, bad_column_name: str = '_bad_column',
                           num_firms: int = 3, firm_id_col: str = 'TICKER',
                           date_col: str = 'Date',
                           begin_datevar: str = 'Begin Date',
                           end_datevar: str = 'End Date',
                           expand_months: int = 3,
                           keep_num_rows: int = 40, output: bool = True,
                           outdir: str = None, as_document=True, author: str = None,
                           title: str=None,
                           overview_caption: str='Outlier Overview and Summary Statistics',
                           quants: FloatSequence=(0.01, 0.1, 0.5, 0.9, 0.99)
                           ) -> DocumentOrTables:

    bad_df_dict, selected_orig_df_dict, bad_df = outlier_summary_dicts(
        df,
        associated_col_dict,
        min_max_dict,
        ascending_sort_dict=ascending_sort_dict,
        always_associated_cols=always_associated_cols,
        num_firms=num_firms,
        firm_id_col=firm_id_col,
        date_col=date_col,
        expand_months=expand_months,
        begin_datevar=begin_datevar,
        end_datevar=end_datevar,
        bad_column_name=bad_column_name
    )

    overview_table = outlier_overview_summary_page_table(
        df,
        bad_df,
        min_max_dict,
        bad_column_name=bad_column_name,
        firm_id_col=firm_id_col,
        as_document=False,
        caption=overview_caption,
        quants=quants
    )

    detail_tables = outlier_by_column_summary(
        bad_df_dict,
        selected_orig_df_dict,
        keep_num_rows=keep_num_rows,
        output=output,
        outdir=outdir,
        as_document=False,
    )

    all_tables = [overview_table] + detail_tables

    if as_document:
        document = Document.from_ambiguous_collection(
            all_tables,
            author=author,
            title=title
        )
        if outdir:
            document.to_pdf_and_move(
                outdir,
                outname=title
            )
        return document

    return all_tables
