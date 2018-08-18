import pandas as pd

from dero.data.summarize.outliers.select import outlier_summary_dicts
from dero.data.summarize.outliers.totex import outlier_summary
from dero.data.summarize.outliers.typing import (
    DocumentOrTables,
    AssociatedColDict,
    MinMaxDict,
    BoolDict,
    StrList,
    Document
)

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
                           outdir: str = None, as_document=True, author: str = None
                           ) -> DocumentOrTables:

    bad_df_dict, selected_orig_df_dict = outlier_summary_dicts(
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

    document_or_tables = outlier_summary(
        bad_df_dict,
        selected_orig_df_dict,
        keep_num_rows=keep_num_rows,
        output=output,
        outdir=outdir,
        as_document=as_document,
        author=author
    )

    return document_or_tables
