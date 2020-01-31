import pyexlatex.table as lt

import pandas as pd

from datacode.summarize import format_numbers_to_decimal_places
from datacode.typing import DfDict, Document
from datacode.typing import DocumentOrTables, DocumentOrTablesOrNone


def outlier_by_column_summary(bad_df_dict: DfDict, selected_orig_df_dict: DfDict,
                              keep_num_rows: int =40, output: bool =False,
                              outdir: str = None, as_document=True, author: str=None) -> DocumentOrTables:

    all_tables = []
    for col in bad_df_dict:
        all_tables.append(
            outlier_summary_for_col(
                bad_df_dict,
                selected_orig_df_dict,
                col,
                keep_num_rows=keep_num_rows,
                output=False,
                as_document=False
            )
        )


    all_tables = [table for table in all_tables if table is not None]

    full_title = 'Outlier Summary'

    document = Document.from_ambiguous_collection(
        all_tables,
        title=full_title,
        author=author
    )

    if output:
        assert outdir is not None
        document.to_pdf_and_move(
            outdir,
            outname=full_title,
            as_document=True
        )

    if as_document:
        return document
    else:
        return all_tables


def outlier_summary_for_col(bad_df_dict: DfDict, selected_orig_df_dict: DfDict,
                            col: str, keep_num_rows: int =40, output: bool =False,
                            outdir: str = None, as_document=True, author: str=None) -> DocumentOrTablesOrNone:

    bad_df = bad_df_dict[col]
    selected_orig_df = selected_orig_df_dict[col]

    if len(bad_df) == 0:
        print(f'No outliers for {col}. Will not add tables.')
        return None

    bad_table = _firm_list_table_from_df(
        bad_df,
        col,
        keep_num_rows=keep_num_rows,
        caption=f'Largest Outliers for {col}',
        below_text=f'''This table shows the largest outliers for {col}.''',
        output=False
    )

    selected_df_tables = []
    processed_rows = 0
    while processed_rows < len(selected_orig_df):
        selected_df_table = _firm_list_table_from_df(
            selected_orig_df.iloc[processed_rows:processed_rows + keep_num_rows],
            col,
            keep_num_rows=keep_num_rows,
            caption=f'Outlier Firm Series for {col}',
            below_text=f'''This table shows observations leading up to, including, and after outliers for {col}.''',
            output=False
        )
        selected_df_tables.append(selected_df_table)
        processed_rows += keep_num_rows

    full_title = 'Outlier Summary for {col}'

    document = Document.from_ambiguous_collection(
        [bad_table] + selected_df_tables,
        title=full_title,
        author=author
    )

    if output:
        assert outdir is not None
        document.to_pdf_and_move(
            outdir,
            outname=full_title,
            as_document=True
        )

    if as_document:
        return document
    else:
        return [bad_table] + selected_df_tables





def _firm_list_table_from_df(df: pd.DataFrame, col: str,
                             keep_num_rows: int =40, caption: str =None,
                             below_text: str =None, output: bool =False,
                             outdir: str =None) -> lt.Table:

    if caption is None:
        caption =f'Largest Outliers for {col}'

    if below_text is None:
        below_text = f'''
This table shows the largest outliers for {col}.
'''

    formatted_df = df.iloc[:keep_num_rows].applymap(format_numbers_to_decimal_places)

    align_str = 'll' + 'c' * (len(formatted_df.columns) - 2)

    table = lt.Table.from_list_of_lists_of_dfs(
        [[formatted_df]],
        caption=caption,
        below_text=below_text,
        align=align_str,
        landscape=True
    )

    if output:
        assert outdir is not None
        table.to_pdf_and_move(
            outdir,
            outname=caption
        )

    return table
