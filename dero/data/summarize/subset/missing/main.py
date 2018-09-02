import pandas as pd
from dero.latex import Document

from dero.data.typing import StrOrNone, DocumentOrLatexObjs
from dero.data.summarize.subset.missing.summary.graphs.pctbyid import missing_pct_by_id_figure
from dero.data.summarize.subset.missing.detail.main import obs_and_id_count_and_missing_pct_table

def missing_data_single_column_analysis(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                        row_byvar: str, col_byvar: str,
                                        missing_tolerance: int=0,
                                        sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                        count_format_str: str = '.0f', pct_format_str: str = '.1f',
                                        missing_display_str: str = 'Missing',
                                        extra_caption: str='', table_extra_below_text: str='',
                                        outfolder: StrOrNone=None, as_document: bool=True,
                                        document_title: StrOrNone=None, author: StrOrNone=None,
                                        ) -> DocumentOrLatexObjs:

    missing_pct_fig = missing_pct_by_id_figure(
        df,
        id_col,
        col_with_missings,
        outfolder=outfolder
    )

    missing_pct_table = obs_and_id_count_and_missing_pct_table(
        df,
        col_with_missings,
        id_col,
        row_byvar,
        col_byvar,
        missing_tolerance=missing_tolerance,
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        count_format_str=count_format_str,
        pct_format_str=pct_format_str,
        missing_display_str=missing_display_str,
        extra_caption=extra_caption,
        extra_below_text=table_extra_below_text,
        outfolder=outfolder
    )

    if not as_document:
        return [missing_pct_fig, missing_pct_table]

    if document_title is None:
        document_title = f'{missing_display_str} {col_with_missings} Analysis'

    if extra_caption:
        document_title = f'{document_title} - {extra_caption}'

    doc = Document(
        [missing_pct_fig, missing_pct_table],
        title=document_title,
        author=author
    )

    doc.to_pdf_and_move(
        outname=document_title,
        outfolder=outfolder
    )

    return doc