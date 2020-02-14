from typing import Sequence, Optional

import pandas as pd
from pyexlatex import Document

from datacode.typing import (
    StrOrNone,
    DocumentOrLatexObjs,
    IntOrNone,
    FloatOrNone,
    IntSequenceOrNone,
    FloatSequenceOrNone
)
from datacode.summarize.subset.missing.summary.graphs.pctbyid import missing_pct_by_id_figure
from datacode.summarize.subset.missing.detail.main import obs_and_id_count_and_missing_pct_table

def missing_data_single_column_analysis(df: pd.DataFrame, col_with_missings: str, id_col: str,
                                        row_byvar: str, col_byvar: str, datevar: str,
                                        missing_tolerance: IntOrNone = 0, missing_quantile: FloatOrNone = None,
                                        summary_missing_tolerances: IntSequenceOrNone = (0, 5, 10),
                                        summary_missing_quantiles: FloatSequenceOrNone = None,
                                        sort_cols_as_numeric: bool = True, sort_rows_as_numeric: bool = True,
                                        sort_cols_as_portvar: bool=False, sort_rows_as_portvar: bool=False,
                                        count_format_str: str = '.0f', pct_format_str: str = '.1f',
                                        missing_display_str: str = 'Missing', period_display_name: str='Period',
                                        extra_caption: str='', table_extra_below_text: str='',
                                        table_align: str=None,
                                        outfolder: StrOrNone=None, as_document: bool=True,
                                        document_title: StrOrNone=None, authors: Optional[Sequence[str]] = None,
                                        ) -> DocumentOrLatexObjs:

    missing_pct_fig = missing_pct_by_id_figure(
        df,
        id_col,
        col_with_missings,
        outfolder=outfolder,
        outname=f'Percentage {missing_display_str} Obs by Firm for {col_with_missings}'
    )

    missing_pct_table = obs_and_id_count_and_missing_pct_table(
        df,
        col_with_missings,
        id_col,
        row_byvar,
        col_byvar,
        datevar,
        missing_tolerance=missing_tolerance,
        missing_quantile=missing_quantile,
        summary_missing_tolerances=summary_missing_tolerances,
        summary_missing_quantiles=summary_missing_quantiles,
        sort_cols_as_numeric=sort_cols_as_numeric,
        sort_rows_as_numeric=sort_rows_as_numeric,
        sort_cols_as_portvar=sort_cols_as_portvar,
        sort_rows_as_portvar=sort_rows_as_portvar,
        count_format_str=count_format_str,
        pct_format_str=pct_format_str,
        missing_display_str=missing_display_str,
        period_display_name=period_display_name,
        extra_caption=extra_caption,
        extra_below_text=table_extra_below_text,
        table_align=table_align,
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
        authors=authors
    )

    if outfolder:
        doc.to_pdf(
            outname=document_title,
            outfolder=outfolder
        )

    return doc