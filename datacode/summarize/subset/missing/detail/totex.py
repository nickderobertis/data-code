import pyexlatex.table as lt
from datacode.typing import SimpleDfDict, StrOrNone
from datacode.summarize.subset.missing.detail.textfuncs import num_or_pct_word, pct_of_if_necessary
from datacode.typing import SimpleDfDict, StrOrNone, IntSequenceOrNone, IntOrNone, FloatSequenceOrNone, FloatOrNone

def missing_detail_df_dict_to_table_and_output(df_dict: SimpleDfDict, summary_panel: lt.Panel,
                                               row_byvar, col_byvar: str,
                                               id_var: str,
                                               count_with_missings_var: str,
                                               missing_tolerance: IntOrNone = 0, missing_quantile: FloatOrNone = None,
                                               summary_missing_tolerances: IntSequenceOrNone = (0, 5, 10),
                                               summary_missing_quantiles: FloatSequenceOrNone = None,
                                               missing_display_str: str = 'Missing',
                                               period_display_name: str='Period',
                                               extra_caption: str='', extra_below_text: str='',
                                               align: str=None,
                                               outfolder: StrOrNone=None) -> lt.Table:

    caption = f'Data Gap Analysis - {missing_display_str} {count_with_missings_var}'

    missing_display_str = missing_display_str.lower()

    if missing_display_str == 'missing':
        missing_long_display_str = 'missing information'
    else:
        missing_long_display_str = missing_display_str

    # percentage or number string
    summary_num_or_pct = num_or_pct_word(summary_missing_tolerances, summary_missing_quantiles)
    # % of if quantiles, empty string if tolerances
    percent_of_or_blank = pct_of_if_necessary(missing_tolerance, missing_quantile)

    below_text = f"""
    This table shows where the {count_with_missings_var} variable is {missing_long_display_str}. 
    For Panels A, B, C, and D, each item represents a subsample analysis where {row_byvar} is the value given by the row
    and where {col_byvar} is the value given by the column, while Panel E is a full sample analysis.
    Panel A describes the number of observations.
    Panel B describes the percentage of observations with {missing_long_display_str} for {count_with_missings_var}.
    Panel C describes the number of unique {id_var}s. 
    Panel D describes the percentage of unique {id_var}s which have more than {missing_tolerance}{percent_of_or_blank} 
    observations with {missing_long_display_str} for {count_with_missings_var}. 
    Panel E has four subsections. The first section shows the count of observations and the percentage that are 
    {missing_long_display_str}. The second section shows the count of unique {id_var}s. The third section shows
    summary statistics for the number of {period_display_name}s per {id_var}. The fourth section shows the percentage
    of {id_var}s which have at least the {summary_num_or_pct} of {period_display_name}s given by the column. 
    """ + extra_below_text

    if extra_caption:
        caption = f'{caption} - {extra_caption}'

    detail_data_tables = {
        name: lt.DataTable.from_df(df, include_index=True, extra_header=col_byvar)
        for name, df in df_dict.items()
    }
    detail_panels = [lt.Panel.from_data_tables([dt], name=name) for name, dt in detail_data_tables.items()]

    table = lt.Table.from_panel_list(
        detail_panels + [summary_panel],
        label_consolidation='str',
        below_text=below_text,
        caption=caption,
        align=align,
        top_left_corner_labels = row_byvar
    )

    if outfolder is not None:
        table.to_pdf_and_move(
            outname=caption,
            outfolder=outfolder
        )

    return table