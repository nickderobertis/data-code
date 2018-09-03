import dero.latex.table as lt
from dero.data.typing import SimpleDfDict, StrOrNone

def missing_detail_df_dict_to_table_and_output(df_dict: SimpleDfDict, summary_panel: lt.Panel,
                                               row_byvar, col_byvar: str,
                                               id_var: str,
                                               count_with_missings_var: str,
                                               missing_tolerance: int,
                                               missing_display_str: str = 'Missing',
                                               extra_caption: str='', extra_below_text: str='',
                                               align: str=None,
                                               outfolder: StrOrNone=None) -> lt.Table:

    caption = f'Data Gap Analysis - {missing_display_str} {count_with_missings_var}'

    missing_display_str = missing_display_str.lower()

    if missing_display_str == 'missing':
        missing_long_display_str = 'missing information'
    else:
        missing_long_display_str = missing_display_str


    below_text = f"""
    This table shows where the {count_with_missings_var} variable is {missing_long_display_str}. 
    For all panels, each item represents a subsample analysis where {row_byvar} is the value given by the row
    and where {col_byvar} is the value given by the column.
    Panel A describes the number of observations.
    Panel B describes the percentage of observations with {missing_long_display_str} for {count_with_missings_var}.
    Panel C describes the number of unique {id_var}s. 
    Panel D describes the percentage of unique {id_var}s which have more than {missing_tolerance} observations
    with {missing_long_display_str} for {count_with_missings_var}. 
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