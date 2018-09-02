import dero.latex.table as lt
from dero.data.typing import DfDict, StrOrNone

def missing_detail_df_dict_to_table_and_output(df_dict: DfDict, row_byvar, col_byvar: str,
                                               id_var: str,
                                               count_with_missings_var: str,
                                               missing_tolerance: int,
                                               extra_caption: str='', extra_below_text: str='',
                                               outfolder: StrOrNone=None) -> lt.Table:


    below_text = f"""
    This table shows any gaps in the {count_with_missings_var} variable where information is missing. 
    For all panels, each item represents a subsample analysis where {row_byvar} is the value given by the row
    and where {col_byvar} is the value given by the column.
    Panel A describes the number of observations.
    Panel B describes the percentage of observations with missing information for {count_with_missings_var}.
    Panel C describes the number of unique {id_var}s. 
    Panel D describes the percentage of unique {id_var}s which have more than {missing_tolerance} observations
    with missing information for {count_with_missings_var}. 
    """ + extra_below_text

    caption = f'Data Gap Analysis - {count_with_missings_var}'

    if extra_caption:
        caption = f'{caption} - {extra_caption}'

    table = lt.Table.from_panel_name_df_dict(
        df_dict,
        include_index=True,
        top_left_corner_labels=col_byvar,
        below_text=below_text,
        caption=caption
    )

    if outfolder is not None:
        table.to_pdf_and_move(
            outname=caption,
            outfolder=outfolder
        )

    return table