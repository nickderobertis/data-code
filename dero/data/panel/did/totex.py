from typing import Optional, List
import pyexlatex.table as lt

def diff_reg_summary_to_latex_table_and_output(summ, entity_var: str,
                                               caption: str = 'Difference-in-Difference Regressions',
                                               extraname: Optional[str] = None,
                                               below_text: Optional[str] = None,
                                               outfolder: Optional[str] = None,
                                               xvars: Optional[List[str]] = None) -> lt.Table:

    if extraname is not None:
        caption = f'{caption} - {extraname}'

    if below_text is None:
        below_text = f"""
        Difference-in-difference regressions are reported. For this sample, the first and last observations for each
        {entity_var} are selected. The first observation is considered before treatment, while the last observation
        is considered after treatment. 
        """

    if xvars is not None:
        below_text += f'Controls include {_comma_and_join(xvars)}.'

    table = lt.Table.from_list_of_lists_of_dfs(
        [[summ.tables[0]]],
        include_index=True,
        caption=caption,
        below_text=below_text
    )

    if outfolder is not None:
        table.to_pdf_and_move(
            outname=caption,
            outfolder=outfolder
        )

    return table

def _comma_and_join(str_list: List[str]) -> str:
    until_and = ', '.join(str_list[:-1])
    return f'{until_and}, and {str_list[-1]}'