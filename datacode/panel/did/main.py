from typing import List, Optional
import pandas as pd
from datacode.panel.did.dataprep import diff_df_from_panel_df
from datacode.panel.did.reg import diff_reg_for_each_yvar_and_produce_summary
from datacode.panel.did.totex import diff_reg_summary_to_latex_table_and_output
import pyexlatex.table as lt

def diff_reg_for_each_yvar_from_panel_df_latex_table(df: pd.DataFrame, entity_var: str, time_var: str, treated_var: str,
                                                     yvars: List[str], treated_time_var: str = 'After',
                                                     xvars: Optional[List[str]] = None,
                                                     caption: str = 'Difference-in-Difference Regressions',
                                                     extraname: Optional[str] = None,
                                                     below_text: Optional[str] = None,
                                                     outfolder: Optional[str] = None) -> lt.Table:
    reg_list, summ = diff_reg_for_each_yvar_from_panel_df(
        df,
        entity_var,
        time_var,
        treated_var,
        yvars,
        treated_time_var=treated_time_var,
        xvars=xvars
    )

    table = diff_reg_summary_to_latex_table_and_output(
        summ,
        entity_var,
        caption=caption,
        extraname=extraname,
        below_text=below_text,
        outfolder=outfolder,
        xvars=xvars
    )

    return table

def diff_reg_for_each_yvar_from_panel_df(df: pd.DataFrame, entity_var: str, time_var: str, treated_var: str,
                                         yvars: List[str], treated_time_var: str = 'After',
                                         xvars: Optional[List[str]] = None):
    diff_df = diff_df_from_panel_df(
        df,
        entity_var,
        time_var,
        treated_var,
        treated_time_var=treated_time_var
    )

    reg_list, summ = diff_reg_for_each_yvar_and_produce_summary(
        diff_df,
        yvars,
        treated_var,
        treated_time_var=treated_time_var,
        xvars=xvars
    )

    return reg_list, summ