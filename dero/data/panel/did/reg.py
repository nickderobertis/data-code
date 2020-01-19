from typing import List, Tuple, Optional
import pandas as pd
from regtools.interact import _interaction_tuple_to_var_name
from regtools import reg_for_each_yvar_and_produce_summary


def diff_reg_for_each_yvar_and_produce_summary(diff_df: pd.DataFrame, yvars: List[str], treated_var: str,
                                               treated_time_var: str = 'After', xvars: Optional[List[str]] = None):
    interaction_tuple = (treated_var, treated_time_var)
    interaction_varname = _interaction_tuple_to_var_name(interaction_tuple)

    all_xvars = [treated_var, treated_time_var]
    if xvars is not None:
        all_xvars.extend(xvars)

    reg_list, summ = reg_for_each_yvar_and_produce_summary(
        diff_df,
        yvars,
        all_xvars,
        [treated_var, treated_time_var, interaction_varname],
        interaction_tuples=[interaction_tuple]
    )

    t_name = '$t$-stat'
    _add_t_of_last_interaction_to_horizontal_summary(summ.tables[0], interaction_tuple, reg_list, t_name=t_name)
    column_order = [treated_var, treated_time_var, interaction_varname, t_name, 'Controls', 'Adj-R2', 'N']
    summ.tables[0] = summ.tables[0][column_order]

    return reg_list, summ

def _add_t_of_last_interaction_to_horizontal_summary(sdf: pd.DataFrame, interaction_tuple: Tuple[str, str],
                                                     reg_list, t_name: str = '$t$-stat') -> None:
    """
    note: inplace

    Args:
        sdf:
        interaction_tuple:
        reg_list:
        t_name:

    Returns:

    """

    interaction_varname = _interaction_tuple_to_var_name(interaction_tuple)
    interaction_stderr = pd.Series([r.bse[interaction_varname] for r in reg_list], index=sdf.index)
    sdf[t_name] = sdf[interaction_varname].apply(lambda x: float(x.strip('*'))) / interaction_stderr
    sdf[t_name] = sdf[t_name].apply(lambda x: f'{x:.2f}')