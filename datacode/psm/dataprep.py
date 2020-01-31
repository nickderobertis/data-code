import pandas as pd
from datacode.psm.typing import StrListOrNone, TwoDfTuple

def treated_and_control_df_from_df(df: pd.DataFrame, treated_var: str, entity_var: str,
                                   keep_vars: StrListOrNone = None) -> TwoDfTuple:
    rows_with_treatment = df[df[treated_var] == 1]
    # Cannot simply take where treatment is zero as control, as some entities may be treated for only a portion of the
    # sample. If this is the case, would end up with the entity in both dfs. Must set control as those entities
    # that are never treated
    treated_entities = rows_with_treatment[entity_var].unique()
    treated_df = df[df[entity_var].isin(treated_entities)]
    control_df = df[~df[entity_var].isin(treated_entities)]


    if keep_vars is not None:
        treated_df = treated_df[keep_vars]
        control_df = control_df[keep_vars]

    return treated_df, control_df

