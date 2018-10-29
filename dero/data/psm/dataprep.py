import pandas as pd
from dero.data.psm.typing import StrListOrNone, TwoDfTuple

def treated_and_control_df_from_df(df: pd.DataFrame, treated_var: str, keep_vars: StrListOrNone = None) -> TwoDfTuple:
    treated_df = df[df[treated_var] == 1]
    control_df = df[df[treated_var] == 0]

    if keep_vars is not None:
        treated_df = treated_df[keep_vars]
        control_df = control_df[keep_vars]

    return treated_df, control_df

