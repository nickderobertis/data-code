from typing import Sequence

import pandas as pd
import sklearn.preprocessing as sklp

from datacode.models.variables.variable import Variable


def assemble_model_data(df: pd.DataFrame, variables: Sequence[Variable], scale: bool = True,
                        robust_scale: bool = False, **scale_kwargs) -> pd.DataFrame:
    _validate_scale(scale, robust_scale)
    mod_df = _assemble_model_data(df, variables)
    if scale:
        sklp.scale(mod_df, copy=False, **scale_kwargs)
    if robust_scale:
        sklp.robust_scale(mod_df, copy=False, **scale_kwargs)

    return mod_df


def _assemble_model_data(df: pd.DataFrame, variables: Sequence[Variable]) -> pd.DataFrame:
    var_names = [var.name for var in variables]
    rename_dict = {var.name: var.unique_key for var in variables}
    return df[var_names].rename(columns=rename_dict).reset_index(drop=True)


def _validate_scale(scale: bool, robust_scale: bool):
    if scale and robust_scale:
        raise ValueError('must select one or neither of scale or robust_scale, not both')
