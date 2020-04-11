from typing import Sequence

import pandas as pd
from datacode.models.variables import Variable
from semopy import Optimizer

from datacode.sem.parse import lambda_df_from_opt


def get_latents(opt: Optimizer, mod_df: pd.DataFrame, latent_vars: Sequence[Variable]) -> pd.DataFrame:
    latent_df = _get_latents(opt, mod_df)
    rename_dict = {var.unique_key: var.name for var in latent_vars}
    return latent_df.rename(columns=rename_dict)


def _get_latents(opt: Optimizer, mod_df: pd.DataFrame) -> pd.DataFrame:
    model = opt.model
    lambda_df = lambda_df_from_opt(opt)
    for_mult_df = mod_df[lambda_df.index]
    factor_df = pd.DataFrame(
        for_mult_df.values @ lambda_df[model.vars['Latents']].values,
        columns=model.vars['Latents']
    )
    return factor_df
