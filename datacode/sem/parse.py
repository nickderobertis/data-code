import pandas as pd
from semopy import Optimizer, inspect

from datacode.utils.suppress import no_stdout


def lambda_df_from_opt(opt: Optimizer) -> pd.DataFrame:
    with no_stdout():
        matrix_results = inspect(opt, mode='mx')
    lambda_df = matrix_results[1][1]
    return lambda_df
