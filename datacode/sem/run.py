import io
from contextlib import redirect_stdout

import pandas as pd
import numpy as np
from semopy import Model, Optimizer

from datacode.sem.exc import SampleCovMatrixNotPositiveDefiniteException, ModelCovMatrixNotPositiveDefiniteException


def run_model(model_def: str, df: pd.DataFrame, pos_def_warnings: str = 'ignore',
              **opt_kwargs) -> Optimizer:
    pos_def_warnings = _validate_pos_def(pos_def_warnings)
    model = Model(model_def)
    model.load_dataset(df)

    if pos_def_warnings == 'error':
        try:
            np.linalg.cholesky(model.mx_cov)
        except np.linalg.LinAlgError:
            raise SampleCovMatrixNotPositiveDefiniteException

    with io.StringIO() as buf, redirect_stdout(buf):
        opt = Optimizer(model)
        opt.optimize(**opt_kwargs)
        stdout_val = buf.getvalue()

    if pos_def_warnings == 'error':
        sigma = opt.get_sigma()[0]
        try:
            np.linalg.cholesky(sigma)
        except np.linalg.LinAlgError:
            raise ModelCovMatrixNotPositiveDefiniteException

    if pos_def_warnings == 'print':
        print(stdout_val)

    opt._sample_cov_pos_df = True
    opt._model_cov_pos_df = True
    if 'a sample covariance matrix is not positive-definite' in stdout_val:
        opt._sample_cov_pos_df = False
    if 'resulting model-implied covariance matrix is not postive-definite' in stdout_val:
        opt._model_cov_pos_df = False

    return opt


def _validate_pos_def(pos_def_str: str) -> str:
    pos_def_str = pos_def_str.casefold().strip()
    if pos_def_str not in ('print', 'ignore', 'error'):
        raise ValueError('invalid positive definite string, pass one of print, ignore, or error')
    return pos_def_str
