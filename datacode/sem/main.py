from typing import Union, Sequence, Dict, Any, List

import pandas as pd

from datacode.models.variables import Variable
from datacode.models.source import DataSource
from datacode.sem.df import assemble_model_data
from datacode.sem.latents import get_latents
from datacode.sem.model import model_str
from datacode.sem.run import run_model
from datacode.sem.summary import SEMSummary


class SEM:

    def __init__(self, data: Union[DataSource, pd.DataFrame], structural_dict: Dict[Variable, Sequence[Variable]],
                 measurement_dict: Dict[Variable, Sequence[Variable]],
                 var_corr_groups: Sequence[Sequence[Variable]], scale: bool = True,
                 robust_scale: bool = False, pos_def_warnings: str = 'ignore', **scale_kwargs):
        self.data = data
        self.structural_dict = structural_dict
        self.measurement_dict = measurement_dict
        self.var_corr_groups = var_corr_groups
        self.scale = scale
        self.robust_scale = robust_scale
        self.scale_kwargs = scale_kwargs
        self.pos_def_warnings = pos_def_warnings
        self.model_def = model_str(structural_dict, measurement_dict, var_corr_groups)

        self.opt = None
        self.model = None
        self.summary = None
        self._model_df = None

    def fit(self, **opt_kwargs) -> SEMSummary:
        self.opt = run_model(self.model_def, self.model_df, pos_def_warnings=self.pos_def_warnings, **opt_kwargs)
        self.model = self.opt.model
        self.summary = SEMSummary(
            self.opt,
            self.observed_endog_vars,
            self.structural_dict,
            self.measurement_dict,
            self.var_corr_groups,
            self.all_vars,
            self.scale,
            self.robust_scale
        )
        return self.summary

    @property
    def df(self) -> pd.DataFrame:
        if isinstance(self.data, pd.DataFrame):
            return self.data
        elif isinstance(self.data, DataSource):
            return self.data.df
        else:
            raise ValueError(f'did not pass DataSource or DataFrame for data, got type {type(self.data)}')

    @property
    def model_df(self) -> pd.DataFrame:
        if self._model_df is None:
            self._model_df = assemble_model_data(
                self.df,
                self.observed_vars,
                scale=self.scale,
                robust_scale=self.robust_scale,
                **self.scale_kwargs
            )
        return self._model_df

    @property
    def observed_endog_vars(self) -> Sequence[Variable]:
        return [var for var in self.structural_dict.keys() if var not in self.measurement_dict.keys()]

    @property
    def latent_vars(self) -> Sequence[Variable]:
        return list(self.measurement_dict.keys())

    @property
    def all_vars(self) -> List[Variable]:
        collected = []
        for var_dict in [self.structural_dict, self.measurement_dict]:
            for lhs_var, rhs_vars in var_dict.items():
                collected.append(lhs_var)
                collected.extend(rhs_vars)
        for var_seq in self.var_corr_groups:
            collected.extend(var_seq)
        return list(set(collected))

    @property
    def observed_vars(self) -> List[Variable]:
        return [var for var in self.all_vars if var not in self.latent_vars]

    @property
    def latents(self) -> pd.DataFrame:
        return get_latents(self.opt, self.model_df, self.latent_vars)

