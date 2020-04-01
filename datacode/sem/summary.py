from typing import Sequence, List, Dict

import pandas as pd
import numpy as np
from IPython.core.display import display, HTML
from semopy import Optimizer, inspect, gather_statistics

from datacode.models.variables import Variable


def structural_summary_dfs(opt: Optimizer, observed_endog_vars: Sequence[Variable],
                           all_vars: Sequence[Variable]) -> List[pd.DataFrame]:
    summ_dfs = _structural_summary_dfs(opt)
    rename_dict = {var.key: var.name for var in all_vars}
    for i, summ_df in enumerate(summ_dfs):
        summ_df.index.name = observed_endog_vars[i].name
        summ_df.rename(index=rename_dict, inplace=True)

    return summ_dfs


def _structural_summary_dfs(opt: Optimizer) -> List[pd.DataFrame]:
    return _param_summary_dfs(opt, opt.model.vars['ObsEndo'], operator='~')


def latent_summary_dfs(opt: Optimizer, measurement_dict: Dict[Variable, Sequence[Variable]],
                       all_vars: Sequence[Variable]) -> List[pd.DataFrame]:
    summ_dfs = _param_summary_dfs(opt, opt.model.vars['Latents'], operator='=~')
    rename_dict = {var.key: var.name for var in all_vars}
    final_summ_dfs = []
    for summ_df in summ_dfs:
        # Need to add the missing 1 coefficient to the df
        measure_var_key = summ_df.index.name
        # Get the variable which is missing
        missing_var = None
        measure_var = None
        for lhs_var, rhs_vars in measurement_dict.items():
            if lhs_var.key == measure_var_key:
                measure_var = lhs_var
                missing_var = rhs_vars[0]
        if missing_var is None or measure_var is None:
            raise ValueError(f'could not find {measure_var_key} in measurement dict {measurement_dict}')
        missing_row = pd.DataFrame(
            pd.Series([1] + [np.nan] * (len(summ_df.columns) - 1), name=missing_var.name, index=summ_df.columns)
        ).T
        final_summ_df = pd.concat([missing_row, summ_df])
        final_summ_df.index.name = measure_var.name
        final_summ_df.rename(index=rename_dict, inplace=True)
        final_summ_dfs.append(final_summ_df)
    return final_summ_dfs


def _param_summary_dfs(opt: Optimizer, var_keys: Sequence[str], operator: str = '~',
                       include_same_var: bool = False) -> List[pd.DataFrame]:
    results = inspect(opt)
    summ_dfs = []
    for var_key in var_keys:
        selected_results_mask = (results['lval'] == var_key) & (results['op'] == operator)
        if not include_same_var:
            selected_results_mask = selected_results_mask & (results['rval'] != var_key)
        selected_results = results.loc[selected_results_mask]
        summary = selected_results.drop(['lval', 'op'], axis=1).rename(columns={'Value': 'Coef.'}).set_index('rval')
        summary.index.name = var_key
        summ_dfs.append(summary)
    return summ_dfs


def correlation_dfs(opt: Optimizer, all_vars: Sequence[Variable]) -> List[pd.DataFrame]:
    summ_dfs = _correlation_dfs(opt)
    rename_dict = {var.key: var.name for var in all_vars}
    for i, summ_df in enumerate(summ_dfs):
        summ_df.index.name = rename_dict[summ_df.index.name]
        summ_df.rename(index=rename_dict, inplace=True)

    return summ_dfs


def _correlation_dfs(opt: Optimizer) -> List[pd.DataFrame]:
    return _param_summary_dfs(opt, opt.model.vars['All'], operator='~~', include_same_var=True)


def get_fit_statistics(opt: Optimizer) -> pd.DataFrame:
    stats = gather_statistics(opt)
    stat_data = {
        'Model $\chi^2$': stats.chi2[0],
        'Model $p$-value ($\chi^2$)': stats.chi2[1],
        'Baseline $\chi^2$': stats.chi2_baseline,
        'Degrees of Freedom': stats.dof,
        'Goodness of Fit Index (GFI)': stats.gfi,
        'Adjusted-GFI (AGFI)': stats.agfi,
        'Tucker-Lewis Index (TLI)': stats.nfi,
        'Comparative Fit Index (CFI)': stats.cfi,
        'Root Mean Square Error of Approximation (RMSEA)': stats.rmsea
    }
    df = pd.DataFrame(pd.Series(stat_data).apply(lambda x: f'{x:.3f}'), columns=['Statistics'])
    df.loc['Degrees of Freedom'] = stats.dof  # go back to integer
    return df


class SEMSummary:

    def __init__(self, opt: Optimizer, observed_endog_vars: Sequence[Variable],
                 measurement_dict: Dict[Variable, Sequence[Variable]],
                 all_vars: Sequence[Variable]):
        self.opt = opt
        self.observed_endog_vars = observed_endog_vars
        self.measurement_dict = measurement_dict
        self.all_vars = all_vars
        self.stats = gather_statistics(opt)

    @property
    def structural(self) -> List[pd.DataFrame]:
        return structural_summary_dfs(self.opt, self.observed_endog_vars, self.all_vars)

    @property
    def latent(self) -> List[pd.DataFrame]:
        return latent_summary_dfs(self.opt, self.measurement_dict, self.all_vars)

    @property
    def correlations(self) -> List[pd.DataFrame]:
        return correlation_dfs(self.opt, self.all_vars)

    @property
    def fit(self) -> pd.DataFrame:
        return get_fit_statistics(self.opt)

    def summary(self):
        display(self.fit)
        to_display = {
            'Structural': self.structural,
            'Measurement': self.latent,
            'Correlations': self.correlations,
        }
        for header, df_list in to_display.items():
            _display_header(header)
            for df in df_list:
                display(df)

    def _repr_html_(self):
        self.summary()


def _display_header(text: str, level: int = 4):
    display(HTML(f'<h{level}>{text}</h{level}>'))
