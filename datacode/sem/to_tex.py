from typing import List, Sequence, Optional, Union

import pandas as pd
import pyexlatex as pl

from datacode.sem.constants import STANDARD_SCALE_MESSAGE, ROBUST_SCALE_MESSAGE


def summary_latex_table(fit_df: pd.DataFrame, structural_dfs: Sequence[pd.DataFrame],
                        latent_dfs: Sequence[pd.DataFrame], scale: bool, robust_scale: bool,
                        equations: Optional[Sequence[pl.Equation]] = None,
                        begin_below_text: Optional[str] = None,
                        end_below_text: Optional[str] = None, replace_below_text: Optional[str] = None,
                        caption: str = 'Structural Equation Model (SEM)') -> pl.Table:
    below_text = _below_text(
        scale,
        robust_scale,
        equations=equations,
        begin_below_text=begin_below_text,
        end_below_text=end_below_text,
        replace_below_text=replace_below_text
    )
    panels = _summary_panels(fit_df, structural_dfs, latent_dfs)
    table = pl.Table.from_panel_list(
        panels,
        caption=caption,
        below_text=below_text
    )
    return table


def _below_text(scale: bool, robust_scale: bool, equations: Optional[Sequence[pl.Equation]] = None,
                begin_below_text: Optional[str] = None,
                end_below_text: Optional[str] = None, replace_below_text: Optional[str] = None) -> Union[str, List[str]]:
    if replace_below_text:
        return replace_below_text

    below_text = []
    if begin_below_text:
        below_text.append(begin_below_text)
    if equations:
        below_text.append('The model is represented by the following equations:')
        below_text.extend(equations)
    if scale:
        below_text.append(STANDARD_SCALE_MESSAGE)
    if robust_scale:
        below_text.append(ROBUST_SCALE_MESSAGE)
    if end_below_text:
        below_text.append(end_below_text)

    return below_text


def _summary_panels(fit_df: pd.DataFrame, structural_dfs: Sequence[pd.DataFrame],
                    latent_dfs: Sequence[pd.DataFrame]) -> List[pl.Panel]:
    fit_panel = pl.Panel.from_df_list([fit_df], name='Goodness of Fit', include_index=True, include_columns=False)
    structural_dts = _coefficient_data_tables(structural_dfs)
    structural_panel = pl.Panel.from_data_tables(structural_dts, name='Structural Equations', )
    latent_dts = _coefficient_data_tables(latent_dfs)
    latent_panel = pl.Panel.from_data_tables(latent_dts, name='Latent Equations')
    return [fit_panel, structural_panel, latent_panel]


def _coefficient_data_tables(coef_dfs: Sequence[pd.DataFrame]) -> List[pl.DataTable]:
    dts = []
    for coef_df in coef_dfs:
        fmt_coef_df = coef_df.applymap(lambda x: f'{x:.3f}' if not pd.isnull(x) else x).fillna('')
        dt = pl.DataTable.from_df(fmt_coef_df, include_index=True, extra_header=coef_df.index.name)
        dts.append(dt)
    return dts
