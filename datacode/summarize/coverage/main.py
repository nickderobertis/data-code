from typing import Sequence, Dict, Optional

import pandas as pd
import pyexlatex as pl

from datacode.models.variables import Variable
from datacode.summarize.coverage.counts import variables_drop_count_df_and_plt_figs
from datacode.summarize.coverage.to_tex import variables_drop_panel, variables_latex_figures


def variables_drop_obs_doc(df: pd.DataFrame, var_groups: Dict[str, Sequence[Variable]],
                           id_var: Optional[str] = None, sources_outfolder: str = '.') -> pl.Document:
    """
    Generates a summary document of coverage of variables

    Produce a summary document of first a table which shows for each variable,
    the count of non-missing observations, and the count of coins which have
    at least one non-missing observation. Then it shows statistics on what
    would happen to the observations if the variable was dropped from the sample.
    These numbers are calculated by looking at every combination of the
    variables and how much the observation count would increase if this
    variable was excluded from the analysis. Summary statistics are shown for that.
    Then the following pages in the document are histograms of the same analysis,
    showing the distribution of observation counts that could be regained if we
    excluded that variable across the combinations of the other variables.

    :param df:
    :param var_groups: keys are names of variable groups, values are variables in the group
    :param id_var:
    :param sources_outfolder:
    :return:
    """
    panels = []
    all_figures = []
    for group_name, selected_vars in var_groups.items():
        var_names = [var.name for var in selected_vars]
        summ_df, figs = variables_drop_count_df_and_plt_figs(df, var_names, id_var)
        panel = variables_drop_panel(summ_df, group_name, id_var)
        panels.append(panel)
        figures = variables_latex_figures(figs, group_name, sources_outfolder)
        all_figures.extend(figures)
    tab = pl.Table.from_panel_list(panels, caption='Variable Drop Analysis')
    doc = pl.Document([tab] + all_figures)
    return doc
