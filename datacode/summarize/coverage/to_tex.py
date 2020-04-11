import math
from typing import Optional, Dict, List

import pyexlatex as pl
import pandas as pd
import matplotlib.pyplot as plt

PLOTS_PER_FIG = 8


def variables_drop_panel(summ_df: pd.DataFrame, group_name: str, id_var: Optional[str] = None) -> pl.Panel:
    blank_label = pl.Label('', span=2 if id_var is not None else 1)
    real_label = pl.Label('Drop Statistics', span=5)
    labels = pl.LabelCollection([blank_label, real_label], underline='3-6')
    dt = pl.DataTable.from_df(summ_df, include_index=True, extra_header=labels)
    panel = pl.Panel.from_data_tables([dt], name=group_name)
    return panel


def variables_latex_figures(figs: Dict[str, plt.Figure], group_name: str,
                             sources_outfolder: str = '.') -> List[pl.Figure]:
    num_figures = math.ceil(len(figs) / PLOTS_PER_FIG)
    fig_tuples = list(figs.items())

    bot = 0
    top = PLOTS_PER_FIG
    out_figs = []
    for i in range(num_figures):
        if num_figures == 1:
            name = group_name
        else:
            name = f'{group_name} ({i + 1}/{num_figures})'
        selected_figs = dict(fig_tuples[bot:top])
        pl_fig = pl.Figure.from_dict_of_names_and_plt_figures(selected_figs, sources_outfolder, figure_name=name)
        out_figs.append(pl_fig)
        bot += PLOTS_PER_FIG
        top += PLOTS_PER_FIG

    return out_figs