from typing import Sequence, Tuple, Dict

import more_itertools
import pandas as pd
import matplotlib.pyplot as plt


def variables_drop_count_df_and_plt_figs(
    df: pd.DataFrame, var_names: Sequence[str], id_var: str
) -> Tuple[pd.DataFrame, Dict[str, plt.Figure]]:
    for_concat = []
    non_missing_counts = df[var_names].count()
    non_missing_counts.name = 'Count'
    for_concat.append(non_missing_counts)
    if id_var is not None:
        by_id_counts = _by_id_counts(df, var_names, id_var)
        by_id_counts.name = id_var
        for_concat.append(by_id_counts)

    count_with_all = len(df.dropna(subset=var_names))

    detail_df = pd.DataFrame(index=var_names, columns=['Min', 'Average', 'Median', 'Max', 'Mode'])
    figs: Dict[str, plt.Figure] = {}
    for var_name in var_names:
        _, hist_fig, min_, avg, median, max_, mode = _variable_drop_count_details(df, var_name, var_names,
                                                                                  count_with_all)
        detail_df.loc[var_name, 'Min'] = min_
        detail_df.loc[var_name, 'Average'] = avg
        detail_df.loc[var_name, 'Median'] = median
        detail_df.loc[var_name, 'Max'] = max_
        detail_df.loc[var_name, 'Mode'] = mode
        figs[var_name] = hist_fig

    out_df = pd.concat([*for_concat, detail_df], axis=1)
    return out_df, figs


def _by_id_counts(df: pd.DataFrame, var_names: Sequence[str], id_var: str) -> pd.Series:
    id_counts = df.groupby(id_var)[var_names].count()
    return (id_counts > 0).sum()


def _variable_drop_count_details(df: pd.DataFrame, var_name: str, var_names: Sequence[str],
                                 count_with_all: int
                                 ) -> Tuple[Dict[Sequence[str], int], plt.Figure, float, float, float, float, float]:
    other_var_names = [name for name in var_names if name != var_name]
    count_with_all_but_var = len(df.dropna(subset=other_var_names))
    gain_from_drop = count_with_all_but_var - count_with_all

    gains = _gain_from_drop_combos(df, var_name, var_names)

    gains_df = pd.DataFrame()
    gains_df['Counts'] = gains.values()
    hist_fig = _gains_histogram(gains_df)

    min_ = gains_df['Counts'].min()
    avg = gains_df['Counts'].mean()
    median = gains_df['Counts'].median()
    max_ = gains_df['Counts'].max()
    mode = gains_df['Counts'].mode().values[0]

    return gains, hist_fig, min_, avg, median, max_, mode


def _gain_from_drop_combos(df: pd.DataFrame, var_name: str, var_names: Sequence[str]):
    other_var_names = [name for name in var_names if name != var_name]
    gains = {}
    for other_var_combo in more_itertools.powerset(other_var_names):
        if not other_var_combo:
            continue
        full_combo = other_var_combo + (var_name,)
        count = len(df.dropna(subset=full_combo))
        count_without = len(df.dropna(subset=other_var_combo))
        gain = count_without - count
        gains[other_var_combo] = gain
    return gains


def _gains_histogram(gains_df: pd.DataFrame) -> plt.Figure:
    ax = gains_df['Counts'].hist()
    fig = ax.get_figure()
    plt.close()

    return fig