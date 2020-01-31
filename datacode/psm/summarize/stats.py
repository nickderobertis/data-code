from functools import partial
import pandas as pd
import numpy as np
from datacode.summarize import format_numbers_to_decimal_places
from datacode.formatters.stars import convert_to_stars
from datacode.psm.names import matched_var, t_stat_var, diff_var
from datacode.psm.typing import StrList



def matching_summary_stats(df: pd.DataFrame, matched_df: pd.DataFrame,
                           treated_var: str, describe_vars: StrList,
                           entity_var: str,
                           control_name: str = 'Control', treated_name: str = 'Treated') -> pd.DataFrame:
    common_args = (
        treated_var,
        describe_vars,
        entity_var
    )

    common_kwargs = dict(
        treated_name=treated_name
    )

    summ = mean_and_diff_df_by_treatment(
        matched_df,
        *common_args,
        control_name=matched_var,
        **common_kwargs
    )

    control_summ = mean_and_diff_df_by_treatment(
        df,
        *common_args,
        control_name=control_name,
        **common_kwargs
    )

    summ[control_name] = control_summ[control_name]
    summ = summ[[control_name, treated_name, matched_var, diff_var, t_stat_var]]

    return summ


def mean_and_diff_df_by_treatment(df: pd.DataFrame, treated_var: str, describe_vars: StrList,
                                  entity_var: str, num_decimals: int = 2, coerce_ints: bool = True,
                                  control_name: str = 'Control', treated_name: str = 'Treated') -> pd.DataFrame:
    common_args = (
        treated_var,
        describe_vars,
        entity_var
    )

    common_kwargs = dict(
        treated_name=treated_name,
        control_name=control_name
    )

    mean_df = _stat_df_by_treatment(
        df,
        *common_args,
        stat='mean',
        **common_kwargs
    )

    std_df = _stat_df_by_treatment(
        df,
        *common_args,
        stat='std',
        **common_kwargs
    )

    count_df = _stat_df_by_treatment(
        df,
        *common_args,
        stat='count',
        **common_kwargs
    )

    diff_t = _diff_and_t_df_from_mean_and_std_df(
        mean_df,
        std_df,
        count_df,
        control_name=control_name,
        treated_name=treated_name
    )

    summ = pd.concat([mean_df, diff_t], axis=1)
    summ = _format_summary_df(summ, num_decimals=num_decimals, coerce_ints=coerce_ints)

    return summ


def _format_summary_df(df: pd.DataFrame, float_format: str = '.2f', num_decimals: int = 2,
                       coerce_ints: bool = True,) -> pd.DataFrame:
    #     num_formatter = lambda x: f'{x:{float_format}}' if isinstance(x, float) else x
    num_formatter = partial(format_numbers_to_decimal_places, decimals=num_decimals, coerce_ints=coerce_ints)
    df[diff_var] = df[diff_var].apply(num_formatter)
    df[diff_var] = df[diff_var] + df[t_stat_var].apply(
        lambda x: convert_to_stars(x) if isinstance(x, float) else x
    )
    df = df.applymap(num_formatter)

    return df


def _stat_df_by_treatment(df: pd.DataFrame, treated_var: str, describe_vars: StrList, entity_var: str ,
                          stat: str = 'mean',
                          control_name: str = 'Control', treated_name: str = 'Treated') -> pd.DataFrame:
    grouped = df.groupby(treated_var)[describe_vars]
    agg_func = getattr(grouped, stat)
    stat_df = agg_func().T
    stat_df.columns = [control_name, treated_name]
    count_series = _count_series(
        df,
        treated_var,
        entity_var,
        control_name=control_name,
        treated_name=treated_name
    )
    stat_df = stat_df.append(count_series)

    return stat_df

def _diff_and_t_df_from_mean_and_std_df(mean_df: pd.DataFrame, std_df: pd.DataFrame, count_df: pd.DataFrame,
                                        control_name: str = 'Control',
                                        treated_name: str = 'Treated') -> pd.DataFrame:
    diff = mean_df[treated_name] - mean_df[control_name]
    standard_errors = (
                              (std_df[control_name] ** 2 ) /count_df[control_name] +
                              (std_df[treated_name] ** 2 ) /count_df[treated_name]
                      ) ** 0.5
    t_stats = diff /standard_errors
    t_stats = t_stats.replace(np.inf, '')

    df = pd.DataFrame([diff, t_stats]).T
    colnames = [diff_var, t_stat_var]
    df.columns = colnames
    df.loc['N', colnames] = ''

    return df

def _count_series(df: pd.DataFrame, treated_var: str, entity_var: str,
                  control_name: str = 'Control', treated_name: str = 'Treated') -> pd.Series:
    count_s = df.groupby(treated_var)[entity_var].count()
    count_s.index = [control_name, treated_name]
    count_s.name = 'N'

    return count_s