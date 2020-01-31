import pandas as pd
from datacode.typing import StrList

from datacode.typing import IntOrNone
from datacode.summarize.subset.missing.detail.textfuncs import (
    missing_more_than_str,
    missing_more_than_pct_str,
    missing_tolerance_count_str,
    id_count_str
)


def by_id_pct_long_df(df: pd.DataFrame, byvars: StrList, id_var: str,
                      count_with_missings_var: str, missing_tolerance: IntOrNone = 0,
                      missing_quantile: IntOrNone = None,
                      missing_display_str: str = 'Missing') -> pd.DataFrame:

    by_id_var = _by_id_count_long_df(
        df,
        byvars,
        id_var,
        count_with_missings_var,
        missing_tolerance=missing_tolerance,
        missing_quantile=missing_quantile,
        missing_display_str=missing_display_str
    )

    name_args = (missing_tolerance, missing_quantile, missing_display_str, id_var)
    missing_more_than_name = missing_more_than_str(*name_args)
    missing_tolerance_count_name = missing_tolerance_count_str(*name_args)
    id_count_name = id_count_str(id_var)

    by_id_var[id_count_name] = by_id_var[missing_more_than_name] + \
                                   by_id_var[missing_tolerance_count_name]

    by_id_var[missing_more_than_pct_str(missing_tolerance, missing_quantile, missing_display_str, id_var)] = \
        (by_id_var[missing_more_than_name] /
        by_id_var[id_count_name]) * 100

    by_id_var.drop([
        missing_more_than_name,
        missing_tolerance_count_name
    ], axis=1, inplace=True)

    return by_id_var


def _by_id_count_long_df(df: pd.DataFrame, byvars: StrList, id_var: str,
                         count_with_missings_var: str, missing_tolerance: IntOrNone = 0,
                         missing_quantile: IntOrNone = None,
                         missing_display_str: str = 'Missing') -> pd.DataFrame:
    """
    Note: uses missing_quantile if specified, else uses missing_tolerance

    Args:
        df:
        byvars:
        id_var:
        count_with_missings_var:
        missing_tolerance:
        missing_quantile:
        missing_display_str:

    Returns:

    Raises:
        ValueError: if missing_tolerance and missing_quantile are both None
        ValueError: if columns _one or _pct are in df

    """

    if '_one' in df.columns or '_pct' in df.columns:
        raise ValueError('will override column _one and _pct in df')

    df['_one'] = 1  # temporary variable for counting without missing
    by_firm_counts = df.groupby([id_var] + byvars)[['_one', count_with_missings_var]].count().reset_index()
    df.drop('_one', axis=1, inplace=True)

    if missing_quantile is not None:
        by_firm_counts['_pct'] = by_firm_counts[count_with_missings_var] / by_firm_counts['_one']  # percentage coverage
        minimum_allowed_coverage = (1 - missing_quantile)
        missing_df = by_firm_counts[by_firm_counts['_pct'] < minimum_allowed_coverage]
        full_df = by_firm_counts[by_firm_counts['_pct'] >= minimum_allowed_coverage]
    elif missing_tolerance is not None:
        missing_df = by_firm_counts[by_firm_counts[count_with_missings_var] + missing_tolerance < by_firm_counts['_one']]
        full_df = by_firm_counts[by_firm_counts[count_with_missings_var] + missing_tolerance >= by_firm_counts['_one']]
    else:
        raise ValueError('pass one of missing_tolerance and missing_quantile. got both as None')

    name_args = (missing_tolerance, missing_quantile, missing_display_str, id_var)

    missing_counts = missing_df.groupby(byvars)[id_var].count()
    missing_counts.name = missing_more_than_str(*name_args)

    full_counts = full_df.groupby(byvars)[id_var].count()
    full_counts.name = missing_tolerance_count_str(*name_args)

    by_id_var = pd.concat([missing_counts, full_counts], axis=1).fillna(0)
    return by_id_var.reset_index()