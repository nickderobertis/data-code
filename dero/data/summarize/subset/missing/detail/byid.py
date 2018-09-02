import pandas as pd
from dero.data.typing import StrList


def by_id_pct_long_df(df: pd.DataFrame, byvars: StrList, id_var: str,
                      count_with_missings_var: str, missing_tolerance: int =0) -> pd.DataFrame:

    by_id_var = _by_id_count_long_df(
        df,
        byvars,
        id_var,
        count_with_missings_var,
        missing_tolerance=missing_tolerance
    )

    by_id_var[f'{id_var} Count'] = by_id_var[f'More than {missing_tolerance} Missing {id_var}'] + \
                                   by_id_var[f'{missing_tolerance} or less Missing {id_var} Count']

    by_id_var[f'More than {missing_tolerance} Missing {id_var} Percentage'] = \
        (by_id_var[f'More than {missing_tolerance} Missing {id_var}'] / \
        by_id_var[f'{id_var} Count']) * 100

    by_id_var.drop([
        f'More than {missing_tolerance} Missing {id_var}',
        f'{missing_tolerance} or less Missing {id_var} Count'
    ], axis=1, inplace=True)

    return by_id_var


def _by_id_count_long_df(df: pd.DataFrame, byvars: StrList, id_var: str,
                         count_with_missings_var: str, missing_tolerance: int =0) -> pd.DataFrame:

    df['_one'] = 1  # temporary variable for counting without missing
    by_firm_counts = df.groupby([id_var] + byvars)[['_one', count_with_missings_var]].count().reset_index()
    df.drop('_one', axis=1, inplace=True)

    missing_df = by_firm_counts[by_firm_counts[count_with_missings_var] + missing_tolerance < by_firm_counts['_one']]
    full_df = by_firm_counts[by_firm_counts[count_with_missings_var] + missing_tolerance >= by_firm_counts['_one']]

    missing_counts = missing_df.groupby(byvars)[id_var].count()
    missing_counts.name = f'More than {missing_tolerance} Missing {id_var}'

    full_counts = full_df.groupby(byvars)[id_var].count()
    full_counts.name = f'{missing_tolerance} or less Missing {id_var} Count'

    by_id_var = pd.concat([missing_counts, full_counts], axis=1).fillna(0)
    return by_id_var.reset_index()