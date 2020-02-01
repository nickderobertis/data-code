import pandas as pd

from datacode.summarize import format_numbers_to_decimal_places
from datacode.summarize.subset.outliers.typing import MinMaxDict


def bad_df_summary_df(bad_df: pd.DataFrame, min_max_dict: MinMaxDict,
                      firm_id_col: str = 'TICKER',
                      bad_column_name: str = '_bad_column') -> pd.DataFrame:
    compare_col_df_list = []
    obs_count_name = 'Obs. Count'
    id_col_count_name = f'{firm_id_col} Count'

    # Count of observations
    obs_count_series = bad_df.groupby(bad_column_name)[firm_id_col].count()
    obs_count_series.name = obs_count_name
    compare_col_df_list.append(obs_count_series)

    # Count of unique ids in observations
    id_count_series = bad_df[[firm_id_col, bad_column_name]].drop_duplicates(
    ).groupby(bad_column_name)[firm_id_col].count()
    id_count_series.name = id_col_count_name
    compare_col_df_list.append(id_count_series)

    # Combine cutoffs with counts
    concat_items = compare_col_df_list + [_min_max_dict_to_df(min_max_dict)]
    summ_df = pd.concat(concat_items, axis=1)

    # Cutoffs df may contain rows which counts will not. This means there were no outliers
    # for that variable. Fill with zeroes
    summ_df.fillna(0, inplace=True)

    # Convert counts to int for correct formatting
    summ_df[obs_count_name] = summ_df[obs_count_name].astype(int)
    summ_df[id_col_count_name] = summ_df[id_col_count_name].astype(int)

    summ_df = summ_df.applymap(format_numbers_to_decimal_places)

    summ_df.index.name = 'Variable'

    return summ_df


def _min_max_dict_to_df(min_max_dict: MinMaxDict) -> pd.DataFrame:
    df = pd.DataFrame(min_max_dict).T
    df.columns = ['Low Cutoff', 'High Cutoff']

    return df