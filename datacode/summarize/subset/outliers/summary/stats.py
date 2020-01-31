import pandas as pd
from datacode.summarize.subset.outliers.typing import StrList, FloatSequence
from datacode.summarize import format_numbers_to_decimal_places


def bad_df_summary_stats_df(bad_df: pd.DataFrame, outlier_cols: StrList,
                            bad_column_name: str = '_bad_column',
                            quants: FloatSequence=(0.01, 0.1, 0.5, 0.9, 0.99)) -> pd.DataFrame:

    compare_col_df_list = []

    # First add columns from direct functions such as grouped.max()
    for stat in ('min', 'max', 'mean', 'std'):
        this_stat_list = []
        for col in outlier_cols:
            compare_series = bad_df[bad_df[bad_column_name] == col][col]
            compare_value = getattr(compare_series, stat)()
            this_stat_list.append(compare_value)

        compare_col_df_list.append(
            pd.Series(this_stat_list, name=stat, index=outlier_cols).apply(format_numbers_to_decimal_places)
        )

    # Now add columns from grouped.quantile()
    quant_names = []  # track quant names for reordering
    for quant in quants:
        this_stat_list = []
        quant_name = _quant_name(quant)
        quant_names.append(quant_name)
        for col in outlier_cols:
            compare_series = bad_df[bad_df[bad_column_name] == col][col]
            compare_value = compare_series.quantile(quant)
            this_stat_list.append(compare_value)

        compare_col_df_list.append(
            pd.Series(this_stat_list, name=quant_name, index=outlier_cols).apply(format_numbers_to_decimal_places)
        )

    summ_df = pd.concat(compare_col_df_list, axis=1)

    # Column order and final formatting
    summ_df.index.name = 'Variable'
    column_order = ['mean', 'std', 'min'] + quant_names + ['max']
    summ_df = summ_df[column_order]

    return summ_df

def full_df_summary_stats_df(df: pd.DataFrame, outlier_cols: StrList,
                             quants: FloatSequence = (0.01, 0.1, 0.5, 0.9, 0.99)) -> pd.DataFrame:
    summ_df = df[outlier_cols].describe(percentiles=quants).T

    quant_names = [_quant_name(quant) for quant in quants]

    out_columns = ['count', 'mean', 'std', 'min'] + quant_names + ['max']
    summ_df = summ_df[out_columns]
    summ_df['count'] = summ_df['count'].astype(int)

    summ_df = summ_df.applymap(format_numbers_to_decimal_places)

    return summ_df



def _quant_name(quant: float) -> str:
    return f'{quant:.0%}'