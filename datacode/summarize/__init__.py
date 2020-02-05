import pandas as pd
from typing import Callable
from functools import partial

from datacode.display import display_df_dict
from datacode.typing import DfDictOrNone, FloatList


def format_numbers_to_decimal_places(item, decimals=2, coerce_ints: bool = False):

    if isinstance(item, (float, int)):
        if abs(item) > 999999.99:
            # Millions formatter
            item = item / 1000000
            # For millions decimals, treat int as float (still have decimals, because 7.02M is better than 7M for int
            return f'{item:,.{decimals}f}M'
        if coerce_ints:
            decimals = 0 if int(item) == item else decimals  # checks if is int stored as type float
        else:
            decimals = decimals if isinstance(item, float) else 0  # uses dtype to handle int vs. float
        return f'{item:,.{decimals}f}'
    else:
        return item

def describe_df(df: pd.DataFrame, disp: bool=True, format_func: Callable=format_numbers_to_decimal_places,
                format_kwargs: dict=None, percentiles: FloatList =None) -> DfDictOrNone:
    """

    Args:
        df:
        disp: True to display summaries, False to only return dict of summaries

    Returns:

    """
    if format_kwargs is None:
        format_kwargs = {}

    if percentiles is None:
        percentiles = [0.01, 0.05, 0.1, 0.25, .75, .9, .95, 0.99]

    summaries_dict = {}
    for dtype, count in df.dtypes.value_counts().iteritems():
        dtype_str = str(dtype)
        summary: pd.DataFrame = df.describe(include=dtype_str, percentiles=percentiles).T
        if 'int' in dtype_str:
            summary = summary.astype(int)  # describe is always float for int dtype, cast output format back to int
        summary_name = f'{dtype_str} Variables'
        summaries_dict[summary_name] = summary

    full_format_func = partial(format_func, **format_kwargs)

    summaries_dict = {
        summary_name: summary.applymap(full_format_func) for summary_name, summary in summaries_dict.items()
    }

    if disp:
        display_df_dict(summaries_dict)
    else:
        return summaries_dict



