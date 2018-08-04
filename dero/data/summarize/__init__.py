import pandas as pd
from typing import Tuple, Callable, Union, Optional
from IPython.display import display
from functools import partial

DfOrSeries = Union[pd.DataFrame, pd.Series]
DfTuple = Tuple[DfOrSeries, DfOrSeries]
DfTupleOrNone = Optional[DfTuple]

def format_numbers_to_decimal_places(item, decimals=2):
    if isinstance(item, (float, int)):
        if item > 999999.99:
            item = item / 1000000
            return f'{item:,.{decimals}f}M'
        return f'{item:,.{decimals}f}'
    else:
        return item

def describe_df(df: pd.DataFrame, disp: bool=True, format_func: Callable=format_numbers_to_decimal_places,
                format_kwargs: dict=None) -> DfTupleOrNone:
    """

    Args:
        df:
        disp: True to display summaries, False to only return tuple of summaries

    Returns:

    """
    if format_kwargs is None:
        format_kwargs = {}

    summaries_list = []
    for dtype, count in df.get_dtype_counts().iteritems():
        summary: pd.DataFrame = df.describe(include=dtype, percentiles=[0.05, 0.1, 0.25, .75, .9, .95]).T
        summaries_list.append(summary)

    full_format_func = partial(format_func, **format_kwargs)

    summaries_tuple = tuple(summary.applymap(full_format_func) for summary in summaries_list)

    if disp:
        display(*summaries_tuple)
    else:
        return summaries_tuple



