import pandas as pd
import pd_utils

from pd_utils.utils import _to_list_if_str
from datacode.ff.fftypes import ListOrStr


def portfolio_returns(df: pd.DataFrame, retvar: str='RET', datevar: str='Date', wtvar: str='Market Equity',
                       byvars: ListOrStr=None,
                       portvar: str='Market Equity Portfolio/B/M Portfolio') -> pd.DataFrame:

    byvars = _to_list_if_str(byvars)

    if byvars is not None:
        all_byvars = byvars + [datevar, portvar]
    else:
        all_byvars = [datevar, portvar]

    avgs = pd_utils.averages(
        df,
        retvar,
        byvars=all_byvars,
        wtvar=wtvar,
        count=False
    )

    _get_weighted_averages_from_averages(avgs)

    return avgs

def market_returns(df: pd.DataFrame, retvar: str='RET', datevar: str='Date', wtvar: str='Market Equity',
                   byvars: ListOrStr=None) -> pd.DataFrame:

    byvars = _to_list_if_str(byvars)

    if byvars is not None:
        all_byvars = byvars + [datevar]
    else:
        all_byvars = [datevar]

    avgs = pd_utils.averages(
        df[all_byvars + [retvar, wtvar]],
        retvar,
        byvars=all_byvars,
        wtvar=wtvar,
        count=False
    )

    _get_weighted_averages_from_averages(avgs)
    avgs.rename(columns={retvar: 'MKT'}, inplace=True)

    return avgs


def _get_weighted_averages_from_averages(df: pd.DataFrame) -> None:
    """
    drops EW cols, replaces with VW cols

    Note: inplace
    """
    weighted_average_cols = [col for col in df.columns if col.endswith('_wavg')]
    rename_dict = {col: '_'.join(col.split('_')[:-1]) for col in weighted_average_cols}
    average_cols = [col for col in rename_dict.values()]

    df.drop(average_cols, axis=1, inplace=True)
    df.rename(columns=rename_dict, inplace=True)