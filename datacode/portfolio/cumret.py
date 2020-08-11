import datetime
import math
from typing import Iterable, Optional, List

import pandas as pd
import pd_utils

from datacode.portfolio.resample import collect_portfolios_through_time


def cumulate_buy_and_hold_portfolios(
    df: pd.DataFrame,
    port_var: str,
    id_var: str,
    date_var: str,
    port_date_var: str,
    ret_var: str,
    cum_days: Iterable[float] = (0, 1, 5),
    freq: str = "d",
    grossify: bool = True,
    weight_var: Optional[str] = None,
):
    """
    Creates buy-and-hold portfolios from normal portfolios and
    cumulates a variable within them.

    For each portfolio in each portfolio date, finds the ids which
    are present in that portfolio. Extends this portfolio for
    however many days are needed to cumulate.

    Then within these extended buy-and-hold portfolios, cumulates then calculates
    the average (and optionally, weighted-average) of the cumulated variable.

    :param df: DataFrame containing portfolios, a date variable, a portfolio
        formation date variable, and id variable, and a variable to be cumulated
    :param port_var: Name of variable containing portfolios
    :param id_var: Name of variable containing entity ids
    :param date_var: Name pf variable containing entity dates
    :param port_date_var: Name of variable containing portfolio formation dates
    :param ret_var: Name of variable to be cumulated
    :param cum_days: Cumulate to between this many days, e.g. (0, 1, 5) means
        give return for initial period (0), return for first period (0 to 1),
        and return for periods 1 to 5 cumulated
    :param freq: 'd' for daily, 'h' for hourly, 'w' for weekly, 'm' for monthly,
        'y' for annual
    :param grossify: Set to True to add one to all variables then subtract one at the end
    :param weight_var: Variable to use for calculating weights in weighted average, None
        to disable weighted averages
    :return: Wide-format DataFrame which has portfolio variable, portfolio formation date
        variable, and cumulative return variables
    """
    daily_multiplier = _daily_multiplier(freq)
    cum_time: List[int] = [int(round(t * daily_multiplier, 0)) for t in cum_days]
    needed_days = math.ceil(max(cum_days))

    # Get buy and hold portfolios
    persist_port_df = collect_portfolios_through_time(
        df,
        port_var,
        id_var,
        needed_days,
        datevar=date_var,
        portfolio_datevar=port_date_var,
    )

    cum_df = pd_utils.cumulate(
        persist_port_df,
        ret_var,
        "between",
        date_var,
        byvars=[port_var, port_date_var, id_var],
        time=cum_time,
        grossify=grossify,
    )

    port_periods = (
        cum_df[[port_var, port_date_var]]
        .drop_duplicates()
        .sort_values([port_var, port_date_var])
    )

    out_df = port_periods
    for cum_period in cum_time:
        period_df = _average_for_cum_time(
            cum_df,
            cum_period,
            port_var,
            date_var,
            port_date_var,
            ret_var,
            freq=freq,
            weight_var=weight_var,
        )
        out_df = out_df.merge(period_df, how="left", on=[port_var, port_date_var])

    return out_df


def _average_for_cum_time(
    cum_df: pd.DataFrame,
    cum_period: int,
    port_var: str,
    date_var: str,
    port_date_var: str,
    ret_var: str,
    freq: str = "d",
    weight_var: Optional[str] = None,
) -> pd.DataFrame:
    td = _offset(cum_period, freq)
    this_cum_df = cum_df[cum_df[date_var] == (cum_df[port_date_var] + td)]

    avgs = pd_utils.averages(
        this_cum_df, f"cum_{ret_var}", [port_var, port_date_var], wtvar=weight_var
    )
    avgs.rename(
        columns={
            f"cum_{ret_var}": f"EW {ret_var} {cum_period}",
            f"cum_{ret_var}_wavg": f"VW {ret_var} {cum_period}",
        },
        inplace=True,
    )
    return avgs


def _offset(nper: int, freq: str) -> datetime.timedelta:
    freq = freq.casefold()
    if freq == "d":
        return datetime.timedelta(days=nper)
    elif freq == "h":
        return datetime.timedelta(hours=nper)
    elif freq == "w":
        return datetime.timedelta(days=nper * 7)
    elif freq == "m":
        return datetime.timedelta(days=nper * 30)
    elif freq == "y":
        return datetime.timedelta(days=nper * 365)
    else:
        raise ValueError(f"unsupported freq {freq}")


def _daily_multiplier(freq: str, trading_calendar: bool = False) -> int:
    # TODO: support trading calendar in cumulative portfolio
    #
    # Some initial work is done in _daily_multipler, but need to add
    # for other functions, and be more flexible for custom calendars
    normal_multipliers = dict(d=1, h=24, w=1 / 7, m=1 / 30, y=1 / 365,)
    trading_multiplers = dict(d=1, h=6.5, w=1 / 5, m=1 / 21, y=1 / 252,)

    try:
        if trading_calendar:
            return trading_multiplers[freq.lower()]
        else:
            return normal_multipliers[freq.lower()]
    except KeyError:
        raise ValueError(f"unsupported freq {freq}")
