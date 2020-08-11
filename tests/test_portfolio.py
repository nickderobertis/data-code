from typing import Iterable

import pandas as pd
from numpy import nan
from pandas._libs.tslibs.timestamps import Timestamp
from pandas._testing import assert_frame_equal

from datacode.portfolio.cumret import cumulate_buy_and_hold_portfolios
from tests.test_data import DataFrameTest


class PortfolioTest(DataFrameTest):
    id_var: str = "PERMNO"
    date_var: str = "Date"
    ret_var: str = "RET"
    port_var: str = "Portfolio"
    port_date_var: str = "Portfolio Date"
    weight_var: str = "Weight"
    cum_days: Iterable[int] = (0, 2, 5)

    daily_port_df = pd.DataFrame(
        [
            (10516, "1/1/2000", 0.01, 1, "1/1/2000", 2),
            (10516, "1/2/2000", 0.02, 1, "1/1/2000", 2),
            (10516, "1/3/2000", 0.03, 1, "1/1/2000", 2),
            (10516, "1/4/2000", 0.04, 1, "1/1/2000", 2),
            (10516, "1/5/2000", 0.01, 2, "1/5/2000", 2),
            (10516, "1/6/2000", 0.02, 2, "1/5/2000", 2),
            (10516, "1/7/2000", 0.03, 2, "1/5/2000", 2),
            (10516, "1/8/2000", 0.04, 2, "1/5/2000", 2),
            (10517, "1/1/2000", 0.05, 2, "1/1/2000", 2),
            (10517, "1/2/2000", 0.06, 2, "1/1/2000", 2),
            (10517, "1/3/2000", 0.07, 2, "1/1/2000", 2),
            (10517, "1/4/2000", 0.08, 2, "1/1/2000", 2),
            (10517, "1/5/2000", 0.05, 1, "1/5/2000", 2),
            (10517, "1/6/2000", 0.06, 1, "1/5/2000", 2),
            (10517, "1/7/2000", 0.07, 1, "1/5/2000", 2),
            (10517, "1/8/2000", 0.08, 1, "1/5/2000", 2),
            (10518, "1/1/2000", 0.11, 1, "1/1/2000", 1),
            (10518, "1/2/2000", 0.12, 1, "1/1/2000", 1),
            (10518, "1/3/2000", 0.13, 1, "1/1/2000", 1),
            (10518, "1/4/2000", 0.14, 1, "1/1/2000", 1),
            (10518, "1/5/2000", 0.11, 2, "1/5/2000", 1),
            (10518, "1/6/2000", 0.12, 2, "1/5/2000", 1),
            (10518, "1/7/2000", 0.13, 2, "1/5/2000", 1),
            (10518, "1/8/2000", 0.14, 2, "1/5/2000", 1),
            (10519, "1/1/2000", 0.15, 2, "1/1/2000", 1),
            (10519, "1/2/2000", 0.16, 2, "1/1/2000", 1),
            (10519, "1/3/2000", 0.17, 2, "1/1/2000", 1),
            (10519, "1/4/2000", 0.18, 2, "1/1/2000", 1),
            (10519, "1/5/2000", 0.15, 1, "1/5/2000", 1),
            (10519, "1/6/2000", 0.16, 1, "1/5/2000", 1),
            (10519, "1/7/2000", 0.17, 1, "1/5/2000", 1),
            (10519, "1/8/2000", 0.18, 1, "1/5/2000", 1),
        ],
        columns=[id_var, date_var, ret_var, port_var, port_date_var, weight_var],
    )
    daily_port_df[date_var] = pd.to_datetime(daily_port_df[date_var])
    daily_port_df[port_date_var] = pd.to_datetime(daily_port_df[port_date_var])

    def ew_ret_name(self, cum_period: int) -> str:
        return f"EW {self.ret_var} {cum_period}"

    def vw_ret_name(self, cum_period: int) -> str:
        return f"VW {self.ret_var} {cum_period}"

    @property
    def expect_daily_cum_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            data=[
                (
                    1,
                    Timestamp("2000-01-01 00:00:00"),
                    0.06000000000000005,
                    0.04333333333333337,
                    0.07,
                    0.0533333333333333,
                    0.35252024000000026,
                    0.2695302400000002,
                ),
                (
                    1,
                    Timestamp("2000-01-05 00:00:00"),
                    0.09999999999999998,
                    0.08333333333333333,
                    0.11,
                    0.09333333333333334,
                    nan,
                    nan,
                ),
                (
                    2,
                    Timestamp("2000-01-01 00:00:00"),
                    0.09999999999999998,
                    0.08333333333333333,
                    0.11,
                    0.09333333333333334,
                    0.5639515999999999,
                    0.47136200000000006,
                ),
                (
                    2,
                    Timestamp("2000-01-05 00:00:00"),
                    0.06000000000000005,
                    0.04333333333333337,
                    0.07,
                    0.0533333333333333,
                    nan,
                    nan,
                ),
            ],
            columns=[
                self.port_var,
                self.port_date_var,
                self.ew_ret_name(0),
                self.vw_ret_name(0),
                self.ew_ret_name(1),
                self.vw_ret_name(1),
                self.ew_ret_name(5),
                self.vw_ret_name(5),
            ],
        )


class TestCumulativePortfolios(PortfolioTest):
    def test_daily_cumulate_bhr(self):
        cum_df = cumulate_buy_and_hold_portfolios(
            self.daily_port_df,
            self.port_var,
            self.id_var,
            self.date_var,
            self.port_date_var,
            self.ret_var,
            weight_var=self.weight_var,
            freq="d",
        )
        assert_frame_equal(cum_df, self.expect_daily_cum_df)
