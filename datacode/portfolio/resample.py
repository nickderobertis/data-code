from typing import Optional
import pandas as pd
import numpy as np


def collect_portfolios_through_time(df: pd.DataFrame, portvar: str, id_var: str, needed_calendar_days: int,
                                    needed_prior_calendar_days: Optional[int] = None,
                                    datevar: str = 'Date', portfolio_datevar: str = 'Portfolio Date'
                                    ) -> pd.DataFrame:
    output_df = pd.DataFrame()
    # TODO [#4]: collect portfolios through time make more efficient
    for port_date in df[portfolio_datevar].unique():
        port_assignments = df.loc[
            df[portfolio_datevar] == port_date,
            [id_var, portvar]
        ].drop_duplicates()
        extended_date = port_date + np.timedelta64(needed_calendar_days, 'D')
        begin_date = _portfolio_begin_date(port_date, needed_prior_calendar_days=needed_prior_calendar_days)
        port_df = df.loc[
            (df[datevar] >= begin_date) &
            (df[datevar] <= extended_date) &
            (df[id_var].isin(port_assignments[id_var]))
            ]

        # Override with beginning port assignments
        port_df.drop(portvar, axis=1, inplace=True)
        port_df = port_df.merge(port_assignments, how='left', on=id_var)
        # Override with portfolio formation date
        port_df[portfolio_datevar] = port_date

        output_df = output_df.append(port_df)

    return output_df

def _portfolio_begin_date(port_date: pd.Timestamp, needed_prior_calendar_days: Optional[int] = None) -> pd.Timestamp:
    if needed_prior_calendar_days is None:
        return port_date

    return port_date - np.timedelta64(needed_prior_calendar_days, 'D')