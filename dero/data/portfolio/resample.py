import pandas as pd
import numpy as np


def collect_portfolios_through_time(df: pd.DataFrame, portvar: str, id_var: str, needed_calendar_days: int,
                                    datevar: str = 'Date', portfolio_datevar: str = 'Portfolio Date'
                                    ) -> pd.DataFrame:
    output_df = pd.DataFrame()
    # TODO: make more efficient
    for port_date in df[portfolio_datevar].unique():
        port_assignments = df.loc[
            df[portfolio_datevar] == port_date,
            [id_var, portvar]
        ].drop_duplicates()
        extended_date = port_date + np.timedelta64(needed_calendar_days, 'D')
        port_df = df.loc[
            (df[datevar] >= port_date) &
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
