from typing import Tuple

from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np

from datacode import VariableCollection, Variable, Column, Index, StringType, ColumnIndex
from tests.test_source import SourceTest
from tests.test_variables import VC_NAME


class SpecificTransformsTest(SourceTest):
    time_index = Index('time', dtype='datetime')
    by_index = Index('id', dtype=StringType(categorical=True))
    test_df_with_ids_and_dates = pd.DataFrame(
        [
            (1, 2, 'd'),
            (2, 4, 'd'),
            (3, 6, 'd'),
            (4, 8, 'e'),
            (5, 10, 'e'),
            (6, 12, 'e'),
        ],
        columns=['a', 'b', 'c']
    )
    orig_date_index = pd.date_range(start='1/1/2000', periods=3, freq='d')
    date_index_with_gaps = pd.Index.union(pd.DatetimeIndex([pd.to_datetime('1/1/2000')]),
                                     pd.date_range(start='1/3/2000', periods=2, freq='d'))
    full_date_index = pd.Index.append(orig_date_index, date_index_with_gaps)
    test_df_with_ids_and_dates['date'] = full_date_index
    expect_loaded_df_with_lags = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (1, 2, 'd'),
            (3, 4, 'e')
        ],
        columns=['A$_{t - 1}$', 'B$_{t - 1}$', 'C'],
    )
    expect_loaded_df_with_two_lags = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (np.nan, np.nan, 'd'),
            (1, 2, 'e')
        ],
        columns=['A$_{t - 2}$', 'B$_{t - 2}$', 'C'],
    )
    expect_loaded_df_with_lags_and_by_var = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (1, 2, 'd'),
            (np.nan, np.nan, 'e')
        ],
        columns=['A$_{t - 1}$', 'B$_{t - 1}$', 'C'],
    )
    expect_lag_df_with_ids_and_dates = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (1, 2, 'd'),
            (2, 4, 'd'),
            (np.nan, np.nan, 'e'),
            (np.nan, np.nan, 'e'),
            (5, 10, 'e'),
        ],
        columns=['A$_{t - 1}$', 'B$_{t - 1}$', 'C'],
    )
    expect_lag_df_with_ids_and_dates['Date'] = full_date_index

    def create_variable_collection(self, **kwargs) -> Tuple[VariableCollection, Variable, Variable, Variable]:
        config_dict = dict(
            name=VC_NAME
        )
        config_dict.update(**kwargs)
        a, b, c = self.create_variables()
        vc = VariableCollection(a, b, c, **config_dict)
        return vc, a, b, c


class TestLag(SpecificTransformsTest):

    def test_lag_with_defaults_no_indices(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a.lag(),
            vc.b.lag(),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_lags)

    def test_two_lags_no_indices(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a.lag(2),
            vc.b.lag(2),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_two_lags)

    def test_lags_with_by_index(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        by_colindex = [ColumnIndex(self.by_index, [c])]
        ac = Column(a, 'a', by_colindex)
        bc = Column(b, 'b', by_colindex)
        cc = Column(c, 'c')
        all_cols = [
            ac, bc, cc
        ]
        load_variables = [
            vc.a.lag(),
            vc.b.lag(),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_lags_and_by_var)

    def test_lags_with_by_index_and_time_index_with_gaps(self):
        vc, a, b, c = self.create_variable_collection()
        d = Variable('d', 'Date', dtype='datetime')
        self.create_csv(df=self.test_df_with_ids_and_dates)
        by_colindex = ColumnIndex(self.by_index, [c])
        time_colindex = ColumnIndex(self.time_index, [d])
        by_time_colindex = [by_colindex, time_colindex]
        ac = Column(a, 'a', by_time_colindex)
        bc = Column(b, 'b', by_time_colindex)
        cc = Column(c, 'c')
        dd = Column(d, 'date')
        all_cols = [
            ac, bc, cc, dd
        ]
        load_variables = [
            vc.a.lag(fill_method=None),
            vc.b.lag(fill_method=None),
            c,
            d
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_lag_df_with_ids_and_dates)


