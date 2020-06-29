from typing import Tuple

from pandas.testing import assert_frame_equal
import pandas as pd
import numpy as np

from datacode import VariableCollection, Variable, Column, Index, StringType, ColumnIndex, SourceTransform
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
    c_df_index = pd.Index(['d', 'd', 'e'], name='C')
    expect_loaded_df_with_lags = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (1, 2, 'd'),
            (3, 4, 'e')
        ],
        columns=['A$_{t - 1}$', 'B$_{t - 1}$', 'C'],
    ).convert_dtypes()
    expect_loaded_df_with_two_lags = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (np.nan, np.nan, 'd'),
            (1, 2, 'e')
        ],
        columns=['A$_{t - 2}$', 'B$_{t - 2}$', 'C'],
    ).convert_dtypes()
    expect_loaded_df_with_lags_and_by_var = pd.DataFrame(
        [
            (np.nan, np.nan),
            (1, 2),
            (np.nan, np.nan)
        ],
        columns=['A$_{t - 1}$', 'B$_{t - 1}$'],
        index=c_df_index,
    ).convert_dtypes()
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
    ).convert_dtypes()
    expect_lag_df_with_ids_and_dates['Date'] = full_date_index
    expect_lag_df_with_ids_and_dates.set_index(['C', 'Date'], inplace=True)
    expect_loaded_df_with_change = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (2, 2, 'd'),
            (2, 2, 'e')
        ],
        columns=['A Change', 'B Change', 'C'],
    ).convert_dtypes()
    expect_loaded_df_with_dual_change = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (np.nan, np.nan, 'd'),
            (0, 0, 'e')
        ],
        columns=['A Change Change', 'B Change Change', 'C'],
    ).convert_dtypes()
    expect_change_df_with_ids_and_dates = pd.DataFrame(
        [
            (np.nan, np.nan, 'd'),
            (1, 2, 'd'),
            (1, 2, 'd'),
            (np.nan, np.nan, 'e'),
            (np.nan, np.nan, 'e'),
            (1, 2, 'e'),
        ],
        columns=['A Change', 'B Change', 'C'],
    ).convert_dtypes()
    expect_change_df_with_ids_and_dates['Date'] = full_date_index
    expect_change_df_with_ids_and_dates.set_index(['C', 'Date'], inplace=True)


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
        assert str(vc.a.lag().symbol) == r'\text{A}_{t - 1}'
        assert str(vc.b.lag().symbol) == r'\text{B}_{t - 1}'
        assert str(vc.c.symbol) == r'\text{C}'

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
        assert str(vc.a.lag(2).symbol) == r'\text{A}_{t - 2}'
        assert str(vc.b.lag(2).symbol) == r'\text{B}_{t - 2}'
        assert str(vc.c.symbol) == r'\text{C}'

    def test_two_separate_lags_no_indices(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a.lag().lag(),
            vc.b.lag().lag(),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_two_lags)
        assert str(vc.a.lag().lag().symbol) == r'\text{A}_{t - 2}'
        assert str(vc.b.lag().lag().symbol) == r'\text{B}_{t - 2}'
        assert str(vc.c.symbol) == r'\text{C}'

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

    def test_lags_as_source_transform_with_subset_and_no_preserve(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a,
            vc.b,
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        lag_transform = [transform for transform in vc.transforms if transform.key == 'lag'][0]
        source_transform = SourceTransform.from_transform(lag_transform, subset=[a, b])
        source_transform.apply(ds, preserve_original=False)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_lags)
        assert str(vc.a.lag().symbol) == r'\text{A}_{t - 1}'
        assert str(vc.b.lag().symbol) == r'\text{B}_{t - 1}'
        assert str(vc.c.symbol) == r'\text{C}'


class TestChange(SpecificTransformsTest):

    def test_change_with_defaults_no_indices(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a.change(),
            vc.b.change(),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_change)
        assert str(vc.a.change().symbol) == r'\delta \text{A}'
        assert str(vc.b.change().symbol) == r'\delta \text{B}'
        assert str(vc.c.symbol) == r'\text{C}'

    def test_two_separate_changes_no_indices(self):
        vc, a, b, c = self.create_variable_collection()
        self.create_csv()
        all_cols = self.create_columns()
        load_variables = [
            vc.a.change().change(),
            vc.b.change().change(),
            c
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_dual_change)
        assert str(vc.a.change().change().symbol) == r'\delta \delta \text{A}'
        assert str(vc.b.change().change().symbol) == r'\delta \delta \text{B}'
        assert str(vc.c.symbol) == r'\text{C}'

    def test_change_with_by_index_and_time_index_with_gaps(self):
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
            vc.a.change(fill_method=None),
            vc.b.change(fill_method=None),
            c,
            d
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        assert_frame_equal(ds.df, self.expect_change_df_with_ids_and_dates)