import os
import shutil
from typing import Optional, Tuple

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode.models.column.column import Column
from datacode.models.source import DataSource
from datacode.models.variables import Variable
from datacode.models.variables.transform import Transform
from tests.utils import GENERATED_PATH


class SourceTest:
    test_df = pd.DataFrame(
        [
            (1, 2, 'd'),
            (3, 4, 'd'),
            (5, 6, 'e')
        ],
        columns=['a', 'b', 'c']
    )
    expect_loaded_df_rename_only = pd.DataFrame(
        [
            (1, 2, 'd'),
            (3, 4, 'd'),
            (5, 6, 'e')
        ],
        columns=['A', 'B', 'C']
    )
    expect_loaded_df_with_transform = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['A_1', 'B_1', 'C']
    )
    transform_name_func = lambda x: f'{x}_1'
    transform_data_func = lambda x: x + 1
    transform = Transform('add_one', transform_name_func, transform_data_func)
    csv_path = os.path.join(GENERATED_PATH, 'data.csv')

    def setup_method(self):
        os.makedirs(GENERATED_PATH)

    def teardown_method(self):
        shutil.rmtree(GENERATED_PATH)

    def create_source(self, **kwargs) -> DataSource:
        config_dict = dict(
            df=self.test_df,
            location=self.csv_path,
        )
        config_dict.update(kwargs)
        return DataSource(**config_dict)

    def create_csv(self, df: Optional[pd.DataFrame] = None):
        if df is None:
            df = self.test_df
        df.to_csv(self.csv_path, index=False)

    def create_variables(self, transform_data: bool = False) -> Tuple[Variable, Variable, Variable]:
        if transform_data:
            transform_dict = dict(
                available_transforms=[self.transform],
                applied_transforms=[self.transform],
            )
        else:
            transform_dict = {}

        a = Variable('a', 'A', dtype='int', **transform_dict)
        b = Variable('b', 'B', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='categorical')
        return a, b, c

    def create_columns(self, transform_data: bool = False) -> Tuple[Column, Column, Column]:
        a, b, c = self.create_variables(transform_data=transform_data)
        ac = Column(a)
        bc = Column(b)
        cc = Column(c)
        return ac, bc, cc


class TestCreateSource(SourceTest):

    def test_create_source_from_df(self):
        ds = self.create_source(location=None)
        assert_frame_equal(ds.df, self.test_df)

    def test_create_source_from_file_path(self):
        self.create_csv()
        ds = self.create_source(df=None)
        assert_frame_equal(ds.df, self.test_df)

    def test_create_source_with_columns(self):
        all_cols = self.create_columns()
        ds = self.create_source(location=None, columns=all_cols)
        assert ds.columns == all_cols


class TestLoadSource(SourceTest):

    def test_load_with_columns(self):
        self.create_csv()
        all_cols = self.create_columns()
        ds = self.create_source(df=None, columns=all_cols)
        assert_frame_equal(ds.df, self.expect_loaded_df_rename_only)

    def test_load_with_columns_and_transforms(self):
        self.create_csv()
        all_cols = self.create_columns(transform_data=True)
        ds = self.create_source(df=None, columns=all_cols)
        assert_frame_equal(ds.df, self.expect_loaded_df_with_transform)