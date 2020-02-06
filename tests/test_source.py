import os
import shutil
from typing import Optional, Tuple

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode.models.source import DataSource
from datacode.models.variables import Variable
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

    def create_variables(self) -> Tuple[Variable, Variable, Variable]:
        a = Variable('a', 'A', dtype='int')
        b = Variable('b', 'B', dtype='int')
        c = Variable('c', 'C', dtype='categorical')
        return a, b, c


class TestCreateSource(SourceTest):

    def test_create_source_from_df(self):
        ds = self.create_source(location=None)
        assert_frame_equal(ds.df, self.test_df)

    def test_create_source_from_file_path(self):
        self.create_csv()
        ds = self.create_source(df=None)
        assert_frame_equal(ds.df, self.test_df)

    def test_create_source_with_variables(self):
        all_vars = self.create_variables()
        ds = self.create_source(location=None, variables=all_vars)
        assert ds.variables == all_vars
