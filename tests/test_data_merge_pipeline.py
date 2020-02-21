import os
from typing import Tuple, Optional

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode import Column, Variable, DataMergePipeline, DataSource
from datacode.models.merge import MergeOptions
from tests.test_source import SourceTest
from tests.utils import GENERATED_PATH


class DataMergePipelineTest(SourceTest):
    merge_var = Variable('c', 'C', dtype='str')
    test_df2 = pd.DataFrame(
        [
            (10, 20, 'd'),
            (50, 60, 'e'),
        ],
        columns=['e', 'f', 'c']
    )
    expect_merged = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20),
            (3, 4, 'd', 10, 20),
            (5, 6, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    )
    csv_path2 = os.path.join(GENERATED_PATH, 'data2.csv')
    csv_path_output = os.path.join(GENERATED_PATH, 'output.csv')

    def create_csv_for_2(self, df: Optional[pd.DataFrame] = None, **to_csv_kwargs):
        if df is None:
            df = self.test_df2
        df.to_csv(self.csv_path2, index=False, **to_csv_kwargs)

    def create_variables_for_2(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        if transform_data:
            transform = self.get_transform(transform_data)
            transform_dict = dict(
                available_transforms=[transform],
            )
            if apply_transforms:
                transform_dict['applied_transforms'] = [transform]
        else:
            transform_dict = {}

        e = Variable('e', 'E', dtype='int', **transform_dict)
        f = Variable('f', 'F', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return e, f, c

    def create_columns_for_2(self, transform_data: str = '', apply_transforms: bool = True):
        e, f, c = self.create_variables_for_2(transform_data=transform_data, apply_transforms=apply_transforms)
        ec = Column(e, 'e')
        fc = Column(f, 'f')
        cc = Column(c, 'c')
        return [
            ec,
            fc,
            cc
        ]

    def create_merge_pipeline(self):
        self.create_csv()
        ds1_cols = self.create_columns()
        ds1 = self.create_source(df=None, columns=ds1_cols, name='one')
        self.create_csv_for_2()
        ds2_cols = self.create_columns_for_2()
        ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')

        mo = MergeOptions([self.merge_var.name])
        dp = DataMergePipeline([ds1, ds2], [mo], outpath=self.csv_path_output)
        return dp


class TestDataMergePipeline(DataMergePipelineTest):

    def test_create_and_run_merge_pipeline_from_sources(self):
        dp = self.create_merge_pipeline()
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dp = self.create_merge_pipeline()

        ds = DataSource(pipeline=dp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_merged)
