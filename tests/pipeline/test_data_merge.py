import os
from typing import Tuple, Optional, Sequence

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode import Column, Variable, DataMergePipeline, DataSource
from datacode.models.pipeline.operations.merge import MergeOptions
from tests.pipeline.test_data_transformation import DataTransformationPipelineTest
from tests.utils import GENERATED_PATH


class DataMergePipelineTest(DataTransformationPipelineTest):
    merge_var = Variable('c', 'C', dtype='str')
    test_df2 = pd.DataFrame(
        [
            (10, 20, 'd'),
            (50, 60, 'e'),
        ],
        columns=['e', 'f', 'c']
    )
    test_df3 = pd.DataFrame(
        [
            (100, 200, 'd'),
            (500, 600, 'e'),
        ],
        columns=['g', 'h', 'c']
    )
    expect_merged_1_2 = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20),
            (3, 4, 'd', 10, 20),
            (5, 6, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    )
    expect_merged_1_transformed_2 = pd.DataFrame(
        [
            (2, 3, 'd', 10, 20),
            (4, 5, 'd', 10, 20),
            (6, 7, 'e', 50, 60),
        ],
        columns=['A', 'B', 'C', 'E', 'F']
    )
    expect_merged_1_2_3 = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20, 100, 200),
            (3, 4, 'd', 10, 20, 100, 200),
            (5, 6, 'e', 50, 60, 500, 600),
        ],
        columns=['A', 'B', 'C', 'E', 'F', 'G', 'H']
    )
    csv_path2 = os.path.join(GENERATED_PATH, 'data2.csv')
    csv_path3 = os.path.join(GENERATED_PATH, 'data3.csv')
    csv_path_output = os.path.join(GENERATED_PATH, 'output.csv')

    def create_csv_for_2(self, df: Optional[pd.DataFrame] = None, **to_csv_kwargs):
        if df is None:
            df = self.test_df2
        df.to_csv(self.csv_path2, index=False, **to_csv_kwargs)

    def create_csv_for_3(self, df: Optional[pd.DataFrame] = None, **to_csv_kwargs):
        if df is None:
            df = self.test_df3
        df.to_csv(self.csv_path3, index=False, **to_csv_kwargs)

    def create_variables_for_2(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        transform_dict = self.get_transform_dict(transform_data=transform_data, apply_transforms=apply_transforms)

        e = Variable('e', 'E', dtype='int', **transform_dict)
        f = Variable('f', 'F', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return e, f, c

    def create_variables_for_3(self, transform_data: str = '', apply_transforms: bool = True
                               ) -> Tuple[Variable, Variable, Variable]:
        transform_dict = self.get_transform_dict(transform_data=transform_data, apply_transforms=apply_transforms)

        g = Variable('g', 'G', dtype='int', **transform_dict)
        h = Variable('h', 'H', dtype='int', **transform_dict)
        c = Variable('c', 'C', dtype='str')
        return g, h, c

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

    def create_columns_for_3(self, transform_data: str = '', apply_transforms: bool = True):
        g, h, c = self.create_variables_for_3(transform_data=transform_data, apply_transforms=apply_transforms)
        gc = Column(g, 'g')
        hc = Column(h, 'h')
        cc = Column(c, 'c')
        return [
            gc,
            hc,
            cc
        ]

    def create_merge_pipeline(self, include_indices: Sequence[int] = (0, 1),
                              data_sources: Optional[Sequence[DataSource]] = None,
                              merge_options_list: Optional[Sequence[MergeOptions]] = None) -> DataMergePipeline:

        if data_sources is None:
            self.create_csv()
            ds1_cols = self.create_columns()
            ds1 = self.create_source(df=None, columns=ds1_cols, name='one')
            self.create_csv_for_2()
            ds2_cols = self.create_columns_for_2()
            ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
            self.create_csv_for_3()
            ds3_cols = self.create_columns_for_3()
            ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')


            data_sources = [ds1, ds2, ds3]
            selected_data_sources = []
            for i, ds in enumerate(data_sources):
                if i in include_indices:
                    selected_data_sources.append(ds)
        else:
            selected_data_sources = data_sources

        if merge_options_list is None:
            mo = MergeOptions([self.merge_var.name])
            merge_options_list = [mo for _ in range(len(selected_data_sources) - 1)]


        dp = DataMergePipeline(selected_data_sources, merge_options_list, outpath=self.csv_path_output)
        return dp


class TestDataMergePipeline(DataMergePipelineTest):

    def test_create_and_run_merge_pipeline_from_sources(self):
        dp = self.create_merge_pipeline()
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged_1_2)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dp = self.create_merge_pipeline()

        ds = DataSource(pipeline=dp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_merged_1_2)

    def test_create_and_run_merge_pipeline_three_sources(self):
        dp = self.create_merge_pipeline(include_indices=(0, 1, 2))
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged_1_2_3)

    def test_raises_error_for_mismatching_data_sources_merge_options(self):
        mo = MergeOptions([self.merge_var.name])

        with self.assertRaises(ValueError) as cm:
            dp = self.create_merge_pipeline(include_indices=(0, 1, 2), merge_options_list=[mo])
            exc = cm.exception
            assert 'must have one fewer merge options than data sources' in str(exc)

    def test_create_nested_pipeline(self):
        dp1 = self.create_merge_pipeline(include_indices=(0, 1))

        self.create_csv_for_3()
        ds3_cols = self.create_columns_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')

        dp2 = self.create_merge_pipeline(data_sources=[dp1, ds3])
        dp2.execute()

        assert_frame_equal(dp2.df, self.expect_merged_1_2_3)

    def test_create_nested_transformation_pipeline(self):
        dt = self.create_transformation_pipeline()

        self.create_csv_for_2()
        ds2_cols = self.create_columns_for_2()
        ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')

        dp = self.create_merge_pipeline(data_sources=[dt, ds2])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged_1_transformed_2)

