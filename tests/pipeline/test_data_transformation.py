import os

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode import DataSource, DataGeneratorPipeline, DataTransformationPipeline, Variable, Column
from tests.test_source import SourceTest
from tests.utils import GENERATED_PATH


def transform_func(col: Column, variable: Variable, ds: DataSource) -> DataSource:
    if variable.dtype.is_numeric:
        ds.df[variable.name] += 1
    return ds


class DataTransformationPipelineTest(SourceTest):
    expect_func_df = df = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['A', 'B', 'C']
    )
    expect_loaded_df_with_transform = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['A_1', 'B_1', 'C_1']
    )
    csv_path_output = os.path.join(GENERATED_PATH, 'output.csv')

    def create_pipeline(self, **pipeline_kwargs) -> DataTransformationPipeline:
        config_dict = dict(
            func=transform_func,
            outpath=self.csv_path_output
        )
        config_dict.update(pipeline_kwargs)
        self.create_csv()
        all_cols = self.create_columns()
        ds = self.create_source(df=None, columns=all_cols)
        dtp = DataTransformationPipeline(ds, **config_dict)
        return dtp


class TestDataTransformationPipeline(DataTransformationPipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dgp = self.create_pipeline()
        dgp.execute()

        assert_frame_equal(dgp.df, self.expect_func_df)

    def test_create_and_run_generator_pipeline_from_transform(self):
        dgp = self.create_pipeline(func=self.transform_source)
        dgp.execute()

        assert_frame_equal(dgp.df, self.expect_loaded_df_with_transform)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dtp = self.create_pipeline()

        ds = DataSource(pipeline=dtp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_func_df)
