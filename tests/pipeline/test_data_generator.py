import os

import pandas as pd
from pandas.testing import assert_frame_equal

from datacode import DataSource, DataGeneratorPipeline
from tests.test_source import SourceTest
from tests.utils import GENERATED_PATH

EXPECT_GENERATED_DF = df = pd.DataFrame(
    [
        (1, 2),
        (3, 4)
    ],
    columns=['a', 'b']
)

def ds_generator_func() -> DataSource:
    ds = DataSource(df=EXPECT_GENERATED_DF)
    return ds


class DataGeneratorPipelineTest(SourceTest):
    csv_path_output = os.path.join(GENERATED_PATH, 'output.csv')

    def create_pipeline(self) -> DataGeneratorPipeline:
        dgp = DataGeneratorPipeline(ds_generator_func, outpath=self.csv_path_output)
        return dgp


class TestDataGeneratorPipeline(DataGeneratorPipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dgp = self.create_pipeline()
        dgp.execute()

        assert_frame_equal(dgp.df, EXPECT_GENERATED_DF)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dgp = self.create_pipeline()

        ds = DataSource(pipeline=dgp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, EXPECT_GENERATED_DF)
