from pandas.testing import assert_frame_equal

from datacode import DataSource
from tests.pipeline.base import PipelineTest, EXPECT_GENERATED_DF


class TestDataGeneratorPipeline(PipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dgp = self.create_generator_pipeline()
        dgp.execute()

        assert_frame_equal(dgp.df, EXPECT_GENERATED_DF)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dgp = self.create_generator_pipeline()

        ds = DataSource(pipeline=dgp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, EXPECT_GENERATED_DF)
