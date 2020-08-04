import datetime
from unittest.mock import patch

from pandas.testing import assert_frame_equal

from datacode import DataSource
from tests.pipeline.base import PipelineTest, EXPECT_GENERATED_DF


class TestDataGeneratorPipeline(PipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dgp = self.create_generator_pipeline()
        dgp.execute()

        assert_frame_equal(dgp.df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dgp = self.create_generator_pipeline()

        ds = DataSource(pipeline=dgp, location=self.csv_path_output)
        ds.touch()  # even with last_modified set, should still load from pipeline
        df = ds.df
        assert_frame_equal(df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)

    def test_auto_run_pipeline_by_load_source_with_newer_pipeline(self):
        now = datetime.datetime.now()
        later = now + datetime.timedelta(minutes=5)
        dgp = self.create_generator_pipeline(last_modified=later)

        self.create_csv()
        ds = self.create_source(pipeline=dgp, df=None)
        df = ds.df
        assert dgp._operation_index == 1
        assert_frame_equal(df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)
