from pandas.testing import assert_frame_equal

from datacode import DataSource
from datacode.models.pipeline.operations.merge import MergeOptions
from tests.pipeline.base import PipelineTest


class TestDataMergePipeline(PipelineTest):

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

