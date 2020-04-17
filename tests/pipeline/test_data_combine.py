from pandas.testing import assert_frame_equal

from datacode import DataSource
from datacode.models.pipeline.operations.combine import CombineOptions
from datacode.models.pipeline.operations.merge import MergeOptions
from tests.pipeline.base import PipelineTest


class TestDataCombinationPipeline(PipelineTest):

    def test_create_and_run_combine_rows_pipeline_from_sources(self):
        dp = self.create_combine_pipeline()
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2)

    def test_create_and_run_combine_rows_drop_rows_pipeline_from_sources(self):
        a, b, c = self.create_variables()
        co = CombineOptions(row_duplicate_vars=[c])
        dp = self.create_combine_pipeline(combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_row_drop_c)

    def test_create_and_run_combine_rows_drop_entities_pipeline_from_sources(self):
        a, b, c = self.create_variables()
        co = CombineOptions(entity_duplicate_vars=[c])
        dp = self.create_combine_pipeline(combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_entity_drop_c)

    def test_create_and_run_combine_cols_pipeline_from_sources(self):
        co = CombineOptions(rows=False)
        dp = self.create_combine_pipeline(combine_options_list=[co])

        with self.assertRaises(ValueError) as cm:
            dp.execute()
            exc = cm.exception
            assert 'exists in multiple data sources' in str(exc)

    def test_create_and_run_combine_rows_pipeline_with_indices(self):
        dp = self.create_combine_pipeline(indexed=True)
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_c_index)

    def test_create_and_run_combine_cols_pipeline_with_indices(self):
        co = CombineOptions(rows=False)
        dp = self.create_combine_pipeline(indexed=True, combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged_1_2_c_index)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dp = self.create_combine_pipeline()

        ds = DataSource(pipeline=dp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_combined_rows_1_2)

    def test_create_and_run_combine_pipeline_three_sources(self):
        dp = self.create_combine_pipeline(include_indices=(0, 1, 2))
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_3)

    def test_raises_error_for_mismatching_data_sources_merge_options(self):
        co = CombineOptions()
        dp = self.create_combine_pipeline(include_indices=(0, 1, 2), combine_options_list=[co])

        with self.assertRaises(ValueError) as cm:
            dp.execute()
            exc = cm.exception
            assert 'must have one fewer combine options than data sources' in str(exc)

    def test_create_nested_pipeline(self):
        dp1 = self.create_combine_pipeline(include_indices=(0, 1))

        self.create_csv_for_3()
        ds3_cols = self.create_columns_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')

        dp2 = self.create_combine_pipeline(data_sources=[dp1, ds3])
        dp2.execute()

        assert_frame_equal(dp2.df, self.expect_combined_rows_1_2_3)

