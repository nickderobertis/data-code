import datetime
from copy import deepcopy
from typing import Sequence
from unittest.mock import patch

from pandas.testing import assert_frame_equal

from datacode import DataSource, Variable
from datacode.models.pipeline.operations.combine import CombineOptions
from datacode.models.pipeline.operations.merge import MergeOptions
from tests.pipeline.base import PipelineTest


def select_variables_not_in_index(source: DataSource) -> Sequence[Variable]:
    selected_vars = []
    for col in source.columns:
        if not col.indices:
            continue
        selected_vars.append(col.variable)
    return selected_vars


class TestDataCombinationPipeline(PipelineTest):

    def test_create_and_run_combine_rows_pipeline_from_sources(self):
        dp = self.create_combine_pipeline()
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2)
        self.assert_all_pipeline_operations_have_pipeline(dp)

    def test_create_and_run_combine_rows_drop_rows_pipeline_from_sources(self):
        a, b, c = self.create_variables()
        co = CombineOptions(row_duplicate_vars=[c])
        dp = self.create_combine_pipeline(combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_row_drop_c)
        self.assert_all_pipeline_operations_have_pipeline(dp)

    def test_create_and_run_combine_rows_drop_entities_pipeline_from_sources(self):
        a, b, c = self.create_variables()
        co = CombineOptions(entity_duplicate_vars=[c])
        dp = self.create_combine_pipeline(combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_entity_drop_c)
        self.assert_all_pipeline_operations_have_pipeline(dp)

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
        self.assert_all_pipeline_operations_have_pipeline(dp)

    def test_create_and_run_combine_cols_pipeline_with_indices(self):
        co = CombineOptions(rows=False)
        dp = self.create_combine_pipeline(indexed=True, combine_options_list=[co])
        dp.execute()

        assert_frame_equal(dp.df, self.expect_merged_1_2_c_index)
        self.assert_all_pipeline_operations_have_pipeline(dp)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dp = self.create_combine_pipeline()

        ds = DataSource(pipeline=dp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_combined_rows_1_2)
        self.assert_all_pipeline_operations_have_pipeline(dp)

    def test_create_and_run_combine_pipeline_three_sources(self):
        dp = self.create_combine_pipeline(include_indices=(0, 1, 2))
        dp.execute()

        assert_frame_equal(dp.df, self.expect_combined_rows_1_2_3)
        self.assert_all_pipeline_operations_have_pipeline(dp)

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
        self.assert_ordered_pipeline_operations(dp2, [dp1, dp2])

    def test_create_nested_transform_pipeline(self):
        self.create_csv_for_2()
        ds2_cols = self.create_indexed_columns_for_2()
        ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
        dtp = self.create_transformation_pipeline(source=ds2)

        self.create_csv_for_3()
        ds3_cols = self.create_indexed_columns_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')

        co = CombineOptions(rows=False)
        dp2 = self.create_combine_pipeline(data_sources=[dtp, ds3], combine_options_list=[co])

        with self.assertRaises(ValueError) as cm:
            dp2.execute()
            exc = cm.exception
            assert 'can only combine columns of data sources with overlapping indices. Column c has' in str(exc)

    def test_create_nested_transform_pipeline_with_variable_subset(self):
        self.create_csv_for_2()
        ds2_cols = self.create_indexed_columns_for_2()
        a, b, c = ds2_cols
        ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
        dtp = self.create_transformation_pipeline(source=ds2, subset=[a.variable, b.variable])

        self.create_csv_for_3()
        ds3_cols = self.create_indexed_columns_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')

        co = CombineOptions(rows=False)
        dp2 = self.create_combine_pipeline(data_sources=[dtp, ds3], combine_options_list=[co])
        dp2.execute()

        assert_frame_equal(dp2.df, self.expect_combined_cols_2_3)
        self.assert_ordered_pipeline_operations(dp2, [dtp, dp2])

    def test_create_nested_transform_pipeline_with_function_subset(self):
        self.create_csv_for_2()
        ds2_cols = self.create_indexed_columns_for_2()
        ds2 = self.create_source(df=None, location=self.csv_path2, columns=ds2_cols, name='two')
        dtp = self.create_transformation_pipeline(source=ds2, subset=select_variables_not_in_index)

        self.create_csv_for_3()
        ds3_cols = self.create_indexed_columns_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, columns=ds3_cols, name='three')

        co = CombineOptions(rows=False)
        dp2 = self.create_combine_pipeline(data_sources=[dtp, ds3], combine_options_list=[co])
        dp2.execute()

        assert_frame_equal(dp2.df, self.expect_combined_cols_2_3)
        self.assert_ordered_pipeline_operations(dp2, [dtp, dp2])
