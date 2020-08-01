import datetime
import time
from copy import deepcopy
from unittest.mock import patch

from pandas.testing import assert_frame_equal

from datacode import DataSource, Column
import datacode.hooks as dc_hooks
from tests.pipeline.base import PipelineTest, source_transform_func2, source_transform_func
import tests.test_hooks as th


class TestDataTransformationPipeline(PipelineTest):

    def test_create_and_run_transformation_pipeline_from_func(self):
        dtp = self.create_transformation_pipeline()
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)

    def test_create_and_run_transformation_pipeline_from_transform(self):
        dtp = self.create_transformation_pipeline(func=self.source_transform)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_loaded_df_with_transform)
        self.assert_all_pipeline_operations_have_pipeline(dtp)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dtp = self.create_transformation_pipeline()

        ds = DataSource(pipeline=dtp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, self.expect_func_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)

    def test_auto_run_pipeline_by_load_source_with_no_location_and_shared_columns(self):
        self.create_csv()
        all_cols = self.create_columns()

        def transform_func(source: DataSource) -> DataSource:
            new_ds = DataSource(df=source.df, columns=all_cols)
            return new_ds

        dtp = self.create_transformation_pipeline(func=transform_func)

        ds = DataSource(pipeline=dtp, location=self.csv_path_output, columns=all_cols)
        df = ds.df
        assert_frame_equal(df, self.expect_loaded_df_rename_only)
        self.assert_all_pipeline_operations_have_pipeline(dtp)

    def test_create_nested_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()

        dtp2 = self.create_transformation_pipeline(source=dtp)
        dtp2.execute()
        assert_frame_equal(dtp2.df, self.expect_df_double_source_transform)
        self.assert_ordered_pipeline_operations(dtp2, [dtp, dtp2])

    def test_create_nested_generation_pipeline(self):
        dgp = self.create_generator_pipeline()

        dtp = self.create_transformation_pipeline(source=dgp)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_generated_transformed)
        self.assert_ordered_pipeline_operations(dtp, [dgp, dtp])

    def test_create_nested_merge_pipeline(self):
        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_merged_1_2_both_transformed)
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_original_variables_not_affected_by_transform(self):
        self.create_csv()
        a, b, c = self.create_variables()
        ac = Column(a, 'a')
        bc = Column(b, 'b')
        cc = Column(c, 'c')
        all_cols = [ac, bc, cc]
        source = self.create_source(df=None, columns=all_cols)
        dtp = self.create_transformation_pipeline(source=source)
        dtp.execute()

        assert not a.applied_transforms
        assert not b.applied_transforms
        assert not c.applied_transforms
        self.assert_all_pipeline_operations_have_pipeline(dtp)


    def test_transform_on_source_with_normal_and_transformed_of_same_variable(self):
        self.create_csv()
        a, b, c = self.create_variables(transform_data='cell', apply_transforms=False)
        ac = Column(a, 'a')
        bc = Column(b, 'b')
        cc = Column(c, 'c')
        all_cols = [ac, bc, cc]
        include_vars = [
            a,
            a.add_one_cell(),
            b,
            c
        ]
        source = self.create_source(df=None, columns=all_cols, load_variables=include_vars)
        dtp = self.create_transformation_pipeline(source=source)
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df_with_a_and_a_transformed)
        self.assert_all_pipeline_operations_have_pipeline(dtp)

    def test_nested_last_modified_of_source_greater_than_pipeline(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        time.sleep(0.01)
        self.create_csv_for_3()
        ds = self.create_source(df=None, location=self.csv_path3, pipeline=dtp)

        # Should not run pipeline as source is newer
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.test_df3)
        assert th.COUNTER == counter_value  # transform operation not called
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_pipeline_touched_to_be_greater_than_source(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        self.create_csv_for_2()
        ds = self.create_source(df=None, location=self.csv_path2, pipeline=dtp)

        before_touch = datetime.datetime.now()
        dtp.touch()
        after_touch = datetime.datetime.now()
        assert dtp.last_modified < after_touch
        assert dtp.last_modified > before_touch
        assert dmp.last_modified is None
        assert dtp.pipeline_last_modified == dmp.pipeline_last_modified
        assert ds.pipeline_last_modified > ds.last_modified
        # Should run pipeline as was just touched
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.expect_merged_1_2_both_transformed)
        assert th.COUNTER == counter_value + 1  # transform operation called once
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_pipeline_manually_set_to_be_greater_than_source(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        now = datetime.datetime.now()
        later = now + datetime.timedelta(minutes=5)
        dtp = self.create_transformation_pipeline(source=dmp, last_modified=later)
        self.create_csv_for_2()
        ds = self.create_source(df=None, location=self.csv_path2, pipeline=dtp)

        assert dtp.last_modified == later
        assert dmp.last_modified is None
        assert dtp.pipeline_last_modified == dmp.pipeline_last_modified
        assert ds.pipeline_last_modified > ds.last_modified
        # Should run pipeline as was manually set last modified in the future
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.expect_merged_1_2_both_transformed)
        assert th.COUNTER == counter_value + 1  # transform operation called once
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_source_less_than_pipeline(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        self.create_csv_for_2()
        ds = self.create_source(df=None, location=self.csv_path2, pipeline=dtp)

        time.sleep(0.01)
        self.create_csv()  # now earlier source is more recently modified
        assert dmp.data_sources[0].last_modified > ds.last_modified

        # Should run pipeline as original source is newer
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.expect_merged_1_2_both_transformed)
        assert th.COUNTER == counter_value + 1  # transform operation called once
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_source_touched_to_be_greater_than_pipeline(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        self.create_csv_for_2()
        ds = self.create_source(df=None, location=self.csv_path2, pipeline=dtp)

        time.sleep(0.01)
        self.create_csv()  # now earlier source is more recently modified
        assert dmp.data_sources[0].last_modified > ds.last_modified

        before_touch = datetime.datetime.now()
        ds.touch()
        after_touch = datetime.datetime.now()
        assert ds.last_modified < after_touch
        assert ds.last_modified > before_touch
        assert dmp.last_modified is None
        assert dtp.last_modified is None
        assert ds.pipeline_last_modified < ds.last_modified

        # Should not run pipeline as source was touched to be newer
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.test_df2)
        assert th.COUNTER == counter_value  # transform operation not called
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_source_manually_overridden_to_be_greater_than_pipeline(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dmp = self.create_merge_pipeline()

        dtp = self.create_transformation_pipeline(source=dmp)
        self.create_csv_for_2()
        now = datetime.datetime.now()
        later = now + datetime.timedelta(minutes=5)
        ds = self.create_source(df=None, location=self.csv_path2, pipeline=dtp, last_modified=later)

        time.sleep(0.01)
        self.create_csv()
        # now earlier source has csv more recently modified, but this
        # should be ignored due to manually passing last_modified in the future

        assert ds.last_modified == later
        assert dmp.last_modified is None
        assert dtp.last_modified is None
        assert ds.pipeline_last_modified < ds.last_modified
        # Should not run pipeline as source was manually set to be newer
        df = ds.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.test_df2)
        assert th.COUNTER == counter_value  # transform operation not called
        self.assert_ordered_pipeline_operations(dtp, [dmp, dtp])

    def test_nested_last_modified_of_source_less_than_earlier_source(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dtp = self.create_transformation_pipeline()
        self.create_csv_for_2()
        cols2 = self.create_columns()
        ds2 = self.create_source(df=None, location=self.csv_path2, pipeline=dtp, columns=cols2)
        dtp2 = self.create_transformation_pipeline(source=ds2)
        time.sleep(0.01)
        self.create_csv_for_3()
        ds3 = self.create_source(df=None, location=self.csv_path3, pipeline=dtp2)

        time.sleep(0.01)
        self.create_csv()
        ds1 = dtp.data_sources[0]
        # now first source is most recently modified, followed by third source. Middle source is oldest
        assert ds1.last_modified > ds2.last_modified
        assert ds3.last_modified > ds2.last_modified

        # Should run both pipelines as original source is newest
        df = ds3.df

        dc_hooks.reset_hooks()
        assert_frame_equal(df, self.expect_df_double_source_transform)
        assert th.COUNTER == counter_value + 2  # transform operation called once
        self.assert_all_pipeline_operations_have_pipeline(dtp)
        self.assert_all_pipeline_operations_have_pipeline(dtp2)

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_transformation_pipeline_cache(self):
        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        # Run initial pipeline
        cols = self.create_columns()
        dtp = self.create_transformation_pipeline(result_kwargs=dict(columns=cols))
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)
        assert th.COUNTER == counter_value + 1  # transform operation called once

        # Now result should be cached to file. Running a new pipeline with
        # the same options should load from cache
        # Options should be checked for equality, so passing deepcopy it should
        # still cache
        dtp = self.create_transformation_pipeline(
            func=deepcopy(source_transform_func), result_kwargs=dict(columns=cols)
        )
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)
        assert th.COUNTER == counter_value + 1  # transform operation not called again

        # Running with different options should run operations again
        dtp = self.create_transformation_pipeline(
            pipeline_kwargs=dict(difficulty=20), result_kwargs=dict(columns=cols)
        )
        dtp.execute()

        assert_frame_equal(dtp.df, self.expect_func_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)
        assert th.COUNTER == counter_value + 2  # transform operation called again

        dc_hooks.reset_hooks()

