import datetime
import time

from pandas.testing import assert_frame_equal

from datacode import DataSource, Column, Variable
import datacode.hooks as dc_hooks
from tests.pipeline.base import PipelineTest
import tests.test_hooks as th


class TestPipelineReset(PipelineTest):

    def test_reset_pipeline_causes_run_of_same_pipeline_again(self):
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg
        dtp = self.create_transformation_pipeline(always_rerun=True)
        assert th.COUNTER == 0
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 1
        dtp.reset()
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 2
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 2
        dtp.reset(forward=True)
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 3
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_func_df)
        assert th.COUNTER == 3

        self.assert_ordered_pipeline_operations(dtp, [dtp])

    def test_reset_pipeline_does_not_cause_run_of_dependent_pipeline_again(self):
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg
        dgp = self.create_generator_pipeline()
        cols = self.create_columns_for_generated()
        ds = self.create_source(pipeline=dgp, df=None, columns=cols)
        dtp = self.create_transformation_pipeline(source=ds, always_rerun=True)
        assert th.COUNTER == 0
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 1
        dgp.reset()
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 1

        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        self.assert_ordered_pipeline_operations(dtp, [dtp])

    def test_forward_reset_pipeline_causes_run_of_dependent_pipeline_again(self):
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg
        dgp = self.create_generator_pipeline()
        cols = self.create_columns_for_generated()
        ds = self.create_source(pipeline=dgp, df=None, columns=cols)
        dtp = self.create_transformation_pipeline(source=ds, always_rerun=True)
        assert th.COUNTER == 0
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 1
        dgp.reset(forward=True)
        assert th.COUNTER == 1
        dtp.execute()
        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        assert th.COUNTER == 2

        assert_frame_equal(dtp.result.df, self.expect_generated_transformed)
        self.assert_ordered_pipeline_operations(dtp, [dtp])