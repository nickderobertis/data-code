import pandas as pd
from pandas._testing import assert_frame_equal

import datacode.hooks as dc_hooks
from datacode import DataSource

from tests.pipeline.base import PipelineTest

COUNTER = 0


def increase_counter_hook(*args, **kwargs):
    global COUNTER
    COUNTER += 1
    return kwargs


def raise_error(*args, **kwargs):
    raise ValueError


def increase_counter_hook_discard_first_arg(*args):
    increase_counter_hook()
    return args[1:]


def increase_counter_hook_return_only_second_arg(*args):
    increase_counter_hook()
    return args[1]


def increment_df_a_variable_hook(source: DataSource, df: pd.DataFrame) -> pd.DataFrame:
    df['a'] += 1
    return df


class TestPipelineHooks(PipelineTest):

    def test_no_pipeline_hook(self):
        counter_value = COUNTER
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value

    def test_reset_hook(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_pipeline = increase_counter_hook
        dc_hooks.reset_hooks()
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value

    def test_chain_at_end(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_pipeline = increase_counter_hook
        dc_hooks.chain('on_begin_execute_pipeline', raise_error)
        dgp = self.create_generator_pipeline()
        with self.assertRaises(ValueError) as cm:
            dgp.execute()
        assert COUNTER == counter_value + 1

    def test_chain_at_beginning(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_pipeline = increase_counter_hook
        dc_hooks.chain('on_begin_execute_pipeline', raise_error, at_end=False)
        dgp = self.create_generator_pipeline()
        with self.assertRaises(ValueError) as cm:
            dgp.execute()
        assert COUNTER == counter_value

    def test_on_begin_execute_pipeline(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_pipeline = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_begin_execute_pipeline', increase_counter_hook)
        dgp.execute()
        assert COUNTER == counter_value + 3

    def test_on_end_execute_pipeline(self):
        counter_value = COUNTER
        dc_hooks.on_end_execute_pipeline = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_end_execute_pipeline', increase_counter_hook)
        dgp.execute()
        assert COUNTER == counter_value + 3

    def test_on_begin_execute_operation(self):
        counter_value = COUNTER
        dc_hooks.on_begin_execute_operation = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_begin_execute_operation', increase_counter_hook)
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 3

    def test_on_end_execute_operation(self):
        counter_value = COUNTER
        dc_hooks.on_end_execute_operation = increase_counter_hook
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_end_execute_operation', increase_counter_hook)
        dgp = self.create_generator_pipeline()
        dgp.execute()
        assert COUNTER == counter_value + 3

    def test_on_begin_load_source(self):
        counter_value = COUNTER
        dc_hooks.on_begin_load_source = increase_counter_hook
        self.create_csv()
        ds = self.create_source(df=None)
        df = ds.df
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_begin_load_source', increase_counter_hook)
        ds = self.create_source(df=None)
        df = ds.df
        assert COUNTER == counter_value + 3

    def test_on_end_load_source(self):
        dc_hooks.on_end_load_source = increment_df_a_variable_hook
        self.create_csv()
        ds = self.create_source(df=None)
        df = ds.df
        assert_frame_equal(df, self.expect_df_no_rename_a_plus_1)
        dc_hooks.chain('on_end_load_source', increment_df_a_variable_hook)
        ds = self.create_source(df=None)
        df = ds.df
        assert_frame_equal(df, self.expect_df_no_rename_a_plus_2)

    def test_on_begin_apply_variable_transform(self):
        counter_value = COUNTER
        dc_hooks.on_begin_apply_variable_transform = increase_counter_hook_discard_first_arg
        self.create_csv()
        cols = self.create_columns(transform_data='series')
        ds = self.create_source(df=None, columns=cols)
        df = ds.df
        assert COUNTER == counter_value + 2
        dc_hooks.chain('on_begin_apply_variable_transform', increase_counter_hook_discard_first_arg)
        cols = self.create_columns(transform_data='series')
        ds = self.create_source(df=None, columns=cols)
        df = ds.df
        assert COUNTER == counter_value + 6

    def test_on_end_apply_variable_transform(self):
        counter_value = COUNTER
        dc_hooks.on_end_apply_variable_transform = increase_counter_hook_return_only_second_arg
        self.create_csv()
        cols = self.create_columns(transform_data='series')
        ds = self.create_source(df=None, columns=cols)
        df = ds.df
        assert COUNTER == counter_value + 2
        dc_hooks.chain('on_end_apply_variable_transform', increase_counter_hook_return_only_second_arg)
        cols = self.create_columns(transform_data='series')
        ds = self.create_source(df=None, columns=cols)
        df = ds.df
        assert COUNTER == counter_value + 6

    def test_on_begin_apply_source_transform(self):
        counter_value = COUNTER
        dc_hooks.on_begin_apply_source_transform = increase_counter_hook_return_only_second_arg
        dtp = self.create_transformation_pipeline()
        dtp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_begin_apply_source_transform', increase_counter_hook_return_only_second_arg)
        dtp = self.create_transformation_pipeline()
        dtp.execute()
        assert COUNTER == counter_value + 3

    def test_on_end_apply_source_transform(self):
        counter_value = COUNTER
        dc_hooks.on_end_apply_source_transform = increase_counter_hook_return_only_second_arg
        dtp = self.create_transformation_pipeline()
        dtp.execute()
        assert COUNTER == counter_value + 1
        dc_hooks.chain('on_end_apply_source_transform', increase_counter_hook_return_only_second_arg)
        dtp = self.create_transformation_pipeline()
        dtp.execute()
        assert COUNTER == counter_value + 3