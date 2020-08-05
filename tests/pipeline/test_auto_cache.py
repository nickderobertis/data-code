import os
import shutil
from typing import Optional

import pytest
from pandas._testing import assert_frame_equal
import pandas as pd
from numpy import nan

import datacode as dc
import datacode.hooks as dc_hooks
from datacode import Variable

from tests.pipeline.base import PipelineTest, EXPECT_GENERATED_DF
from tests.utils import AUTO_CACHE_PATH, start_count_operation_hook
import tests.utils as tu
import tests.test_hooks as th


class AutoCacheTest(PipelineTest):
    expect_out_path = os.path.join(AUTO_CACHE_PATH, 'None.csv')
    cache_key = 'my-cache-key'
    expect_out_path_with_cache_key = os.path.join(AUTO_CACHE_PATH, f'None.{cache_key}.csv')
    pipeline_key = ''
    expect_cached_df: pd.DataFrame
    expect_cached_loaded_df: pd.DataFrame

    def setup_method(self, *args, **kwargs):
        super().setup_method(*args, **kwargs)
        start_count_operation_hook()

    def teardown_method(self, *args, **kwargs):
        super().teardown_method(*args, **kwargs)
        dc.options.reset()
        if os.path.exists(AUTO_CACHE_PATH):
            shutil.rmtree(AUTO_CACHE_PATH)

    def execute_pipeline(self, expect_cached_loaded_df: Optional[pd.DataFrame] = None, **kwargs) -> dc.DataPipeline:
        raise NotImplementedError

    def cached_df(self, with_key: bool = False) -> pd.DataFrame:
        if with_key:
            out_path = self.expect_out_path_with_cache_key
        else:
            out_path = self.expect_out_path
        return pd.read_csv(out_path).convert_dtypes()

    def assert_main_operation_called(self, n: int):
        assert tu.OPERATION_COUNTER[self.pipeline_key] == n

    def assert_load_opertion_called(self, n: int):
        assert tu.OPERATION_COUNTER['load'] == n

    def assert_cached(self, expect_cached_df: Optional[pd.DataFrame] = None,
                      expect_cached_loaded_df: Optional[pd.DataFrame] = None,
                      with_key: bool = False, **kwargs):
        if expect_cached_df is None:
            expect_cached_df = self.expect_cached_df
        if expect_cached_loaded_df is None:
            expect_cached_loaded_df = self.expect_cached_loaded_df

        self.assert_main_operation_called(0)
        self.assert_load_opertion_called(0)
        dp = self.execute_pipeline(expect_cached_loaded_df=expect_cached_loaded_df, **kwargs)
        assert_frame_equal(self.cached_df(with_key=with_key), expect_cached_df)
        assert_frame_equal(dp.result.df, expect_cached_loaded_df)
        self.assert_main_operation_called(1)
        self.assert_load_opertion_called(0)
        dp = self.execute_pipeline(
            expect_cached_loaded_df=expect_cached_loaded_df, create_csv=False, **kwargs
        )  # this time loads from cache
        self.assert_main_operation_called(1)
        self.assert_load_opertion_called(1)
        assert_frame_equal(self.cached_df(with_key=with_key), expect_cached_df)
        assert_frame_equal(dp.result.df, expect_cached_loaded_df)

    def assert_not_cached(self, with_key: bool = False, **kwargs):
        dgp = self.execute_pipeline(**kwargs)
        with self.assertRaises(FileNotFoundError) as cm:
            assert_frame_equal(self.cached_df(with_key=with_key), self.expect_cached_df)


class TestAutoCacheGenerator(AutoCacheTest):
    pipeline_key = 'generator'
    expect_cached_df = expect_cached_loaded_df = EXPECT_GENERATED_DF

    def execute_pipeline(self, expect_cached_loaded_df: Optional[pd.DataFrame] = None, **kwargs) -> dc.DataPipeline:
        if expect_cached_loaded_df is None:
            expect_cached_loaded_df = self.expect_cached_loaded_df
        config = dict(
            out_path=None
        )
        config.update(kwargs)

        dgp = self.create_generator_pipeline(**config)
        dgp.execute()

        assert_frame_equal(dgp.df, expect_cached_loaded_df)
        self.assert_all_pipeline_operations_have_pipeline(dgp)
        return dgp

    def test_no_options_no_cache(self):
        self.assert_not_cached()

    def test_set_auto_location_option_get_cache(self):
        dc.options.set_default_cache_location(AUTO_CACHE_PATH)
        self.assert_cached()

    def test_set_pipeline_cache_location_get_cache(self):
        self.assert_cached(pipeline_kwargs=dict(auto_cache_location=AUTO_CACHE_PATH))

    def test_set_pipeline_cache_location_and_cache_key(self):
        self.assert_cached(
            pipeline_kwargs=dict(auto_cache_location=AUTO_CACHE_PATH, cache_key=self.cache_key),
            with_key=True
        )

    def test_set_auto_location_get_cache_with_key(self):
        dc.options.set_default_cache_location(AUTO_CACHE_PATH)
        self.assert_cached(
            pipeline_kwargs=dict(cache_key=self.cache_key),
            with_key=True
        )

    def test_set_auto_location_pass_no_cache_get_no_cache(self):
        dc.options.set_default_cache_location(AUTO_CACHE_PATH)
        self.assert_not_cached(
            pipeline_kwargs=dict(auto_cache=False),
        )
        self.assert_not_cached(
            pipeline_kwargs=dict(auto_cache=False, cache_key=self.cache_key),
        )


class TestAutoCacheTransform(TestAutoCacheGenerator):
    pipeline_key = 'transform'
    expect_cached_df = pd.DataFrame(
        [
            (2, 3, 'd'),
            (4, 5, 'd'),
            (6, 7, 'e')
        ],
        columns=['a', 'b', 'c']
    ).convert_dtypes()
    expect_cached_loaded_df = PipelineTest.expect_func_df

    def execute_pipeline(self, expect_cached_loaded_df: Optional[pd.DataFrame] = None, **kwargs) -> dc.DataPipeline:
        if expect_cached_loaded_df is None:
            expect_cached_loaded_df = self.expect_cached_loaded_df

        config = dict(
            out_path=None
        )
        config.update(kwargs)

        dtp = self.create_transformation_pipeline(**config)
        dtp.execute()

        assert_frame_equal(dtp.df, expect_cached_loaded_df)
        self.assert_all_pipeline_operations_have_pipeline(dtp)
        return dtp

    # TODO [#123]: Saving calculated variables
    #
    # When variables are calculated there is no corresponding column
    # being passed to DataSource so it does not have a consistent load_key
    # for saving purposes. Passing the column results in an error for it not
    # existing in the original data. Need to be able to pass columns which are
    # from calculations and not to load from existing data
    @pytest.mark.skip(reason='saving calculated variables not working')
    def test_cache_with_variable_transforms(self):
        dc.options.set_default_cache_location(AUTO_CACHE_PATH)
        self.create_csv()
        all_cols = self.create_columns()
        a, b, c = self.create_variables(transform_data='cell', apply_transforms=False)
        d = Variable('d', 'D', calculation=a + b.add_one_cell())
        d_col = dc.Column(d, 'd', dtype='int')
        all_cols.append(d_col)
        ds = self.create_source(df=None, columns=all_cols, load_variables=[a.add_one_cell(), b.add_one_cell(), c, d])
        self.assert_cached(
            source=ds,
            func=lambda source: source,
            expect_cached_loaded_df=self.expect_loaded_df_with_calculate_on_transformed_before_and_after_transform,
            result_kwargs=dict(columns=all_cols)
        )


class TestAutoCacheCombine(TestAutoCacheGenerator):
    pipeline_key = 'combine'
    expect_cached_df = pd.DataFrame(
        [
            (1, 2, 'd', nan, nan),
            (3, 4, 'd', nan, nan),
            (5, 6, 'e', nan, nan),
            (nan, nan, 'd', 10, 20),
            (nan, nan, 'e', 50, 60),
        ],
        columns=['a', 'b', 'c', 'e', 'f'],
    ).convert_dtypes()
    expect_cached_loaded_df = PipelineTest.expect_combined_rows_1_2

    def execute_pipeline(self, expect_cached_loaded_df: Optional[pd.DataFrame] = None, **kwargs) -> dc.DataPipeline:
        if expect_cached_loaded_df is None:
            expect_cached_loaded_df = self.expect_cached_loaded_df

        config = dict(
            last_option_config=dict(out_path=None),
        )
        config.update(kwargs)

        dp = self.create_combine_pipeline(**config)
        dp.execute()

        assert_frame_equal(dp.df, expect_cached_loaded_df)
        self.assert_all_pipeline_operations_have_pipeline(dp)
        return dp


class TestAutoCacheMerge(TestAutoCacheGenerator):
    pipeline_key = 'merge'
    expect_cached_df = pd.DataFrame(
        [
            (1, 2, 'd', 10, 20),
            (3, 4, 'd', 10, 20),
            (5, 6, 'e', 50, 60),
        ],
        columns=['a', 'b', 'c', 'e', 'f']
    ).convert_dtypes()
    expect_cached_loaded_df = PipelineTest.expect_merged_1_2

    def execute_pipeline(self, expect_cached_loaded_df: Optional[pd.DataFrame] = None, **kwargs) -> dc.DataPipeline:
        if expect_cached_loaded_df is None:
            expect_cached_loaded_df = self.expect_cached_loaded_df

        config = dict(
           last_option_config=dict(out_path=None),
        )
        config.update(kwargs)

        dp = self.create_merge_pipeline(**config)
        dp.execute()

        assert_frame_equal(dp.df, expect_cached_loaded_df)
        self.assert_all_pipeline_operations_have_pipeline(dp)
        return dp
