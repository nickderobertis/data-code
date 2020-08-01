import datetime
from typing import Callable, List, Union, Tuple, Optional, Dict, Any, Sequence, TYPE_CHECKING

from datacode.logger import logger

if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column
    from datacode.models.pipeline.merge import DataMergePipeline
import pandas as pd
from functools import partial

from datacode.models.logic.merge.display import display_merge_summary
from datacode.models.pipeline.operations.operation import DataOperation, OperationOptions
from datacode.models.source import DataSource
from datacode.models.logic.merge import left_merge_df
from datacode.summarize import describe_df

StrList = List[str]
StrListOrNone = Union[None, StrList]
TwoDfTuple = Tuple[pd.DataFrame, pd.DataFrame]


class DataMerge(DataOperation):
    num_required_sources = 2
    options: 'MergeOptions'
    result: 'DataSource'

    def __init__(self, pipeline: 'DataMergePipeline', data_sources: Sequence[DataSource],
                 merge_options: 'MergeOptions', **result_kwargs):
        self._merged_name = None
        self._merged_type = None
        self._merged_str = None
        super().__init__(
            pipeline,
            data_sources,
            merge_options,
            **result_kwargs
        )
        self.output_name = self.merged_name

    def _execute(self):
        logger.info(f'Running merge function {self.merge_str}')
        left_df, right_df = self._get_merge_dfs()
        self.result.df = self.options.merge_function(
            left_df, right_df,
            self.options.on_names,
            **self.options.merge_function_kwargs
        )
        if self.options.post_merge_func is not None:
            self.result.df = self.options.post_merge_func(self.result.df)

        # TODO [#78]: merge source variable combine logic doesn't seem to be working completely correctly
        #
        # Had to put safe=False in merge pipeline output to make it happen
        if 'columns' not in self.result_kwargs:
            left_ds, right_ds = self.data_sources[0], self.data_sources[1]
            load_variables = []
            columns = []
            if left_ds.load_variables:
                for var in left_ds.load_variables:
                    if self.options.left_df_keep_cols is None or var.name in self.options.left_df_keep_cols:
                        load_variables.append(var)
                        columns.append(left_ds.col_for(var))
            if right_ds.load_variables:
                for var in right_ds.load_variables:
                    if self.options.right_df_keep_cols is None or var.name in self.options.right_df_keep_cols:
                        # don't repeat variables and columns. Merge on variables will be repeated,
                        # perhaps even with different transformations, so explictly skip them
                        if var not in load_variables and var.name not in self.options.on_names:
                            load_variables.append(var)
                            columns.append(right_ds.col_for(var))
            self.result.columns = columns
            self.result.load_variables = load_variables

        logger.info(f"""
        {self.data_sources[0].name} obs: {len(left_df)}
        {self.data_sources[1].name} obs: {len(right_df)}
        Merged obs: {len(self.result.df)}
        """)

    def summary(self, *summary_args, summary_method: str=None, summary_function: Callable=None,
                             summary_attr: str=None, **summary_method_kwargs):
        display_merge_summary(
            self,
            *summary_args,
            summary_method=summary_method,
            summary_function=summary_function,
            summary_attr=summary_attr,
            **summary_method_kwargs
        )

    def describe(self):
        display_merge_summary(
            self,
            summary_function=describe_df,
            disp=False # don't display from describe_df as will display from display_merge_summary
        )

    def _get_merge_dfs(self) -> TwoDfTuple:
        left_df = self.data_sources[0].df
        right_df = self.data_sources[1].df

        # Handle pre process funcs
        if self.options.left_df_pre_process_func is not None:
            left_df = self.options.left_df_pre_process_func(left_df)
        if self.options.right_df_pre_process_func is not None:
            right_df = self.options.right_df_pre_process_func(right_df)

        # Handle selecting variables on processed df
        if self.options.left_df_keep_cols is not None:
            left_df = left_df[self.options.left_df_keep_cols]
        if self.options.right_df_keep_cols is not None:
            right_df = right_df[self.options.right_df_keep_cols]

        return left_df, right_df

    @property
    def merged_name(self):
        if self._merged_name is None:
            self._merged_name = f'{self.data_sources[0].name} & {self.data_sources[1].name}'
        return self._merged_name

    @property
    def merge_str(self):
        if self._merged_str is None:
            self._merged_str = f'''
            {self.options.merge_function.__name__}(
                {self.data_sources[0].name},
                {self.data_sources[1].name},
                {self.options.on_names},
                **{self.options.merge_function_kwargs}
            )
            '''
        return self._merged_str

    def __repr__(self):
        return f'<DataMerge(left={self.data_sources[0]}, right={self.data_sources[1]})>'


class LastMergeFinishedException(Exception):
    pass


class MergeOptions(OperationOptions):
    op_class = DataMerge

    def __init__(self, on: Sequence[Union[str, 'Variable', 'Column']], out_path=None, merge_function=left_merge_df,
                 left_df_keep_cols: StrListOrNone=None, right_df_keep_cols: StrListOrNone=None,
                 left_df_pre_process_func: Callable=None, right_df_pre_process_func: Callable=None,
                 left_df_pre_process_kwargs: Optional[Dict[str, Any]] = None,
                 right_df_pre_process_kwargs: Optional[Dict[str, Any]] = None,
                 post_merge_func: Callable = None, post_merge_func_kwargs: Optional[Dict[str, Any]] = None,
                 allow_modifying_result: bool = True, result_kwargs: Optional[Dict[str, Any]] = None,
                 always_rerun: bool = False, last_modified: Optional[datetime.datetime] = None,
                 **merge_function_kwargs):
        """

        passed args to merge func will be (
            left_df_pre_process_func(left_df, **left_df_pre_process_kwargs)[left_df_keep_cols],
            right_df_pre_process_func(right_df, **right_df_pre_process_kwargs)[right_df_keep_cols],
            *merge_function_kwargs,
            **merge_function_kwargs
        )

        if left_df_keep_cols is None, will instead pass left_df_pre_process_func(left_df, **left_df_pre_process_kwargs).
        If right_df_keep_cols is None, will instead pass right_df_pre_process_func(right_df, **right_df_pre_process_kwargs).
        If left_df_pre_process_func is None, will instead pass left_df or left_df[left_df_keep_cols] depending
            on whether left_df_keep_cols was passed. Similar behavior for right.

        Args:
            *merge_function_args:
            out_path:
            merge_function:
            left_df_keep_cols:
            right_df_keep_cols:
            post_merge_func: function to be called on data after merge
            post_merge_func_kwargs: kwargs to be passed to post_merge_func
            **merge_function_kwargs:

        :param allow_modifying_result: When DataSources are directly linked to pipelines, loading
            source from pipeline can cause modifications in the pipeline's result source. Set to False
            to ensure it won't be modified (but uses more memory). Setting to False should only be needed
            if multiple sources load from the same pipeline in one session
        :param always_rerun: Whether to re-run operation if executed multiple times
        :param last_modified: manually override last modified
        """
        from datacode.models.variables.variable import Variable
        from datacode.models.column.column import Column

        if left_df_pre_process_kwargs is None:
            left_df_pre_process_kwargs = {}

        if right_df_pre_process_kwargs is None:
            right_df_pre_process_kwargs = {}

        if post_merge_func_kwargs is None:
            post_merge_func_kwargs = {}

        if left_df_pre_process_func is None:
            left_df_pre_process_func = lambda x: x

        if right_df_pre_process_func is None:
            right_df_pre_process_func = lambda x: x

        if post_merge_func is None:
            post_merge_func = lambda x: x

        on_names = []
        for item in on:
            if isinstance(item, str):
                on_names.append(item)
            elif isinstance(item, Variable):
                on_names.append(item.name)
            elif isinstance(item, Column):
                on_names.append(item.variable.name)

        self.on = on
        self.on_names = on_names
        self.out_path = out_path
        self.merge_function = merge_function
        self.merge_function_kwargs = merge_function_kwargs
        self.left_df_keep_cols = left_df_keep_cols
        self.right_df_keep_cols = right_df_keep_cols
        self.left_df_pre_process_func = partial(left_df_pre_process_func, **left_df_pre_process_kwargs)
        self.right_df_pre_process_func = partial(right_df_pre_process_func, **right_df_pre_process_kwargs)
        self.post_merge_func = partial(post_merge_func, **post_merge_func_kwargs)
        self.allow_modifying_result = allow_modifying_result
        self.result_kwargs = result_kwargs
        self.always_rerun = always_rerun
        self.last_modified = last_modified

    def __repr__(self):
        return f'<DataMerge(on_names={self.on_names}, merge_function={self.merge_function.__name__}, ' \
               f'kwargs={self.merge_function_kwargs})>'

    def update(self, **kwargs):
        self.merge_function_kwargs.update(**kwargs)