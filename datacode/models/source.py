import uuid
from copy import deepcopy
from enum import Enum

import pandas as pd
from functools import partial
import os
import datetime
from typing import List, Optional, Any, Dict, Sequence, Type

from mixins import ReprMixin

from datacode.graph.base import GraphObject, Graphable, GraphFunction
from datacode.graph.edge import Edge
from datacode.models.dethash import DeterministicHashDictMixin
from datacode.graph.node import Node
from datacode.logger import logger
from datacode.models.last_modified import LinkedLastModifiedItem, most_recent_last_modified
from datacode.models.outputter import DataOutputter
from datacode.models.types import SourceCreatingPipeline
from datacode.summarize import describe_df

from datacode.models.variables.variable import Variable
from datacode.models.column.column import Column
from datacode.models.loader import DataLoader
import datacode.hooks as hooks


class DataSource(LinkedLastModifiedItem, Graphable, DeterministicHashDictMixin, ReprMixin):
    copy_keys = [
        'location',
        'name',
        'tags',
        'loader_class',
        'outputter_class',
        'pipeline',
        'columns',
        'load_variables',
        'read_file_kwargs',
        'data_outputter_kwargs',
        'optimize_size',
        'difficulty'
    ]
    update_keys = copy_keys + [
        '_orig_columns',
        '_columns_for_calculate',
        '_orig_load_variables',
        '_vars_for_calculate',
    ]
    repr_cols = ['name', 'pipeline', 'columns', 'load_variables']
    _pipeline: Optional[SourceCreatingPipeline] = None

    def __init__(self, location: Optional[str] = None, df: Optional[pd.DataFrame] = None,
                 pipeline: Optional[SourceCreatingPipeline] = None,
                 columns: Optional[Sequence[Column]] = None,
                 load_variables: Optional[Sequence[Variable]] = None,
                 name: Optional[str] = None, tags: Optional[List[str]] = None,
                 loader_class: Optional[Type[DataLoader]] = None, read_file_kwargs: Optional[Dict[str, Any]] = None,
                 outputter_class: Optional[Type[DataOutputter]] = None,
                 data_outputter_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False, last_modified: Optional[datetime.datetime] = None,
                 difficulty: float = 0):
        super().__init__()

        if read_file_kwargs is None:
            read_file_kwargs = {}
        if data_outputter_kwargs is None:
            data_outputter_kwargs = {}
        if loader_class is None:
            loader_class = DataLoader
        if outputter_class is None:
            outputter_class = DataOutputter
        if load_variables is None and columns is not None:
            load_variables = [col.variable for col in columns]
        if columns is not None and not isinstance(columns, list):
            columns = list(columns)

        # In case columns are shared between sources, don't modify the
        # existing columns. Copy the passed columns
        if columns is not None:
            columns = [col.copy() for col in columns]

        # Handle setup for loading columns needed only for calculations
        extra_cols_for_calcs = []
        extra_vars_for_calcs = []
        if load_variables is not None and columns is not None:
            vars_for_calculate = []
            for var in load_variables:
                if var.calculation is not None:
                    for var_for_calc in var.calculation.variables:
                        if var_for_calc not in vars_for_calculate:
                            vars_for_calculate.append(var_for_calc)
            for var in vars_for_calculate:
                all_vars = load_variables + extra_vars_for_calcs
                current_var_keys = [load_var.key for load_var in all_vars]
                if var not in all_vars:
                    # TODO [#40]: don't trigger extra columns when the extra columns are just the untransformed columns
                    #
                    # We are adding extra columns here for calculated variables which require variables not
                    # included in `load_variables`. Currently, it will load extra variables even if
                    # the calculation could just be done before variable transforms. For example, the
                    # test `TestLoadSource.test_load_with_calculate_on_transformed_before_transform` should be able
                    # to complete without adding any extra columns
                    if var.key in current_var_keys:
                        # This is a variable that is already being loaded but with a different transformation
                        # Need to create an extra column for it for later loading handling
                        existing_col = [col for col in columns if col.variable.key == var.key][0]
                        new_col = deepcopy(existing_col)
                        new_col.variable = var
                        extra_cols_for_calcs.append(new_col)
                    extra_vars_for_calcs.append(var)

        if columns is not None:
            all_columns = columns + extra_cols_for_calcs
        else:
            all_columns = columns

        if load_variables is not None:
            all_load_vars = load_variables + extra_vars_for_calcs
        else:
            all_load_vars = load_variables

        self._check_inputs(location, df)
        self.location = location
        self.name = name
        self.tags = tags # TODO: better handling for tags
        self.loader_class = loader_class
        self.outputter_class = outputter_class
        self.pipeline = pipeline
        self._orig_columns: Optional[List[Column]] = columns
        self._columns_for_calculate = extra_cols_for_calcs
        self.columns: Optional[List[Column]] = all_columns
        self._orig_load_variables = load_variables
        self._vars_for_calculate = extra_vars_for_calcs
        self.load_variables = all_load_vars
        self.read_file_kwargs = read_file_kwargs
        self.data_outputter_kwargs = data_outputter_kwargs
        self.optimize_size = optimize_size
        self.difficulty = difficulty
        self._last_modified = last_modified
        self._df = df

        self._validate()

    def _validate(self):
        self._validate_load_variables()

    def _validate_load_variables(self):
        if self.load_variables is None:
            return
        # Ensure uniqueness of loaded variables' names
        var_names = [var.name for var in self.load_variables]
        existing_names = []
        for name in var_names:
            if name in existing_names:
                raise ValueError(f'variable name {name} repeated in load variables')
            existing_names.append(name)

    @property
    def df(self):
        if self._df is None:
            self._df = self._load()
        return self._df

    @df.setter
    def df(self, df):
        self._df = df
        self.refresh_columns_series()

    @property
    def data_exists_at_location(self) -> bool:
        return self.location is not None and os.path.exists(self.location)

    @property
    def last_modified(self) -> Optional[datetime.datetime]:
        if self._last_modified is not None:
            return self._last_modified

        if not self.data_exists_at_location:
            # No location. Will trigger pipeline instead
            return None

        return datetime.datetime.fromtimestamp(os.path.getmtime(self.location))

    @last_modified.setter
    def last_modified(self, value: Optional[datetime.datetime]):
        self._last_modified = value

    @property
    def pipeline(self) -> Optional[SourceCreatingPipeline]:
        return self._pipeline

    @pipeline.setter
    def pipeline(self, item: SourceCreatingPipeline):
        self._pipeline = item
        if item is not None:
            self._add_back_link(item)
            item._add_forward_link(self)

    @property
    def __dict__(self):
        if hasattr(self, '_df'):
            return {**super().__dict__, 'last_modified': self.last_modified}
        return super().__dict__

    def _load(self):
        logger.debug(f'Started loading source {self}')
        hooks.on_begin_load_source(self)
        if not hasattr(self, 'data_loader'):
            self._set_data_loader(self.loader_class, pipeline=self.pipeline, **self.read_file_kwargs)
        df = self.data_loader()
        df = hooks.on_end_load_source(self, df)
        logger.debug(f'Finished loading source {self}')
        return df

    def output(self, **data_outputter_kwargs):
        logger.debug(f'Outputting source {self.name}')
        config_dict = deepcopy(self.data_outputter_kwargs)
        config_dict.update(**data_outputter_kwargs)
        outputter = self.outputter_class(self, **config_dict)
        outputter.output()

    def reset(self, forward: bool = False):
        logger.debug(f'Resetting source {self.name}')
        del self._df
        self._df = None
        if forward:
            for item in self.forward_links:
                item.reset(forward=True)

    def touch(self):
        """
        Mark last_modified as now
        """
        logger.debug(f'Touching source {self.name}')
        self.last_modified = datetime.datetime.now()

    def _check_inputs(self, filepath, df):
        pass
        # assert not (filepath is None) and (df is None)

    @property
    def should_run_pipeline(self) -> bool:
        lm = most_recent_last_modified(self.last_modified, self.pipeline_last_modified)
        if lm is None:
            return True
        if lm == self.last_modified:
            return False
        return True

    def _set_data_loader(self, data_loader_class: Type[DataLoader], pipeline: SourceCreatingPipeline = None,
                         **read_file_kwargs):
        logger.debug(f'Setting data loader for source {self.name}')
        run_pipeline = False
        if pipeline is not None:

            reason = LoadFromPipelineReason.PIPELINE_NEWER
            if self.data_exists_at_location:
                # if a source in the pipeline to create this data source was modified
                # more recently than this data source
                source_lm = self.last_modified
                pipeline_lm = self.pipeline_last_modified
                lm = most_recent_last_modified(source_lm, pipeline_lm)
                if lm is None:
                    run_pipeline = True
                elif lm == pipeline_lm and lm != source_lm:
                    run_pipeline = True
                if pipeline_lm is None:
                    reason = LoadFromPipelineReason.NO_LAST_MODIFIED_IN_PIPELINE
                elif source_lm is None:
                    reason = LoadFromPipelineReason.NO_DATA_AT_LOCATION
            else:
                # No location or no data at location, must run pipeline regardless of last modified
                run_pipeline = True

            if run_pipeline:
                # a prior source used to construct this data source has changed. need to re run pipeline
                report_load_from_pipeline_reason(self, pipeline, reason)

            # otherwise, don't need to worry about pipeline, continue handling

        loader = data_loader_class(self, read_file_kwargs, self.optimize_size)

        # If necessary, run pipeline before loading
        # Still necessary to use loader as may be transforming the data
        if run_pipeline:
            def run_pipeline_then_load(pipeline: SourceCreatingPipeline):
                logger.info(f'Running pipeline then loading source {self.name}')
                pipeline.execute() # outputs to file
                result = loader.load_from_existing_source(
                    pipeline.result,
                    preserve_original=not pipeline.allow_modifying_result
                )
                return result
            self.data_loader = partial(run_pipeline_then_load, self.pipeline)
        else:
            self.data_loader = loader.load_from_location
        logger.debug(f'Finished setting data loader {self.data_loader} for source {self.name}')

    def update_from_source(self, other: 'DataSource', exclude_attrs: Optional[Sequence[str]] = tuple(),
                           include_attrs: Optional[Sequence[str]] = tuple()):
        """
        Updates attributes of this DataSource with another DataSources attributes

        :param other:
        :param exclude_attrs: Any attributes to exclude when updating
        :param include_attrs: Defaults to DataSource.update_keys + ['_df'] but can manually select attributes
        :return:
        """
        if not include_attrs:
            include_attrs = self.update_keys + ['_df']

        for attr in include_attrs:
            if attr not in exclude_attrs:
                other_value = getattr(other, attr)
                setattr(self, attr, other_value)

    def copy(self, keep_refs: Sequence[str] = ('pipeline',), **kwargs) -> 'DataSource':
        """
        Create a new DataSource from this DataSource

        :param keep_refs: Any attributes for which the original reference should
            be kept rather than deep copying
        :param kwargs: DataSource kwargs which should be taken rather than copying
        :return:
        """
        self._wipe_columns_series()
        detached_attrs: Dict[str, Any] = {}
        if keep_refs:
            detached_attrs = self._detach_attrs(keep_refs)
        if not kwargs:
            obj = deepcopy(self)
            self.update(detached_attrs)
            obj.update(detached_attrs)
            obj.refresh_columns_series()
            return obj

        config_dict = {attr: deepcopy(getattr(self, attr)) for attr in self.copy_keys}

        # Handle df only if not passed as do not want load df unnecessarily
        if 'df' not in kwargs:
            config_dict['df'] = self.df

        config_dict.update(kwargs)

        klass = type(self)
        obj = klass(**config_dict)
        obj.update(detached_attrs)
        self.update(detached_attrs)
        obj.refresh_columns_series()
        self.refresh_columns_series()
        return obj

    def _detach_attr(self, attr: str) -> Any:
        value = getattr(self, attr)
        setattr(self, attr, None)
        return value

    def _detach_attrs(self, attrs: Sequence[str]) -> Dict[str, Any]:
        collected_attrs = {attr: self._detach_attr(attr) for attr in attrs}
        return collected_attrs

    def update(self, attrs_dict: Dict[str, Any]):
        for attr, value in attrs_dict.items():
            setattr(self, attr, value)

    def untransformed_col_for(self, variable: Variable) -> Column:
        possible_cols = [col for col in self.columns if col.variable.key == variable.key]
        var_applied_transform_keys = [transform.key for transform in variable.applied_transforms]
        for col in possible_cols:
            if not col.applied_transform_keys:
                # Matches key and no transformations, this is the correct col
                return col
            # Check for subset of transformations
            for i, var_transform_key in enumerate(var_applied_transform_keys):
                if i + 1 >= len(col.applied_transform_keys):
                    # More transforms applied to var than in col, must be the matching col
                    return col
                if col.applied_transform_keys[i] != var_applied_transform_keys[i]:
                    # Mismatching transform between column and variable, must be incorrect column
                    break
        raise NoColumnForVariableException(f'could not find untransformed col for {variable} in {self.columns}')

    def col_for(self, variable: Optional[Variable] = None, var_key: Optional[str] = None,
                orig_only: bool = False, for_calculate_only: bool = False,
                is_unique_key: bool = False) -> Column:
        try:
            # Prefer exact match, need to try this because there may be multiple columns with identical
            # variables besides the applied transforms
            col = self._col_for(
                variable, var_key, orig_only=orig_only,
                for_calculate_only=for_calculate_only, is_unique_key=is_unique_key
            )
        except NoColumnForVariableException:
            # Fall back to matching only on variable key as it may be that it is the correct column but
            # the transforms have not been applied yet
            col = self._col_for(
                variable, var_key, match_key_only=True,
                orig_only=orig_only, for_calculate_only=for_calculate_only,
                is_unique_key=is_unique_key,
            )
        return col

    def _col_for(self, variable: Optional[Variable] = None, var_key: Optional[str] = None,
                 match_key_only: bool = False, orig_only: bool = False, for_calculate_only: bool = False,
                 is_unique_key: bool = False) -> Column:
        if orig_only and for_calculate_only:
            raise ValueError('pass only one of orig_only and for_calculate_only')
        if variable is None and var_key is None:
            raise ValueError('must pass variable or variable key')

        if orig_only:
            col_list = self._orig_columns
        elif for_calculate_only:
            col_list = self._columns_for_calculate
        else:
            col_list = self.columns

        if col_list is None:
            raise NoColumnForVariableException(f'no columns in {self.name}')

        if is_unique_key:
            key_attr = 'unique_key'
        else:
            key_attr = 'key'

        if variable is None:
            possible_cols = [col for col in self.columns if getattr(col.variable, key_attr) == var_key]
            if len(possible_cols) == 0:
                raise NoColumnForVariableException(f'cannot look up col for key {var_key} as no columns match')
            if len(possible_cols) > 1:
                raise ValueError(f'cannot look up col for key {var_key} as multiple columns match: {possible_cols}')
            variable = possible_cols[0].variable
        for col in col_list:
            if match_key_only:
                if getattr(col.variable, key_attr) == getattr(variable, key_attr):
                    return col
            else:
                if col.variable == variable:
                    return col
        raise NoColumnForVariableException(f'could not find column matching {variable} in {self.columns}')

    def col_key_for(self, variable: Optional[Variable] = None, var_key: Optional[str] = None,
                    orig_only: bool = False, for_calculate_only: bool = False) -> Column:
        col = self.col_for(variable, var_key, orig_only=orig_only, for_calculate_only=for_calculate_only)
        return col.load_key

    @property
    def col_var_keys(self) -> List[str]:
        return [col.variable.key for col in self.columns]

    @property
    def col_load_keys(self) -> List[str]:
        return [col.load_key for col in self.columns]

    @property
    def load_var_keys(self) -> List[str]:
        if self.load_variables is None:
            return []

        return [var.key for var in self.load_variables]

    def refresh_columns_series(self):
        logger.debug(f'Refreshing columns series in source {self.name}')
        if self._df is None or self.columns is None:
            return
        for col in self.columns:
            if col.variable not in self.load_variables:
                continue
            if col.variable.name not in list(self._df.columns) + list(self._df.index.names):
                col.series = None
                continue
            series = self.get_series_for(var=col.variable)
            col.series = series

    def _wipe_columns_series(self):
        logger.debug(f'Wiping columns series in source {self.name}')
        cols_attrs = [
            'columns',
            '_orig_columns',
            '_columns_for_calculate',
        ]
        for col_attr in cols_attrs:
            cols = getattr(self, col_attr)
            if cols is not None:
                for col in cols:
                    col.series = None

    def unlink_columns_and_variables(self):
        self._wipe_columns_series()
        copy_attrs = [
            'columns',
            'load_variables',
            '_orig_columns',
            '_columns_for_calculate',
            '_orig_load_variables',
            '_vars_for_calculate',
        ]
        for attr in copy_attrs:
            orig_value = getattr(self, attr)
            setattr(self, attr, deepcopy(orig_value))
        self.refresh_columns_series()

    def get_var_by_key(self, key: str, is_unique_key: bool = False) -> Variable:
        if is_unique_key:
            attr = 'unique_key'
        else:
            attr = 'key'

        collected_variables = []
        for var in self.load_variables:
            match_key = getattr(var, attr)
            if key == match_key:
                collected_variables.append(var)

        matching_str = f'matching {"unique " if is_unique_key else ""}key {key}'
        if len(collected_variables) == 0:
            raise ValueError(f'could not find variable {matching_str}')
        if len(collected_variables) > 1:
            raise ValueError(f'got multiple variables {matching_str}')

        selected_var = collected_variables[0]
        return selected_var

    def get_series_for(self, var_name: Optional[str] = None, var: Optional[Variable] = None,
                       col: Optional[Column] = None, df: Optional[pd.DataFrame] = None) -> pd.Series:
        """
        Extracts series for a variable or column, regardless of whether it is a column or index

        :param var_name:
        :param var:
        :param col:
        :param df: Will use source.df by default, but can also pass a custom df to use
        :return:
        """
        # Validate inputs
        conditions = [
            var_name is not None,
            var is not None,
            col is not None
        ]
        num_passed = len([cond for cond in conditions if cond])

        if num_passed == 0:
            raise ValueError('must pass one of var_name, var, or col to get series')
        elif num_passed > 1:
            raise ValueError('must pass at most one of var_name, var, or col to get series')

        # Main logic
        if var is not None:
            var_name = var.name
        if col is not None:
            var_name = col.variable.name
        if df is None:
            df = self.df

        if var_name in df.index.names:
            # Need to get from index and convert to series
            return pd.Series(df.index.get_level_values(var_name))
        else:
            # Regular column, just look it up normally
            return df[var_name]

    @property
    def index_cols(self) -> List[Column]:
        if self.columns is None:
            return []

        index_vars = self.index_vars

        index_cols = []
        for col in self.columns:
            if col.variable in index_vars:
                index_cols.append(col)

        return index_cols

    @property
    def index_vars(self) -> List[Variable]:
        if self.columns is None:
            return []

        index_vars = []
        for col in self.columns:
            if col.indices:
                for col_idx in col.indices:
                    for var in col_idx.variables:
                        if var not in index_vars:
                            index_vars.append(var)

        # Sort according to order passed in load_variables
        index_vars.sort(key=lambda x: self.load_var_keys.index(x.key))

        return index_vars

    @property
    def index_var_names(self) -> List[str]:
        index_vars = self.index_vars
        return [var.name for var in index_vars]

    @property
    def loaded_columns(self) -> Optional[List[Column]]:
        if self.columns is None:
            return None
        cols = []
        for var in self.load_variables:
            col = self.col_for(var)
            cols.append(col)
        return cols

    def describe(self):
        # TODO [#48]: use columns, variables, indices, etc. in describe
        return describe_df(self.df)

    def _create_series_in_df_for_calculation(self, df: pd.DataFrame, col: Column):
        new_key = str(uuid.uuid4())  # temporary key for this variable
        # should get column which already has data for this variable
        existing_col = self.col_for(col.variable)
        df[new_key] = deepcopy(df[existing_col.load_key])
        col.load_key = new_key

    def _duplicate_column_for_calculation(self, df: pd.DataFrame, orig_var: Variable, new_var: Variable,
                                          pre_rename: bool = True):
        # should get column which already has data for this variable
        existing_col = self.col_for(orig_var)

        if pre_rename:
            existing_var_name = existing_col.load_key
        else:
            existing_var_name = orig_var.name

        col = deepcopy(existing_col)
        col.variable = new_var

        if pre_rename:
            new_key = str(uuid.uuid4())  # temporary key for this variable
            df[new_key] = deepcopy(df[existing_var_name])
            col.load_key = new_key
        else:
            df[new_var.name] = deepcopy(df[existing_var_name])

        self.columns.append(col)

    def _graph_contents(self, include_attrs: Optional[Sequence[str]] = None,
                        func_dict: Optional[Dict[str, GraphFunction]] = None) -> List[GraphObject]:
        pn = self.primary_node(include_attrs, func_dict)
        elems = [pn]
        if self.pipeline is not None:
            elems.extend(self.pipeline._graph_contents(include_attrs, func_dict))
            elems.append(Edge(self.pipeline.primary_node(include_attrs, func_dict), pn))
        return elems

    @property
    def cache_json_location(self) -> Optional[str]:
        if self.location is None:
            return None

        return f'{self.location}.dcc.json'


class NoColumnForVariableException(Exception):
    pass


class LoadFromPipelineReason(Enum):
    PIPELINE_NEWER = 'pipeline_newer'
    NO_DATA_AT_LOCATION = 'no_data_at_location'
    NO_LAST_MODIFIED_IN_PIPELINE = 'no_last_modified_in_pipeline'


def report_load_from_pipeline_reason(
    source: DataSource,
    pipeline: SourceCreatingPipeline,
    reason: LoadFromPipelineReason
):
    if reason == LoadFromPipelineReason.NO_LAST_MODIFIED_IN_PIPELINE:
        logger.warning(
            f"Was not able to determine last modified of pipeline "
            f"{pipeline.name}. Will always run pipeline due to this. "
            f"Consider manually setting last_modified when creating "
            f"the pipeline."
        )
    elif reason == LoadFromPipelineReason.NO_DATA_AT_LOCATION:
        logger.warning(
            f"Was not able to determine last modified of source "
            f"{source.name}. Will run pipeline due to this. "
            f"This is due to no file currently existing for this source."
        )
    elif reason == LoadFromPipelineReason.PIPELINE_NEWER:
        recent_obj, obj_lm = source.pipeline_obj_last_modified
        try:
            recent_obj_name = recent_obj.name
        except AttributeError:
            # Must be Operation, get name from pipeline instead
            recent_obj_name = pipeline.name
        logger.info(
            f'{recent_obj_name} was modified at {obj_lm}. '
            f'This data source {source.name} was modified at '
            f'{source.last_modified}. To get new changes, '
            f'will load this data source through pipeline '
            f'rather than from file.'
        )