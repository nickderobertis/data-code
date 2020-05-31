import uuid
from copy import deepcopy
import pandas as pd
from functools import partial
import os
import warnings
import datetime
from typing import List, Optional, Any, Dict, Sequence, Type

from graphviz import Digraph

from datacode.graph.base import GraphObject
from datacode.graph.edge import Edge
from datacode.graph.node import Node
from datacode.models.outputter import DataOutputter
from datacode.models.types import SourceCreatingPipeline
from datacode.summarize import describe_df

from datacode.models.variables.variable import Variable
from datacode.models.column.column import Column
from datacode.models.loader import DataLoader


class DataSource:
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
    ]
    update_keys = copy_keys + [
        '_orig_columns',
        '_columns_for_calculate',
        '_orig_load_variables',
        '_vars_for_calculate',
    ]

    def __init__(self, location: Optional[str] = None, df: Optional[pd.DataFrame] = None,
                 pipeline: Optional[SourceCreatingPipeline] = None,
                 columns: Optional[Sequence[Column]] = None,
                 load_variables: Optional[Sequence[Variable]] = None,
                 name: Optional[str] = None, tags: Optional[List[str]] = None,
                 loader_class: Optional[Type[DataLoader]] = None, read_file_kwargs: Optional[Dict[str, Any]] = None,
                 outputter_class: Optional[Type[DataOutputter]] = None,
                 data_outputter_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):

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
        self.primary_node = Node(self.name)
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

    @property
    def last_modified(self) -> Optional[datetime.datetime]:
        if self.location is None or not os.path.exists(self.location):
            # No location. Will trigger pipeline instead
            return None

        return datetime.datetime.fromtimestamp(os.path.getmtime(self.location))

    def _load(self):
        if not hasattr(self, 'data_loader'):
            self._set_data_loader(self.loader_class, pipeline=self.pipeline, **self.read_file_kwargs)
        return self.data_loader()

    def output(self, **data_outputter_kwargs):
        config_dict = deepcopy(self.data_outputter_kwargs)
        config_dict.update(**data_outputter_kwargs)
        outputter = self.outputter_class(self, **config_dict)
        outputter.output()

    def _check_inputs(self, filepath, df):
        pass
        # assert not (filepath is None) and (df is None)

    def _set_data_loader(self, data_loader_class: Type[DataLoader], pipeline: SourceCreatingPipeline = None,
                         **read_file_kwargs):
        run_pipeline = False
        if pipeline is not None:
            # if a source in the pipeline to create this data source was modified more recently than this data source
            # note: if there is no location, will always enter the next block, as last modified time will set
            # to a long time ago
            if (
                    # no existing location for this source, must use pipeline
                    self.last_modified is None or
                    # not able to determine when pipeline sources were modified, must always run pipeline
                    pipeline.last_modified is None or
                    # pipeline sources were modified more recently than this source, run pipeline
                    pipeline.last_modified > self.last_modified
            ):
                # a prior source used to construct this data source has changed. need to re run pipeline
                run_pipeline = True
                if pipeline.last_modified is None:
                    warnings.warn(f"""
                    Was not able to determine last modified of pipeline {pipeline}.
                    Will always run pipeline due to this. Consider manually setting last_modified when creating
                    the pipeline.
                    """.strip())
                elif self.last_modified is None:
                    warnings.warn(f"""
                   Was not able to determine last modified of source {self}.
                   Will run pipeline due to this. This is due to no file currently existing for this source.
                   """.strip())
                else:
                    recent_obj = pipeline.obj_last_modified
                    warnings.warn(f'''{recent_obj} was modified at {recent_obj.last_modified}.
    
                    this data source {self} was modified at {self.last_modified}.
    
                    to get new changes, will load this data source through pipeline rather than from file.''')

            # otherwise, don't need to worry about pipeline, continue handling

        loader = data_loader_class(self, read_file_kwargs, self.optimize_size)

        # If necessary, run pipeline before loading
        # Still necessary to use loader as may be transforming the data
        if run_pipeline:
            def run_pipeline_then_load(pipeline: SourceCreatingPipeline):
                pipeline.execute() # outputs to file
                return loader.load_from_existing_source(
                    pipeline.result,
                    preserve_original=not pipeline.allow_modifying_result
                )
            self.data_loader = partial(run_pipeline_then_load, self.pipeline)
        else:
            self.data_loader = loader.load_from_location

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

    def copy(self, **kwargs):
        if not kwargs:
            return deepcopy(self)

        config_dict = {attr: deepcopy(getattr(self, attr)) for attr in self.copy_keys}

        # Handle df only if not passed as do not want load df unnecessarily
        if 'df' not in kwargs:
            config_dict['df'] = self.df

        config_dict.update(kwargs)

        klass = type(self)
        return klass(**config_dict)

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

        if var_name in self.index_var_names:
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

    @property
    def _graph_contents(self) -> Sequence[GraphObject]:
        elems = [self.primary_node]
        if self.pipeline is not None:
            elems.extend(self.pipeline._graph_contents)
            elems.append(Edge(self.pipeline.primary_node, self.primary_node))
        return elems

    @property
    def graph(self) -> Digraph:
        elems = self._graph_contents
        graph = Digraph(self.name)
        for elem in elems:
            elem.add_to_graph(graph)
        return graph


    def __repr__(self):
        return f'<DataSource(name={self.name}, columns={self.columns})>'


class NoColumnForVariableException(Exception):
    pass
