from copy import deepcopy
import pandas as pd
from functools import partial
import os
import warnings
import datetime
from typing import Callable, TYPE_CHECKING, List, Optional, Any, Dict, Sequence, Type

from pd_utils.optimize.load import read_file as read_file_into_df

from datacode.models.variables.variable import Variable
from datacode.models.column.column import Column
from datacode.models.loader import DataLoader

if TYPE_CHECKING:
    from datacode.models.pipeline.merge import DataMergePipeline


class DataSource:

    def __init__(self, location: Optional[str] = None, df: Optional[pd.DataFrame] = None,
                 pipeline: Optional['DataMergePipeline'] = None, columns: Optional[Sequence[Column]] = None,
                 load_variables: Optional[Sequence[Variable]] = None,
                 name: Optional[str] = None, tags: Optional[List[str]] = None,
                 loader_class: Optional[Type[DataLoader]] = None, read_file_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):

        if read_file_kwargs is None:
            read_file_kwargs = {}
        if loader_class is None:
            loader_class = DataLoader
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
                    vars_for_calculate.extend(var.calculation.variables)
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
        self.pipeline = pipeline
        self._orig_columns: Optional[List[Column]] = columns
        self._columns_for_calculate = extra_cols_for_calcs
        self.columns: Optional[List[Column]] = all_columns
        self._orig_load_variables = load_variables
        self._vars_for_calculate = extra_vars_for_calcs
        self.load_variables = all_load_vars
        self.read_file_kwargs = read_file_kwargs
        self.optimize_size = optimize_size
        self._df = df
        self.name_type = f'{name}'

    @property
    def df(self):
        if self._df is None:
            self._df = self._load()
        return self._df

    @df.setter
    def df(self, df):
        self._df = df

    @property
    def last_modified(self):
        # TODO: cleaner way of handling non-existent DataSource location for modified time
        #
        # Currently just setting to a date far in the past, but then the output shows this
        if self.location is None or not os.path.exists(self.location):
            # No location. Setting last modified time as a long time ago, so will trigger pipeline instead
            return datetime.datetime(1899, 1, 1)

        return datetime.datetime.fromtimestamp(os.path.getmtime(self.location))

    def _load(self):
        self._touch_pipeline()
        if not hasattr(self, 'data_loader'):
            self._set_data_loader(self.loader_class, pipeline=self.pipeline, **self.read_file_kwargs)
        return self.data_loader()

    def output(self, **to_csv_kwargs):
        self.df.to_csv(self.location, **to_csv_kwargs)

    def _check_inputs(self, filepath, df):
        pass
        # assert not (filepath is None) and (df is None)

    def _set_data_loader(self, data_loader_class: Type[DataLoader], pipeline: 'DataMergePipeline' =None, **read_file_kwargs):
        run_pipeline = False
        if pipeline is not None:
            # if a source in the pipeline to create this data source was modified more recently than this data source
            # note: if there is no location, will always enter the next block, as last modified time will set
            # to a long time ago
            if pipeline.last_modified > self.last_modified:
                # a prior source used to construct this data source has changed. need to re run pipeline
                recent_source = pipeline.source_last_modified
                warnings.warn(f'''data source {recent_source} was modified at {recent_source.last_modified}.

                this data source {self} was modified at {self.last_modified}.

                to get new changes, will load this data source through pipeline rather than from file.''')

                run_pipeline = True
            # otherwise, don't need to worry about pipeline, continue handling

        loader = data_loader_class(self, read_file_kwargs, self.optimize_size)

        # If necessary, run pipeline before loading
        # Still necessary to use loader as may be transforming the data
        if run_pipeline:
            def run_pipeline_then_load(pipeline):
                # TODO: should not have to write to disk with pipeline to then load it in source
                #
                # Loader should be able to take a DataFrame instead of just a filepath, then use
                # that here. Will need to handle columns, variables, transformations correctly
                pipeline.execute() # outputs to file
                return loader.load()
            self.data_loader = partial(run_pipeline_then_load, self.pipeline)
        else:
            self.data_loader = loader.load

    def _touch_pipeline(self):
        """
        Pipeline may be passed using pyfileconf.Selector, in which case it is
        a pyfileconf.selector.models.itemview.ItemView object. _set_data_loader accesses a property of
        the pipeline before it's configured, and so won't work correctly. By accessing the .item proprty of the ItemView,
        we get the original item back
        Returns:

        """
        from pyfileconf.selector.models.itemview import _is_item_view

        if _is_item_view(self.pipeline):
            self.pipeline = self.pipeline.item

    def copy(self, **kwargs):
        if not kwargs:
            return deepcopy(self)

        copy_keys = [
            'location',
            'pipeline',
            'columns',
            'load_variables',
            'name',
            'tags',
            'loader_class',
            'read_file_kwargs',
            'optimize_size'
        ]

        config_dict = {attr: deepcopy(getattr(self, attr)) for attr in copy_keys}

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
                orig_only: bool = False, for_calculate_only: bool = False) -> Column:
        try:
            # Prefer exact match, need to try this because there may be multiple columns with identical
            # variables besides the applied transforms
            col = self._col_for(variable, var_key, orig_only=orig_only, for_calculate_only=for_calculate_only)
        except NoColumnForVariableException:
            # Fall back to matching only on variable key as it may be that it is the correct column but
            # the transforms have not been applied yet
            col = self._col_for(
                variable, var_key, match_key_only=True, orig_only=orig_only, for_calculate_only=for_calculate_only
            )
        return col

    def _col_for(self, variable: Optional[Variable] = None, var_key: Optional[str] = None,
                match_key_only: bool = False, orig_only: bool = False, for_calculate_only: bool = False) -> Column:
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

        if variable is None:
            possible_cols = [col for col in self.columns if col.variable.key == var_key]
            if len(possible_cols) != 1:
                raise ValueError(f'cannot look up col for key {var_key} as multiple columns match: {possible_cols}')
            variable = possible_cols[0].variable
        for col in col_list:
            if match_key_only:
                if col.variable.key == variable.key:
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
        return index_vars


    def __repr__(self):
        return f'<DataSource(name={self.name}, columns={self.columns})>'


class NoColumnForVariableException(Exception):
    pass
