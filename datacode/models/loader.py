import uuid
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional, List

import pandas as pd
from datacode.models.column.column import Column
from pd_utils.optimize.load import read_file

from datacode.models.dtypes.datetime_type import DatetimeType

if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.variables.variable import Variable

class DataLoader:

    def __init__(self, source: 'DataSource', read_file_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):
        if read_file_kwargs is None:
            read_file_kwargs = {}
        self.source = source
        self.optimize_size = optimize_size
        self.read_file_kwargs = read_file_kwargs

    def load(self) -> pd.DataFrame:
        df = self.read_file_into_df()
        self.rename_columns(df)
        if self.optimize_size:
            df = self.optimize_df_size(df)
        self.assign_series_to_columns(df)
        df = self.try_to_calculate_variables(df)
        df = self.pre_transform_clean_up(df)
        df = self.apply_transforms(df)
        df = self.post_transform_clean_up(df)
        df = self.try_to_calculate_variables(df)
        self.assign_series_to_columns(df)

        return df

    def read_file_into_df(self) -> pd.DataFrame:
        if self.source.location is None:
            return pd.DataFrame()

        read_file_config = dict()

        # Set which columns to load
        if self.source.load_variables and self.source.columns:
            variable_keys = [var.key for var in self.source.load_variables]
            usecols = []
            for var_key in variable_keys:
                for col_key, col in self.source.columns.items():
                    if col.variable.key == var_key:
                        # Got column matching the desired variable
                        usecols.append(col_key)  # add the original column name in the dataset to usecols
            read_file_config['usecols'] = usecols

        # Set the data types of the columns
        if self.source.columns:
            dtypes = {}
            datetime_dtypes = []  # pandas requires separate handling for datetime
            for col_key, col in self.source.columns.items():
                if col.dtype is not None:
                    if col.dtype.categorical:
                        dtypes[col_key] = 'category'
                    elif col.dtype == DatetimeType():
                        # Track datetime separately
                        datetime_dtypes.append(col_key)
                    else:
                        dtypes[col_key] = col.dtype.pd_class
            if dtypes:
                read_file_config['dtype'] = dtypes
            if datetime_dtypes:
                read_file_config['parse_dates'] = datetime_dtypes

        read_file_config.update(self.read_file_kwargs)

        return read_file(self.source.location, **read_file_config)

    def assign_series_to_columns(self, df: pd.DataFrame):
        if not self.source.columns:
            return
        for var in self.source.load_variables:
            if var.key not in self.source.col_var_keys:
                if var.calculation is None:
                    raise ValueError(f'passed variable {var} but not calculated and not '
                                     f'in columns {self.source.columns}')
                continue
            col = self.source.col_for(var)
            col.series = df[var.name]

    def optimize_df_size(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO [#17]: implement df size optimization
        #
        # Needs to be after adding data types to variables. Then can use data types to optimize
        raise NotImplementedError('implement df size optimization')

    def rename_columns(self, df: pd.DataFrame):
        if not self.source.columns:
            return

        rename_dict = {}
        for variable in self.source.load_variables:
            if variable.key not in self.source.col_var_keys:
                if variable.calculation is None:
                    raise ValueError(f'passed variable {variable} but not calculated and not '
                                     f'in columns {self.source.columns}')
                continue
            orig_name = self.source.col_key_for(var_key=variable.key)
            rename_dict[orig_name] = variable.name
        df.rename(columns=rename_dict, inplace=True)

    def try_to_calculate_variables(self, df: pd.DataFrame):
        if not self.source.columns:
            return df

        # Create temporary source so that transform can have access to df and all columns with one object
        temp_source = deepcopy(self.source)
        temp_source.df = df
        temp_source.name = '_temp_source_for_transform'

        for variable in self.source.load_variables:
            if variable.key in self.source.col_var_keys:
                # Variable already exists in the data, either from original source or previously calculated
                continue

            if variable.calculation is None:
                raise ValueError(f'passed variable {variable} but not calculated and not '
                                 f'in columns {self.source.columns}')
            required_variables = variable.calculation.variables
            has_all_required_variables = True
            calc_with_cols = []
            for req_var in required_variables:
                if not has_all_required_variables:
                    break
                col = self.source.col_for(req_var)
                calc_with_cols.append(col)
                col_pre_applied_transform_keys = deepcopy(col.applied_transform_keys)
                for transform in req_var.applied_transforms:
                    # Need to make sure all the same transforms have been applied to
                    # the column before the calculation
                    if transform.key in col_pre_applied_transform_keys:
                        col_pre_applied_transform_keys.remove(transform.key)
                    else:
                        has_all_required_variables = False
                        break

            if has_all_required_variables:
                # Actually do calculation
                new_series = variable.calculation.func(calc_with_cols)
                new_series.name = variable.name
                # TODO: determine how to set index for columns from calculated variables
                new_col = Column(variable, dtype=str(new_series.dtype), series=new_series)
                temp_source.df[variable.name] = new_series
                # TODO: better way of storing calculated columns than uuid in columns dictionary
                #
                # The dictionary of columns has keys as names in the original source and values as columns.
                # A calculated column is not in the original source, so uuid was used for now just to ensure
                # that these columns can be in the dictionary, but they should be tracked separately.
                temp_source.columns[uuid.uuid4()] = new_col
                temp_source = _apply_transforms_to_var(variable, new_col, temp_source)
                self.source.columns[uuid.uuid4()] = new_col

        return temp_source.df

    def apply_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.source.columns:
            return df

        # Create temporary source so that transform can have access to df and all columns with one object
        # TODO [#28]: don't copy df, use same df
        temp_source = deepcopy(self.source)
        temp_source.df = df
        temp_source.name = '_temp_source_for_transform'

        for var in self.source.load_variables:
            if var.key not in self.source.col_var_keys:
                if var.calculation is None:
                    raise ValueError(f'passed variable {var} but not calculated and not '
                                     f'in columns {self.source.columns}')
                continue
            column = self.source.col_for(var_key=var.key)
            temp_source = _apply_transforms_to_var(var, column, temp_source)
        return temp_source.df

    def pre_transform_clean_up(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform_clean_up(self, df: pd.DataFrame) -> pd.DataFrame:
        return df


def _apply_transforms_to_var(var: 'Variable', column: Column, source: 'DataSource') -> 'DataSource':
    col_pre_applied_transform_keys = deepcopy(column.applied_transform_keys)
    for transform in var.applied_transforms:
        if transform.key in col_pre_applied_transform_keys:
            # Transformation was already applied in the saved data source, skip this transformation
            # remove from applied transformations, because same transformation may be applied multiple times.
            # If desired transformation happens twice, and it is only once in the source column, will still
            # need to apply it once
            col_pre_applied_transform_keys.remove(transform.key)
            continue
        source = transform._apply_transform_for_column_and_variable_to_source(source, column, var)
        column.applied_transform_keys.append(transform.key)
    return source
