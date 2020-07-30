import uuid
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional, List, Tuple

import pandas as pd
from mixins import ReprMixin

from datacode.logger import logger
from datacode.models.column.column import Column
from pd_utils.optimize.load import read_file

from datacode.models.dtypes.date_type import DateType
from datacode.models.dtypes.datetime_type import DatetimeType

if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.variables.variable import Variable


class DataLoader(ReprMixin):
    repr_cols = ['read_file_kwargs']

    def __init__(self, source: 'DataSource', read_file_kwargs: Optional[Dict[str, Any]] = None,
                 optimize_size: bool = False):
        if read_file_kwargs is None:
            read_file_kwargs = {}
        self.source = source
        self.optimize_size = optimize_size
        self.read_file_kwargs = read_file_kwargs

        # First var in tup is original variable, second is variable which needs column created
        self._calculated_variables_that_need_duplication: Dict[str, Tuple[Variable, Variable]] = {}

    def load_from_location(self) -> pd.DataFrame:
        """
        Used when df does not already exist in the source, loads it from location

        :return:
        """
        logger.debug(f'Loading source {self.source.name} from location {self.source.location} with {self}')
        self.pre_read()
        df = self.read_file_into_df()
        df = self.post_read(df)
        logger.debug(f'Setting columns and index for source {self.source.name}')
        df = self.duplicate_columns_for_calculations_assign_series(df)
        self.rename_columns(df)
        df = self.post_rename(df)
        if self.optimize_size:
            df = self.optimize_df_size(df)
        self.set_df_index(df)
        logger.debug(f'Finished setting columns and index for source {self.source.name}')
        df = self.apply_calculations_transforms_and_drops(df)

        return df

    def load_from_existing_source(self, other_source: 'DataSource', preserve_original: bool = False) -> pd.DataFrame:
        """
        Used when df already exists in another source, just selects load_variables and
        applies calculations and transformations

        :return:
        """
        if preserve_original:
            df = other_source.df.copy()
        else:
            df = other_source.df

        # TODO [#59]: when loading from existing source, handle when indices do not match
        #
        # Currently the code assumes the same index in the existing source and loaded source.
        # Need to add code to change the index. But if this was due to a desired aggregation,
        # how should the user select what aggregation would be applied?

        self.select_variables_in_existing_source(df)
        df = self.apply_calculations_transforms_and_drops(df)
        return df

    def apply_calculations_transforms_and_drops(self, df: pd.DataFrame):
        logger.debug(f'Applying calculations, transforms, and drops for for source {self.source.name} in loader {self}')
        self.assign_series_to_columns(df)
        df = self.pre_calculate(df)
        df = self.try_to_calculate_variables(df)
        self.duplicate_calculated_columns_if_necessary(df)
        df = self.pre_transform(df)
        df = self.apply_transforms(df)
        df = self.post_transform(df)
        df = self.try_to_calculate_variables(df)
        self.assign_series_to_columns(df)
        self.drop_variables(df)
        df = self.post_load(df)
        logger.debug(f'Finished applying calculations, transforms, and drops '
                     f'for for source {self.source.name} in loader {self}')
        return df

    def duplicate_calculated_columns_if_necessary(self, df: pd.DataFrame):
        for var_key, (orig_var, new_var) in self._calculated_variables_that_need_duplication.items():
            self.source._duplicate_column_for_calculation(
                df,
                orig_var=orig_var,
                new_var=new_var,
                pre_rename=False,
            )

    def select_variables_in_existing_source(self, df: pd.DataFrame):
        if not (self.source.load_variables and self.source.columns):
            return

        keep_names = []
        for var in self.source.load_variables:
            for col in self.source.columns:
                if col.variable.key == var.key:
                    # Got column matching the desired variable
                    keep_names.append(var.name)  # add the variable name in the dataset to keep_names
        drop_cols = [col for col in df.columns if col not in keep_names]
        df.drop(drop_cols, axis=1, inplace=True)

    def read_file_into_df(self) -> pd.DataFrame:
        logger.debug(f'Reading file into df for source {self.source.name} from location {self.source.location}')
        if self.source.location is None:
            logger.debug(f'No location so empty DataFrame was loaded for {self.source.name}')
            return pd.DataFrame()

        read_file_config = dict()

        # Set which columns to load
        if self.source.load_variables and self.source.columns:
            usecols = []
            for var in self.source.load_variables:
                for col in self.source.columns:
                    if col.variable.key == var.key:
                        # Got column matching the desired variable
                        usecols.append(col.load_key)  # add the original column name in the dataset to usecols
            read_file_config['usecols'] = usecols

        # Set the data types of the columns
        date_dtypes = []
        datetime_dtypes = {}  # pandas requires separate handling for datetime
        if self.source.columns:
            dtypes = {}
            for col in self.source.columns:
                if col.dtype is not None:
                    if isinstance(col.dtype, DatetimeType):
                        # Track datetime separately
                        datetime_dtypes[col.load_key] = col.dtype
                    elif isinstance(col.dtype, DateType):
                        datetime_dtypes[col.load_key] = col.dtype
                        date_dtypes.append(col.load_key)
                    else:
                        dtypes[col.load_key] = col.dtype.read_file_arg
            if dtypes:
                read_file_config['dtype'] = dtypes
            if datetime_dtypes:
                read_file_config['parse_dates'] = list(datetime_dtypes.keys())


        read_file_config.update(self.read_file_kwargs)

        df = read_file(self.source.location, **read_file_config)

        # Extra cleanup for dates
        for col_key, dtype in datetime_dtypes.items():
            if isinstance(dtype, DateType):
                # Remove timestamp portion from date types
                df[col_key] = df[col_key].dt.date
            elif isinstance(dtype, DatetimeType) and dtype.tz is not None:
                # Convert to time zone stored in dtype
                try:
                    df[col_key] = df[col_key].dt.tz_localize(dtype.tz)
                except TypeError as e:
                    if 'Already tz-aware, use tz_convert' in str(e):
                        df[col_key] = df[col_key].dt.tz_convert(dtype.tz)
                    else:
                        raise e

        logger.debug(f'Finished reading df of {len(df)} rows for source '
                     f'{self.source.name} from location {self.source.location}')
        return df

    def set_df_index(self, df: pd.DataFrame):
        if not self.source.index_vars:
            return

        df.set_index(self.source.index_var_names, inplace=True)

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
            series = self.source.get_series_for(var=var, df=df)
            col.series = series

    def duplicate_columns_for_calculations_assign_series(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO [#39]: more efficient implementation of loading variables for calculations
        #
        # The `DataLoader` checks what variables are needed for calculations that are not
        # included in `load_variables`, and if it requires multiple transformations of
        # a variable, then it copies that series for as many transformations are needed.
        # It would be better to have an implementation that doesn't require carrying copies
        # through everything.

        for col in self.source._columns_for_calculate:
            # Extra column is already in source, but need to add to df
            self.source._create_series_in_df_for_calculation(df, col)

        if not self.source.load_variables:
            return df

        # Now need to see if we need multiple transformations of a variable, then also copy that column
        # so that it won't get used up for just one of the transformations/original variable
        unique_var_keys: Dict[str, Variable] = {}
        for var in self.source.load_variables:
            # If the variable is needed multiple times, but not because of calculations (multiple transforms)
            if var.key in unique_var_keys and var not in self.source._vars_for_calculate:
                if var.calculation is not None:
                    # This calculated variable needs to be duplicated, but it has not been calculated yet.
                    # Therefore add to a list of variables which need to be duplicated after calculation.
                    if var.key not in self._calculated_variables_that_need_duplication:
                        self._calculated_variables_that_need_duplication[var.key] = (unique_var_keys[var.key], var)
                    continue
                # Got a variable multiple times, duplicate the column
                # Use the original variable for duplication as the column will already exist for that variable
                self.source._duplicate_column_for_calculation(
                    df,
                    orig_var=unique_var_keys[var.key],
                    new_var=var,
                )
            else:
                # Not a repeated variable, just add to tracking dict
                unique_var_keys[var.key] = var

        # Reorder df to be the same order as passed load variables
        col_order: Dict[str, int] = {}
        for col in self.source.columns:
            if col.variable.key not in self.source.load_var_keys:
                # Must be an unloaded column, skip it
                continue
            if col.variable.calculation is not None:
                # Calculated variables won't be in the data yet, skip them
                continue
            order = self.source.load_var_keys.index(col.variable.key)
            col_order[col.load_key] = order
        order_tups = list(col_order.items())
        order_tups.sort(key=lambda x: x[1])
        col_keys = [key for key, order in order_tups]
        df = df[col_keys]

        return df

    def optimize_df_size(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO [#17]: implement df size optimization
        #
        # Needs to be after adding data types to variables. Then can use data types to optimize
        raise NotImplementedError('implement df size optimization')

    def rename_columns(self, df: pd.DataFrame):
        from datacode.models.source import NoColumnForVariableException
        if not self.source.columns:
            return

        rename_dict = {}
        for variable in self.source._orig_load_variables:
            if variable.key not in self.source.col_var_keys:
                if variable.calculation is None:
                    raise ValueError(f'passed variable {variable} but not calculated and not '
                                     f'in columns {self.source.columns}')
                continue
            col = self.source.col_for(variable)
            rename_dict[col.load_key] = variable.name
            col.variable = variable
        for variable in self.source._vars_for_calculate:
            try:
                col = self.source.col_for(variable, for_calculate_only=True)
                rename_dict[col.load_key] = variable.name
                col.variable = variable
            except NoColumnForVariableException:
                # Must be using a pre-existing column rather than a newly generated column, need to rename that instead
                col = self.source.col_for(variable, orig_only=True)
                rename_dict[col.load_key] = variable.name
                col.variable = variable
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
                # TODO [#34]: determine how to set index for columns from calculated variables
                new_col = Column(variable, dtype=str(new_series.dtype), series=new_series)
                temp_source.df[variable.name] = new_series
                self.source.columns.append(new_col)

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
            if not var.applied_transforms:
                continue
            if var.key not in self.source.col_var_keys:
                if var.calculation is None:
                    raise ValueError(f'passed variable {var} but not calculated and not '
                                     f'in columns {self.source.columns}')
                continue
            column = self.source.col_for(var)
            temp_source = _apply_transforms_to_var(var, column, temp_source)
        return temp_source.df

    def drop_variables(self, df: pd.DataFrame):
        if not self.source._vars_for_calculate:
            # Only need to drop if extra variables were loaded for calculations
            return

        drop_names = [var.name for var in self.source._vars_for_calculate]
        df.drop(drop_names, axis=1, inplace=True)

    def pre_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def pre_read(self):
        pass

    def post_read(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def pre_calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def post_load(self, df: pd.DataFrame) -> pd.DataFrame:
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
        column.variable = var  # overwrite untransformed variable with transformed variable
    return source
