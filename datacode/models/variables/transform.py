from copy import deepcopy
from typing import Optional, TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.column.column import Column
    from datacode.models.variables.variable import Variable

from datacode.models.variables.typing import StrFunc, ValueFunc
from datacode.models.logic.partial import partial


class Transform:

    def __init__(self, key: str, name_func: StrFunc = None, data_func: ValueFunc = None,
                 data_func_target: str = 'series'):
        self.key = key
        self.name_func = name_func
        self.data_func = data_func
        self.data_func_target = self._validate_data_func_target(data_func_target)

    def _validate_data_func_target(self, data_func_target: str):
        data_func_target = data_func_target.lower()
        cell_values = ('cell', 'value', 'item', 'c', 'v')
        series_values = ('series', 's')
        df_values = ('df', 'dataframe')
        source_values = ('ds', 'datasource', 'source')
        if data_func_target in cell_values:
            return 'cell'
        if data_func_target in series_values:
            return 'series'
        if data_func_target in df_values:
            return 'dataframe'
        if data_func_target in source_values:
            return 'source'
        raise ValueError(f'Did not pass appropriate data_func_target to Transform {self}, got {data_func_target} '
                         f'which should be one of cell, series, dataframe, or source')

    def apply_to_source(self, source: 'DataSource', preserve_original: bool = True,
                        subset: Optional[Sequence['Variable']] = None) -> 'DataSource':
        """
        Applies transformation to entire data source

        :param source:
        :param preserve_original: True to copy the source before applying transformations. False will decrease
        memory usage but will cause the original source to be partially modified
        :param subset: Variables to apply the transformation to. Defaults to all variables.
        :return:
        """
        if preserve_original:
            source = deepcopy(source)

        if subset:
            variables = subset
        else:
            variables = source.load_variables

        rename_dict = {}
        for selected_var in variables:
            col = source.col_for(var_key=selected_var.key)
            var = col.variable  # Don't use passed variable directly besides key as may be different instance

            # Apply data transformation
            source = self._apply_transform_for_column_and_variable_to_source(source, col, var)

            # Update variable
            orig_name = var.name
            var._add_applied_transform(self)
            new_name = var.name

            # Update column name
            if orig_name != new_name:
                rename_dict[orig_name] = new_name

        if rename_dict:
            source.df.rename(columns=rename_dict, inplace=True)

        return source

    def _apply_transform_for_column_and_variable_to_source(self, source: 'DataSource', column: 'Column',
                                                           variable: 'Variable') -> 'DataSource':
        source = self._apply_data_transform_to_column_and_variable_in_source(source, column, variable)
        return source

    def _apply_data_transform_to_column_and_variable_in_source(self, source: 'DataSource', column: 'Column',
                                                               variable: 'Variable') -> 'DataSource':
        if self.data_func is None:
            return source
        data_func_with_col = partial(self.data_func, column, variable)
        if self.data_func_target == 'cell':
            source.df[variable.name] = source.df[variable.name].apply(data_func_with_col)
        elif self.data_func_target == 'series':
            source.df[variable.name] = data_func_with_col(source.df[variable.name])
        elif self.data_func_target == 'dataframe':
            source.df = data_func_with_col(source.df)
        elif self.data_func_target == 'source':
            source = data_func_with_col(source)
        else:
            raise ValueError(f'Did not pass appropriate data_func_target to Transform {self}, got '
                             f'{self.data_func_target} which should be one of cell, series, dataframe, or source')
        return source



class AppliedTransform(Transform):
    """
    Works like Transform but allows passing of arg and kwargs in advance, similar to functools.partial
    """

    def __init__(self, key: str, *args, name_func: StrFunc = None, data_func: StrFunc = None,
                 data_func_target: Optional[str] = None, **kwargs):
        super().__init__(key, name_func=name_func, data_func=data_func, data_func_target=data_func_target)
        self.args = args
        self.kwargs = kwargs

        if self.name_func is not None:
            self.name_func = partial(self.name_func, ..., *args, **kwargs)
        if self.data_func is not None:
            self.data_func = partial(self.data_func, ..., *args, **kwargs)

    @classmethod
    def from_transform(cls, transform: Transform, *args, **kwargs):
        obj = cls(
            transform.key,
            *args,
            name_func=transform.name_func,
            data_func=transform.data_func,
            data_func_target=transform.data_func_target,
            **kwargs)
        return obj
