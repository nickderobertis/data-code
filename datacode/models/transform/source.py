import uuid
from copy import deepcopy
from functools import partial
from typing import TYPE_CHECKING, Callable, Optional, Sequence


if TYPE_CHECKING:
    from datacode.models.source import DataSource
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column

from datacode.models.transform.transform import Transform
from datacode.models.variables.typing import StrFunc, ValueFunc, SymbolFunc


class SourceTransform(Transform):
    """
    Tracks and applies changes to an entire data source for data, name, and symbol together
    """

    def __init__(self, key: str, name_func: StrFunc = None, data_func: ValueFunc = None,
                 symbol_func: SymbolFunc = None, subset: Sequence['Variable'] = None):
        super().__init__(
            key,
            name_func=name_func,
            data_func=data_func,
            symbol_func=symbol_func,
            data_func_target='source'
        )
        self.subset = subset


    def apply(self, source: 'DataSource', preserve_original: bool = True) -> 'DataSource':
        """
        Applies transformation to data source

        :param source:
        :param preserve_original: True to copy the source before applying transformations. False will decrease
        memory usage but will cause the original source to be partially modified
        :return:
        """
        from datacode.models.source import NoColumnForVariableException
        if preserve_original:
            source = deepcopy(source)
        else:
            # Even when not preserving original, don't want to modify original variables or columns
            # as they may be used in other sources

            # TODO [#64]: Preserving variables in transform apply to source inplace not working
            #
            # This code is supposed to prevent that but is not working as expected.
            # The original variables are still being modified. The problem occurs with both
            # SourceTransform.apply and Transform.apply_to_source. A test has been added which
            # catches this issue in test_lags_as_source_transform_with_subset but it has been
            # commented out for now.
            source.load_variables = deepcopy(source.load_variables)
            source.columns = deepcopy(source.columns)

        if self.subset is None:
            subset = source.load_variables
        else:
            subset = deepcopy(self.subset)

        # Call transformation on source data
        if self.data_func is not None:
            source = self.data_func(source)

        # Collect necessary renames
        rename_dict = {}
        for selected_var in subset:
            try:
                col = source.col_for(variable=selected_var)
            except NoColumnForVariableException:
                # Must have removed this column in the transformation
                continue
            var = col.variable  # Don't use variable directly besides key as may be different instance

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

    @classmethod
    def from_func(cls, func: Callable[['DataSource'], 'DataSource'] = None, key: Optional[str] = None,
                  subset: Sequence['Variable'] = None):
        if key is None:
            key = str(uuid.uuid4())
        return cls(
            key,
            data_func=func,
            subset=subset
        )

    @classmethod
    def from_transform(cls, transform: Transform, subset: Sequence['Variable'] = None):
        # Transform is by variable, need to create new data function which
        # accepts source and applies to subset of variables one by one
        def data_func(subset: Optional[Sequence[str]], source: 'DataSource', **kwargs) -> 'DataSource':
            if subset is None:
                subset = source.load_variables

            for variable in subset:
                col = source.col_for(variable=variable)
                source = transform.data_func(col, variable, source, **kwargs)

            return source

        data_func = partial(data_func, subset)

        return cls(
            transform.key,
            name_func=transform.name_func,
            data_func=data_func,
            symbol_func=transform.symbol_func,
            subset=subset,
        )
