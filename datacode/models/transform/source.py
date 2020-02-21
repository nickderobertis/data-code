import uuid
from copy import deepcopy
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from datacode.models.source import DataSource

from datacode.models.transform.transform import Transform
from datacode.models.variables.typing import StrFunc, ValueFunc, SymbolFunc


class SourceTransform(Transform):
    """
    Tracks and applies changes to an entire data source for data, name, and symbol together
    """

    def __init__(self, key: str, name_func: StrFunc = None, data_func: ValueFunc = None,
                 symbol_func: SymbolFunc = None):
        super().__init__(
            key,
            name_func=name_func,
            data_func=data_func,
            symbol_func=symbol_func,
            data_func_target='source'
        )

    def apply(self, source: 'DataSource', preserve_original: bool = True) -> 'DataSource':
        """
        Applies transformation to data source

        :param source:
        :param preserve_original: True to copy the source before applying transformations. False will decrease
        memory usage but will cause the original source to be partially modified
        :return:
        """
        if preserve_original:
            source = deepcopy(source)

        # Call transformation on source data
        if self.data_func is not None:
            source = self.data_func(source)

        # Collect necessary renames
        rename_dict = {}
        for selected_var in source.load_variables:
            col = source.col_for(var_key=selected_var.key)
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
    def from_func(cls, func: Callable[['DataSource'], 'DataSource'] = None, key: Optional[str] = None):
        if key is None:
            key = str(uuid.uuid4())
        return cls(
            key,
            data_func=func
        )
