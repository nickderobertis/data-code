import uuid
from typing import Optional, Callable, Any, TYPE_CHECKING
if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column
    from datacode.models.source import DataSource

from datacode.models.transform.transform import Transform
from datacode.models.logic.partial import partial
from datacode.models.variables.typing import StrFunc, ValueFunc, SymbolFunc


class AppliedTransform(Transform):
    """
    Works like Transform but allows passing of arg and kwargs in advance, similar to functools.partial
    """
    repr_cols = ['key', 'name_func', 'data_func', 'symbol_func', 'data_func_target', 'args', 'kwargs']

    def __init__(self, key: str, *args, name_func: StrFunc = None, data_func: ValueFunc = None,
                 symbol_func: SymbolFunc = None,
                 data_func_target: Optional[str] = None, **kwargs):
        super().__init__(
            key,
            name_func=name_func,
            symbol_func=symbol_func,
            data_func=data_func,
            data_func_target=data_func_target
        )
        self.args = args
        self.kwargs = kwargs

        if self.name_func is not None:
            self.name_func = partial(self.name_func, ..., *args, **kwargs)
        if self.symbol_func is not None:
            self.symbol_func = partial(self.symbol_func, ..., *args, **kwargs)
        if self.data_func is not None:
            self.data_func = partial(self.data_func, ..., *args, **kwargs)

    @classmethod
    def from_transform(cls, transform: Transform, *args, **kwargs):
        obj = cls(
            transform.key,
            *args,
            name_func=transform.name_func,
            symbol_func=transform.symbol_func,
            data_func=transform.data_func,
            data_func_target=transform.data_func_target,
            **kwargs)
        return obj

    def __eq__(self, other) -> bool:
        same = super().__eq__(other)
        if not same:
            return False
        try:
            same = same and self.args == other.args
            same = same and self.kwargs == other.kwargs
            return same
        except AttributeError:
            return False

    # TODO [#69]: should not be necessary to explicitly hash AppliedTransform
    #
    # Already hashing `Transform` base class. But was getting unhashable type: 'AppliedTransform'
    # even with that.
    def __hash__(self):
        hash_attrs = ['key', 'data_func_target']
        hash_items = tuple([getattr(self, attr) for attr in hash_attrs])
        return hash(hash_items)

    @classmethod
    def from_func(cls, func: Callable[['Column', 'Variable', 'DataSource', Any], 'DataSource'],
                  key: Optional[str] = None,
                  data_func_target: str = 'source', **func_kwargs):
        if key is None:
            key = str(uuid.uuid4())
        return cls(
            key,
            data_func=func,
            data_func_target=data_func_target,
            **func_kwargs
        )