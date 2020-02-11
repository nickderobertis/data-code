from typing import Type, Optional, Sequence

from datacode.models.variables.transform import Transform


class DataType:
    names: Sequence[str] = tuple()

    def __init__(self, py_class: Type, pd_class: Optional[Type] = None, names: Optional[Sequence[str]] = None,
                 transforms: Optional[Sequence[Transform]] = None, categorical: bool = False,
                 ordered: bool = False):
        if names is None:
            names = []
        names = [name.lower() for name in names]

        if transforms is None:
            transforms = []
            
        if pd_class is None:
            pd_class = py_class

        self.py_class = py_class
        self.pd_class = pd_class
        self.names = names
        self.transforms = transforms
        self.categorical = categorical
        self.ordered = ordered

    @classmethod
    def from_str(cls, dtype: str, categorical: bool = False, ordered: bool = False):
        # TODO [#22]: eliminate repeated from_str methods in dtypes
        #
        # Currently the same from_str method is in the subclasses because they have a different __init__
        # Only int and float have different from_str methods, and both of those are the same. Create
        # mixin or intermediate classes to eliminate repeated code.
        raise NotImplementedError('must implement from_str in subclass of DataType')


