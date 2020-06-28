from typing import Type, Optional, Sequence, Union, List

from mixins import ReprMixin
from mixins.attrequals import EqOnAttrsMixin, EqHashMixin

from datacode.models.transform.transform import Transform


class DataType(EqHashMixin, EqOnAttrsMixin, ReprMixin):
    _recursive_hash_convert = True
    names: Sequence[str] = tuple()
    name_roots: Sequence[str] = tuple()
    equal_attrs : List[str] = [
        'py_class',
        'pd_class',
        'categorical',
        'ordered',
        'names',
    ]
    repr_cols = ['categorical', 'ordered']

    def __init__(self, py_class: Type, pd_class: Optional[Type] = None,
                 transforms: Optional[Sequence[Transform]] = None, is_numeric: bool = False,
                 categorical: bool = False,
                 ordered: bool = False):
        self.names = [name.lower() for name in self.names]

        if transforms is None:
            transforms = []
            
        if pd_class is None:
            pd_class = py_class

        self.py_class = py_class
        self.pd_class = pd_class
        self.transforms = transforms
        self.categorical = categorical
        self.ordered = ordered
        self.is_numeric = is_numeric

    @classmethod
    def from_str(cls, dtype: str, categorical: bool = False, ordered: bool = False):
        # TODO [#22]: eliminate repeated from_str methods in dtypes
        #
        # Currently the same from_str method is in the subclasses because they have a different __init__
        # Only int and float have different from_str methods, and both of those are the same. Create
        # mixin or intermediate classes to eliminate repeated code.
        raise NotImplementedError('must implement from_str in subclass of DataType')

    @property
    def read_file_arg(self) -> Union[Type, str]:
        """
        The argument which should be passed to read_csv and other file reading
        methods in Pandas to ensure the correct data type
        """
        if self.categorical:
            return 'category'
        return self.pd_class

    @property
    def index_arg(self) -> Union[Type, str]:
        """
        The argument which should be passed to index.astype
        :return:
        """
        return self.read_file_arg
