from copy import deepcopy
from typing import Sequence, Optional, Union

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables.transform import Transform, AppliedTransform


class Variable:

    def __init__(self, key: str, name: Optional[str]=None, dtype: Optional[Union[str, DataType]] = None,
                 available_transforms: Optional[Sequence[Transform]] = None,
                 applied_transforms: Optional[Sequence[AppliedTransform]] = None):
        if available_transforms is None:
            available_transforms = []

        if applied_transforms is None:
            applied_transforms = []

        dtype = convert_str_to_data_type_if_necessary(dtype)

        self.key = key
        self.dtype = dtype
        self.available_transforms = available_transforms
        self.applied_transforms = applied_transforms

        if name is None:
            name = _from_var_name_to_display_name(key)
        self.name = name
        self._orig_name = name
        self._update_from_transforms()

    def __repr__(self):
        return f'Variable(key={self.key}, name={self.name}, applied_transforms={self.applied_transforms})'

    def __eq__(self, other):
        compare_attrs = ('key', 'applied_transforms')
        # If any comparison attribute is missing in the other object, not equal
        if any([not hasattr(other, attr) for attr in compare_attrs]):
            return False
        # If all compare attributes are equal, objects are equal
        return all([getattr(self, attr) == getattr(other, attr) for attr in compare_attrs])

    def to_tuple(self):
        return self.key, self.name

    @classmethod
    def from_display_name(cls, display_name):
        name = _from_display_name_to_var_name(display_name)
        return cls(name, display_name)

    def _add_transform(self, transform: Transform):
        self.available_transforms.append(transform)
        self._add_transform_attr(transform)
        self._set_name_by_transforms()

    def _update_from_transforms(self):
        """
        Adds attributes for current available_transforms

        Notes:
            Does not remove any previous attributes for previous available_transforms
        """

        # Add attributes for available transforms
        for transform in self.available_transforms:
            if not hasattr(self, transform.key):
                self._add_transform_attr(transform)

        # Apply name changes from applied transforms
        self._set_name_by_transforms()

    def _set_name_by_transforms(self):
        """
        Apply name changes from applied transforms
        """
        name = self._orig_name
        for transform in self.applied_transforms:
            name = transform.name_func(name)
        self.name = name

    def _add_transform_attr(self, transform: Transform):
        def transform_func(*args, **kwargs):
            transform_value = deepcopy(self)
            applied_transform = AppliedTransform.from_transform(transform, *args, **kwargs)
            transform_value.applied_transforms.append(applied_transform)
            transform_value._set_name_by_transforms()
            return transform_value
        setattr(self, transform.key, transform_func)


def _from_var_name_to_display_name(var_name):
    return ' '.join([word for word in var_name.split('_')]).title()


def _from_display_name_to_var_name(display_name):
    return '_'.join([word for word in display_name.split(' ')]).lower()
