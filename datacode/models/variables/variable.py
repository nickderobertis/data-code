from functools import partial
from typing import Sequence, Callable


class Variable:

    def __init__(self, name, display_name=None):
        self.name = name

        if display_name is None:
            display_name = _from_var_name_to_display_name(name)
        self.display_name = display_name

    def __repr__(self):
        return f'Variable(name={self.name}, display_name={self.display_name})'

    def __eq__(self, other):
        compare_attrs = ('name', 'display_name')
        # If any comparison attribute is missing in the other object, not equal
        if any([not hasattr(other, attr) for attr in compare_attrs]):
            return False
        # If all compare attributes are equal, objects are equal
        return all([getattr(self, attr) == getattr(other, attr) for attr in compare_attrs])

    def to_tuple(self):
        return self.name, self.display_name

    @classmethod
    def from_display_name(cls, display_name):
        name = _from_display_name_to_var_name(display_name)
        return cls(name, display_name)

    def _add_names(self, names: Sequence[str]) -> str:
        out_str = self.display_name
        for name in names:
            out_str += f' {name}'
        return out_str

    def _set_attr_for_names(self, attr: str, names: Sequence[str]):
        new_name = self._add_names(names)
        setattr(self, attr, new_name)

    def _set_attr_for_name_func(self, attr: str, name_func: Callable):
        self_func = partial(name_func, self.display_name)
        setattr(self, attr, self_func)


def _from_var_name_to_display_name(var_name):
    return ' '.join([word for word in var_name.split('_')]).title()


def _from_display_name_to_var_name(display_name):
    return '_'.join([word for word in display_name.split(' ')]).lower()
