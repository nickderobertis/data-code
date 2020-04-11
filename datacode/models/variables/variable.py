import operator
from copy import deepcopy
from typing import Sequence, Optional, Union, Callable

from datacode.models.dtypes.base import DataType
from datacode.models.dtypes.convert import convert_str_to_data_type_if_necessary
from datacode.models.variables.expression import Expression
from datacode.models.transform.transform import Transform
from datacode.models.transform.applied import AppliedTransform
from datacode.models.symbols import Symbol, var_key_to_symbol_str, to_symbol_if_necessary


class Variable:

    def __init__(self, key: str, name: Optional[str]=None, symbol: Optional[Union[str, Symbol]] = None,
                 dtype: Optional[Union[str, DataType]] = None,
                 available_transforms: Optional[Sequence[Transform]] = None,
                 applied_transforms: Optional[Sequence[AppliedTransform]] = None,
                 description: str = '', calculation: Optional[Expression] = None):
        if symbol is None:
            symbol = var_key_to_symbol_str(key)
        symbol = to_symbol_if_necessary(symbol)

        if available_transforms is None:
            available_transforms = []

        if applied_transforms is None:
            applied_transforms = []

        dtype = convert_str_to_data_type_if_necessary(dtype)

        self.key = key
        self.dtype = dtype
        self.symbol = symbol
        self.available_transforms = available_transforms
        self.applied_transforms = applied_transforms
        self.description = description
        self.calculation = calculation

        if name is None:
            name = _from_var_name_to_display_name(key)
        self.name = name
        self._orig_name = name
        self._orig_symbol = symbol
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

    def __hash__(self):
        applied_transform_hash = hash(tuple([hash(transform) for transform in self.applied_transforms]))
        return hash((self.key, applied_transform_hash))

    def __add__(self, other) -> Expression:
        return self._create_expression_from_other_and_operator(other, operator.add, 'add')

    def __sub__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.sub, 'subtract', preposition='from')

    def __mul__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.mul, 'multiply', preposition='by')

    def __truediv__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.truediv, 'divide', preposition='by')

    def __pow__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.pow, 'exponentiate', preposition='by')

    @property
    def unique_key(self) -> str:
        key = self.key
        if not self.applied_transforms:
            return key

        transform_keys = [transform.key for transform in self.applied_transforms]
        full_key = '_'.join([key] + transform_keys)
        return full_key

    def _create_expression_from_other_and_operator(self, other, op_func: Callable, operator_name: str,
                                                   preposition: str = 'to') -> 'Expression':
        if isinstance(other, Variable):
            sympy_expr = op_func(self.symbol, other.symbol)
            expr = Expression.from_sympy_expr([self, other], sympy_expr)
            return expr

        if isinstance(other, Expression):
            if other.expr is None:
                raise ValueError(f'cannot {operator_name} expression which does not have .expr, got {other}')
            sympy_expr = op_func(self.symbol, other.expr)
            expr = Expression.from_sympy_expr([self, *other.variables], sympy_expr)
            return expr

        raise ValueError(f'Cannot {operator_name} {other} of type {type(other)} {preposition} {self}, '
                         f'must be Variable or Expression')


    def to_tuple(self):
        return self.key, self.name

    @classmethod
    def from_display_name(cls, display_name):
        name = _from_display_name_to_var_name(display_name)
        return cls(name, display_name)

    def _add_transform(self, transform: Transform):
        self.available_transforms.append(transform)
        self._add_transform_attr(transform)
        self._set_name_and_symbol_by_transforms()

    def _add_applied_transform(self, transform: Transform):
        self.applied_transforms.append(transform)
        self._add_transform(transform)

    def _update_from_transforms(self):
        """
        Adds attributes for current available_transforms

        Notes:
            Does not remove any previous attributes for previous available_transforms
        """

        # Add attributes for available transforms
        for transform in self.available_transforms:
            self._add_transform_attr(transform)

        # Apply name changes from applied transforms
        self._set_name_and_symbol_by_transforms()

    def _set_name_and_symbol_by_transforms(self):
        """
        Apply name changes from applied transforms
        """
        name = self._orig_name
        sym = self._orig_symbol
        for transform in self.applied_transforms:
            if transform.name_func is not None:
                name = transform.name_func(name)
            if transform.symbol_func is not None:
                sym = transform.symbol_func(sym)
        self.name = name
        self.symbol = sym

    def _add_transform_attr(self, transform: Transform):
        def transform_func(*args, **kwargs):
            transform_value = deepcopy(self)
            applied_transform = AppliedTransform.from_transform(transform, *args, **kwargs)
            transform_value.applied_transforms.append(applied_transform)
            transform_value._update_from_transforms()
            transform_value._set_name_and_symbol_by_transforms()
            return transform_value
        setattr(self, transform.key, transform_func)


def _from_var_name_to_display_name(var_name):
    return ' '.join([word for word in var_name.split('_')]).title()


def _from_display_name_to_var_name(display_name):
    return '_'.join([word for word in display_name.split(' ')]).lower()
