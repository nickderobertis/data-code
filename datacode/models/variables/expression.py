import operator
from typing import TYPE_CHECKING, Sequence, Callable, Optional

from sympy import Expr, lambdify, latex
import pandas as pd

from datacode.models.variables.compare import functions_are_equal

if TYPE_CHECKING:
    from datacode.models.variables.variable import Variable
    from datacode.models.column.column import Column


class Expression:

    def __init__(self, variables: Sequence['Variable'], func: Callable[[Sequence['Column']], pd.Series],
                 expr: Optional[Expr] = None, summary: Optional[str] = None):
        self.variables = variables
        self.func = func
        self.expr = expr
        self.summary = summary

    def __repr__(self):
        return f'<Expression(variables={self.variables}, explanation="{self.explanation}")>'

    @classmethod
    def from_sympy_expr(cls, variables: Sequence['Variable'], expr: Expr, **kwargs):
        def eval_sympy_eq(cols: Sequence['Column']) -> pd.Series:
            symbols = []
            data = []
            for col in cols:
                if col.series is None:
                    raise ValueError(f'Need column {col} to have data for calculation, but col.series is None')
                symbols.append(col.variable.symbol)
                data.append(col.series)

            func = lambdify(symbols, expr)
            series = func(*data)
            return series

        return cls(
            variables,
            eval_sympy_eq,
            expr=expr
        )

    @property
    def explanation(self) -> Optional[str]:
        if self.summary is not None:
            return self.summary

        if self.expr is not None:
            return latex(self.expr)

    def __eq__(self, other):
        same = True
        try:
            same = same and self.variables == other.variables
            same = same and functions_are_equal(self.func, other.func)
            same = same and self.expr == other.expr
            same = same and self.summary == other.summary
            return same
        except AttributeError:
            return False

    def __add__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.add, 'add')

    def __sub__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.sub, 'subtract', preposition='from')

    def __mul__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.mul, 'multiply', preposition='by')

    def __truediv__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.truediv, 'divide', preposition='by')

    def __pow__(self, other):
        return self._create_expression_from_other_and_operator(other, operator.pow, 'exponentiate', preposition='by')

    def _create_expression_from_other_and_operator(self, other, op_func: Callable, operator_name: str,
                                                   preposition: str = 'to') -> 'Expression':
        from datacode.models.variables.variable import Variable

        if self.expr is None:
            raise ValueError(f'cannot {operator_name} expression which does not have .expr, got {self}')

        if isinstance(other, Variable):
            sympy_expr = op_func(self.expr, other.symbol)
            expr = Expression.from_sympy_expr([*self.variables, other], sympy_expr)
            return expr

        if isinstance(other, Expression):
            if other.expr is None:
                raise ValueError(f'cannot {operator_name} expression which does not have .expr, got {other}')
            sympy_expr = op_func(self.expr, other.expr)
            expr = Expression.from_sympy_expr([*self.variables, *other.variables], sympy_expr)
            return expr

        raise ValueError(f'Cannot {operator_name} {other} of type {type(other)} {preposition} {self}, '
                         f'must be Variable or Expression')
