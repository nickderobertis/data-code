from typing import TYPE_CHECKING, Sequence, Callable, Optional, Union

from sympy import Expr, lambdify, latex
import pandas as pd

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
