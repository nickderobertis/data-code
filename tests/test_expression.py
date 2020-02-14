from datacode.models.variables.variable import Variable

from datacode.models.variables.expression import Expression
from tests.test_variables import VariableTest, VAR1_ARGS, VAR2_ARGS, VAR3_ARGS


class ExpressionTest(VariableTest):
    v = Variable(*VAR1_ARGS)
    v2 = Variable(*VAR2_ARGS)
    v3 = Variable(*VAR3_ARGS)
    expect_add = Expression.from_sympy_expr([v, v2], v.symbol + v2.symbol)
    expect_add_multiple = Expression.from_sympy_expr([v, v2, v3], v.symbol + v2.symbol + v3.symbol)
    expect_subtract = Expression.from_sympy_expr([v, v2], v.symbol - v2.symbol)
    expect_subtract_multiple = Expression.from_sympy_expr([v, v2, v3], v.symbol - v2.symbol - v3.symbol)
    expect_multiply = Expression.from_sympy_expr([v, v2], v.symbol * v2.symbol)
    expect_multiply_multiple = Expression.from_sympy_expr([v, v2, v3], v.symbol * v2.symbol * v3.symbol)
    expect_divide = Expression.from_sympy_expr([v, v2], v.symbol / v2.symbol)
    expect_divide_multiple = Expression.from_sympy_expr([v, v2, v3], v.symbol / v2.symbol / v3.symbol)
    expect_pow = Expression.from_sympy_expr([v, v2], v.symbol ** v2.symbol)
    expect_pow_multiple = Expression.from_sympy_expr([v, v2, v3], v.symbol ** v2.symbol ** v3.symbol)
    add_calc_v = Variable('calc', calculation=expect_add)


class TestCreateFromVariables(ExpressionTest):

    def test_create_from_add(self):
        assert self.v + self.v2 == self.expect_add

    def test_create_from_add_multiple(self):
        assert self.v + self.v2 + self.v3 == self.expect_add_multiple

    def test_create_from_subtract(self):
        assert self.v - self.v2 == self.expect_subtract

    def test_create_from_subtract_multiple(self):
        assert self.v - self.v2 - self.v3 == self.expect_subtract_multiple

    def test_create_from_multiply(self):
        assert self.v * self.v2 == self.expect_multiply

    def test_create_from_multiply_multiple(self):
        assert self.v * self.v2 * self.v3 == self.expect_multiply_multiple

    def test_create_from_divide(self):
        assert self.v / self.v2 == self.expect_divide

    def test_create_from_divide_multiple(self):
        assert self.v / self.v2 / self.v3 == self.expect_divide_multiple

    def test_create_from_pow(self):
        assert self.v ** self.v2 == self.expect_pow

    def test_create_from_pow_multiple(self):
        assert self.v ** self.v2 ** self.v3 == self.expect_pow_multiple

