from datacode import StringType, FloatType, IntType, BooleanType, TimedeltaType, DatetimeType, ObjectType


def test_dtype_eq():
    assert StringType() == StringType()


def test_is_numeric():
    assert FloatType().is_numeric == IntType().is_numeric == True
    assert StringType().is_numeric == BooleanType().is_numeric == TimedeltaType().is_numeric == \
           DatetimeType().is_numeric == ObjectType().is_numeric == False
