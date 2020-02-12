from datacode import StringType


def test_dtype_eq():
    assert StringType() == StringType()
