from datacode.summarize import format_numbers_to_decimal_places


class TestFormatNumbers:

    def test_format_small_int(self):
        assert format_numbers_to_decimal_places(5) == '5'
        assert format_numbers_to_decimal_places(5.00, coerce_ints=True) == '5'
        assert format_numbers_to_decimal_places(5.00) == '5.00'

    def test_format_millions_int(self):
        assert format_numbers_to_decimal_places(5000000) == '5.00M'
        assert format_numbers_to_decimal_places(5000000, coerce_ints=True) == '5M'

    def test_format_billions_int(self):
        assert format_numbers_to_decimal_places(5000000000) == '5.00B'
        assert format_numbers_to_decimal_places(5000000000, coerce_ints=True) == '5B'

    def test_format_small_float(self):
        assert format_numbers_to_decimal_places(5.6546541321) == '5.65'
        assert format_numbers_to_decimal_places(5431.5546543) == '5,431.6'
        assert format_numbers_to_decimal_places(5431.5546543, reduce_decimals_after_digits=2) == '5,432'
        assert format_numbers_to_decimal_places(5431.5546543, reduce_decimals_after_digits=1) == '5,432'

    def test_format_millions_float(self):
        assert format_numbers_to_decimal_places(500000000.9873513) == '500.00M'
        assert format_numbers_to_decimal_places(500000000.9873513, reduce_decimals_after_digits=2) == '500.0M'
        assert format_numbers_to_decimal_places(500000000.9873513, reduce_decimals_after_digits=1) == '500M'

    def test_format_billions_float(self):
        assert format_numbers_to_decimal_places(500000000000.9873513) == '500.00B'
        assert format_numbers_to_decimal_places(500000000000.9873513, reduce_decimals_after_digits=2) == '500.0B'
        assert format_numbers_to_decimal_places(500000000000.9873513, reduce_decimals_after_digits=1) == '500B'

