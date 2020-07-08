import unittest
from copy import deepcopy

import datacode as dc
from datacode import DataSource

ORIG_COPY_KEYS = deepcopy(DataSource.copy_keys)


class OptionsTest(unittest.TestCase):
    def tearDown(self) -> None:
        dc.options.reset()


class TestOptions(OptionsTest):
    def test_set_class_attr_and_reset(self):
        new_value = ["a", "b"]
        assert DataSource.copy_keys == ORIG_COPY_KEYS
        dc.options.set_class_attr("DataSource", "copy_keys", new_value)
        assert DataSource.copy_keys == new_value
        dc.options.reset()
        assert DataSource.copy_keys == ORIG_COPY_KEYS

    def test_set_class_attr_context_manager(self):
        new_value = ["a", "b"]
        assert DataSource.copy_keys == ORIG_COPY_KEYS
        with dc.options:
            dc.options.set_class_attr("DataSource", "copy_keys", new_value)
            assert DataSource.copy_keys == new_value
        assert DataSource.copy_keys == ORIG_COPY_KEYS
        with dc.options.set_class_attr("DataSource", "copy_keys", new_value):
            assert DataSource.copy_keys == new_value
        assert DataSource.copy_keys == ORIG_COPY_KEYS
