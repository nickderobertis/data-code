import datetime
import unittest
from copy import deepcopy
from unittest.mock import patch

import datacode as dc
from datacode import DataSource
from tests.test_hash import check_hash_dict
from tests.test_source import SourceTest

ORIG_COPY_KEYS = deepcopy(DataSource.copy_keys)


class OptionsTest(SourceTest):
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

    def test_set_non_existing_class_attr_and_reset(self):
        new_value = ["a", "b"]
        assert not hasattr(DataSource, '_something_unused')
        dc.options.set_class_attr("DataSource", "_something_unused", new_value)
        assert DataSource._something_unused == new_value
        dc.options.reset()
        assert not hasattr(DataSource, '_something_unused')

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

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_set_hash_options(self):
        new_value = dict(exclude_types=[object], ignore_type_subclasses=True)
        self.create_csv()
        ds = self.create_source()
        hd = ds.hash_dict()
        check_hash_dict(hd, 'source')
        with dc.options.set_hash_options(new_value):
            hd = ds.hash_dict()
            assert hd == {}
        hd = ds.hash_dict()
        check_hash_dict(hd, 'source')
