import datetime
import json
import os
from copy import deepcopy
from typing import Union, Dict
from unittest.mock import patch

from datacode import DataPipeline, DataSource, Variable
from tests.pipeline.base import PipelineTest
from tests.utils import INPUT_FILES_PATH

GENERATED_HASH_DIR = os.path.join(INPUT_FILES_PATH, 'hashes')
SHOULD_GENERATE = os.environ.get('DATACODE_GENERATE_HASH_TESTS', False) == 'true'


def check_or_store_hash_dict(obj_with_hd: Union[DataSource, DataPipeline], obj_name: str, pre_execute: bool = False):
    if pre_execute:
        hd = obj_with_hd._pre_execute_hash_dict
    else:
        hd = obj_with_hd.hash_dict()

    if SHOULD_GENERATE:
        store_hash_dict(hd, obj_name)
    else:
        check_hash_dict(hd, obj_name)


def check_hash_dict(hd: Dict[str, str], obj_name: str):
    static_path = os.path.join(GENERATED_HASH_DIR, f'{obj_name}.json')
    with open(static_path, 'r') as f:
        expect_hash = json.load(f)
    assert hd == expect_hash


def store_hash_dict(hd: Dict[str, str], obj_name: str):
    static_path = os.path.join(GENERATED_HASH_DIR, f'{obj_name}.json')
    with open(static_path, 'w') as f:
        json.dump(hd, f, indent=2)
    return


class HashTest(PipelineTest):
    pass


class TestSourceHash(HashTest):
    
    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source(self):
        self.create_csv()
        ds = self.create_source()
        check_or_store_hash_dict(ds, 'source')
        df = ds.df
        check_or_store_hash_dict(ds, 'source')

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source_with_calculated_variable(self):
        self.create_csv()
        all_cols = self.create_columns()
        a, b, c = self.create_variables()
        d = Variable('d', 'D', calculation=a + b)
        ds = self.create_source(df=None, columns=all_cols, load_variables=[a, b, c, d])
        dtp = self.create_transformation_pipeline(source=ds, func=lambda source: source)
        check_or_store_hash_dict(dtp, 'transform_source_with_calculated')
        dtp.execute()
        check_or_store_hash_dict(dtp, 'transform_source_with_calculated', pre_execute=True)

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source_with_repeated_variables_different_transforms(self):
        self.create_csv()
        all_cols = self.create_columns(transform_data='cell', apply_transforms=False)
        a, b, c = self.create_variables(transform_data='cell', apply_transforms=False)

        # First with original variable first, then transformation
        load_variables = [
            a,
            a.add_one_cell(),
            b,
            c,
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_variables)
        dtp = self.create_transformation_pipeline(source=ds, func=lambda source: source)
        check_or_store_hash_dict(dtp, 'transform_source_with_repeated_variables_different_transforms')
        dtp.execute()
        check_or_store_hash_dict(dtp, 'transform_source_with_repeated_variables_different_transforms', pre_execute=True)

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source_with_calculated_and_same_calculated_variable_transformed(self):
        self.create_csv()

        # Try with plain calculated variable first
        all_cols = self.create_columns()
        a, b, c = self.create_variables()
        tran = self.get_transform('cell')
        d = Variable('d', 'D', calculation=a + b, available_transforms=[tran])
        load_vars = [
            a,
            b,
            c,
            d,
            d.add_one_cell()
        ]
        ds = self.create_source(df=None, columns=all_cols, load_variables=load_vars)
        dtp = self.create_transformation_pipeline(source=ds, func=lambda source: source)
        check_or_store_hash_dict(dtp, 'transform_source_with_calculated_and_calculated_transformed')
        dtp.execute()
        check_or_store_hash_dict(dtp, 'transform_source_with_calculated_and_calculated_transformed', pre_execute=True)

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source_with_calculate_on_transformed_before_and_after_transform(self):
        self.create_csv()
        all_cols = self.create_columns()
        a, b, c = self.create_variables(transform_data='cell', apply_transforms=False)
        d = Variable('d', 'D', calculation=a + b.add_one_cell())
        ds = self.create_source(df=None, columns=all_cols, load_variables=[a.add_one_cell(), b.add_one_cell(), c, d])
        dtp = self.create_transformation_pipeline(source=ds, func=lambda source: source)
        check_or_store_hash_dict(dtp, 'transform_source_with_calculate_on_transformed_before_after')
        dtp.execute()
        check_or_store_hash_dict(dtp, 'transform_source_with_calculate_on_transformed_before_after', pre_execute=True)

class TestPipelineHash(HashTest):
    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_analysis_pipeline(self):
        dap = self.create_analysis_pipeline()
        check_or_store_hash_dict(dap, 'analysis')
        dap.execute()
        check_or_store_hash_dict(dap, 'analysis')

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_combine_pipeline(self):
        dcp = self.create_combine_pipeline()
        check_or_store_hash_dict(dcp, 'combine')
        dcp.execute()
        check_or_store_hash_dict(dcp, 'combine')

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_generator_pipeline(self):
        dgp = self.create_generator_pipeline()
        check_or_store_hash_dict(dgp, 'generator')
        dgp.execute()
        check_or_store_hash_dict(dgp, 'generator')

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        check_or_store_hash_dict(dmp, 'merge')
        dmp.execute()
        check_or_store_hash_dict(dmp, 'merge')

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()
        check_or_store_hash_dict(dtp, 'transform')
        dtp.execute()
        check_or_store_hash_dict(dtp, 'transform')
