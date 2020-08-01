import datetime
from unittest.mock import patch

from tests.pipeline.base import PipelineTest


class HashTest(PipelineTest):
    pass


class TestSourceHash(HashTest):
    
    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_source(self):
        self.create_csv()
        ds = self.create_source()
        hd1 = ds.hash_dict()
        df = ds.df
        hd2 = ds.hash_dict()
        assert hd1 == hd2 == {
            "location": "75553e5c242e7858a243efffe3279b3f30cc94cbba887e660b574552a86f0506",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "tags": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "loader_class": "cb732c35e5222bdd8e353abeee43b3426247a69981c0f003310ce1279e7c2a08",
            "outputter_class": "fa25e85aaf795e38115c41cbeaae334ffba50917980fffedcf823c0585ada928",
            "_pipeline": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "_orig_columns": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "_columns_for_calculate": "e78b481f6a5083ac6d266e434fd0da3dc14bf48ac1376d0476b6e310c721e6d9",
            "columns": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "_orig_load_variables": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "_vars_for_calculate": "e78b481f6a5083ac6d266e434fd0da3dc14bf48ac1376d0476b6e310c721e6d9",
            "load_variables": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "read_file_kwargs": "306f6a85c8136a673f6eac5fffe265a196613180ebdfe2b9e6fdd6fdd62bb8fd",
            "data_outputter_kwargs": "306f6a85c8136a673f6eac5fffe265a196613180ebdfe2b9e6fdd6fdd62bb8fd",
            "optimize_size": "b195620d3676be89da6277412918e9f4e5e2bf23b0eaacfcf674c87609c67f3a",
            "difficulty": "0721514aeeb11d91796d9f3769a20fde80566ddf03ce7f00832632e766b09596",
            "last_modified": "caec90dd700c1651c357c7111c1aa3236603817e15d5716b1ecd0dc912deb421",
        }

class TestPipelineHash(HashTest):
    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_analysis_pipeline(self):
        dap = self.create_analysis_pipeline()
        hd1 = dap.hash_dict()
        dap.execute()
        hd2 = dap.hash_dict()
        assert hd1 == hd2 == {
            "_data_sources": "b723a0cb0aa8c106d7857484846835a8279049425c474f6f06ad07dcb521b1b5",
            "_operations_options": "fc88ae22bf3ca43b567a82443e1ea75a8799fe2be195794c857f98b516bfb655",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "difficulty": "f71d3c329180a20f409d73572d25e0975ae38db1230fd18c59671532b2f9fcda",
        }

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_combine_pipeline(self):
        dcp = self.create_combine_pipeline()
        hd1 = dcp.hash_dict()
        dcp.execute()
        hd2 = dcp.hash_dict()
        assert hd1 == hd2 == {
            "_data_sources": "f057dc9c4b6f3d664c120969bca6b094db9539b397ac32ac0cc62ce042e96fb6",
            "_operations_options": "9705db1f1fa78dac3c74d509a39ce608063a7a334795b7cb1b9a010c117a2a3c",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "difficulty": "f71d3c329180a20f409d73572d25e0975ae38db1230fd18c59671532b2f9fcda",
        }

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_generator_pipeline(self):
        dgp = self.create_generator_pipeline()
        hd1 = dgp.hash_dict()
        dgp.execute()
        hd2 = dgp.hash_dict()
        assert hd1 == hd2 == {
            "_data_sources": "e78b481f6a5083ac6d266e434fd0da3dc14bf48ac1376d0476b6e310c721e6d9",
            "_operations_options": "8616241cb51ef1210d920715625b671172688a9cf2cf5e37e55344ff2a5ccbb4",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "difficulty": "f71d3c329180a20f409d73572d25e0975ae38db1230fd18c59671532b2f9fcda",
        }

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        hd1 = dmp.hash_dict()
        dmp.execute()
        hd2 = dmp.hash_dict()
        assert hd1 == hd2 == {
            "has_post_merge_cleanup_func": "b195620d3676be89da6277412918e9f4e5e2bf23b0eaacfcf674c87609c67f3a",
            "cleanup_kwargs": "306f6a85c8136a673f6eac5fffe265a196613180ebdfe2b9e6fdd6fdd62bb8fd",
            "_data_sources": "f057dc9c4b6f3d664c120969bca6b094db9539b397ac32ac0cc62ce042e96fb6",
            "_operations_options": "08aaebf91e162866cccee6b5ab2bae1d21595ad5da53f1dfc465e407f792cf58",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "difficulty": "f71d3c329180a20f409d73572d25e0975ae38db1230fd18c59671532b2f9fcda",
        }

    @patch('datacode.models.source.DataSource.last_modified', datetime.datetime(2020, 7, 29))
    def test_hash_dict_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()
        hd1 = dtp.hash_dict()
        dtp.execute()
        hd2 = dtp.hash_dict()
        assert hd1 == hd2 == {
            "_data_sources": "4a7dfc739fcb55ed75da70d8102acd0a02b7b70bad64d6004d5b31a69d889458",
            "_operations_options": "28ba857cd5a6a95bcdc5d2a94b0c13839ed0b3c9de2b982cbb8894653d5c41c0",
            "name": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
            "difficulty": "f71d3c329180a20f409d73572d25e0975ae38db1230fd18c59671532b2f9fcda",
        }