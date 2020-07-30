import datetime
from unittest.mock import patch

from pandas.testing import assert_frame_equal

from datacode import DataSource
from tests.pipeline.base import PipelineTest, EXPECT_GENERATED_DF


class TestDataGeneratorPipeline(PipelineTest):

    def test_create_and_run_generator_pipeline_from_func(self):
        dgp = self.create_generator_pipeline()
        dgp.execute()

        assert_frame_equal(dgp.df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)

    def test_auto_run_pipeline_by_load_source_with_no_location(self):
        dgp = self.create_generator_pipeline()

        ds = DataSource(pipeline=dgp, location=self.csv_path_output)
        df = ds.df
        assert_frame_equal(df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)

    def test_auto_run_pipeline_by_load_source_with_newer_pipeline(self):
        now = datetime.datetime.now()
        later = now + datetime.timedelta(minutes=5)
        dgp = self.create_generator_pipeline(last_modified=later)

        self.create_csv()
        ds = self.create_source(pipeline=dgp, df=None)
        df = ds.df
        assert dgp._operation_index == 1
        assert_frame_equal(df, EXPECT_GENERATED_DF)
        self.assert_all_pipeline_operations_have_pipeline(dgp)

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
            "last_modified": "bbd393a60007e5f9621b8fde442dbcf493227ef7ced9708aa743b46a88e1b49e",
        }
