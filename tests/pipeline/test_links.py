import datetime
import time

from pandas.testing import assert_frame_equal

from datacode import DataSource, Column
from tests.pipeline.base import PipelineTest


class TestPipelineLinks(PipelineTest):

    def test_forward_and_back_links(self):
        dgp = self.create_generator_pipeline()
        ds = self.create_source(pipeline=dgp)
        dtp = self.create_transformation_pipeline(source=ds)
        ds2 = self.create_source()
        dmp = self.create_merge_pipeline(data_sources=[dtp, ds2])

        assert ds in dgp.forward_links
        assert dtp in ds.forward_links
        assert dmp in dtp.forward_links
        assert dmp in ds2.forward_links

        assert dgp in ds.back_links
        assert ds in dtp.back_links
        assert dtp in dmp.back_links
        assert ds2 in dmp.back_links

        dtp.data_sources = []
        del ds
        assert len(dgp.forward_links) == 0
        assert len(dtp.back_links) == 0

        dmp.data_sources = []
        del dtp
        assert len(dmp.back_links) == 1
