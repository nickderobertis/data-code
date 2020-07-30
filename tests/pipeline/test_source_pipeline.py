from pandas._testing import assert_frame_equal

from tests.pipeline.base import PipelineTest, EXPECT_GENERATED_DF


class TestCopySource(PipelineTest):

    def test_copy_no_args(self):
        dgp = self.create_generator_pipeline()
        ds = self.create_source(pipeline=dgp, df=None)
        new_ds = ds.copy()
        for attr in ds.copy_keys:
            assert getattr(ds, attr) == getattr(new_ds, attr)
        assert_frame_equal(new_ds.df, EXPECT_GENERATED_DF)
        assert ds.pipeline is new_ds.pipeline is dgp
        assert ds.df is new_ds.df
        assert len(ds.back_links) == 1
        assert len(new_ds.back_links) == 1
        assert len(dgp.forward_links) == 3

    def test_copy_custom_keep(self):
        dgp = self.create_generator_pipeline()
        dgp.name = 'woo'
        ds = self.create_source(pipeline=dgp, df=None)
        new_ds = ds.copy(keep_refs=tuple())
        for attr in ds.copy_keys:
            if attr != 'pipeline':
                assert getattr(ds, attr) == getattr(new_ds, attr)
        assert_frame_equal(new_ds.df, EXPECT_GENERATED_DF)
        assert not ds.pipeline is new_ds.pipeline
        assert not new_ds.pipeline is dgp
        assert ds.pipeline.name == new_ds.pipeline.name == dgp.name
        assert ds.pipeline.operation_options[0].out_path == \
               new_ds.pipeline.operation_options[0].out_path == \
               dgp.operation_options[0].out_path
        assert ds.df is new_ds.df
        assert len(ds.back_links) == 1
        assert len(new_ds.back_links) == 1
        assert len(dgp.forward_links) == 2
