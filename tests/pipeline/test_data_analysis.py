from tests.pipeline.base import PipelineTest


class TestDataAnalysisPipeline(PipelineTest):

    def test_create_and_run_analysis_pipeline_from_source(self):
        dap = self.create_analysis_pipeline()
        dap.execute()

        assert dap.result.result == self.ds_one_result

    def test_create_and_run_analysis_pipeline_from_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        dap = self.create_analysis_pipeline(source=dmp)
        dap.execute()

        assert dap.result.result == self.ds_one_and_two_result
