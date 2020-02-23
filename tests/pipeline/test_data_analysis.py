from datacode import AnalysisOptions
from tests.pipeline.base import PipelineTest, analysis_from_source


class TestDataAnalysisPipeline(PipelineTest):

    def test_create_and_run_analysis_pipeline_from_source(self):
        dap = self.create_analysis_pipeline()
        dap.execute()

        assert dap.result.result == self.ds_one_analysis_result

    def test_analysis_pipeline_output(self):
        options = AnalysisOptions(analysis_from_source, out_path=self.csv_path_output)
        dap = self.create_analysis_pipeline(options=options)
        dap.execute()

        with open(self.csv_path_output, 'r') as f:
            result_from_file = int(f.read())

        assert result_from_file == self.ds_one_analysis_result

    def test_create_and_run_analysis_pipeline_from_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        dap = self.create_analysis_pipeline(source=dmp)
        dap.execute()

        assert dap.result.result == self.ds_one_and_two_analysis_result

    def test_create_and_run_analysis_pipeline_from_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()
        dap = self.create_analysis_pipeline(source=dtp)
        dap.execute()

        assert dap.result.result == self.ds_one_transformed_analysis_result

    def test_create_and_run_analysis_pipeline_from_generation_pipeline(self):
        dgp = self.create_generator_pipeline()
        dap = self.create_analysis_pipeline(source=dgp)
        dap.execute()

        assert dap.result.result == self.ds_one_generated_analysis_result
