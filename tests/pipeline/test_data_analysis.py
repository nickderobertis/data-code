from typing import Optional

from datacode import DataAnalysisPipeline, DataSource, FloatType, AnalysisOptions
from datacode.models.types import DataSourceOrPipeline
from tests.pipeline.test_data_merge import DataMergePipelineTest
from tests.test_source import SourceTest


def analysis_from_source(ds: DataSource) -> float:
    running_sum = 0
    for var in ds.load_variables:
        if var.dtype.is_numeric:
            running_sum += ds.df[var.name].sum()
    return running_sum


class DataAnalysisPipelineTest(DataMergePipelineTest):
    ds_one_result = 21
    ds_one_and_two_result = 191

    def create_analysis_pipeline(self, source: Optional[DataSourceOrPipeline] = None,
                                 options: Optional[AnalysisOptions] = None):
        if source is None:
            self.create_csv()
            ds1_cols = self.create_columns()
            source = self.create_source(df=None, columns=ds1_cols, name='one')

        if options is None:
            options = AnalysisOptions(analysis_from_source)

        dap = DataAnalysisPipeline(source, options)
        return dap


class TestDataAnalysisPipeline(DataAnalysisPipelineTest):

    def test_create_and_run_analysis_pipeline_from_source(self):
        dap = self.create_analysis_pipeline()
        dap.execute()

        assert dap.result.result == self.ds_one_result

    def test_create_and_run_analysis_pipeline_from_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        dap = self.create_analysis_pipeline(source=dmp)
        dap.execute()

        assert dap.result.result == self.ds_one_and_two_result
