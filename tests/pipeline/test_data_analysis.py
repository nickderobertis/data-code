from datacode import DataAnalysisPipeline, DataSource, FloatType
from tests.test_source import SourceTest


def analysis_from_source(ds: DataSource) -> float:
    running_sum = 0
    for var in ds.load_variables:
        if var.dtype.is_numeric:
            running_sum += ds.df[var.name].sum()
    return running_sum


class DataAnalysisPipelineTest(SourceTest):

    def create_analysis_pipeline(self):
        self.create_csv()
        ds1_cols = self.create_columns()
        ds1 = self.create_source(df=None, columns=ds1_cols, name='one')

        dap = DataAnalysisPipeline(ds1, analysis_from_source)
        return dap


class TestDataAnalysisPipeline(DataAnalysisPipelineTest):

    def test_create_and_run_merge_pipeline_from_source(self):
        dp = self.create_analysis_pipeline()
        dp.execute()

        assert dp.result == 21
