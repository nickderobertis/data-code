import datetime
from unittest.mock import patch

from datacode import AnalysisOptions
from datacode.models.logic.partial import partial
import datacode.hooks as dc_hooks
from tests.pipeline.base import PipelineTest, analysis_from_source
import tests.test_hooks as th


class TestDataAnalysisPipeline(PipelineTest):

    def test_create_and_run_analysis_pipeline_from_source(self):
        dap = self.create_analysis_pipeline()
        dap.execute()

        assert dap.result.result == self.ds_one_analysis_result
        self.assert_all_pipeline_operations_have_pipeline(dap)

    def test_analysis_pipeline_output(self):
        options = AnalysisOptions(analysis_from_source, out_path=self.csv_path_output)
        dap = self.create_analysis_pipeline(options=options)
        dap.execute()

        with open(self.csv_path_output, 'r') as f:
            result_from_file = int(f.read())

        assert result_from_file == self.ds_one_analysis_result
        self.assert_all_pipeline_operations_have_pipeline(dap)

    def test_create_and_run_analysis_pipeline_from_merge_pipeline(self):
        dmp = self.create_merge_pipeline()
        dap = self.create_analysis_pipeline(source=dmp)
        dap.execute()

        assert dap.result.result == self.ds_one_and_two_analysis_result
        self.assert_ordered_pipeline_operations(dap, [dmp, dap])

    def test_create_and_run_analysis_pipeline_from_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()
        dap = self.create_analysis_pipeline(source=dtp)
        dap.execute()

        assert dap.result.result == self.ds_one_transformed_analysis_result
        self.assert_ordered_pipeline_operations(dap, [dtp, dap])

    def test_create_and_run_analysis_pipeline_from_generation_pipeline(self):
        dgp = self.create_generator_pipeline()
        dap = self.create_analysis_pipeline(source=dgp)
        dap.execute()

        assert dap.result.result == self.ds_one_generated_analysis_result
        self.assert_ordered_pipeline_operations(dap, [dgp, dap])

    def test_create_and_run_multiple_analysis_pipelines_from_same_transformation_pipeline(self):
        dtp = self.create_transformation_pipeline()
        dap1 = self.create_analysis_pipeline(source=dtp)

        analysis_from_source_2 = partial(analysis_from_source, sum_offset=10)
        ao2 = AnalysisOptions(analysis_from_source_2)
        dap2 = self.create_analysis_pipeline(source=dtp, options=ao2)

        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dap1.execute()
        dap2.execute()

        dc_hooks.reset_hooks()

        assert dap1.operations[0] is dap2.operations[0]
        assert dap1.operations[0].data_source is dap2.operations[0].data_source
        assert dap1.result.result == self.ds_one_transformed_analysis_result
        assert dap2.result.result == self.ds_one_transformed_analysis_result_offset_10
        assert th.COUNTER == counter_value + 1  # transform operation called only once
        self.assert_ordered_pipeline_operations(dap1, [dtp, dap1])
        self.assert_ordered_pipeline_operations(dap2, [dtp, dap2])

    def test_create_and_run_multiple_analysis_pipelines_from_same_transformation_pipeline_with_always_rerun(self):
        dtp = self.create_transformation_pipeline(always_rerun=True)
        dap1 = self.create_analysis_pipeline(source=dtp)

        analysis_from_source_2 = partial(analysis_from_source, sum_offset=10)
        ao2 = AnalysisOptions(analysis_from_source_2)
        dap2 = self.create_analysis_pipeline(source=dtp, options=ao2)

        counter_value = th.COUNTER
        dc_hooks.on_begin_apply_source_transform = th.increase_counter_hook_return_only_second_arg

        dap1.execute()
        dap2.execute()

        dc_hooks.reset_hooks()

        assert dap1.operations[0] is dap2.operations[0]
        assert dap1.operations[0].data_source is dap2.operations[0].data_source
        assert dap1.result.result == self.ds_one_transformed_analysis_result
        assert dap2.result.result == self.ds_one_transformed_analysis_result_offset_10
        assert th.COUNTER == counter_value + 2  # transform operation called twice as always rerun
        self.assert_ordered_pipeline_operations(dap1, [dtp, dap1])
        self.assert_ordered_pipeline_operations(dap2, [dtp, dap2])

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
