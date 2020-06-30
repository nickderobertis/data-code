from unittest.mock import patch

from datacode import DataExplorer
from tests.pipeline.base import PipelineTest

COUNT = 0


def str_counter() -> str:
    global COUNT
    COUNT += 1
    return str(COUNT)


class ExplorerTest(PipelineTest):

    def teardown_method(self, *args, **kwargs):
        super().teardown_method()
        global COUNT
        COUNT = 0

    def create_explorer(self):
        dp = self.create_merge_pipeline()
        ds = self.create_source()

        data = dict(
            sources=[ds],
            pipelines=[dp]
        )

        explorer = DataExplorer.from_dict(data)
        return explorer


class TestCreateExplorer(ExplorerTest):

    def test_create_from_sources_and_pipelines_dict(self):
        dp = self.create_merge_pipeline()
        ds = self.create_source()

        data = dict(
            sources=[ds],
            pipelines=[dp]
        )

        explorer = DataExplorer.from_dict(data)
        got_ds, got_dp = explorer.items
        assert ds == got_ds
        assert dp == got_dp


class TestExplorerGraph(ExplorerTest):

    @patch('uuid.uuid4', str_counter)
    def test_graph_no_attrs(self):
        explorer = self.create_explorer()
        graph_str = str(explorer.graph())
        assert graph_str.startswith('digraph "Data Explorer" {')
        assert '1 -> 4' in graph_str
        assert '2 -> 4' in graph_str
        assert '5' in graph_str
        assert '2 [label=two]' in graph_str
        assert '1 [label=one]' in graph_str
        assert graph_str.endswith("\n}")

    @patch('uuid.uuid4', str_counter)
    def test_graph_attrs(self):
        explorer = self.create_explorer()
        graph_str = str(explorer.graph(include_attrs=['location', 'operation_options']))
        assert graph_str.startswith('digraph "Data Explorer" {')
        assert '1 -> 4' in graph_str
        assert '2 -> 4' in graph_str
        assert '5 [label="{  | location = tests/generated_files/data.csv }" shape=Mrecord]' in graph_str
        assert '4 [label="{  | operation_options = [\\<DataMerge(on_names=[\'C\'], merge_function=left_merge_df, kwargs=\\{\\})\\>] }" shape=Mrecord]'
        assert '2 [label="{ two | location = tests/generated_files/data2.csv }" shape=Mrecord]' in graph_str
        assert '1 [label="{ one | location = tests/generated_files/data.csv }" shape=Mrecord]' in graph_str
        assert graph_str.endswith("\n}")
