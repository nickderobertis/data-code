from typing import List, Union
from unittest.mock import patch

from datacode import DataExplorer, DataSource
from datacode.models.pipeline.base import DataPipeline
from tests.pipeline.base import PipelineTest

COUNT = 0
EXPECT_DIFFICULTY = 50


def str_counter() -> str:
    global COUNT
    COUNT += 1
    return str(COUNT)


def get_list_if_df_else_get_num(data: Union[DataSource, DataPipeline]) -> Union[int, List[int]]:
    df = data.df
    if df is None:
        return 0
    return [len(df), len(df)]


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

    @patch('uuid.uuid4', str_counter)
    def test_graph_function_dict(self):
        explorer = self.create_explorer()
        func_dict = dict(obs=get_list_if_df_else_get_num)
        graph_str = str(explorer.graph(func_dict=func_dict))
        assert graph_str.startswith('digraph "Data Explorer" {')
        assert '1 -> 4' in graph_str
        assert '2 -> 4' in graph_str
        assert '5 [label="{  | obs_0 = 3 | obs_1 = 3 }" shape=Mrecord]' in graph_str
        assert '4 [label="{  | obs = 0 }" shape=Mrecord]'
        assert '2 [label="{ two | obs_0 = 2 | obs_1 = 2 }" shape=Mrecord]' in graph_str
        assert '1 [label="{ one | obs_0 = 3 | obs_1 = 3 }' in graph_str
        assert graph_str.endswith("\n}")


class TestDifficulty(ExplorerTest):

    def test_get_difficulty_by_property(self):
        explorer = self.create_explorer()
        assert explorer.difficulty == EXPECT_DIFFICULTY

    def test_get_difficulty_by_class_method(self):
        dp = self.create_merge_pipeline()
        ds = self.create_source()

        difficulty = DataExplorer.get_difficulty_for([dp, ds])
        assert difficulty == EXPECT_DIFFICULTY

    def test_get_custom_difficulty(self):
        dp = self.create_merge_pipeline()
        dp.difficulty = 15
        ds = self.create_source()
        ds.difficulty = 5

        difficulty = DataExplorer.get_difficulty_for([dp, ds])
        assert difficulty == 20

    def test_no_overlapping_difficulty(self):
        dp = self.create_merge_pipeline()
        ds = self.create_source()
        da1 = self.create_analysis_pipeline(source=ds)
        da1.name = 'Analysis One'
        da2 = self.create_analysis_pipeline(source=ds)
        da2.name = 'Analysis Two'

        difficulty = DataExplorer.get_difficulty_for([dp, ds, da1, da2])
        assert difficulty == EXPECT_DIFFICULTY + 100

    def test_get_difficulty_between(self):
        ds = self.create_source(name='one', difficulty=10)
        gp = self.create_generator_pipeline()
        gp.name = 'Generate One'
        ds2 = self.create_source(pipeline=gp, name='two', difficulty=20)
        dp = self.create_merge_pipeline(data_sources=[ds, ds2])
        dp.name = 'Merge One Two'
        da1 = self.create_analysis_pipeline(source=dp)
        da1.name = 'Analysis One'
        ds3 = self.create_source(name='three', difficulty=70)
        da2 = self.create_analysis_pipeline(source=ds2)
        da2.name = 'Analysis Two'

        de = DataExplorer([ds, gp, ds2, dp, da1, da2])
        # Single on each side
        difficulty = de.difficulty_between([ds2], [da1])
        assert difficulty == 120
        difficulty = de.difficulty_between([gp], [da1])
        assert difficulty == 170
        difficulty = de.difficulty_between([ds], [dp])
        assert difficulty == 60

        # Multiple begins, single end
        difficulty = de.difficulty_between([ds, ds2], [da1])
        assert difficulty == 130

        # Multiple ends, single begin
        difficulty = de.difficulty_between([ds2], [da1, da2])
        assert difficulty == 170

        # Multiple begins, multiple ends
        difficulty = de.difficulty_between([ds, ds2], [da1, da2])
        assert difficulty == 180

        # Test errors
        with self.assertRaises(ValueError) as cm:
            difficulty = de.difficulty_between([ds3], [da1])
            exc = cm.exception
            assert 'no direct link between the items could be determined' == str(exc)
        ds4 = self.create_source(name='four')
        with self.assertRaises(ValueError) as cm:
            difficulty = de.difficulty_between([ds4], [da1])
            exc = cm.exception
            assert f'must pass items which are already in DataExplorer, but got {ds4}' == str(exc)


class TestRoots(ExplorerTest):

    def test_get_roots(self):
        ds = self.create_source(name='one', difficulty=10)
        gp = self.create_generator_pipeline()
        gp.name = 'Generate One'
        ds2 = self.create_source(pipeline=gp, name='two', difficulty=20)
        dp = self.create_merge_pipeline(data_sources=[ds, ds2])
        dp.name = 'Merge One Two'
        da1 = self.create_analysis_pipeline(source=dp)
        da1.name = 'Analysis One'
        ds3 = self.create_source(name='three', difficulty=70)
        da2 = self.create_analysis_pipeline(source=ds2)
        da2.name = 'Analysis Two'

        de = DataExplorer([ds, gp, ds2, dp, da1, da2])
        roots = de.roots
        assert len(roots) == 2
        assert ds in roots
        assert gp in roots

        de = DataExplorer([ds, ds2, dp, da1, da2])
        roots = de.roots
        assert len(roots) == 2
        assert ds in roots
        assert ds2 in roots

        de = DataExplorer([ds, gp, dp, da1, da2])
        roots = de.roots
        assert len(roots) == 3
        assert ds in roots
        assert gp in roots
        assert da2 in roots

        de = DataExplorer([da1])
        roots = de.roots
        assert len(roots) == 1
        assert da1 in roots
