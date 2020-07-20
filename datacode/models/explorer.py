from typing import Sequence, Union, List, Optional, Dict, Tuple

from mixins import ReprMixin

from datacode.models.source import DataSource
from datacode.models.pipeline.base import DataPipeline
from datacode.graph.base import GraphObject, Graphable, GraphFunction


class DataExplorer(Graphable, ReprMixin):
    """
    Pass it DataSources and DataPipelines to explore and inspect them
    """
    repr_cols = ["item_names"]

    def __init__(
        self,
        items: Sequence[Union[DataSource, DataPipeline]],
        name: str = "Data Explorer",
    ):
        self.items = items
        self.name = name
        super().__init__()

    @classmethod
    def from_dict(cls, data: dict) -> "DataExplorer":
        items = _get_list_of_items_from_nested_dict(data)
        return cls(items)

    @property
    def item_names(self) -> List[str]:
        return [item.name for item in self.items]

    def __getitem__(self, item):
        return self.items[item]

    @property
    def difficulty(self) -> float:
        total = 0
        for item in self:
            item_diff = getattr(item, 'difficulty', 0)
            total += item_diff
        return total

    @classmethod
    def get_difficulty_for(cls, items: Sequence[Union[DataSource, DataPipeline]]):
        de = cls(items)
        return de.difficulty

    def difficulty_between(self, begin: Union[DataSource, DataPipeline], end: Union[DataSource, DataPipeline]) -> float:
        if begin not in self:
            raise ValueError(f'must pass items which are already in DataExplorer, but got {begin}')
        if end not in self:
            raise ValueError(f'must pass items which are already in DataExplorer, but got {end}')

        # Start from end, working back to beginning in a tree search. Once the path has been
        # found, then work back up to add up the difficulties
        total, found = _work_back_through_pipelines_totaling_difficulty_until(end, begin)
        if not found:
            raise ValueError(f'no direct link between the items could be determined')
        return total


    def _graph_contents(
        self, include_attrs: Optional[Sequence[str]] = None,
        func_dict: Optional[Dict[str, GraphFunction]] = None
    ) -> List[GraphObject]:
        # TODO [#94]: more efficient DataExplorer.graph
        #
        # Examining last_modified or pipeline_last_modified on
        # a large pipeline structure is extremely slow. Performance
        # of DataExplorer graphing could be improved if it first found
        # only the terminal pipelines and sources and used only those,
        # as the nested is included anyway.
        all_contents = []
        for item in self.items:
            all_contents.extend(item._graph_contents(include_attrs, func_dict))
        all_contents = list(set(all_contents))
        return all_contents


def _get_list_of_items_from_nested_dict(data: dict):
    all_items = []
    for key, value in data.items():
        if isinstance(value, dict):
            nested_items = _get_list_of_items_from_nested_dict(value)
            all_items.extend(nested_items)
        elif isinstance(value, (list, tuple)):
            all_items.extend(value)
        else:
            all_items.append(value)
    return all_items


def _work_back_through_pipelines_totaling_difficulty_until(item: Union[DataSource, DataPipeline],
                                                           end: Union[DataSource, DataPipeline]) -> Tuple[float, bool]:
    total = 0
    if isinstance(item, DataSource):
        if item.pipeline is None:
            # Hit end of this branch but did not find
            return total, False
        if item.pipeline == end:
            # Hit end of this branch and found
            total += item.pipeline.difficulty
            total += item.difficulty
            return total, True
        # There is a pipeline, and it is not the end, so continue recursively
        sub_total, sub_found = _work_back_through_pipelines_totaling_difficulty_until(item.pipeline, end)
        if sub_found:
            total += item.difficulty  # now we are on the correct path, so add this item
            total += sub_total
            return total, True
        # Did not find in nested pipeline, this layer is also invalid
        return total, False
    elif isinstance(item, DataPipeline):
        for sub_item in item.data_sources:
            if sub_item == end:
                # Hit the end of this branch and found
                total += item.difficulty
                total += sub_item.difficulty
                return total, True
            sub_total, sub_found = _work_back_through_pipelines_totaling_difficulty_until(sub_item, end)
            if sub_found:
                total += item.difficulty  # now we are on the correct path, so add this item
                total += sub_total
                return total, True
        # Did not find in nested pipeline, this layer is also invalid
        return total, False
    else:
        raise ValueError(f'expected DataSource or DataPipeline, got {item} of type {type(item)}')
