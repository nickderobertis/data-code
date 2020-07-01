from typing import Sequence, Union, List, Optional

from mixins import ReprMixin

from datacode.models.source import DataSource
from datacode.models.pipeline.base import DataPipeline
from datacode.graph.base import GraphObject, Graphable


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

    def _graph_contents(
        self, include_attrs: Optional[Sequence[str]] = None
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
            all_contents.extend(item._graph_contents(include_attrs))
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
