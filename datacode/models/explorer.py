import itertools
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
        """
        The total difficulty of all items in the explorer

        :return: Total difficulty
        """
        total = 0
        for item in self:
            item_diff = getattr(item, "difficulty", 0)
            total += item_diff
        return total

    @classmethod
    def get_difficulty_for(cls, items: Sequence[Union[DataSource, DataPipeline]]):
        """
        Get total difficulty for a set of items

        :param items: Items which have difficulty
        :return: Total difficulty
        """
        de = cls(items)
        return de.difficulty

    def difficulty_between(
        self,
        begin: Sequence[Union[DataSource, DataPipeline]],
        end: Sequence[Union[DataSource, DataPipeline]],
    ) -> float:
        """
        Calculates the total difficulty to execute all the passed pipelines and
        sources, starting from begin and ending at end.

        :param begin: Items which are earlier in the pipelines
        :param end: Items which are later in the pipelines
        :return: Total difficulty

        :Notes:

            Items will not be double counted. If a pipeline is on the path
            to run between multiple begin and end items, its difficulty will
            be added only once.

        """
        for item in list(begin) + list(end):
            if item not in self:
                raise ValueError(
                    f"must pass items which are already in DataExplorer, but got {item}"
                )

        # Start from end, working back to beginning in a tree search. Once the path has been
        # found, then work back up to add up the difficulties
        total, found = _work_back_through_all_data_totaling_difficulty_until(end, begin)
        if not found:
            raise ValueError(f"no direct link between the items could be determined")
        return total

    def _graph_contents(
        self,
        include_attrs: Optional[Sequence[str]] = None,
        func_dict: Optional[Dict[str, GraphFunction]] = None,
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


def _work_back_through_all_data_totaling_difficulty_until(
    items: Sequence[Union[DataSource, DataPipeline]],
    end_items: Sequence[Union[DataSource, DataPipeline]],
) -> Tuple[float, bool]:
    any_found = False
    total = 0
    counted_item_ids: List[int] = []
    for item, end in itertools.product(items, end_items):
        sub_total, found = _work_back_through_data_totaling_difficulty_until(
            item, end, counted_item_ids
        )
        if found:
            any_found = True
            total += sub_total
    return total, any_found


def _work_back_through_data_totaling_difficulty_until(
    item: Union[DataSource, DataPipeline],
    end: Union[DataSource, DataPipeline],
    counted_item_ids: List[int],
) -> Tuple[float, bool]:
    if isinstance(item, DataSource):
        return _work_back_through_data_source_totaling_difficulty_until(
            item, end, counted_item_ids
        )
    elif isinstance(item, DataPipeline):
        return _work_back_through_pipeline_totaling_difficulty_until(
            item, end, counted_item_ids
        )
    else:
        raise ValueError(
            f"expected DataSource or DataPipeline, got {item} of type {type(item)}"
        )


def _work_back_through_data_source_totaling_difficulty_until(
    item: DataSource, end: Union[DataSource, DataPipeline], counted_item_ids: List[int]
) -> Tuple[float, bool]:
    total = 0
    if item.pipeline is None:
        # Hit end of this branch but did not find
        return total, False
    if item.pipeline == end:
        # Hit end of this branch and found
        total += item.pipeline.difficulty
        total += item.difficulty
        return total, True
    # There is a pipeline, and it is not the end, so continue recursively
    sub_total, sub_found = _work_back_through_data_totaling_difficulty_until(
        item.pipeline, end, counted_item_ids
    )
    return _aggregate_subtotal(item, total, sub_total, sub_found, counted_item_ids)


def _work_back_through_pipeline_totaling_difficulty_until(
    item: DataPipeline,
    end: Union[DataSource, DataPipeline],
    counted_item_ids: List[int],
) -> Tuple[float, bool]:
    total = 0
    for sub_item in item.data_sources:
        if sub_item == end:
            # Hit the end of this branch and found
            if id(item) not in counted_item_ids:
                # This item was not previously added, so add this item's difficulty
                total += item.difficulty
                counted_item_ids.append(id(item))
            if id(sub_item) not in counted_item_ids:
                # The sub-item was not previously added, so add the sub-item's difficulty
                total += sub_item.difficulty
                counted_item_ids.append(id(sub_item))
            return total, True
        sub_total, sub_found = _work_back_through_data_totaling_difficulty_until(
            sub_item, end, counted_item_ids
        )
        total, _ = _aggregate_subtotal(
            item, total, sub_total, sub_found, counted_item_ids
        )
        if sub_found:
            return total, True
    # Did not find in nested pipeline, this layer is also invalid
    return total, False


def _aggregate_subtotal(
    item: Union[DataSource, DataPipeline],
    total: float,
    sub_total: float,
    sub_found: bool,
    counted_item_ids: List[int],
):
    if sub_found:
        # now we are on the correct path
        total += sub_total
        if id(item) not in counted_item_ids:
            # This item was not previously added, so add this item's difficulty
            total += item.difficulty
            counted_item_ids.append(id(item))
        return total, True

    # Did not find in nested pipeline, this layer is also invalid
    return total, False
