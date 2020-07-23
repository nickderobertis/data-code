import itertools
from typing import Sequence, Union, List, Optional, Dict, Tuple, cast

from mixins import ReprMixin
from typing_extensions import Protocol, runtime_checkable

from datacode.models.links import ILinkedItem
from datacode.models.source import DataSource
from datacode.models.pipeline.base import DataPipeline
from datacode.graph.base import GraphObject, Graphable, GraphFunction
from datacode.models.types import HasDifficulty, HasDataSources, HasPipeline


@runtime_checkable
class HasDifficultyAndOrigin(HasDifficulty, ILinkedItem, Protocol):
    pass


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
        begin: Sequence[HasDifficultyAndOrigin],
        end: Sequence[HasDifficultyAndOrigin],
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

    @property
    def roots(self) -> List[Union[DataSource, DataPipeline]]:
        """
        The items passed to the explorer which do not have any
        prior items also in the explorer
        :return: root items
        """
        return _work_back_through_all_data_finding_roots(self.items)

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
    items: Sequence[HasDifficultyAndOrigin],
    end_items: Sequence[HasDifficultyAndOrigin],
) -> Tuple[float, bool]:
    any_found = False
    total: float = 0
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
    item: HasDifficultyAndOrigin,
    end: HasDifficultyAndOrigin,
    counted_item_ids: List[int],
) -> Tuple[float, bool]:
    total: float = 0
    for sub_item in item.back_links:
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
    item: HasDifficultyAndOrigin,
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


def _work_back_through_all_data_finding_roots(
    items: Sequence[HasDifficultyAndOrigin],
) -> List[Union[DataSource, DataPipeline]]:
    roots: List[Union[DataSource, DataPipeline]] = []
    found_item_ids: List[int] = []
    for item in items:
        roots.extend(_work_back_through_data_finding_roots(item, items, found_item_ids))
    return roots


def _work_back_through_data_finding_roots(
    item: HasDifficultyAndOrigin, items: Sequence[HasDifficultyAndOrigin], found_item_ids: List[int],
) -> List[Union[DataSource, DataPipeline]]:
    collected_items = []
    has_valid_back_link = False
    for sub_item in item.back_links:
        if sub_item in items:
            # We are still navigating down the tree, call recursively
            has_valid_back_link = True
            collected_items.extend(_work_back_through_data_finding_roots(sub_item, items, found_item_ids))
    if not has_valid_back_link:
        # Hit the end of this branch
        if id(item) not in found_item_ids:
            # This item was not previously added, so add this item
            found_item_ids.append(id(item))
            collected_items.append(item)
    return collected_items