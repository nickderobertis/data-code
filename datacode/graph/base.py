import uuid
from typing import Callable, Dict, List, Sequence, Optional, TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from datacode.graph.node import Node
    from datacode.models.source import DataSource
    from datacode.models.pipeline.base import DataPipeline

from graphviz import Digraph

GraphFunction = Callable[[Union['DataSource', 'DataPipeline']], Any]

ESCAPE_CHARS = ['<', '>', '{', '}', '|']


class GraphObject:

    def add_to_graph(self, graph: Digraph):
        raise NotImplementedError


class Graphable:
    name: str

    def __init__(self):
        self._node_id = str(uuid.uuid4())

    def _graph_contents(self, include_attrs: Optional[Sequence[str]] = None,
                        func_dict: Optional[Dict[str, GraphFunction]] = None) -> List[GraphObject]:
        raise NotImplementedError

    def graph(self, include_attrs: Optional[Sequence[str]] = None,
              func_dict: Optional[Dict[str, GraphFunction]] = None) -> Digraph:
        elems = self._graph_contents(include_attrs, func_dict)
        graph = Digraph(self.name)
        for elem in elems:
            elem.add_to_graph(graph)
        return graph

    def primary_node(self, include_attrs: Optional[Sequence[str]] = None,
                     func_dict: Optional[Dict[str, GraphFunction]] = None) -> 'Node':
        from datacode.graph.node import Node

        label_parts = [self.name if self.name is not None else '']
        label_parts.extend(self._include_attrs_labels(include_attrs))
        label_parts.extend(self._function_dict_labels(func_dict))
        label = get_multirow_label_from_parts(label_parts)
        if len(label_parts) == 1:
            # Did not find any included attributes
            return Node(self.name, id_=self._node_id)

        # Has valid included attributes
        return Node(label, shape='Mrecord', id_=self._node_id)

    def _include_attrs_labels(self, include_attrs: Optional[Sequence[str]] = None) -> List[str]:
        if include_attrs is None:
            return []

        label_parts = []
        for attr in include_attrs:
            if hasattr(self, attr):
                value = getattr(self, attr)
                str_value = get_valid_label_part(str(value))
                value_label = f'{attr} = {str_value}'
                label_parts.append(value_label)
        return label_parts

    def _function_dict_labels(self, func_dict: Optional[Dict[str, GraphFunction]] = None) -> List[str]:
        if func_dict is None:
            return []

        label_parts = []
        for attr_name, func in func_dict.items():
            label_parts.extend(self._function_labels(func, attr_name))
        return label_parts

    def _function_labels(self, func: Optional[GraphFunction] = None, attr_name: Optional[str] = None) -> List[str]:
        if func is None:
            return []

        if attr_name is None:
            attr_name = func.__name__

        orig_parts = func(self)  # type: ignore

        if not isinstance(orig_parts, (list, tuple)):
            return [f'{attr_name} = {get_valid_label_part(str(orig_parts))}']

        # Handle multiple returned items
        label_parts: List[str] = []
        for i, value in enumerate(orig_parts):
            str_value = get_valid_label_part(str(value))
            value_label = f'{attr_name}_{i} = {str_value}'
            label_parts.append(value_label)
        return label_parts


def get_valid_label_part(label: str) -> str:
    for replace_char in ESCAPE_CHARS:
        label = label.replace(replace_char, '\\' + replace_char)
    return label


def get_multirow_label_from_parts(parts: Sequence[str]) -> str:
    label = ' | '.join(parts)
    label = '{ ' + label + ' }'
    return label
