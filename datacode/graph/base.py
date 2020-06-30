import uuid
from typing import List, Sequence, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from datacode.graph.node import Node

from graphviz import Digraph

ESCAPE_CHARS = ['<', '>', '{', '}', '|']


class GraphObject:

    def add_to_graph(self, graph: Digraph):
        raise NotImplementedError


class Graphable:
    name: str

    def __init__(self):
        self._node_id = str(uuid.uuid4())

    def _graph_contents(self, include_attrs: Optional[Sequence[str]] = None) -> List[GraphObject]:
        raise NotImplementedError

    def graph(self, include_attrs: Optional[Sequence[str]] = None) -> Digraph:
        elems = self._graph_contents(include_attrs)
        graph = Digraph(self.name)
        for elem in elems:
            elem.add_to_graph(graph)
        return graph

    def primary_node(self, include_attrs: Optional[Sequence[str]] = None) -> 'Node':
        from datacode.graph.node import Node

        if include_attrs is None:
            return Node(self.name, id_=self._node_id)

        label_parts = [self.name if self.name is not None else '']
        for attr in include_attrs:
            if hasattr(self, attr):
                value = getattr(self, attr)
                str_value = str(value)
                for replace_char in ESCAPE_CHARS:
                    str_value = str_value.replace(replace_char, '\\' + replace_char)
                value_label = f'{attr} = {str_value}'
                label_parts.append(value_label)
        label = ' | '.join(label_parts)
        label = '{ ' + label + ' }'
        if len(label_parts) == 1:
            # Did not find any included attributes
            return Node(self.name, id_=self._node_id)

        # Has valid included attributes
        return Node(label, shape='Mrecord', id_=self._node_id)