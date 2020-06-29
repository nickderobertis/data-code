from typing import Tuple, Optional

from graphviz import Digraph
from mixins import EqOnAttrsMixin, EqHashMixin

from datacode.graph.base import GraphObject
from datacode.graph.node import Node
from datacode.graph.subgraph import Subgraph


class Edge(GraphObject, EqHashMixin, EqOnAttrsMixin):
    _recursive_hash_convert = True
    equal_attrs = [
        'start',
        'end',
        'edge_kwargs',
    ]

    def __init__(self, start: Node, end: Node, for_subgraphs: Optional[Tuple[Subgraph, Subgraph]] = None, **edge_kwargs):
        self.start = start
        self.end = end
        if for_subgraphs is not None:
            edge_kwargs.update({'ltail': 'cluster' + for_subgraphs[0].name, 'lhead': 'cluster' + for_subgraphs[1].name})
        self.edge_kwargs = edge_kwargs

    def add_to_graph(self, graph: Digraph):
        graph.edge(self.start.id, self.end.id, **self.edge_kwargs)

    def __repr__(self):
        return f'<Edge(start={self.start}, end={self.end}, edge_kwargs: {self.edge_kwargs})>'
