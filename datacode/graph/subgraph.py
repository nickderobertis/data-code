from typing import Sequence

from graphviz import Digraph

from datacode.graph.base import GraphObject


class Subgraph(GraphObject):

    def __init__(self, elems: Sequence[GraphObject], name=None, digraph=True, **graph_kwargs):
        self.elems = elems
        self.name = name
        self.digraph = digraph
        self.graph_kwargs = graph_kwargs

    def add_to_graph(self, graph: Digraph):
        full_name = 'cluster' + (self.name if self.name else '')
        subgraph = Digraph(full_name, **self.graph_kwargs)
        subgraph.attr('graph', label=self.name)
        [elem.add_to_graph(subgraph) for elem in self.elems]
        graph.subgraph(subgraph)

    def __repr__(self):
        return f'<Subgraph(name={self.name}, elems={self.elems})>'