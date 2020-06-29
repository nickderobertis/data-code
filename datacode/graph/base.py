from typing import List

from graphviz import Digraph


class GraphObject:

    def add_to_graph(self, graph: Digraph):
        raise NotImplementedError


class Graphable:
    name: str

    @property
    def _graph_contents(self) -> List[GraphObject]:
        raise NotImplementedError

    @property
    def graph(self) -> Digraph:
        elems = self._graph_contents
        graph = Digraph(self.name)
        for elem in elems:
            elem.add_to_graph(graph)
        return graph