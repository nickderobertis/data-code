from graphviz import Digraph


class GraphObject:

    def add_to_graph(self, graph: Digraph):
        raise NotImplementedError