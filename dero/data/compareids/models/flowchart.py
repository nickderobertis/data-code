import graphviz
import uuid

class Node:

    def __init__(self, name, label='name', **node_kwargs):
        if label == 'name':
            self.label = name
        elif label is None:
            self.label = ''
        else:
            self.label = label

        self.name = name
        self.id = str(uuid.uuid1())
        self.node_kwargs = node_kwargs

    def add_to_graph(self, graph):
        graph.node(self.id, label=self.label, **self.node_kwargs)

    def __repr__(self):
        return f'<Node(name={self.name})>'

class NodeCollection:

    def __init__(self, *nodes):
        self.nodes = nodes

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.nodes[item]
        elif isinstance(item, str):
            return [node for node in self.nodes if node.name in (item, item + 'DUMMY')][0]

    def __repr__(self):
        return f'<NodeCollection(nodes={self.nodes})>'


class Subgraph:

    def __init__(self, nodes: [Node], name=None, digraph=True, **graph_kwargs):
        self.nodes = NodeCollection(*nodes)
        self.name = name
        self.digraph = digraph
        self.graph_kwargs = graph_kwargs

    def add_to_graph(self, graph):
        full_name = 'cluster' + (self.name if self.name else '')
        Graph = _get_digraph_if_true_else_graph(self.digraph)
        subgraph = Graph(full_name, **self.graph_kwargs)
        subgraph.attr('graph', label=self.name)
        [node.add_to_graph(subgraph) for node in self.nodes]
        graph.subgraph(subgraph)

    def __repr__(self):
        return f'<Subgraph(name={self.name}, nodes={self.nodes})>'

class Edge:

    def __init__(self, start: Node, end: Node, for_subgraphs: [None, (Subgraph,)] = None, **edge_kwargs):
        self.start = start
        self.end = end
        if for_subgraphs is not None:
            edge_kwargs.update({'ltail': 'cluster' + for_subgraphs[0].name, 'lhead': 'cluster' + for_subgraphs[1].name})
        self.edge_kwargs = edge_kwargs

    def add_to_graph(self, graph):
        graph.edge(self.start.id, self.end.id, **self.edge_kwargs)

    def __repr__(self):
        return f'<Edge(start={self.start}, end={self.end}, edge_kwargs: {self.edge_kwargs})>'


def _get_digraph_if_true_else_graph(boolean):
    if boolean:
        return graphviz.Digraph
    else:
        return graphviz.Graph
