import uuid

from graphviz import Digraph
from mixins import EqOnAttrsMixin, EqHashMixin

from datacode.graph.base import GraphObject


class Node(GraphObject, EqHashMixin, EqOnAttrsMixin):
    _recursive_hash_convert = True
    equal_attrs = ['label', 'name', 'id', 'node_kwargs']

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

    def add_to_graph(self, graph: Digraph):
        graph.node(self.id, label=self.label, **self.node_kwargs)

    def __repr__(self):
        return f'<Node(name={self.name})>'
