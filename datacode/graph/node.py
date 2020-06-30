import uuid
from typing import Optional

from graphviz import Digraph
from mixins import EqOnAttrsMixin, EqHashMixin

from datacode.graph.base import GraphObject


class Node(GraphObject, EqHashMixin, EqOnAttrsMixin):
    _recursive_hash_convert = True
    equal_attrs = ['label', 'name', 'id', 'node_kwargs']

    def __init__(self, name: str, label: str = 'name', id_: Optional[str] = None, **node_kwargs):
        if label == 'name':
            self.label = name
        elif label is None:
            self.label = ''
        else:
            self.label = label

        self.name = name
        if id_ is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id_
        self.node_kwargs = node_kwargs

    def add_to_graph(self, graph: Digraph):
        graph.node(self.id, label=self.label, **self.node_kwargs)

    def __repr__(self):
        return f'<Node(name={self.name})>'
