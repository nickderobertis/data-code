import pandas as pd
from .flowchart import Node, Subgraph
from .interface import PipelineOptions

class DataSource:

    def __init__(self, name=None, filepath=None, data_loader=None, **data_loader_kwargs):
        self.name = name
        self.filepath = filepath
        self._df = None
        self.data_loader = _data_loader_or_pd_read_csv(data_loader)
        self.data_loader_kwargs = data_loader_kwargs

    @property
    def df(self):
        if self._df is None:
            self._df = self.data_loader(self.filepath, **self.data_loader_kwargs)
        return self._df

    def to_node(self, **node_kwargs):
        return Node(name=self.name, **node_kwargs)

    def __repr__(self):
        return f'<DataSource(name={self.name}, filepath={self.filepath})>'

class DataSubject:

    def __init__(self, *sources: [DataSource], name=None):
        self.sources = sources
        self.name = name

    def __getitem__(self, item):
        return self.sources[item]

    def to_subgraph(self, options: PipelineOptions):
        nodes = self.to_nodes(options)

        return Subgraph(nodes, name=self.name, digraph=options.digraph, **options.graph_attr)

    def to_nodes(self, options: PipelineOptions):
        if options.data_sources:
            nodes = [source.to_node(**options.node_attr) for source in self.sources]
        else:
            nodes = [Node(name=self.name + 'DUMMY', **options.node_attr)]
        return nodes

    def __repr__(self):
        return f'<DataSubject(name={self.name}, sources={self.sources})>'



def _data_loader_or_pd_read_csv(data_loader=None):
    if data_loader is not None:
        return data_loader
    else:
        return pd.read_csv