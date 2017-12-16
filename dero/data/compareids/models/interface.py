
class PipelineOptions:

    def __init__(self, data_sources=False, digraph=True, node_kwargs={}, graph_kwargs={}):
        self.data_sources = data_sources
        self.digraph = digraph
        self.node_attr = node_kwargs
        self.graph_attr = graph_kwargs

    def keys(self):
        return ['data_sources', 'digraph', 'node_attr', 'graph_attr']

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        return f'<PipelineOptions(data_sources={self.data_sources}, digraph={self.digraph})>'
