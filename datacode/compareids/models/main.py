from .steps import Process
from .interface import PipelineOptions
from .flowchart import _get_digraph_if_true_else_graph

class Pipeline:

    def __init__(self, process: Process, options: [PipelineOptions, None] = None, name=None):
        self.process = process
        if options is None:
            options = PipelineOptions() #initialize with defaults
        if name is None:
            name = 'default graph'
        self.options = options
        self.name = name

    def build_graph(self, **graph_kwargs):
        subgraphs = self.process.to_subgraphs(self.options)
        edges = self.process.to_edges(self.options)
        graph_elements = subgraphs + edges
        if 'graph_attr' in graph_kwargs:
            graph_kwargs['graph_attr'].update({'compound': 'true'})
        else:
            graph_kwargs['graph_attr'] = {'compound': 'true'}

        Graph = _get_digraph_if_true_else_graph(self.options.digraph)
        graph = Graph(self.name, **graph_kwargs)
        [elem.add_to_graph(graph) for elem in graph_elements]

        self.graph = graph
        return graph


class LogicalPipeline(Pipeline):

    def __init__(self, process: Process, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions(data_sources=False) #initialize with defaults
        else:
            options.data_sources = False
        super().__init__(process, options)


class DataMergePipeline(Pipeline):

    def __init__(self, process: Process, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions(data_sources=True) #initialize with defaults
        else:
            options.data_sources = True
        super().__init__(process, options)
