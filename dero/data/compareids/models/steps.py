from itertools import product

from .datasets import DataSubject, DataSource
from .flowchart import Edge
from .interface import PipelineOptions


class Step:

    def __init__(self, data_subject: DataSubject):
        self.data_subject = data_subject
        self.all_subjects = [data_subject]

    def to_subgraphs(self, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions() #initialize with defaults
        return [subject.to_subgraph(options) for subject in self.all_subjects]

    def __repr__(self):
        return f'<Step(data_subject={self.data_subject})>'


class MergeStep(Step):

    def __init__(self, data_subject: DataSubject, merge_into_data_subject: DataSubject):
        self.merge_into_subject = merge_into_data_subject
        super().__init__(data_subject)
        self.all_subjects = [data_subject, merge_into_data_subject]

    def to_subgraphs(self, options: [PipelineOptions, None] = None):
        subgraphs = []
        subgraphs.append(self._merge_subgraph(options))
        subgraphs.extend(super().to_subgraphs(options))
        return subgraphs

    def _merge_subgraph(self, options: PipelineOptions):
        orig_nodes = self.data_subject.to_nodes(options)
        merge_nodes = self.merge_into_subject.to_nodes(options)

        data_sources = []
        for orig_source, merge_source in product(orig_nodes, merge_nodes):
            name = orig_source.name + '/' + merge_source.name
            data_sources.append(DataSource(name=name))

        subject_name = self.data_subject.name + '/' + self.merge_into_subject.name
        combined_subject = DataSubject(*data_sources, name=subject_name)
        return combined_subject.to_subgraph(options)

    def __repr__(self):
        return f'<MergeStep(data_subject={self.data_subject}, merge_subject={self.merge_into_subject})>'


class Process:

    def __init__(self, *steps: [Step]):
        self.steps = steps

    def __getitem__(self, item):
        return self.steps[item]

    def to_subgraphs(self, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions() #initialize with defaults

        subgraphs = []
        for step in self.steps:
            subgraphs.extend(step.to_subgraphs(options))

        return subgraphs


    def to_edges(self, options: [PipelineOptions, None] = None, **edge_kwargs):
        if options is None:
            options = PipelineOptions() #initialize with defaults

        edges = []
        for i, step in enumerate(self.steps):
            if i == 0:
                continue #don't need to create edges with beginning step
            edges.extend(self._create_edges_for_step(step, options, **edge_kwargs))

        return edges

    def _create_edges_for_step(self, step: MergeStep, options: PipelineOptions, **edge_kwargs):
        # For current, need to split combined nodes and merge subgraphs
        combined_subgraph, last_subgraph, merge_subgraph = step.to_subgraphs(options)
        edges = []
        for combined_node in combined_subgraph.nodes:
            last_node_name, merge_node_name = _combined_name_to_input_and_merge_name(combined_node.name)
            last_node = last_subgraph.nodes[last_node_name]
            merge_node = merge_subgraph.nodes[merge_node_name]

            if options.data_sources:
                edges.append(Edge(last_node, combined_node, **edge_kwargs))
                edges.append(Edge(merge_node, combined_node, **edge_kwargs))
            else:
                edges.append(Edge(last_node, combined_node, for_subgraphs=(last_subgraph, combined_subgraph)))
                edges.append(Edge(merge_node, combined_node, for_subgraphs=(merge_subgraph, combined_subgraph)))

        return edges

    def __repr__(self):
        return f'<Process(steps={self.steps})>'


def _to_combined_name(input_name, merge_name):
    return f'{input_name}/{merge_name}'

def _combined_name_to_input_and_merge_name(combined_name):
    combined_name = combined_name.strip('DUMMY')
    if '/' not in combined_name:
        raise ValueError(f'passed regular name instead of combined name. combined name must have /. got {combined_name}')
    parts = combined_name.split('/')
    return '/'.join(parts[:-1]), parts[-1]


