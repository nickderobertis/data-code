from itertools import product

from .datasets import DataSubject, DataCombination
from .flowchart import Edge
from .interface import PipelineOptions


class Step:

    def __init__(self, data_subject: DataSubject):
        self.data_subject = data_subject
        self.all_subjects = [data_subject]
        self._subgraphs = None

    def subgraphs(self, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions() #initialize with defaults

        if self._subgraphs is None:
            self._subgraphs = [subject.to_subgraph(options) for subject in self.all_subjects]
        return self._subgraphs

    def __repr__(self):
        return f'<Step(data_subject={self.data_subject})>'


class MergeStep(Step):

    def __init__(self, data_subject: DataSubject, merge_into_data_subject: DataSubject):
        self.merge_into_subject = merge_into_data_subject
        super().__init__(data_subject)
        self.all_subjects = [data_subject, merge_into_data_subject]
        self._combined_subject = None

    def subgraphs(self, options: [PipelineOptions, None] = None):
        if options is None:
            options = PipelineOptions() #initialize with defaults

        if self._subgraphs is None:
            subgraphs = []
            subgraphs.append(self._merge_subgraph(options))
            subgraphs.extend(super().subgraphs(options))
            self._subgraphs = subgraphs
        return self._subgraphs

    def _merge_subgraph(self, options: PipelineOptions):
        data_sources = []
        for orig_source, merge_source in product(self.data_subject.sources, self.merge_into_subject.sources):
            name = orig_source.name + ' ' + self.data_subject.name + '/' + merge_source.name + ' ' + self.merge_into_subject.name
            data_sources.append(DataCombination(orig_source, merge_source, name=name))

        subject_name = self.data_subject.name + '/' + self.merge_into_subject.name
        self._combined_subject = DataSubject(*data_sources, name=subject_name)
        return self._combined_subject.to_subgraph(options)

    @property
    def combined_subject(self):
        if self._combined_subject is None:
            self._merge_subgraph(PipelineOptions())
        return self._combined_subject

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
            if isinstance(step, MergeStep):
                subgraphs.extend(step.subgraphs(options)) #contains last subgraph, combined subgraph, and to merge subgraph
            elif isinstance(step, Step):
                continue #step subgraph will be handled within merge step subgraph
            else:
                raise ValueError(f'must pass Step or MergeStep, got type {type(step)}')

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
        combined_subgraph, last_subgraph, merge_subgraph = step.subgraphs(options)
        edges = []
        last_source_name, merge_source_name = _combined_source_name_to_individual_source_names(combined_subgraph.name)
        for combined_node in combined_subgraph.nodes:
            if options.data_sources: #extract node names by parsing combined name
                last_node_name, merge_node_name = _combined_name_to_input_and_merge_name(combined_node.name, last_source_name, merge_source_name)
            else: #now the source names are representative of the node names
                last_node_name = last_source_name
                merge_node_name = merge_source_name
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

def _combined_name_to_input_and_merge_name(combined_name, last_source_name, merge_source_name):
    combined_name = combined_name.strip('DUMMY').replace(last_source_name, '').replace(merge_source_name, '')
    return _combined_source_name_to_individual_source_names(combined_name)


def _combined_source_name_to_individual_source_names(combined_name):
    if '/' not in combined_name:
        raise ValueError(f'passed regular name instead of combined name. combined name must have /. got {combined_name}')
    parts = combined_name.split('/')
    return '/'.join(parts[:-1]).strip(), parts[-1].strip()