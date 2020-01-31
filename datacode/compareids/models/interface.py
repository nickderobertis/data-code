from .base import NamedList, NamedSet


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


class IDSet(NamedSet):
    pass

class IDCollection(NamedList):
    pass

    def to_id_list(self):
        out_set = set()
        for set_ in self:
            out_set.update(set_)
        return list(out_set)


class MatchData:

    def __iter__(self):
        return (i for i in self.tuple)

    def __len__(self):
        return len(self.tuple)

    def __getitem__(self, item):
        return self.tuple[item]


class IDComparison(MatchData):

    def __init__(self, left_unmatched, matched, right_unmatched, name=None):
        self.left_unmatched = left_unmatched
        self.matched = matched
        self.right_unmatched = right_unmatched
        self.name = name
        self.tuple = (left_unmatched, matched, right_unmatched)


class MatchComparisonBarData(IDComparison):

    def __init__(self, left_unmatched, matched, right_unmatched, name=None):
        super().__init__(left_unmatched, matched, right_unmatched, name=name)
        self._set_midpoints()

    def _set_midpoints(self):
        left_midpoint = self.left_unmatched/2
        matched_midpoint = self.left_unmatched + self.matched/2
        right_midpoint = self.left_unmatched + self.matched + self.right_unmatched/2
        self.midpoints = (left_midpoint, matched_midpoint, right_midpoint)

    @classmethod
    def from_id_comparison(cls, id_comp: IDComparison):
        return cls(*id_comp, name=id_comp.name)


class IDComparisonCollection(NamedList):
    pass


class MatchComparisonBarGraphData(MatchData):

    def __init__(self, left_unmatched_tup, matched_tup, right_unmatched_tup, name=None):
        self.left_unmatched_tup = left_unmatched_tup
        self.matched_tup = matched_tup
        self.right_unmatched_tup = right_unmatched_tup
        self.name = name
        self.tuple = (left_unmatched_tup, matched_tup, right_unmatched_tup)