import pandas as pd
import os

from ..tools import _ids_str_or_tuple_to_id_name
from .flowchart import Node, Subgraph
from .interface import PipelineOptions, IDSet, IDCollection
from ..backend import compare_id_collections
from .bars import MatchComparisonBarGraph


class DataItem:

    def __init__(self, name):
        self.name = name


class DataSource(DataItem):

    def __init__(self, name=None, id_cols=None, data_source=None, data_loader=None, **data_loader_kwargs):
        """

        :param name:
        :type name:
        :param id_cols: list of tuples or strs of id sets to match on, e.g. ['CUSIP6', ('CUSIP6','Year'), 'ISIN', ('ISIN', 'Year')]
        :type id_cols: list of tuples
        :param data_source:
        :type data_source:
        :param data_loader: function that takes self.data_source as the arg, and usecols=self.id_cols as kwargs
        :type data_loader: Function
        :param data_loader_kwargs:
        :type data_loader_kwargs:
        """

        self.data_source = data_source
        self._df = None
        super().__init__(name)
        if id_cols is not None:
            self.id_cols = _id_cols_collection_to_flattened_list(id_cols)
            self.set_ids(id_cols)
        else:
            self.id_cols = []
            self.ids = []
        self.data_loader = _data_loader_or_pd_read_csv(data_loader)
        self.data_loader_kwargs = data_loader_kwargs

    @property
    def df(self):
        if self._df is None:
            if isinstance(self.data_source, pd.DataFrame):
                self._df = _dummy_load(self.data_source, self.id_cols)
            else:
                self._df = self.data_loader(self.data_source, usecols=self.id_cols, **self.data_loader_kwargs)
        return self._df

    def set_ids(self, id_cols):
        id_sets = _extract_id_sets(self.df, id_cols, name=self.name)
        self.ids = IDCollection(id_sets)

    def to_node(self, **node_kwargs):
        return Node(name=self.name, **node_kwargs)

    def __repr__(self):
        return f'<DataSource(name={self.name}, data_source={self.data_source})>'

class DataSubject:

    def __init__(self, *sources: [DataSource, 'DataCombination'], name=None):
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
            node_kwargs = options.node_attr.copy()
            _add_no_node_line_to_node_kwargs(node_kwargs)
            nodes = [Node(name=self.name + 'DUMMY', label=None, **node_kwargs)]
        return nodes

    def __repr__(self):
        return f'<DataSubject(name={self.name}, sources={self.sources})>'

class DataCombination(DataItem):

    def __init__(self, data_source: DataSource, other_data_source: DataSource, name=None):
        self.data_source = data_source
        self.other_data_source = other_data_source
        super().__init__(name)
        self._ids = None

    def compare(self):
        return compare_id_collections(self.data_source.ids, self.other_data_source.ids, name=self.name)

    def to_node(self, plt=None, **node_kwargs):
        if plt is None:
            import matplotlib.pyplot as plt

        # TODO [#3]: finish compare ids data combination to node

        #### TEMP
        import pdb
        pdb.set_trace()
        #### END TEMP

        comparison = self.compare()
        # No ids, could not generate comparison, display normal node
        if not comparison:
            return Node(name=self.name, **node_kwargs)

        # Implicit else, has ids, able to generate comparison, now put graph in node
        graph = MatchComparisonBarGraph.from_id_comparison_collection(comparison, plt=plt)

        #### TEMP
        graph.draw(plt=plt)
        name_for_path = self.name.replace('/','-') + '.png'
        img_path = os.path.join(r'C:\Users\derobertisna.UFAD\Dropbox (Personal)\UF\Andy\ETF Project\Temp\idcomp', name_for_path)
        plt.savefig(img_path, bbox_inches='tight')

        # Set node style
        this_node_kwargs = node_kwargs.copy()
        _add_no_node_line_to_node_kwargs(this_node_kwargs)

        return Node(name=self.name, label=None, image=img_path, **this_node_kwargs)

        #### END TEMP

    @property
    def ids(self):
        if self._ids is None:
            # Find overlapping id sets
            id_sets = [id_set for id_set in self.data_source.ids if id_set in self.other_data_source.ids]
            self._ids = IDCollection(id_sets)
        return self._ids

    def __repr__(self):
        return f'<DataCombination(name={self.name}, data_source={self.data_source.name}, merge_source={self.other_data_source.name})>'



def _data_loader_or_pd_read_csv(data_loader=None):
    if data_loader is not None:
        return data_loader
    else:
        return pd.read_csv

def _dummy_load(df, usecols=None):
    if usecols is None:
        return df
    return df[usecols]

def _extract_id_sets(df, id_cols, name=None):

    collection_items = []
    for ids in id_cols:
        id_set_name = _ids_str_or_tuple_to_id_name(ids)
        unique = df[ids].drop_duplicates().dropna()
        # 'itertuples' for dfs, 'tolist' for series
        _extract_from_df_or_series_attr, kwargs = _get_func_attr_and_kwargs_to_reduce_from_df_or_series(unique)
        reduce_func = getattr(unique, _extract_from_df_or_series_attr)
        id_rows = reduce_func(**kwargs)
        id_set = IDSet(id_rows, name=id_set_name)
        collection_items.append(id_set)

    return IDCollection(collection_items, name=name)

def _id_cols_collection_to_flattened_list(id_cols):
    out_list = []
    for col_or_tuple in id_cols:
        if isinstance(col_or_tuple, (tuple, list)):
            [out_list.append(item) for item in col_or_tuple]
        else:
            out_list.append(col_or_tuple)
    return out_list

def _get_func_attr_and_kwargs_to_reduce_from_df_or_series(df_or_series):
    if isinstance(df_or_series, pd.DataFrame):
        return 'itertuples', {'index': False, 'name':'ids'}
    elif isinstance(df_or_series, pd.Series):
        return 'tolist', {}
    else:
        raise ValueError(f'Must pass series or dataframe, got type {type(df_or_series)}')


def _add_no_node_line_to_node_kwargs(node_kwargs):
    """
    NOTE: inplace
    """
    # Set node style
    no_line_str = 'setlinewidth(0)'
    if 'style' in node_kwargs:
        style = node_kwargs['style'] + ',' + no_line_str
    else:
        style = no_line_str
    node_kwargs['style'] = style