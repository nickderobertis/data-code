from .interface import MatchComparisonBarData, MatchComparisonBarGraphData, IDComparisonCollection


class MatchComparisonBar:

    def __init__(self, data: MatchComparisonBarData):
        self.data = data


class MatchComparisonBarGraph:

    def __init__(self, *bars: [MatchComparisonBarData], plt=None, name=None, relative=True, thickness=0.5):
        if plt is None:
            import matplotlib.pyplot as plt
        self._absolute_bars = bars
        self._relative_bars = _convert_bar_data_to_relative(bars)
        self.plt = plt
        self.thickness = thickness
        self.relative = relative
        self._set_graph_data(name)

    @property
    def relative(self):
        return self._relative

    @relative.setter
    def relative(self, relative):
        if relative:
            self.bars = self._relative_bars
        else:
            self.bars = self._absolute_bars

        self._relative = relative

    def _set_graph_data(self, name):
        self.graph_data = _convert_bar_data_to_bar_graph_data(self.bars, name=name)
        self._set_max_bar_length(self.bars)

    def _set_max_bar_length(self, bars):
        self.max_len = max([sum(bar) for bar in bars])

    def draw(self, plt):
        plt.clf() #clear out any previous figures

        #### TEMP
        # import pdb
        # pdb.set_trace()
        #### END TEMP

        if self.graph_data.name is not None:
            _draw_title(plt, self.graph_data.name)
        _draw_bars(plt, *self.graph_data, thickness=self.thickness)
        _draw_bar_labels(plt, [bar.name for bar in self.bars], self.max_len)
        _draw_number_labels(plt, self._absolute_bars, self.max_len, self.relative)
        plt.axis('off')
        return plt

    @classmethod
    def from_id_comparison_collection(cls, id_comparison: IDComparisonCollection, plt=None):
        if plt is None:
            import matplotlib.pyplot as plt
        return cls(*[MatchComparisonBarData.from_id_comparison(compare) for compare in id_comparison], name=id_comparison.name, plt=plt)

def _convert_bar_data_to_bar_graph_data(data: [MatchComparisonBarData], name=None):
    split_items = [items for items in zip(*data)]
    return MatchComparisonBarGraphData(*split_items, name=name)

def _convert_bar_data_to_relative(data: [MatchComparisonBarData], scale=100):
    out_list = []
    for bardata in data:
        total_items = sum(bardata)
        new_bardata = MatchComparisonBarData(*[(item / total_items) * scale for item in bardata], name=bardata.name)
        out_list.append(new_bardata)

    return out_list

def _draw_bars(plt, left_items, match_items, right_items, thickness=0.5):
    index = [i for i in range(len(left_items))]

    # first bar starts at zero, second bar starts where first left off.
    # then add the left and middle bars to get start for last bar
    new_lefts = [sum(items) for items in zip(*[left_items, match_items])]

    _draw_bars_part(plt, index, left_items, thickness=thickness, left=None)
    _draw_bars_part(plt, index, match_items, thickness=thickness, left=left_items)
    _draw_bars_part(plt, index, right_items, thickness=thickness, left=new_lefts)


def _draw_bars_part(plt, index, items, thickness=0.5, left=None):
    return plt.barh(index, items, thickness, left=left)

def _draw_title(plt, title):
    plt.title(title)

def _draw_bar_labels(plt, labels, bar_max_width=5):
    [plt.text(-0.05 * bar_max_width, height, label, horizontalalignment='right') for height, label in enumerate(labels)]

def _draw_number_labels(plt, bars: [MatchComparisonBarData], max_len, relative=True):
    for i, bar in enumerate(bars):
        for j, num_label in enumerate(bar):
            label = str(num_label)
            sum_bar = sum(bar)
            if relative:
                pos = (bar.midpoints[j]/sum_bar) * max_len
            else:
                pos = bar.midpoints[j]
            plt.text(pos, i, label, horizontalalignment='center', verticalalignment='center')