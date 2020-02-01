from .interface import MatchComparisonBarData, MatchComparisonBarGraphData, IDComparisonCollection
import math

class MatchComparisonBar:

    def __init__(self, data: MatchComparisonBarData):
        self.data = data

class BarText:

    def __init__(self, label, bar_length, total_length, xy=(0,0), **text_kwargs):
        self.label = label
        self.bar_length = bar_length
        self.total_length = total_length
        self.xy = xy
        self.text_kwargs = text_kwargs
        self.shifted = False

    def draw(self, plt=None, bar_num=0):
        """

        :param plt:
        :type plt:
        :param bar_num: position of bar within triple bar. i.e. a single bar has three bars in it. 0 is the left.
            1 is the middle, and 2 is the right.
        :type bar_num: int
        :return:
        :rtype:
        """
        if plt is None:
            import matplotlib.pyplot as plt

        self._set_position(bar_num)
        self._set_text_kwarg_defaults()
        plt.annotate(self.label, xy=self.xy, xytext=self.text_pos, **self.text_kwargs)

    def _set_position(self, bar_num):
        """
        Determine postion of text depending on length of bar and length of text
        :return:
        :rtype:
        """

        # TODO [#2]: handle when labels are so large that they overlap due to not enough x shift.
        #
        # E.g. `MatchComparisonBarData(100000000, 100000, 100000, name='Unbalanced')`

        # Testing shows it takes about 51 numbers to fill a bar
        max_digits_total_bar = 51
        max_digits_this_bar = math.floor((self.bar_length / self.total_length) * max_digits_total_bar)

        # Label fits -- no need to shift
        if len(self.label) < max_digits_this_bar:
            self.text_pos = self.xy
            # self.shifted remains False
            return

        #Implicit else, label does not fit, need to shift

        shift_multiplier = bar_num - 1 #change to -1, 0, 1 instead of 0, 1, 2
        rl_shift = 0.1 * self.total_length
        x_shift = shift_multiplier * rl_shift

        self.text_pos = (self.xy[0] + x_shift, self.xy[1] - 0.35)
        self.shifted = True

    def _set_text_kwarg_defaults(self):
        # Only add arrow if shifted text
        if self.shifted:
            if 'arrowprops' not in self.text_kwargs:
                self.text_kwargs['arrowprops'] = dict(
                    width=0.01, facecolor='black', shrink=0.05, headwidth=0
                )
        if 'horizontalalignment' not in self.text_kwargs:
            self.text_kwargs['horizontalalignment'] = 'center'
        if 'verticalalignment' not in self.text_kwargs:
            self.text_kwargs['verticalalignment'] = 'center'


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
        if not bars:
            return 0
        self.max_len = max([sum(bar) for bar in bars])

    def draw(self, plt):

        if not self.graph_data:
            # Don't plot
            return plt

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

        if not id_comparison:
            return cls(name=id_comparison.name, plt=plt)
        return cls(*[MatchComparisonBarData.from_id_comparison(compare) for compare in id_comparison], name=id_comparison.name, plt=plt)

def _convert_bar_data_to_bar_graph_data(data: [MatchComparisonBarData], name=None):
    if not data:
        return []
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
                bar_len = (bar[j]/sum_bar) * max_len
            else:
                pos = bar.midpoints[j]
                bar_len = bar[j]
            text = BarText(label, bar_len, max_len, xy=(pos, i), horizontalalignment='center', verticalalignment='center')
            text.draw(plt, bar_num=j)