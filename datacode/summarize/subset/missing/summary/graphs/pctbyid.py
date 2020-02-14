import pandas as pd
import os
import matplotlib.pyplot as plt
from pyexlatex.figure import Figure

def missing_pct_by_id_figure(df: pd.DataFrame, id_col: str, col_with_missings: str,
                             outname: str='Percentage Missing Obs by Firm',
                             outfolder: str='.') -> Figure:

    if outfolder is None:
        outfolder = '.'

    matplotlib_figure = _missing_pct_by_id_plot(
        df,
        id_col,
        col_with_missings
    )

    base_outpath = os.path.join(outfolder, outname)
    source_outpath = f'{base_outpath} source.pdf'
    matplotlib_figure.savefig(source_outpath)
    plt.clf()

    figure = Figure.from_dict_of_names_and_filepaths(
        {outname: source_outpath},
        figure_name=outname,
        landscape=True,
        position_str_name_dict={outname: r'[t]{0.9\linewidth}'}
    )

    figure.to_pdf_and_move(
        outfolder=outfolder,
        outname=outname
    )

    return figure

def _missing_pct_by_id_plot(df: pd.DataFrame, id_col: str, col_with_missings: str) -> plt.Figure:

    id_pct_missing_series = _missing_pct_by_id_series(
        df,
        id_col,
        col_with_missings
    )

    ax = id_pct_missing_series.plot(kind='hist', bins=50, figsize=(14, 6))
    fig = ax.get_figure()

    return fig

def _missing_pct_by_id_series(df: pd.DataFrame, id_col: str, col_with_missings: str) -> pd.Series:

    if '_ones' in df.columns:
        raise ValueError('will overwrite existing column _ones')

    df['_ones'] = 1  # temporary column never missing for counting
    id_pct_missing = 1 - (df.groupby(id_col)[col_with_missings].count() / df.groupby(id_col)['_ones'].count())
    df.drop('_ones', axis=1, inplace=True)

    return id_pct_missing