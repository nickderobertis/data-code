from typing import Dict
import pandas as pd

from pd_utils.optimize.dftypes import df_types_dict

from datacode.models.variables.typing import PrefixVariableCollectionDict
from datacode.models.variables.collection import VariableCollection


class NoQuotes:
    """
    When printing, prints as a str would except no quotes
    """

    def __init__(self, disp: str):
        self.disp = disp

    def __repr__(self):
        return f'{self.disp}'

    def __eq__(self, other):
        if hasattr(other, 'disp'):
            return self.disp == other.disp
        else:
            return self.disp == other

    def __hash__(self):
        return hash(self.disp)


def variable_lookup_dtype_dict(prefix_variable_dict: PrefixVariableCollectionDict,
                               df: pd.DataFrame) -> Dict[NoQuotes, str]:
    dtypes = df_types_dict(df)
    out_dtype_dict = {}
    for prefix, variable_collection in prefix_variable_dict.items():
        out_dtype_dict.update(
            _variable_lookup_dtype_dict(
                dtypes,
                variable_collection,
                prefix=prefix
            )
        )

    return out_dtype_dict

def _variable_lookup_dtype_dict(dtypes: pd.Series, variable_collection: VariableCollection,
                               prefix='vd') -> Dict[NoQuotes, str]:
    out_dtype_dict = {
        NoQuotes(f'{prefix}.' + variable.name): dtypes[variable.display_name]
        for variable in variable_collection.variables
        if variable.display_name in dtypes
    }

    return out_dtype_dict