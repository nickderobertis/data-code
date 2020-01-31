from datacode.ff.fftypes import (
    DictofStrsandStrLists,
    GroupvarNgroupsDict,
    TwoStrTupleList,
    StrBoolDict,
    Tuple
)
from datacode.ff.create.sort import _other_groupvar_portname

def _standardize_custom_args(custom_labels: DictofStrsandStrLists = None,
                             custom_groupvar_ngroups_dict: GroupvarNgroupsDict = None,
                             custom_pairings: TwoStrTupleList = None,
                             custom_low_minus_high_dict: StrBoolDict = None
                             ) -> Tuple[DictofStrsandStrLists, GroupvarNgroupsDict, TwoStrTupleList, StrBoolDict]:

    # Replace variables with portfolio variables
    if custom_labels is not None:
        custom_labels = {_other_groupvar_portname(key): value for key, value in custom_labels.items()}

    if custom_low_minus_high_dict is not None:
        custom_low_minus_high_dict = {_other_groupvar_portname(key): value for key, value in custom_low_minus_high_dict.items()}

    if custom_pairings is not None:
        new_custom_pairings = []
        for pairing in custom_pairings:
            new_custom_pairings.append(
                (
                    _other_groupvar_portname(pairing[0]),
                    _other_groupvar_portname(pairing[1])
                )
            )
    else:
        new_custom_pairings = None

    return custom_labels, custom_groupvar_ngroups_dict, new_custom_pairings, custom_low_minus_high_dict