from datacode.ff.fftypes import (
    StrOrInt,
    DictofStrsandStrLists,
    GroupvarNgroupsDict,
    TwoStrTupleList,
    StrBoolDict,
    Tuple
)

def parse_model(model_selector: StrOrInt) -> StrOrInt:

    if isinstance(model_selector, int):
        if model_selector in (3, 5):
            return model_selector
        else:
            raise ValueError(f'expected model 3 or 5 factor. got {model_selector} factor')

    model_selector = model_selector.strip().lower()

    if model_selector in ('3', '5'):
        return int(model_selector)

    if _is_custom_str(model_selector):
        return 'custom'

    raise ValueError(f'could not parse model selector. pass 3, 5, or custom. got {model_selector}')

def _is_custom_str(model_str: str):
    return model_str in ('c', 'custom', 'cust')

def _validate_model(model_selector: StrOrInt,
                    custom_labels: DictofStrsandStrLists = None,
                    custom_groupvar_ngroups_dict: GroupvarNgroupsDict = None,
                    custom_pairings: TwoStrTupleList = None,
                    custom_low_minus_high_dict: StrBoolDict = None) -> None:
    model = parse_model(model_selector)

    # base model validation is done in parse_model, this is for custom model validation
    if model != 'custom':
        return

    for item in [custom_labels, custom_groupvar_ngroups_dict, custom_pairings, custom_low_minus_high_dict]:
        if item is None:
            raise ValueError('got custom FF model type. must pass all of custom_labels, '
                             'custom_groupvar_ngroups_dict, custom_pairings, '
                             'and custom_low_minus_high_dict.')

