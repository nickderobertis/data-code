def _get_obj_or_attr(obj, obj_or_attr):
    if obj_or_attr == 'obj':
        return obj
    else:
        return getattr(obj, obj_or_attr)


def _to_list_if_str(list_or_str):
    if isinstance(list_or_str, str):
        return [list_or_str]
    return list_or_str