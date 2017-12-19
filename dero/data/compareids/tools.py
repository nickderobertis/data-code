def _ids_str_or_tuple_to_id_name(ids):
    if isinstance(ids, str):
        return ids
    return '/'.join(ids)