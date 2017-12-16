import itertools

def compare_identifiers(df_tup_list, id_var = 'CQID'):
    """
    df_tup_list is in the format
    df_tup_list = [
    ('Financials', df),
    ('Government Ownership', gdf),
    ]
    """

    id_lists = [df[id_var].unique().tolist() for key, df in df_tup_list]

    for i, j in itertools.combinations([i for i in range(len(df_tup_list))], 2):

        output_str = _compare_idlists_and_output_summary(
            id_lists[i],
            id_lists[j],
            df_tup_list[i][0],
            df_tup_list[j][0]
        )
        print(output_str)

    print(', '.join([_separate_summary(tup[0], id_lists[i]) for i, tup in enumerate(df_tup_list)]))

def _compare_idlists_and_output_summary(own_ids, other_ids, own_name, other_name):
    output_str = ''
    output_str += _compare_idlists_and_output_summary_one_way(
        own_ids,
        other_ids,
        own_name,
        other_name
    )
    output_str += _compare_idlists_and_output_summary_one_way(
        other_ids,
        own_ids,
        other_name,
        own_name
    )

    return output_str


def _compare_idlists_and_output_summary_one_way(own_ids, other_ids, own_name, other_name):

    own_ids_in_other = len([item for item in own_ids if item in other_ids])

    return f'{own_ids_in_other} ids from {own_name} in {other_name}.\n'

def _separate_summary(name, ids):
    return f'{len(ids)} ids in {name}'