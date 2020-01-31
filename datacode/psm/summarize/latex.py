from datacode.psm.typing import DfDict, StrOrNone, FloatOrNone
import pyexlatex.table as lt

def matching_latex_table_from_df_dict(df_dict: DfDict, entity_var: str, time_var: str, below_text: StrOrNone = None,
                                      caption: str = 'Propensity Score Matching',
                                      min_matching_pct: FloatOrNone = None) -> lt.Table:
    model_dt = lt.DataTable.from_df(
        df_dict['model'],
        include_index=True,
        extra_header='Coefficients and Standard Errors',
    )

    predict_dt = lt.DataTable.from_df(
        df_dict['predict'],
        include_index=True,
        extra_header='Prediction Accuracy',
    )

    logit_panel = lt.Panel.from_data_tables(
        [model_dt, predict_dt],
        shape=(1, 2),
        name='Logistic Regression Results'
    )

    match_panel = lt.Panel.from_df(
        df_dict['match'],
        include_index=True,
        name='Summary Statistics by Sample'
    )

    if below_text is None:
        below_text = """
        Propensity Score Matching (PSM) is used to create a sample of non-treated which is similar
        to the treated firms across observable characteristics.
        """

    if min_matching_pct is not None:
        below_text += ' ' + f"""
        When finding matches, only those {entity_var}s with {min_matching_pct * 100:.3g}% of overlapping
        {time_var} observations were used.
        """.strip()

    table = lt.Table.from_panel_list(
        [logit_panel, match_panel],
        below_text=below_text,
        caption=caption
    )

    return table