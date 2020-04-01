import pandas as pd
from semopy import Model, Optimizer


def run_model(model_def: str, df: pd.DataFrame, **opt_kwargs) -> Optimizer:
    model = Model(model_def)
    model.load_dataset(df)
    opt = Optimizer(model)
    opt.optimize(**opt_kwargs)
    return opt
