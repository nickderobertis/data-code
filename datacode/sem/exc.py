

class SEMException(Exception):
    pass


class SampleException(SEMException):
    pass


class ModelException(SEMException):
    pass


class SampleCovMatrixNotPositiveDefiniteException(SampleException):
    pass


class ModelCovMatrixNotPositiveDefiniteException(ModelException):
    pass
