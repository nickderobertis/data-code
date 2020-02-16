from typing import Tuple

from datacode.models.variables import Variable, VariableCollection
from datacode import Transform, AppliedTransform
from datacode.models.symbols import Symbol
from tests.input_funcs import lag

VAR1_ARGS = ('stuff', 'My Stuff')
VAR2_ARGS = ('thing_me', 'My Thing')
VAR3_ARGS = ('other', 'My Other')
VC_NAME = 'my_collection'
TRANSFORMS = [
    Transform('port', name_func=lambda x: f'{x} Port Var'),
    Transform('lag', name_func=lag),
]


class VariableTest:

    def create_variables(self, **kwargs) -> Tuple[Variable, Variable]:
        config_dict = dict(

        )
        config_dict.update(**kwargs)
        v = Variable(*VAR1_ARGS, **config_dict)
        v2 = Variable(*VAR2_ARGS, **config_dict)
        return v, v2

    def create_variable_collection(self, **kwargs) -> Tuple[VariableCollection, Variable, Variable]:
        config_dict = dict(
            name=VC_NAME
        )
        config_dict.update(**kwargs)
        v, v2 = self.create_variables()
        vc = VariableCollection(v, v2, **config_dict)
        return vc, v, v2


class TestVariable(VariableTest):

    def test_create_variables(self):
        v, v2 = self.create_variables()

        assert v.name == VAR1_ARGS[1]
        assert v.key == VAR1_ARGS[0]
        assert v.to_tuple() == VAR1_ARGS
        assert v.symbol == Symbol(r'\text{Stuff}')
        assert v2.name == VAR2_ARGS[1]
        assert v2.key == VAR2_ARGS[0]
        assert v2.symbol == Symbol('TM')
        assert v2.to_tuple() == VAR2_ARGS

    def test_set_description(self):
        desc = 'my desc'
        v, v2 = self.create_variables(description=desc)
        assert v.description == desc
        assert v2.description == desc


class TestVariableCollection(VariableTest):

    def test_create_variable_collection(self):
        vc, v, v2 = self.create_variable_collection()

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing_me == v2

    def test_create_variable_collection_with_transforms(self):
        vc, v, v2 = self.create_variable_collection(
            transforms=TRANSFORMS
        )

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing_me == v2
        assert vc.stuff.port().name == VAR1_ARGS[1] + ' Port Var'
        assert vc.stuff.port().port().name == VAR1_ARGS[1] + ' Port Var Port Var'
        assert str(vc.stuff.port().symbol) == r'\text{Stuff} Port Var'
        assert str(vc.stuff.port().port().symbol) == r'\text{Stuff} Port Var Port Var'
        assert vc.thing_me.port().name == VAR2_ARGS[1] + ' Port Var'
        assert str(vc.thing_me.port().symbol) == 'TM Port Var'
        assert vc.stuff.lag(1).name == VAR1_ARGS[1] + '_{t - 1}'
        assert str(vc.stuff.lag(1).symbol) == r'\text{Stuff}_{t - 1}'
        assert vc.thing_me.lag(1).name == VAR2_ARGS[1] + '_{t - 1}'
        assert str(vc.thing_me.lag(1).symbol) == 'TM_{t - 1}'

    def test_create_variable_collection_with_default_attr(self):
        vc, v, v2 = self.create_variable_collection(
            default_attr='name'
        )

        assert vc.name == VC_NAME
        assert vc.stuff == VAR1_ARGS[1]
        assert vc.thing_me == VAR2_ARGS[1]

    def test_create_variable_collection_with_default_transform_no_args(self):
        vc, v, v2 = self.create_variable_collection(
            transforms=TRANSFORMS,
            default_transforms=[AppliedTransform.from_transform(TRANSFORMS[0])],
        )

        assert vc.name == VC_NAME
        assert vc.stuff.name == VAR1_ARGS[1] + ' Port Var'
        assert str(vc.stuff.symbol) == r'\text{Stuff} Port Var'
        assert vc.thing_me.name == VAR2_ARGS[1] + ' Port Var'
        assert str(vc.thing_me.symbol) == 'TM Port Var'

    def test_create_variable_collection_with_default_transform_with_args(self):
        vc, v, v2 = self.create_variable_collection(
            transforms=TRANSFORMS,
            default_transforms=[AppliedTransform.from_transform(TRANSFORMS[1], 5)],
        )

        assert vc.name == VC_NAME
        assert vc.stuff.name == VAR1_ARGS[1] + '_{t - 5}'
        assert str(vc.stuff.symbol) == r'\text{Stuff}_{t - 5}'
        assert vc.thing_me.name == VAR2_ARGS[1] + '_{t - 5}'
        assert str(vc.thing_me.symbol) == 'TM_{t - 5}'

    def test_create_variable_collection_with_multiple_default_transforms(self):
        vc, v, v2 = self.create_variable_collection(
            transforms=TRANSFORMS,
            default_transforms=[
                AppliedTransform.from_transform(TRANSFORMS[0]),
                AppliedTransform.from_transform(TRANSFORMS[1], 5)
            ],
        )

        assert vc.name == VC_NAME
        assert vc.stuff.name == VAR1_ARGS[1] + ' Port Var_{t - 5}'
        assert str(vc.stuff.symbol) == r'\text{Stuff} Port Var_{t - 5}'
        assert vc.thing_me.name == VAR2_ARGS[1] + ' Port Var_{t - 5}'
        assert str(vc.thing_me.symbol) == 'TM Port Var_{t - 5}'

    def test_create_variable_collection_with_default_attr_and_default_transform(self):
        vc, v, v2 = self.create_variable_collection(
            transforms=TRANSFORMS,
            default_transforms=[AppliedTransform.from_transform(TRANSFORMS[0])],
            default_attr='name'
        )

        assert vc.name == VC_NAME
        assert vc.stuff == VAR1_ARGS[1] + ' Port Var'
        assert vc.thing_me == VAR2_ARGS[1] + ' Port Var'

    def test_create_multiple_variable_collections_no_overlap(self):
        vc, v, v2 = self.create_variable_collection()
        vc2 = VariableCollection(
            v,
            v2,
            name=VC_NAME,
            transforms=TRANSFORMS,
            default_transforms=[AppliedTransform.from_transform(TRANSFORMS[0])],
        )

        assert vc.name == VC_NAME
        assert vc2.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing_me == v2
        assert not vc.stuff.applied_transforms
        assert not vc.thing_me.applied_transforms
        assert vc2.stuff.name == VAR1_ARGS[1] + ' Port Var'
        assert vc2.thing_me.name == VAR2_ARGS[1] + ' Port Var'
        assert vc2.stuff.applied_transforms
        assert vc2.thing_me.applied_transforms

