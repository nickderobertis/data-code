from typing import Tuple

from datacode.models.variables import Variable, VariableCollection
from tests.input_funcs import lag

VAR1_ARGS = ('stuff', 'My Stuff')
VAR2_ARGS = ('thing', 'My Thing')
VC_NAME = 'my_collection'
NAME_DICT_NAME_SEQ = {
    'port': ['Port', 'Var'],
}
NAME_DICT_NAME_FUNC = {
    'lag': lag
}
ALL_NAME_DICT = {}
ALL_NAME_DICT.update(NAME_DICT_NAME_SEQ)
ALL_NAME_DICT.update(NAME_DICT_NAME_FUNC)


class VariableTest:

    def create_variables(self) -> Tuple[Variable, Variable]:
        v = Variable(*VAR1_ARGS)
        v2 = Variable(*VAR2_ARGS)
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

        assert v.display_name == VAR1_ARGS[1]
        assert v.name == VAR1_ARGS[0]
        assert v.to_tuple() == VAR1_ARGS
        assert v2.display_name == VAR2_ARGS[1]
        assert v2.name == VAR2_ARGS[0]
        assert v2.to_tuple() == VAR2_ARGS


class TestVariableCollection(VariableTest):

    def test_create_variable_collection(self):
        vc, v, v2 = self.create_variable_collection()

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing == v2

    def test_create_variable_collection_with_name_map_sequence(self):
        vc, v, v2 = self.create_variable_collection(
            name_map=NAME_DICT_NAME_SEQ
        )

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing == v2
        assert vc.stuff.port == VAR1_ARGS[1] + ' Port Var'
        assert vc.thing.port == VAR2_ARGS[1] + ' Port Var'

    def test_create_variable_collection_with_name_map_func(self):
        vc, v, v2 = self.create_variable_collection(
            name_map=NAME_DICT_NAME_FUNC
        )

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing == v2
        assert vc.stuff.lag(1) == VAR1_ARGS[1] + '_{t - 1}'
        assert vc.thing.lag(1) == VAR2_ARGS[1] + '_{t - 1}'

    def test_create_variable_collection_with_name_map_func_and_seq(self):
        vc, v, v2 = self.create_variable_collection(
            name_map=ALL_NAME_DICT
        )

        assert vc.name == VC_NAME
        assert vc.stuff == v
        assert vc.thing == v2
        assert vc.stuff.port == VAR1_ARGS[1] + ' Port Var'
        assert vc.thing.port == VAR2_ARGS[1] + ' Port Var'
        assert vc.stuff.lag(1) == VAR1_ARGS[1] + '_{t - 1}'
        assert vc.thing.lag(1) == VAR2_ARGS[1] + '_{t - 1}'

    def test_create_variable_collection_with_default_attr_and_name_map_seq(self):
        vc, v, v2 = self.create_variable_collection(
            name_map=NAME_DICT_NAME_SEQ,
            default_attr='port'
        )

        assert vc.name == VC_NAME
        assert vc.stuff == VAR1_ARGS[1] + ' Port Var'
        assert vc.thing == VAR2_ARGS[1] + ' Port Var'

    def test_create_variable_collection_with_default_attr_and_name_map_func(self):
        vc, v, v2 = self.create_variable_collection(
            name_map=NAME_DICT_NAME_FUNC,
            default_attr='lag'
        )

        assert vc.name == VC_NAME
        assert vc.stuff(1) == VAR1_ARGS[1] + '_{t - 1}'
        assert vc.thing(1) == VAR2_ARGS[1] + '_{t - 1}'


