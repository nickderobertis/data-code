from copy import deepcopy
from typing import Sequence, Optional, List

from mixins import ReprMixin

from datacode.models.variables.tools import _get_obj_or_attr
from datacode.models.transform.transform import Transform
from datacode.models.transform.applied import AppliedTransform
from datacode.models.transform.specific import DEFAULT_TRANSFORMS
from .variable import Variable


class VariableCollection(ReprMixin):
    repr_cols = ['name', 'keys']

    _default_attr = 'obj'

    def __init__(self, *variables: Variable, name: str = 'variables', default_attr: str = 'obj',
                 transforms: Optional[Sequence[Transform]] = DEFAULT_TRANSFORMS,
                 default_transforms: Optional[Sequence[AppliedTransform]] = None):
        if transforms is None:
            transforms = []
        if default_transforms is None:
            default_transforms = []
        self.transforms = transforms
        self.default_transforms = default_transforms
        self.items: [Variable, VariableCollection] = deepcopy(list(variables))
        self.default_attr = default_attr
        self.name = name

    def __contains__(self, item):
        return item in self.items

    @property
    def default_attr(self):
        return self._default_attr

    @default_attr.setter
    def default_attr(self, default_attr):
        self._default_attr = default_attr
        # TODO [#13]: could make variable collection initialization more efficient
        #
        # Currently calling self._set_variables_and_collections() before self._create_variable_map()
        # as variables need to have the custom name attributes created. But then still calling after to
        # set the variables attributes correctly. Could reorganize this.
        self._set_variables_and_collections()
        self.variable_map = self._create_variable_map()
        self._set_variables_and_collections()
        self._set_default_attr_in_nested_collections()

    @property
    def items(self):
        return self._items

    @items.setter
    def items(self, variables):
        # Goes inside any collections which are inside this one to populate all nested variables as well as collections
        self._items = _create_variable_and_collection_list(variables)

    @property
    def keys(self) -> List[str]:
        return [var.key for var in self.variables]

    # Must override the standard list methods which add and remove elements because
    # we must reflect those operations on the variable hash map
    def append(self, item):
        if item not in self.items:
            self.items.append(item)
        self._add_item_to_map(item)

    def extend(self, iterable):
        self.items.extend(iterable)
        self._add_items_to_map(iterable)

    def remove(self, item: Variable):
        self.items.remove(item)
        self._delete_from_map(item.key)

    def pop(self, i):
        variable = self.items.pop(i)
        self._delete_from_map(variable.key)
        return

    def _add_transforms_to_variables(self):
        for item in self.variables:
            for transform in self.transforms:
                item._add_transform(transform)

            item.applied_transforms = []  # reset so that are not applied multiple times
            for applied_transform in self.default_transforms:
                item.applied_transforms.append(applied_transform)
                item._set_name_and_symbol_by_transforms()

    def _add_item_to_map(self, item):
        self.variable_map.update({
            item.name: _get_obj_or_attr(item, self.default_attr)
        })
        self._set_variables_and_collections()

    def _add_items_to_map(self, items):
        self.variable_map.update(_create_variable_map(items, self.default_attr))
        self._set_variables_and_collections()


    def _delete_from_map(self, item_name):
        self._set_variables_and_collections()
        return self.variable_map.pop(item_name)

    def _create_variable_map(self):
        return _create_variable_map(self.items, output=self.default_attr)

    def _get_from_variable_map(self, item):
        try:
            return self.variable_map[item]
        except KeyError:
            raise AttributeError(f'attribute {item} does not exist on {self}')

    def _set_variables_and_collections(self):
        self.variables = [item for item in self.items if isinstance(item, Variable)]
        self._add_transforms_to_variables()
        self.collections = [item for item in self.items if isinstance(item, VariableCollection)]

    def _set_default_attr_in_nested_collections(self):
        """
        When default attr is changed, must reflect that change in lower collections
        """
        for collection in self.collections:
            # Changing default attr one level deep will trigger changes in the next level, and so on (recursive)
            collection.default_attr = self.default_attr

    def __getitem__(self, item):
        if isinstance(item, str):
            return self._get_from_variable_map(item)

        return self.items[item]

    def __getattr__(self, item: str):

        # Implement standard public list methods other than those explicitly defined
        if (not item.startswith('_')) and hasattr(list, item):
            return getattr(self.items, item)

        return self._get_from_variable_map(item)

    def __dir__(self):
        return self.variable_map.keys()

    @classmethod
    def from_display_names(cls, *names):
        variables = [Variable.from_display_name(disp_name) for disp_name in names]
        return cls(*variables, default_attr=cls._default_attr)

    @classmethod
    def from_variable_definition_dict(cls, def_dict, name=None):
        return _definition_dict_to_collection(def_dict, name=name, default_attr=cls._default_attr)



def _create_variable_map(variables, output='obj'):
    tups = _create_variable_tuples(variables, output=output)
    return dict(tups)

def _create_variable_tuples(variables, output='obj', tups=None):
    if tups is None:
        tups = []
    for variable_or_collection in variables:
        if isinstance(variable_or_collection, VariableCollection):
            # Add all variables in collection
            tups += _create_variable_tuples(variable_or_collection.items, output=output, tups=tups)
            # Now add the collection itself
            tups.append((variable_or_collection.name, variable_or_collection))
        elif isinstance(variable_or_collection, Variable):
            # Add single variable
            tups.append((variable_or_collection.key, _get_obj_or_attr(variable_or_collection, output)))
        else:
            raise ValueError(f'must pass iterable of Variable or VariableCollection. {variable_or_collection} has type {type(variable_or_collection)}')

    return tups

def _create_variable_and_collection_list(variables, collected=None):
    if collected is None:
        collected = []

    for variable_or_collection in variables:
        if isinstance(variable_or_collection, VariableCollection):
            # Add all variables in collection
            collected = _create_variable_and_collection_list(variable_or_collection.items, collected=collected)
            # Now add the collection itself
            collected.append(variable_or_collection)
        elif isinstance(variable_or_collection, Variable):
            # Add single variable
            if variable_or_collection not in collected:
                collected.append(variable_or_collection)
        else:
            raise ValueError(f'must pass iterable of Variable or VariableCollection. {variable_or_collection} has type {type(variable_or_collection)}')

    return collected

def _definition_dict_to_collection(def_dict, name=None, collection=None, default_attr='obj'):
    if collection is None:
        collection = []

    for key, value in def_dict.items():
        if isinstance(value, dict):
            # recurse through dicts, each time creating another layer of collection
            collection.append(_definition_dict_to_collection(value, name=key))
        elif isinstance(value, list):
            # add each variable in list to current collection
            add_items = [_definition_dict_tuple_or_str_to_variable(val) for val in value]
            collection.extend(add_items)
        elif isinstance(value, (str, tuple)):
            # if just str or tuple is passed, parse and add directly to collection
            collection.append(_definition_dict_tuple_or_str_to_variable(value))
        else:
            raise ValueError(f'invalid definition dict. only dicts, lists, strs, and tuples are allowed, got type {type(value)} for value {value}')

    return VariableCollection(*collection, name=name, default_attr=default_attr)

def _definition_dict_tuple_or_str_to_variable(value):
    if isinstance(value, str):
        # if just str is passed, str is for display name
        return Variable.from_display_name(value)
    elif isinstance(value, tuple):
        # if tuple is passed, it is tuple of name, display name
        return Variable(key=value[0], name=value[1])
    elif isinstance(value, Variable):
        # variable itself was in data, just return it
        return value
    else:
        raise ValueError(f'must pass str or tuple, got type {type(value)} for value {value}')

class VariableDefaultCollection(VariableCollection):

    def __init__(self, *variables, name='variables'):
        super().__init__(*variables, name=name, default_attr=self._default_attr)

class VariableNameCollection(VariableDefaultCollection):

    _default_attr = 'name'


class VariableDisplayNameCollection(VariableDefaultCollection):

    _default_attr = 'display_name'

class VariableChangeNameCollection(VariableDefaultCollection):

    _default_attr = 'change_name'


class VariablePortfolioNameCollection(VariableDefaultCollection):

    _default_attr = 'port_name'

class VariableChangePortfolioNameCollection(VariableDefaultCollection):

    _default_attr = 'change_port_name'