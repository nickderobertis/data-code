from itertools import product

from .models.interface import IDSet, IDCollection, IDComparison, IDComparisonCollection


def compare_id_collections(id_collection_1: IDCollection, id_collection_2: IDCollection, name=None) -> IDComparisonCollection:
    comparisons = []
    for id_set1, id_set2 in product(id_collection_1, id_collection_2):
        if id_set1.name == id_set2.name:
            comparisons.append(compare_id_sets(id_set1, id_set2, name=id_set1.name))
        # If not same name, not same type of id so continue
    return IDComparisonCollection(comparisons, name=name)

def compare_id_sets(id_set1: IDSet, id_set2: IDSet, name=None):
    compare_values = _compare_id_sets(id_set1, id_set2)
    return IDComparison(*compare_values, name=name)

def _compare_id_sets(id_set1: IDSet, id_set2: IDSet):
    differences_intersection = _set_differences_and_intersection(id_set1, id_set2)
    return [len(set_) for set_ in differences_intersection]

def _set_differences_and_intersection(id_set1: IDSet, id_set2: IDSet):
    """

    :param id_set1:
    :type id_set1:
    :param id_set2:
    :type id_set2:
    :return: differences, tuple of three sets. First is elements of 1 not in 2, second is elements in both, third is
        elements in 2 that are not in 1
    :rtype: tuple
    """
    return id_set1.difference(id_set2), id_set1.intersection(id_set2), id_set2.difference(id_set1)
