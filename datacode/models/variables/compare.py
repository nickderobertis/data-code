from typing import Callable


def functions_are_equal(func: Callable, func2: Callable) -> bool:
    return func.__code__.co_code == func2.__code__.co_code
