from typing import Any
from weakref import WeakSet

from typing_extensions import Protocol


class ILinkedItem(Protocol):
    forward_links: WeakSet
    back_links: WeakSet


class LinkedItem:
    forward_links: WeakSet
    back_links: WeakSet

    def __init__(self):
        self.forward_links = WeakSet()
        self.back_links = WeakSet()
        super().__init__()

    def _add_forward_link(self, item: Any):
        self.forward_links.add(item)

    def _add_back_link(self, item: Any):
        self.back_links.add(item)


