
class NamedBase:

    def __repr__(self):
        cls_name = self.__class__.__name__
        return f'<{cls_name}(name={self.name}, items={[item for item in self]})>'

class NamedSet(NamedBase, set):

    def __init__(self, iterable=None, name=None):
        self.name = name
        super().__init__(iterable)


class NamedList(NamedBase, list):

    def __init__(self, iterable=None, name=None):
        self.name = name
        super().__init__(iterable)