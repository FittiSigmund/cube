from abc import ABC

from cube import RegularDimension


class Level(ABC):
    def __init__(self, name: str, parent, child, dimension: RegularDimension):
        self._name: str = name
        self.table_name: str = name
        self._parent = parent
        self._child = child
        self._dimension: RegularDimension = dimension

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        self._parent = value

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, value):
        self._child = value

    @property
    def dimension(self):
        return self._dimension

    @dimension.setter
    def dimension(self, value):
        self._dimension = value
