from abc import ABC


class Level(ABC):
    def __init__(self, name, parent, child, dimension):
        self._name = name
        self._parent = parent
        self._child = child
        self.dimension = dimension

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
