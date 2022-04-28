from abc import ABC


class Dimension(ABC):
    def __init__(self, name, level_list):
        self._name = name
        self.level_list = level_list

    @property
    def name(self):
        return self._name
