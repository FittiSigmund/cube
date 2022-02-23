from Enums import Backend
from Interface import CubeInterface, DimensionInterface, Interface


class Level:
    def __init__(self, name):
        self._name = name

    def members(self):
        raise NotImplementedError

    @property
    def name(self):
        return self._name


class Hierarchy:
    def __init__(self, levels):
        self.levels = levels

    def go_up(self):
        pass

    def go_down(self):
        pass

    def get_current_level(self):
        raise NotImplementedError

    def __str__(self):
        level_names = list(map(lambda x: x.name, self.levels))
        return ' -> '.join(level_names)


class Dimension:
    def __init__(self, name, hierarchy, level_list):
        self.name = name
        self._hierarchy = hierarchy
        for level in level_list:
            setattr(self, level.name, level)

    def roll_up(self):
        pass

    def drill_down(self):
        pass

    # @property
    # def hierarchy(self):
    #     pass

    def __str__(self):
        return f"Dimension Name: {self.name}, Hierarchy: {self._hierarchy}, \n Class Attributes: {self.__dict__.keys()}"


class Measure:
    def __init__(self, name):
        self.name = name


class Cube:
    def __init__(self, dimension_list):
        self.dimension_list = dimension_list
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)

    def columns(self, value_set):
        pass

    def rows(self, value_set):
        pass

    def where(self, slicer):
        pass

