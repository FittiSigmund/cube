from Enums import Backend
from Interface import CubeInterface, DimensionInterface, Interface


class Level:
    def __init__(self, name):
        self._name = name
        # Initialize parent to None first in order to set it later using the parent.setter
        # If I didn't do this, I would have a never ending chain of Level initializations
        self._parent = None

    def members(self):
        raise NotImplementedError

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        self._parent = value


class Dimension:
    def __init__(self, name, level_list):
        self.name = name
        self._lowest_level = level_list[0]
        for level in level_list:
            setattr(self, level.name, level)

    def roll_up(self):
        pass

    def drill_down(self):
        pass

    @property
    def lowest_level(self):
        return self._lowest_level

    # @property
    # def hierarchy(self):
    #     pass

    def __str__(self):
        return f"Dimension Name: {self.name}, Class Attributes: {self.__dict__.keys()}"


class AggregateFunction:
    def __init__(self, name, function):
        self.__name = name
        self.__function = function

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name

    def eval(self, *args):
        return self.__function(*args)


class Measure:
    def __init__(self, name, function):
        self.__name = name
        self.__aggregate_function = function

    def aggregate(self, *args):
        return self.__aggregate_function.eval(*args)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name

    @property
    def aggregate_function(self):
        return self.__aggregate_function


class Cube:
    def __init__(self, dimension_list, measure_list, name):
        self.__dimension_list = dimension_list
        self.__measure_list = measure_list
        self.__name = name
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name

    def columns(self, value_set):
        pass

    def rows(self, value_set):
        pass

    def where(self, slicer):
        pass

    def measures(self):
        return self.__measure_list
