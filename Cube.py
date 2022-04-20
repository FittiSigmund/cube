class Dimension:
    def __init__(self, name, level_list):
        self.__name = name
        self.__lowest_level = level_list[0]
        for level in level_list:
            setattr(self, level.name, level)

    @property
    def name(self):
        return self.__name

    def roll_up(self):
        pass

    def drill_down(self):
        pass

    @property
    def lowest_level(self):
        return self.__lowest_level

    def hierarchies(self):
        current_level = self.lowest_level
        hierarchy = [current_level]
        while current_level != current_level.parent:
            current_level = current_level.parent
            hierarchy.append(current_level)
        return list(map(lambda x: x.name, hierarchy))


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
        return list(map(lambda x: x.name, self.__measure_list))

    def dimensions(self):
        return list(map(lambda x: x.name, self.__dimension_list))
