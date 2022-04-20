class Dimension:
    def __init__(self, name, level_list):
        self.__name = name
        self.__lowest_level = level_list[0]
        for level in level_list:
            setattr(self, level.name, level)

    @property
    def name(self):
        return self.__name

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


