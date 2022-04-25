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

    def __repr__(self):
        return self.name
