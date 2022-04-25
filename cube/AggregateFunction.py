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

    def __repr__(self):
        return self.name
