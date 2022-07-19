class Measure:
    def __init__(self, name, function):
        self._name = name
        self._aggregate_function = function

    def aggregate(self, *args):
        return self._aggregate_function.eval(*args)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def aggregate_function(self):
        return self._aggregate_function

    def __repr__(self):
        return self.name
