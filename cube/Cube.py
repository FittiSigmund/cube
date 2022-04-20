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

    def output(self):
        pass

    def measures(self):
        return list(map(lambda x: x.name, self.__measure_list))

    def dimensions(self):
        return list(map(lambda x: x.name, self.__dimension_list))
