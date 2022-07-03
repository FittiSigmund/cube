from abc import ABC, abstractmethod


class Interface(ABC):
    pass


class CubeInterface(Interface):
    # @abstractmethod
    # def drill_out(self, dimension_name):
    #     pass

    @abstractmethod
    def columns(self, column):
        pass

    @abstractmethod
    def rows(self, row):
        pass

    @abstractmethod
    def where(self, **kwargs):
        pass


class DimensionInterface(Interface):
    @abstractmethod
    def roll_up(self):
        pass

    @abstractmethod
    def drill_down(self):
        pass
