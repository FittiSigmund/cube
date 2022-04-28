from cube.Dimension import Dimension


class SlicedDimension(Dimension):
    def __init__(self, name, level_list):
        super().__init__(name, level_list)
        self.fixed_level = None
        self.fixed_level_member = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


