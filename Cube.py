from Enums import Backend
from Interface import CubeInterface, DimensionInterface, Interface


class Level:
    def __init__(self, name, backend):
        self._name = name
        if backend == Backend.MONDRIAN.name:
            self.level = MondrianLevel(name)

    def members(self):
        raise NotImplementedError

    @property
    def name(self):
        return self._name


class Hierarchy:
    def __init__(self, levels, backend):
        if backend == Backend.MONDRIAN.name:
            self.hierarchy = MondrianHierarchy(levels)
        self.levels = levels

    def go_up(self):
        self.hierarchy.go_up()

    def go_down(self):
        self.hierarchy.go_down()

    def get_current_level(self):
        raise NotImplementedError


class Dimension:
    def __init__(self, name, hierarchy, level_list, backend):
        self.name = name
        self._hierarchy = hierarchy
        self.level_list = level_list
        for level in level_list:
            setattr(self, level.name, level)
        if backend == Backend.MONDRIAN.name:
            self.dimensionModel = MondrianDimension(name, hierarchy)

    def roll_up(self):
        self.dimensionModel.roll_up()

    def drill_down(self):
        self.dimensionModel.drill_down()

    @property
    def hierarchy(self):
        pass


class Measure:
    def __init__(self, name):
        self.name = name


class Cube:
    def __init__(self, dimension_list, active_columns, active_row_dimensions, backend):
        self.dimension_list = dimension_list
        self.active_columns = active_columns
        self.active_row_dimensions = active_row_dimensions
        for dimension in dimension_list:
            setattr(self, dimension.name, dimension)
        if backend == Backend.MONDRIAN.name:
            self.cubeModel = MondrianCube(dimension_list, active_columns, active_row_dimensions)

    # def drill_out(self, dimension_name):
    #     return self.cubeModel.drill_out(dimension_name)

    # def get_cell_value(self, dimension_name_list):
    #     raise NotImplementedError

    def columns(self, column):
        return self.cubeModel.columns(column)

    def rows(self, row):
        return self.cubeModel.rows(row)

    def where(self, slicer):
        return self.cubeModel.where(slicer)

    def __str__(self):
        return "Active Columns : {0}\n Active Row Dimensions: {1}\n Class: {2}" \
            .format(
                self.cubeModel.active_columns,
                self.cubeModel.active_rows,
                self.__class__
            )


class MondrianCube(CubeInterface):
    def __init__(self, dimension_list, active_columns, active_rows):
        self.dimension_list = dimension_list
        self.active_columns = active_columns
        self.active_rows = active_rows
        for dimension in dimension_list:
            setattr(self, dimension.name, MondrianDimension.from_dimension(dimension))

    # def drill_out(self, dimension_name):
    #     return MondrianCube(self.dimension_list, self.active_column_dimensions + [dimension_name])

    def columns(self, column):
        return Cube(self.dimension_list, [column], self.active_rows, Backend.MONDRIAN.name)

    def rows(self, row):
        return Cube(self.dimension_list, self.active_columns, [row], Backend.MONDRIAN.name)

    def where(self, slicer):
        return Cube(self.dimension_list, self.active_columns, self.active_rows, Backend.MONDRIAN.name)

    def __str__(self):
        dimension_list = [x.name for x in self.dimension_list]
        return "Active Column Dimensions: {0}\n Active Row Dimensions: {1}" \
            .format(
                str(self.active_columns),
                str(self.active_rows),
                str(dimension_list)
            )


class MondrianDimension(DimensionInterface):
    @classmethod
    def from_dimension(cls, dimension):
        cls.name = dimension.name
        cls.hierarchy = dimension.hierarchy

    def __init__(self, name, hierarchy):
        self.name = name
        self.hierarchy = hierarchy

    def roll_up(self):
        print("rollUp")

    def drill_down(self):
        print("drillDown")


class MondrianHierarchy(Interface):
    def __init__(self, levels):
        self.levels = levels
        self.currentActiveIndex = 0
        self.currentActive = self.levels[self.currentActiveIndex]

    def go_up(self):
        if self.currentActiveIndex < 1:
            self.currentActive = self.levels[self.currentActiveIndex]
        else:
            self.currentActive = self.levels[self.currentActiveIndex - 1]
            self.currentActiveIndex -= 1

    def go_down(self):
        if self.currentActiveIndex == len(self.levels):
            self.currentActive = self.levels[self.currentActiveIndex]
        else:
            self.currentActive = self.levels[self.currentActiveIndex + 1]
            self.currentActiveIndex += 1


class MondrianLevel(Interface):
    def __init__(self, name):
        self.name = name
