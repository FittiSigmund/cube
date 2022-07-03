import psycopg2
from psycopg2 import Error

from cube.RegularDimension import RegularDimension
from cube.NonTopLevel import NonTopLevel
from cube.SlicedDimension import SlicedDimension


def construct_query(select_stmt, from_stmt):
    return select_stmt + " " + from_stmt + ";"


def go_to_parent(current_level):
    return current_level.parent


def go_to_child(current_level):
    return current_level.child


def get_hierarchy_up_to_current_level(dimension, level):
    hierarchy = dimension.hierarchies()
    return hierarchy[:hierarchy.index(level) + 1]


class Cube:
    def __init__(self, fact_table_name, dimension_list, measure_list, name, metadata, engine):
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._engine = engine
        self._columns = []
        self._cursor = self._get_new_cursor()
        self._condition = None
        if measure_list:
            self._default_measure = measure_list[0]
            self._measure_list = measure_list
        else:
            print("No measures! (Exception handling has not been implemented yet)")
        for dimension in dimension_list:
            if isinstance(dimension, RegularDimension):
                dimension.metadata = self._metadata
            setattr(self, dimension.name, dimension)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, cursor):
        self._cursor = cursor

    def columns(self, value_list):
        self._columns = value_list

    def rows(self, value_list):
        pass

    def where(self, *args, **kwargs):
        print(args)
        print(kwargs)

    ## Half way through implementing output when the date dimension has been drilled down to the year level
    ## TODO: Make a decision on how to return the result (xarray, pandas, etc.)
    def output(self):
        dimensions = self.dimensions()
        current_levels = [(x, x.current_level) for x in dimensions if isinstance(x.current_level, NonTopLevel)]
        if current_levels:
            up_to_current_level = get_hierarchy_up_to_current_level(current_levels[0][0], current_levels[0][1])
        select_stmt = self._get_select_stmt()
        from_stmt = self._get_from_stmt()
        query = construct_query(select_stmt, from_stmt)
        return self.execute_query(query)

    def measures(self):
        return self._measure_list

    def dimensions(self):
        return self._dimension_list

    def _traverse_hierarchy(self, dimension, direction):
        if dimension in self._dimension_list:
            new_dimension = RegularDimension(dimension.name, dimension.level_list, dimension.engine,
                                             dimension.fact_table_fk)
        else:
            raise NotImplementedError("dimension not found in dimension list")

        new_dimension.current_level = direction(dimension.current_level)
        new_dimension_list = [new_dimension if x == dimension else x for x in self._dimension_list]
        return Cube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata, self._engine)

    def _roll_up(self, dimension):
        return self._traverse_hierarchy(dimension, go_to_parent)

    def _drill_down(self, dimension):
        return self._traverse_hierarchy(dimension, go_to_child)

    def _slice(self, dimension, value):
        sliced_dimension = SlicedDimension(dimension.name, dimension.level_list)
        sliced_dimension.fixed_level = dimension.current_level
        sliced_dimension.fixed_level_member = value
        new_dimension_list = [sliced_dimension if x == dimension else x for x in self._dimension_list]
        return Cube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata, self._engine)

    def _dice(self, condition):
        cube = Cube(self._fact_table_name, self._dimension_list, self._measure_list, self.name, self._metadata, self._engine)
        cube._condition = condition
        return cube

    def _get_select_stmt(self):
        return f"SELECT {self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"

    def _get_from_stmt(self):
        return f"FROM {self._fact_table_name}"

    def execute_query(self, query):
        self._cursor.execute(query)
        return self._cursor.fetchall()

    def _get_new_cursor(self):
        try:
            connection = psycopg2.connect(user=self._engine.user,
                                          password=self._engine.password,
                                          host=self._engine.host,
                                          port=self._engine.port,
                                          database=self._engine.dbname)
            return connection.cursor()
        except (Exception, Error) as error:
            print("ERROR: ", error)
