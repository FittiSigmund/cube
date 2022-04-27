from cube.NonTopLevel import NonTopLevel


def _construct_query(select_stmt, from_stmt):
    return select_stmt + " " + from_stmt + ";"


class Cube:
    def __init__(self, fact_table_name, dimension_list, measure_list, name, metadata):
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._columns = []
        self._cursor = None
        if measure_list:
            self._default_measure = measure_list[0]
            self._measure_list = measure_list
        else:
            print("No measures! (Exception handling has not been implemented yet)")
        for dimension in dimension_list:
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

    def where(self, slicer):
        pass

    def output(self):
        dimensions = self.dimensions()
        current_levels = map(lambda x: (x, x.current_level), dimensions)
        current_non_top_levels = list(filter(lambda x: isinstance(x[1], NonTopLevel), current_levels))
        print(current_non_top_levels)

        select_stmt = self._get_select_stmt()
        from_stmt = self._get_from_stmt()
        query = _construct_query(select_stmt, from_stmt)
        return self.execute_query(query)

    def measures(self):
        return self._measure_list

    def dimensions(self):
        return self._dimension_list

    def _get_select_stmt(self):
        return f"SELECT {self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"

    def _get_from_stmt(self):
        return f"FROM {self._fact_table_name}"

    def execute_query(self, query):
        self._cursor.execute(query)
        return self._cursor.fetchall()
