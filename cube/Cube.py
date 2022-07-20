import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import Error

from cube.RegularDimension import RegularDimension
from cube.NonTopLevel import NonTopLevel
from cube.SlicedDimension import SlicedDimension


def construct_query(select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt):
    stmt_list = [select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt]
    return " ".join(stmt_list) + ";"


def go_to_parent(current_level):
    return current_level.parent


def go_to_child(current_level):
    return current_level.child


def get_hierarchy_up_to_current_level(dimension, level):
    hierarchy = dimension.hierarchies()
    return hierarchy[:hierarchy.index(level) + 1]


def get_fact_table_join_stmt(fact_table_name, lowest_level):
    return [
        f"{fact_table_name}.{lowest_level.dimension.fact_table_fk} = {lowest_level.name}.{lowest_level.pk_name}"
    ]


def get_hierarchy_table_join_stmt(fact_table_name, join_tables):
    hierarchy_table_join = get_fact_table_join_stmt(fact_table_name, join_tables[0])
    for i in range(0, len(join_tables) - 1):
        hierarchy_table_join.append(
            f"{join_tables[i].name}.{join_tables[i]._fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1]._pk_name}")

    return " AND ".join(hierarchy_table_join)


def get_list_of_values(column_list):
    value_list = []
    if type(column_list[0].name) is int:
        for column in column_list:
            value_list.append(f"{column.name}")
    else:
        for column in column_list:
            value_list.append(f"'{column.name}'")
    return value_list


def get_table_and_column_name(column_level):
    return f"{column_level.name}.{column_level.level_member_name}"


def format_query_result_to_pandas_df(result):
    length = len(result[0]) - 1
    values = [list(map(lambda x: x[length], result))]
    columns = list(map(lambda x: x[0], result))
    return pd.DataFrame(values, index=[0], columns=columns)


class Cube:
    def __init__(
            self,
            fact_table_name,
            dimension_list,
            measure_list,
            name,
            metadata,
            engine,
            columns=None,
            previous=None
    ):
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._engine = engine
        self._columns = []
        self._cursor = self._get_new_cursor()
        self._condition = None
        self._column_list = columns
        self._previous = previous
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
        return Cube(
            self._fact_table_name,
            self._dimension_list,
            self._measure_list,
            self.name,
            self._metadata,
            self._engine,
            columns=value_list,
            previous=self
        )

    def rows(self, value_list):
        pass

    def where(self, *args, **kwargs):
        print(args)
        print(kwargs)

    ## TODO: Make a decision on how to return the result (xarray, pandas, etc.)
    def output(self):
        above_tables = self._get_tables_above_column_list_level()
        below_tables_including = self._get_tables_below_column_list_level()
        select_stmt = self._get_select_stmt(above_tables)
        from_tables = [self._fact_table_name] + list(map(lambda x: x.name, below_tables_including)) + list(map(lambda x: x.name, above_tables))
        from_stmt = self._get_from_stmt(from_tables)
        where_stmt = self._get_where_stmt(below_tables_including, above_tables)
        group_by_stmt = self._get_group_by_stmt(above_tables)
        order_by_stmt = self._get_order_by_stmt()
        query = construct_query(select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt)
        print(query)
        query_result = self.execute_query(query)
        return format_query_result_to_pandas_df(query_result)

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
        return Cube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
                    self._engine)

    def _roll_up(self, dimension):
        return self._traverse_hierarchy(dimension, go_to_parent)

    def _drill_down(self, dimension):
        return self._traverse_hierarchy(dimension, go_to_child)

    def _slice(self, dimension, value):
        sliced_dimension = SlicedDimension(dimension.name, dimension.level_list)
        sliced_dimension.fixed_level = dimension.current_level
        sliced_dimension.fixed_level_member = value
        new_dimension_list = [sliced_dimension if x == dimension else x for x in self._dimension_list]
        return Cube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
                    self._engine)

    def _dice(self, condition):
        cube = Cube(self._fact_table_name, self._dimension_list, self._measure_list, self.name, self._metadata,
                    self._engine)
        cube._condition = condition
        return cube

    def _get_select_stmt(self, tables):
        select_table_name = get_table_and_column_name(self._column_list[0].level)
        above_tables = []
        for table in tables:
            above_tables.append(get_table_and_column_name(table))
        above_tables_string = ", ".join(above_tables)
        select_aggregate = f"{self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"
        if above_tables:
            return "SELECT " + select_table_name + ", " + above_tables_string + ", " + select_aggregate
        else:
            return "SELECT " + select_table_name + ", " + select_aggregate

    def _get_tables_below_column_list_level(self):
        column_level = self._column_list[0].level
        result = [column_level]
        while column_level != column_level.child:
            column_level = column_level.child
            result.append(column_level)
        return list(reversed(result))

    def _get_tables_above_column_list_level(self):
        column_level = self._column_list[0].level
        result = []
        while column_level != column_level.parent:
            column_level = column_level.parent
            if isinstance(column_level, NonTopLevel):
                result.append(column_level)
        return list(result)

    def _get_from_stmt(self, from_tables):
        return "FROM " + ", ".join(from_tables)

    def _get_where_stmt(self, tables_below, tables_above):
        table_hierarchy_join = get_hierarchy_table_join_stmt(self._fact_table_name, tables_below + tables_above)
        column_level = get_table_and_column_name(self._column_list[0].level)
        value_list = get_list_of_values(self._column_list)
        values = "(" + ", ".join(value_list) + ")"
        result = "WHERE " + table_hierarchy_join + " AND " + column_level + " IN " + values
        return result

    def _get_group_by_stmt(self, above_tables):
        column_of_interest = get_table_and_column_name(self._column_list[0].level)
        above_columns = []
        for table in above_tables:
            above_columns.append(get_table_and_column_name(table))
        auxiliary_columns = ", ".join(above_columns)
        if auxiliary_columns:
            return f"GROUP BY " + column_of_interest + ", " + auxiliary_columns
        else:
            return f"GROUP BY " + column_of_interest

    def _get_order_by_stmt(self):
        value_list = get_list_of_values(self._column_list)
        values = ", ".join(value_list)
        table_column_name = get_table_and_column_name(self._column_list[0].level)
        return f"ORDER BY array_position(array[{values}], {table_column_name})"

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
