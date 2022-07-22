import pandas as pd
import psycopg2
from psycopg2 import Error

from cube.Cube import Cube
from cube.Cuboid import Cuboid
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


def get_tables_above_column_list_level(column_level):
    result = []
    while column_level != column_level.parent:
        column_level = column_level.parent
        if isinstance(column_level, NonTopLevel):
            result.append(column_level)
    return list(result)


def get_tables_below_column_list_level(column_level):
    result = [column_level]
    while column_level != column_level.child:
        column_level = column_level.child
        result.append(column_level)
    return list(reversed(result))


def get_ancestor_lm_and_values(level_member):
    result = []
    while level_member.parent is not None:
        level_member = level_member.parent
        result.append([level_member.level, level_member])
    return result


def get_ancestor_value_stmt(level_member):
    lm_value_list = get_ancestor_lm_and_values(level_member)
    result = []
    for k, v in lm_value_list:
        if type(v.name) is int:
            result.append(f"{k.name}.{k.level_member_name} IN ({v.name})")
        else:
            result.append(f"{k.name}.{k.level_member_name} IN ('{v.name}')")
    if result:
        return " AND " + " AND ".join(result)
    else:
        return ""


def get_current_value_stmt(column_list):
    column_level = get_table_and_column_name(column_list[0].level)
    value_list = get_list_of_values(column_list)
    values = "(" + ", ".join(value_list) + ")"
    return f" AND {column_level} IN {values}"


class BaseCube(Cube):
    def __init__(
            self,
            fact_table_name,
            dimension_list,
            measure_list,
            name,
            metadata,
            engine,
            previous=None
    ):
        super().__init__(dimension_list, measure_list, engine, previous)
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._condition = None
        self._previous = previous

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def columns(self, value_list):
        return Cuboid(
            self._dimension_list,
            self._measure_list,
            self._engine,
            value_list,
            self
        )

    def rows(self, value_list):
        pass

    def pages(self, value_list):
        pass

    def where(self, *args, **kwargs):
        print(args)
        print(kwargs)

    def output(self, column_list=None):
        above_tables = get_tables_above_column_list_level(column_list[0].level)
        below_tables_including = get_tables_below_column_list_level(column_list[0].level)
        select_stmt = self._get_select_stmt(column_list[0].level, above_tables)
        from_stmt = self._get_from_stmt(above_tables, below_tables_including)
        where_stmt = self._get_where_stmt(below_tables_including, above_tables, column_list)
        group_by_stmt = self._get_group_by_stmt(above_tables, column_list[0].level)
        order_by_stmt = self._get_order_by_stmt(column_list)
        query = construct_query(select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt)
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
        return BaseCube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
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
        return BaseCube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
                        self._engine)

    def _dice(self, condition):
        cube = BaseCube(self._fact_table_name, self._dimension_list, self._measure_list, self.name, self._metadata,
                        self._engine)
        cube._condition = condition
        return cube

    def _get_select_stmt(self, column_level, tables):
        select_table_name = get_table_and_column_name(column_level)
        above_tables = []
        for table in tables:
            above_tables.append(get_table_and_column_name(table))
        above_tables_string = ", ".join(above_tables)
        select_aggregate = f"{self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"
        if above_tables:
            return "SELECT " + select_table_name + ", " + above_tables_string + ", " + select_aggregate
        else:
            return "SELECT " + select_table_name + ", " + select_aggregate

    def _get_from_stmt(self, above_tables, below_tables):
        from_tables = [self._fact_table_name] + list(map(lambda x: x.name, below_tables)) + list(
            map(lambda x: x.name, above_tables))
        return "FROM " + ", ".join(from_tables)

    def _get_where_stmt(self, tables_below, tables_above, column_list):
        table_hierarchy_join = get_hierarchy_table_join_stmt(self._fact_table_name, tables_below + tables_above)
        current_value_str = get_current_value_stmt(column_list)
        ancestor_value_str = get_ancestor_value_stmt(column_list[0])
        return "WHERE " + table_hierarchy_join + current_value_str + ancestor_value_str

    def _get_group_by_stmt(self, above_tables, column_level):
        column_of_interest = get_table_and_column_name(column_level)
        above_columns = []
        for table in above_tables:
            above_columns.append(get_table_and_column_name(table))
        auxiliary_columns = ", ".join(above_columns)
        if auxiliary_columns:
            return f"GROUP BY " + column_of_interest + ", " + auxiliary_columns
        else:
            return f"GROUP BY " + column_of_interest

    def _get_order_by_stmt(self, column_list):
        value_list = get_list_of_values(column_list)
        values = ", ".join(value_list)
        table_column_name = get_table_and_column_name(column_list[0].level)
        return f"ORDER BY array_position(array[{values}], {table_column_name})"

    def execute_query(self, query):
        conn = self._get_new_connection()

        with conn.cursor() as curs:
            curs.execute(query)
            result = curs.fetchall()
        conn.close()
        return result

    def _get_new_connection(self):
        try:
            return psycopg2.connect(user=self._engine.user,
                                    password=self._engine.password,
                                    host=self._engine.host,
                                    port=self._engine.port,
                                    database=self._engine.dbname)
        except (Exception, Error) as error:
            print("ERROR: ", error)
