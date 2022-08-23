from __future__ import annotations
from typing import List, Union, TypeVar, Optional, Dict

import pandas as pd
import psycopg2
from psycopg2 import Error
from psycopg2._psycopg import connection

from cube.Cube import Cube
from cube.CubeOperators import rollup, dice
from cube.Cuboid import Cuboid
from cube.Level import Level
from cube.LevelMember import LevelMember
from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from cube.NonTopLevel import NonTopLevel
from cube.SlicedDimension import SlicedDimension

DataFrame = TypeVar(pd.DataFrame)


def construct_query(select_stmt: str, from_stmt: str, where_stmt: str, group_by_stmt: str,
                    order_by_stmt: str = "") -> str:
    stmt_list: List[str] = [select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt]
    return " ".join(stmt_list) + ";"


def go_to_parent(current_level):
    return current_level.parent


def go_to_child(current_level):
    return current_level.child


def get_hierarchy_up_to_current_level(dimension, level):
    hierarchy = dimension.hierarchies()
    return hierarchy[:hierarchy.index(level) + 1]


def get_fact_table_join_stmt(fact_table_name: str, lowest_level: NonTopLevel) -> str:
    return f"{fact_table_name}.{lowest_level.dimension.fact_table_fk} = {lowest_level.name}.{lowest_level.pk_name}"


def get_hierarchy_table_join_stmt(fact_table_name: str, join_tables: List[NonTopLevel]) -> str:
    hierarchy_table_join: List[str] = [get_fact_table_join_stmt(fact_table_name, join_tables[0])]
    for i in range(0, len(join_tables) - 1):
        hierarchy_table_join.append(
            f"{join_tables[i].name}.{join_tables[i].fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1].pk_name}")

    return " AND ".join(hierarchy_table_join)


def get_list_of_values(column_list: List[LevelMember]) -> List[str]:
    value_list: List[str] = []
    if type(column_list[0].name) is int:
        for column in column_list:
            value_list.append(f"{column.name}")
    else:
        for column in column_list:
            value_list.append(f"'{column.name}'")
    return value_list


def get_table_and_column_name(column_level: NonTopLevel) -> str:
    return f"{column_level.name}.{column_level.level_member_name}"


def format_query_result_to_pandas_df(result) -> DataFrame:
    length: int = len(result[0]) - 1
    values = [list(map(lambda x: x[length], result))]
    columns: List[Union[str, int]] = list(map(lambda x: x[0], result))
    return pd.DataFrame(values, index=[0], columns=columns)


def _get_tables_above_column_list_level(column_level: Level) -> List[NonTopLevel]:
    result: List[NonTopLevel] = []
    while column_level != column_level.parent:
        column_level = column_level.parent
        if isinstance(column_level, NonTopLevel):
            result.append(column_level)
    return list(result)


def get_tables_below_column_list_level(column_level: Level) -> List[NonTopLevel]:
    result: List[NonTopLevel] = [column_level]
    while column_level != column_level.child:
        column_level: NonTopLevel = column_level.child
        result.append(column_level)
    return list(reversed(result))


def get_ancestor_lm_and_values(level_member: LevelMember) -> List[List[Union[NonTopLevel, LevelMember]]]:
    result: List[List[Union[NonTopLevel, LevelMember]]] = []
    while level_member.parent is not None:
        level_member = level_member.parent
        result.append([level_member.level, level_member])
    return result


def get_ancestor_value_stmt(level_member: LevelMember) -> str:
    lm_value_list: List[List[Union[NonTopLevel, LevelMember]]] = get_ancestor_lm_and_values(level_member)
    result: List[str] = []
    for k, v in lm_value_list:
        if type(v.name) is int:
            result.append(f"{k.name}.{k.level_member_name} IN ({v.name})")
        else:
            result.append(f"{k.name}.{k.level_member_name} IN ('{v.name}')")
    if result:
        return " AND " + " AND ".join(result)
    else:
        return ""


def get_current_value_stmt(column_list: List[LevelMember]) -> str:
    column_level: str = get_table_and_column_name(column_list[0].level)
    value_list: List[str] = get_list_of_values(column_list)
    values: str = "(" + ", ".join(value_list) + ")"
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
    ):
        super().__init__(dimension_list, measure_list, engine, previous_cube=None, base_cube=None, next_cube=None)
        self._fact_table_name = fact_table_name
        self._dimension_list = dimension_list
        self._name = name
        self._metadata = metadata
        self._condition = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def columns(self, value_list) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            dimension_name: Union[str, int] = value_list[0].level.dimension.name
            level_name: Union[str, int] = value_list[0].level.name
            kwargs: Dict[Union[str, int], Union[str, int]] = {dimension_name: level_name}
            cube1: Cuboid = rollup(self, **kwargs)
            cube2: Cuboid = dice(cube1, value_list, "column")
            cube2.visual_column = value_list[0].level
            self.next_cube: Cuboid = cube2
            return cube2

    def rows(self, value_list) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            dimension_name: Union[str, int] = value_list[0].level.dimension.name
            level_name: Union[str, int] = value_list[0].level.name
            kwargs: Dict[Union[str, int], Union[str, int]] = {dimension_name: level_name}
            cube1: Cuboid = rollup(self, **kwargs)
            cube2: Cuboid = dice(cube1, value_list, "row")
            cube2.visual_row = value_list[0].level
            self.next_cube: Cuboid = cube2
            return cube2

    def pages(self, value_list):
        pass

    def where(self, *args, **kwargs):
        print(args)
        print(kwargs)

    def output(self) -> DataFrame:
        above_tables: List[NonTopLevel] = _get_tables_above_column_list_level(self.next_cube.visual_column)
        below_tables_including: List[NonTopLevel] = get_tables_below_column_list_level(self.next_cube.visual_column)
        select_stmt: str = self._get_select_stmt(self.next_cube)
        from_stmt: str = self._get_from_stmt(above_tables, below_tables_including)
        where_stmt: str = self._get_where_stmt(below_tables_including, above_tables, self.next_cube.column_value_list)
        group_by_stmt: str = self._get_group_by_stmt(above_tables, self.next_cube.visual_column)
        order_by_stmt: str = self._get_order_by_stmt(self.next_cube.column_value_list)
        query: str = construct_query(select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt)
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
        cube = BaseCube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
                        self._engine)
        cube.base_cube = cube
        return cube

    def _slice(self, dimension, value):
        sliced_dimension = SlicedDimension(dimension.name, dimension.level_list)
        sliced_dimension.fixed_level = dimension.current_level
        sliced_dimension.fixed_level_member = value
        new_dimension_list = [sliced_dimension if x == dimension else x for x in self._dimension_list]
        cube = BaseCube(self._fact_table_name, new_dimension_list, self._measure_list, self.name, self._metadata,
                        self._engine)
        cube.base_cube = cube
        return cube

    def _get_select_stmt(self, next_cube: Cube) -> str:
        column_level = self.next_cube.visual_column
        select_table_name: str = get_table_and_column_name(column_level)
        tables: List[NonTopLevel] = _get_tables_above_column_list_level(self.next_cube.visual_column)
        above_tables: List[str] = []

        # for table in tables:
        #     above_tables.append(get_table_and_column_name(table))

        above_tables_string: str = ", ".join(above_tables)
        if next_cube.use_temp_measure:
            select_aggregate: str = f"{next_cube.temp_measure.aggregate_function.name}({self._fact_table_name}.{next_cube.temp_measure.name})"
            next_cube.use_temp_measure = False
        else:
            select_aggregate: str = f"{self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"
        if above_tables:
            return "SELECT " + select_table_name + ", " + above_tables_string + ", " + select_aggregate
        else:
            return "SELECT " + select_table_name + ", " + select_aggregate

    def _get_from_stmt(self, above_tables: List[NonTopLevel], below_tables: List[NonTopLevel]) -> str:
        from_tables = [self._fact_table_name] + list(map(lambda x: x.name, below_tables)) + list(
            map(lambda x: x.name, above_tables))
        return "FROM " + ", ".join(from_tables)

    def _get_where_stmt(self, tables_below: List[NonTopLevel], tables_above: List[NonTopLevel],
                        column_list: List[LevelMember]) -> str:
        table_hierarchy_join: str = get_hierarchy_table_join_stmt(self._fact_table_name, tables_below + tables_above)
        current_value: str = get_current_value_stmt(column_list)
        ancestor_value: str = get_ancestor_value_stmt(column_list[0])
        return "WHERE " + table_hierarchy_join + current_value + ancestor_value

    def _get_group_by_stmt(self, above_tables: List[NonTopLevel], column_level: NonTopLevel) -> str:
        column_of_interest: str = get_table_and_column_name(column_level)
        above_columns: List[str] = []
        for table in above_tables:
            above_columns.append(get_table_and_column_name(table))
        auxiliary_columns: str = ", ".join(above_columns)
        if auxiliary_columns:
            return f"GROUP BY " + column_of_interest + ", " + auxiliary_columns
        else:
            return f"GROUP BY " + column_of_interest

    def _get_order_by_stmt(self, column_list: List[LevelMember]) -> str:
        value_list: List[str] = get_list_of_values(column_list)
        values: str = ", ".join(value_list)
        table_column_name: str = get_table_and_column_name(column_list[0].level)
        return f"ORDER BY array_position(array[{values}], {table_column_name})"

    def execute_query(self, query: str):
        conn: connection = self._get_new_connection()

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

    def __repr__(self):
        return self.name
