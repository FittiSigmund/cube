from __future__ import annotations

import copy
from collections import deque
from typing import List, Union, TypeVar, Optional, Dict, Any, Tuple, Set, Deque

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
from cube.TopLevel import TopLevel

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


def get_list_of_values(lms: List[LevelMember]) -> List[str]:
    value_list: List[str] = []
    if type(lms[0].name) is int:
        for column in lms:
            value_list.append(f"{column.name}")
    else:
        for column in lms:
            value_list.append(f"'{column.name}'")
    return value_list


def get_table_and_column_name(column_level: NonTopLevel) -> str:
    return f"{column_level.name}.{column_level.level_member_name}"


def _fill_in_missing_values_for_df(values: Deque[Tuple[Any, ...]],
                                   columns: List[str | int],
                                   rows: List[str | int],
                                   length: int) -> List[List[float]]:
    values_with_missing: List[List[float]] = []
    for row in rows:
        row_value = []
        for column in columns:
            if values and (column and row in values[0]):
                row_value.append(values.popleft()[length])
            else:
                row_value.append(None)
        values_with_missing.append(row_value)
    return values_with_missing


def get_tables_above(level: Level) -> List[NonTopLevel]:
    result: List[NonTopLevel] = []
    while level != level.parent:
        level = level.parent
        if isinstance(level, NonTopLevel):
            result.append(level)
    return list(result)


def get_tables_below_including(level: Level) -> List[NonTopLevel]:
    result: List[NonTopLevel] = [level]
    while level != level.child:
        level: NonTopLevel = level.child
        result.append(level)
    return list(reversed(result))


def get_ancestor_lm_and_values(lm_list: List[LevelMember]) -> List[Tuple[NonTopLevel, List[LevelMember], bool]]:
    result: List[Tuple[NonTopLevel, List[LevelMember], bool]] = []
    anc_amount: int = 0
    parent_lm: LevelMember = lm_list[0].parent
    while parent_lm is not None:
        anc_amount += 1
        parent_lm: LevelMember = parent_lm.parent
    for i in range(0, anc_amount):
        lms: List[LevelMember] = []
        level: NonTopLevel | None = None
        for lm in lm_list:
            lm: LevelMember = lm.parent
            level: NonTopLevel = lm.level
            is_int: bool = True if type(lm.name) is int else False
            lms.append(lm)
        result.append((level, lms, is_int))
    return result


def get_ancestor_value_stmt(level_member: List[LevelMember]) -> str:
    lm_value_list: List[Tuple[NonTopLevel, List[LevelMember], bool]] = get_ancestor_lm_and_values(level_member)
    result: List[str] = []
    for k, v, is_int in lm_value_list:
        values = ", ".join(list(map(lambda x: str(x.name), v)))
        if is_int:
            result.append(f"{k.name}.{k.level_member_name} IN ({values})")
        else:
            result.append(f"{k.name}.{k.level_member_name} IN ('{values}')")
    if result:
        return " AND ".join(result)
    else:
        return ""


def get_current_value_stmt(value_list: List[LevelMember]) -> str:
    column_level: str = get_table_and_column_name(value_list[0].level)
    value_list: List[str] = get_list_of_values(value_list)
    values: str = "(" + ", ".join(value_list) + ")"
    return f" {column_level} IN {values}"


def _get_all_value_list(value_list: List[LevelMember]) -> List[LevelMember]:
    if value_list[0].level.all_lm_loaded:
        return value_list
    result: List[List[LevelMember]] = []
    for value in value_list:
        original_value = value
        while value.parent:
            value = value.parent
        if isinstance(value.level.parent, TopLevel):
            result.append([original_value])
        else:
            tmp: List[LevelMember] = []
            parents: List[NonTopLevel] = []
            level = value.level.parent
            while not isinstance(level, TopLevel):
                parents.append(level)
                level = level.parent
            parents = list(reversed(parents))
            parent_lms: List[LevelMember] = parents[0].members()
            for i in range(1, len(parents)):
                tmp: List[List[LevelMember]] = list(map(lambda x: x.children, parent_lms))
                parent_lms = [item for sublist in tmp for item in sublist]

            for lm in parent_lms:
                try:
                    level_member: LevelMember = lm[value.name]
                    tmp.append(level_member)
                except AttributeError:
                    continue
            result.append(tmp)

    return [x for value in result for x in value]


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
        super().__init__(dimension_list, measure_list, engine, base_cube=None, next_cube=None)
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

    def columns(self, value_list: List[LevelMember]) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            value_list: List[LevelMember] = _get_all_value_list(value_list)
            c = Cuboid(self.dimension_list,
                       self.measure_list,
                       self.engine,
                       self,
                       visual_column=value_list[0].level,
                       column_value_list=value_list)
            c.previous = self
            return c

    def rows(self, value_list: List[LevelMember]) -> Cuboid:
        if not value_list:
            raise ValueError("Value_list cannot be empty")
        else:
            c = Cuboid(self.dimension_list,
                       self.measure_list,
                       self.engine,
                       self,
                       visual_row=value_list[0].level,
                       row_value_list=value_list)
            c.previous = self
            return c

    def pages(self, value_list):
        pass

    def where(self, *args, **kwargs):
        print(args)
        print(args[0])
        print(kwargs)

    def output(self) -> DataFrame:
        select_stmt: str = self._get_select_stmt()
        from_stmt: str = self._get_from_stmt()
        where_stmt: str = self._get_where_stmt()
        group_by_stmt: str = self._get_group_by_stmt()
        order_by_stmt: str = self._get_order_by_stmt()
        query: str = construct_query(select_stmt, from_stmt, where_stmt, group_by_stmt, order_by_stmt)
        query_result: List[Tuple[Any, ...]] = self.execute_query(query)
        return self._format_query_result_to_pandas_df(query_result)

    def measures(self):
        return self._measure_list

    def dimensions(self):
        return self._dimension_list

    def _format_query_result_to_pandas_df(self, result: List[Tuple[Any, ...]]) -> DataFrame:
        length: int = len(result[0]) - 1
        columns: List[str | int] = list(map(lambda x: x.name, self.next_cube.column_value_list))
        if length >= 2:
            rows: List[str | int] = list(map(lambda x: x.name, self.next_cube.row_value_list))
            if len(columns) * len(rows) > len(result):
                values: List[List[float]] = _fill_in_missing_values_for_df(deque(result), columns, rows, length)
                return pd.DataFrame(values, index=rows, columns=columns)
            else:
                values: List[List[float]] = [list(map(lambda x: x[length], result))]
                return pd.DataFrame(values, index=rows, columns=columns)
        else:
            values: List[List[float]] = [list(map(lambda x: x[length], result))]
            return pd.DataFrame(values, index=[0], columns=columns)



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

    def _get_column_name_if_exists(self) -> str:
        return get_table_and_column_name(self.next_cube.visual_column) if self.next_cube.visual_column else ""

    def _get_row_name_if_exists(self) -> str:
        return get_table_and_column_name(self.next_cube.visual_row) if self.next_cube.visual_row else ""

    def _get_select_stmt(self) -> str:
        column_name: str = self._get_column_name_if_exists()
        row_name: str = self._get_row_name_if_exists()
        metadata_name: str = ", ".join(list(filter(lambda x: x != "", [column_name, row_name])))

        # tables: List[NonTopLevel] = _get_tables_above_column_list_level(self.next_cube.visual_column)

        if self.next_cube.use_temp_measure:
            select_aggregate: str = f"{self.next_cube.temp_measure.aggregate_function.name}({self._fact_table_name}.{self.next_cube.temp_measure.name})"
            self.next_cube.use_temp_measure = False
        else:
            select_aggregate: str = f"{self._default_measure.aggregate_function.name}({self._fact_table_name}.{self._default_measure.name})"
        return "SELECT " + metadata_name + ", " + select_aggregate

    def _get_from_row_join_conditions(self) -> Dict[str, str]:
        if self.next_cube.visual_row:
            join_tables: List[NonTopLevel] = \
                get_tables_below_including(self.next_cube.visual_row) \
                + get_tables_above(self.next_cube.visual_row)
            hierarchy_table_join: Dict[str, str] = {join_tables[0].name: get_fact_table_join_stmt(self._fact_table_name, join_tables[0])}
            for i in range(0, len(join_tables) - 1):
                hierarchy_table_join[join_tables[i + 1].name] = f"{join_tables[i].name}.{join_tables[i].fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1].pk_name}"
            return hierarchy_table_join
        else:
            return {}

    def _get_from_column_join_conditions(self) -> Dict[str, str]:
        if self.next_cube.visual_column:
            join_tables: List[NonTopLevel] = \
                get_tables_below_including(self.next_cube.visual_column) \
                + get_tables_above(self.next_cube.visual_column)
            hierarchy_table_join: Dict[str, str] = {join_tables[0].name: get_fact_table_join_stmt(self._fact_table_name, join_tables[0])}
            for i in range(0, len(join_tables) - 1):
                hierarchy_table_join[join_tables[i + 1].name] = f"{join_tables[i].name}.{join_tables[i].fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1].pk_name}"

            return hierarchy_table_join
        else:
            return {}

    def _get_from_column_names(self) -> str:
        col_name_and_cond: Dict[str, str] = self._get_from_column_join_conditions()
        join_names: List[str] = []
        for k, v in col_name_and_cond.items():
            join_names.append(f"{k} ON {v}")
        if join_names:
            return "JOIN " + " JOIN ".join(join_names)
        else:
            return ""

    def _get_from_row_names(self) -> str:
        row_name_and_cond: Dict[str, str] = self._get_from_row_join_conditions()
        join_names: List[str] = []
        for k, v in row_name_and_cond.items():
            join_names.append(f"{k} ON {v}")
        if join_names:
            return "JOIN " + " JOIN ".join(join_names)
        else:
            return ""

    def _get_from_stmt(self) -> str:
        fact_table: str = self._fact_table_name
        column_names: str = self._get_from_column_names()
        row_names: str = self._get_from_row_names()
        result: str = "FROM " + fact_table
        if column_names and row_names:
            return result + " " + column_names + " " + row_names
        if column_names:
            return result + " " + column_names
        if row_names:
            return result + " " + row_names

    def _get_column_hierarchy_join(self) -> str:
        if self.next_cube.visual_column:
            join_tables = get_tables_below_including(self.next_cube.visual_column) \
                          + get_tables_above(self.next_cube.visual_column)
            hierarchy_table_join: List[str] = [get_fact_table_join_stmt(self._fact_table_name, join_tables[0])]
            for i in range(0, len(join_tables) - 1):
                hierarchy_table_join.append(
                    f"{join_tables[i].name}.{join_tables[i].fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1].pk_name}")

            return " AND ".join(hierarchy_table_join)
        else:
            return ""

    def _get_row_hierarchy_join(self) -> str:
        if self.next_cube.visual_row:
            join_tables = get_tables_below_including(self.next_cube.visual_row) \
                          + get_tables_above(self.next_cube.visual_row)
            hierarchy_table_join: List[str] = [get_fact_table_join_stmt(self._fact_table_name, join_tables[0])]
            for i in range(0, len(join_tables) - 1):
                hierarchy_table_join.append(
                    f"{join_tables[i].name}.{join_tables[i].fk_name} = {join_tables[i + 1].name}.{join_tables[i + 1].pk_name}")

            return " AND ".join(hierarchy_table_join)
        else:
            return ""

    def _get_column_where_stmt(self) -> str:
        current_value: str = get_current_value_stmt(self.next_cube.column_value_list)
        ancestor_value: str = get_ancestor_value_stmt(self.next_cube.column_value_list)
        if ancestor_value:
            return current_value + " AND " + ancestor_value
        else:
            return current_value

    def _get_row_where_stmt(self) -> str:
        current_value: str = get_current_value_stmt(self.next_cube.row_value_list)
        ancestor_value: str = get_ancestor_value_stmt(self.next_cube.row_value_list)
        if ancestor_value:
            return current_value + " AND " + ancestor_value
        else:
            return current_value

    def _get_where_stmt(self) -> str:
        column_where_stmt: str = ""
        row_where_stmt: str = ""
        if self.next_cube.visual_column:
            column_where_stmt = self._get_column_where_stmt()
        if self.next_cube.visual_row:
            row_where_stmt = self._get_row_where_stmt()
        result = "WHERE "
        if column_where_stmt and row_where_stmt:
            result += column_where_stmt + " AND " + row_where_stmt
        elif column_where_stmt:
            result += column_where_stmt
        else:
            result += row_where_stmt
        return result

    def _get_group_by_column_stmt(self) -> str:
        column_name: str = self._get_column_name_if_exists()
        above_columns = []
        above_tables: List[NonTopLevel] = get_tables_above(self.next_cube.visual_column)
        above_columns: List[str] = []
        for table in above_tables:
            above_columns.append(get_table_and_column_name(table))
        auxiliary_columns: str = ", ".join(above_columns)
        return column_name + ", " + auxiliary_columns if auxiliary_columns else column_name

    def _get_group_by_row_stmt(self) -> str:
        row_name: str = self._get_row_name_if_exists()
        above_rows = []
        above_tables: List[NonTopLevel] = get_tables_above(self.next_cube.visual_row)
        above_rows: List[str] = []
        for table in above_tables:
            above_rows.append(get_table_and_column_name(table))
        auxiliary_columns: str = ", ".join(above_rows)
        return row_name + ", " + auxiliary_columns if auxiliary_columns else row_name

    def _get_group_by_stmt(self) -> str:
        column_name: str = ""
        row_name: str = ""
        if self.next_cube.visual_column:
            column_name = self._get_group_by_column_stmt()
        if self.next_cube.visual_row:
            row_name = self._get_group_by_row_stmt()
        result = "GROUP BY "
        if column_name and row_name:
            result += column_name + ", " + row_name
        elif column_name:
            result += column_name
        else:
            result += row_name
        return result

    def _get_order_by_stmt(self) -> str:
        column_arr_pos: str = ""
        row_arr_pos: str = ""
        result: str = "ORDER BY "
        if self.next_cube.column_value_list:
            column_value_list: List[str] = get_list_of_values(self.next_cube.column_value_list)
            values: str = ", ".join(column_value_list)
            table_column_name: str = get_table_and_column_name(self.next_cube.column_value_list[0].level)
            column_arr_pos = f"array_position(array[{values}], {table_column_name})"
        if self.next_cube.row_value_list:
            row_value_list: List[str] = get_list_of_values(self.next_cube.row_value_list)
            values: str = ", ".join(row_value_list)
            table_column_name: str = get_table_and_column_name(self.next_cube.row_value_list[0].level)
            row_arr_pos = f"array_position(array[{values}], {table_column_name})"
        if column_arr_pos and row_arr_pos:
            return result + column_arr_pos + ", " + row_arr_pos
        if column_arr_pos:
            return result + column_arr_pos
        if row_arr_pos:
            return result + row_arr_pos

    def execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        conn: connection = self._get_new_connection()

        with conn.cursor() as curs:
            curs.execute(query)
            result: List[Tuple[Any, ...]] = curs.fetchall()
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

