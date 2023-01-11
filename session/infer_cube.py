from __future__ import annotations
from functools import reduce
from typing import List, Tuple, Dict, Any

from psycopg2.extensions import cursor as psycur
from Levenshtein import distance as levenshtein_distance

from cube import Level
from cube.AggregateFunction import AggregateFunction
from cube.RegularDimension import RegularDimension
from cube.Measure import Measure
from cube.NonTopLevel import NonTopLevel
from cube.TopLevel import TopLevel
from engines import Postgres
from session.sql_queries import ALL_USER_TABLES_QUERY, table_cardinality_query, lowest_levels_query, \
    get_non_key_columns_query, get_next_level_query, get_all_measures_query, get_pk_and_fk_columns_query


def get_fact_table_name(db_cursor: psycur) -> str:
    all_table_names: List[str] = get_all_table_names(db_cursor)
    result_tuple: List[Tuple[str, int]] = []
    for table_name in all_table_names:
        db_cursor.execute(table_cardinality_query(table_name))
        result_tuple.append((table_name, db_cursor.fetchall()[0][0]))

    return list(reduce(lambda x, y: x if x[1] >= y[1] else y, result_tuple))[0]


def get_all_table_names(db_cursor: psycur) -> List[str]:
    db_cursor.execute(ALL_USER_TABLES_QUERY)
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def attach_children_to_levels(levels: List[Level]) -> List[Level]:
    for i in range(len(levels)):
        if i == len(levels) - 1:
            levels[i].child = levels[i]
        else:
            levels[i].child = levels[i + 1]
    return levels


def attach_parents_to_levels(levels: List[Level]) -> List[Level]:
    for i in range(len(levels)):
        if i == 0:
            levels[i].parent = levels[i]
        else:
            levels[i].parent = levels[i - 1]
    return levels


def attach_levels_to_dto_list(level_dtos: List[LevelDTO], levels: List[Level]) -> List[LevelDTO]:
    for i in range(len(levels)):
        level_dtos[i].level = levels[i]
    return level_dtos

def get_non_top_levels_in_hierarchy(hierarchy: List[LevelDTO]) -> List[LevelDTO]:
    return hierarchy[1:]

def create_levels_in_hierarchy(db_cursor: psycur, lowest_level: LowestLevelDTO, engine: Postgres) -> List[LevelDTO]:
    hierarchy: List[LevelDTO] = create_hierarchy(db_cursor, lowest_level.level_name, lowest_level.fact_table_fk)
    levels: List[Level] = [TopLevel()]
    for lv in get_non_top_levels_in_hierarchy(hierarchy):
        levels.append(NonTopLevel(lv.name, lv.member, engine, lv.pk_name, lv.fk_name))

    levels: List[Level] = attach_parents_to_levels(levels)
    levels: List[Level] = attach_children_to_levels(levels)
    hierarchy: List[LevelDTO] = attach_levels_to_dto_list(hierarchy, levels)

    return hierarchy


def create_levels(db_cursor: psycur, lowest_levels: List[LowestLevelDTO], engine: Postgres) -> List[List[LevelDTO]]:
    return list(map(lambda x: create_levels_in_hierarchy(db_cursor, x, engine), lowest_levels))


def get_lowest_level_names(db_cursor: psycur, fact_table_name: str) -> List[LowestLevelDTO]:
    db_cursor.execute(lowest_levels_query(fact_table_name))
    return list(map(lambda x: LowestLevelDTO(x[0], x[1]), db_cursor.fetchall()))


class LevelDTO:
    level = []

    def __init__(self,
                 level_name: str = "",
                 level_member: str = "",
                 level_attributes=None,
                 pk: str = "",
                 fk: str = "",
                 fact_table_fk: str = "",
                 top_level: bool = False):
        if level_attributes is None:
            level_attributes = []
        self.level_member_instances = []
        self.member = level_member
        self.attributes = level_attributes
        self.pk_name = pk
        self.fk_name = fk
        self.fact_table_fk = fact_table_fk
        self.name = level_name
        self.top_level = top_level

    def __repr__(self):
        return f"LevelDTO: {self.name}"


class LowestLevelDTO:
    def __init__(self, level_name, fact_table_fk):
        self.level_name = level_name
        self.fact_table_fk = fact_table_fk


def get_pk_and_fk_column_names(cursor: psycur, level_name: str) -> Tuple[str, str]:
    cursor.execute(get_pk_and_fk_columns_query(level_name))
    pk: str
    fk: str
    pk, fk = "", ""
    for t in cursor.fetchall():
        if t[1] == 'PRIMARY KEY':
            pk = t[0]
        else:
            fk = t[0]

    return pk, fk


def create_hierarchy(db_cursor: psycur, level_name: str, fact_table_fk: str) -> List[LevelDTO]:
    current_level: str = level_name
    found_top_level: bool = False
    level_member: str
    level_attributes: List[str]
    level_member, level_attributes = get_level_attributes_and_member_name(db_cursor, current_level)
    pk: str
    fk: str
    pk, fk = get_pk_and_fk_column_names(db_cursor, level_name)
    hierarchies: List[LevelDTO] = [LevelDTO(current_level, level_member, level_attributes, pk, fk, fact_table_fk)]

    while not found_top_level:
        current_level = get_next_level_name(db_cursor, current_level)
        if not current_level:
            found_top_level = True
            continue
        level_member, level_attributes = get_level_attributes_and_member_name(db_cursor, current_level)
        pk, fk = get_pk_and_fk_column_names(db_cursor, current_level)
        hierarchies.append(
            LevelDTO(current_level, level_member, level_attributes, pk, fk, fact_table_fk))
    hierarchies.append(LevelDTO(top_level=True))

    hierarchies.reverse()
    return hierarchies


def get_level_attributes_and_member_name(db_cursor: psycur, level_name: str) -> Tuple[str, List[str]]:
    db_cursor.execute(get_non_key_columns_query(level_name))
    non_key_columns: List[str] = list(map(lambda x: x[0], db_cursor.fetchall()))

    if len(non_key_columns) > 1:
        level_member: str
        level_attributes: List[str]
        level_member, level_attributes = remove_level_member(level_name, non_key_columns)
        return level_member, level_attributes

    return non_key_columns[0], []


def get_next_level_name(db_cursor: psycur, current_level: str) -> str:
    db_cursor.execute(get_next_level_query(current_level))
    result: List[Tuple[str, Any]] = db_cursor.fetchall()
    return result[0][0] if result else ""


def remove_level_member(level_name: str, level_attribute_names: List[str]) -> Tuple[str, List[str]]:
    distance_dict: Dict[int, str] = {}
    for name in level_attribute_names:
        distance_dict[levenshtein_distance(level_name, name)] = name

    level_member: str = distance_dict.pop(min(list(distance_dict.keys())))
    return level_member, list(distance_dict.values())


def create_dimensions(levelDTOs: List[List[LevelDTO]], engine: Postgres) -> List[RegularDimension]:
    return list(map(lambda x: create_dimension(x[::-1], engine), levelDTOs))


def create_dimension(levelDTOs: List[LevelDTO], engine: Postgres) -> RegularDimension:
    dimension_name: str = levelDTOs[0].name.split('_')[0]
    levels: List[Level] = list(map(lambda x: x.level, levelDTOs))
    return RegularDimension(dimension_name, levels, engine, levelDTOs[0].fact_table_fk)


def get_measures(db_cursor: psycur, fact_table: str) -> List[str]:
    db_cursor.execute(get_all_measures_query(fact_table))
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def create_measures(measure_list: List[str]):
    return list(map(lambda x: create_measure(x), measure_list))


def create_measure(measure: str) -> Measure:
    sum_agg_func: AggregateFunction = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, sum_agg_func)
