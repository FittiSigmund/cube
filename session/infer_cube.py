from functools import reduce

import psycopg2
from Levenshtein import distance as levenshtein_distance

from cube.AggregateFunction import AggregateFunction
from cube.Dimension import Dimension
from cube.Measure import Measure
from cube.Level import Level
from cube.LevelMember import LevelMember
from session.sql_queries import ALL_USER_TABLES_QUERY, TABLE_CARDINALITY_QUERY, LOWEST_LEVELS_QUERY, \
    GET_NON_KEY_COLUMNS_QUERY, GET_NEXT_LEVEL_QUERY, GET_ALL_MEASURES_QUERY, GET_PK_AND_FK_COLUMNS_QUERY


def get_fact_table_name(db_cursor):
    all_table_names = get_all_table_names(db_cursor)
    result_tuple = []
    for table_name in all_table_names:
        db_cursor.execute(TABLE_CARDINALITY_QUERY(table_name))
        result_tuple.append((table_name, db_cursor.fetchall()[0][0]))

    return list(reduce(lambda x, y: x if x[1] >= y[1] else y, result_tuple))[0]


def get_all_table_names(db_cursor):
    db_cursor.execute(ALL_USER_TABLES_QUERY)
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def create_level_member_value(values):
    return values


def create_level_member_instances(level_dto_list):
    result = []
    if not level_dto_list:
        return result
    head, tail = level_dto_list.popleft(), level_dto_list

    for value in head.member_values:
        result.append(LevelMember(value, create_level_member_instances(tail)))

    head.level_member_instances = result
    return result


def attach_level_member_instances(level_dto_list, level_member_instance_tree):
    current_level_members = level_member_instance_tree
    i = 0
    while current_level_members:
        level_dto_list[i].level_member_instances = current_level_members
        i += 1
        current_level_members = current_level_members[0].children()


def attach_children_to_levels(levels):
    for i in range(len(levels)):
        if i == len(levels) - 1:
            levels[i].child = levels[i]
        else:
            levels[i].child = levels[i + 1]
    return levels


def attach_parents_to_levels(levels):
    for i in range(len(levels)):
        if i == 0:
            levels[i].parent = levels[i]
        else:
            levels[i].parent = levels[i - 1]
    return levels


def attach_levels_to_dto_list(level_dto_list, levels):
    for i in range(len(levels)):
        level_dto_list[i].level = levels[i]
    return level_dto_list


def create_levels_in_hierarchy(db_cursor, lowest_level_dto, engine):
    level_dto_list = create_hierarchy(db_cursor, lowest_level_dto.level_name, lowest_level_dto.fact_table_fk)
    levels = list(map(lambda l: Level(l.name, l.member, engine, l.pk_name, l.fk_name), level_dto_list))
    levels = attach_parents_to_levels(levels)
    levels = attach_children_to_levels(levels)
    level_dto_list = attach_levels_to_dto_list(level_dto_list, levels)

    return level_dto_list


def create_levels(db_cursor, lowest_level_names, engine):
    return list(map(lambda x: create_levels_in_hierarchy(db_cursor, x, engine), lowest_level_names))


def get_lowest_level_names(db_cursor, fact_table_name):
    db_cursor.execute(LOWEST_LEVELS_QUERY(fact_table_name))
    lowest_level_dto_list = list(map(lambda x: LowestLevelDTO(x[0], x[1]), db_cursor.fetchall()))
    return lowest_level_dto_list


def get_level_member_values(db_cursor, level_member_name, level_name):
    db_cursor.execute(f"""
        SELECT {level_member_name}
        FROM {level_name}
    """)
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def create_level_member_values(db_cursor, level_name, level_member_name):
    return get_level_member_values(db_cursor, level_member_name, level_name)


class LevelDTO:
    level = []

    def __init__(self, level_name, level_member, level_attributes, pk, fk, fact_table_fk):
        self.level_member_instances = []
        self.name = level_name
        self.member = level_member
        self.attributes = level_attributes
        self.pk_name = pk
        self.fk_name = fk
        self.fact_table_fk = fact_table_fk

    def __repr__(self):
        return f"LevelDTO: {self.name}"


class LowestLevelDTO:
    def __init__(self, level_name, fact_table_fk):
        self.level_name = level_name
        self.fact_table_fk = fact_table_fk


def get_pk_and_fk_column_names(cursor, level_name):
    cursor.execute(GET_PK_AND_FK_COLUMNS_QUERY(level_name))
    pk, fk = None, None
    for t in cursor.fetchall():
        if t[1] == 'PRIMARY KEY':
            pk = t[0]
        else:
            fk = t[0]

    return pk, fk


def create_hierarchy(db_cursor, level_name, fact_table_fk):
    current_level = level_name
    found_top_level = False
    level_member_name, level_attribute_names = get_level_attributes_and_member_name(db_cursor, current_level)
    pk, fk = get_pk_and_fk_column_names(db_cursor, level_name)
    hierarchy_dto_list = [LevelDTO(current_level, level_member_name, level_attribute_names, pk, fk, fact_table_fk)]

    while not found_top_level:
        current_level = get_next_level_name(db_cursor, current_level)
        if not current_level:
            found_top_level = True
            continue
        level_member_name, level_attribute_names = get_level_attributes_and_member_name(db_cursor, current_level)
        pk, fk = get_pk_and_fk_column_names(db_cursor, current_level)
        hierarchy_dto_list.append(LevelDTO(current_level, level_member_name, level_attribute_names, pk, fk, fact_table_fk))

    hierarchy_dto_list.reverse()
    return hierarchy_dto_list


def get_level_attributes_and_member_name(db_cursor, level_name):
    db_cursor.execute(GET_NON_KEY_COLUMNS_QUERY(level_name))
    non_key_columns_tuples = db_cursor.fetchall()
    non_key_columns = list(map(lambda x: x[0], non_key_columns_tuples))

    if len(non_key_columns) > 1:
        level_member, level_attributes = remove_level_member(level_name, non_key_columns)
        return level_member, level_attributes

    return non_key_columns[0], []


def get_next_level_name(db_cursor, current_level):
    db_cursor.execute(GET_NEXT_LEVEL_QUERY(current_level))
    result = db_cursor.fetchall()
    return result[0][0] if result else None


def remove_level_member(level_name, level_attribute_names):
    distance_dict = {}
    for name in level_attribute_names:
        distance_dict[levenshtein_distance(level_name, name)] = name

    level_member = distance_dict.pop(min(list(distance_dict.keys())))
    return level_member, list(distance_dict.values())


def create_dimensions(level_dto_list_list, engine):
    return list(map(lambda x: create_dimension(x[::-1], engine), level_dto_list_list))


def create_dimension(level_dto_list, engine):
    dimension_name = level_dto_list[0].name.split('_')[0]
    levels = list(map(lambda x: x.level, level_dto_list))
    return Dimension(dimension_name, levels, engine, level_dto_list[0].fact_table_fk)


def get_measures(db_cursor, fact_table):
    db_cursor.execute(GET_ALL_MEASURES_QUERY(fact_table))
    return db_cursor.fetchall()


def create_measures(measure_list):
    return list(map(lambda x: create_measure(x[0]), measure_list))


def create_measure(measure):
    sum_agg_func = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, sum_agg_func)


def get_db_cursor(engine):
    connection = psycopg2.connect(user=engine.user,
                                  password=engine.password,
                                  host=engine.host,
                                  port=engine.port,
                                  database=engine.dbname)
    return connection.cursor()
