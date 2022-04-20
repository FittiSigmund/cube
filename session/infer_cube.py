from functools import reduce
from Levenshtein import distance as levenshtein_distance

from Cube import Dimension, AggregateFunction, Measure
from cube.Level import Level
from cube.LevelMember import LevelMember
from session.sql_queries import ALL_USER_TABLES_QUERY, TABLE_CARDINALITY_QUERY, LOWEST_LEVELS_QUERY, \
    GET_NON_KEY_COLUMNS_QUERY, GET_NEXT_LEVEL_QUERY, GET_ALL_MEASURES_QUERY


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


def create_levels_in_hierarchy(db_cursor, level_name):
    level_dto_list = create_hierarchy(db_cursor, level_name)
    # list(map(lambda x: print(x.member_values), level_dto_list))
    for i in range(len(level_dto_list)):
        for j in range(len(level_dto_list[i].member_values)):
            if i == len(level_dto_list) - 1:
                level_dto_list[i].level_member_instances.append(LevelMember(level_dto_list[i].member_values[j], []))
                continue
            level_dto_list[i].level_member_instances.append(LevelMember(level_dto_list[i].member_values[j], level_dto_list[i + 1].member_values))

    # for dto in level_dto_list:
    #     for inst in dto.level_member_instances:
    #         print("Level member name -> ", inst.name, "Level member children -> ", inst.children())

    levels = list(map(lambda l: Level(l.name, l.level_member_instances), level_dto_list))
    for i in range(len(levels)):
        if i == 0:
            levels[i].parent = levels[i]
        else:
            levels[i].parent = levels[i - 1]
    ## CREATE LEVEL MEMBER INSTANCES AND ATTACH THEM TO LEVELS
    ## THE ATTACHMENT MUST BE DONE IN A "TOP DOWN" MANNER, MEANING THAT LEVEL_DTO_LIST AND LEVELS MUST BE REVERSED

    for i in range(len(levels)):
        level_dto_list[i].level = levels[i]

    return level_dto_list


def create_levels(db_cursor, lowest_level_names):
    return list(map(lambda x: create_levels_in_hierarchy(db_cursor, x), lowest_level_names))


def get_lowest_level_names(db_cursor, fact_table_name):
    db_cursor.execute(LOWEST_LEVELS_QUERY(fact_table_name))
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def get_level_member_values(db_cursor, level_member_name, level_name):
    db_cursor.execute(f"""
        SELECT DISTINCT {level_member_name}
        FROM {level_name}
    """)
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def create_level_member_values(db_cursor, level_name, level_member_name):
    return get_level_member_values(db_cursor, level_member_name, level_name)


class LevelDTO:
    level = []

    def __init__(self, level_name, level_member, level_member_values, level_attributes):
        self.level_member_instances = []
        self.name = level_name
        self.member = level_member
        self.member_values = level_member_values
        self.attributes = level_attributes


def create_hierarchy(db_cursor, level_name):
    current_level = level_name
    found_top_level = False
    level_member_name, level_attribute_names = get_level_attributes_and_member_name(db_cursor, current_level)
    level_member_values = create_level_member_values(db_cursor, current_level, level_member_name)
    hierarchy_dto_list = [LevelDTO(current_level, level_member_name, level_member_values, level_attribute_names)]

    while not found_top_level:
        current_level = get_next_level_name(db_cursor, current_level)
        if not current_level:
            found_top_level = True
            continue
        level_member_name, level_attribute_names = get_level_attributes_and_member_name(db_cursor, current_level)
        level_member_values = create_level_member_values(db_cursor, current_level, level_member_name)
        hierarchy_dto_list.append(
            LevelDTO(current_level, level_member_name, level_member_values, level_attribute_names))

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


def create_dimensions(level_dto_list_list):
    return list(map(lambda x: create_dimension(x), level_dto_list_list))


def create_dimension(level_dto_list):
    dimension_name = level_dto_list[0].name.split('_')[0]
    levels = list(map(lambda x: x.level, level_dto_list))
    return Dimension(dimension_name, levels)


def get_measures(db_cursor, fact_table):
    db_cursor.execute(GET_ALL_MEASURES_QUERY(fact_table))
    return db_cursor.fetchall()


def create_measures(measure_list):
    return list(map(lambda x: create_measure(x[0]), measure_list))


def create_measure(measure):
    sum_agg_func = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, sum_agg_func)
