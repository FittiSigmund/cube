from functools import reduce
from Levenshtein import distance as levenshtein_distance

from Cube import Level, Dimension, AggregateFunction, Measure
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


def create_levels_in_hierarchies(db_cursor):
    lowest_level_names = get_lowest_levels(db_cursor)
    hierarchies = []
    level_attributes = []
    for level_name in lowest_level_names:
        level_names, level_attribute_names = create_hierarchy(db_cursor, level_name)
        levels = list(map(lambda x: Level(x), level_names))
        for i in range(len(levels)):
            if i == len(levels) - 1:
                levels[i].parent = levels[i]
            else:
                levels[i].parent = levels[i + 1]

        hierarchies.append(levels)
        level_attributes.append(level_attribute_names)

    return hierarchies, level_attributes


def get_lowest_levels(db_cursor):
    db_cursor.execute(LOWEST_LEVELS_QUERY)
    return list(map(lambda x: x[0], db_cursor.fetchall()))


def create_hierarchy(db_cursor, level_name):
    hierarchy_list = [level_name]
    level_attributes = []
    found_top_level = False
    current_level = level_name
    level_attribute_names = get_level_attributes(db_cursor, current_level)
    if level_attribute_names:
        level_attributes.append(level_attribute_names)

    while not found_top_level:
        next_level = get_next_level_name(db_cursor, current_level)
        if not next_level:
            found_top_level = True
            continue
        level_attribute_names = get_level_attributes(db_cursor, next_level)
        if level_attribute_names:
            level_attributes.append(level_attribute_names)
        hierarchy_list.append(next_level)
        current_level = next_level

    return hierarchy_list, level_attributes


def get_level_attributes(db_cursor, level_name):
    db_cursor.execute(GET_NON_KEY_COLUMNS_QUERY(level_name))
    non_key_columns_tuples = db_cursor.fetchall()
    non_key_columns = list(map(lambda x: x[0], non_key_columns_tuples))

    if len(non_key_columns) > 1:
        level_attributes = remove_level_member(level_name, non_key_columns)
        return level_name, level_attributes

    return ()


def get_next_level_name(db_cursor, current_level):
    db_cursor.execute(GET_NEXT_LEVEL_QUERY(current_level))
    result = db_cursor.fetchall()
    return result[0][0] if result else None


def remove_level_member(level_name, level_attribute_names):
    distance_dict = {}
    for name in level_attribute_names:
        distance_dict[levenshtein_distance(level_name, name)] = name

    distance_dict.pop(min(list(distance_dict.keys())))
    return list(distance_dict.values())


def create_dimensions(level_list):
    return list(map(lambda x: create_dimension(x), level_list))


def create_dimension(levels):
    dimension_name = levels[0].name.split('_')[0]
    return Dimension(dimension_name, levels)


def get_measures(db_cursor, fact_table):
    db_cursor.execute(GET_ALL_MEASURES_QUERY(fact_table))
    return db_cursor.fetchall()


def create_measures(measure_list):
    return list(map(lambda x: create_measure(x[0]), measure_list))


def create_measure(measure):
    sum_agg_func = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, sum_agg_func)
