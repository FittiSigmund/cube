import psycopg2
from psycopg2 import Error
from rdflib import Graph, URIRef, BNode, Namespace
from rdflib.namespace import RDF, RDFS, QB
from Levenshtein import distance as levenshtein_distance

from Cube import Level, Dimension, Cube, Measure, AggregateFunction

fact_table_list = []

EG_NAMESPACE_PREFIX = "http://example.org/"
QB4O_NAMESPACE_PREFIX = "http://purl.org/qb4olap/cubes/"

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


class Session:
    def __init__(self, cubes):
        self._cube_list = cubes

    @property
    def cubes(self):
        return list(map(lambda x: x.name, self._cube_list))

    def load_cube(self, cube_name):
        cube_candidate = list(filter(lambda x: x.name == cube_name, self._cube_list))
        return cube_candidate[0] if len(cube_candidate) == 1 else f"No cube found with name: {cube_name}"


def get_fact_table():
    return fact_table_list[0] if len(set(fact_table_list)) == 1 else None


def get_measures(db_cursor, fact_table):
    db_cursor.execute(f"""
            SELECT pg_attribute.attname
            FROM pg_attribute
            WHERE pg_attribute.attrelid = (SELECT oid FROM pg_class WHERE pg_class.relname = '{fact_table}')
            AND pg_attribute.attnum >= 0
            AND pg_attribute.attname NOT IN (
                                         SELECT kcu.column_name 
                                         FROM information_schema.key_column_usage AS kcu 
                                         WHERE kcu.table_name = '{fact_table}'
                                         );
    """)
    return db_cursor.fetchall()


def create_measure(measure):
    sum_agg_func = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, sum_agg_func)


def create_measures(measure_list):
    return list(map(lambda x: create_measure(x[0]), measure_list))


def initialize_rdf_graph():
    g = Graph()
    g.bind("eg", EG)
    g.bind("qb4o", QB4O)
    return g


def create_namespaces():
    return Namespace(EG_NAMESPACE_PREFIX), Namespace(QB4O_NAMESPACE_PREFIX)


def add_data_structure_definition(metadata, dsd_node):
    metadata.add((dsd_node, RDF.type, QB.DataStructureDefinition))


def create_dsd_node(dbname):
    dsd_name = dbname + "_dsd"
    return EG[dsd_name]


def create_metadata_for_measure(measure, metadata, dsd_node):
    blank_measure_node = BNode()
    metadata.add((dsd_node, QB.component, blank_measure_node))
    ## TODO: Add mapping from aggregate function names to qb4o names
    metadata.add((blank_measure_node, QB4O.hasAggregateFunction, QB4O.sum))
    metadata.add((blank_measure_node, QB.measure, EG[measure.name]))
    metadata.add((EG[measure.name], RDF.type, QB.MeasureProperty))


def create_metadata_for_measures(measures, metadata, dsd_node):
    # Need the call to list, because map is lazily evaluated
    list(map(lambda x: create_metadata_for_measure(x, metadata, dsd_node), measures))


def remove_tuple_from_list(table_with_no_fks_tuple):
    return list(map(lambda x: x[0], table_with_no_fks_tuple))


def create_metadata_for_dimension(dimension, metadata, dsd_node):
    blank_node = BNode()
    dimension_node = EG[dimension.name]

    metadata.add((dsd_node, QB.component, blank_node))
    metadata.add((blank_node, QB4O.level, EG[dimension.lowest_level.name]))

    metadata.add((dimension_node, RDF.type, QB.DimensionProperty))

    level = dimension.lowest_level
    while level.parent is not level:
        level_node = EG[level.name]
        parent_level_node = EG[level.parent.name]
        metadata.add((level_node, RDF.type, QB4O.LevelProperty))
        metadata.add((level_node, QB4O.inDimension, dimension_node))
        metadata.add((level_node, QB4O.parentLevel, parent_level_node))
        level = level.parent

    level_node = EG[level.name]
    metadata.add((level_node, RDF.type, QB4O.LevelProperty))
    metadata.add((level_node, QB4O.inDimension, dimension_node))


def create_metadata_for_dimensions(dimensions, metadata, dsd_node):
    list(map(lambda x: create_metadata_for_dimension(x, metadata, dsd_node), dimensions))


def create_metadata_for_level_attribute(metadata, level_attribute):
    for level_name, level_attribute_names in level_attribute.items():
        for level_attribute_name in level_attribute_names:
            metadata.add((EG[level_attribute_name], RDF.type, QB.AttributeProperty))
            metadata.add((EG[level_name], QB4O.hasAttribute, EG[level_attribute_name]))


def create_metadata_for_level_attributes(metadata, level_attributes):
    list(map(lambda x: create_metadata_for_level_attribute(metadata, x), level_attributes))


def create_cube_metadata(dsd_name, dimensions, level_attributes, measures):
    ## TODO: Generate proper URIs (or use proper prefix)
    metadata = initialize_rdf_graph()
    dsd_node = create_dsd_node(dsd_name)
    add_data_structure_definition(metadata, dsd_node)
    create_metadata_for_dimensions(dimensions, metadata, dsd_node)
    create_metadata_for_level_attributes(metadata, level_attributes)
    create_metadata_for_measures(measures, metadata, dsd_node)
    # print(metadata.serialize(format="turtle"))


def infer_cube_structure(db_cursor):
    top_level_names = get_table_with_no_fks(db_cursor)
    result = create_levels_in_hierarchies(db_cursor, top_level_names)
    levels, level_attributes = zip(*result)
    fact_table = get_fact_table()
    return create_dimensions(levels), level_attributes, create_measures(get_measures(db_cursor, fact_table))


def create_session(engine):
    try:
        cursor = get_db_cursor(engine.user, engine.password, engine.host, engine.port, engine.dbname)
        dimensions, level_attributes, measures = infer_cube_structure(cursor)
        create_cube_metadata(engine.dbname, dimensions, level_attributes, measures)
        return Session([create_cube(dimensions, measures, engine.dbname)])
    except (Exception, Error) as error:
        print("ERROR: ", error)


def get_db_cursor(user, password, host, port, database):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    return connection.cursor()


def fetch_table_with_no_fks(db_cursor):
    db_cursor.execute("""
        SELECT
            table_info.table_name
        FROM information_schema.tables AS table_info
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_info.table_type = 'BASE TABLE'
        AND table_info.table_schema || '.' || table_info.table_name NOT IN 
            (SELECT DISTINCT table_schema || '.' || table_name
             FROM information_schema.table_constraints
             WHERE constraint_type = 'FOREIGN KEY');
    """)

    return db_cursor.fetchall()


def get_table_with_no_fks(db_cursor):
    table_with_no_fks_tuple = fetch_table_with_no_fks(db_cursor)
    return list(map(lambda x: x[0], table_with_no_fks_tuple))


def get_next_level_name(db_cursor, level):
    db_cursor.execute(f"""
        SELECT relname
        FROM pg_class
        WHERE pg_class.oid = (
            SELECT conrelid
            FROM pg_constraint
            WHERE pg_constraint.contype = 'f'
            AND pg_constraint.confrelid = (SELECT oid FROM pg_class WHERE pg_class.relname = '{level}')
        )
    """)
    return db_cursor.fetchall()


def get_level_attributesREMOVE(level, attribute_list):
    attribute_distance = {}
    for attribute in attribute_list:
        attribute_distance[levenshtein_distance(level, attribute[0])] = attribute[0]

    attribute_distance.pop(min(attribute_distance.keys()))
    return list(attribute_distance.values())


def get_level_attributes_metadata(db_cursor, level):
    db_cursor.execute(f"""
        SELECT info_col.column_name
        FROM information_schema.columns AS info_col
        WHERE info_col.table_name = '{level}'
        AND info_col.column_name NOT IN (SELECT kcu.column_name FROM information_schema.key_column_usage AS kcu WHERE kcu.table_name = '{level}')
    """)

    non_key_columns = db_cursor.fetchall()
    return get_level_attributesREMOVE(level, non_key_columns) if len(non_key_columns) > 1 else None


def get_levels(db_cursor, top_level_name):
    level_name_list = [top_level_name]
    current_level_name = top_level_name
    fact_table_found = False
    metadata_list = []
    top_level_attributes = get_level_attributes_metadata(db_cursor, top_level_name)
    if top_level_attributes is not None:
        metadata_list.append(top_level_attributes)

    while fact_table_found is False:
        next_level_name = get_next_level_name(db_cursor, current_level_name)[0][0]
        potential_level_name = get_next_level_name(db_cursor, next_level_name)

        if potential_level_name:
            level_attributes_metadata = get_level_attributes_metadata(db_cursor, next_level_name)
            if level_attributes_metadata is not None:
                metadata_list.append(level_attributes_metadata)
            level_name_list.append(next_level_name)
            current_level_name = next_level_name
            continue
        else:
            fact_table_list.append(next_level_name)
            fact_table_found = True

    return level_name_list


def remove_tuples_from_list(list_of_tuples):
    return list(map(lambda x: x[0], list_of_tuples))


def remove_level_member(level_name, level_attribute_names):
    distance_dict = {}
    for name in level_attribute_names:
        distance_dict[levenshtein_distance(level_name, name)] = name

    distance_dict.pop(min(list(distance_dict.keys())))
    return list(distance_dict.values())


def get_level_attributes(db_cursor, level_name_list):
    level_attributes_dict = {}
    for level_name in level_name_list:
        db_cursor.execute(f"""
            SELECT info_col.column_name
            FROM information_schema.columns AS info_col
            WHERE info_col.table_name = '{level_name}'
            AND info_col.column_name NOT IN (SELECT kcu.column_name FROM information_schema.key_column_usage AS kcu WHERE kcu.table_name = '{level_name}')
        """)
        non_key_columns = db_cursor.fetchall()
        if len(non_key_columns) > 1:
            non_key_columns_without_tuples = remove_tuples_from_list(non_key_columns)
            level_attributes = remove_level_member(level_name, non_key_columns_without_tuples)
            level_attributes_dict[level_name] = level_attributes

    return level_attributes_dict


def convert_to_levels(level_names):
    return list(map(lambda x: Level(x), level_names))


def create_levels_in_hierarchy(db_cursor, top_level_name):
    level_name_list = get_levels(db_cursor, top_level_name)
    level_list = convert_to_levels(level_name_list)
    level_attributes = get_level_attributes(db_cursor, level_name_list)
    for i in range(len(level_list)):
        if i == 0:
            level_list[i].parent = level_list[i]
        else:
            level_list[i].parent = level_list[i - 1]

    return list(reversed(level_list)), level_attributes


def create_dimension(levels):
    dimension_name = levels[0].name.split('_')[0]
    return Dimension(dimension_name, levels)


def create_cube(dimension_list, measure_list, dbname):
    return Cube(dimension_list, measure_list, dbname)


def create_levels_in_hierarchies(db_cursor, top_level_names):
    return list(map(lambda x: create_levels_in_hierarchy(db_cursor, x), top_level_names))


def create_dimensions(level_list):
    return list(map(lambda x: create_dimension(x), level_list))
