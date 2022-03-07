import psycopg2
from psycopg2 import Error
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

from Cube import Level, Hierarchy, Dimension, Cube, Measure, AggregateFunction

fact_table_list = []


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
    SUM = AggregateFunction("SUM", lambda x, y: x + y)
    return Measure(measure, SUM)


def create_measures(measure_list):
    return list(map(lambda x: create_measure(x), measure_list))


def create_session(engine):
    try:
        cursor = get_cursor(engine.user, engine.password, engine.host, engine.port, engine.dbname)
        qb = Namespace("http://purl.org/linked-data/cube#")
        qb4o = Namespace("http://purl.org/qb4olap/cubes#")
        metadata = Graph()
        dsd_name = URIRef("http://example.org/" + engine.dbname + "_dsd")
        metadata.add((dsd_name, RDF.type, qb.DataStructureDefinition))
        for s, p, o in metadata:
            print(s, p, o)
        print(metadata.serialize(format="turtle"))
        ## TODO: Add more QB4OLAP structures
        table_with_no_fks = get_table_with_no_fks(cursor)
        hierarchies = create_hierarchies(cursor, table_with_no_fks)
        dimensions = create_dimensions(hierarchies)
        fact_table = get_fact_table()
        measures = create_measures(get_measures(cursor, fact_table))
        return Session([create_cube(dimensions, measures, engine.dbname)])
    except (Exception, Error) as error:
        print("ERROR: ", error)


def get_cursor(user, password, host, port, database):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    return connection.cursor()


def get_table_with_no_fks(db_cursor):
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


def get_next_level(db_cursor, level):
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


def get_levels(db_cursor, level):
    level_name_list = [level]
    current_level = level
    fact_table_found = False

    while fact_table_found is False:
        next_level = get_next_level(db_cursor, current_level)[0][0]
        potential_level = get_next_level(db_cursor, next_level)

        if potential_level:
            level_name_list.append(next_level)
            current_level = next_level
            continue
        else:
            fact_table_list.append(next_level)
            fact_table_found = True

    return list(map(lambda x: Level(x), level_name_list))


def create_hierarchy(db_cursor, level_tuple):
    levels = get_levels(db_cursor, level_tuple[0])
    return Hierarchy(levels)


def create_dimension(hierarchy):
    dimension_name = hierarchy.levels[0].name.split('_')[0]
    return Dimension(dimension_name, hierarchy, hierarchy.levels)


def create_cube(dimension_list, measure_list, dbname):
    return Cube(dimension_list, measure_list, dbname)


def create_hierarchies(db_cursor, no_fk_table_list):
    return list(map(lambda x: create_hierarchy(db_cursor, x), no_fk_table_list))


def create_dimensions(hierarchy_list):
    return list(map(lambda x: create_dimension(x), hierarchy_list))
