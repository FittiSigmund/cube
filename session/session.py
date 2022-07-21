import psycopg2
from psycopg2 import Error

from cube.RegularDimension import RegularDimension
from .cube_metadata import create_cube_metadata, create_cube
from .infer_cube import get_fact_table_name, create_levels, create_dimensions, get_measures, \
    create_measures, get_lowest_level_names


class Session:
    def __init__(self, cubes, engine):
        self._cube_list = cubes
        self._engine = engine

    @property
    def cubes(self):
        return self._cube_list

    def load_cube(self, cube_name):
        cube_candidate = list(filter(lambda x: x.name == cube_name, self._cube_list))
        return cube_candidate[0] if len(cube_candidate) == 1 else f"No cube found with name: {cube_name}"


def attach_metadata_to_dimensions(dimensions, metadata):
    for dimension in dimensions:
        if isinstance(dimension, RegularDimension):
            dimension.metadata = metadata


def create_session(engine):
    conn, cursor = get_db_connection_and_cursor(engine)
    try:
        fact_table_name = get_fact_table_name(cursor)
        lowest_level_dto_list = get_lowest_level_names(cursor, fact_table_name)
        level_dto_list_list = create_levels(cursor, lowest_level_dto_list, engine)
        dimensions = create_dimensions(level_dto_list_list, engine)
        measures = create_measures(get_measures(cursor, fact_table_name))
        metadata = create_cube_metadata(engine.dbname, dimensions, level_dto_list_list, measures)
        attach_metadata_to_dimensions(dimensions, metadata)
        cube = create_cube(fact_table_name, dimensions, measures, engine.dbname, metadata, engine)
        return Session([cube], engine)
    except (Exception, Error) as error:
        print("ERROR: ", error)
    finally:
        cursor.close()
        conn.close()


def get_db_connection_and_cursor(engine):
    conn = psycopg2.connect(user=engine.user,
                            password=engine.password,
                            host=engine.host,
                            port=engine.port,
                            database=engine.dbname)
    return conn, conn.cursor()
