from functools import reduce

import psycopg2
from psycopg2 import Error

from .cube_metadata import create_cube_metadata, create_cube
from .infer_cube import get_fact_table_name, create_levels_in_hierarchies, create_dimensions, get_measures, \
    create_measures


class Session:
    def __init__(self, cubes):
        self._cube_list = cubes

    @property
    def cubes(self):
        return list(map(lambda x: x.name, self._cube_list))

    def load_cube(self, cube_name):
        cube_candidate = list(filter(lambda x: x.name == cube_name, self._cube_list))
        return cube_candidate[0] if len(cube_candidate) == 1 else f"No cube found with name: {cube_name}"


def create_session(engine):
    try:
        cursor = get_db_cursor(engine.user, engine.password, engine.host, engine.port, engine.dbname)
        fact_table_name = get_fact_table_name(cursor)
        levels, level_attributes = create_levels_in_hierarchies(cursor)
        dimensions = create_dimensions(levels)
        measures = create_measures(get_measures(cursor, fact_table_name))
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
