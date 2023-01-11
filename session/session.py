from typing import Tuple, List

import psycopg2
from psycopg2 import Error
from psycopg2.extensions import connection as psyconn
from psycopg2.extensions import cursor as psycur

from cube import Measure
from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.Cube import Cube
from cube.CubeView import CubeView
from cube.Filter import Filter
from cube.RegularDimension import RegularDimension
from engines import Postgres
from rdflib import Graph
from .cube_metadata import create_cube_metadata, create_cube
from .infer_cube import get_fact_table_name, create_levels, create_dimensions, get_measures, \
    create_measures, get_lowest_level_names, LowestLevelDTO, LevelDTO


class Session:
    def __init__(self, views: List[CubeView], engine: Postgres):
        self._views: List[CubeView] = views
        self._engine: Postgres = engine

    @property
    def cubes(self) -> List[CubeView]:
        return self._views

    def load_view(self, cube_name: str) -> CubeView | str:
        view_candidate: List[CubeView] = list(filter(lambda x: x.name == cube_name, self._views))
        return view_candidate[0] if len(view_candidate) == 1 else f"No cube found with name: {cube_name}"


def attach_metadata_to_dimensions(dimensions: List[RegularDimension], metadata: Graph) -> None:
    for dimension in dimensions:
        if isinstance(dimension, RegularDimension):
            dimension.metadata = metadata


def get_default_axes(dimensions: List[RegularDimension]) -> List[Axis]:
    return list(map(lambda x: Axis(x.lowest_level(), x.lowest_level().members()), dimensions))


def get_default_measure(cube: BaseCube) -> List[Measure]:
    return [cube.default_measure]


def get_default_filters() -> List[Filter]:
    return []


def create_view(cube: BaseCube) -> CubeView:
    axes: List[Axis] = get_default_axes(cube.dimensions())
    measures: List[Measure] = get_default_measure(cube)
    filters: List[Filter] = get_default_filters()
    return CubeView(axes, measures, filters, cube)


def create_session(engine: Postgres) -> Session:
    conn: psyconn
    cursor: psycur
    conn, cursor = get_db_connection_and_cursor(engine)
    try:
        fact_table_name: str = get_fact_table_name(cursor)
        lowest_levels: List[LowestLevelDTO] = get_lowest_level_names(cursor, fact_table_name)
        levelDTOs: List[List[LevelDTO]] = create_levels(cursor, lowest_levels, engine)
        dimensions: List[RegularDimension] = create_dimensions(levelDTOs, engine)
        measures: List[Measure] = create_measures(get_measures(cursor, fact_table_name))
        metadata: Graph = create_cube_metadata(engine.dbname, dimensions, levelDTOs, measures)
        attach_metadata_to_dimensions(dimensions, metadata)
        cube: BaseCube = create_cube(fact_table_name, dimensions, measures, engine.dbname, metadata, engine)
        view: CubeView = create_view(cube)
        return Session([view], engine)
    except (Exception, Error) as error:
        print("ERROR: ", error)
    finally:
        cursor.close()
        conn.close()


def get_db_connection_and_cursor(engine: Postgres) -> Tuple[psyconn, psycur]:
    conn = psycopg2.connect(user=engine.user,
                            password=engine.password,
                            host=engine.host,
                            port=engine.port,
                            database=engine.dbname)
    return conn, conn.cursor()
