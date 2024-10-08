from __future__ import annotations

from typing import Tuple, List, TYPE_CHECKING

import psycopg2
from psycopg2 import Error
from psycopg2.extensions import connection as psyconn
from psycopg2.extensions import cursor as psycur

if TYPE_CHECKING:
    from cube import Measure
from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.View import View
from cube.Predicate import Predicate
from cube.Dimension import Dimension
from engines import Postgres
from rdflib import Graph
from .cube_metadata import create_cube_metadata, create_cube
from .infer_cube import get_fact_table_name, create_levels, create_dimensions, get_measures, \
    create_measures, get_lowest_level_names, LowestLevelDTO, LevelDTO


class Session:
    def __init__(self, views: List[View], engine: Postgres):
        self._views: List[View] = views
        self._engine: Postgres = engine
        for view in views:
            setattr(self, view.name, view)

    @property
    def views(self) -> List[str]:
        return list(map(lambda v: str(v), self._views))

    def load_view(self, view_name: str) -> View | str:
        view_candidate: List[View] = list(filter(lambda x: x.cube.name == view_name, self._views))
        return view_candidate[0] if len(view_candidate) == 1 else f"No view found with name: {view_name}"


def attach_metadata_to_dimensions(dimensions: List[Dimension], metadata: Graph) -> None:
    for dimension in dimensions:
        if isinstance(dimension, Dimension):
            dimension.metadata = metadata


def get_default_axes(dimensions: List[Dimension]) -> List[Axis]:
    return []
    # I have commented out the members method for now
    # return list(map(lambda x: Axis(x.lowest_level(), x.lowest_level().members()), dimensions))


def get_default_measure(cube: BaseCube) -> List[Measure]:
    return cube.measure_list


def get_default_filters() -> List[Predicate]:
    return []


def create_view(cube: BaseCube) -> View:
    axes: List[Axis] = get_default_axes(cube.dimensions())
    measures: List[Measure] = get_default_measure(cube)
    predicates: None = None
    return View(axes, measures, predicates, cube, cube.name)


def create_session(engine: Postgres) -> Session:
    conn: psyconn
    cursor: psycur
    conn, cursor = get_db_connection_and_cursor(engine)
    try:
        fact_table_name: str = get_fact_table_name(cursor)
        lowest_levels: List[LowestLevelDTO] = get_lowest_level_names(cursor, fact_table_name)
        levelDTOs: List[List[LevelDTO]] = create_levels(cursor, lowest_levels, engine)

        # Renaming role playing dimensions
        rel_names = [x[len(x) - 1].name for x in levelDTOs]
        counter = 1
        for i, rel_name in enumerate(rel_names):
            if rel_name in rel_names[i + 1:]:
                levelDTOs[i][len(levelDTOs[i]) - 1].name = levelDTOs[i][len(levelDTOs[i]) - 1].name + str(counter)
                counter += 1

        dimensions: List[Dimension] = create_dimensions(levelDTOs, engine)
        measures: List[Measure] = create_measures(get_measures(cursor, fact_table_name), fact_table_name)
        metadata: Graph = create_cube_metadata(engine.dbname, dimensions, levelDTOs, measures)
        attach_metadata_to_dimensions(dimensions, metadata)
        cube: BaseCube = create_cube(fact_table_name, dimensions, measures, engine.dbname, metadata, engine)
        view: View = create_view(cube)
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
