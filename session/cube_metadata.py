from typing import List

from rdflib import Graph, Namespace, BNode
from rdflib.namespace import RDF, QB

from cube.BaseCube import BaseCube
from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from cube.TopLevel import TopLevel
from engines import Postgres
from session.infer_cube import LevelDTO

EG = Namespace("http://example.org/")
QB4O = Namespace("http://purl.org/qb4olap/cubes/")


def create_cube_metadata(dsd_name: str,
                         dimensions: List[RegularDimension],
                         levelDTOs: List[List[LevelDTO]],
                         measures: List[Measure]) -> Graph:
    ## TODO: Generate proper URIs (or use proper prefix)
    metadata = initialize_rdf_graph()
    dsd_node = create_dsd_node(dsd_name)
    add_data_structure_definition(metadata, dsd_node)
    create_metadata_for_dimensions(dimensions, metadata, dsd_node)
    create_metadata_for_level_attributes(metadata, levelDTOs)
    create_metadata_for_measures(measures, metadata, dsd_node)
    # print(metadata.serialize())
    return metadata


def initialize_rdf_graph() -> Graph:
    g: Graph = Graph()
    g.bind("eg", EG)
    g.bind("qb4o", QB4O)
    return g


def create_dsd_node(dbname: str):
    dsd_name = dbname + "_dsd"
    return EG[dsd_name]


def add_data_structure_definition(metadata, dsd_node):
    metadata.add((dsd_node, RDF.type, QB.DataStructureDefinition))


def create_metadata_for_dimensions(dimensions, metadata, dsd_node):
    list(map(lambda x: create_metadata_for_dimension(x, metadata, dsd_node), dimensions))


def create_metadata_for_dimension(dimension, metadata, dsd_node):
    blank_node = BNode()
    dimension_node = EG[dimension.name]

    metadata.add((dsd_node, QB.component, blank_node))
    metadata.add((blank_node, QB4O.level, EG[dimension.lowest_level().name]))

    metadata.add((dimension_node, RDF.type, QB.DimensionProperty))

    level = dimension.lowest_level()
    while not isinstance(level.parent, TopLevel):
        level_node = EG[level.name]
        parent_level_node = EG[level.parent.name]
        metadata.add((level_node, RDF.type, QB4O.LevelProperty))
        metadata.add((level_node, QB4O.inDimension, dimension_node))
        metadata.add((level_node, QB4O.parentLevel, parent_level_node))
        level = level.parent

    level_node = EG[level.name]
    metadata.add((level_node, RDF.type, QB4O.LevelProperty))
    metadata.add((level_node, QB4O.inDimension, dimension_node))


def create_metadata_for_level_attributes(metadata, level_dto_list_list):
    list(map(lambda x: create_metadata_for_level_attribute(metadata, x[1:]), level_dto_list_list))


def create_metadata_for_level_attribute(metadata, level_dto_list):
    for dto in level_dto_list:
        for attribute in dto.attributes:
            metadata.add((EG[attribute], RDF.type, QB.AttributeProperty))
            metadata.add((EG[dto.name], QB4O.hasAttribute, EG[attribute]))


def create_metadata_for_measures(measures, metadata, dsd_node):
    # Need the call to list, because map is lazily evaluated
    list(map(lambda x: create_metadata_for_measure(x, metadata, dsd_node), measures))


def create_metadata_for_measure(measure, metadata, dsd_node):
    blank_measure_node = BNode()
    metadata.add((dsd_node, QB.component, blank_measure_node))
    ## TODO: Add mapping from aggregate function names to qb4o names
    metadata.add((blank_measure_node, QB4O.hasAggregateFunction, QB4O.sum))
    metadata.add((blank_measure_node, QB.measure, EG[measure.name]))
    metadata.add((EG[measure.name], RDF.type, QB.MeasureProperty))


def create_cube(fact_table_name: str,
                dimensions: List[RegularDimension],
                measures: List[Measure],
                dbname: str,
                metadata: Graph,
                engine: Postgres) -> BaseCube:
    cube: BaseCube = BaseCube(fact_table_name, dimensions, measures, dbname, metadata, engine)
    cube.base_cube = cube
    return cube
