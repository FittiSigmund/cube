from itertools import groupby
from operator import itemgetter
from typing import Dict, List

from cube.Cube import Cube
from cube.Cuboid import Cuboid
from cube.Level import Level
from cube.NonTopLevel import NonTopLevel
from cube.RegularDimension import RegularDimension
from cube.TopLevel import TopLevel


def get_new_levels(level: NonTopLevel) -> List[Level]:
    new_levels: List[Level] = [level]
    while level.parent is not level:
        level: Level = level.parent
        new_levels.append(level)
    return new_levels


def create_new_regular_dimension(dimension: RegularDimension, levels: List[Level]) -> RegularDimension:
    return RegularDimension(dimension.name, levels, dimension.engine, dimension.fact_table_fk)


def create_new_dimension_list(dimension_list: List[RegularDimension], cube: Cube) -> List[RegularDimension]:
    result = dimension_list
    result_name = list(map(lambda x: x.name, result))
    for old_dimension in cube.dimension_list:
        if old_dimension.name not in result_name:
            result.append(old_dimension)
    return result


def rollup(cube: Cube, **kwargs: str) -> Cube:
    dimension_names_list = list(map(lambda x: x.name, cube.dimension_list))
    new_dimension_list: List[RegularDimension] = []
    for key in kwargs.keys():
        _check_if_cube_contains_dimension(cube, dimension_names_list, key)
        dimension: RegularDimension = _get_dimension(cube, key)
        level_names_list = _get_level_list_names(dimension, including=False)
        _check_if_dimension_contains_level(cube, dimension, key, kwargs, level_names_list)
        if kwargs[key] == "ALL":
            new_levels: List[TopLevel] = _get_the_all_level(dimension, rollup=True)
            new_dimension_list.append(create_new_regular_dimension(dimension, new_levels))
        else:
            new_dimension_list.append(_get_levels_and_create_dimension(dimension, key, kwargs))
    dimension_list = create_new_dimension_list(new_dimension_list, cube)
    return Cuboid(dimension_list, cube.measure_list, cube.engine, cube, cube.base_cube)


def drilldown(cube: Cube, **kwargs: str) -> Cube:
    dimension_names_list = list(map(lambda x: x.name, cube.base_cube.dimension_list))
    new_dimension_list: List[RegularDimension] = []
    for key in kwargs.keys():
        _check_if_cube_contains_dimension(cube, dimension_names_list, key)
        dimension: RegularDimension = _get_dimension(cube, key)
        base_dimension: RegularDimension = _get_dimension(cube.base_cube, key)
        level_list_names: List[str] = _get_level_list_names(dimension, including=True)
        _check_if_level_already_contained_in_dimension(dimension, key, kwargs, level_list_names)
        base_level_list_names: List[str] = _get_level_list_names(base_dimension, including=False)
        _check_if_dimension_contains_level(cube, dimension, key, kwargs, base_level_list_names)
        if kwargs[key] == "ALL":
            new_levels: List[TopLevel] = _get_the_all_level(base_dimension, rollup=False)
            new_dimension_list.append(create_new_regular_dimension(base_dimension, new_levels))
        else:
            new_dimension_list.append(_get_levels_and_create_dimension(base_dimension, key, kwargs))
    dimension_list = create_new_dimension_list(new_dimension_list, cube)
    return Cuboid(dimension_list, cube.measure_list, cube.engine, cube, cube.base_cube)


def _get_levels_and_create_dimension(dimension: RegularDimension, key: str, kwargs: Dict[str, str]) -> RegularDimension:
    level: NonTopLevel = getattr(dimension, kwargs[key])
    new_levels: List[Level] = get_new_levels(level)
    return create_new_regular_dimension(dimension, new_levels)


def _check_if_dimension_contains_level(cube: Cube, dimension: RegularDimension, key: str, kwargs: Dict[str, str],
                                       level_names_list: List[str]) -> None:
    if kwargs[key] not in level_names_list:
        raise ValueError(f"'{dimension.name}' dimension in '{cube}' cube has no level '{kwargs[key]}'")


def _check_if_level_already_contained_in_dimension(dimension: RegularDimension, key: str, kwargs: Dict[str, str],
                                                   level_list_names: List[str]) -> None:
    if kwargs[key] in level_list_names:
        raise ValueError(
            f"Cannot drill up. Trying to drill down to '{kwargs[key]}' level, while '{dimension.level_list[0].name}' is the "
            f"lowest level in '{dimension.name}' dimension.")


def _get_level_list_names(dimension: RegularDimension, including: bool):
    result = list(map(lambda x: x.name, dimension.level_list))
    return result[1:] if including else result


def _check_if_cube_contains_dimension(cube: Cube, dimension_names_list: List[str], key: str) -> None:
    if key not in dimension_names_list:
        raise ValueError(f"'{cube}' cube has no dimension '{key}'")


def _get_dimension(cube: Cube, key: str) -> RegularDimension:
    return getattr(cube, key)


def _get_the_all_level(dimension: RegularDimension, rollup: bool) -> List[TopLevel]:
    if rollup:
        return list(filter(lambda x: isinstance(x, TopLevel), dimension.level_list))
    else:
        # Drilling down to ALL is only possible if dimension only contains the ALL level
        return [dimension.level_list[0]]
