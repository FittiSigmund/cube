from __future__ import annotations

from typing import List, Tuple, TYPE_CHECKING

from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.LevelMember import LevelMember
from cube.LevelMemberType import LevelMemberType
from cube.Measure import Measure
from cube.NonTopLevel import NonTopLevel

if TYPE_CHECKING:
    from cube.Predicate import Predicate


class View:
    def __init__(self,
                 axes: List[Axis] = None,
                 measures: List[Measure] = None,
                 filters: Predicate = None,
                 cube: BaseCube | None = None) -> None:
        self.axes: List[Axis] = axes if axes else []
        self._measures: List[Measure] = measures if measures else []
        self.predicates: Predicate = filters
        self.cube: BaseCube = cube

    # Checks not implemented
    # All level members same
    def axis(self, ax: int, lms: List[LevelMember]) -> View:
        if lms:
            lm = lms[0]
            new_axis = Axis(lm.attribute.level.dimension, lm.attribute.level, lm.attribute, lms)
        else:
            raise Exception("Empty Level Member list")
        self.axes.insert(ax, new_axis)
        return self

    def columns(self, lms: List[LevelMember]) -> View:
        return self.axis(0, lms)

    def rows(self, lms: List[LevelMember]) -> View:
        return self.axis(1, lms)

    def where(self, predicate: Predicate) -> View:
        self.predicates = predicate
        return self

    def measures(self, *args: Measure) -> View:
        self._measures = list(args)
        return self

    def output(self) -> str:
        query: str = self._create_sql_query()
        return query

    def __getattr__(self, item):
        return self.cube.__getattribute__(item)

    def _create_sql_query(self) -> str:
        select_clause: str = self._create_select_clause()
        from_clause: str = self._create_from_clause()
        where_clause: str = self._create_where_clause()
        group_by_clause: str = self._create_group_by_clause()
        return select_clause + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"

    def _create_select_clause(self) -> str:
        levels: List[str] = list(map(lambda x: f"{x.level.name}.{x.attribute.name}", self.axes))
        measures: List[str] = list(
            map(lambda x: f"{x.aggregate_function.name}({self.cube.fact_table_name}.{x.name})", self._measures))
        return "SELECT " + ", ".join(levels) + ", " + ", ".join(measures)

    def _create_from_clause(self) -> str:
        subset_clauses: List[str] = []
        ## Refactor _create_from_subset_clause() method to take in attribute as parameter
        ## Then when I will refactor measures to include (among others) attributes i can use same method for both
        ## Attributes and Measures
        for level in list(map(lambda x: x.level, self.axes)):
            subset_clauses.append(self._create_from_subset_clause(level))
        return f"FROM {self.cube.fact_table_name} " + " ".join(subset_clauses)

    def _create_where_clause(self) -> str:
        axes: List[str] = self._create_axes_where_clause()
        axes: str = " AND ".join(axes)

        # Hardcoded filters for testing
        # self._filters.append(Filter(self._axes[0].level, list(filter(lambda x: x.name == "January", self._axes[0].level_members))[0], FilterOperator.EQ))
        # self._filters.append(Filter(self._axes[0].level.child, list(filter(lambda x: x.name == 7, self._axes[0].level.child._level_members))[0], FilterOperator.LEQ))

        filters: List[str] = self._create_filters_where_clause()
        filters: str = " AND ".join(filters)
        conditions: str = axes + " AND " + filters if filters else axes
        return f"WHERE " + conditions

    def _create_group_by_clause(self) -> str:
        result: List[str] = []
        for level in list(map(lambda x: x.level, self.axes)):
            result.append(self._create_group_by_clause_for_level(level))
        return "GROUP BY " + ", ".join(result)

    def _create_group_by_clause_for_level(self, l: NonTopLevel):
        return ", ".join(list(map(lambda x: f"{x.table_name}.{x.column_name}", [l] + self._get_parents(l))))

    def _create_axes_where_clause(self) -> List[str]:
        def format_level_members(a: Axis, lms: List[LevelMember]) -> str:
            if a.type == LevelMemberType.STR:
                return ", ".join(list(map(lambda x: f"'{x.name}'", lms)))
            elif a.type == LevelMemberType.INT:
                return ", ".join(list(map(lambda x: f"{x.name}", lms)))

        return list(
            map(lambda x: f"{x.level.name}.{x.level.column_name} IN ({format_level_members(x, x.level_members)})",
                self.axes))

    def _create_filters_where_clause(self) -> List[str]:
        def format_filters(f: Predicate):
            if f.level_member_type is LevelMemberType.STR:
                return f"{f.level.table_name}.{f.level.column_name} {f.operator.value} '{f.value.name}'"
            elif f.level_member_type is LevelMemberType.INT:
                return f"{f.level.name}.{f.level.column_name} {f.operator.value} {f.value.name}"

        return list(map(lambda x: format_filters(x), self.predicates))

    def _create_from_subset_clause(self, level: NonTopLevel) -> str:
        # The order in hierarchy is the lowest level first and highest last
        hierarchy: List[NonTopLevel] = self._get_children(level) + [level] + self._get_parents(level)
        try:
            result: List[str] = [self._create_on_condition_for_fact_table(self.cube.fact_table_name, hierarchy[0])]
            for i in range(len(hierarchy) - 1):
                result.append(self._create_on_condition(hierarchy[i], hierarchy[i + 1]))
        except IndexError as e:
            print(f"IndexError: {e}")
            return ""
        return "JOIN " + " JOIN ".join(result)

    def _create_on_condition_for_fact_table(self, fact_table: str, level: NonTopLevel) -> str:
        return f"{level.name} ON {fact_table}.{level.dimension.fact_table_fk} = {level.name}.{level.pk_name}"

    def _get_children(self, level: NonTopLevel) -> List[NonTopLevel]:
        result: List[NonTopLevel] = []
        while level != level.child:
            level: NonTopLevel = level.child
            result.append(level)
        return list(reversed(result))

    def _get_parents(self, level: NonTopLevel) -> List[NonTopLevel]:
        result: List[NonTopLevel] = []
        while level != level.parent:
            level = level.parent
            if isinstance(level, NonTopLevel):
                result.append(level)
        return list(result)

    def _create_on_condition(self, child: NonTopLevel, parent: NonTopLevel):
        return f"{parent.table_name} ON {child.table_name}.{child.fk_name} = {parent.table_name}.{parent.pk_name}"
