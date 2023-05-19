from __future__ import annotations

from typing import List, Tuple, TYPE_CHECKING, Any, Dict

import pandas as pd
from sqlalchemy import create_engine, text

from cube.AggregateFunction import AggregateFunction
from cube.Attribute import Attribute
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

    def measures(self, *args: Measure, **kwargs: Dict[str, str | AggregateFunction]) -> View:
        self._measures = list(args)
        if kwargs:
            calculated_measures: List[Measure] = []
            for k, v in kwargs.items():
                calculated_measures.append(Measure(k, v["function"], v["sqlname"]))
            self._measures += calculated_measures
        return self

    # Check for axes before converting
    def output(self, hack=False) -> pd.DataFrame:
        query: str = self._create_sql_query()
        if hack:
            result = self._convert_to_df_hack(query)
        else:
            result = self._convert_to_df(query)
        return result

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
            map(lambda x: f"{x.aggregate_function.name}({x.sqlname}) AS {x.name}", self._measures))
        # HACK
        if levels and measures:
            return "SELECT " + ", ".join(levels) + ", " + ", ".join(measures)
        if levels:
            return "SELECT " + ", ".join(levels)
        if measures:
            return "SELECT " + ", ".join(measures)
        else:
            return "SELECT "

    # Add support for role playing dimensions (i.e., alias the tables when self joining)
    def _create_from_clause(self) -> str:
        subset_clauses: List[str] = []
        axis_lvls: List[NonTopLevel] = [x.level for x in self.axes]

        all_pred_lvls: List[NonTopLevel] = [pred.attribute.level for pred in self._get_all_predicates()]
        pred_lvls: List[NonTopLevel] = [lvl for lvl in all_pred_lvls if lvl.dimension
                                        not in [x.dimension for x in axis_lvls]]

        for level in axis_lvls + pred_lvls:
            subset_clauses.append(self._create_from_subset_clause(level))

        return f"FROM {self.cube.fact_table_name} " + " ".join(subset_clauses)

    def _get_all_predicates(self) -> List[Predicate]:
        if not self.predicates:
            return []
        current_pred = self.predicates
        # HACK
        result: List[Predicate] = [current_pred] if type(current_pred.attribute) == Attribute else []
        while current_pred.next_pred is not None:
            current_pred = current_pred.next_pred
            # HACK
            if type(current_pred.attribute) == Attribute:
                result.append(current_pred)
        return result

    def _create_where_clause(self) -> str:
        axes: List[str] = self._create_axes_where_clause()
        axes: str = " AND ".join(axes) if axes else ""

        predicates: List[str] = self._create_predicates_where_clause()
        predicates: str = " AND ".join(predicates)
        if axes and predicates:
            return "WHERE " + axes + " AND " + predicates
        elif axes:
            return "WHERE " + axes
        elif predicates:
            return "WHERE " + predicates
        else:
            return ""

    def _create_group_by_clause(self) -> str:
        result: List[str] = []
        for x in self.axes:
            result.append(f"{x.level.name}.{x.attribute.name}")
            result.append(f"{x.level.name}.{x.level.key}")
        return "GROUP BY " + ", ".join(result) if result else ""

    # Does not work currently. Can not figure out how to assign tuples to cells
    def _convert_to_df1(self) -> pd.DataFrame:
        levels: List[str] = list(map(lambda x: f"{x.level.name}.{x.attribute.name}", self.axes))

        measures: List[str] = list(
            map(lambda x: f"{x.aggregate_function.name}({x.sqlname}) AS {x.name}", self._measures))
        select_clause: str = "SELECT " + ", ".join(levels) + ", " + ", ".join(measures)
        select_distinct_list: List[str] = [f"SELECT DISTINCT {levels[i]}" for i in range(len(levels))]

        from_clause: str = self._create_from_clause()
        where_clause: str = self._create_where_clause()
        group_by_clause: str = self._create_group_by_clause()

        query: str = select_clause + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"
        query_distinct: List[str] = [distinct + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"
                                     for distinct in select_distinct_list]

        db_result = self.cube.execute_query(query)
        db_result_distinct: List[List[Tuple[Any, ...]]] = [self.cube.execute_query(x) for x in query_distinct]

        column_results: List[List[Tuple[Any, ...]]] = [x for i, x in enumerate(db_result_distinct) if i % 2 == 0]
        row_results: List[List[Tuple[Any, ...]]] = [x for i, x in enumerate(db_result_distinct) if i % 2 == 1]

        columns: List[List[Any]] = []
        rows: List[List[Any]] = []
        for res in column_results:
            columns.append([x[0] for x in res])
        for res in row_results:
            rows.append([x[0] for x in res])

        df_columns = pd.MultiIndex.from_product(columns)
        df_rows = pd.MultiIndex.from_product(rows)
        df = pd.DataFrame(columns=df_columns, index=df_rows)
        for row in db_result:
            df.at[row[1], row[0]] = (row[2], row[3])
        return df

    def _convert_to_df(self, query: str) -> pd.DataFrame:
        engine = create_engine("postgresql+psycopg2://sigmundur:@localhost/ssb_snowflake")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            df["Measures"] = df[df.columns[len(self.axes):]].apply(lambda x: tuple((x[i] for i in range(len(x)))),
                                                                   axis=1)
            columns = [ax.attribute.name for ax in [ax for i, ax in enumerate(self.axes) if i % 2 == 0]]
            rows = [ax.attribute.name for ax in [ax for i, ax in enumerate(self.axes) if i % 2 == 1]]
            final_df = df.pivot(columns=columns, index=rows, values="Measures")
        engine.dispose()
        return final_df

    def _convert_to_df_hack(self, query: str) -> pd.DataFrame:
        engine = create_engine("postgresql+psycopg2://sigmundur:@localhost/ssb_snowflake")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        engine.dispose()
        return df

    def _create_axes_where_clause(self) -> List[str]:
        def format_level_members(a: Axis, lms: List[LevelMember]) -> str:
            if a.type == LevelMemberType.STR:
                return ", ".join(list(map(lambda x: f"'{x.name}'", lms)))
            elif a.type == LevelMemberType.INT:
                return ", ".join(list(map(lambda x: f"{x.name}", lms)))

        return list(
            map(lambda x: f"{x.level.name}.{x.attribute.name} IN ({format_level_members(x, x.level_members)})",
                self.axes))

    def _create_predicates_where_clause(self) -> List[str]:
        def format_predicates(p: Predicate):
            if p.level_member_type is LevelMemberType.STR:
                return f"{p.attribute.level.name}.{p.attribute.name} {p.operator.value} '{p.value}'"
            elif p.level_member_type is LevelMemberType.INT:
                return f"{p.attribute.level.name}.{p.attribute.name} {p.operator.value} {p.value}"

        if not self.predicates:
            return []
        pred = self.predicates
        result: List[str] = [format_predicates(pred)]
        while pred.next_pred is not None:
            pred = pred.next_pred
            # HACK
            if type(pred.attribute) == Measure:
                result.append(f"{self.cube.fact_table_name}.{pred.attribute.name} {pred.operator.value} {pred.value}")
            else:
                result.append(format_predicates(pred))
        return result

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
        return f"{level.name} ON {fact_table}.{level.dimension.fact_table_fk} = {level.name}.{level.key}"

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
        return f"{parent.table_name} ON {child.table_name}.{child.fk_name} = {parent.table_name}.{parent.key}"
