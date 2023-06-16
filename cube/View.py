from __future__ import annotations

from numbers import Number
from typing import List, Tuple, TYPE_CHECKING, Any, Dict

import pandas as pd
from sqlalchemy import create_engine, text

from cube.AggregateFunction import AggregateFunction
from cube.Attribute import Attribute
from cube.Axis import Axis
from cube.BaseCube import BaseCube
from cube.BooleanConnective import BooleanConnective
from cube.LevelMember import LevelMember
from cube.LevelMemberType import LevelMemberType
from cube.Measure import Measure
from cube.NonTopLevel import NonTopLevel
from cube.PredicateOperator import PredicateOperator

if TYPE_CHECKING:
    from cube.Predicate import Predicate


class View:
    def __init__(self,
                 axes: List[Axis] = None,
                 measures: List[Measure] = None,
                 predicates: Predicate = None,
                 cube: BaseCube = None) -> None:
        self.axes: List[Axis] = axes if axes else []
        self._measures: List[Measure] = measures if measures else []
        self.predicates: Predicate = predicates
        self.cube: BaseCube = cube

    # View method generally modifies self and returns self instead of creating and returning a new View
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

    def pages(self, lms: List[LevelMember]) -> View:
        return self.axis(2, lms)

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
        from_clause: str = self._create_from_clause()
        select_clause: str = self._create_select_clause()
        where_clause: str = self._create_where_clause()
        group_by_clause: str = self._create_group_by_clause()
        return select_clause + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"

    def _create_select_clause(self) -> str:
        levels: List[str] = list(map(lambda x: f"{x.level.alias}.{x.attribute.name} AS {x.level.alias}", self.axes))
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

    def _create_from_clause(self) -> str:
        subset_clauses: List[str] = []
        axis_lvls: List[NonTopLevel] = [x.level for x in self.axes]

        all_pred_lvls: List[NonTopLevel] = list(set(self._get_all_pred_levels(self.predicates)))
        pred_lvls: List[NonTopLevel] = [lvl for lvl in all_pred_lvls if lvl.dimension
                                        not in [x.dimension for x in axis_lvls]]

        for i, level in enumerate(axis_lvls + pred_lvls):
            subset_clauses.append(self._create_from_subset_clause(level, i))

        return f"FROM {self.cube.fact_table_name} " + " ".join(subset_clauses)

    def _get_all_pred_levels(self, pred: Predicate) -> List[NonTopLevel]:
        match pred.value:
            case Number():
                return []
            case str():
                return []
            case Measure():
                return []
            case Attribute():
                return [pred.value.level]
            case _:
                return self._get_all_pred_levels(pred.left_child) + self._get_all_pred_levels(pred.right_child)

    def _create_where_clause(self) -> str:
        axes: List[str] = self._create_axes_where_clause()
        axes: str = " AND ".join(axes) if axes else ""

        predicates: str = self._create_predicates_where_clause()
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
            result.append(f"{x.level.alias}.{x.attribute.name}")
            result.append(f"{x.level.alias}.{x.level.key}")
        return "GROUP BY " + ", ".join(result) if result else ""

    # # Does not work currently. Can not figure out how to assign tuples to cells
    # def _convert_to_df1(self) -> pd.DataFrame:
    #     levels: List[str] = list(map(lambda x: f"{x.level.name}.{x.attribute.name}", self.axes))

    #     measures: List[str] = list(
    #         map(lambda x: f"{x.aggregate_function.name}({x.sqlname}) AS {x.name}", self._measures))
    #     select_clause: str = "SELECT " + ", ".join(levels) + ", " + ", ".join(measures)
    #     select_distinct_list: List[str] = [f"SELECT DISTINCT {levels[i]}" for i in range(len(levels))]

    #     from_clause: str = self._create_from_clause()
    #     where_clause: str = self._create_where_clause()
    #     group_by_clause: str = self._create_group_by_clause()

    #     query: str = select_clause + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"
    #     query_distinct: List[str] = [distinct + " " + from_clause + " " + where_clause + " " + group_by_clause + ";"
    #                                  for distinct in select_distinct_list]

    #     db_result = self.cube.execute_query(query)
    #     db_result_distinct: List[List[Tuple[Any, ...]]] = [self.cube.execute_query(x) for x in query_distinct]

    #     column_results: List[List[Tuple[Any, ...]]] = [x for i, x in enumerate(db_result_distinct) if i % 2 == 0]
    #     row_results: List[List[Tuple[Any, ...]]] = [x for i, x in enumerate(db_result_distinct) if i % 2 == 1]

    #     columns: List[List[Any]] = []
    #     rows: List[List[Any]] = []
    #     for res in column_results:
    #         columns.append([x[0] for x in res])
    #     for res in row_results:
    #         rows.append([x[0] for x in res])

    #     df_columns = pd.MultiIndex.from_product(columns)
    #     df_rows = pd.MultiIndex.from_product(rows)
    #     df = pd.DataFrame(columns=df_columns, index=df_rows)
    #     for row in db_result:
    #         df.at[row[1], row[0]] = (row[2], row[3])
    #     return df

    def _convert_to_df(self, query: str) -> pd.DataFrame:
        engine = create_engine("postgresql+psycopg2://sigmundur:@localhost/ssb_snowflake")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            columns = [ax.attribute.level.alias for ax in [ax for i, ax in enumerate(self.axes) if i % 2 == 0]]
            rows = [ax.attribute.level.alias for ax in [ax for i, ax in enumerate(self.axes) if i % 2 == 1]]
            measures = [m.name for m in self._measures]
            final_df = df.pivot(columns=columns, index=rows, values=measures)
            final_df = final_df.reorder_levels(list(range(1, len(columns) + 1)) + [0], axis=1)
            # final_df.columns = final_df.columns.sortlevel(level=list(range(0, len(columns))))[0]
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
            map(lambda x: f"{x.level.alias}.{x.attribute.name} IN ({format_level_members(x, x.level_members)})",
                [x for x in self.axes if not x.attribute.all_lms_loaded]))

    def _create_predicates_where_clause(self) -> str:
        pred_list: List[str] = self._create_predicates_where_clause_aux(self.predicates)
        return " ".join(pred_list)

    def _create_predicates_where_clause_aux(self, pred: Predicate) -> List[str]:
        match pred.value:
            case BooleanConnective():
                left_child: List[str] = self._create_predicates_where_clause_aux(pred.left_child)
                right_child: List[str] = self._create_predicates_where_clause_aux(pred.right_child)
                return ["("] + left_child + [pred.value.value] + right_child + [")"]
            case PredicateOperator():
                left_child: List[str] = self._create_predicates_where_clause_aux(pred.left_child)
                right_child: List[str] = self._create_predicates_where_clause_aux(pred.right_child)
                return left_child + [pred.value.value] + right_child
            case _:
                return [self._format_predicate_value(pred)]

    def _format_predicate_value(self, pred: Predicate) -> str:
        match pred.value:
            case Attribute():
                return f"{pred.value.level.alias}.{pred.value.name}"
            case Measure():
                return f"{pred.value.sqlname}"
            case str():
                return f"'{pred.value}'"
            case int():
                return str(pred.value)

    def _create_from_subset_clause(self, level: NonTopLevel, counter: int) -> str:
        # The order in hierarchy is the lowest level first and highest last
        hierarchy: List[NonTopLevel] = self._get_children(level) + [level] + self._get_parents(level)
        try:
            result: List[str] = [
                self._create_on_condition_for_fact_table(self.cube.fact_table_name, hierarchy[0], counter)
            ]
            for i in range(len(hierarchy) - 1):
                result.append(self._create_on_condition(hierarchy[i], hierarchy[i + 1], counter))
        except IndexError as e:
            print(f"IndexError: {e}")
            return ""
        return "JOIN " + " JOIN ".join(result)

    def _create_on_condition_for_fact_table(self, fact_table: str, level: NonTopLevel, counter: int) -> str:
        level.alias = f"{level.name}{counter}"
        return f"{level.name} AS {level.alias} ON {fact_table}.{level.dimension.fact_table_fk} = {level.alias}.{level.key}"

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

    def _create_on_condition(self, child: NonTopLevel, parent: NonTopLevel, counter: int) -> str:
        parent.alias = f"{parent.table_name}{counter}"
        return f"{parent.table_name} AS {parent.alias} ON {child.alias}.{child.fk_name} = {parent.alias}.{parent.key}"
