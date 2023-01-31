from itertools import product
from typing import List, Dict

from sklearn.utils.extmath import cartesian
import numpy as np
from psycopg2._psycopg import cursor

import psycopg2

conn = psycopg2.connect(
    dbname="salesdb_snowflake_gen",
    user="sigmundur",
    password=""
)

name_table: Dict[str, List[str]] = {}
table_tuples: Dict[str, int] = {}
max_num_of_tuples = 0

def create_dimensions(cur: cursor, num_dimensions: int, num_levels: List[int]) -> None:
    if len(num_levels) != num_dimensions:
        raise Exception("num levels != num dimensions")
    dimensions: List[str] = ["dimension" + str(i) for i in range(num_dimensions)]
    for i, dimension in enumerate(dimensions):
        create_dimension(cur, dimension, num_levels[i])
def create_dimension(cur: cursor, dimension: str, num_levels: int) -> None:
    levels: List[str] = ["level" + str(i) for i in range(num_levels)]
    name_table[dimension] = levels
    for i, level in enumerate(levels):
        if i == 0:
            cur.execute(f"CREATE TABLE {dimension}_{level}({level}_id SERIAL PRIMARY KEY, {level} TEXT);")
        else:
            cur.execute(create_level_query(dimension, level, levels[i - 1]))
def create_level_query(dimension: str, child_level: str, parent_level: str) -> str:
    return f"CREATE TABLE {dimension}_{child_level}({child_level}_id SERIAL PRIMARY KEY, {child_level} TEXT, {parent_level}_id INTEGER REFERENCES {dimension}_{parent_level}({parent_level}_id));"

def delete_dimensions(cur: cursor) -> None:
    for key in name_table.keys():
        for level in list(reversed(name_table[key])):
            cur.execute(f"DROP TABLE {key}_{level};")

def add_level_members_to_dimensions(cur: cursor, starting_numbers: List[int]) -> None:
    global max_num_of_tuples
    if len(starting_numbers) != len(name_table.keys()):
        raise Exception("starting numbers != name table keys")
    for i, key in enumerate(name_table.keys()):
        new_num_members: int = starting_numbers[i]
        for j, level in enumerate(list(name_table[key])):
            if j == 0:
                add_level_members_to_fact_table(cur, key, level, new_num_members)
            else:
                add_level_members_to_dimension(cur, key, level, starting_numbers[i], new_num_members)
                new_num_members = new_num_members * starting_numbers[i]
            max_num_of_tuples = new_num_members if new_num_members > max_num_of_tuples else max_num_of_tuples

def add_level_members_to_fact_table(cur: cursor, dimension: str, level: str, amount: int) -> None:
    values: str = ", ".join([f"(DEFAULT, \'member{str(i)}\')" for i in range(1, amount + 1)])
    table_tuples[f"{dimension}_{level}"] = amount
    cur.execute(f"INSERT INTO {dimension}_{level} VALUES {values};")

def add_level_members_to_dimension(cur: cursor, dimension: str, level: str, child_amount: int, parent_amount: int) -> None:
    value_list: List[str] = []
    for i in range(1, parent_amount + 1):
        value_list = value_list + [f"(DEFAULT, \'member{str(j)}\', {str(i)})" for j in range(1, child_amount + 1)]
    table_tuples[f"{dimension}_{level}"] = len(value_list)
    values: str = ", ".join(value_list)
    cur.execute(f"INSERT INTO {dimension}_{level} VALUES {values};")

def create_fact_table(cur: cursor, num_of_measures: int) -> None:
    dimensions = "".join([f"{dimension}_{level[0]} INTEGER REFERENCES {dimension}_{level[1]}({level[1]}_id), "
                                    for dimension, level in name_table.items()])
    primary_key = "PRIMARY KEY (" + ", ".join([f"{dimension}_{level[0]}" for dimension, level in name_table.items()]) \
                  + ")"
    measures = ", ".join([f"measure{str(i)} NUMERIC" for i in range(num_of_measures)])
    query = f"CREATE TABLE fact_table({dimensions} {measures}, {primary_key});"
    cur.execute(query)

def delete_fact_table(cur: cursor) -> None:
    cur.execute("DROP TABLE fact_table;")

def add_facts_to_fact_table(cur: cursor, num_of_tuples: int) -> None:
    if num_of_tuples < max_num_of_tuples:
        raise Exception("num of tuples < max num of tuples")
    dimensions = []
    for k, v in name_table.items():
        dimensions.append(list(range(1, table_tuples[f"{k}_{list(reversed(v))[0]}"] + 1)))

    values = []
    for i, t in enumerate(product(*dimensions)):
        values.append(str(t + (i,)))
    values = ", ".join(values)
    cur.execute(f"INSERT INTO fact_table VALUES {values};")


with conn:
    with conn.cursor() as curs:
        create_dimensions(curs, 2, [2, 3])
        create_fact_table(curs, 1)
        add_level_members_to_dimensions(curs, [2, 3])
        add_facts_to_fact_table(curs, 700)

with conn:
    with conn.cursor() as curs:
        delete_fact_table(curs)
        delete_dimensions(curs)

conn.close()