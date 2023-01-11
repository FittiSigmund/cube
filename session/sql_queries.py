ALL_USER_TABLES_QUERY = """
        SELECT
            table_info.table_name
        FROM information_schema.tables AS table_info
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_info.table_type = 'BASE TABLE';
"""

def table_cardinality_query(table_name: str) -> str:
    return f"""SELECT COUNT(*) FROM {table_name};"""

def lowest_levels_query(fact_table_name: str) -> str:
    return f"""
        SELECT p.relname, kcu.column_name 
        FROM 
            pg_class AS p, 
            pg_constraint AS c, 
            information_schema.key_column_usage AS kcu 
        WHERE p.oid = c.confrelid 
        AND contype = 'f' 
        AND c.conrelid = (SELECT oid FROM pg_class WHERE relname = '{fact_table_name}') 
        AND c.conname = kcu.constraint_name
"""

# """
#         SELECT relname
#         FROM pg_class
#         WHERE oid IN
#             (SELECT confrelid
#             FROM pg_constraint
#             WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = '{fact_table_name}')
#             AND contype = 'f');
# """

def get_pk_and_fk_columns_query(level_name: str) -> str:
    return f"""
        SELECT k.column_name, tc.constraint_type 
        FROM information_schema.table_constraints AS tc, information_schema.key_column_usage AS k 
        WHERE tc.table_name = '{level_name}' 
        AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
        AND tc.constraint_name = k.constraint_name;
"""
def get_non_key_columns_query(level_name: str) -> str:
    return f"""
        SELECT info_col.column_name
        FROM information_schema.columns AS info_col
        WHERE info_col.table_name = '{level_name}'
        AND info_col.column_name NOT IN (
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage AS kcu 
            WHERE kcu.table_name = '{level_name}'
            )
"""

def get_next_level_query(current_level: str) -> str:
    return f"""
        SELECT relname 
        FROM pg_class 
        WHERE oid = 
            (SELECT confrelid 
            FROM pg_constraint 
            WHERE conrelid = 
                (SELECT oid 
                FROM pg_class 
                WHERE relname = '{current_level}') 
                AND contype = 'f')
"""

def get_all_measures_query(fact_table: str) -> str:
    return f"""
            SELECT pg_attribute.attname
            FROM pg_attribute
            WHERE pg_attribute.attrelid = (SELECT oid FROM pg_class WHERE pg_class.relname = '{fact_table}')
            AND pg_attribute.attnum >= 0
            AND pg_attribute.attname NOT IN (
                                         SELECT kcu.column_name 
                                         FROM information_schema.key_column_usage AS kcu 
                                         WHERE kcu.table_name = '{fact_table}'
                                         );
"""
