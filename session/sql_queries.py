ALL_USER_TABLES_QUERY = """
        SELECT
            table_info.table_name
        FROM information_schema.tables AS table_info
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_info.table_type = 'BASE TABLE';
"""

TABLE_CARDINALITY_QUERY = lambda table_name: f"""SELECT COUNT(*) FROM {table_name};"""

LOWEST_LEVELS_QUERY = """
        SELECT relname 
        FROM pg_class 
        WHERE oid IN 
            (SELECT confrelid 
            FROM pg_constraint 
            WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'sales') 
            AND contype = 'f');
"""

GET_NON_KEY_COLUMNS_QUERY = lambda level_name: f"""
        SELECT info_col.column_name
        FROM information_schema.columns AS info_col
        WHERE info_col.table_name = '{level_name}'
        AND info_col.column_name NOT IN (
            SELECT kcu.column_name 
            FROM information_schema.key_column_usage AS kcu 
            WHERE kcu.table_name = '{level_name}'
            )
"""

GET_NEXT_LEVEL_QUERY = lambda current_level: f"""
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

GET_ALL_MEASURES_QUERY = lambda fact_table: f"""
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
