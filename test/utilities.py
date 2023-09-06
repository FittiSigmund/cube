import psycopg2

import engines

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "salesdb_snowflake_test"


class Utilities:
    @staticmethod
    def get_cursor():
        connection = psycopg2.connect(user=DATABASE_USER,
                                      password=DATABASE_PASSWORD,
                                      host=DATABASE_HOST,
                                      port=DATABASE_PORT,
                                      database=DATABASE_NAME)
        return connection.cursor()

    @staticmethod
    def get_engine():
        return engines.postgres(dbname=DATABASE_NAME,
                                user=DATABASE_USER,
                                password=DATABASE_PASSWORD,
                                host=DATABASE_HOST,
                                port=DATABASE_PORT)
