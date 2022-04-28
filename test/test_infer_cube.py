import unittest

import psycopg2

from cube.RegularDimension import RegularDimension
from session.infer_cube import get_all_table_names, get_fact_table_name, get_lowest_level_names

DATABASE_USER = "sigmundur"
DATABASE_PASSWORD = ""
DATABASE_HOST = "127.0.0.1"
DATABASE_PORT = "5432"
DATABASE_NAME = "salesdb_snowflake"

expected_tables = ['date_day', 'date_month', 'date_year', 'product_category', 'product_name', 'sales', 'store_address',
                   'store_city', 'store_county', 'supplier_continent', 'supplier_name', 'supplier_nation']
expected_lowest_level_names = ['date_day', 'product_name', 'store_address', 'supplier_name']


class TestInferCube(unittest.TestCase):
    cursor = None

    @classmethod
    def setUpClass(cls):
        cls.cursor = cls.get_cursor()

    @classmethod
    def tearDownClass(cls):
        cls.cursor.close()

    def setUp(self):
        self.fact_table_name = get_fact_table_name(self.cursor)
        self.addTypeEqualityFunc(RegularDimension, dimension_compare)

    @staticmethod
    def get_cursor():
        connection = psycopg2.connect(user=DATABASE_USER,
                                      password=DATABASE_PASSWORD,
                                      host=DATABASE_HOST,
                                      port=DATABASE_PORT,
                                      database=DATABASE_NAME)
        return connection.cursor()

    def test_all_user_tables_query(self):
        actual_tables_unsorted = get_all_table_names(self.cursor)
        actual_tables = sorted(actual_tables_unsorted)
        self.assertEqual(expected_tables, actual_tables)

    def test_get_fact_table_name(self):
        expected_fact_table_name = 'sales'
        self.assertEqual(expected_fact_table_name, self.fact_table_name)

    def test_get_lowest_level_names(self):
        actual_lowest_level_names_unsorted = get_lowest_level_names(self.cursor, self.fact_table_name)
        actual_lowest_level_names = sorted(actual_lowest_level_names_unsorted)
        self.assertEqual(expected_lowest_level_names, actual_lowest_level_names)


def dimension_compare(dimension1, dimension2, msg=None):
    return dimension1.name == dimension2.name  # and dimension1.hierarchies() == dimension2.hierarchies()
