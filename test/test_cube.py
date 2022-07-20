import unittest

from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from session.session import create_session
from test.utilities import Utilities as util


class TestCube(unittest.TestCase):
    cursor = None

    @classmethod
    def setUpClass(cls):
        cls.cursor = util.get_cursor()
        cls.engine = util.get_engine()

    def setUp(self):
        session = create_session(self.engine)
        self.cube = session.load_cube("salesdb_snowflake_test")

    def test_measures(self):
        cube_function = lambda: self.cube.measures()
        name_list = ["total_sales_price", "unit_sales"]
        self.assert_equal_instance_and_name(cube_function, 2, Measure, name_list)

    def test_dimensions(self):
        cube_function = lambda: self.cube.dimensions()
        name_list = ["product", "date", "store", "supplier"]
        self.assert_equal_instance_and_name(cube_function, 4, RegularDimension, name_list)

    def test_columns_on_year(self):
        cube = self.cube.columns([self.cube.date.date_year["2022"], self.cube.date.date_year["2021"]])
        result = cube.output()
        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns.values[0], 2022)
        self.assertEqual(result.columns.values[1], 2021)
        self.assertEqual(result[2022][0], 190799.0)
        self.assertEqual(result[2021][0], 176971.0)

    def test_columns_on_january(self):
        cube = self.cube.columns([self.cube.date.date_month.January])
        result = cube.output()
        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns.values[0], "January")
        self.assertEqual(result.columns.values[1], "January")
        self.assertEqual(result.iloc[0, 0], 15605.0)
        self.assertEqual(result.iloc[0, 1], 16883.0)

    def test_columns_on_january_and_february(self):
        cube = self.cube.columns([self.cube.date.date_month.January, self.cube.date.date_month.February])
        result = cube.output()
        self.assertEqual(result.shape, (1, 4))
        self.assertEqual(result.columns.values[0], "January")
        self.assertEqual(result.columns.values[1], "January")
        self.assertEqual(result.columns.values[2], "February")
        self.assertEqual(result.columns.values[3], "February")
        self.assertEqual(result.iloc[0, 0], 15605.0)
        self.assertEqual(result.iloc[0, 1], 16883.0)
        self.assertEqual(result.iloc[0, 2], 14078.0)
        self.assertEqual(result.iloc[0, 3], 13420.0)

    def test_columns_on_all_months_in_year(self):
        cube = self.cube.columns([
            self.cube.date.date_month.January,
            self.cube.date.date_month.February,
            self.cube.date.date_month.March,
            self.cube.date.date_month.April,
            self.cube.date.date_month.May,
            self.cube.date.date_month.June,
            self.cube.date.date_month.July,
            self.cube.date.date_month.August,
            self.cube.date.date_month.September,
            self.cube.date.date_month.October,
            self.cube.date.date_month.November,
            self.cube.date.date_month.December,
        ])
        result = cube.output()
        print(result)
        ## KOMIN HERTIL

    def assert_equal_instance_and_name(self, cube_function, length, instance, name_list):
        self.assertEqual(len(cube_function()), length)
        for i in range(0, len(cube_function())):
            self.assertIsInstance(cube_function()[i], instance)
            self.assertEqual(cube_function()[i].name, name_list[i])

    @classmethod
    def tearDownClass(cls):
        cls.cursor.close()
