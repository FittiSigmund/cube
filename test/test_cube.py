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
        ## Test that result contains the correct numbers in each of the two cells

    def assert_equal_instance_and_name(self, cube_function, length, instance, name_list):
        self.assertEqual(len(cube_function()), length)
        for i in range(0, len(cube_function())):
            self.assertIsInstance(cube_function()[i], instance)
            self.assertEqual(cube_function()[i].name, name_list[i])

    @classmethod
    def tearDownClass(cls):
        cls.cursor.close()
