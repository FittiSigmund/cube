import unittest

from cube.BaseCube import BaseCube
from cube.Cuboid import Cuboid
from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from session.session import create_session
from test.utilities import Utilities as util


def get_all_months_in_year_twice():
    return ["January", "January", "February", "February", "March", "March",
            "April", "April", "May", "May", "June", "June", "July", "July",
            "August", "August", "September", "September", "October", "October",
            "November", "November", "December", "December"]


def get_all_expected_values():
    return [16883, 15605, 14078, 13420, 18942, 13096, 14512, 12210, 15614, 17347,
            14928, 17445, 16620, 16470, 15645, 13669, 14342, 14497, 16609, 16351,
            13732, 16706, 12604, 16445]


def get_all_date_month_references(cube):
    return [
        cube.date.date_month.January,
        cube.date.date_month.February,
        cube.date.date_month.March,
        cube.date.date_month.April,
        cube.date.date_month.May,
        cube.date.date_month.June,
        cube.date.date_month.July,
        cube.date.date_month.August,
        cube.date.date_month.September,
        cube.date.date_month.October,
        cube.date.date_month.November,
        cube.date.date_month.December,
    ]


class TestCube(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)
        result = cube.output()
        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns.values[0], 2022)
        self.assertEqual(result.columns.values[1], 2021)
        self.assertEqual(result[2022][0], 190799.0)
        self.assertEqual(result[2021][0], 176971.0)

    def test_columns_on_january(self):
        cube = self.cube.columns([self.cube.date.date_month.January])
        result = cube.output()
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)
        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns.values[0], "January")
        self.assertEqual(result.columns.values[1], "January")
        self.assertEqual(result.iloc[0, 0], 15605.0)
        self.assertEqual(result.iloc[0, 1], 16883.0)

    def test_columns_on_january_and_february(self):
        cube = self.cube.columns([self.cube.date.date_month.January, self.cube.date.date_month.February])
        result = cube.output()
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)
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
        cube = self.cube.columns(get_all_date_month_references(self.cube))
        result = cube.output()
        columns = get_all_months_in_year_twice()
        values = get_all_expected_values()

        self.assertEqual(result.shape, (1, 24))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_returns_another_cube(self):
        cube = self.cube.columns([self.cube.date.date_month.January])
        self.assertNotEqual(cube, self.cube)
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)

    def test_columns_twice_should_materialize_all_cuboids(self):
        c1 = self.cube.columns([self.cube.date.date_month.January, self.cube.date.date_month.February])
        c2 = c1.columns([self.cube.date.date_month.January])
        result = c2.output()
        print(result)
        ## Komin hertil <--------------------

    def assert_equal_instance_and_name(self, cube_function, length, instance, name_list):
        self.assertEqual(len(cube_function()), length)
        for i in range(0, len(cube_function())):
            self.assertIsInstance(cube_function()[i], instance)
            self.assertEqual(cube_function()[i].name, name_list[i])

    def tearDown(self):
        del self.cube
