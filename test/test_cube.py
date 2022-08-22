import unittest
from typing import List

import pandas as pd

from cube.BaseCube import BaseCube
from cube.Cuboid import Cuboid
from cube.Measure import Measure
from cube.RegularDimension import RegularDimension
from session.session import create_session
from test.utilities import Utilities as util


def get_all_months_in_year():
    return ["January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December"]


def get_all_months_in_year_twice():
    return ["January", "January", "February", "February", "March", "March",
            "April", "April", "May", "May", "June", "June", "July", "July",
            "August", "August", "September", "September", "October", "October",
            "November", "November", "December", "December"]


def get_expected_values_for_all_months_in_all_years():
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


def get_all_date_month_references_for_2021(cube):
    return [
        cube.date.date_year["2021"].January,
        cube.date.date_year["2021"].February,
        cube.date.date_year["2021"].March,
        cube.date.date_year["2021"].April,
        cube.date.date_year["2021"].May,
        cube.date.date_year["2021"].June,
        cube.date.date_year["2021"].July,
        cube.date.date_year["2021"].August,
        cube.date.date_year["2021"].September,
        cube.date.date_year["2021"].October,
        cube.date.date_year["2021"].November,
        cube.date.date_year["2021"].December,
    ]


def get_all_date_month_references_for_2022(cube):
    return [
        cube.date.date_year["2022"].January,
        cube.date.date_year["2022"].February,
        cube.date.date_year["2022"].March,
        cube.date.date_year["2022"].April,
        cube.date.date_year["2022"].May,
        cube.date.date_year["2022"].June,
        cube.date.date_year["2022"].July,
        cube.date.date_year["2022"].August,
        cube.date.date_year["2022"].September,
        cube.date.date_year["2022"].October,
        cube.date.date_year["2022"].November,
        cube.date.date_year["2022"].December,
    ]


def get_expected_values_for_all_months_in_2021() -> List[float]:
    return [15605.0, 14078.0, 13096.0, 12210.0, 17347.0, 14928,
            16620.0, 15645.0, 14497.0, 16609.0, 13732.0, 12604.0]


def get_expected_values_for_all_months_in_2022() -> List[float]:
    return [16883.0, 13420.0, 18942.0, 14512.0, 15614.0, 17445.0, 16470.0,
            13669.0, 14342.0, 16351.0, 16706.0, 16445.0]


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

    def test_columns_on_all_months_in_all_years(self):
        cube = self.cube.columns(get_all_date_month_references(self.cube))
        result = cube.output()
        columns = get_all_months_in_year_twice()
        values = get_expected_values_for_all_months_in_all_years()

        self.assertEqual(result.shape, (1, 24))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_on_all_months_in_2022(self):
        cube = self.cube.columns(get_all_date_month_references_for_2022(self.cube))
        result = cube.output()
        columns = get_all_months_in_year()
        values = get_expected_values_for_all_months_in_2022()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_on_all_months_in_2021(self):
        cube = self.cube.columns(get_all_date_month_references_for_2021(self.cube))
        result = cube.output()
        columns = get_all_months_in_year()
        expected_values = get_expected_values_for_all_months_in_2021()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], expected_values[i])

    def test_columns_on_all_years_using_members(self):
        cube = self.cube.columns(self.cube.date.date_year.members())
        result = cube.output()

        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns[0], 2022)
        self.assertEqual(result.columns[1], 2021)
        self.assertEqual(result.iloc[0, 0], 190799.0)
        self.assertEqual(result.iloc[0, 1], 176971.0)

    def test_columns_all_years_members_and_explicit(self):
        cube1 = self.cube.columns(self.cube.date.date_year.members())
        result1 = cube1.output()
        cube2 = self.cube.columns([self.cube.date.date_year["2022"], self.cube.date.date_year["2021"]])
        result2 = cube2.output()

        pd.testing.assert_frame_equal(result1, result2)

    def test_columns_returns_another_cube(self):
        cube = self.cube.columns([self.cube.date.date_month.January])
        self.assertNotEqual(cube, self.cube)
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)

    def test_columns_all_months_in_2022_with_children(self):
        cube = self.cube.columns(self.cube.date.date_year["2022"].children())
        result = cube.output()
        columns = get_all_months_in_year()
        values = get_expected_values_for_all_months_in_2022()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_on_year_using_unit_sales(self):
        self.cube.default_measure = self.cube.unit_sales
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

    def test_columns_on_all_months_in_all_years(self):
        cube = self.cube.columns(get_all_date_month_references(self.cube))
        result = cube.output()
        columns = get_all_months_in_year_twice()
        values = get_expected_values_for_all_months_in_all_years()

        self.assertEqual(result.shape, (1, 24))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_on_all_months_in_2022(self):
        cube = self.cube.columns(get_all_date_month_references_for_2022(self.cube))
        result = cube.output()
        columns = get_all_months_in_year()
        values = get_expected_values_for_all_months_in_2022()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])

    def test_columns_on_all_months_in_2021(self):
        cube = self.cube.columns(get_all_date_month_references_for_2021(self.cube))
        result = cube.output()
        columns = get_all_months_in_year()
        expected_values = get_expected_values_for_all_months_in_2021()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], expected_values[i])

    def test_columns_on_all_years_using_members(self):
        cube = self.cube.columns(self.cube.date.date_year.members())
        result = cube.output()

        self.assertEqual(result.shape, (1, 2))
        self.assertEqual(result.columns[0], 2022)
        self.assertEqual(result.columns[1], 2021)
        self.assertEqual(result.iloc[0, 0], 190799.0)
        self.assertEqual(result.iloc[0, 1], 176971.0)

    def test_columns_all_years_members_and_explicit(self):
        cube1 = self.cube.columns(self.cube.date.date_year.members())
        result1 = cube1.output()
        cube2 = self.cube.columns([self.cube.date.date_year["2022"], self.cube.date.date_year["2021"]])
        result2 = cube2.output()

        pd.testing.assert_frame_equal(result1, result2)

    def test_columns_returns_another_cube(self):
        cube = self.cube.columns([self.cube.date.date_month.January])
        self.assertNotEqual(cube, self.cube)
        self.assertIsInstance(self.cube, BaseCube)
        self.assertIsInstance(cube, Cuboid)

    def test_columns_all_months_in_2022_with_children(self):
        cube = self.cube.columns(self.cube.date.date_year["2022"].children())
        result = cube.output()
        columns = get_all_months_in_year()
        values = get_expected_values_for_all_months_in_2022()

        self.assertEqual(result.shape, (1, 12))
        for i, column in enumerate(result.columns):
            self.assertEqual(column, columns[i])
            self.assertEqual(result.iloc[0, i], values[i])
    def assert_equal_instance_and_name(self, cube_function, length, instance, name_list):
        self.assertEqual(len(cube_function()), length)
        for i in range(0, len(cube_function())):
            self.assertIsInstance(cube_function()[i], instance)
            self.assertEqual(cube_function()[i].name, name_list[i])

    def tearDown(self):
        del self.cube
