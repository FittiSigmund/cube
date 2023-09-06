import unittest

from cube.Cube import Cube
from session.session import create_session
from test.utilities import Utilities as util
from cube.CubeOperators import rollup, drilldown

CUBE_SUPPLIER_LEVEL_NAMES = ["supplier_name", "supplier_nation", "supplier_continent", "ALL"]
CUBE_PRODUCT_LEVEL_NAMES = ["product_name", "product_category", "ALL"]
CUBE_STORE_LEVEL_NAMES = ["store_address", "store_city", "store_county", "ALL"]
CUBE_DATE_LEVEL_NAMES = ["date_day", "date_month", "date_year", "ALL"]


def get_supplier_level_names(cutoff):
    return CUBE_SUPPLIER_LEVEL_NAMES[cutoff:]


def get_product_level_names(cutoff):
    return CUBE_PRODUCT_LEVEL_NAMES[cutoff:]


def get_store_level_names(cutoff):
    return CUBE_STORE_LEVEL_NAMES[cutoff:]


def get_date_level_names(cutoff):
    return CUBE_DATE_LEVEL_NAMES[cutoff:]


def create_rolled_up_cube(cube: Cube) -> Cube:
    return rollup(cube, supplier="ALL", product="ALL", store="ALL", date="ALL")


class TestCubeOperators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = util.get_engine()

    def setUp(self):
        session = create_session(self.engine)
        self.cube = session.load_view("salesdb_snowflake_test")

    def test_roll_up_one_level_one_dimension(self):
        c = rollup(self.cube, supplier="supplier_nation")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 3)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(1))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c.supplier.level_list)))

    def test_roll_up_two_levels_one_dimension(self):
        c = rollup(self.cube, supplier="supplier_continent")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 2)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(2))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c.supplier.level_list)))
        self.assertNotIn("supplier_nation", list(map(lambda x: x.name, c.supplier.level_list)))

    def test_roll_up_one_level_two_dimensions(self):
        c = rollup(self.cube, supplier="supplier_nation", product="product_category")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 3)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(1))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c.supplier.level_list)))

        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c.product.level_list), 2)
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c.product.level_list, get_product_level_names(1))
        self.assertNotIn("product_name", list(map(lambda x: x.name, c.product.level_list)))

    def test_roll_up_two_levels_two_dimensions(self):
        c = rollup(self.cube, supplier="supplier_continent", product="ALL")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 2)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(2))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c.supplier.level_list)))
        self.assertNotIn("supplier_nation", list(map(lambda x: x.name, c.supplier.level_list)))

        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c.product.level_list), 1)
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c.product.level_list, get_product_level_names(2))
        self.assertNotIn("product_name", list(map(lambda x: x.name, c.product.level_list)))
        self.assertNotIn("product_category", list(map(lambda x: x.name, c.product.level_list)))

    def test_roll_up_one_dimension_to_ALL(self):
        c = rollup(self.cube, product="ALL")
        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c.product.level_list), 1)
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c.product.level_list, get_product_level_names(2))
        self.assertNotIn("product_name", list(map(lambda x: x.name, c.product.level_list)))
        self.assertNotIn("product_category", list(map(lambda x: x.name, c.product.level_list)))

    def test_roll_up_wrong_dimension_wrong_level(self):
        self.assertRaises(ValueError, rollup, self.cube, INVALID_DIMENSION_NAME="ALL")

    def test_roll_up_correct_dimension_wrong_level(self):
        self.assertRaises(ValueError, rollup, self.cube, supplier="INVALID_LEVEL_NAME")

    def test_roll_up_no_dimensions(self):
        c = rollup(self.cube)
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 4)
        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c.product.level_list), 3)
        self.assertEqual(len(self.cube.store.level_list), 4)
        self.assertEqual(len(c.store.level_list), 4)
        self.assertEqual(len(self.cube.date.level_list), 4)
        self.assertEqual(len(c.date.level_list), 4)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(self.cube.store.level_list, get_store_level_names(0))
        self.assert_level_names_are_equal(c.store.level_list, get_store_level_names(0))
        self.assert_level_names_are_equal(self.cube.date.level_list, get_date_level_names(0))
        self.assert_level_names_are_equal(c.date.level_list, get_date_level_names(0))
        self.assertNotEqual(self.cube, c)

    def test_roll_up_down_one_level_one_dimension_should_fail(self):
        c = rollup(self.cube, supplier="supplier_nation")
        self.assertRaises(ValueError, rollup, c, supplier="supplier_name")

    def test_roll_up_same_level_one_dimension(self):
        c = rollup(self.cube, supplier="supplier_name")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 4)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(0))
        self.assertNotEqual(self.cube, c)

    def test_drill_down_one_level_one_dimension(self):
        c1 = create_rolled_up_cube(self.cube)
        c2 = drilldown(c1, supplier="supplier_continent")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c2.supplier.level_list), 2)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c2.supplier.level_list, get_supplier_level_names(2))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c2.supplier.level_list)))
        self.assertNotIn("supplier_nation", list(map(lambda x: x.name, c2.supplier.level_list)))

    def test_drill_down_two_levels_one_dimension(self):
        c1 = create_rolled_up_cube(self.cube)
        c2 = drilldown(c1, supplier="supplier_nation")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c2.supplier.level_list), 3)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c2.supplier.level_list, get_supplier_level_names(1))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c2.supplier.level_list)))

    def test_drill_down_one_level_two_dimensions(self):
        c1 = create_rolled_up_cube(self.cube)
        c2 = drilldown(c1, supplier="supplier_continent", product="product_category")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c2.supplier.level_list), 2)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c2.supplier.level_list, get_supplier_level_names(2))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c2.supplier.level_list)))
        self.assertNotIn("supplier_nation", list(map(lambda x: x.name, c2.supplier.level_list)))

        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c2.product.level_list), 2)
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c2.product.level_list, get_product_level_names(1))
        self.assertNotIn("product_name", list(map(lambda x: x.name, c2.product.level_list)))

    def test_drill_down_two_levels_two_dimensions(self):
        c1 = create_rolled_up_cube(self.cube)
        c2 = drilldown(c1, supplier="supplier_nation", product="product_name")
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c2.supplier.level_list), 3)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c2.supplier.level_list, get_supplier_level_names(1))
        self.assertNotIn("supplier_name", list(map(lambda x: x.name, c2.supplier.level_list)))

        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c2.product.level_list), 3)
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c2.product.level_list, get_product_level_names(0))

    def test_drill_down_same_level_ALL_one_dimension(self):
        c1 = create_rolled_up_cube(self.cube)
        c2 = drilldown(c1, supplier="ALL")
        self.assertEqual(len(c1.supplier.level_list), 1)
        self.assertEqual(len(c2.supplier.level_list), 1)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c2.supplier.level_list, get_supplier_level_names(0))
        self.assertNotEqual(c1, c2)

    def test_drill_down_higher_level_one_dimension_should_fail(self):
        self.assertRaises(ValueError, drilldown, self.cube, supplier="supplier_nation")

    def test_drill_down_no_dimensions(self):
        c = drilldown(self.cube)
        self.assertEqual(len(self.cube.supplier.level_list), 4)
        self.assertEqual(len(c.supplier.level_list), 4)
        self.assertEqual(len(self.cube.product.level_list), 3)
        self.assertEqual(len(c.product.level_list), 3)
        self.assertEqual(len(self.cube.store.level_list), 4)
        self.assertEqual(len(c.store.level_list), 4)
        self.assertEqual(len(self.cube.date.level_list), 4)
        self.assertEqual(len(c.date.level_list), 4)
        self.assert_level_names_are_equal(self.cube.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(c.supplier.level_list, get_supplier_level_names(0))
        self.assert_level_names_are_equal(self.cube.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(c.product.level_list, get_product_level_names(0))
        self.assert_level_names_are_equal(self.cube.store.level_list, get_store_level_names(0))
        self.assert_level_names_are_equal(c.store.level_list, get_store_level_names(0))
        self.assert_level_names_are_equal(self.cube.date.level_list, get_date_level_names(0))
        self.assert_level_names_are_equal(c.date.level_list, get_date_level_names(0))
        self.assertNotEqual(self.cube, c)

    def test_drill_down_wrong_dimension_wrong_level_should_fail(self):
        c = create_rolled_up_cube(self.cube)
        self.assertRaises(ValueError, drilldown, c, INVALID_DIMENSION_NAME="INVALID_LEVEL_NAME")

    def test_drill_down_correct_dimension_wrong_level_should_fail(self):
        c = create_rolled_up_cube(self.cube)
        self.assertRaises(ValueError, drilldown, c, supplier="INVALID_LEVEL_NAME")

    def assert_level_names_are_equal(self, level_list, level_name_list):
        for i, level in enumerate(level_list):
            self.assertEqual(level.name, level_name_list[i])
