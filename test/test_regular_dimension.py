import unittest

from cube.NonTopLevel import NonTopLevel
from cube.TopLevel import TopLevel
from session.session import create_session
from test.utilities import Utilities as util


class TestRegularDimension(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = create_session(util.get_engine())
        cls.cube = session.load_view("salesdb_snowflake_test")
        cls.date = cls.cube.date
        cls.product = cls.cube.product
        cls.store = cls.cube.store
        cls.supplier = cls.cube.supplier

    def test_date_lowest_level(self):
        self.assertIsInstance(self.date.lowest_level(), NonTopLevel)
        self.assertEqual(self.date.lowest_level().name, "date_day")

    def test_product_lowest_level(self):
        self.assertIsInstance(self.product.lowest_level(), NonTopLevel)
        self.assertEqual(self.product.lowest_level().name, "product_name")

    def test_store_lowest_level(self):
        self.assertIsInstance(self.store.lowest_level(), NonTopLevel)
        self.assertEqual(self.store.lowest_level().name, "store_address")

    def test_supplier_lowest_level(self):
        self.assertIsInstance(self.supplier.lowest_level(), NonTopLevel)
        self.assertEqual(self.supplier.lowest_level().name, "supplier_name")

    def test_date_hierarchies(self):
        name_list = ["date_day", "date_month", "date_year"]
        self.assert_hierarchies_is_instance_and_name(self.date.hierarchies(), name_list)

    def test_product_hierarchies(self):
        name_list = ["product_name", "product_category"]
        self.assert_hierarchies_is_instance_and_name(self.product.hierarchies(), name_list)

    def test_store_hiearchies(self):
        name_list = ["store_address", "store_city", "store_county"]
        self.assert_hierarchies_is_instance_and_name(self.store.hierarchies(), name_list)

    def test_supplier_hierarchies(self):
        name_list = ["supplier_name", "supplier_nation", "supplier_continent"]
        self.assert_hierarchies_is_instance_and_name(self.supplier.hierarchies(), name_list)

    def assert_hierarchies_is_instance_and_name(self, hierarchy, name_list):
        for i in range(0, len(hierarchy)):
            if i != len(hierarchy) - 1:
                self.assertIsInstance(hierarchy[i], NonTopLevel)
                self.assertEqual(hierarchy[i].name, name_list[i])
            else:
                self.assertIsInstance(hierarchy[i], TopLevel)
                self.assertEqual(hierarchy[i].name, "ALL")

