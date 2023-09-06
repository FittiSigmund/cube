import unittest

from cube.TopLevel import TopLevel


class TestTopLevel(unittest.TestCase):
    def setUp(self):
        self.level = TopLevel()

    def test_get_name(self):
        self.assertEqual(self.level.name, "ALL")

    def test_get_parent(self):
        self.assertEqual(self.level.parent, None)

    def test_get_child(self):
        self.assertEqual(self.level.child, None)

    def test_get_dimension(self):
        self.assertEqual(self.level.dimension, None)

    def test_repr(self):
        self.assertEqual(self.level.__repr__(), f"TopLevel({self.level.name})")
