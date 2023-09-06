import unittest

from test.utilities import Utilities as util
from cube.NonTopLevel import NonTopLevel

LEVEL_NAME = "supplier_name"
LEVEL_MEMBER_NAME = "name"
PK = "name_id"
FK = "nation_id"
VALID_LEVEL_NAME = "POMPdeLUX"
INVALID_LEVEL_NAME = "INVALID_LEVEL_NAME"


class TestNonTopLevel(unittest.TestCase):
    engine = None

    @classmethod
    def setUpClass(cls):
        cls.engine = util.get_engine()

    def setUp(self):
        self.level = NonTopLevel(LEVEL_NAME, LEVEL_MEMBER_NAME, self.engine, PK, FK)

    def assert_level_member(self, level_member_reference):
        self.assertEqual(self.level._level_members, [])
        self.assertNotIn(VALID_LEVEL_NAME, self.level.__dict__.keys())
        level_member = level_member_reference()
        self.assertEqual(len(self.level._level_members), 1)
        self.assertIn(VALID_LEVEL_NAME, self.level.__dict__.keys())
        self.assertEqual(self.level._level_members[0], level_member)
        self.assertEqual(self.level.POMPdeLUX, level_member)

    def test_getattr(self):
        self.assert_level_member(lambda: self.level.POMPdeLUX)

    def test_getattr_fail(self):
        self.assertRaises(AttributeError, self.level.__getattr__, INVALID_LEVEL_NAME)

    def test_getitem(self):
        self.assert_level_member(lambda: self.level[VALID_LEVEL_NAME])

    def test_getitem_not_fetch_level_twice_from_db(self):
        self.level[VALID_LEVEL_NAME]
        result = next((x for x in self.level._level_members if x.name == VALID_LEVEL_NAME), False)
        self.assertEqual(result, self.level[VALID_LEVEL_NAME])

    def test_getitem_fail(self):
        self.assertRaises(AttributeError, self.level.__getitem__, INVALID_LEVEL_NAME)
