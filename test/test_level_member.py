import unittest
from typing import List

from cube.Cube import Cube
from cube.LevelMember import LevelMember
from session.session import create_session
from test.utilities import Utilities as util


def get_months_in_2020(cube: Cube) -> List[LevelMember]:
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


class TestNonTopLevel(unittest.TestCase):
    engine = None

    @classmethod
    def setUpClass(cls):
        cls.engine = util.get_engine()

    def setUp(self):
        session = create_session(self.engine)
        self.cube = session.load_view("salesdb_snowflake_test")

    def test_children(self):
        level_member = self.cube.date.date_year["2022"]
        actual_result = level_member.children()
        expected_result = get_months_in_2020(self.cube)
        for i in range(len(actual_result)):
            self.assertEqual(actual_result[i], expected_result[i])
            self.assertEqual(actual_result[i].parent.name, 2022)
