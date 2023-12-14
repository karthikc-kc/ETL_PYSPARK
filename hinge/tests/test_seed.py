from unittest import TestCase

from hinge.main.seed import add


class TestSeed(TestCase):
    def test_add(self):
        assert add(1, 2) == 3

    def test_add_fail(self):
        assert add(1, 2) != 4
