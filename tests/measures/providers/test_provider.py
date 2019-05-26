import unittest

# from mensor.constraints import And, Constraint, Or
from mensor.measures import MutableMeasureProvider


class ProviderTests(unittest.TestCase):

    def setUp(self):
        self.mp = (
            MutableMeasureProvider(name='test_mp')
            .add_identifier('person', role='primary')
            .add_dimension('test_dimension')
            # .add_measure('testMeasure')
        )

    def test_hierarchical(self):
        fk = self.mp.resolve('person:seller', 'person:seller', role='identifier')
        self.assertEqual(fk, 'person:seller')
        self.assertEqual(fk.name, 'person')
