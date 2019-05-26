import os
import pytest
import unittest

from mensor.backends.pandas import PandasMeasureProvider
from mensor.measures import MetaMeasureProvider


class TestPandasMeasureProvider:

    @pytest.mark.skip
    def test_simple(self):
        df = self.registry.evaluate(
            'transaction',
            measures=['person:seller/age'],
            segment_by=['person:buyer/name'],
            where=None
        )

        self.assertGreater(len(df.raw), 0)
        self.assertEqual(
            set(df.columns),
            set([
                'person:seller/age|normal|count',
                'person:seller/age|normal|sos',
                'person:seller/age|normal|sum',
                'person:buyer/name'
            ])
        )
        self.assertFalse(df.raw.duplicated('person:buyer/name').any())
