class TestMetaMeasureProvider:

    def test_simple(self, metaprovider):
        df = metaprovider.evaluate(
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
