import os
import sys

import pandas as pd
import six

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mensor.measures import MeasureProvider, MeasureRegistry  # noqa
from mensor.measures.context import Constraint  # noqa


class PandasMeasureProvider(MeasureProvider):

    def __init__(self, name, data=None, **kwargs):
        MeasureProvider.__init__(self, name, **kwargs)
        self.data = data

    def _evaluate(self, unit_type, measures, where=None, segment_by=None, **opts):
        """
        Should return a dataframe satisfying the following properties:
        - measures should, depending on distribution, provide columns:
            <measure>:norm:sum
            <measure>:norm:sos
            <measure>:norm:count
        - dimensions should
        """

        def measure_map(name, op):
            return lambda df: op(df[name])

        def measure_maps(measures):
            d = {}
            for measure in measures:
                if measure.external:
                    continue
                d[measure.name + ':norm:sum'] = measure_map(measure.name, lambda x: x)
                d[measure.name + ':norm:sos'] = measure_map(measure.name, lambda x: x**2)
                d[measure.name + ':norm:count'] = measure_map(measure.name, lambda x: 1 * x.notnull())
            return d

        measure_cols = measure_maps(measures)

        d = (
            self.data
            .rename(
                columns={identifier.expr: identifier.name for identifier in self.identifiers},
            )
            .rename(
                columns={dimension.expr: dimension.name for dimension in self.dimensions},
            )
            .rename(
                columns={dimension.expr: dimension.name for dimension in self.measures},
            )
            .assign(**measure_cols)
        )
        for constraint in where:
            assert isinstance(constraint, Constraint), 'Unexpected constraint type: {}'.format(type(constraint))
            d = d.query('{} == {}'.format(
                constraint.expr,
                constraint.rhs if not isinstance(constraint.rhs, six.string_types) else '"{}"'.format(constraint.rhs)
            ))
        d = (
            d
            .groupby([x.via_name for x in segment_by if not x.external])
            .sum()  # TODO: potentially expensive
            [list(measure_cols)]
            .reset_index()
        )
        return d

    def _is_compatible_with(self, other):
        return False


m = MeasureRegistry()


data_dir = os.path.join(os.path.dirname(__file__), 'data')

geographies = (
    PandasMeasureProvider(
        name='geographies',
        data=pd.read_csv(os.path.join(data_dir, 'geographies.csv')).reset_index().rename(columns={'index': 'id'})
    )
    .add_identifier('geography', expr='id', role='primary')
    .add_dimension('city')
    .add_dimension('country')
    .add_measure('population', expr='pop')
)
m.register(geographies)

people = (
    PandasMeasureProvider(
        name='people',
        data=pd.read_csv(os.path.join(data_dir, 'people.csv'))
    )
    .add_identifier('person', expr='id', role='primary')
    .add_identifier('geography', expr='id_country', role='foreign')
    .add_dimension('name')
    .add_measure('age')
)
m.register(people)

transactions = (
    PandasMeasureProvider(
        name='transactions',
        data=pd.read_csv(os.path.join(data_dir, 'transactions.csv'))
    )
    .add_identifier('transaction', expr='id', role='primary')
    .add_identifier('person:buyer', expr='id_buyer', role='foreign')
    .add_identifier('person:seller', expr='id_seller', role='foreign')
    .add_measure('value')
)
m.register(transactions)

m.show()

# Test strategies

strategy = m.evaluate(
    'person',
    measures=['age', 'geography/population'],
    segment_by=['geography/country'],
    where=None
)

df = strategy.run()
print(df)
print(type(df))
print(df['age'])
print(df.set_index('geography/country')['geography/population'])

# strategy = m.evaluate(
#     'transaction',
#     measures=['value'],
#     segment_by=['person:seller/name'],
#     where=None
# )
#
# print(strategy)
# print(strategy.run())
