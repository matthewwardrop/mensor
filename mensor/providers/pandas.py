import pandas as pd
import six

from mensor.measures.context import Constraint
from mensor.measures.provider import MeasureProvider


class PandasMeasureProvider(MeasureProvider):

    def __init__(self, name, data=None, **kwargs):
        MeasureProvider.__init__(self, name, **kwargs)
        if isinstance(data, str):
            data = pd.read_csv(data)
        self.data = data

        self.add_measure('count', measure_agg='count')

    def _evaluate(self, unit_type, measures, where=None, segment_by=None, **opts):
        """
        Should return a dataframe satisfying the following properties:
        - measures should, depending on distribution, provide columns:
            <measure>:norm:sum
            <measure>:norm:sos
            <measure>:norm:count
        - dimensions should correspond to a single column, with the value being
            the value of that dimension.
        """

        def measure_map(name, op):
            return lambda df: op(df[name])

        def measure_maps(measures):
            d = {}
            for measure in measures:
                if measure.external:
                    continue
                if measure.measure_agg == 'normal':
                    d[measure.name + ':norm:sum'] = measure_map(measure.name, lambda x: x)
                    d[measure.name + ':norm:sos'] = measure_map(measure.name, lambda x: x**2)
                    d[measure.name + ':norm:count'] = measure_map(measure.name, lambda x: 1 * x.notnull())
                elif measure.measure_agg == 'count':
                    d[measure.name + ':count'] = measure_map(measure.name, lambda x: x)
                else:
                    raise ValueError("Measure agg {} not recognised.".format(measure.measure_agg))
            return d

        measure_cols = measure_maps(measures)

        d = (
            self.data
            .assign(count=1)
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

        if len(segment_by) > 0:
            d = (
                d
                .groupby([x.via_name for x in segment_by if not x.external])
                .sum()  # TODO: potentially expensive
                [list(measure_cols)]
                .reset_index()
            )
        else:
            d = d[list(measure_cols)].sum()

        return d

    def _is_compatible_with(self, other):
        return False
