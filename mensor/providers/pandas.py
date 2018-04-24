import pandas as pd
import six

from mensor.measures.context import Constraint
from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS


class PandasMeasureProvider(MeasureProvider):
    # TODO: Handle unit-aggregation

    def __init__(self, name, data=None, **kwargs):
        MeasureProvider.__init__(self, name, **kwargs)
        if isinstance(data, str):
            data = pd.read_csv(data)
        self.data = data

        self.add_measure('count', shared=True, distribution=None)

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
            col_maps = {}
            col_aggs = {}
            for measure in measures:
                if not measure.external:
                    for field_suffix, (col_agg, col_map) in self._get_distribution_fields(measure.distribution).items():
                        col_aggs[measure.via_name + field_suffix] = col_agg
                        col_maps[measure.name + field_suffix] = measure_map(measure.name, col_map)
            return col_maps, col_aggs

        measure_cols, measure_aggs = measure_maps(measures)

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

        segments = [x.via_name for x in segment_by if not x.external]

        if len(d) == 0:
            d = pd.DataFrame([], columns=segments + list(measure_aggs))
        elif len(segments) > 0 and len(measure_aggs) > 0:
            d = (
                d
                .groupby(segments)
                .agg(measure_aggs)
                .reset_index()
            )
        elif len(segment_by) > 0:
            d = (
                d
                .assign(dummy=1)
                .groupby(segments)
                .sum()
                .reset_index()
                [segments]
            )
        else:
            d = d[list(measure_cols)].agg(measure_aggs)

        return d

    @property
    def _agg_methods(self):
        return {
            AGG_METHODS.SUM: ('sum', lambda x: x),
            AGG_METHODS.MEAN: ('mean', lambda x: x),
            AGG_METHODS.SQUARE_SUM: ('sum', lambda x: x**2),
            AGG_METHODS.COUNT: ('sum', lambda x: 1)
        }
