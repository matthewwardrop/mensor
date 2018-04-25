import numbers

import pandas as pd

from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS
from mensor.measures.context import CONSTRAINTS


class PandasMeasureProvider(MeasureProvider):
    # TODO: Handle unit-aggregation

    def __init__(self, name, data=None, **kwargs):
        MeasureProvider.__init__(self, name, **kwargs)
        if isinstance(data, str):
            data = pd.read_csv(data)
        self.data = data

        self.add_measure('count', shared=True, distribution=None)

    def _evaluate(self, unit_type, measures, where=None, segment_by=None,
                  stats=True, covariates=False, **opts):

        def measure_map(name, op):
            return lambda df: op(df[name])

        def measure_maps(measures):
            col_maps = {}
            col_aggs = {}
            for measure in measures:
                if not measure.external:
                    if stats:
                        for field_suffix, (col_agg, col_map) in self._get_distribution_fields(measure.distribution).items():
                            col_aggs[measure.via_name + field_suffix] = col_agg
                            col_maps[measure.name + field_suffix] = measure_map(measure.name, col_map)
                    else:
                        col_aggs[measure.via_name] = 'sum'
                        col_maps[measure.name] = lambda x: x
            return col_maps, col_aggs

        measure_cols, measure_aggs = measure_maps(measures)

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
            .assign(count=1)
            .assign(**measure_cols)  # May include count, so don't roll up into above.
        )

        if where:
            d = d.query(self._constraint_str(where))

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

    @property
    def _constraint_maps(self):
        return {
            CONSTRAINTS.AND: lambda x: '({})'.format(' & '.join(self._constraint_str(o) for o in x.operands)),
            CONSTRAINTS.OR: lambda x: '({})'.format(' | '.join(self._constraint_str(o) for o in x.operands)),
            CONSTRAINTS.EQUALITY: lambda x: '{} == {}'.format(x.field, self._constraint_quote(x.value)),
            CONSTRAINTS.INEQUALITY_GT: lambda x: '{} > {}'.format(x.field, self._constraint_quote(x.value)),
            CONSTRAINTS.INEQUALITY_GTE: lambda x: '{} >= {}'.format(x.field, self._constraint_quote(x.value)),
            CONSTRAINTS.INEQUALITY_LT: lambda x: '{} < {}'.format(x.field, self._constraint_quote(x.value)),
            CONSTRAINTS.INEQUALITY_LTE: lambda x: '{} <= {}'.format(x.field, self._constraint_quote(x.value)),
        }

    def _constraint_str(self, constraint):
        return self._constraint_map(constraint.kind)(constraint)

    def _constraint_quote(cls, value):
        "This method quotes values appropriately."
        if isinstance(value, str):
            return '"{}"'.format(value)  # TODO: Worry about quotes in string.
        elif isinstance(value, numbers.Number):
            return str(value)
        raise ValueError("Pandas backend does not support quoting objects of type: `{}`".format(type(value)))
