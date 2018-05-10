import pandas as pd

from mensor.constraints import Constraint
from mensor.measures.registry import MeasureRegistry
from mensor.measures.types import MeasureDataFrame, MeasureSeries

from .types import Metric


class MetricRegistry(object):

    def __init__(self, measure_registry=None):
        self.measures = measure_registry or MeasureRegistry()
        self._metrics = {}

    def register(self, metric):
        assert isinstance(metric, Metric), "Metrics must be instances of `Metric`."
        self._metrics[metric.name] = metric

    def unregister(self, name):
        del self._metrics[name]

    def evaluate(self, metric, segment_by=None, where=None, dry_run=False, ir_only=False, **opts):
        if isinstance(metric, list):
            return [self.evaluate(m, segment_by=segment_by, where=where, dry_run=dry_run, opts=opts) for m in metric]

        metric = self._metrics[metric]
        if segment_by is None:
            segment_by = []
        if not isinstance(segment_by, list):
            segment_by = [segment_by]

        measures = metric.required_measures
        if metric.required_segmentation:
            segment_by += list(set(metric.required_segmentation).difference(segment_by))
        marginal_dimensions = list(set(metric.marginal_dimensions or []).difference(segment_by))
        segment_by += marginal_dimensions

        if metric.required_constraints:
            required_constraints = Constraint.from_spec(metric.required_constraints)
            if where is None:
                where = required_constraints
            else:
                where = Constraint.from_spec(where) & required_constraints

        strategy = self.measures.evaluate(metric.unit_type, measures=measures, segment_by=segment_by, where=where, dry_run=True, **opts.pop('measure_opts', {}))

        if dry_run:
            return strategy

        result = metric.evaluate(strategy, marginal_dimensions, ir_only=ir_only, **opts)

        if isinstance(result, pd.Series):
            return MeasureSeries(result)
        else:
            return MeasureDataFrame(result)
