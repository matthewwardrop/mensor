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

    def evaluate(self, metrics, segment_by=None, where=None, dry_run=False, ir_only=False, **opts):

        results = []

        if isinstance(metrics, str):
            metrics = [metrics]

        for strategy, marginal_dimensions, metrics in self._group_metric_evaluations(metrics=metrics, segment_by=segment_by, where=where, **opts):
            result = metrics[0].evaluate(strategy, marginal_dimensions, **opts)

            if isinstance(result, pd.Series):
                result = MeasureSeries(result)
            else:
                result = MeasureDataFrame(result)

            results.append(result)

        return pd.concat([result.set_index(segment_by) for result in results], axis=1)

    def get_ir(self, metrics, segment_by=None, where=None, dry_run=False, **opts):
        for strategy, marginal_dimensions, metrics in self._group_metric_evaluations(metrics=metrics, segment_by=segment_by, where=where, **opts):
            return metrics[0].get_ir(strategy, marginal_dimensions, **opts)

    def _group_metric_evaluations(self, metrics, segment_by, where, **opts):

        if segment_by is None:
            segment_by = []
        if not isinstance(segment_by, list):
            segment_by = [segment_by]

        for metric in metrics:

            metric = self._metrics[metric]

            measures = metric.required_measures(**opts)
            if metric.required_segmentation(**opts):
                segment_by += list(set(metric.required_segmentation(**opts)).difference(segment_by))
            marginal_dimensions = list(set(metric.marginal_dimensions(**opts) or []).difference(segment_by))
            segment_by += marginal_dimensions

            if metric.required_constraints(**opts):
                required_constraints = Constraint.from_spec(metric.required_constraints(**opts))
                if where is None:
                    where = required_constraints
                else:
                    where = Constraint.from_spec(where) & required_constraints

            strategy = self.measures.get_strategy(metric.unit_type, measures=measures, segment_by=segment_by, where=where, **opts.pop('measure_opts', {}))

            yield strategy, marginal_dimensions, [metric]
