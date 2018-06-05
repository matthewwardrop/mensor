import os

import pandas as pd

from mensor.constraints import Constraint
from mensor.measures.registry import MeasureRegistry
from mensor.measures.types import EvaluatedMeasures

from .types import Metric


class MetricRegistry(object):

    def __init__(self, measure_registry=None):
        self.measures = measure_registry or MeasureRegistry()
        self._metrics = {}

    def register(self, metric):
        assert isinstance(metric, Metric), "Metrics must be instances of `Metric`."
        self._metrics[metric.name] = metric

    def register_from_yaml(self, path_or_yaml):
        if '\n' in path_or_yaml or not os.path.isdir(os.path.expanduser(path_or_yaml)):
            return self.register(Metric.from_yaml(path_or_yaml))
        else:
            for dirpath, dirnames, filenames in os.walk(os.path.expanduser(path_or_yaml)):
                for filename in filenames:
                    if filename.endswith('.yml'):
                        try:
                            provider = Metric.from_yaml(os.path.join(dirpath, filename))
                            self.register(provider)
                        except AssertionError:
                            pass

    def unregister(self, name):
        del self._metrics[name]

    def evaluate(self, metrics, segment_by=None, where=None, **opts):
        """
        Parameters:
            metrics (str, list<str): A metric (or list of metrics) to evaluate.
            segment_by (str, list<str>): A dimension or list of dimensions by which
                to segment the metric aggregation.
            where (dict, BaseConstraint): A representation of the constraints to
                apply during this evaluation.
            opts (dict): Additional options to pass through to the metric
                evaluation process.
        """

        results = []

        metrics = [] if metrics is None else metrics
        segment_by = [] if segment_by is None else segment_by

        if not isinstance(metrics, list):
            metrics = [metrics]
        if not isinstance(segment_by, list):
            segment_by = [segment_by]

        for strategy, required_marginal_segmentation, metrics in self._group_metric_evaluations(metrics=metrics, segment_by=segment_by, where=where):
            result = metrics[0].evaluate(strategy, required_marginal_segmentation, compatible_metrics=metrics[1:], **opts)

            result = EvaluatedMeasures.for_measures(result)

            results.append(result)

        if len(segment_by):
            return pd.concat([result.raw.set_index(segment_by) for result in results], axis=1)
        else:
            return pd.concat([result.raw for result in results], axis=1)

    def get_ir(self, metrics, segment_by=None, where=None, dry_run=False, **opts):
        metrics = [] if metrics is None else metrics
        segment_by = [] if segment_by is None else segment_by

        if not isinstance(metrics, list):
            metrics = [metrics]
        if not isinstance(segment_by, list):
            segment_by = [segment_by]

        for strategy, required_marginal_segmentation, metrics in self._group_metric_evaluations(metrics=metrics, segment_by=segment_by, where=where):
            yield metrics, metrics[0].get_ir(strategy, required_marginal_segmentation, compatible_metrics=metrics[1:], **opts)

    def _get_strategy_for_metric(self, metric, segment_by, where):
        measures = metric.required_measures
        if metric.required_segmentation:
            segment_by = segment_by + list(set(metric.required_segmentation).difference(segment_by))
        required_marginal_segmentation = list(set(metric.required_marginal_segmentation or []).difference(segment_by))
        segment_by = segment_by + required_marginal_segmentation

        if metric.required_constraints:
            required_constraints = Constraint.from_spec(metric.required_constraints)
            if where is None:
                where = required_constraints
            else:
                where = Constraint.from_spec(where) & required_constraints

        return self.measures.get_strategy(
            metric.unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where
        )

    def _group_metric_evaluations(self, metrics, segment_by, where, **opts):

        metrics = [self._metrics[metric] if not isinstance(metric, Metric) else metric for metric in metrics]
        strategies = {metric: self._get_strategy_for_metric(metric, segment_by, where, **opts) for metric in metrics}

        for metric in metrics:
            strategy = strategies[metric]
            required_marginal_segmentation = set(strategy.segment_by).difference(segment_by)
            yield strategy, required_marginal_segmentation, [metric]

        # TODO: Generalise grouping correctly. The following is incorrect due to
        # ignoring where constraints in nested joins.
        #
        # def is_compatible(metric1, metric2):
        #     strategy1 = strategies[metric1]
        #     strategy2 = strategies[metric2]
        #
        #     for field in ['unit_type', 'segment_by']:
        #         if getattr(strategy1, field) != getattr(strategy2, field):
        #             return False
        #
        #     implementation1 = metric1.implementation_for_strategy(strategy1)
        #     implementation2 = metric2.implementation_for_strategy(strategy2)
        #
        #     return implementation1._is_compatible_with_metric(implementation2)
        #
        # def strategy_for_metrics(metrics):
        #     unit_type = None
        #     measures = []
        #     segment_by = None
        #     where = None
        #
        #     for metric in metrics:
        #         if unit_type is None:
        #             unit_type = strategies[metric].unit_type
        #         else:
        #             assert unit_type == strategies[metric].unit_type
        #
        #         for measure in strategies[metric].measures:
        #             if measure not in measures:
        #                 measures.append(measure)
        #
        #         if segment_by is None:
        #             segment_by = list(strategies[metric].segment_by)
        #         else:
        #             assert set(segment_by) == set(list(strategies[metric].segment_by))
        #
        #         if where is None:
        #             where = strategies[metric].where
        #         else:
        #             assert where == strategies[metric].where
        #
        #     return self.measures.get_strategy(
        #         unit_type=unit_type,
        #         measures=measures,
        #         segment_by=segment_by,
        #         where=where
        #     )
        #
        # if segment_by is None:
        #     segment_by = []
        # if not isinstance(segment_by, list):
        #     segment_by = [segment_by]
        #
        # offset = 0
        # while len(metrics) > 0:
        #     metric = metrics.pop(0)
        #     compatible = [metric]
        #
        #     for i, other in enumerate(metrics[:]):
        #         if is_compatible(metric, other):
        #             compatible.append(other)
        #             metrics.pop(i + offset)
        #             offset -= 1
        #
        #     strategy = strategy_for_metrics(compatible)
        #     required_marginal_segmentation = set(strategy.segment_by).difference(segment_by)  # Todo: check case when required_dimensions is not empty
        #
        #     yield strategy_for_metrics(compatible), required_marginal_segmentation, compatible
