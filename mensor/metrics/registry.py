import pandas as pd

from mensor.measures.registry import MeasureRegistry
from mensor.measures.types import MeasureSeries, MeasureDataFrame

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

    def evaluate(self, metrics, segment_by=None, where=None, dry_run=False, **opts):
        if isinstance(metrics, str):
            metrics = [metrics]

        evaluations = {}
        for metric in metrics:
            for unit_type, measures in self._metrics[metric].evaluations.items():
                if unit_type not in evaluations:
                    evaluations[unit_type] = set()
                evaluations[unit_type].update(measures)

        if len(evaluations) == 0:
            return
        elif len(evaluations) > 1 and (segment_by is not None or where is not None):
            raise RuntimeError("Cannot apply segmentation or constraints when multiple unit_types are being used.")

        results = []
        for unit_type, measures in evaluations.items():
            result = self.measures.evaluate(unit_type, measures=measures, segment_by=segment_by, where=where, dry_run=dry_run, **opts)
            if not dry_run:
                result = result.add_prefix(unit_type + '/')
            results.append(result)

        if dry_run:
            return results

        if len(results) == 0:
            results = results[0]
        else:
            results = pd.concat(results)

        if isinstance(results, pd.Series):
            results = MeasureSeries(results)
            return pd.Series({metric: self._metrics[metric].evaluate(results) for metric in metrics})
        else:
            results = MeasureDataFrame(results)
            return pd.DataFrame(
                {metric: self._metrics[metric].evaluate(results) for metric in metrics}
            )
