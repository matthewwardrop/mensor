from mensor.metrics.registry import MetricRegistry

from .constraints import Constraint, NullConstraint


class EvaluationContext(object):

    @property
    def metrics(self):
        if not hasattr(self, '_metrics'):
            raise NotImplementedError
        return self._metrics

    @metrics.setter
    def metrics(self, metrics):
        assert isinstance(metrics, MetricRegistry)
        self._metrics = metrics

    @property
    def measures(self):
        return self.metrics.measures

    @property
    def constraints(self):
        return getattr(self, '_constraints', NullConstraint())

    @constraints.setter
    def constraints(self, constraints):
        self._constraints = Constraint.from_spec(constraints)

    def evaluate(self, *args, where=None, **kwargs):
        return self.metrics.evaluate(
            *args, where=self.constraints & Constraint.from_spec(where), **kwargs
        )

    def evaluate_measures(self, *args, where=None, **kwargs):
        return self.metrics.measures.evaluate(
            *args, where=self.constraints & Constraint.from_spec(where), **kwargs
        )
