from mensor.metrics.registry import MetricRegistry

from .constraints import Constraint, NullConstraint


class EvaluationContext(object):
    @property
    def metrics(self):
        if not hasattr(self, "_metrics"):
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
    def context(self):
        return getattr(self, "_context", {})

    @context.setter
    def context(self, context):
        self._context = context

    @property
    def constraints(self):
        return getattr(self, "_constraints", NullConstraint())

    @constraints.setter
    def constraints(self, constraints):
        self._constraints = Constraint.from_spec(constraints)

    def evaluate(self, *args, where=None, ir_only=False, **kwargs):
        if ir_only:
            f = self.metrics.get_ir
        else:
            f = self.metrics.evaluate
        return f(
            *args,
            where=self.constraints & Constraint.from_spec(where),
            context=self.context,
            measure_opts={},
            **kwargs
        )

    def evaluate_measures(self, *args, where=None, ir_only=False, **kwargs):
        if ir_only:
            f = self.measures.get_ir
        else:
            f = self.measures.evaluate
        return f(
            *args,
            where=self.constraints & Constraint.from_spec(where),
            context=self.context,
            **kwargs
        )

    def __repr__(self):
        return "{}<contraints={}, context={}>".format(
            self.__class__.__name__, self.constraints, self.context
        )
