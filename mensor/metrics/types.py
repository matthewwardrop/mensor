from abc import abstractmethod, abstractproperty
from collections import OrderedDict


class Metric(object):

    def __init__(self, name, unit_type=None, required_measures=None, required_segmentation=None,
                 required_constraints=None, marginal_dimensions=None, implementations=None, desc=None):
        self.name = name
        self.unit_type = unit_type
        self.implementations = implementations or OrderedDict()
        self._required_measures = required_measures or []
        self._required_segmentation = required_segmentation
        self._required_constraints = required_constraints
        self._marginal_dimensions = marginal_dimensions or []
        self.desc = desc

    @property
    def required_measures(self):
        return self._required_measures

    @property
    def required_segmentation(self):
        return self._required_segmentation

    @property
    def required_constraints(self):
        return self._required_constraints

    @property
    def marginal_dimensions(self):
        return self._marginal_dimensions

    def _implementation_for_strategy(self, strategy):
        for implementation in self.implementations.values():
            if implementation._is_compatible_with(strategy):
                return implementation
        raise RuntimeError("No valid implementation for strategy.")

    def _is_compatible_with(self, strategy):
        for implementation in self.implementations.values():
            if implementation._is_compatible_with(strategy):
                return True
        return False

    def evaluate(self, strategy, marginalise=None, ir_only=False, **opts):
        # TODO: Check that strategy has required measures, segmentation and constraints.
        implementation = self._implementation_for_strategy(strategy)
        return implementation.evaluate(strategy, marginalise=marginalise, ir_only=ir_only, **opts)


class MetricImplementation(object):

    def _is_compatible_with(self, strategy, marginalise=None, ir_only=False, **opts):
        raise NotImplementedError

    def evaluate(self, strategy):
        raise NotImplementedError

# The following no longer works with the latest Metric code, but we need
# something like it to be implemented, so we here leave it commented out here.
# class RatioMetric(Metric):
#     # TODO: account for covariance
#
#     def __init__(self, name, numerator, denominator):
#         Metric.__init__(self, name)
#         self.numerator = numerator
#         self.denominator = denominator
#
#     def _evaluate(self, mdf):
#         return mdf[self.numerator] / mdf[self.denominator]
#
#     @property
#     def required_measures(self):
#         return [self.numerator.split('|')[0], self.denominator.split('|')[0]]
