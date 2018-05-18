from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

from mensor.utils import OptionsMixin


class Metric(OptionsMixin, metaclass=ABCMeta):

    def _process_opts(f):
        def wrapped(self, *args, **opts):
            opts = self.opts.process(**opts)
            return f(self, *args, **opts)
        return wrapped

    def __init__(self, name, unit_type=None, desc=None, **kwargs):
        self.name = name
        self.unit_type = unit_type
        self.desc = desc

        self.implementations = OrderedDict()

        self.opts.add_option('implementation', "The implementation to use to evaluate the metrics.", False)

        self._init(**kwargs)

    def _init(self, **kwargs):
        pass

    @_process_opts
    def required_measures(self, **opts):
        return self._required_measures(**opts)

    @abstractmethod
    def _required_measures(self, **opts):
        raise NotImplementedError

    @_process_opts
    def required_segmentation(self, **opts):
        return self._required_segmentation(**opts)

    @abstractmethod
    def _required_segmentation(self, **opts):
        raise NotImplementedError

    @_process_opts
    def required_constraints(self, **opts):
        return self._required_constraints(**opts)

    @abstractmethod
    def _required_constraints(self, **opts):
        raise NotImplementedError

    @_process_opts
    def marginal_dimensions(self, **opts):
        return self._margin_dimensions(**opts)

    def _margin_dimensions(self, **opts):
        return []

    def _implementation_for_strategy(self, strategy):
        for implementation in self.implementations.values():
            if implementation._is_compatible_with_strategy(strategy):
                return implementation
        raise RuntimeError("No valid implementation for strategy.")

    def _is_compatible_with(self, strategy):
        for implementation in self.implementations.values():
            if implementation._is_compatible_with_strategy(strategy):
                return True
        return False

    @_process_opts
    def evaluate(self, strategy, marginalise=None, **opts):
        # TODO: Check that strategy has required measures, segmentation and constraints.
        implementation = self._implementation_for_strategy(strategy)
        return implementation.evaluate(strategy, marginalise=marginalise, **opts)

    @_process_opts
    def get_ir(self, strategy, marginalise=None, **opts):
        implementation = self._implementation_for_strategy(strategy)
        return implementation.get_ir(strategy, marginalise=marginalise, **opts)


class MetricImplementation(metaclass=ABCMeta):

    @abstractmethod
    def evaluate(self, strategy, marginalise=None, ir_only=False, **opts):
        raise NotImplementedError

    @abstractmethod
    def get_ir(self, strategy, marginalise=None, ir_only=False, **opts):
        raise NotImplementedError

    @abstractmethod
    def _is_compatible_with_strategy(self, strategy, marginalise=None, **opts):
        raise NotImplementedError

    def _is_compatible_with_metric(self, metric, marginalise=None, **opts):
        return False
