import inspect
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

from mensor.utils import OptionsMixin


class Metric(OptionsMixin, metaclass=ABCMeta):

    def _process_opts(f):
        signature = inspect.getfullargspec(f).args
        def wrapped(self, *args, **opts):
            base_args = {}
            for opt in list(opts):
                if opt in signature:
                    base_args[opt] = opts.pop(opt)
            opts = self.opts.process(**opts)
            return f(self, *args, **base_args, **opts)
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

    @property
    def required_measures(self):
        return self._required_measures(**self.opts.process())

    @abstractmethod
    def _required_measures(self, **opts):
        raise NotImplementedError

    @property
    def required_segmentation(self):
        return self._required_segmentation(**self.opts.process())

    @abstractmethod
    def _required_segmentation(self, **opts):
        raise NotImplementedError

    @property
    def required_constraints(self):
        return self._required_constraints(**self.opts.process())

    @abstractmethod
    def _required_constraints(self, **opts):
        raise NotImplementedError

    @property
    def marginal_dimensions(self):
        return self._marginal_dimensions(**self.opts.process())

    def _marginal_dimensions(self, **opts):
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
    def evaluate(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        # TODO: Check that strategy has required measures, segmentation and constraints.
        print(compatible_metrics)
        implementation = self._implementation_for_strategy(strategy)
        return implementation.evaluate(strategy, marginalise=marginalise,
                                       compatible_metrics=compatible_metrics, **opts)

    @_process_opts
    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        implementation = self._implementation_for_strategy(strategy)
        return implementation.get_ir(strategy, marginalise=marginalise,
                                     compatible_metrics=compatible_metrics, **opts)


class MetricImplementation(metaclass=ABCMeta):

    @abstractmethod
    def evaluate(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        raise NotImplementedError

    @abstractmethod
    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        raise NotImplementedError

    @abstractmethod
    def _is_compatible_with_strategy(self, strategy, **opts):
        raise NotImplementedError

    def _is_compatible_with_metric(self, metric, **opts):
        return False
