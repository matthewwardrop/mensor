import inspect
import os
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

import yaml

from mensor.utils import OptionsMixin
from mensor.utils.registry import SubclassRegisteringABCMeta


class Metric(OptionsMixin, metaclass=SubclassRegisteringABCMeta):
    """
    This is the base class of all Metric implementations inside of Mensor.
    """

    @classmethod
    def from_yaml(cls, yml):
        if '\n' not in yml:
            with open(os.path.expanduser(yml)) as f:
                return cls.from_dict(yaml.load(f))
        else:
            return cls.from_dict(yaml.loads(yml))

    @classmethod
    def from_dict(cls, d):
        assert 'kind' in d
        assert d.get('role') in (None, 'metric')
        klass = cls.for_kind(d['kind'])
        instance = klass(
            name=d.get('name'),
            unit_type=d.get('unit_type'),
            desc=d.get('desc'),
            **d.get('opts')
        )
        return instance

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

        self.opts.add_option('name', 'The name of the metric', False, default=name)
        self.opts.add_option('measure_opts', 'Additional options to pass through to measures.', False, default={})
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

    def evaluate(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        # TODO: Check that strategy has required measures, segmentation and constraints.
        implementation = self._implementation_for_strategy(strategy)
        return implementation.evaluate(
            strategy,
            marginalise=marginalise,
            compatible_metrics=compatible_metrics,
            **self.opts.process(**opts)
        )

    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        implementation = self._implementation_for_strategy(strategy)
        return implementation.get_ir(
            strategy,
            marginalise=marginalise,
            compatible_metrics=compatible_metrics,
            **self.opts.process(**opts)
        )


class MetricImplementation(metaclass=SubclassRegisteringABCMeta):

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
