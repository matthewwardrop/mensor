import inspect
import os
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict

import yaml

from mensor.constraints import Constraint, NullConstraint
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
            return cls.from_dict(yaml.load(yml))

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

    def __init__(self, name, unit_type=None, desc=None, **kwargs):
        self.name = name
        self.unit_type = unit_type
        self.desc = desc

        self._implementations = []

        self.opts.add_option('name', 'The name of the metric', False, default=name)
        self.opts.add_option('measure_opts', 'Additional options to pass through to measures.', False, default={})
        self.opts.add_option('implementation', "The implementation to use to evaluate the metrics.", False)

        self._init(**kwargs)

    def _init(self, **kwargs):
        pass

    # Implementation Management

    @property
    def implementations(self):
        return self._implementations

    def add_implementation(self, implementation):
        self._implementations.append(implementation.register_for_metric(self))
        return self

    def implementation_for_strategy(self, strategy):
        for implementation in self.implementations:
            if implementation._is_compatible_with_strategy(strategy):
                return implementation
        raise RuntimeError("No valid implementation for strategy.")

    def _is_compatible_with(self, strategy):
        for implementation in self.implementations:
            if implementation._is_compatible_with_strategy(strategy):
                return True
        return False

    # Manage requirements from measure registry

    @abstractproperty
    def required_measures(self):
        raise NotImplementedError

    @property
    def required_segmentation(self):
        return []

    @property
    def required_marginal_segmentation(self):
        return []

    @property
    def required_constraints(self):
        return NullConstraint()

    # Metric evaluation

    def evaluate(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        # TODO: Check that strategy has required measures, segmentation and constraints.
        implementation = self.implementation_for_strategy(strategy)
        return implementation.evaluate(
            strategy,
            marginalise=marginalise,
            compatible_metrics=compatible_metrics,
            **self.opts.process(**opts)
        )

    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        implementation = self.implementation_for_strategy(strategy)
        return implementation.get_ir(
            strategy,
            marginalise=marginalise,
            compatible_metrics=compatible_metrics,
            **self.opts.process(**opts)
        )


class CustomMetric(Metric):

    def _init(self):
        self._required_measures = []
        self._required_segmentation = []
        self._required_marginal_segmentation = []
        self._required_constraints = NullConstraint()

    @property
    def required_measures(self):
        return self._required_measures

    def require_measures(self, *measures):
        self._required_measures.extend(measures)
        return self

    @property
    def required_segmentation(self):
        return self._required_segmentation

    def require_segmentation(self, *dimensions):
        self._required_segmentation.extend(dimensions)
        return self

    @property
    def required_marginal_segmentation(self):
        return self._required_marginal_segmentation

    def require_marginal_segmentation(self, *dimensions):
        self._required_marginal_segmentation.extend(dimensions)
        return self

    @property
    def required_constraints(self):
        return self._required_constraints

    def require_constraints(self, **constraints):
        self._required_constraints &= Constraint.from_spec(constraints)
        return self


class MetricImplementation(metaclass=SubclassRegisteringABCMeta):

    REGISTRY_KEYS = None

    def register_for_metric(self, metric):
        self.metric = metric
        return self

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
