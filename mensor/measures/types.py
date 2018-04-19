import copy
import re
import uuid
from collections import namedtuple, OrderedDict
from enum import Enum

import numpy as np
import pandas as pd
import scipy.stats.distributions
import six
from uncertainties import ufloat
from uncertainties.unumpy import uarray

from ..utils import startseq_match

__all__ = ['Join', '_Dimension', '_StatisticalUnitIdentifier', '_Measure', 'MeasureDataFrame']


class Join(object):

    # TODO: Review Join API (esp. which arguments are essential, etc)

    def __init__(self, provider, unit_type, left_on, right_on, object,
                 compatible=False, name=None, measures=None, dimensions=None,
                 how='left'):
        self.provider = provider
        self.unit_type = unit_type
        self.left_on = left_on
        self.right_on = right_on
        self.name = name
        self.measures = measures
        self.dimensions = dimensions
        self.object = object
        self.compatible = compatible
        self.how = how

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if name is None:
            name = str(uuid.uuid1())
        self._name = name


class _Dimension(object):

    @classmethod
    def from_spec(cls, spec, provider=None):
        if isinstance(spec, cls):
            spec.provider = provider
            return spec
        elif isinstance(spec, str):
            return cls(name=spec, provider=provider)
        elif isinstance(spec, dict):
            spec.update({'provider': provider})
            return cls(**spec)
        else:
            raise ValueError("Unrecognised specification of {}: {}".format(cls.__name__, spec))

    def __init__(self, name, expr=None, desc=None, shared=False, provider=None, external=False, private=False, via=None):
        self.name = name
        self.expr = expr or name
        self.desc = desc
        self.shared = shared
        self.provider = provider
        self.external = external
        self.private = private
        self.via = via

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not re.match(r'^(?![0-9])[\w_:]+$', name):
            raise ValueError("Invalid dimension name. All names must consist only of word characters, numbers, underscores and colons, and cannot start with a number.")
        self._name = name

    def __repr__(self):
        attrs = (['e'] if self.external else []) + (['p'] if self.private else [])
        return ('/'.join([self.via, self.name]) if self.via is not None else self.name) + ('({})'.format(','.join(attrs)) if attrs else '')

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if other.name == self.name:
                return True
            return False
        elif isinstance(other, six.string_types):
            if self.name == other:
                return True
            return False
        else:
            return NotImplemented

    @property
    def as_external(self):
        dim = copy.copy(self)
        dim.external = True
        return dim

    @property
    def as_private(self):
        dim = copy.copy(self)
        dim.private = True
        return dim

    def as_via(self, *vias):
        vias = [via.name if isinstance(via, _Dimension) else via for via in vias]
        dim = copy.copy(self)
        current_vias = dim.via.split('/') if self.via is not None else []
        dim.via = '/'.join(vias + current_vias)
        return dim

    @property
    def via_name(self):
        if self.via is None:
            return self.name
        return '{}/{}'.format(self.via, self.name)

    def __lt__(self, other):
        return self.name.__lt__(other.name)


class _StatisticalUnitIdentifier(_Dimension):

    def __init__(self, name, expr=None, desc=None, role='foreign', provider=None):
        _Dimension.__init__(self, name, expr=expr, desc=desc, shared=True, provider=provider)
        assert role in ('primary', 'unique', 'foreign')
        self.role = role

    @property
    def unit_type(self):
        return self.name

    @property
    def is_primary(self):
        return self.role == 'primary'

    @property
    def is_unique(self):
        return self.role in ('primary', 'unique')

    def __repr__(self):
        prefix = ''
        if self.is_primary:
            prefix = '^'
        if self.is_unique:
            prefix = '*'
        return prefix + _Dimension.__repr__(self)

    def matches(self, unit_type, reverse=False):
        '''
        Checks to see whether unit_type is at least as specific as this identifier.
        For example:
        'user'.matches('user:guest') == True
        'user:guest'.matches('user:guest') == True
        'user:guest'.matches('user') == False

        If `reverse`, then checks to see whether this unit type is at least as
        specific as `unit_type`.
        '''
        if isinstance(unit_type, _StatisticalUnitIdentifier):
            unit_type = unit_type.name
        if reverse:
            return startseq_match(unit_type.split(':'), self.name.split(':'))
        return startseq_match(self.name.split(':'), unit_type.split(':'))


class AGG_METHODS(Enum):
    MEAN = 'mean'
    SUM = 'sum'
    SQUARE_SUM = 'sqsum'
    COUNT = 'count'


class MEASURE_AGG_METHODS(Enum):
    MEAN = 'mean'
    SUM = 'sum'
    COUNT = 'count'


class DISTRIBUTIONS(Enum):
    NONE = None
    NORMAL = 'normal'
    BINOMIAL = 'binomial'


DISTRIBUTION_FIELDS = {
    DISTRIBUTIONS.NONE: OrderedDict([
        ('sum', AGG_METHODS.SUM),
        ('count', AGG_METHODS.COUNT)
    ]),
    DISTRIBUTIONS.NORMAL: OrderedDict([
        ('sum', AGG_METHODS.SUM),
        ('sos', AGG_METHODS.SQUARE_SUM),
        ('count', AGG_METHODS.COUNT)
    ]),
    DISTRIBUTIONS.BINOMIAL: OrderedDict([
        ('sum', AGG_METHODS.SUM),
        ('count', AGG_METHODS.COUNT)
    ])
}

DISTRIBUTION_STATS = {
    DISTRIBUTIONS.NONE: {
        MEASURE_AGG_METHODS.MEAN: lambda sum, count: sum / count,
        MEASURE_AGG_METHODS.SUM: lambda sum, count: count
    },
    DISTRIBUTIONS.NORMAL: {
        MEASURE_AGG_METHODS.MEAN: (
            scipy.stats.distributions.norm,
            {
                'loc': lambda sum, sos, count: sum / count,
                'scale': lambda sum, sos, count: np.sqrt((sos - sum**2 / count) / (count - 1)) / count
            }
        ),
        MEASURE_AGG_METHODS.SUM: (
            scipy.stats.distributions.norm,
            {
                'loc': lambda sum, sos, count: sum,
                'scale': lambda sum, sos, count: np.sqrt((sos - sum**2 / count) / (count - 1))
            }
        )
    },
    DISTRIBUTIONS.BINOMIAL: {
        MEASURE_AGG_METHODS.MEAN: (
            scipy.stats.distributions.binom,
            {
                'n': lambda sum, count: count,
                'p': lambda sum, count: sum / count
            }
        ),
        MEASURE_AGG_METHODS.MEAN: (
            scipy.stats.distributions.binom,
            {
                'n': lambda sum, count: count,
                'p': lambda sum, count: sum / count
            }
        )
    }
}


class _Measure(_Dimension):

    # TODO: Types of measures
    # raw: <name>:type = 'exact', <name>:sum, <name>:sample_size
    # normal distribution: <name>:type = 'normal', <name>:sum, <name>:sos, <name>:sample_size
    # binomial distribution: <name>:type = 'binomial', <name>:sum, <name>:sample_size
    # other

    def __init__(self, name, expr=None, desc=None, unit_agg='sum',
                 distribution='normal', shared=False, provider=None):
        _Dimension.__init__(self, name, expr=expr, desc=desc, shared=shared, provider=provider)
        self.unit_agg = unit_agg if isinstance(unit_agg, AGG_METHODS) else AGG_METHODS(unit_agg)
        self.distribution = distribution if isinstance(distribution, DISTRIBUTIONS) else DISTRIBUTIONS(distribution)


class MeasureDataFrame(pd.DataFrame):
    """
    This is a hacky prototype of what will be a convenient way to access measures
    and metrics via a DataFrame.
    """

    @property
    def _constructor(self):
        return MeasureDataFrame

    @property
    def _constructor_sliced(self):
        return pd.Series

    @property
    def measure_fields(self):
        return [col for col in self.columns if '|' in col]

    @property
    def measures(self):
        return list(set(field.split('|')[0] for field in self.measure_fields))

    @property
    def dimensions(self):
        return [col for col in self.columns if '|' not in col]

    def _get_measure_distribution(self, name):
        for field in self.measure_fields:
            if field.startswith(name):
                if len(field.split('|')) == 3:
                    return DISTRIBUTIONS[field.split('|')[1].upper()]
                return DISTRIBUTIONS.NONE

    def _get_measure_distribution_fields(self, name):
        distribution = self._get_measure_distribution(name)
        if distribution == DISTRIBUTIONS.NONE:
            return self[['{}|{}'.format(name, field) for field in DISTRIBUTION_FIELDS[distribution]]]
        else:
            return self[['{}|{}|{}'.format(name, distribution.name.lower(), field) for field in DISTRIBUTION_FIELDS[distribution]]]

    def _get_measure(self, name):
        if '|' in name:
            name, feature = name.split('|', 1)
        else:
            feature = "sum"
        feature = MEASURE_AGG_METHODS(feature)

        # Check if measure exists
        if name not in self.measures:
            raise KeyError

        distribution = self._get_measure_distribution(name)
        distribution_fields = self._get_measure_distribution_fields(name).values.transpose()

        stats = DISTRIBUTION_STATS[distribution][feature]

        if isinstance(stats, tuple):
            model = stats[0]
            params = {
                param: f(*distribution_fields) for param, f in stats[1].items()
            }
            return pd.Series(uarray(model.mean(**params), model.std(**params)), name=name, index=self.index)
        else:
            return pd.Series(stats(*distribution_fields), name=name, index=self.index)

    # Allow getting measures by distribution stats
    def __getitem__(self, name):
        try:
            return pd.DataFrame.__getitem__(self, name)
        except KeyError as e:
            try:
                return self._get_measure(name)
            except KeyError:
                raise e
            # if '|' not in name:
            #     distribution =
            #     if '{}|normal|sum'.format(name) in self.columns:
            #         mean = self['{}|normal|sum'.format(name)] / self['{}|normal|count'.format(name)]
            #         var = (self['{}|normal|sos'.format(name)] - self['{}|normal|sum'.format(name)]**2 / self['{}|normal|count'.format(name)]) / (self['{}|normal|count'.format(name)] - 1) / self['{}|normal|count'.format(name)]
            #         return pd.Series(uarray(mean, np.sqrt(var)), name=name, index=self.index)
            #     elif '{}|count'.format(name) in self.columns:
            #         return self['{}|count'.format(name)]
            raise e

    def segmentby(self, segment_by=None):
        segment_by = segment_by or []
        if len(segment_by):
            return (
                self
                .groupby(segment_by)
                [self.measure_fields]
                .sum()
            )
        return self[self.measure_fields].sum()

    @property
    def as_measures(self):
        measures = pd.DataFrame().assign(
            **{dimension: self[dimension] for dimension in self.dimensions},
            **{measure: self[measure] for measure in self.measures}
        )
        if len(self.dimensions):
            measures = measures.set_index(self.dimensions)
        return measures

    def _repr_html_(self):
        return "Use `.as_measures` to get metrics as data frame.<br/>" + pd.DataFrame._repr_html_(self)

    def __repr__(self):
        return "Use `.as_measures` to get metrics as data frame.\n" + pd.DataFrame.__repr__(self)

    def _reduce(self, *args, **kwargs):
        out = pd.DataFrame._reduce(self, *args, **kwargs)
        if isinstance(out, pd.Series):
            return MeasureSeries(out)
        return out


class MeasureSeries(pd.Series):

    @property
    def measure_fields(self):
        return [col for col in self.index if '|' in col]

    @property
    def measures(self):
        return list(set(field.split('|')[0] for field in self.measure_fields))

    @property
    def dimensions(self):
        return [col for col in self.index if '|' not in col]

    # Allow getting measures by distribution stats
    def __getitem__(self, name):
        try:
            return pd.Series.__getitem__(self, name)
        except KeyError as e:
            if '|' not in name:
                if '{}|norm|sum'.format(name) in self.index:
                    mean = self['{}|norm|sum'.format(name)] / self['{}|norm|count'.format(name)]
                    var = (self['{}|norm|sos'.format(name)] - self['{}|norm|sum'.format(name)]**2 / self['{}|norm|count'.format(name)]) / (self['{}|norm|count'.format(name)] - 1) / self['{}|norm|count'.format(name)]
                    return ufloat(mean, np.sqrt(var))
                elif '{}|count'.format(name) in self.index:
                    return self['{}|count'.format(name)]
            raise e

    @property
    def as_measures(self):
        return pd.Series(
            {measure: self[measure] for measure in self.measures}
        )


def quantilesofscores(self, as_weights=False, *, pre_sorted=False, sort_fields=None):
    idx = self.index.copy()
    s = self
    if not pre_sorted:
        s = s.sort_values()
    if as_weights:
        return (s.cumsum() / s.sum()).reindex(idx)
    return (pd.Series(np.ones(len(s)).cumsum(), index=s.index) / len(s)).reindex(idx)


pd.Series.quantilesofscores = quantilesofscores


class _ResolvedDimension(object):

    def __init__(self, name, via='', providers=[], external=False, private=False):
        self.name = name
        self.via = via
        self.providers = providers
        self.external = external
        self.private = private

    @property
    def path(self):
        return '/'.join('/'.join([self.via, self.name]).split('/')[1:])

    @property
    def providers(self):
        return self._providers

    @providers.setter
    def providers(self, providers):
        from .provider import MeasureProvider
        self._providers = {}
        if isinstance(providers, list):
            for provider in providers:
                assert isinstance(provider, MeasureProvider), "Invalid provider of type({})".format(type(provider))
                self._providers[provider.name] = provider
        elif isinstance(providers, dict):
            self._providers.update(providers)
        else:
            raise ValueError("Invalid provider specification.")

    @property
    def via_next(self):
        s = self.via.split('/')
        if len(s) > 1:
            return s[1]

    @property
    def resolved_next(self):
        s = self.via.split('/')
        if len(s) > 1:
            return self.__class__(self.name, via='/'.join(s[1:]), providers=self.providers, external=self.external)
        return self

    @property
    def as_external(self):
        return self.__class__(self.name, via=self.via, providers=self.providers, external=True, private=self.private)

    @property
    def as_private(self):
        return self.__class__(self.name, via=self.via, providers=self.providers, external=self.external, private=True)

    def from_provider(self, provider):
        from .provider import MeasureProvider
        if not isinstance(provider, MeasureProvider):
            provider = self.providers[provider]

        dim = provider.resolve(self.name)
        if self.external:
            dim = dim.as_external
        if self.private:
            dim = dim.as_private

        return dim

    def choose_provider(self, provider):
        self.providers = {provider: self.providers[provider]}

    def __repr__(self):
        attrs = (['e'] if self.external else []) + (['p'] if self.private else [])
        return ('/'.join([self.via, self.name]) if self.via is not None else self.name) + ('({})'.format(','.join(attrs)) if attrs else '')

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, _ResolvedDimension):
            if other.name == self.name and other.via == self.via:
                return True
        elif isinstance(other, _Dimension):
            if other.name == self.name:
                return True
        elif isinstance(other, six.string_types):
            if '/'.join([self.via, self.name]) == other:
                return True
        else:
            return NotImplemented
        return False


class _ResolvedMeasure(_ResolvedDimension):

    pass


Provision = namedtuple('Provision', ['provider', 'measures', 'dimensions'])
DimensionBundle = namedtuple('DimensionBundle', ['dimensions', 'measures'])
