import copy
import numpy as np
import pandas as pd
import six
from pandas import Series
from uncertainties.unumpy import uarray

from ..utils import startseq_match

__all__ = ['Join', '_Dimension', '_StatisticalUnitIdentifier', '_Measure', 'MeasureDataFrame']


class Join(object):

    def __init__(self, provider, unit_type, object, compatible=False, how='left'):
        self.provider = provider
        self.unit_type = unit_type
        self.object = object
        self.compatible = compatible
        self.how = how


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

    def matches(self, unit_type):
        '''
        Checks to see whether unit_type is at least as specific as this identifier.
        For example:
        'user'.matches('user:guest') == True
        'user:guest'.matches('user:guest') == True
        'user:guest'.matches('user') == False
        '''
        if isinstance(unit_type, _StatisticalUnitIdentifier):
            unit_type = unit_type.name
        return startseq_match(self.name.split(':'), unit_type.split(':'))


class _Measure(_Dimension):

    # TODO: Types of measures
    # raw: <name>:type = 'exact', <name>:sum, <name>:sample_size
    # normal distribution: <name>:type = 'normal', <name>:sum, <name>:sos, <name>:sample_size
    # binomial distribution: <name>:type = 'binomial', <name>:sum, <name>:sample_size
    # other

    def __init__(self, name, expr=None, desc=None, unit_agg=None, measure_agg='normal', shared=False, provider=None):
        _Dimension.__init__(self, name, expr=expr, desc=desc, shared=shared, provider=provider)
        self.unit_agg = unit_agg
        self.measure_agg = measure_agg


class MeasureDataFrame(pd.DataFrame):

    @property
    def _constructor(self):
        return MeasureDataFrame

    @property
    def _constructor_sliced(self):
        return pd.Series

    # Allow getting measures by distribution stats
    def __getitem__(self, name):
        try:
            return pd.DataFrame.__getitem__(self, name)
        except KeyError as e:
            if ':' not in name:
                if '{}:norm:sum'.format(name) in self.columns:
                    mean = self['{}:norm:sum'.format(name)] / self['{}:norm:count'.format(name)]
                    var = (self['{}:norm:sos'.format(name)] - self['{}:norm:sum'.format(name)]**2 / self['{}:norm:count'.format(name)]) / (self['{}:norm:count'.format(name)] - 1)
                    return pd.Series(uarray(mean, np.sqrt(var)), name=name, index=self.index)
            raise e


def quantilesofscores(self, as_weights=False, *, pre_sorted=False, sort_fields=None):
    idx = self.index.copy()
    s = self
    if not pre_sorted:
        s = s.sort_values()
    if as_weights:
        return (s.cumsum() / s.sum()).reindex(idx)
    return (pd.Series(np.ones(len(s)).cumsum(), index=s.index) / len(s)).reindex(idx)


Series.quantilesofscores = quantilesofscores
