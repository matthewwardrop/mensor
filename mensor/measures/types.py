import copy
import re
from abc import ABCMeta, abstractmethod
from collections import OrderedDict, namedtuple

import numpy as np
import pandas as pd
import six
import yaml
from uncertainties.unumpy import uarray

from mensor.utils import startseq_match, OptionsMixin, SequenceMap

from .stats import global_stats_registry

__all__ = ['Join', '_Dimension', '_StatisticalUnitIdentifier', '_Measure', 'EvaluatedMeasures']


class MeasureEvaluator(OptionsMixin, metaclass=ABCMeta):

    @abstractmethod
    def identifier_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def foreign_keys_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def reverse_foreign_keys_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def dimensions_for_unit(self, unit_type, include_partitions=True):
        raise NotImplementedError

    @abstractmethod
    def partitions_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def measures_for_unit(self, unit_type):
        raise NotImplementedError

    # Resolution

    def resolve(self, unit_type, features, role=None, with_attrs=None):
        """
        This method resolves one or more features optionally associated with a
        unit_type and a role. Note that this method is concerned about
        *functional* resolution, so if `role='dimension'` both identifiers and
        measures will be resolved, since they can be used as dimensions.

        Parameters:
            names (str, list<str>): A name or list of names to resolve.
            unit_type (str, None): A unit type for which the resolution should
                be done.
            role (str, None): One of 'measure', 'dimension', 'identifier' or `None`.
            with_attrs (dict, None): Attributes to set on the returned feature.
                Note that these are *additive* to any attributes already inherited
                from feature_type (which are otherwise preserved).

        Returns:
            _Dimension, _Measure, _StatisticalUnitIdentifier: The resolved object.
        """
        return_one = False

        if not isinstance(features, (list, SequenceMap)):
            return_one = True
            features = [features]

        unresolvable = []
        resolved = SequenceMap()
        for feature in features:
            try:
                attrs = with_attrs.copy() if with_attrs else {}
                if isinstance(feature, tuple):
                    feature = FeatureSpec(feature[0], **feature[1])
                if isinstance(feature, dict):
                    feature = FeatureSpec(**feature)
                if isinstance(feature, FeatureSpec):
                    feature, extra_attrs = feature.as_source_with_attrs(unit_type)
                    attrs.update(extra_attrs)
                r = self._resolve(unit_type=unit_type, feature=feature, role=role)._with_attrs(**attrs)
                resolved[r] = r
            except ValueError:
                unresolvable.append(feature)
        if len(unresolvable):
            raise ValueError("Could not resolve {}(s) associated with unit_type '{}' for: '{}'".format(role or 'feature', unit_type.__repr__(), "', '".join(str(dim) for dim in unresolvable)))

        if return_one:
            return resolved.first
        return resolved

    def _resolve(self, unit_type, feature, role=None):
        if not isinstance(feature, six.string_types):
            return feature
        if role in (None, 'identifier', 'dimension', 'foreign_key'):
            if feature in self.foreign_keys_for_unit(unit_type):
                return self.foreign_keys_for_unit(unit_type)[feature]
        if role in (None, 'reverse_foreign_key'):
            if feature in self.reverse_foreign_keys_for_unit(unit_type):
                return self.reverse_foreign_keys_for_unit(unit_type)[feature]
        if role in (None, 'dimension') and feature in self.dimensions_for_unit(unit_type):
            return self.dimensions_for_unit(unit_type)[feature]
        if role in (None, 'dimension', 'measure') and feature in self.measures_for_unit(unit_type):
            return self.measures_for_unit(unit_type)[feature]
        raise ValueError("No such {} for unit type {} named: {}.".format(role or 'feature', unit_type, feature))

    # Evaluation

    @abstractmethod
    def evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                 joins=None, stats=True, covariates=False, **opts):
        raise NotImplementedError

    @abstractmethod
    def get_ir(self, unit_type, measures=None, segment_by=None, where=None,
               joins=None, stats=True, covariates=False, **opts):
        raise NotImplementedError


class FeatureSpec:

    ALLOWED_TRANSFORMS = ['pre_agg', 'agg', 'post_agg', 'pre_rebase_agg', 'rebase_agg', 'post_rebase_agg', 'alias']

    def __init__(self, alias, source=None, transforms=None):
        self._alias = alias
        self._source = source or alias
        self._transforms = transforms or {}

    def __repr__(self):
        return "<Measure" + ("[{alias}]".format(alias=self.alias) if self.alias else "") + (" (with transforms)" if self.transforms else "") + ">"

    def source(self, source):
        self._source = source
        return self

    def with_transforms(self, unit_type, pre_agg=None, agg=None, post_agg=None, pre_rebase_agg=None, rebase_agg=None, post_rebase_agg=None, alias=None):
        variables = locals()
        transforms = {
            name: variables[name]
            for name in self.ALLOWED_TRANSFORMS
            if variables[name] is not None
        }
        if transforms:
            self._transforms[unit_type] = transforms
        return self

    def get_attrs(self, unit_type):
        attrs = {
            'transforms': self._transforms
        }
        if unit_type not in attrs['transforms']:
            attrs['transforms'][unit_type] = {}
        attrs['transforms'][unit_type]['alias'] = self._alias
        return attrs

    def as_source_with_attrs(self, unit_type):
        return self._source, self.get_attrs(unit_type)

    @property
    def as_dict(self):
        return {
            'source': self._source,
            'alias': self._alias,
            'transforms': self._transforms
        }

    @property
    def as_yaml(self):
        return yaml.dump(self.as_dict, default_flow_style=False, indent=4)


class MeasureSpec(FeatureSpec):

    ALLOWED_TRANSFORMS = ['pre_agg', 'agg', 'post_agg', 'pre_rebase_agg', 'rebase_agg', 'post_rebase_agg', 'alias']


class DimensionSpec(FeatureSpec):

    ALLOWED_TRANSFORMS = ['pre_agg', 'pre_rebase_agg', 'alias']


Provision = namedtuple('Provision', ['provider', 'join_prefix', 'measures', 'dimensions'])
FeatureBundle = namedtuple('FeatureBundle', ['unit_type', 'dimensions', 'measures'])


class Join(object):

    # TODO: Review Join API (esp. which arguments are essential, etc)

    def __init__(self, provider, unit_type, left_on, right_on, object,
                 compatible=False, join_prefix=None, name=None, measures=None, dimensions=None,
                 how='left'):
        self.provider = provider
        self.unit_type = unit_type
        self.join_prefix = join_prefix
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
            name = "join_{}_{}".format(self.provider.name, self.unit_type.name)
        self._name = name


# Output types

class EvaluatedMeasures(object):

    @classmethod
    def for_measures(cls, evaluations, stats_registry=None):
        if isinstance(evaluations, EvaluatedMeasures):
            return evaluations
        elif isinstance(evaluations, pd.DataFrame):
            return cls(evaluations, stats_registry=stats_registry)
        raise RuntimeError("Invalid measures type: {}".format(evaluations.__class__.__name__))

    def __init__(self, evaluations, stats_registry=None):
        self._evaluations = evaluations
        self._stats_registry = stats_registry or global_stats_registry

    @property
    def raw(self):
        return self._evaluations

    def to_frame(self, *measures, keep_raw_fields=False):
        measures = list(measures)

        if not measures:
            measures = self.measures

        if keep_raw_fields:
            measures += self.measure_fields

        df = self.raw
        for measure in measures:
            if measure not in df.columns and measure in self.measures:
                df = df.assign(**{measure: self._get_measure(measure)})

        if len(self.dimensions):
            return df.set_index(self.dimensions).sort_index()[measures]
        return df[measures].T[0]

    @property
    def columns(self):
        return self.raw.columns

    @property
    def measure_fields(self):
        return [col for col in self._evaluations.columns if '|' in col]

    @property
    def measures(self):
        return list(set(field.split('|')[0] for field in self.measure_fields))

    @property
    def dimensions(self):
        return [col for col in self._evaluations.columns if '|' not in col]

    def _get_measure_distribution(self, name):
        for field in self.measure_fields:
            if field.startswith(name):
                if len(field.split('|')) == 3:
                    return field.split('|')[1].lower()
                return None

    def _get_measure_distribution_fields(self, name):
        distribution = self._get_measure_distribution(name)
        return self.raw[[
            '{}|{}'.format(name, field) if distribution is None else '{}|{}|{}'.format(name, distribution, field)
            for field in self._stats_registry.distributions.get_stats(distribution)
        ]]

    def _get_measure(self, name):

        # Check if measure exists
        if name not in self.measures:
            raise KeyError(name)

        distribution = self._get_measure_distribution(name)
        distribution_fields = self._get_measure_distribution_fields(name).values.transpose()

        stats = self._stats_registry.distributions.get_scipy_repr(distribution)

        if isinstance(stats, tuple):
            model = stats[0]
            if model:
                params = {
                    param: f(*distribution_fields) for param, f in stats[1].items()
                }
                return pd.Series(uarray(model.mean(**params), model.std(**params)), name=name, index=self.raw.index)
        elif stats:
            return pd.Series(stats(*distribution_fields), name=name, index=self.raw.index)

        return distribution_fields[0]  # If no stats, return raw sum field

    # Allow getting measures by distribution stats
    def __getitem__(self, name):
        if isinstance(name, list):
            return self.to_frame(*name)
        else:
            return self.to_frame(name)[name]

    def segmentby(self, segment_by=None):
        segment_by = segment_by or []
        if len(segment_by):
            return EvaluatedMeasures(
                self._evaluations
                .groupby(segment_by)
                [self.measure_fields]
                .sum()
            )
        return EvaluatedMeasures(self._evaluations[self.measure_fields].sum())

    def query(self, *args, **kwargs):
        # TODO: Replace with `where`, and use Pandas constraint -> pandas expr filters.
        return EvaluatedMeasures(self.raw.query(*args, **kwargs))

    def __repr__(self):
        return self.to_frame().__repr__()

    def _repr_html_(self):
        df = self.to_frame()
        if hasattr(df, '_repr_html_'):
            return df._repr_html_()
        raise NotImplementedError

    def add_prefix(self, prefix):
        return EvaluatedMeasures(self._evaluations.add_prefix(prefix))


def quantilesofscores(self, as_weights=False, *, pre_sorted=False, sort_fields=None):
    idx = self.index.copy()
    s = self
    if not pre_sorted:
        s = s.sort_values()
    if as_weights:
        return (s.cumsum() / s.sum()).reindex(idx)
    return (pd.Series(np.ones(len(s)).cumsum(), index=s.index) / len(s)).reindex(idx)


pd.Series.quantilesofscores = quantilesofscores


# Internal classes

class _FeatureAttrsMixin(object):

    ALLOW_ALL_ATTRIBUTES = False
    EXTRA_ATTRIBUTES = {}

    def __init__(self, name, unit_type=None, via=None, external=False, private=False, implicit=False, kind=None, mask=None, transforms=None, **extra_attrs):

        self.name = name
        self.unit_type = unit_type
        self.external = external
        self.private = private
        self.implicit = implicit
        self.via = via
        self.kind = kind
        self.mask = mask
        self.transforms = transforms

        self._extra_attributes = {}

        for attr, value in extra_attrs.items():
            if self.ALLOW_ALL_ATTRIBUTES or attr in self.EXTRA_ATTRIBUTES:
                setattr(self, attr, value)
            else:
                raise KeyError("No such attribute {}.".format(attr))

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError
        if name in self.EXTRA_ATTRIBUTES:
            return self.EXTRA_ATTRIBUTES[name]
        elif self.ALLOW_ALL_ATTRIBUTES and name in self._extra_attributes:
            return self._extra_attributes.get(name)
        raise AttributeError("No such attribute: {}".format(name))

    # TODO: Catch addition of new attributes via setattr?

    def _with_attrs(self, **attrs):
        obj = copy.copy(self)
        for attr, value in attrs.items():
            if not hasattr(obj, attr):
                if self.ALLOW_ALL_ATTRIBUTES:
                    self._extra_attributes[attr] = value
                else:
                    raise ValueError("'{}' is not a valid feature attribute.".format(attr))
            setattr(obj, attr, value)
        return obj

    @property
    def attrs(self):
        return {
            'name': self.name,
            'unit_type': self.unit_type,
            'external': self.external,
            'private': self.private,
            'implicit': self.implicit,
            'via': self.via,
            'kind': self.kind,
            'mask': self.mask,
            'transforms': self.transforms,
            **{name: getattr(self, name, self.EXTRA_ATTRIBUTES.get(name)) for name in list(self.EXTRA_ATTRIBUTES) + list(self._extra_attributes)}
        }

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not re.match(r'^(?![0-9])[\w_:]+$', name):
            raise ValueError("Invalid feature name '{}'. All names must consist only of word characters, numbers, underscores and colons, and cannot start with a number.".format(name))
        self._name = name

    @property
    def mask(self):
        return self._mask or self.name

    @mask.setter
    def mask(self, mask):
        if mask is not None and not re.match(r'^(?![0-9])[\w_:]+$', mask):
            raise ValueError("Invalid feature mask '{}'. All masks must consist only of word characters, numbers, underscores and colons, and cannot start with a number.".format(mask))
        self._mask = mask

    @property
    def transforms(self):
        return self._transforms or {}

    @transforms.setter
    def transforms(self, transforms):
        # TODO: Check structure of transforms dict
        if not transforms:
            self._transforms = {}
        else:
            self._transforms = transforms

    @property
    def as_external(self):
        return self._with_attrs(external=True)

    @property
    def as_internal(self):
        return self._with_attrs(external=False)

    @property
    def as_private(self):
        return self._with_attrs(private=True)

    @property
    def as_public(self):
        return self._with_attrs(private=False)

    @property
    def as_implicit(self):
        return self._with_attrs(implicit=True)

    @property
    def as_explicit(self):
        return self._with_attrs(implicit=True)

    def with_mask(self, mask):
        return self._with_attrs(mask=mask or None)

    @property
    def via(self):
        return self._via

    @via.setter
    def via(self, via):
        self._via = via or None

    def as_via(self, *vias):
        vias = [via.name if isinstance(via, _ProvidedFeature) else via for via in vias]
        # In the case that we are adding a single via, there cannot be two identical components
        # in a row (patterns can repeat in some instances). To simplify code
        # elsewhere, we suppress via in the case that len(vias) == 1 and the provided
        # via is alrady in the via path.
        current_vias = self.via.split('/') if self.via else []
        if len(vias) == 1 and (not vias[0] or len(current_vias) > 0 and vias[0] == current_vias[-1]):
            return self
        return self._with_attrs(via='/'.join(vias + current_vias))

    @property
    def via_next(self):
        s = self.via.split('/')
        if len(s) > 0:
            return self._with_attrs(unit_type=s[0], via='/'.join(s[1:]))
        return None

    @property
    def unit_type(self):
        return self._unit_type

    @unit_type.setter
    def unit_type(self, unit_type):
        self._unit_type = unit_type

    @property
    def next_unit_type(self):
        if self.via:
            return self.via.split('/')[0]

    # TODO: Use this hash to allow multiple measures based on same source measure
    # @property
    # def attr_hash(self):
    #     if self.alias == self.mask:
    #         return
    #
    #     return hashlib.sha256(
    #         json.dumps(
    #             {
    #                 'alias': self.alias,
    #                 **self._extra_attributes
    #             }, sort_keys=True
    #         )
    #         .encode()
    #     ).hexdigest()

    @property
    def via_name(self):
        # TODO: Use this hash to allow multiple measures based on same source measure?
        # hash_suffix = ("_{}".format(self.attr_hash) if self.attr_hash else '')
        if self.via:
            return '{}/{}'.format(self.via, self.mask)
        return self.mask

    def via_alias(self, unit_type=None):
        if not self.transforms:
            return self.via_name
        if unit_type and unit_type in self.transforms and self.transforms[unit_type].get('alias'):
            return self.transforms[unit_type].get('alias')

        if self.via:
            unit_types = self.via.split('/')
            for i, unit_type in enumerate(unit_types):
                if unit_type in self.transforms and self.transforms[unit_type].get('alias'):
                    return '/'.join(unit_types[:i + 1] + [self.transforms[unit_type].get('alias')])

        return self.via_name

    # Methods to assist MeasureProviders with handling data field names
    def fieldname(self, role=None, unit_type=None):
        return self.via_alias(unit_type=unit_type)

    def prev_fieldname(self, role=None):
        if self.via:
            return "/".join([self.next_unit_type, self.via_next.fieldname(role=role, unit_type=self.next_unit_type)])

    def __lt__(self, other):  # TODO: Where is this used?
        return self.name.__lt__(other.name)

    def __repr__(self):
        attrs = []
        for attr in ['external', 'private', 'implicit']:
            if getattr(self, attr, False):
                attrs.append(attr[0])
        return (
            self.via_name
            + ('[{}]'.format(self.name) if self.mask != self.name else '')
            + ('({})'.format(','.join(attrs)) if attrs else '')
        )


class _ProvidedFeature(_FeatureAttrsMixin):

    @classmethod
    def from_spec(cls, spec, provider=None):
        if isinstance(spec, str):
            spec = {'name': spec}
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

    def __init__(self, name, expr=None, default=None, desc=None, shared=False, provider=None,
                 **attrs):

        _FeatureAttrsMixin.__init__(self, name=name, **attrs)

        self.expr = expr or name
        self.default = default
        self.desc = desc
        self.shared = shared
        self.provider = provider

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if other.via_name == self.via_name:
                return True
            return False
        elif isinstance(other, six.string_types):
            if self.via_name == other:
                return True
            return False
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self.via_name)


class _ResolvedFeature(_FeatureAttrsMixin):

    ALLOW_ALL_ATTRIBUTES = True

    def __init__(self, name, providers=[], **attrs):
        _FeatureAttrsMixin.__init__(self, name=name, **attrs)
        self.providers = providers

    @property
    def path(self):
        if self.unit_type:
            return '/'.join([self.unit_type.name, self.via_name])
        return '*/{}'.format(self.via_name)

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

    def from_provider(self, provider):
        from .provider import MeasureProvider
        if not isinstance(provider, MeasureProvider):
            provider = self.providers[provider]
        return provider.resolve(None, features=self.name)._with_attrs(**self.attrs)

    def __repr__(self):
        return (
            "Resolved([{}/]{}, {})".format(
                (self.unit_type if isinstance(self.unit_type, str) else self.unit_type.mask) if self.unit_type else '*',
                _FeatureAttrsMixin.__repr__(self),
                len(self.providers),
            )
        )

    def __hash__(self):
        return hash(self.via_name)

    def __eq__(self, other):
        if isinstance(other, (_ResolvedFeature, _ProvidedFeature)):
            if other.via_name == self.via_name:
                return True
        elif isinstance(other, six.string_types):
            if self.via_name == other:
                return True
        else:
            return NotImplemented
        return False

    @property
    def desc(self):
        for provider in self.providers:
            instance = self.from_provider(provider)
            if instance.desc:
                return instance.desc


class _Dimension(_ProvidedFeature):

    def __init__(self, name, expr=None, default=None, desc=None, shared=False, partition=False, requires_constraint=False, provider=None):
        _ProvidedFeature.__init__(self, name, expr=expr, default=default, desc=desc, shared=shared, provider=provider)
        if not shared and partition:
            raise ValueError("Partitions must be shared.")
        self.partition = partition
        self.requires_constraint = requires_constraint


class _StatisticalUnitIdentifier(_ProvidedFeature):

    def __init__(self, name, expr=None, desc=None, role='foreign', dummy=False, provider=None):
        _ProvidedFeature.__init__(self, name, expr=expr, desc=desc, shared=not dummy, provider=provider)
        assert role in ('primary', 'unique', 'foreign')
        if dummy:
            assert role == 'primary', "Dummy identifiers currently only makes sense when it is to be treated as primary."
        self.role = role
        self._dummy = dummy

    @property
    def unit_type(self):
        return self.name

    @unit_type.setter
    def unit_type(self, unit_type):
        pass

    @property
    def is_primary(self):
        return self.role == 'primary'

    @property
    def is_unique(self):
        return self.role in ('primary', 'unique')

    @property
    def is_dummy(self):
        """
        If a unit type is a dummy, then it can never be linked to actual data,
        which has the following consequences:
        - The dimensions associated with it can never be used via foreign keys.
        - It cannot be used as a member of `segment_by` in an evaluation.
        - Its data provisions cannot be shared among other providers of the same
          type.

        These semantics are implied by setting 'shared' to False, so that is
        what is done here.

        Note that data is still accessible via reverse foreign keys.
        """
        return self._dummy

    def __repr__(self):
        prefix = suffix = ''
        if self.is_primary:
            prefix = '^'
        elif self.is_unique:
            prefix = '*'
        if self.is_dummy:
            suffix += '(d)'
        return prefix + _ProvidedFeature.__repr__(self) + suffix

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
        elif isinstance(unit_type, _ResolvedFeature):
            assert unit_type.kind in ('identifier', 'foreign_key', 'reverse_foreign_key'), "{} (of type {}) is not a valid unit type.".format(unit_type, type(unit_type))
            unit_type = unit_type.name
        if reverse:
            return startseq_match(unit_type.split(':'), self.name.split(':'))
        return startseq_match(self.name.split(':'), unit_type.split(':'))


class _Measure(_ProvidedFeature):

    def __init__(self, name, expr=None, default=None, desc=None,
                 distribution='normal', shared=False, provider=None):
        _ProvidedFeature.__init__(self, name, expr=expr, default=default, desc=desc, shared=shared, provider=provider)
        self.distribution = distribution

    def transforms_for_unit_type(self, unit_type, stats_registry=None):
        transforms = {
            'pre_agg': None,
            'agg': 'sum',
            'post_agg': None,
            'pre_rebase_agg': None,
            'rebase_agg': 'sum',
            'post_rebase_agg': None
        }
        if isinstance(self.transforms, dict):
            transforms.update(self.transforms.get(unit_type, {}))

        backend_aggs = stats_registry.aggregations.for_provider(self.provider)
        transform_ops = stats_registry.transforms.for_provider(self.provider)

        for key in ['agg', 'rebase_agg']:
            if transforms[key] is not None:
                transforms[key] = backend_aggs[transforms[key]]

        for key in ['pre_agg', 'post_agg', 'pre_rebase_agg', 'post_rebase_agg']:
            if transforms[key] is not None:
                transforms[key] = transform_ops[transforms[key]]

        return transforms

    def fieldname(self, role='measure', unit_type=None):
        name = _ProvidedFeature.fieldname(self, role=role, unit_type=unit_type)
        if role == 'measure':
            return '{}|raw'.format(name)
        return name

    def get_fields(self, unit_type=None, stats=True, rebase_agg=False, stats_registry=None, for_pandas=False):
        """
        This is a convenience method for subclasses to use to get the
        target fields associated with a particular distribution.

        Parameters:
            stats (bool): Whether this measure is being aggregated into
                distribution statistics.

        Returns:
            OrderedDict: A mapping of field suffixes to agg methods to collect
                in order to reproduce the distribution from which a measure was
                sampled.
        """
        assert stats_registry is not None
        assert not (rebase_agg and stats)
        if for_pandas:
            from mensor.backends.pandas import PandasMeasureProvider
            provider = PandasMeasureProvider
        else:
            provider = self.provider

        if stats:
            transforms = self.transforms_for_unit_type(unit_type, stats_registry=stats_registry)
            return OrderedDict([
                (
                    (
                        "{via_name}|{field_name}".format(via_name=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None), field_name=field_name)
                        if self.distribution is None else
                        "{via_name}|{dist_name}|{field_name}".format(via_name=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None), field_name=field_name, dist_name=self.distribution.lower())
                    ),
                    {
                        'pre_agg': transforms['pre_agg'],
                        'agg': agg_method
                    }
                )
                for field_name, agg_method in stats_registry.distribution_for_provider(self.distribution, provider).items()
            ])
        else:
            transforms = self.transforms_for_unit_type(unit_type, stats_registry=stats_registry)
            return OrderedDict([
                (
                    '{fieldname}|raw'.format(fieldname=self.fieldname(role=None, unit_type=unit_type if not rebase_agg else None)),
                    {
                        'agg': transforms['rebase_agg'] if rebase_agg else transforms['agg'],
                        'pre_agg': transforms['pre_rebase_agg'] if rebase_agg else transforms['pre_agg'],
                        'post_agg': transforms['post_rebase_agg'] if rebase_agg else transforms['post_agg'],
                    }
                )
            ])

    @classmethod
    def get_all_fields(self, measures, unit_type=None, stats=True, rebase_agg=False, stats_registry=None, for_pandas=False):
        fields = []
        for measure in measures:
            fields.extend(measure.get_fields(unit_type=unit_type, stats=stats, rebase_agg=rebase_agg, stats_registry=stats_registry, for_pandas=for_pandas))
        return fields
