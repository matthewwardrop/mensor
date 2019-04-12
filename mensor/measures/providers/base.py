import os
import uuid
from abc import abstractmethod, abstractproperty

import six
import yaml

from mensor.utils import OptionsMixin, SequenceMap
from mensor.utils.registry import SubclassRegisteringABCMeta

from ..common.feature_spec import FeatureSpec


class MeasureProvider(OptionsMixin, metaclass=SubclassRegisteringABCMeta):
    """
    The primitive abstract class that is the parent of all measure providers.

    This class defines the minimal API contract required of all providers of
    measures in the `mensor` universe. Every `MeasureProvider` instance can be
    thought of as a proxy to a data source, allowing identifiers, measures and
    dimensions to be evaluated in different contexts; and the class exists
    simply to provide metadata about the data stored therein.
    """

    REGISTRY_KEYS = None

    @classmethod
    def from_yaml(cls, yml):
        if '\n' not in yml:
            with open(os.path.expanduser(yml)) as f:
                return cls.from_dict(yaml.safe_load(f))
        else:
            return cls.from_dict(yaml.safe_load(yml))

    @classmethod
    def from_dict(cls, d):
        assert 'kind' in d
        assert d.get('role') in (None, 'provider')
        klass = cls.for_kind(d['kind'])
        return klass._from_dict(d)

    def _from_dict(cls, d):
        raise NotImplementedError

    def __init__(self, name=None):
        # TODO: Support adding metadata like measure provider maintainer
        self.name = name or str(uuid.uuid4())

    # Statistical unit specifications
    @abstractproperty
    def identifiers(self):
        raise NotImplementedError

    @abstractmethod
    def identifier_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def foreign_keys_for_unit(self, unit_type):
        raise NotImplementedError

    @abstractmethod
    def reverse_foreign_keys_for_unit(self, unit_type):
        raise NotImplementedError

    # Dimensions
    @abstractmethod
    def dimensions_for_unit(self, unit_type, include_partitions=True):
        raise NotImplementedError

    @abstractmethod
    def partitions_for_unit(self, unit_type):
        raise NotImplementedError

    # Measures
    @abstractmethod
    def measures_for_unit(self, unit_type):
        raise NotImplementedError

    # Provisions
    # TODO: Upgrade from MutableMeasureProvider?

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

    @abstractmethod
    def evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                 joins=None, stats_registry=None, stats=True, covariates=False, **opts):
        pass

    @abstractmethod
    def get_ir(self, unit_type, measures=None, segment_by=None, where=None,
               joins=None, stats_registry=None, stats=True, covariates=False, **opts):
        pass

    # MeasureProvider compatibility
    def is_compatible_with(self, provider):
        '''
        If this method returns True, this MeasureProvider can take responsibility
        for evaluation and/or interpreting the required fields from the provided
        provider; otherwise, any required joins will be performed in memory in
        pandas.
        '''
        return False

    # Runtime introspection
    def show(self, *unit_types, kind=None):
        unit_types = [self.identifier_for_unit(ut) for ut in unit_types] if len(unit_types) > 0 else sorted(self.identifiers)
        if isinstance(kind, str):
            kind = [kind]
        if not kind:
            kind = ['foreign_key', 'reverse_foreign_key', 'dimension', 'partition', 'measure']

        for unit_type in unit_types:
            section_title = "{}:{}".format(
                unit_type.name,
                " [{}]".format(unit_type.desc) if unit_type.desc else ""
            )
            section_title_shown = False

            features = {
                'foreign_key': self.foreign_keys_for_unit(unit_type),
                'reverse_foreign_key': self.reverse_foreign_keys_for_unit(unit_type),
                'dimension': self.dimensions_for_unit(unit_type, include_partitions=False),
                'partition': self.partitions_for_unit(unit_type),
                'measure': self.measures_for_unit(unit_type)
            }

            for k in kind:
                feature_name = "{}s".format(k.replace('_', ' ').title())
                feature_set = features[k]
                if not len(feature_set) or len(feature_set) == 1 and feature_set.first == unit_type:
                    continue
                if not section_title_shown:
                    print(section_title)
                    section_title_shown = True
                print("    {}:".format(feature_name))
                for feature in sorted(feature_set):
                    if feature != unit_type:
                        print(
                            "        - {}{}".format(
                                feature.mask,
                                " [{}]".format(feature.desc) if feature.desc else ""
                            )
                        )
