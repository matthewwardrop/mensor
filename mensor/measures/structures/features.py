"""Classes used to represent features internally."""
import re

import six

from mensor.utils import startseq_match


class Feature:
    """
    The generic base class from which all features offered by a `MeasureProvider`
    are derived.
    """

    # TODO: re-add support for loading from configuration
    # @classmethod
    # def from_spec(cls, spec, provider=None):
    #     if isinstance(spec, str):
    #         spec = {'name': spec}
    #     if isinstance(spec, cls):
    #         spec.provider = provider
    #         return spec
    #     elif isinstance(spec, str):
    #         return cls(name=spec, provider=provider)
    #     elif isinstance(spec, dict):
    #         spec.update({'provider': provider})
    #         return cls(**spec)
    #     else:
    #         raise ValueError("Unrecognised specification of {}: {}".format(cls.__name__, spec))

    def __init__(self, name, expr=None, default=None, desc=None, shared=False, provider=None):
        self.name = name
        self.expr = expr or name
        self.default = default
        self.desc = desc
        self.shared = shared
        self.provider = provider

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not re.match(r'^(?![0-9])[\w\._:]+$', name):
            raise ValueError("Invalid feature name '{}'. All names must consist only of word characters, numbers, underscores and colons, and cannot start with a number.".format(name))
        self._name = name

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

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self.name}>"

    def resolve(self):
        from .resolved import ResolvedFeature
        return ResolvedFeature(self)


class Identifier(Feature):

    def __init__(self, name, expr=None, desc=None, role='foreign', provider=None):
        assert role in ('primary', 'unique', 'foreign', 'relation')
        self.role = role
        super().__init__(name, expr=expr, desc=desc, shared=True, provider=provider)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        if not re.match(r'^(?:\!)?(?![0-9])[\w\._:]+$', name):
            raise ValueError("Invalid feature name '{}'. All names must consist only of word characters, numbers, underscores and colons, and cannot start with a number.".format(name))
        if name.startswith('!') and not self.is_relation:
            raise ValueError("Only provider level relations can be prefixed with '!'.")
        self._name = name

    @property
    def is_primary(self):
        return self.role in ('primary', 'relation')

    @property
    def is_unique(self):
        return self.role in ('primary', 'unique', 'relation')

    @property
    def is_relation(self):
        """
        If a unit type is a relation, then it can never be linked to actual data,
        which has the following consequences:
        - The dimensions associated with it can never be used via foreign keys.
        - It cannot be used as a member of `segment_by` in an evaluation.
        - Its data provisions cannot be shared among other providers of the same
          type.

        These semantics are implied by setting 'shared' to False, so that is
        what is done here.

        Note that data is still accessible via reverse foreign keys.
        """
        return self.role == 'relation'

    def __repr__(self):
        prefix = suffix = ''
        if self.is_primary:
            prefix = '^'
        elif self.is_unique:
            prefix = '*'
        if self.is_relation:
            suffix += '(r)'
        return f"Identifier<{prefix}{self.name}{suffix}>"

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
        from .resolved import ResolvedFeature

        if isinstance(unit_type, Identifier):
            unit_type = unit_type.name
        # elif isinstance(unit_type, ResolvedFeature):  TODO: Fix this
        #     assert unit_type.kind in ('identifier', 'foreign_key', 'reverse_foreign_key'), "{} (of type {}) is not a valid unit type.".format(unit_type, type(unit_type))
        #     unit_type = unit_type.name
        if reverse:
            return startseq_match(unit_type.split(':'), self.name.split(':'))
        return startseq_match(self.name.split(':'), unit_type.split(':'))


class Dimension(Feature):

    def __init__(self, name, expr=None, default=None, desc=None, shared=False, partition=False, requires_constraint=False, provider=None):
        super().__init__(name, expr=expr, default=default, desc=desc, shared=shared, provider=provider)
        if not shared and partition:
            raise ValueError("Partitions must be shared.")
        self.partition = partition
        self.requires_constraint = requires_constraint


class Measure(Feature):

    def __init__(self, name, expr=None, default=None, desc=None,
                 distribution='normal', shared=False, provider=None):
        super().__init__(name, expr=expr, default=default, desc=desc, shared=shared, provider=provider)
        self.distribution = distribution
