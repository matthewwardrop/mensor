# TODO: metrics.for_segment(Segment/Target instance).metrics.bookings.evaluate()
# TODO: metrics.for_segment(Segment/Target instance).measures.bookings.evaluate()
# TODO: metrics.for_segment(Segment/Target instance).measures.evaluate('bookings/trains')
#
# TODO: metrics.measures.booking_value.evaluate(segment_by='test')

import six

from ..utils import AttrDict
from .types import _Dimension, _Measure, _StatisticalUnitIdentifier, Join, MeasureDataFrame

__all__ = ['MeasureProvider']


class MeasureProvider(object):

    def __init__(self, name=None, *, identifiers=None, measures=None, dimensions=None):
        # TODO: Support adding metadata like measure provider maintainer
        self.name = name

        self.identifiers = identifiers
        self.dimensions = dimensions
        self.measures = measures

    def _get_dimensions_from_specs(self, cls, specs):
        dims = AttrDict()
        if specs is None:
            return dims
        for spec in specs:
            dim = cls.from_spec(spec, provider=self)
            dims[dim] = dim
        return dims

    def __repr__(self):
        return '{}<{}>'.format(self.__class__.__name__, self.name)

    # Statistical unit specifications

    @property
    def identifiers(self):
        '''
        Dict matching type of abstract statistical unit ('user', 'user:guest', 'user:host',
        'reservation', etc) to a material internal id specification.
        To use a namespace, add a name after a ':' character,
        e.g. 'user:guest' or 'user:host', whereupon all of the features
        granted to a 'user' type will be prefixed in this context,
        e.g. 'guest:dim_country'
        '''
        return self._identifiers

    @identifiers.setter
    def identifiers(self, identifiers):
        self._identifiers = self._get_dimensions_from_specs(_StatisticalUnitIdentifier, identifiers)

    def add_identifier(self, unit_type=None, expr=None, role='foreign'):
        identifier = _StatisticalUnitIdentifier(unit_type, expr=expr, role=role, provider=self)
        self._identifiers[identifier] = identifier
        return self

    @property
    def unit_types(self):
        return set(self._identifiers.keys())

    def identifier_for_unit(self, unit_type):
        if unit_type in self.identifiers:
            return self.identifiers[unit_type]
        for identifier in sorted(self.identifiers, key=lambda x: len(x.name), reverse=True):
            if identifier.matches(unit_type):
                return identifier
        raise ValueError("No such identifier: '{}'.".format(unit_type))

    def foreign_keys_for_unit(self, unit_type=None):
        if unit_type is None:
            return self.identifiers
        unit_type = self.identifier_for_unit(unit_type)

        foreign_keys = {}
        for foreign_key in self.foreign_keys:
            if self._unit_has_foreign_key(unit_type, foreign_key):
                foreign_keys[foreign_key] = foreign_key
        return foreign_keys

    def _unit_has_foreign_key(self, unit_type, foreign_key):
        return unit_type.is_unique

    # Dimension secifications

    @property
    def dimensions(self):
        return self._dimensions

    @dimensions.setter
    def dimensions(self, dimensions):
        self._dimensions = self._get_dimensions_from_specs(_Dimension, dimensions)

    def add_dimension(self, name=None, desc=None, expr=None, shared=False):
        dimension = _Dimension(name, desc=desc, expr=expr, shared=shared, provider=self)
        self._dimensions[dimension] = dimension
        return self

    def dimensions_for_unit(self, unit_type=None):
        if unit_type is None:
            return self.dimensions
        unit_type = self.identifier_for_unit(unit_type)

        dimensions = {}
        for dimension in self.dimensions:
            if self._unit_has_dimension(unit_type, dimension):
                dimensions[dimension] = dimension
        return dimensions

    def _unit_has_dimension(self, unit_type, dimension):
        return unit_type.is_unique

    # Measure specifications

    @property
    def measures(self):
        return self._measures

    @measures.setter
    def measures(self, measures):
        self._measures = self._get_dimensions_from_specs(_Measure, measures)

    def add_measure(self, name=None, expr=None, desc=None, shared=False, unit_agg=None, measure_agg='normal'):
        measure = _Measure(name, expr=expr, desc=desc, shared=shared, unit_agg=unit_agg, measure_agg=measure_agg, provider=self)
        self._measures[measure] = measure
        return self

    def measures_for_unit(self, unit_type=None):
        if unit_type is None:
            return self.measures
        unit_type = self.identifier_for_unit(unit_type)

        measures = {}
        for measure in self.measures:
            if self._unit_has_measure(unit_type, measure):
                measures[measure] = measure
        return measures

    def _unit_has_measure(self, unit_type, measure):
        return True

    # Resolution

    def resolve(self, names, unit_type=None, kind=None):
        if isinstance(names, dict):
            names = list(names)
        if not isinstance(names, list):
            return self._resolve(names, unit_type=unit_type, kind=kind)

        unresolvable = []
        resolved = {}
        for name in names:
            try:
                r = self._resolve(name, unit_type=unit_type, kind=kind)
                resolved[r] = r
            except ValueError:
                unresolvable.append(name)
        if len(unresolvable):
            raise RuntimeError("Could not resolve {}(s) for: '{}'".format(kind or 'dimension', "', '".join(str(dim) for dim in unresolvable)))
        return resolved

    def _resolve(self, name, unit_type=None, kind=None):
        if not isinstance(name, six.string_types):
            return name
        if kind in (None, 'identifier', 'dimension') and name in self.foreign_keys_for_unit(unit_type):
            return self.identifiers[name]
        if kind in (None, 'dimension') and name in self.dimensions_for_unit(unit_type):
            return self.dimensions[name]
        if kind in (None, 'dimension', 'measure') and name in self.measures_for_unit(unit_type):
            return self.measures[name]
        raise ValueError("No such {} name: {}.".format(kind or 'dimension', name))

    # Measure evaluation
    def _prepare_evaluation_args(f):
        def wrapped(self, unit_type, measures=None, segment_by=None, where=None, join=None, **opts):
            unit_type = self.identifier_for_unit(unit_type)
            measures = {} if measures is None else self.resolve(measures, kind='measure')
            segment_by = {} if segment_by is None else self.resolve(segment_by, kind='dimension')
            return f(self, unit_type, measures=measures, segment_by=segment_by, where=where, join=join, **opts)
        return wrapped

    @_prepare_evaluation_args
    def evaluate(self, unit_type, measures=None, segment_by=None, where=None, join=None, **opts):
        # TODO: Enforce that all arguments have the correct types, to simplify
        # subclasses work

        joins = []
        post_joins = []

        for j in join:
            if j.compatible:
                joins.append(j)
            else:
                post_joins.append(j)

        result = MeasureDataFrame(
            self._evaluate(
                unit_type,
                measures,
                where=where,
                segment_by=segment_by,
                join=joins, **opts
            )
        )

        # Join in precomputed
        if len(post_joins) > 0:
            for join in post_joins:
                result = (
                    result
                    .merge(join.object, on=join.unit_type.name, how=join.how)
                    .drop([d for d in segment_by if d.private], axis=1)
                    .groupby(
                        [x.via_name for x in segment_by if not x.private]
                    )
                    .sum()
                    .reset_index()
                )

        return MeasureDataFrame(result)

    def _evaluate(self, unit_type, measures=None, segment_by=None, where=None, join=None, **opts):
        raise NotImplementedError("Generic implementation not implemented.")

    @_prepare_evaluation_args
    def get_ir(self, unit_type, measures=None, segment_by=None, where=None, join=None, via=None, **opts):
        # Get intermediate representation for this evaluation query
        if not all(isinstance(j, Join) and j.compatible for j in join):
            raise RuntimeError("All joins for IR must be compatible with this provider.")
        return self._get_ir(
            unit_type=unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            join=join,
            via=tuple() if via is None else via,
            **opts
        )

    def _get_ir(self, unit_type, measures=None, segment_by=None, where=None, join=None, via=None, **opts):
        raise NotImplementedError

    def _is_compatible_with(self, *providers):
        '''
        If this method returns True, this MeasureProvider will take responsibility
        for evaluation and/or interpreting the required fields from the provided
        providers; otherwise, any required joins will be performed in memory in
        pandas.
        '''
        return all(provider.__class__ is self.__class__ for provider in providers)
