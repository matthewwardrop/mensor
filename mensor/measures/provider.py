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
    """
    This is the base class that provides the API contract for all data sources
    in the `mensor` universe. Every `MeasureProvider` instance is a proxy to
    a different data source, allowing identifiers, measures and dimensions to
    be evaluated in different contexts; and the class exists simply to provide
    metadata about the data stored therein.

    Terminology:
        There are three classes of metadata: identifiers, dimensions and
        measures.

        Identifiers - Specifications of statistical unit types; i.e. the
            indivisible unit of an analysis. For example: "user", or "session",
            etc.
        Dimensions - Features associated with a statistical unit that are not
            aggregatable, such as "country" of a "user" or "platform" of a
            "client".
        Measures - Features associated with a statistical unit that are
            aggregatable (extensive), such as age, length, etc.

        While not relevant in the context of MeasureProviders, "metrics" are
        arbitrary functions of measures.

        Note that all measures and identifiers can be used as dimensions, but
        not vice versa.

    Defining Metadata:

        Setting and extracting metadata is done via a series of methods, which
        are similar for each type of metadata.

        Identifiers:
        - .identifiers
        - .add_identifier
        - .unit_types
        - .identifier_for_unit
        - .foreign_keys_for_unit

        Dimensions:
        - .dimensions
        - .add_dimension
        - .dimensions_for_unit

        Measures:
        - .measures
        - .add_measure
        - .measures_for_unit

    `MeasureProvider`s are registered into pools of `MeasureProvider`s called
    `MeasureRegistry`s. Once registered, the registry can evaluate measures
    transparently across all `MeasureProvider`s, handling the joins as necessary.
    """

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
        for foreign_key in self.identifiers:
            if foreign_key != unit_type and self._unit_has_foreign_key(unit_type, foreign_key):
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
        return unit_type.is_primary

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
        return unit_type.is_primary

    # Resolution

    def resolve(self, names, unit_type=None, kind=None):
        """
        This method resolves one or more names of features optionally associated
        with a unit_type and a kind. Note that this method is concerned about
        *functional* resolution, so if `kind='dimension'` both identifiers
        and measures will still be resolved, since they can be used as
        dimensions.

        Parameters:
            names (str, list<str>): A name or list of names to resolve.
            unit_type (str, None): A unit type for which the resolution should
                be done.
            kind (str): One of 'measure', 'dimension' or 'identifier'.

        Returns:
            _Dimension, _Measure, _StatisticalUnitIdentifier: The resolved object.
        """
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

        joins = [j for j in join if j.compatible]
        post_joins = [j for j in join if not j.compatible]

        # If there are post-joins, we will need to add the 'count' measure
        # (assuming it has not already been requested).
        if len(post_joins) > 0 and 'count' not in measures:
            count_measure = self.measures['count'].as_private
            measures[count_measure] = count_measure

        # Evaluate the requested measures from this MeasureProvider
        result = MeasureDataFrame(
            self._evaluate(
                unit_type,
                measures,
                where=where,
                segment_by=segment_by,
                join=joins, **opts
            )
        )

        # Join in precomputed incompatible joins
        # TODO: Clean-up how joined measures are detected
        joined_measures = set()
        if len(post_joins) > 0:
            for join in post_joins:
                joined_measures.update(join.object.columns)
                result = result.merge(
                    join.object,
                    left_on=join.left_on,
                    right_on=join.right_on,
                    how=join.how
                ).drop(join.right_on, axis=1)
        joined_measures.difference(segment_by)

        # All new joined in measures need to be multiplied by the count series of
        # this dataframe, so that they are properly weighted.
        if len(joined_measures) > 0:
            result = result.apply(lambda col: result['count:count'] * col if col.name in joined_measures else col, axis=0)

        # Remove the private 'count:count' measure.
        # TODO: Make this more general just in case other measures are private for some reason
        if 'count' in measures and measures['count'].private:
            result = result.drop('count:count', axis=1)

        # Resegment after deleting private dimensions as necessary
        if len(set(d.name for d in segment_by if d.private).intersection(result.columns)) > 0:
            result = (
                result
                .drop([d.name for d in segment_by if d.private], axis=1)
            )
            segment_by = [x.via_name for x in segment_by if not x.private]
            if len(segment_by):
                result = result.groupby(segment_by).sum().reset_index()
            else:
                result = result.sum()

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
