# TODO: metrics.for_segment(Segment/Target instance).metrics.bookings.evaluate()
# TODO: metrics.for_segment(Segment/Target instance).measures.bookings.evaluate()
# TODO: metrics.for_segment(Segment/Target instance).measures.evaluate('bookings/trains')
#
# TODO: metrics.measures.booking_value.evaluate(segment_by='test')
import itertools
from collections import OrderedDict

import pandas as pd
import six

from .context import CONSTRAINTS, And, Constraint
from ..utils import AttrDict
from .types import _Dimension, _Measure, _StatisticalUnitIdentifier, Join, MeasureDataFrame, MeasureSeries, AGG_METHODS, DISTRIBUTIONS, DISTRIBUTION_FIELDS

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
        - .add_partition
        - .partitions_or_unit

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

    # Dimension specifications

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

    def dimensions_for_unit(self, unit_type=None, include_partitions=True):
        if unit_type is None:
            return self.dimensions
        unit_type = self.identifier_for_unit(unit_type)

        dimensions = {}
        for dimension in self.dimensions:
            if (
                self._unit_has_dimension(unit_type, dimension)
                and (include_partitions or not dimension.partition)
            ):
                dimensions[dimension] = dimension
        return dimensions

    def _unit_has_dimension(self, unit_type, dimension):
        return unit_type.is_unique

    # Semantic distinction between standard dimension and partition
    # Since difference is semantically different but technically almost
    # identical, we expose them as two different things.
    # Note that partitions also appears as dimensions, since they are
    # functionally equivalent in most cases.
    # (partitions behave differently in joins TODO: document this difference)
    def add_partition(self, name=None, desc=None, expr=None):
        dimension = _Dimension(name, desc=desc, expr=expr, shared=True, partition=True, provider=self)
        self._dimensions[dimension] = dimension
        return self

    def partitions_for_unit(self, unit_type=None):
        return {
            dimension: dimension for dimension in self.dimensions_for_unit(unit_type) if dimension.partition
        }

    # Measure specifications

    @property
    def measures(self):
        return self._measures

    @measures.setter
    def measures(self, measures):
        self._measures = self._get_dimensions_from_specs(_Measure, measures)

    def add_measure(self, name=None, expr=None, desc=None, shared=False, unit_agg='sum', distribution='normal'):
        measure = _Measure(name, expr=expr, desc=desc, shared=shared, unit_agg=unit_agg, distribution=distribution, provider=self)
        assert measure.unit_agg in self._agg_methods, "This provider does not support aggregating at the unit level using '{}'.".format(measure.measure_agg)
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
        return unit_type.is_unique

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
        def wrapped(self, unit_type, measures=None, segment_by=None, where=None, joins=None, **opts):
            unit_type = self.identifier_for_unit(unit_type)
            measures = {} if measures is None else self.resolve(measures, kind='measure')
            segment_by = {} if segment_by is None else self.resolve(segment_by, kind='dimension')
            where = Constraint.from_spec(where)
            joins = joins or []
            return f(self, unit_type, measures=measures, segment_by=segment_by, where=where, joins=joins, **opts)
        return wrapped

    @_prepare_evaluation_args
    def evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                 joins=None, stats=True, covariates=False, **opts):
        """
        This method evaluates the requested `measures` in this MeasureProvider
        segmented by the dimensions in `segment_by` after joining in the
        joins in `joins` and subject to the constraints in `where`; treating
        `unit_type` objects as indivisible.

        Parameters:
            unit_type (str, _StatisticalUnitIdentifier): The unit to treat as
                indivisible in this analysis.
            measures (list<str, _Measure>): The measures to be calculated.
            segment_by (list<str, _Feature>): The dimensions by which to segment
                the measure computations.
            where (dict, list, tuple, Constraint, EvaluationContext): The
                constraints within which measures should be computed.
            stats (bool): Whether to keep track of the distribution of the
                measures, rather than just their sum.
            covariates (bool, list<tuple>): Whether to compute all covariates
                (if bool) or else a list of tuples of measures within which
                all pairs of covariates should be computed.
            opts (dict): Additional arguments to be passed onto `._evalaute`
                implementations.

        Returns:
            MeasureDataFrame: A dataframe of the results of the computation.
        """

        if not unit_type.is_unique and len(segment_by) > 0:
            raise RuntimeError("Cannot segment by any features when rebasing units.")

        if not unit_type.is_unique:
            raise NotImplementedError("Unit rebasing for reverse-foreign-key joins is not yet implemented.")  # TODO: Implement!

        post_joins = [j for j in joins if not j.compatible]
        joins = [j for j in joins if j.compatible]

        # If there are post-joins, we will need to add the 'count' measure
        # (assuming it has not already been requested).
        if len(post_joins) > 0 and 'count' not in measures:
            count_measure = self.measures['count'].as_private
            measures[count_measure] = count_measure

        # If there are post-joins and where constraints, some of the constraints
        # may need to be applied after joins. As such, we split the where
        # constraints into where_prejoin and where_postjoin.
        def resolvable_pre_join(constraint):
            for dimension in constraint.dimensions:
                if dimension not in itertools.chain(self.identifiers, self.dimensions, self.measures):
                    return False
            return True

        where_prejoin = []
        where_postjoin = []
        if len(post_joins) > 0 and where:
            if where.kind is CONSTRAINTS.AND:
                for op in where.operands:
                    if resolvable_pre_join(op):
                        where_prejoin.append(op)
                    else:
                        where_postjoin.append(op)
            else:
                if resolvable_pre_join(where):
                    where_prejoin.append(where)
                else:
                    where_postjoin.append(where)
        else:
            where_prejoin = where
        where_prejoin = And.from_operands(where_prejoin)
        where_postjoin = And.from_operands(where_postjoin)

        # Evaluate the requested measures from this MeasureProvider
        result = self._evaluate(
            unit_type,
            measures,
            where=where_prejoin,
            segment_by=segment_by,
            joins=joins,
            stats=stats,
            covariates=covariates,
            **opts
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
                )
        joined_measures.difference(segment_by)

        # Apply post-join constraints
        if where_postjoin:
            raise NotImplementedError("Post-join generic where clauses not implemented yet.")

        # All new joined in measures need to be multiplied by the count series of
        # this dataframe, so that they are properly weighted.
        if len(joined_measures) > 0:
            result = result.apply(lambda col: result['count|count'] * col if col.name in joined_measures else col, axis=0)

        # Remove the private 'count:count' measure.
        # TODO: Make this more general just in case other measures are private for some reason
        if 'count' in measures and measures['count'].private:
            result = result.drop('count|count', axis=1)

        # Resegment after deleting private dimensions as necessary
        if isinstance(result, pd.DataFrame) and len(set(d.name for d in segment_by if d.private).intersection(result.columns)) > 0:
            result = (
                result
                .drop([d.name for d in segment_by if d.private], axis=1)
            )
            segment_by = [x.via_name for x in segment_by if not x.private]
            if len(segment_by):
                result = result.groupby(segment_by).sum().reset_index()
            else:
                result = result.sum()

        if isinstance(result, pd.Series):
            return MeasureSeries(result)
        return MeasureDataFrame(result)

    def _evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                  joins=None, stats=True, covariates=False, **opts):
        raise NotImplementedError("Generic implementation not implemented.")

    @_prepare_evaluation_args
    def get_ir(self, unit_type, measures=None, segment_by=None, where=None,
               joins=None, stats=True, covariates=False, **opts):
        # Get intermediate representation for this evaluation query
        if not all(isinstance(j, Join) and j.compatible for j in joins):
            raise RuntimeError("All joins for IR must be compatible with this provider.")
        return self._get_ir(
            unit_type=unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            joins=joins,
            stats=stats,
            covariates=covariates,
            **opts
        )

    def _get_ir(self, unit_type, measures=None, segment_by=None, where=None,
                joins=None, stats=True, covariates=False, **opts):
        raise NotImplementedError

    # Compatibility
    def _is_compatible_with(self, provider):
        '''
        If this method returns True, this MeasureProvider can take responsibility
        for evaluation and/or interpreting the required fields from the provided
        provider; otherwise, any required joins will be performed in memory in
        pandas.
        '''
        return False

    # Aggregation methods for measures
    @property
    def _agg_methods(self):
        """
        A dictionary of MeasureProvider implementation specific representations
        of actions to perform for each of the types of aggregation specified
        in `.types.AGG_METHODS`.
        """
        return {}

    def _agg_method(self, agg_type):
        """
        Parameters:
            agg_type (AGG_METHOD): The agg method type for which to extract
                its representation for this instance of MeasureProvider.
        """
        if not isinstance(agg_type, AGG_METHODS):
            raise ValueError("Agg type `{}` is not a valid instance of `mensor.measures.types.AGG_METHODS`.".format(agg_type))
        if agg_type not in self._agg_methods:
            raise NotImplementedError("Agg type `{}` is not implemented by `{}`.".format(agg_type, self.__class__))
        return self._agg_methods[agg_type]

    # Measure distribution methods
    def _get_distribution_fields(self, dist_type):
        """
        This is a convenience method for subclasses to use to get the
        target fields associated with a particular distribution.

        Parameters:
            dist_type (DISTRIBUTIONS): The distribution type for which to
                extract target fields and aggregation methods.

        Returns:
            OrderedDict: A mapping of field suffixes to agg methods to collect
                in order to reproduce the distribution from which a measure was
                sampled.
        """
        return OrderedDict([
            (
                ("|{field_name}" if dist_type == DISTRIBUTIONS.NONE else "|{dist_name}|{field_name}").format(field_name=field_name, dist_name=dist_type.name.lower()),
                self._agg_method(agg_type)
            )
            for field_name, agg_type in DISTRIBUTION_FIELDS[dist_type].items()
        ])

    # Constraint interpretation
    @property
    def _constraint_maps(self):
        """
        A dictionary of mappings from CONSTRAINTS types to an internal
        representation useful to apply the constraint.
        """
        return {}

    def _constraint_map(self, kind):
        """
        Parameters:
            kind (CONSTRAINTS): The type of constraint for which to extract the
                internal represtation of the mapper.
        """

        if kind not in self._constraint_maps:
            raise NotImplementedError("{} cannot apply constraints of kind: `{}`".format(self.__class__.__name__, kind))
        return self._constraint_maps[kind]
