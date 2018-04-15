from collections import Counter

from .strategy import EvaluationStrategy
from .types import _Dimension, _ResolvedDimension, _ResolvedMeasure, Provision
from ..utils import nested_dict_copy

__all__ = ['MeasureRegistry']


class MeasureRegistry(object):
    """
    A `MeasureRegistry` instance is a wrapper around a pool of `MeasureProvider`
    instances that generates a graph of relationships between all of the
    provided identifiers, measures and dimensions. Relationships between these
    features can then be extracted and used in various tasks, chief among which
    being the evaluation of measures for a statistical unit type segmented by
    various dimensions. The logic for the evaluation is handled by the
    `.stategy.EvaluationStrategy` class. The only purpose of this class is to
    construct and manage the graph.

    To add `MeasureProvider`s to instances of this class, simply call the
    `.registry()` method of this class and pass to it the relevant
    `MeasureProvider` instance.

    Relationships:
        The graph formed by registering `MeasureProvider`s has the following
        relationships.

        unit_type -> measure
        unit_type -> dimension
        unit_type -> foreign_key
        unit_type <- foreign_key [-> reverse_foreign_key]
    """

    class GraphCache(object):

        def __init__(self, providers=None, identifiers=None, foreign_keys=None,
                     reverse_foreign_keys=None, dimensions=None, measures=None):
            self.providers = providers or {}
            self.identifiers = identifiers or {}
            self.foreign_keys = foreign_keys or {}
            self.reverse_foreign_keys = reverse_foreign_keys or {}
            self.dimensions = dimensions or {}
            self.measures = measures or {}

        def copy(self):
            return MeasureRegistry.GraphCache(
                **{
                    key: nested_dict_copy(getattr(self, key))
                    for key in ['providers', 'identifiers', 'foreign_keys', 'reverse_foreign_keys', 'dimensions', 'measures']
                }
            )

        def register(self, provider):
            for identifier in provider.identifiers:
                self.register_identifier(identifier)

                for unit_type in provider.identifiers:
                    self.register_foreign_key(identifier, unit_type)

                for dimension in provider.dimensions_for_unit(identifier):
                    self.register_dimension(identifier, dimension)

                for measure in provider.measures_for_unit(identifier):
                    self.register_measure(identifier, measure)

        def register_identifier(self, unit_type):
            self._append(self.identifiers, [unit_type], unit_type)

        def register_foreign_key(self, unit_type, foreign_key):
            if unit_type.is_unique:
                self._append(self.foreign_keys, [unit_type, foreign_key], foreign_key)
            elif foreign_key.is_unique:
                self._append(self.reverse_foreign_keys, [unit_type, foreign_key], foreign_key)

        def register_dimension(self, unit_type, dimension):
            self._append(self.dimensions, [unit_type, dimension], dimension)

        def register_measure(self, unit_type, measure):
            self._append(self.measures, [unit_type, measure], measure)

        def _extract(self, store, keys):
            for i, key in enumerate(keys):
                if key not in store:
                    return []
                store = store[key]
            assert isinstance(store, list)
            return store

        def _append(self, store, keys, value):
            for i, key in enumerate(keys):
                if key not in store:
                    if i == len(keys) - 1:
                        store[key] = []
                    else:
                        store[key] = {}
                store = store[key]
            assert isinstance(store, list)
            if len(store) > 0:
                assert value.shared and all([d.shared for d in store]), "Attempted to add duplicate non-shared dimension '{}'.".format(value)
            store.append(value)

    def __init__(self):
        self._providers = {}
        self._cache = MeasureRegistry.GraphCache()

    def _cache_refresh(self):
        self._cache = MeasureRegistry.GraphCache()
        for provider in self._providers.values():
            self._cache.register(provider)

    def register(self, provider):
        """
        This method atomically registers a provider, and extends the graph to
        include it. Once registered, its features will be immediately available
        to all evaluations.
        """
        # TODO: Enforce that measures and dimensions share same namespace, and never conflict with stat types
        # TODO: Ensure no contradictory key types (e.g. Two identifiers primary on one table and not both primary on a secondary table)
        if provider.name in self._providers:
            raise ValueError("A MeasureProvider named '{}' has already been registered.".format(provider.name))
        self._providers[provider.name] = provider

        cache = self._cache.copy()
        cache.register(provider)
        # Committing cache
        self._cache = cache

    def unregister(self, provider_name):
        provider = self._providers.pop(provider_name)
        self._cache_refresh()
        return provider

    @property
    def unit_types(self):
        return set(self._cache.identifiers.keys())

    def dimensions_for_unit(self, unit_type):
        dims = set()
        for avail_unit_type in self._cache.dimensions:
            if avail_unit_type.matches(unit_type):
                dims.update([v[0] for v in self._cache.dimensions[avail_unit_type].values()])
        return dims

    def measures_for_unit(self, unit_type):
        ms = set()
        for avail_unit_type in self._cache.measures:
            if avail_unit_type.matches(unit_type):
                ms.update([v[0] for v in self._cache.measures[avail_unit_type].values()])
        return ms

    def foreign_keys_for_unit(self, unit_type):
        fks = set()
        for avail_unit_type in self._cache.foreign_keys:
            if avail_unit_type.matches(unit_type):
                fks.update([v[0] for v in self._cache.foreign_keys[avail_unit_type].values()])
        return fks

    def reverse_foreign_keys_for_unit(self, unit_type):
        fks = set()
        for avail_unit_type in self._cache.reverse_foreign_keys:
            if avail_unit_type.matches(unit_type):
                fks.update([v[0] for v in self._cache.reverse_foreign_keys[avail_unit_type].values()])
        return fks

    def _resolve_identifier(self, unit_type):
        return self._cache.identifiers[unit_type][0]  # TODO: Use below mechanism?

    def __resolve_dimension(self, unit_type, dimension, kind='dimension', dimension_index=None):
        """
        This is an internal method that traverses the `GraphCache` in order to
        resolve a particular measure/dimension/identifier for a specified
        unit_type. Note that if `dimension` is a string representation graph
        traversal (e.g. "transaction/person:seller/age") then the full graph
        traversal is not verified, only the last step from e.g. "person:seller"
        to "age", and the remainder of the path is appended to the 'via'
        attribute.

        Parameters:
            unit_type (str, _StatisticalUnitIdentifier): The unit type for which
                to resolve a nominated dimension.
            dimension (str): The dimension/measure to resolved. Note that
                dimensions cannot be transitive at this level in the
                mensor platform.+
            kind (str): The kind of feature to resolve (one of: 'dimension',
                'measure', 'foreign_key' or 'reverse_foreign_key')
            dimension_index (dict): Override for standard kind-detected cache
                index.

        Returns:
            _ResolvedDimension: The resolved feature, with information about
                provider and required joins.
        """
        # TODO: Actually apply checks.
        unit_type = self._resolve_identifier(unit_type)
        dimension_index = dimension_index or getattr(self._cache, kind + 's', {})
        via = unit_type.name
        dimensions = None

        private = external = False

        if isinstance(dimension, (_ResolvedDimension, _Dimension)):
            private = dimension.private
            external = dimension.external

        if isinstance(dimension, _ResolvedDimension):
            dimension = dimension.name

        if isinstance(dimension, str):
            s = dimension.split('/')
            # assert len(s) == 1, '/'.join([str(unit_type), str(dimension)])
            if len(s) > 1 and s[0] == unit_type.name:  # Remove reference to current unit_type
                raise RuntimeError("Self-referencing foreign_key.")
            via_suffix = '/'.join(s[:-1])
            dimension = s[-1]
            if via_suffix:
                unit_type = self._resolve_identifier(s[-2])
                via += '/' + via_suffix

            # Look for dimension starting from most specific unit key with specificity
            # less than or equal to provided unit_type. Unit_type name length
            # is a good proxy for this.
            for avail_unit_type in sorted(dimension_index, key=lambda x: len(x.name), reverse=True):
                if avail_unit_type.matches(unit_type) and dimension in dimension_index[avail_unit_type]:
                    dimensions = dimension_index[avail_unit_type][dimension]
                    break
            if dimensions is None:
                raise ValueError("No such {} `{}` for unit type `{}`.".format(kind, dimension, unit_type))

        elif isinstance(dimension, _Dimension):
            dimensions = [dimension]

        if not isinstance(dimensions, list) and all(isinstance(d, _Dimension) for d in dimensions):
            raise ValueError("Invalid type for {}: `{}`".format(kind, dimension.__class__))

        if kind in ('dimension', 'foreign_key', 'reverse_foreign_key'):
            r = _ResolvedDimension(dimension.name if isinstance(dimension, _Dimension) else dimension, via, providers=[d.provider for d in dimensions])
        elif kind == 'measure':
            r = _ResolvedMeasure(dimension.name if isinstance(dimension, _Dimension) else dimension, via, providers=[d.provider for d in dimensions])
        else:
            raise RuntimeError("SHOULD NOT BE POSSIBLE. Invalid kind '{}'.".format(kind))

        if external:
            r = r.as_external
        if private:
            r = r.as_private
        return r

    def _resolve_foreign_key(self, unit_type, foreign_type):
        return self.__resolve_dimension(unit_type, foreign_type, kind='foreign_key')

    def _resolve_reverse_foreign_key(self, unit_type, foreign_type):
        return self.__resolve_dimension(unit_type, foreign_type, kind='reverse_foreign_key')

    def _resolve_measure(self, unit_type, measure):
        return self.__resolve_dimension(unit_type, measure, kind='measure')

    def _resolve_dimension(self, unit_type, dimension):
        try:
            return self.__resolve_dimension(unit_type, dimension, kind='dimension')
        except ValueError:
            pass
        try:
            return self._resolve_measure(unit_type, dimension)
        except ValueError:
            pass
        try:
            return self._resolve_foreign_key(unit_type, dimension)
        except ValueError:
            pass
        raise ValueError("No such dimension {} for unit type '{}'".format(dimension, unit_type))

    def _find_optimal_provision(self, unit_type, measures, dimensions, require_primary=True):
        """
        This method takes a set of meaures and dimensions for a given unit_type,
        and generates a somewhat optimised sequence of `Provision` instances,
        which indicate the MeasureProvider instance from which measures and
        dimensions should be extracted. This is primarily useful for the
        generation of an `EvaluationStrategy`.

        Parameters:
            unit_type (str, _StatisticalUnitIdentifier): The statistical unit
                type for which indicated measures and dimensions should be
                extracted.
            measures (list<str,_Measure>): A set of measures to be extracted.
            dimensions (list<str, _Dimension): A set of dimensions to be
                extracted.
            require_primary (bool): Whether to require the first `Provision` to
                be from a `MeasureProvider` with `unit_type` as a primary
                identifier.

        Returns:
            list<Provision>: A list of `Provision` instances which optimally
                supply the requested measures and dimensions.
        """
        # [Provision(provider, measures, dimensions), ...]
        unit_type = self._resolve_identifier(unit_type)
        measures = {measure: self._resolve_measure(unit_type, measure) for measure in measures}
        dimensions = {dimension: self._resolve_dimension(unit_type, dimension) for dimension in dimensions}

        def get_next_provider(unit_type, measures, dimensions, primary=False):
            provider_count = Counter()
            provider_count.update(provider for measure in measures.values() for provider in measure.providers.values())
            provider_count.update(provider for dimension in dimensions.values() for provider in dimension.providers.values())

            provider = None
            if primary:
                # Try to extract primary provider from used providers, or locate
                # one in the unit_type registry.
                for p, _ in provider_count.most_common() + [(ut.provider, 0) for ut in self._cache.identifiers[unit_type.name] if ut.is_primary]:
                    if p.identifier_for_unit(unit_type).is_primary:
                        provider = p
                        break
                if provider is None:
                    raise ValueError("No primary key for {}.".format(unit_type.name))
            else:
                provider = provider_count.most_common(1)[0][0]

            return provider

        provisions = []
        dimension_count = len(measures) + len(dimensions)
        while dimension_count > 0:
            p = get_next_provider(unit_type, measures, dimensions, primary=True if require_primary and len(provisions) == 0 else False)
            provisions.append(Provision(
                p,
                measures=[measures.pop(measure).from_provider(p) for measure in measures.copy() if measure in p.measures],
                dimensions=[dimensions.pop(dimension).from_provider(p) for dimension in dimensions.copy() if dimension in p.dimensions or dimension in p.identifiers or dimension in p.measures]
            ))
            if len(measures) + len(dimensions) == dimension_count:
                raise RuntimeError("Could not provide provisions for: measures={}, dimensions={}. This is a bug.".format(list(measures), list(dimensions)))
            dimension_count = len(measures) + len(dimensions)

        return provisions

    def evaluate(self, unit_type, measures=None, segment_by=None, where=None, dry_run=False, **opts):
        strategy = EvaluationStrategy.from_spec(
            self, unit_type, measures, where=where, segment_by=segment_by, **opts
        )
        if dry_run:
            return strategy
        return strategy.execute()

    def show(self, *unit_types):
        unit_types = [self._resolve_identifier(ut) for ut in unit_types] if len(unit_types) > 0 else sorted(self.unit_types)
        for unit_type in unit_types:
            print("{}:".format(unit_type.name))
            if self.foreign_keys_for_unit(unit_type):
                print("\tForeign Keys:")
                for foreign_key in sorted(self.foreign_keys_for_unit(unit_type)):
                    if foreign_key != unit_type:
                        print("\t\t{}::{}: {}".format(foreign_key.provider.name, foreign_key.name, foreign_key.desc or "No description."))
            if self.reverse_foreign_keys_for_unit(unit_type):
                print("\tReverse Foreign Keys:")
                for foreign_key in sorted(self.reverse_foreign_keys_for_unit(unit_type)):
                    if foreign_key != unit_type:
                        print("\t\t{}::{}: {}".format(foreign_key.provider.name, foreign_key.name, foreign_key.desc or "No description."))
            if self.dimensions_for_unit(unit_type):
                print("\tDimensions:")
                for measure in sorted(self.dimensions_for_unit(unit_type)):
                    print("\t\t{}::{}: {}".format(measure.provider.name, measure.name, measure.desc or "No description."))
            if self.measures_for_unit(unit_type):
                print("\tMeasures:")
                for measure in sorted(self.measures_for_unit(unit_type)):
                    print("\t\t{}::{}: {}".format(measure.provider.name, measure.name, measure.desc or "No description."))
