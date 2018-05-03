from collections import Counter

from ..utils import nested_dict_copy
from .strategy import EvaluationStrategy
from .types import Provision, _ProvidedFeature, _ResolvedFeature

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
            # TODO: Enforce that measures and dimensions share same namespace, and never conflict with stat types
            # TODO: Ensure no contradictory key types (e.g. Two identifiers primary on one table and not both primary on a secondary table)

            # Require that each provider have at least one primary key
            # and a measure "count".
            if len(list(identifier for identifier in provider.identifiers if identifier.is_unique)) == 0:
                raise RuntimeError("MeasureProvider '{}' does not have at least one unique identifier.".format(provider))
            if 'count' not in provider.measures:
                raise RuntimeError("MeasureProvider '{}' does not provide a 'count' measure.".format(provider))

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
            if len(store) > 0 and not (value.shared and all([d.shared for d in store])):
                raise RuntimeError("Attempted to add duplicate non-shared feature '{}'.".format(value))
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

    def dimensions_for_unit(self, unit_type, include_partitions=True):
        dims = set()
        for avail_unit_type in self._cache.dimensions:
            if avail_unit_type.matches(unit_type):
                dims.update([v[0] for v in self._cache.dimensions[avail_unit_type].values() if include_partitions or not v[0].partition])
        return dims

    def partitions_for_unit(self, unit_type):
        dims = set()
        for avail_unit_type in self._cache.dimensions:
            if avail_unit_type.matches(unit_type):
                dims.update([v[0] for v in self._cache.dimensions[avail_unit_type].values() if v[0].partition])
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

    def __resolve_feature(self, unit_type, feature, kind='dimension', feature_index=None):
        """
        This is an internal method that traverses the `GraphCache` in order to
        resolve a particular feature (measure/dimension/identifier) for a specified
        unit_type. Note that if `dimension` is a string representation graph
        traversal (e.g. "transaction/person:seller/age") then the full graph
        traversal is not verified, only the last step from e.g. "person:seller"
        to "age", and the remainder of the path is appended to the 'via'
        attribute.

        Parameters:
            unit_type (str, _StatisticalUnitIdentifier): The unit type for which
                to resolve a nominated feature.
            feature (str, _ProvidedFeature): The feature to resolved. Note that
                features must be directly related to the unit_type.
            kind (str): The kind of feature to resolve (one of: 'dimension',
                'measure', 'foreign_key' or 'reverse_foreign_key')
            feature_index (dict): Override for standard kind-detected cache
                index.

        Returns:
            _ResolvedFeature: The resolved feature, with information about
                provider and required joins.
        """
        # TODO: Actually apply checks.
        assert kind in ('foreign_key', 'reverse_foreign_key', 'dimension', 'measure')

        unit_type = self._resolve_identifier(unit_type)
        feature_index = feature_index or getattr(self._cache, kind + 's', {})
        via = ''
        features = None

        attrs = {'kind': kind}

        if isinstance(feature, (_ResolvedFeature, _ProvidedFeature)):
            attrs = feature.attrs
            del attrs['name']
            feature = feature.via_name  # Re-resolve any resolved feature, since resolved features are currently not deeply resolved

        if isinstance(feature, str):
            s = feature.split('/')
            # assert len(s) == 1, '/'.join([str(unit_type), str(feature)])
            if len(s) > 1 and s[0] == unit_type.name:  # Remove reference to current unit_type
                s = s[1:]
            via_suffix = '/'.join(s[:-1])
            feature = s[-1]
            eff_unit_type = unit_type
            if via_suffix:
                eff_unit_type = self._resolve_identifier(s[-2])
                via += ('/' + via_suffix) if via else via_suffix
            attrs['via'] = via

            # Look for feature starting from most specific unit key with specificity
            # less than or equal to provided unit_type. Unit_type name length
            # is a good proxy for this.
            for avail_unit_type in sorted(feature_index, key=lambda x: len(x.name), reverse=True):
                if kind in ('foreign_key', 'reverse_foreign_key'):  # Handle self-lookup of hierarchical types. TODO: Do this more intelligently
                    if avail_unit_type.matches(eff_unit_type):
                        for feature_candidate in sorted(feature_index[avail_unit_type], key=lambda x: len(x.name), reverse=True):
                            if feature_candidate.matches(feature):
                                features = feature_index[avail_unit_type][feature_candidate]
                                if feature != feature_candidate.name:
                                    attrs['alias'] = feature
                                    feature = feature_candidate.name
                                break
                else:  # Handle all other cases.
                    if avail_unit_type.matches(eff_unit_type) and feature in feature_index[avail_unit_type]:
                        features = feature_index[avail_unit_type][feature]
                        break
            if features is None:
                raise ValueError("No such {} `{}` for unit type `{}`.".format(kind, feature, eff_unit_type))

        else:
            raise ValueError("Invalid type for {}: `{}`".format(kind, feature.__class__))

        r = _ResolvedFeature(feature, providers=[d.provider for d in features], **attrs)
        return r

    def _resolve_foreign_key(self, unit_type, foreign_type):
        return self.__resolve_feature(unit_type, foreign_type, kind='foreign_key')

    def _resolve_reverse_foreign_key(self, unit_type, foreign_type):
        return self.__resolve_feature(unit_type, foreign_type, kind='reverse_foreign_key')

    def _resolve_measure(self, unit_type, measure):
        return self.__resolve_feature(unit_type, measure, kind='measure')

    def _resolve_dimension(self, unit_type, dimension):
        try:
            return self.__resolve_feature(unit_type, dimension, kind='dimension')
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

    def _find_primary_key_for_unit_type(self, unit_type):
        for identifier in sorted(self._cache.identifiers, key=lambda x: len(x.name), reverse=True):
            if identifier.matches(unit_type) and any(i.is_primary for i in self._cache.identifiers[identifier]):
                return identifier
        raise RuntimeError("No primary key exists for unit_type `{}`.".format(unit_type))

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
        measures = {self._resolve_measure(unit_type, measure): self._resolve_measure(unit_type, measure) for measure in measures}
        dimensions = {self._resolve_dimension(unit_type, dimension): self._resolve_dimension(unit_type, dimension) for dimension in dimensions}

        def get_next_provider(unit_type, measures, dimensions, primary=False):
            provider_count = Counter()
            provider_count.update(provider for measure in measures.values() for provider in measure.providers.values())
            provider_count.update(provider for dimension in dimensions.values() for provider in dimension.providers.values())

            provider = None
            if primary:
                primary_unit_type = self._find_primary_key_for_unit_type(unit_type)
                # Try to extract primary provider from used providers, or locate
                # one in the unit_type registry.

                for p, _ in provider_count.most_common() + [(ut.provider, 0) for ut in self._cache.identifiers[primary_unit_type.name] if ut.is_primary]:
                    if p.identifiers.get(primary_unit_type) and p.identifiers.get(primary_unit_type).is_primary:
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
            join_prefix = unit_type.name

            provisions.append(Provision(
                p,
                join_prefix,
                measures=[measures.pop(measure).from_provider(p) for measure in measures.copy() if measure in p.measures],
                dimensions=[dimensions.pop(dimension).from_provider(p) for dimension in dimensions.copy() if dimension in p.dimensions or dimension in p.identifiers or dimension in p.measures]
            ))
            if len(measures) + len(dimensions) == dimension_count and not (require_primary is True and len(provisions) > 0):
                raise RuntimeError("Could not provide provisions for: measures={}, dimensions={}. This is a bug.".format(list(measures), list(dimensions)))
            dimension_count = len(measures) + len(dimensions)

        return provisions

    def evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                 stats=True, covariates=False, dry_run=False, **opts):
        strategy = EvaluationStrategy.from_spec(
            self, unit_type, measures, where=where, segment_by=segment_by
        )
        if dry_run:
            return strategy
        return strategy.execute(stats=stats, covariates=covariates, **opts)

    def show(self, *unit_types):
        unit_types = [self._resolve_identifier(ut) for ut in unit_types] if len(unit_types) > 0 else sorted(self.unit_types)
        for unit_type in unit_types:
            print("{}:".format(unit_type.name))

            features = [
                ('Foreign Keys', self.foreign_keys_for_unit(unit_type)),
                ('Reverse Foreign Keys', self.reverse_foreign_keys_for_unit(unit_type)),
                ('Dimensions', self.dimensions_for_unit(unit_type, include_partitions=False)),
                ('Partitions', self.partitions_for_unit(unit_type)),
                ('Measures', self.measures_for_unit(unit_type))
            ]

            for feature_name, feature_set in features:
                if not len(feature_set):
                    continue
                print("\t{}".format(feature_name))
                for feature in sorted(feature_set):
                    if feature != unit_type:
                        print("\t\t{}::{}: {}".format(feature.provider.name, feature.name, feature.desc or "No description."))
