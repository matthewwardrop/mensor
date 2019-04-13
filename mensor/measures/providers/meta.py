"""Implementation of MetaMeasureProvider."""

import os
from collections import Counter, namedtuple

from mensor.utils import nested_dict_copy, SequenceMap

from .base import MeasureProvider
from ..evaluation.strategy import EvaluationStrategy
from ..registries import global_stats_registry, StatsRegistry
from ..structures.features import _ProvidedFeature, _ResolvedFeature

__all__ = ['MetaMeasureProvider']


Provision = namedtuple('Provision', ['provider', 'join_prefix', 'measures', 'dimensions'])


class MetaMeasureProvider(MeasureProvider):
    """
    A `MeasureProvider` subclass that acts as a host for other `MeasureProvider`
    instances, allowing evaluations of measures that span multiple providers.

    Instances of this class generate a graph of relationships between all of the
    identifiers, measures and dimensions provided by all hosted providers.
    Relationships between these features can then be extracted and used in
    various tasks, chief among which being the evaluation of measures for a
    statistical unit type segmented by various dimensions. The logic for the
    evaluation is handled by the `mensor.measures.evaluation.EvaluationStrategy`
    class.

    The graph formed by registering `MeasureProvider` instances has the
    following relationships:
        - unit_type -> foreign_key
        - unit_type <- foreign_key [-> reverse_foreign_key]
        - unit_type -> dimension
        - unit_type -> measure
    """

    class GraphCache:
        """
        The internal representation of the relationships between features
        across multiple MeasureProviders.
        """

        def __init__(self, providers=None, identifiers=None, foreign_keys=None,
                     reverse_foreign_keys=None, dimensions=None, measures=None):
            self.providers = providers or {}
            self.identifiers = identifiers or {}
            self.foreign_keys = foreign_keys or {}
            self.reverse_foreign_keys = reverse_foreign_keys or {}
            self.dimensions = dimensions or {}
            self.measures = measures or {}

        def copy(self):
            return MetaMeasureProvider.GraphCache(
                **{
                    key: nested_dict_copy(getattr(self, key))
                    for key in [
                        'providers', 'identifiers', 'foreign_keys',
                        'reverse_foreign_keys', 'dimensions', 'measures'
                    ]
                }
            )

        def register(self, provider):
            # TODO: Enforce that measures and dimensions share same namespace,
            # and never conflict with stat types
            # TODO: Ensure no contradictory key types (e.g. Two identifiers
            # primary on one table and not both primary on a secondary table)

            # Require that each provider have at least one primary key and a
            # measure "count".
            # TODO: Uncomment these checks and retain compatibility with nested
            # MetaMeasureProvider instances.
            # if len(list(identifier for identifier in provider.identifiers if identifier.is_unique)) == 0:
            #     raise RuntimeError("MeasureProvider '{}' does not have at least one unique identifier.".format(provider))
            # if 'count' not in provider.measures:
            #     raise RuntimeError("MeasureProvider '{}' does not provide a 'count' measure.".format(provider))

            for identifier in provider.identifiers:
                self.register_identifier(identifier)

                for unit_type in provider.identifiers:
                    self.register_foreign_key(identifier, unit_type)

                for dimension in provider.dimensions_for_unit(identifier):
                    self.register_dimension(identifier, dimension)

                for measure in provider.measures_for_unit(identifier):
                    self.register_measure(identifier, measure)

        def _handled_resolved_features(f):
            def wrapped(self, unit_type, *args):
                assert len(args) in (0, 1)

                if args:
                    if isinstance(unit_type, _ResolvedFeature):
                        unit_type = unit_type.from_provider(list(unit_type._providers.values())[0])

                    if isinstance(args[0], _ResolvedFeature):
                        for provider in args[0]._providers:
                            f(self, unit_type, args[0].from_provider(provider))
                    else:
                        f(self, unit_type, args[0])

                else:
                    if isinstance(unit_type, _ResolvedFeature):
                        for provider in unit_type._providers:
                            f(self, unit_type.from_provider(provider))
                    else:
                        f(self, unit_type)
            return wrapped

        @_handled_resolved_features
        def register_identifier(self, unit_type):
            if isinstance(unit_type, _ResolvedFeature):
                for provider in unit_type._providers:
                    provided = unit_type.from_provider(provider)
                    self._append(self.identifiers, [provided], provided)
            else:
                self._append(self.identifiers, [unit_type], unit_type)

        @_handled_resolved_features
        def register_foreign_key(self, unit_type, foreign_key):
            if unit_type.is_unique:
                self._append(self.foreign_keys, [unit_type, foreign_key], foreign_key)
            elif foreign_key.is_unique:
                self._append(self.reverse_foreign_keys, [unit_type, foreign_key], foreign_key)

        @_handled_resolved_features
        def register_dimension(self, unit_type, dimension):
            self._append(self.dimensions, [unit_type, dimension], dimension)

        @_handled_resolved_features
        def register_measure(self, unit_type, measure):
            self._append(self.measures, [unit_type, measure], measure)

        @staticmethod
        def _extract(store, keys):
            for key in keys:
                if key not in store:
                    return []
                store = store[key]
            assert isinstance(store, list)
            return store

        @staticmethod
        def _append(store, keys, value):
            for i, key in enumerate(keys):
                if key not in store:
                    if i == len(keys) - 1:
                        store[key] = []
                    else:
                        store[key] = {}
                store = store[key]
            assert isinstance(store, list)
            if store and not (value.shared and all([d.shared for d in store])):
                raise RuntimeError(
                    "Attempted to add duplicate non-shared feature '{}'.".format(value)
                )
            store.append(value)

    # Initialisation methods

    def __init__(self, name=None):
        MeasureProvider.__init__(self, name)
        self._providers = SequenceMap()
        self._stats_registry = StatsRegistry(fallback=global_stats_registry)
        self._cache = MetaMeasureProvider.GraphCache()

    # MeasureProvider registration

    @property
    def providers(self):
        """A SequenceMap of all of the providers hosted by this registry."""
        return self._providers

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

        return self

    def register_from_yaml(self, path_or_yaml):
        if '\n' in path_or_yaml or not os.path.isdir(os.path.expanduser(path_or_yaml)):
            return self.register(MeasureProvider.from_yaml(path_or_yaml))
        else:
            for dirpath, dirnames, filenames in os.walk(os.path.expanduser(path_or_yaml)):
                for filename in filenames:
                    if filename.endswith('.yml'):
                        try:
                            provider = MeasureProvider.from_yaml(os.path.join(dirpath, filename))
                            self.register(provider)
                        except AssertionError:
                            pass

    def unregister(self, provider):
        """
        Remove a nominated provider from this registry.

        Args:
            provider (MeasureProvider, str): The provider to be removed.

        Returns:
            MeasureProvider: The removed provider.
        """
        provider = self._providers.pop(provider)
        self._cache_refresh()
        return provider

    def _cache_refresh(self):
        self._cache = MetaMeasureProvider.GraphCache()
        for provider in self._providers.values():
            self._cache.register(provider)

    # Transform registration
    def register_transform(self, transform, name=None, backend=None):
        return self._stats_registry.transforms.register(transform=transform, name=name, backend=backend)

    def register_agg(self, agg, name=None, backend=None):
        return self._stats_registry.aggregations.register(agg=agg, name=name, backend=backend)

    @property
    def identifiers(self):
        return SequenceMap(
            self.identifier_for_unit(ut) for ut in self._cache.identifiers.keys()
        )

    # MeasureEvaluator methods
    def identifier_for_unit(self, unit_type):
        return _ResolvedFeature(
            name=unit_type if isinstance(unit_type, str) else unit_type.name,
            providers=[d.provider for d in self._cache.identifiers[unit_type]],
            kind='identifier'
        )

    def _features_lookup(self, unit_type, kind, attr_filter=None):
        assert kind in ('foreign_key', 'reverse_foreign_key', 'dimension', 'measure')

        unit_type = self.identifier_for_unit(unit_type)
        feature_source = getattr(self._cache, kind + 's')

        features = SequenceMap()
        for avail_unit_type in feature_source:
            if avail_unit_type.matches(unit_type):
                for feature, instances in feature_source[avail_unit_type].items():
                    if feature not in features and (not attr_filter or attr_filter(feature)):
                        mask = None
                        if kind in ('foreign_key', 'reverse_foreign_key') and avail_unit_type == feature.name:
                            mask = unit_type.name
                        features.append(
                            _ResolvedFeature(feature.name, providers=[d.provider for d in instances], unit_type=unit_type, mask=mask, kind=kind)
                        )
        return features

    def foreign_keys_for_unit(self, unit_type):
        return self._features_lookup(unit_type, 'foreign_key')

    def reverse_foreign_keys_for_unit(self, unit_type):
        return self._features_lookup(unit_type, 'reverse_foreign_key')

    def dimensions_for_unit(self, unit_type, include_partitions=True):
        return self._features_lookup(
            unit_type, 'dimension',
            attr_filter=None if include_partitions else lambda feature: not feature.partition
        )

    def partitions_for_unit(self, unit_type):
        return self._features_lookup(
            unit_type, 'dimension',
            attr_filter=lambda feature: feature.partition
        )

    def measures_for_unit(self, unit_type):
        return self._features_lookup(unit_type, 'measure')

    def _resolve(self, unit_type, feature, role=None):
        unit_type = self.identifier_for_unit(unit_type)
        via = ''
        attrs = {}
        eff_unit_type = unit_type

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
            if via_suffix:
                eff_unit_type = self.identifier_for_unit(s[-2])
                via += ('/' + via_suffix) if via else via_suffix
            attrs['unit_type'] = unit_type

        return MeasureProvider._resolve(self, eff_unit_type, feature, role=role)._with_attrs(**attrs).as_via(via)

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

        Args:
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

        # TODO: Handle relation case, where ...

        # [Provision(provider, measures, dimensions), ...]
        unit_type = self.identifier_for_unit(unit_type)
        measures = {self.resolve(unit_type, measure, role='measure'): self.resolve(unit_type, measure, role='measure') for measure in measures}
        dimensions = {self.resolve(unit_type, dimension, role='dimension'): self.resolve(unit_type, dimension, role='dimension') for dimension in dimensions}

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
                measures=[measures.pop(measure).from_provider(p) for measure in measures.copy() if measure in p.measures_for_unit(unit_type)],
                dimensions=[dimensions.pop(dimension).from_provider(p) for dimension in dimensions.copy() if dimension in p.dimensions_for_unit(unit_type) or dimension in p.foreign_keys_for_unit(unit_type) or dimension in p.measures_for_unit(unit_type)]  # TODO: Use p.resolve?
            ))
            if len(measures) + len(dimensions) == dimension_count and not (require_primary is True and len(provisions) == 1):
                raise RuntimeError("Could not provide provisions for: measures={}, dimensions={}. This is a bug.".format(list(measures), list(dimensions)))
            dimension_count = len(measures) + len(dimensions)

        return provisions

    def evaluate(self, unit_type, measures=None, segment_by=None, where=None,
                 joins=None, stats=True, covariates=False, context=None,
                 stats_registry=None, **opts):
        strategy = self.get_strategy(
            unit_type, measures=measures, segment_by=segment_by, where=where, context=context
        )
        return strategy.execute(stats=stats, covariates=covariates, context=context, **opts)

    def get_ir(self, unit_type, measures=None, segment_by=None, where=None,
               joins=None, stats=True, covariates=False, context=None,
               stats_registry=None, **opts):
        strategy = self.get_strategy(
            unit_type, measures=measures, segment_by=segment_by, where=where, context=context
        )
        return strategy.execute(stats=stats, covariates=covariates, ir_only=True, context=context, **opts)

    def get_strategy(self, unit_type, measures=None, segment_by=None, where=None, context=None):
        # TODO: incorporate context into strategy evaluation
        # TODO: Add support for joins to meta measure provider
        # TODO: Add support for stats_registry
        return EvaluationStrategy.from_spec(
            self, unit_type, measures=measures, segment_by=segment_by, where=where
        )
