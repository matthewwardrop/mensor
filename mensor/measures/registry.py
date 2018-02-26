import json

import six
from collections import namedtuple, Counter

from .context import EvaluationContext
from .provider import MeasureProvider
from .types import _Dimension, Join

__all__ = ['MeasureRegistry']


Provision = namedtuple('Provision', ['provider', 'measures', 'dimensions'])
DimensionBundle = namedtuple('DimensionBundle', ['dimensions', 'measures'])


class _ResolvedDimension(object):

    def __init__(self, name, via='', providers=[], external=False, private=False):
        self.name = name
        self.via = via
        self.providers = providers
        self.external = external
        self.private = private

    @property
    def path(self):
        return '/'.join('/'.join([self.via, self.name]).split('/')[1:])

    @property
    def providers(self):
        return self._providers

    @providers.setter
    def providers(self, providers):
        self._providers = {}
        if isinstance(providers, list):
            for provider in providers:
                assert isinstance(provider, MeasureProvider), "Invalid provider of type({})".format(type(provider))
                self._providers[provider.name] = provider
        elif isinstance(providers, dict):
            self._providers.update(providers)
        else:
            raise ValueError("Invalid provider specification.")

    @property
    def via_next(self):
        s = self.via.split('/')
        if len(s) > 1:
            return s[1]

    @property
    def resolved_next(self):
        s = self.via.split('/')
        if len(s) > 1:
            return self.__class__(self.name, via='/'.join(s[1:]), providers=self.providers, external=self.external)
        return self

    @property
    def as_external(self):
        return self.__class__(self.name, via=self.via, providers=self.providers, external=True, private=self.private)

    @property
    def as_private(self):
        return self.__class__(self.name, via=self.via, providers=self.providers, external=self.external, private=True)

    def from_provider(self, provider):
        if not isinstance(provider, MeasureProvider):
            provider = self.providers[provider]

        dim = provider.resolve(self.name)
        if self.external:
            dim = dim.as_external
        if self.private:
            dim = dim.as_private

        return dim

    def choose_provider(self, provider):
        self.providers = {provider: self.providers[provider]}

    def __repr__(self):
        return '/'.join([self.via, self.name]) + '<{}>'.format(len(self.providers)) + (' (external)' if self.external else '')

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, _ResolvedDimension):
            if other.name == self.name and other.via == self.via:
                return True
        elif isinstance(other, _Dimension):
            if other.name == self.name:
                return True
        elif isinstance(other, six.string_types):
            if '/'.join([self.via, self.name]) == other:
                return True
        else:
            return NotImplemented
        return False


class _ResolvedMeasure(_ResolvedDimension):

    pass


class EvaluationStrategy(object):

    def __init__(self, registry, provider, unit_type, measures, segment_by=None, where=None, join=None, **opts):
        self.registry = registry
        self.provider = provider
        self.unit_type = unit_type
        self.measures = measures or []
        self.where = where or []
        self.segment_by = segment_by or []
        self.join = join or []
        self.opts = opts

    def __repr__(self):
        class StrategyEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, EvaluationStrategy):
                    return {
                        'provider': o.provider,
                        'unit_type': o.unit_type,
                        'measures': o.measures,
                        'where': o.where,
                        'segment_by': o.segment_by,
                        'join': o.join,
                        'join_type': o.join_type
                    }
                return str(o)
        return 'EvaluationStrategy(' + json.dumps(self, indent=4, cls=StrategyEncoder) + ')'

    def add_join(self, unit_type, strategy):
        # TODO: Make atomic
        assert isinstance(strategy, EvaluationStrategy)
        self.measures.extend(
            (
                measure.as_external.as_via(strategy.unit_type)
                if strategy.unit_type != self.unit_type else
                measure.as_external
            ) for measure in strategy.measures if not measure.private
        )
        self.segment_by.extend(
            (
                dimension.as_external.as_via(strategy.unit_type)
                if strategy.unit_type != self.unit_type else
                dimension.as_external
            )
            for dimension in strategy.segment_by if not dimension.private
        )
        if unit_type not in strategy.segment_by:
            strategy.segment_by.insert(0, strategy.provider.identifier_for_unit(unit_type.name))
        self.join.append(strategy)
        return self

    @property
    def join_type(self):
        if any(not w.scoped for w in self.where):
            return 'inner'
        for join in self.join:
            if join.join_type == 'inner':
                return 'inner'
        return 'left'

    def run(self, ir_only=False, as_join=False, compatible=False, via=None):
        # Step 1: Build joins
        joins = []
        if via is None:
            via = tuple()
        via += (self.unit_type.name,)

        for join in self.join:
            joins.append(join.run(
                as_join=True,
                compatible=join.provider._is_compatible_with(self.provider),
                via=via
            ))

        # Step 2: Evaluate provider
        if as_join and compatible:
            try:
                return Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    object=self.provider.get_ir(
                        unit_type=self.unit_type,
                        measures=self.measures,
                        segment_by=self.segment_by,
                        where=self.where,
                        join=joins,
                        via=via[1:],
                        **self.opts
                    ),
                    how=self.join_type,
                    compatible=True
                )
            except NotImplementedError:
                pass

        if ir_only:
            return self.provider.get_ir(
                unit_type=self.unit_type,
                measures=self.measures,
                segment_by=self.segment_by,
                where=self.where,
                join=joins,
                via=via[1:],
                **self.opts
            )
        else:
            evaluated = self.provider.evaluate(
                unit_type=self.unit_type,
                measures=self.measures,
                segment_by=self.segment_by,
                where=self.where,
                join=joins,
                **self.opts
            )

            if as_join:
                evaluated.columns = ['{}/{}'.format(self.unit_type.name, c) if c != self.unit_type else c for c in evaluated.columns]
                return Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    object=evaluated,
                    how=self.join_type,
                    compatible=False
                )
            return evaluated

    @classmethod
    def from_spec(cls, registry, unit_type, measures, segment_by=None, where=None, **opts):

        # Step 0: Resolve applicable measures and dimensions
        unit_type = registry._resolve_identifier(unit_type)

        measures = [
            registry._resolve_measure(unit_type, measure) for measure in measures
        ]

        segment_by = [
            registry._resolve_dimension(unit_type, dimension) for dimension in segment_by
        ]

        where = EvaluationContext.from_spec(unit_type=unit_type.name, spec=where)
        assert where.unit_type == unit_type.name
        where_dimensions = [
            registry._resolve_dimension(unit_type, dimension)
            for dimension in where.dimensions if dimension not in segment_by
        ]

        # Step 1: Collect measures and dimensions into groups based on current unit_type
        # and next unit_type

        current_evaluation = DimensionBundle(dimensions=[], measures=[])
        next_evaluations = {}

        def collect_dimensions(dimensions, kind='measures'):
            for dimension in dimensions:
                if not dimension.via_next:
                    current_evaluation._asdict()[kind].append(dimension)
                else:
                    next_unit_type = registry._resolve_identifier(dimension.via_next)
                    if dimension.via_next not in next_evaluations:
                        next_evaluations[next_unit_type] = DimensionBundle(dimensions=[], measures=[])
                    next_evaluations[next_unit_type]._asdict()[kind].append(dimension.resolved_next)

        collect_dimensions(measures, kind='measures')
        collect_dimensions(segment_by, kind='dimensions')
        collect_dimensions(where_dimensions, kind='dimensions')

        # Add required dimension for joining in next unit_types
        for next_unit_type in next_evaluations:
            foreign_key = registry._resolve_foreign_key(unit_type, next_unit_type)
            if foreign_key not in current_evaluation.dimensions:
                current_evaluation.dimensions.append(foreign_key.as_private)

        # Step 2: Create optimal joins for current unit_type

        provisions = registry._find_optimal_provision(
            unit_type=unit_type,
            measures=current_evaluation.measures,
            dimensions=current_evaluation.dimensions
        )

        def constraints_for_provision(provision):
            provision_constraints = []
            for constraint in where.scoped_resolvable:
                if len(
                    set(constraint.dimensions)
                    .difference(provision.provider.identifiers)
                    .difference(provision.provider.dimensions)
                    .difference(provision.provider.measures)
                ) == 0:
                    provision_constraints.append(constraint)
            return provision_constraints

        evaluations = [
            cls(
                registry=registry,
                provider=provision.provider,
                unit_type=unit_type,
                measures=provision.measures,
                segment_by=provision.dimensions,
                where=constraints_for_provision(provision)
            ) for provision in provisions
        ]

        # Step 3: For each next unit_type, recurse problem and join into above query

        for foreign_key, dim_bundle in next_evaluations.items():
            foreign_strategy = cls.from_spec(registry=registry, unit_type=foreign_key,
                                             measures=dim_bundle.measures, segment_by=dim_bundle.dimensions,
                                             where=where.via_next(foreign_key.name) if where is not None else None, **opts)
            added = False
            for sub_strategy in evaluations:
                if foreign_key in sub_strategy.segment_by:
                    sub_strategy.add_join(foreign_key, foreign_strategy)
                    added = True
                    break
            if not added:
                raise RuntimeError("Could not add foreign strategy: {}".format(foreign_strategy))

        strategy = evaluations[0]
        for sub_strategy in evaluations[1:]:
            strategy.add_join(unit_type, sub_strategy)

        strategy.where = list(set(strategy.where).union(where.resolvable))

        # Step 4: Mark any resolved where dependencies as private, unless otherwise
        # requested in `segment_by`

        for dimension in strategy.segment_by:
            for constraint in where.resolvable:
                if dimension in constraint.dimensions and dimension not in segment_by:
                    dimension.private = True

        # Step 5: Return EvaluationStrategy, and profit.

        return strategy


class MeasureRegistry(object):

    def __init__(self):
        self._providers = {}
        self._identifiers = {}
        self._foreign_keys = {}
        self._dimensions = {}
        self._measures = {}

    def register(self, provider):
        # TODO: Check for duplicates
        # TODO: Support unregistering provider?
        # TODO: Enforce that measures and dimensions share same namespace, and never conflict with stat types
        self._providers[provider.name] = provider

        for identifier in provider.identifiers:
            self._register_identifier(identifier)

            for unit_type in provider.identifiers:
                self._register_foreign_key(identifier, unit_type)

            for dimension in provider.dimensions_for_unit(identifier):
                self._register_dimension(identifier, dimension)

            for measure in provider.measures_for_unit(identifier):
                self._register_measure(identifier, measure)

    def _register_identifier(self, unit_type):
        self.__registry_append(self._identifiers, [unit_type], unit_type)

    def _register_foreign_key(self, unit_type, foreign_key):
        self.__registry_append(self._foreign_keys, [unit_type, foreign_key], foreign_key)

    def _register_dimension(self, unit_type, dimension):
        self.__registry_append(self._dimensions, [unit_type, dimension], dimension)

    def _register_measure(self, unit_type, measure):
        self.__registry_append(self._measures, [unit_type, measure], measure)

    def __registry_extract(self, store, keys):
        for i, key in enumerate(keys):
            if key not in store:
                return []
            store = store[key]
        assert isinstance(store, list)
        return store

    def __registry_append(self, store, keys, value):
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

    def dimensions(self, unit_type):
        dims = set()
        for avail_unit_type in self._dimensions:
            if avail_unit_type.matches(unit_type):
                dims.update([v[0] for v in self._dimensions[avail_unit_type].values()])
        return dims

    def measures(self, unit_type):
        ms = set()
        for avail_unit_type in self._measures:
            if avail_unit_type.matches(unit_type):
                ms.update([v[0] for v in self._measures[avail_unit_type].values()])
        return ms

    def foreign_keys(self, unit_type):
        fks = set()
        for avail_unit_type in self._foreign_keys:
            if avail_unit_type.matches(unit_type):
                fks.update([v[0] for v in self._foreign_keys[avail_unit_type].values()])
        return fks

    def _resolve_identifier(self, unit_type):
        return self._identifiers[unit_type][0]  # TODO: Use below mechanism?

    def __resolve_dimension(self, unit_type, dimension, kind='dimension', dimension_index=None):
        # Look for dimension starting from most specific unit key with specificity
        # less than or equal to provided unit_type. Unit_type name length
        # is a good proxy for this.
        unit_type = self._resolve_identifier(unit_type)
        dimension_index = dimension_index or getattr(self, '_' + kind + 's', {})
        via = unit_type.name
        dimensions = None

        if isinstance(dimension, str):
            s = dimension.split('/')
            if len(s) > 1 and s[0] == unit_type.name:  # Remove reference to current unit_type
                raise RuntimeError("Self-referencing foreign_key.")
            via_suffix = '/'.join(s[:-1])
            dimension = s[-1]
            if via_suffix:
                unit_type = self._resolve_identifier(s[-2])
                via += '/' + via_suffix
            for avail_unit_type in sorted(dimension_index, key=lambda x: len(x.name), reverse=True):
                if avail_unit_type.matches(unit_type) and dimension in dimension_index[avail_unit_type]:
                    dimensions = dimension_index[avail_unit_type][dimension]
                    break
            if dimensions is None:
                raise ValueError("No such {} `{}` for unit type `{}`.".format(kind, dimension, unit_type))

        elif isinstance(dimension, _Dimension):
            dimensions = [dimension]

        elif isinstance(dimension, _ResolvedDimension):
            return dimension

        if not isinstance(dimensions, list) and all(isinstance(d, _Dimension) for d in dimensions):
            raise ValueError("Invalid type for {}: `{}`".format(kind, dimension.__class__))

        if kind in ('dimension', 'foreign_key'):
            return _ResolvedDimension(dimension.name if isinstance(dimension, _Dimension) else dimension, via, providers=[d.provider for d in dimensions])
        elif kind == 'measure':
            return _ResolvedMeasure(dimension.name if isinstance(dimension, _Dimension) else dimension, via, providers=[d.provider for d in dimensions])
        else:
            raise RuntimeError("SHOULD NOT BE POSSIBLE. Invalid kind '{}'.".format(kind))

    def _resolve_foreign_key(self, unit_type, foreign_type):
        return self.__resolve_dimension(unit_type, foreign_type, kind='foreign_key')

    def _resolve_measure(self, unit_type, measure):
        return self.__resolve_dimension(unit_type, measure, kind='measure')

    def _resolve_dimension(self, unit_type, dimension):
        try:
            return self.__resolve_dimension(unit_type, dimension, kind='dimension')
        except:
            pass
        try:
            return self._resolve_measure(unit_type, dimension)
        except:
            pass
        try:
            return self._resolve_foreign_key(unit_type, dimension)
        except:
            pass
        raise ValueError("No such dimension {} for unit type '{}'".format(dimension, unit_type))

    def _find_optimal_provision(self, unit_type, measures, dimensions, require_primary=True):
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
                for p, _ in provider_count.most_common() + [(ut.provider, 0) for ut in self._identifiers[unit_type.name] if ut.is_primary]:
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

    def evaluate(self, unit_type, measures, segment_by=None, where=None, **opts):
        return EvaluationStrategy.from_spec(self, unit_type, measures, where=where, segment_by=segment_by, **opts)

    def show(self, *unit_types):
        unit_types = [self._resolve_identifier(ut) for ut in unit_types] if len(unit_types) > 0 else sorted(self._identifiers)
        for unit_type in unit_types:
            print("{}:".format(unit_type.name))
            if self.foreign_keys(unit_type):
                print("\tForeign Keys:")
                for foreign_key in sorted(self.foreign_keys(unit_type)):
                    if foreign_key != unit_type:
                        print("\t\t{}::{}: {}".format(foreign_key.provider.name, foreign_key.name, foreign_key.desc or "No description."))
            if self.dimensions(unit_type):
                print("\tDimensions:")
                for measure in sorted(self.dimensions(unit_type)):
                    print("\t\t{}::{}: {}".format(measure.provider.name, measure.name, measure.desc or "No description."))
            if self.measures(unit_type):
                print("\tMeasures:")
                for measure in sorted(self.measures(unit_type)):
                    print("\t\t{}::{}: {}".format(measure.provider.name, measure.name, measure.desc or "No description."))

        # TODO: deduplicate providers to ensure actions are as optimal as possible
        # TODO: split into multiple actions depending on whether measures are in same provider
