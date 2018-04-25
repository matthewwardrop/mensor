import json
from collections import OrderedDict
from enum import Enum

from .context import EvaluationContext, And
from .types import Join, DimensionBundle, _StatisticalUnitIdentifier, _Dimension


class STRATEGY_TYPE(Enum):
    REGULAR = 1
    UNIT_REBASE = 2


class EvaluationStrategy(object):

    def __init__(self, registry, provider, unit_type, measures, segment_by=None,
                 where=None, join_on_left=None, join_on_right=None,
                 join_prefix=None, joins=None):
        self.registry = registry
        self.provider = provider

        # Statistical unit used for evaluation
        self.unit_type = unit_type

        # Anticipated measures, segmentations and constraints
        # TODO: Use dictionaries to improve lookup performance
        self.measures = measures or []
        self.segment_by = segment_by or []
        self.where = where

        # Join parameters
        self.is_joined = False
        self.join_on_left = join_on_left or [self.unit_type.name]
        self.join_on_right = join_on_right or [self.matched_unit_type.name]
        self.joins = joins or []
        self.join_prefix = join_prefix or self.unit_type.name

    def _check_constraints(self, prefix=None, raise_on_unconstrained=True):
        """
        This method checks whether dimensions that require constraints have been
        constrained.
        """
        unconstrained = []
        constrained_dimensions = self.where.dimensions if self.where else []
        constrained_dimensions.extend(self.join_on_right)
        for dimension in self.segment_by:
            if isinstance(dimension, _Dimension) and dimension.requires_constraint and dimension not in constrained_dimensions:
                unconstrained.append('{}/{}'.format(prefix, dimension.name) if prefix else dimension.name)

        for join in self.joins:
            unconstrained.extend(join._check_constraints(prefix='{}/{}'.format(prefix, join.unit_type.name) if prefix else join.unit_type.name, raise_on_unconstrained=False))

        if raise_on_unconstrained and len(unconstrained) > 0:
            raise RuntimeError("The following dimensions require and lack constraints: {}.".format(unconstrained))

        return unconstrained

    @property
    def matched_unit_type(self):
        for identifier in sorted(self.provider.identifiers, key=lambda x: len(x.name)):
            if identifier.matches(self.unit_type):
                return identifier

    def reverse_matched_unit_type(self, strategy):
        for identifier in sorted(self.provider.identifiers, key=lambda x: len(x.name), reverse=True):
            if identifier.matches(strategy.unit_type, reverse=False):
                return identifier

    @property
    def strategy_type(self):
        if not self.matched_unit_type.is_primary:
            return STRATEGY_TYPE.UNIT_REBASE
        else:
            return STRATEGY_TYPE.REGULAR

    def __repr__(self):
        class StrategyEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, EvaluationStrategy):
                    d = OrderedDict([
                        ('provider', o.provider),
                        ('unit_type', o.unit_type)
                    ])
                    if o.measures:
                        d['measures'] = o.measures
                        d['strategy_type'] = o.strategy_type
                    if o.segment_by:
                        d['segment_by'] = o.segment_by
                    # if o.where:
                    d['where'] = o.where
                    if o.is_joined:
                        d['join_on_left'] = o.join_on_left
                        d['join_on_right'] = o.join_on_right
                        d['join_type'] = o.join_type
                        if o.join_prefix != o.unit_type.name:
                            d['join_prefix'] = o.join_prefix
                    if o.joins:
                        d['joins'] = o.joins
                    return d
                return str(o)
        return 'EvaluationStrategy(' + json.dumps(self, indent=4, cls=StrategyEncoder) + ')'

    def add_join(self, unit_type, strategy):
        # TODO: Make atomic
        assert isinstance(strategy, EvaluationStrategy)

        # Add primary join key if missing and set join
        self_unit_type = self.provider.identifier_for_unit(unit_type.name)
        join_unit_type = strategy.provider.identifier_for_unit(unit_type.name)
        if self_unit_type not in self.segment_by:
            self.segment_by.insert(0, self_unit_type.as_private)
        if join_unit_type not in strategy.segment_by:
            strategy.segment_by.insert(0, join_unit_type)
        else:
            strategy.segment_by[strategy.segment_by.index(join_unit_type)].private = False

        if strategy.strategy_type is STRATEGY_TYPE.UNIT_REBASE:
            strategy.join_on_left = [self.reverse_matched_unit_type(strategy).name]
            strategy.join_on_right = [strategy.unit_type.name]
        else:
            strategy.join_on_left = [strategy.unit_type.name]
            strategy.join_on_right = [strategy.matched_unit_type.name]

        # Add common partitions to join keys
        common_partitions = list(
            set(self.provider.partitions_for_unit(self.matched_unit_type.name))
            .intersection(strategy.provider.partitions_for_unit(strategy.unit_type.name))
        )

        for partition in common_partitions:
            if partition not in self.segment_by:
                self.segment_by.append(self.provider.resolve(partition, kind='dimension').as_private)
            if partition not in strategy.segment_by:
                strategy.segment_by.append(strategy.provider.resolve(partition, kind='dimension'))
            else:
                strategy.segment_by[strategy.segment_by.index(partition)].private = False
            strategy.join_on_left.extend([p.name for p in common_partitions])
            strategy.join_on_right.extend([p.name for p in common_partitions])

        # Add measures and segmentations in parent from join
        self.measures.extend(
            (
                measure.as_external.as_via(strategy.unit_type)
                if strategy.unit_type != self.unit_type else
                measure.as_external
            )
            for measure in strategy.measures
            if not measure.private
        )

        self.segment_by.extend(
            (
                dimension.as_external.as_via(strategy.unit_type)
                if strategy.unit_type != self.unit_type else
                dimension.as_external
            )
            for dimension in strategy.segment_by
            if (
                not dimension.private
                and dimension not in strategy.join_on_right
            )
        )

        if self.where:
            self.segment_by.extend(
                (
                    dimension.as_external.as_via(strategy.unit_type).as_private
                    if strategy.unit_type != self.unit_type else
                    dimension.as_external.as_private
                )
                for dimension in strategy.segment_by
                if (
                    dimension.as_via(strategy.unit_type) not in self.segment_by
                    and dimension.as_via(strategy.unit_type) in self.where.dimensions
                )
            )

        # Set joined flag
        strategy.is_joined = True

        self.joins.append(strategy)
        return self

    @property
    def join_type(self):
        if self.where is not None and len(self.where.dimensions) > 0:
            return 'inner'
        for join in self.joins:
            if join.join_type == 'inner':
                return 'inner'
        return 'left'

    def execute(self, stats=True, ir_only=False, as_join=False,
                compatible=False, **opts):

        self._check_constraints()

        # Step 1: Build joins
        stats = stats and not self.is_joined
        joins = []

        for join in self.joins:
            joins.append(join.execute(
                as_join=True,
                compatible=join.provider._is_compatible_with(self.provider)
            ))

        # Step 2: Evaluate provider
        if as_join and compatible:
            try:
                return Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    left_on=self.join_on_left,
                    right_on=self.join_on_right,
                    measures=self.measures,
                    dimensions=self.segment_by,
                    object=self.provider.get_ir(
                        unit_type=self.unit_type,
                        measures=self.measures,
                        segment_by=self.segment_by,
                        where=self.where,
                        joins=joins,
                        stats=stats,
                        **opts
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
                joins=joins,
                stats=stats,
                **opts
            )
        else:
            evaluated = self.provider.evaluate(
                unit_type=self.unit_type,
                measures=self.measures,
                segment_by=self.segment_by,
                where=self.where,
                joins=joins,
                stats=stats,
                **opts
            )

            if as_join:
                evaluated = evaluated.add_prefix('{}/'.format(self.join_prefix))
                return Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    left_on=self.join_on_left,
                    right_on=['{}/{}'.format(self.join_prefix, j) for j in self.join_on_right],
                    measures=self.measures,
                    dimensions=self.segment_by,
                    how=self.join_type,
                    object=evaluated,
                    compatible=False
                )
            return evaluated

    @classmethod
    def from_spec(cls, registry, unit_type, measures=None, segment_by=None, where=None, **opts):

        # Step 0: Resolve applicable measures and dimensions
        unit_type = registry._resolve_identifier(unit_type)
        measures = [] if measures is None else measures
        segment_by = [] if segment_by is None else segment_by

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
            for dimension in where.scoped_applicable_dimensions
            if dimension not in segment_by
        ]

        # Step 1: Collect measures and dimensions into groups based on current unit_type
        # and next unit_type

        current_evaluation = DimensionBundle(unit_type=unit_type, dimensions=[], measures=[])
        next_evaluations = {}

        def collect_dimensions(dimensions, kind='measures', for_constraint=False):
            for dimension in dimensions:
                if not dimension.via_next:
                    current_evaluation._asdict()[kind].append(dimension)
                elif (  # Handle reverse foreign key joins
                    (for_constraint or kind == 'measures')
                    and dimension.via_next in registry.reverse_foreign_keys_for_unit(unit_type)
                ):
                    next_unit_type = registry._resolve_reverse_foreign_key(unit_type, dimension.via_next)
                    if next_unit_type not in next_evaluations:
                        next_evaluations[next_unit_type] = DimensionBundle(unit_type=unit_type, dimensions=[], measures=[])
                    next_evaluations[next_unit_type]._asdict()[kind].append(dimension.resolved_next)
                else:
                    next_unit_type = registry._resolve_foreign_key(unit_type, dimension.via_next)
                    if next_unit_type not in next_evaluations:
                        next_evaluations[next_unit_type] = DimensionBundle(unit_type=next_unit_type, dimensions=[], measures=[])
                    next_evaluations[next_unit_type]._asdict()[kind].append(dimension.resolved_next)

        collect_dimensions(measures, kind='measures')
        collect_dimensions(segment_by, kind='dimensions')
        collect_dimensions(where_dimensions, kind='dimensions', for_constraint=True)

        # Add required dimension for joining in next unit_types
        for dimension_bundle in next_evaluations.values():
            if dimension_bundle.unit_type not in current_evaluation.dimensions:
                current_evaluation.dimensions.append(dimension_bundle.unit_type.as_private)

        # Step 2: Create optimal joins for current unit_type

        provisions = registry._find_optimal_provision(
            unit_type=unit_type,
            measures=current_evaluation.measures,
            dimensions=current_evaluation.dimensions
        )

        def constraints_for_provision(provision):
            provision_constraints = []
            for constraint in where.generic_applicable:
                if len(
                    set(constraint.dimensions)
                    .difference(provision.provider.identifiers)
                    .difference(provision.provider.dimensions)
                    .difference(provision.provider.measures)
                ) == 0:
                    provision_constraints.append(constraint)
            return And.from_operands(provision_constraints)

        evaluations = [
            cls(
                registry=registry,
                provider=provision.provider,
                unit_type=unit_type,
                measures=provision.measures,
                segment_by=provision.dimensions,
                where=constraints_for_provision(provision),
                join_prefix=provision.join_prefix
            ) for provision in provisions
        ]

        # Step 3: For each next unit_type, recurse problem and join into above query

        for foreign_key, dim_bundle in next_evaluations.items():
            foreign_strategy = cls.from_spec(registry=registry, unit_type=foreign_key,
                                             measures=dim_bundle.measures, segment_by=dim_bundle.dimensions,
                                             where=where.via_next(foreign_key.name) if where is not None else None, **opts)

            if foreign_key != dim_bundle.unit_type:  # Reverse foreign key join
                foreign_key = dim_bundle.unit_type
                foreign_strategy.unit_type = dim_bundle.unit_type

            added = False
            for sub_strategy in evaluations:
                for dimension in sub_strategy.segment_by:
                    if isinstance(dimension, _StatisticalUnitIdentifier) and dimension.matches(foreign_key):
                        sub_strategy.add_join(foreign_key, foreign_strategy)
                        added = True
                        break
            if not added:
                raise RuntimeError("Could not add foreign strategy: {}".format(foreign_strategy))

        strategy = evaluations[0]
        for sub_strategy in evaluations[1:]:
            strategy.add_join(unit_type, sub_strategy)

        strategy.where = And.from_operands(strategy.where, where.scoped_applicable)

        # Step 4: Mark any resolved where dependencies as private, unless otherwise
        # requested in `segment_by`

        for dimension in where.scoped_applicable_dimensions:
            if dimension not in segment_by:
                try:
                    index = strategy.segment_by.index(dimension)
                    strategy.segment_by[index] = strategy.segment_by[index].as_private
                except ValueError:
                    raise ValueError("Could not find dependency for where clause `{}`. This is most likely because you attempted to have conditional joins spanning foreign_key and reverse_foreign_key joins, which does not make sense.".format(dimension))

        # Step 5: Return EvaluationStrategy, and profit.

        return strategy
