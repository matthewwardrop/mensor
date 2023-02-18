import json
from collections import namedtuple, OrderedDict
from enum import Enum

from mensor.constraints import And, Constraint
from mensor.utils import SequenceMap

from ..structures.features import Identifier

FeatureBundle = namedtuple('FeatureBundle', ['unit_type', 'dimensions', 'measures'])


class EvaluationStrategy(object):

    class Type(Enum):
        REGULAR = 1
        UNIT_REBASE = 2

    class Join(object):

        # TODO: Review Join API (esp. which arguments are essential, etc)

        def __init__(self, provider, unit_type, left_on, right_on, object,
                     compatible=False, join_prefix=None, name=None, measures=None, dimensions=None,
                     how='left'):
            self.provider = provider
            self.unit_type = unit_type
            self.join_prefix = join_prefix
            self.left_on = left_on
            self.right_on = right_on
            self.name = name
            self.measures = measures
            self.dimensions = dimensions
            self.object = object
            self.compatible = compatible
            self.how = how

        @property
        def name(self):
            return self._name

        @name.setter
        def name(self, name):
            if name is None:
                name = "join_{}_{}".format(self.provider.name, self.unit_type.name)
            self._name = name

    @classmethod
    def from_spec(cls, registry, unit_type, measures=None, segment_by=None, where=None, **opts):

        # Step 0: Resolve applicable measures and dimensions
        unit_type = registry.identifier_for_unit(unit_type)
        measures = [] if measures is None else measures
        segment_by = [] if segment_by is None else segment_by

        if not isinstance(measures, list):
            measures = [measures]
        if not isinstance(segment_by, list):
            segment_by = [segment_by]

        measures = [
            registry.resolve(unit_type, measure, role='measure') for measure in measures
        ]

        segment_by = [
            registry.resolve(unit_type, dimension, role='dimension') for dimension in segment_by
        ]

        where = Constraint.from_spec(where)
        where_dimensions = [
            (
                registry.resolve(unit_type, dimension, role='dimension').as_implicit
            )
            for dimension in where.scoped_for_unit_type(unit_type).dimensions
            if dimension not in segment_by
        ]

        # Step 1: Collect measures and dimensions into groups based on current unit_type
        # and next unit_type

        current_evaluation = FeatureBundle(unit_type=unit_type, dimensions=[], measures=[])
        next_evaluations = {}

        def collect_dimensions(dimensions, kind='measures', for_constraint=False):
            for dimension in dimensions:
                if not dimension.via:
                    current_evaluation._asdict()[kind].append(dimension)
                elif (  # Handle reverse foreign key joins
                    dimension.next_unit_type in registry.reverse_foreign_keys_for_unit(unit_type)
                ):
                    next_unit_type = registry.resolve(unit_type, dimension.next_unit_type, role='reverse_foreign_key')
                    if next_unit_type not in next_evaluations:
                        next_evaluations[next_unit_type] = FeatureBundle(unit_type=unit_type, dimensions=[], measures=[])
                    next_evaluations[next_unit_type]._asdict()[kind].append(dimension.via_next)
                else:
                    next_unit_type = registry.resolve(unit_type, dimension.next_unit_type, role='foreign_key')
                    if next_unit_type not in next_evaluations:
                        next_evaluations[next_unit_type] = FeatureBundle(unit_type=next_unit_type, dimensions=[], measures=[])
                    next_evaluations[next_unit_type]._asdict()[kind].append(dimension.via_next)

        collect_dimensions(measures, kind='measures')
        collect_dimensions(segment_by, kind='dimensions')
        collect_dimensions(where_dimensions, kind='dimensions', for_constraint=True)

        # Add required dimension for joining in next unit_types
        for dimension_bundle in next_evaluations.values():
            fk = registry.resolve(unit_type, dimension_bundle.unit_type, role='foreign_key')
            if fk not in current_evaluation.dimensions:
                current_evaluation.dimensions.append(fk.as_private)

        # Step 2: Create optimal joins for current unit_type

        provisions = registry._find_optimal_provision(
            unit_type=unit_type,
            measures=current_evaluation.measures,
            dimensions=current_evaluation.dimensions
        )

        evaluations = []
        for provision in provisions:
            generic_constraints = where.generic_for_provider(provision.provider)
            generic_constraint_dimensions = [
                provision.provider.resolve(unit_type, dimension).as_private
                for dimension in generic_constraints.dimensions
                if not provision.dimensions or dimension not in provision.dimensions
            ]
            evaluations.append(
                cls(
                    registry=registry,
                    provider=provision.provider,
                    unit_type=provision.provider.identifier_for_unit(unit_type.name),
                    measures=provision.measures,
                    segment_by=provision.dimensions + generic_constraint_dimensions,
                    where=generic_constraints,
                    join_prefix=provision.join_prefix
                )
            )

        # Step 3: For each next unit_type, recurse problem and join into above query

        for foreign_key, dim_bundle in next_evaluations.items():
            foreign_strategy = cls.from_spec(registry=registry, unit_type=foreign_key,
                                             measures=dim_bundle.measures, segment_by=dim_bundle.dimensions,
                                             where=where.via_next(foreign_key.name), **opts)

            if foreign_key != dim_bundle.unit_type:  # Reverse foreign key join
                foreign_key = dim_bundle.unit_type
                foreign_strategy.unit_type = foreign_strategy.provider.identifier_for_unit(dim_bundle.unit_type.name)

            added = False
            for sub_strategy in evaluations:
                for dimension in sub_strategy.segment_by:
                    # TODO: consider using enum type on ResolvedFeature?
                    if isinstance(dimension.feature, Identifier) and dimension.matches(foreign_key.name):
                        sub_strategy.add_join(foreign_key, foreign_strategy)
                        added = True
                        break
            if not added:
                raise RuntimeError("Could not add foreign strategy: {}".format(foreign_strategy))

        strategy = evaluations[0]
        for sub_strategy in evaluations[1:]:
            strategy.add_join(unit_type, sub_strategy)

        strategy.where = And.from_operands(strategy.where, where.scoped_for_unit_type(unit_type).scoped_applicable)

        # Step 4: Mark any resolved where dependencies as private, unless otherwise
        # requested in `segment_by`

        for dimension in strategy.segment_by:
            if dimension.implicit and dimension in where.scoped_for_unit_type(unit_type).dimensions:
                strategy.segment_by[dimension] = strategy.segment_by[dimension].as_private

        # Step 5: Return EvaluationStrategy, and profit.

        return strategy

    def __init__(self, registry, provider, unit_type, measures, segment_by=None,
                 where=None, join_on_left=None, join_on_right=None,
                 join_prefix=None, joins=None):
        self.registry = registry
        self.provider = provider

        # Statistical unit used for evaluation
        self.unit_type = unit_type

        # Anticipated measures, segmentations and constraints
        self.measures = SequenceMap(measures or [])
        self.segment_by = SequenceMap(segment_by or [])
        self.where = where

        # Join parameters
        self.is_joined = False
        self.join_is_compatible = True
        self.join_on_left = join_on_left
        self.join_on_right = join_on_right or [self.matched_unit_type.name]
        self.joins = joins or []
        self.join_prefix = join_prefix or self.unit_type.name

    def _check_constraints(self, prefix=None, raise_on_unconstrained=True):
        """
        This method checks whether dimensions that require constraints have been
        constrained.
        """
        unconstrained = []
        constrained_dimensions = self.where.dimensions
        constrained_dimensions.extend(self.join_on_right)

        for dimension in self.provider.dimensions_for_unit(self.unit_type):
            if dimension.requires_constraint and dimension not in constrained_dimensions:
                unconstrained.append('{}/{}'.format(prefix, dimension.name) if prefix else dimension.name)

        for join in self.joins:
            unconstrained.extend(join._check_constraints(prefix='{}/{}'.format(prefix, join.unit_type.name) if prefix else join.unit_type.name, raise_on_unconstrained=False))

        if raise_on_unconstrained and len(unconstrained) > 0:
            raise RuntimeError("The following dimensions require and lack constraints: {}.".format(unconstrained))

        return unconstrained

    @property
    def matched_unit_type(self):
        return self.provider.identifier_for_unit(self.unit_type)

    @property
    def strategy_type(self):
        if not self.matched_unit_type.is_unique:
            return self.Type.UNIT_REBASE
        else:
            return self.Type.REGULAR

    @property
    def joins_all_compatible(self):
        for join in self.joins:
            if (
                not self.provider.is_compatible_with(join.provider)
                or not join.joins_all_compatible
            ):
                return False
        return True

    def __repr__(self):
        class StrategyEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, EvaluationStrategy):
                    d = OrderedDict([
                        ('provider', o.provider),
                        ('unit_type', o.unit_type)
                    ])
                    d['strategy_type'] = o.strategy_type
                    if o.measures:
                        d['measures'] = o.measures
                    if o.segment_by:
                        d['segment_by'] = o.segment_by
                    if o.where:
                        d['where'] = o.where
                    if o.is_joined:
                        d['join_on_left'] = o.join_on_left
                        d['join_on_right'] = o.join_on_right
                        d['join_type'] = o.join_type
                        if o.join_prefix != o.unit_type.name:
                            d['join_prefix'] = o.join_prefix
                        d['join_is_compatible'] = o.join_is_compatible
                    if o.joins:
                        d['joins'] = o.joins
                        d['joins_all_compatible'] = o.joins_all_compatible
                    return d
                return o.__repr__()
        return 'EvaluationStrategy(' + json.dumps(self, indent=4, cls=StrategyEncoder, ensure_ascii=False) + ')'

    def add_join(self, unit_type, strategy):
        # TODO: Make atomic
        assert isinstance(strategy, EvaluationStrategy)

        # Add primary join key if missing and set join
        self_unit_type = self.provider.identifier_for_unit(unit_type.name).resolve(mask=unit_type.name)
        join_unit_type = strategy.provider.identifier_for_unit(unit_type.name).resolve()
        if self_unit_type not in self.segment_by:
            self.segment_by.prepend(self_unit_type.as_private)
        if join_unit_type not in strategy.segment_by:
            strategy.segment_by.prepend(join_unit_type)
        else:
            strategy.segment_by[join_unit_type].private = False

        strategy.join_on_left = [self_unit_type.fieldname(role='dimension')]
        strategy.join_on_right = [join_unit_type.fieldname(role='dimension')]

        # Add common partitions to join keys
        common_partitions = list(
            set(self.provider.partitions_for_unit(self_unit_type.fieldname(role='dimension')))
            .intersection(strategy.provider.partitions_for_unit(join_unit_type.fieldname(role='dimension')))
        )

        for partition in common_partitions:
            if partition not in self.segment_by:
                self.segment_by.append(self.provider.resolve(self.unit_type, partition, role='dimension').as_private)
            if partition not in strategy.segment_by:
                strategy.segment_by.append(strategy.provider.resolve(strategy.unit_type, partition, role='dimension'))
            else:
                strategy.segment_by[partition].private = False
            strategy.join_on_left.extend([p.fieldname(role='dimension') for p in common_partitions])
            strategy.join_on_right.extend([p.fieldname(role='dimension') for p in common_partitions])

        # Add measures and segmentations in parent from join
        self.measures.extend(
            (
                measure.as_external.as_via(strategy.join_prefix)
                if strategy.join_prefix != self.unit_type else
                measure.as_external
            )
            for measure in strategy.measures
            if not measure.private
        )

        self.segment_by.extend(
            (
                dimension.as_external.as_via(strategy.join_prefix)
                if strategy.join_prefix != self.unit_type else
                dimension.as_external
            )
            for dimension in strategy.segment_by
            if (
                not dimension.private
                and (
                    dimension not in strategy.join_on_right
                    or dimension.implicit
                )
            )
        )

        # Set join metadata on incoming strategy
        strategy.is_joined = True
        strategy.join_is_compatible = (
            self.provider.is_compatible_with(strategy.provider)
            and strategy.joins_all_compatible
        )
        if strategy.join_prefix == self.join_prefix:
            strategy.join_prefix = None

        self.joins.append(strategy)
        return self

    @property
    def join_type(self):
        if self.strategy_type == self.Type.UNIT_REBASE:
            return 'left'
        if len(self.where.dimensions) > 0:
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
                compatible=self.provider.is_compatible_with(join.provider),
                **opts
            ))

        # Step 2: Evaluate provider
        if as_join and compatible:
            try:
                return EvaluationStrategy.Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    join_prefix=self.join_prefix,
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
                        stats_registry=self.registry._stats_registry,
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
                stats_registry=self.registry._stats_registry,
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
                stats_registry=self.registry._stats_registry,
                stats=stats,
                **opts
            )

            if as_join:
                if self.join_prefix:
                    evaluated = evaluated.add_prefix('{}/'.format(self.join_prefix))
                    right_on = ['{}/{}'.format(self.join_prefix, j) for j in self.join_on_right]
                else:
                    right_on = self.join_on_right

                return EvaluationStrategy.Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    join_prefix=self.join_prefix,
                    left_on=self.join_on_left,
                    right_on=right_on,
                    measures=self.measures,
                    dimensions=self.segment_by,
                    how=self.join_type,
                    object=evaluated,
                    compatible=False
                )
            return evaluated
