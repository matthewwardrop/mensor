import json

from .context import EvaluationContext
from .types import Join, DimensionBundle


class EvaluationStrategy(object):

    def __init__(self, registry, provider, unit_type, measures, segment_by=None,
                 where=None, join_on_left=None, join_on_right=None, joins=None, **opts):
        self.registry = registry
        self.provider = provider
        self.unit_type = unit_type
        self.measures = measures or []
        self.where = where or []
        self.segment_by = segment_by or []
        self.join_on_left = join_on_left or [self.unit_type.name]
        self.join_on_right = join_on_right or [self.matched_unit_type.name]
        self.joins = joins or []
        self.opts = opts

    @property
    def matched_unit_type(self):
        for identifier in sorted(self.provider.identifiers, key=lambda x: len(x.name)):
            if identifier.matches(self.unit_type):
                return identifier

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
                        'join_on_left': o.join_on_left,
                        'join_on_right': o.join_on_right,
                        'join_type': o.join_type,
                        'joins': o.joins,
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

        # Add partitions to join keys and segment_by as necessary
        common_partitions = (
            set(self.provider.partitions_for_unit(self.matched_unit_type.name))
            .intersection(strategy.provider.partitions_for_unit(strategy.unit_type.name))
        )
        if len(common_partitions) > 0:
            for partition in common_partitions:
                if partition not in self.segment_by:
                    self.segment_by.append(self.provider.resolve(partition, kind='dimension').as_private)
                if partition not in strategy.segment_by:
                    strategy.segment_by.append(strategy.provider.resolve(partition, kind='dimension'))
                strategy.join_on_left.extend([p.name for p in common_partitions])
                strategy.join_on_right.extend([p.name for p in common_partitions])

        self.joins.append(strategy)
        return self

    @property
    def join_type(self):
        if any(not w.scoped for w in self.where):
            return 'inner'
        for join in self.joins:
            if join.join_type == 'inner':
                return 'inner'
        return 'left'

    def execute(self, ir_only=False, as_join=False, compatible=False, via=None):
        # Step 1: Build joins
        joins = []
        if via is None:
            via = tuple()
        via += (self.unit_type.name,)

        for join in self.joins:
            joins.append(join.execute(
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
                joins=joins,
                via=via[1:],
                **self.opts
            )
        else:
            evaluated = self.provider.evaluate(
                unit_type=self.unit_type,
                measures=self.measures,
                segment_by=self.segment_by,
                where=self.where,
                joins=joins,
                **self.opts
            )

            if as_join:
                evaluated.columns = ['{}/{}'.format(self.unit_type.name, c) if c != self.unit_type else c for c in evaluated.columns]
                return Join(
                    provider=self.provider,
                    unit_type=self.unit_type,
                    left_on=self.join_on_left,
                    right_on=['{}/{}'.format(self.unit_type.name, j) for j in self.join_on_right],
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
