import numbers
import re
import textwrap

import jinja2

from mensor.constraints import CONSTRAINTS
from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS
from mensor.metrics.types import MetricImplementation
from mensor.utils.registry import SubclassRegisteringABCMeta

# TODO: Consider using sqlalchemy to generate SQL
# TODO: Consider creating an option to produce SQL using WITH clauses
#       subqueries are safer, but perhaps less readable


# Dialects
class SQLDialect(object):

    QUOTE_COL = '"'
    QUOTE_STR = "'"
    COLUMN_PATTERN = re.compile(r"^[^0-9\W][\w/|:_.]*$")

    AGG_METHODS = {
        AGG_METHODS.SUM: lambda x: "SUM({})".format(x),
        AGG_METHODS.MEAN: lambda x: "AVG({})".format(x),
        AGG_METHODS.SQUARE_SUM: lambda x: "SUM(POW({}, 2))".format(x),
        AGG_METHODS.COUNT: lambda x: "COUNT({})".format(x)
    }

    TEMPLATE_BASE = textwrap.dedent("""
        SELECT
            {%- for dimension in dimensions %}
            {% if loop.index0 > 0 %}, {% endif %}{{ dimension }}
            {%- endfor %}
            {%- for measure in measures %}
            {% if dimensions or loop.index0 > 0 %}, {% endif %}{{ measure }}
            {%- endfor %}
        FROM (
            {{ _sql | indent(width=4) }}
        ) {{ table_name | col }}
        {%- if joins|length > 0 %}
        {%- for join in joins %}
        {{ join.how | upper }} JOIN  (
            {{ join.object | indent(width=4) }}
        ) {{ join.name | col }}
        ON
        {%- for field in join.left_on %}
            {% if loop.index0 > 0 %}AND {% endif %}{{ field_map['dimensions'][field] }} = {{ join.name | col }}.{{ join.right_on[loop.index0] | col }}
        {%- endfor -%}
        {%- endfor %}
        {%- endif %}
        {%- if constraints %}
        WHERE {{ constraints }}
        {%- endif %}
        {%- if groupby|length > 0 %}
        GROUP BY
        {%- for gb in groupby %}
            {% if loop.index > 1 %},{% endif %} {{ gb }}
        {%- endfor %}
        {%- endif %}
    """).strip()

    TEMPLATE_TABLE = textwrap.dedent("""
        SELECT
            {%- for dimension in dimensions %}
            {% if loop.index0 > 0 or identifiers%}, {% endif %}{{ dimension.expr | col }} AS {{ dimension.fieldname(role='dimension') | col }}
            {%- endfor %}
            {%- for measure in measures %}
            {% if loop.index0 > 0 or identifiers or dimensions %}, {% endif %}{{ measure.expr | col }} AS {{ measure.fieldname(role='measure') | col }}
            {%- endfor %}
        FROM {{table}}
    """).strip()

    @classmethod
    def constraint_maps(cls):
        """
        Each mapped value for a contraint should be a function taking three parameter:
            - a where clause
            - a field mapping
            - a resolver for constraints taking arguments field_mapping and a where clause
        """
        ve = cls.value_encode
        return {
            CONSTRAINTS.AND: lambda w, f, m: '({})'.format(' AND '.join(m(f, o) for o in w.operands)),
            CONSTRAINTS.OR: lambda w, f, m: '({})'.format(' OR '.join(m(f, o) for o in w.operands)),
            CONSTRAINTS.EQUALITY: lambda w, f, m: "{} = {}".format(f['dimensions'][w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_GT: lambda w, f, m: "{} > {}".format(f['dimensions'][w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_GTE: lambda w, f, m: "{} >= {}".format(f['dimensions'][w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_LT: lambda w, f, m: "{} < {}".format(f['dimensions'][w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_LTE: lambda w, f, m: "{} <= {}".format(f['dimensions'][w.field], ve(w.value)),
            CONSTRAINTS.IN: lambda w, f, m: "{} IN ({})".format(f['dimensions'][w.field], ", ".join(ve(v) for v in w.value)),
        }

    # SQL rendering helpers
    @classmethod
    def column_encode(cls, column_expr):
        if cls.COLUMN_PATTERN.match(column_expr):
            return '{quote}{col}{quote}'.format(
                quote=cls.QUOTE_COL,
                col=column_expr
            )
        return column_expr

    @classmethod
    def column_decode(cls, column_name):
        return column_name

    @classmethod
    def value_encode(cls, value):
        "This method quotes values appropriately."
        if isinstance(value, str):
            return '{quote}{value}{quote}'.format(quote=cls.QUOTE_STR, value=value)  # TODO: escape quotes in string
        elif isinstance(value, numbers.Number):
            return str(value)
        elif value is None:
            return 'NULL'
        raise ValueError("SQL dialect `{}` does not support quoting objects of type: `{}`".format(cls, type(value)))

    # TODO?
    # @classmethod
    # def value_decode(cls, value):
    #     return value

    @classmethod
    def source_column_encode(cls, source_name, column_expr, default=None):
        if cls.COLUMN_PATTERN.match(column_expr):
            column = "{source}.{column}".format(
                source=cls.column_encode(source_name),
                column=cls.column_encode(column_expr)
            )
        else:
            column = cls.column_encode(column_expr)
        if default is not None:
            column = "COALESCE({column}, {default})".format(
                column=column,
                default=cls.value_encode(default)
            )
        return column


class PrestoDialect(SQLDialect):
    pass


class HiveDialect(SQLDialect):

    QUOTE_COL = '`'

    @classmethod
    def column_encode(cls, column_expr):
        if cls.COLUMN_PATTERN.match(column_expr):
            return SQLDialect.column_encode(cls, column_expr).replace(':', '+').replace('/', '-')
        return SQLDialect.column_encode(cls, column_expr)

    @classmethod
    def column_decode(cls, column_name):
        return column_name.replace('+', ':').replace('+', '/')


DIALECTS = {
    'presto': PrestoDialect,
    'hive': HiveDialect,
}


class SQLExecutor(metaclass=SubclassRegisteringABCMeta):

    REGISTRY_KEYS = None

    @property
    def dialect(self):
        return None

    def query(self, sql):
        raise NotImplementedError


class DebugSQLExecutor(SQLExecutor):

    REGISTRY_KEYS = ['debug']

    @property
    def dialect(self):
        return 'presto'

    def query(self, sql):
        print(sql)
        raise NotImplementedError("This SQLExecutor goes no further.")


class SQLMeasureProvider(MeasureProvider):

    REGISTRY_KEYS = ['sql']
    COLUMN_EXPR_PREAPPLIED = False

    def __init__(self, *args, sql=None, executor=None, **kwargs):

        if not executor:
            executor = DebugSQLExecutor()
        elif isinstance(executor, str):
            executor = SQLExecutor.for_kind(executor)()
        elif issubclass(executor, SQLExecutor):
            executor = executor()

        MeasureProvider.__init__(self, *args, **kwargs)
        self._base_sql = textwrap.dedent(sql).strip() if sql else None
        self.executor = executor
        self.dialect = DIALECTS[executor.dialect]

        self.provides_measure('count', shared=True, distribution=None, default=0)

        self._template_environment = jinja2.Environment(loader=jinja2.FunctionLoader(lambda x: x), undefined=jinja2.StrictUndefined)
        self._template_environment.filters.update({
            'col': self._col,
            'val': self._val
        })

    def _sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        assert all(self._is_compatible_with(es.provider) and es.joins_all_compatible for es in self.provisions.values())
        return self._template_environment.get_template(self._base_sql).render(
            **(opts['context'] or {}),
            **{
                name: es.execute(ir_only=True, stats=False)
                for name, es in self.provisions.items()
            }
        )

    def _evaluate(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        df = self.executor.query(self.get_sql(
            unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            joins=joins,
            stats=stats,
            covariates=covariates,
            **opts
        ))
        df.columns = [self.dialect.column_decode(col) for col in df.columns]
        return df

    def get_sql(self, *args, **kwargs):
        return self.get_ir(*args, **kwargs)

    def _get_ir(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        field_map = self._field_map(unit_type, measures, segment_by, joins)
        unit_agg = not unit_type.is_unique
        sql = self._template_environment.get_template(self.dialect.TEMPLATE_BASE).render(
            _sql=self._sql(unit_type=unit_type, measures=measures, segment_by=segment_by, where=where, joins=joins, stats=stats, covariates=covariates, **opts),
            field_map=field_map,
            provider=self,
            table_name=self._table_name(unit_type),
            dimensions=self._get_dimensions_sql(field_map, segment_by),
            measures=self._get_measures_sql(field_map, measures, unit_agg, stats, covariates),
            groupby=self._get_groupby_sql(field_map, segment_by),
            joins=joins,
            constraints=self._get_where_sql(field_map, where),
        )
        return sql

    # SQL rendering Methods
    def _table_name(self, unit_type):
        return "provision_{}_{}".format(self.name, unit_type.name)

    def _col(self, column_expr):
        return self.dialect.column_encode(column_expr)

    def _val(self, value):
        return self.dialect.value_encode(value)

    def _field_map(self, unit_type, measures, dimensions, joins):
        field_map = {'measures': {}, 'dimensions': {}}

        self_table_name = self._table_name(unit_type)

        for measure in measures:
            if not measure.external:
                if measure.via_name in field_map['measures']:
                    raise ValueError(measure.via_name)
                field_map['measures'][measure.via_name] = self.dialect.source_column_encode(self_table_name, measure.fieldname(role='measure') if self.COLUMN_EXPR_PREAPPLIED else measure.expr, measure.default)

        for dimension in dimensions:
            if not dimension.external:
                if dimension.via_name in field_map['dimensions']:
                    raise ValueError(dimension.via_name)
                field_map['dimensions'][dimension.via_name] = self.dialect.source_column_encode(self_table_name, dimension.fieldname(role='dimension') if self.COLUMN_EXPR_PREAPPLIED else dimension.expr, dimension.default)

        for join in joins:
            for measure in join.measures:
                if measure.as_via(join.join_prefix) in measures and measures[measure.as_via(join.join_prefix)].external:
                    map_name = measure.as_via(join.join_prefix).via_name
                else:
                    map_name = '/'.join([join.name, measure.via_name])
                field_map['measures'][map_name] = self.dialect.source_column_encode(join.name, measure.fieldname(role='measure'), measure.default)
            for dimension in join.dimensions:
                if dimension.as_via(join.join_prefix) in dimensions and dimensions[dimension.as_via(join.join_prefix)].external:
                    map_name = dimension.as_via(join.join_prefix).via_name
                else:
                    map_name = '/'.join([join.name, dimension.via_name])
                field_map['dimensions'][map_name] = self.dialect.source_column_encode(join.name, dimension.fieldname(role='dimension'), dimension.default)

        return field_map

    def _get_dimensions_sql(self, field_map, dimensions):
        dims = []
        for dimension in dimensions:
            if not dimension.private:
                dims.append(
                    '{} AS {}'.format(
                        field_map['dimensions'][dimension.via_name],
                        self._col(dimension.via_name)
                    )
                )
        return dims

    def _get_measures_sql(self, field_map, measures, unit_agg, stats, covariates):
        aggs = []

        if unit_agg and stats:
            raise NotImplementedError("Computing stats and rebasing units simultaneously has not been implemented for the SQL backend.")
        else:
            for measure in measures:
                if not measure.private:
                    for fieldname, col_map in measure.get_fields(stats=stats, unit_agg=unit_agg).items():
                        aggs.append(
                            '{col_op} AS {f}'.format(
                                col_op=col_map('1' if measure == 'count' else field_map['measures'][measure.via_name]),
                                f=self._col(fieldname),
                            )
                        )

        return aggs

    def _get_groupby_sql(self, field_map, dimensions):
        return [field_map['dimensions'][dimension.via_name] for dimension in dimensions if not dimension.private]

    def _get_where_sql(self, field_map, where):
        if where:
            return self._constraint_map(where.kind)(where, field_map, self._get_where_sql)

    @property
    def _agg_methods(self):
        return self.dialect.AGG_METHODS

    @property
    def _constraint_maps(self):
        return self.dialect.constraint_maps()

    def _is_compatible_with(self, provider):
        return isinstance(provider, SQLMeasureProvider)


class SQLTableMeasureProvider(SQLMeasureProvider):

    REGISTRY_KEYS = ['sql_table']
    COLUMN_EXPR_PREAPPLIED = True

    def _sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        if len(self.identifiers) + len(self.dimensions) + len(self.measures) == 0:
            raise RuntimeError("No columns identified in table.")
        return self._template_environment.get_template(self.dialect.TEMPLATE_TABLE).render(
            table=SQLMeasureProvider._sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts),
            identifiers=None,
            measures=[m for m in measures if m != 'count' and not m.external],
            dimensions=[d for d in segment_by if not d.external],
        )


class SQLMetricImplementation(MetricImplementation):

    REGISTRY_KEYS = ['sql']

    def __init__(self, sql, post_stats=True):
        self._sql = textwrap.dedent(sql) if sql else sql
        self.post_stats = post_stats

    @property
    def sql(self):
        return self._sql

    def _is_compatible_with_strategy(self, strategy):
        return isinstance(strategy.provider, SQLMeasureProvider) and strategy.joins_all_compatible

    def evaluate(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        ir = self.get_ir(strategy, marginalise=marginalise, compatible_metrics=compatible_metrics, **opts)
        return strategy.provider.executor.query(ir)

    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        return strategy.provider._template_environment.get_template(self.sql).render(
            measures=[m for m in strategy.measures if not m.private],
            segment_by=[d for d in strategy.segment_by if not d.private],
            marginalise=marginalise or [],
            provision=strategy.execute(ir_only=True, stats=self.post_stats, **opts.pop('measure_opts', {})),
            **opts
        )


class SimpleSQLMetricImplementation(SQLMetricImplementation):

    REGISTRY_KEYS = ['sql_simple']

    TEMPLATE = """
        SELECT
            {%- with ns=namespace(_cnt=0) %}
            {%- for dimension in segment_by if dimension not in marginalise and not dimension.private %}
            {% if loop.index0 > 0 %}, {% endif %}{{ dimension.via_name | col }}
            {%- set ns._cnt = ns._cnt + 1 %}
            {%- endfor %}
            {%- for metric in metrics %}
            {% if ns._cnt > 0 or loop.index0 > 0 %}, {% endif %}{{ metric }}
            {%- endfor %}
            {%- endwith %}
        FROM (
            {{ provision | indent(width=4) }}
        )
    """

    def __init__(self, metrics_callback):
        SQLMetricImplementation.__init__(self, self.TEMPLATE)
        self.metrics_callback = metrics_callback

    def get_ir(self, strategy, marginalise=None, compatible_metrics=None, **opts):
        metrics = self.metrics_callback(strategy=strategy, **opts)

        for metric in (compatible_metrics or []):
            impl = metric.implementation_for_strategy(strategy)
            metrics.extend(impl.metrics_callback(strategy=strategy, **impl.metric.opts.process()))

        return SQLMetricImplementation.get_ir(self, strategy, marginalise=marginalise, metrics=metrics, **opts)

    def _is_compatible_with_metric(self, metric):
        if isinstance(metric, self.__class__):
            return True
        return False
