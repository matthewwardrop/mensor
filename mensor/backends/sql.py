import numbers
import textwrap

import jinja2

from mensor.constraints import CONSTRAINTS
from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS
from mensor.metrics.types import MetricImplementation

# TODO: Consider using sqlalchemy to generate SQL
# TODO: Consider creating an option to produce SQL using WITH clauses
#       subqueries are safer, but perhaps less readable


# Dialects
class SQLDialect(object):

    QUOTE_COL = '"'
    QUOTE_STR = "'"

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
            {% if loop.index0 > 0 or identifiers%}, {% endif %}{{ dimension.expr }}
            {%- endfor %}
            {%- for measure in measures %}
            {% if loop.index0 > 0 or identifiers or dimensions %}, {% endif %}{{ measure.expr }}
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
    def column_encode(cls, column_name):
        return '{quote}{col}{quote}'.format(
            quote=cls.QUOTE_COL,
            col=column_name
        )

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
    def source_column_encode(cls, source_name, column_name, default=None):
        return (
            "COALESCE({source}.{column}, {default})"
            if default is not None else
            "{source}.{column}"
        ).format(
            source=cls.column_encode(source_name),
            column=cls.column_encode(column_name),
            default=cls.value_encode(default) if default is not None else None
        )


class PrestoDialect(SQLDialect):
    pass


class HiveDialect(SQLDialect):

    QUOTE_COL = '`'

    @classmethod
    def column_encode(cls, column_name):
        return '{quote}{col}{quote}'.format(
            quote=cls.QUOTE_COL,
            col=column_name.replace(':', '+').replace('/', '-')
        )

    @classmethod
    def column_decode(cls, column_name):
        return column_name.replace('+', ':').replace('+', '/')


DIALECTS = {
    'presto': PrestoDialect,
    'hive': HiveDialect,
}


class SQLMeasureProvider(MeasureProvider):

    def __init__(self, *args, sql=None, db_client=None, dialect='presto', **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self._base_sql = textwrap.dedent(sql).strip() if sql else None
        self.db_client = db_client
        self.dialect = DIALECTS[dialect]

        self.provides_measure('count', shared=True, distribution=None)

        self._template_environment = jinja2.Environment(loader=jinja2.FunctionLoader(lambda x: x))
        self._template_environment.filters.update({
            'col': self._col,
            'val': self._val
        })

    def _sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        assert all(self._is_compatible_with(es.provider) and es.joins_all_compatible for es in self.provisions.values())
        return self._template_environment.get_template(self._base_sql).render(
            **{
                name: es.execute(ir_only=True, stats=False)
                for name, es in self.provisions.items()
            }
        )

    def _evaluate(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        df = self.db_client.query(self.get_sql(
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

    def _col(self, column_name):
        return self.dialect.column_encode(column_name)

    def _val(self, value):
        return self.dialect.value_encode(value)

    def _field_map(self, unit_type, measures, dimensions, joins):
        field_map = {'measures': {}, 'dimensions': {}}

        self_table_name = self._table_name(unit_type)

        for measure in measures:
            if not measure.external:
                if measure.via_name in field_map['measures']:
                    raise ValueError(measure.via_name)
                field_map['measures'][measure.via_name] = self.dialect.source_column_encode(self_table_name, measure.expr, measure.default)

        for dimension in dimensions:
            if not dimension.external:
                if dimension.via_name in field_map['dimensions']:
                    raise ValueError(dimension.via_name)
                field_map['dimensions'][dimension.via_name] = self.dialect.source_column_encode(self_table_name, dimension.expr, dimension.default)

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
        return isinstance(provider, self.__class__)


class SQLTableMeasureProvider(SQLMeasureProvider):

    def _sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        if len(self.identifiers) + len(self.dimensions) + len(self.measures) == 0:
            raise RuntimeError("No columns identified in table.")
        return self._template_environment.get_template(self.dialect.TEMPLATE_TABLE).render(
            table=self.name,
            measures=[m for m in measures if m != 'count' and not m.external],
            dimensions=[d for d in segment_by if not d.external and d not in measures],
        )


class SQLMetricImplementation(MetricImplementation):

    def __init__(self, sql, post_stats=True):
        self.sql = textwrap.dedent(sql) if sql else sql
        self.post_stats = post_stats

    def _is_compatible_with(self, strategy):
        return isinstance(strategy.provider, SQLMeasureProvider) and strategy.joins_all_compatible

    def evaluate(self, strategy, marginalise=None, ir_only=False, **opts):
        ir = self.get_ir(strategy, marginalise=marginalise, **opts)
        if ir_only:
            return ir
        return strategy.provider.db_client.query(ir)

    def get_ir(self, strategy, marginalise=None, **opts):
        return jinja2.Template(self.sql).render(
            measures=[m for m in strategy.measures if not m.private],
            segment_by=[d for d in strategy.segment_by if not d.private],
            marginalise=marginalise or [],
            provision=strategy.execute(ir_only=True, stats=self.post_stats),
            **opts
        )
