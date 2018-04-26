import numbers

import jinja2

from mensor.measures.context import CONSTRAINTS
from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS

# TODO: Consider using sqlalchemy to generate SQL
# TODO: Consider creating an option to produce SQL using WITH clauses
#       subqueries are safer, but perhaps less readable
TEMPLATE = jinja2.Template("""
SELECT
    {%- for dimension in dimensions %}
    {% if loop.index0 > 0 %}, {% endif %}{{ dimension }}
    {%- endfor %}
    {%- for measure in measures %}
    {% if dimensions or loop.index0 > 0 %}, {% endif %}{{ measure }}
    {%- endfor %}
FROM (
    {{ _sql |indent(width=4)}}
) "{{ provider.name }}_query"
{%- if joins|length > 0 %}
{%- for join in joins %}
JOIN  (
    {{join.object|indent(width=4)}}
) "{{join.name}}"
ON
{%- for field in join.left_on -%}
{% if loop.index0 > 0 %} AND{% endif %} "{{ provider.name }}_query"."{{provider.resolve(field, kind='dimension').expr}}" = "{{join.name}}"."{{join.right_on[loop.index0]}}"
{%- endfor -%}
{%- endfor %}
{%- endif %}
{%- if constraints %}
WHERE {{ constraints }}
{%- endif %}
{%- if groupby|length > 0 %}
GROUP BY
{%- for gb in groupby %}
{%- if positional_groupby %}
{%- if loop.index > 1 %},{% endif %} {{ gb }}
{%- else %}
    {% if loop.index > 1 %},{% endif %} {{ gb }}
{%- endif %}
{%- endfor %}
{%- endif %}
""".strip())

TEMPLATE_TABLE = jinja2.Template("""
SELECT
    {%- for identifier in identifiers %}
    {% if loop.index0 > 0 %}, {% endif %}{{ identifier.expr }}
    {%- endfor %}
    {%- for dimension in dimensions %}
    {% if loop.index0 > 0 or identifiers%}, {% endif %}{{ dimension.expr }}
    {%- endfor %}
    {%- for measure in measures %}
    {% if loop.index0 > 0 or identifiers or dimensions %}, {% endif %}{{ measure.expr }}
    {%- endfor %}
FROM {{table}}
""".strip())


# Dialects
class SQLDialect(object):

    QUOTE_COL = '"'
    QUOTE_STR = "'"

    POSITIONAL_GROUPBY = True

    AGG_METHODS = {
        AGG_METHODS.SUM: lambda x: "SUM({})".format(x),
        AGG_METHODS.MEAN: lambda x: "AVG({})".format(x),
        AGG_METHODS.SQUARE_SUM: lambda x: "SUM(POW({}, 2)".format(x),
        AGG_METHODS.COUNT: lambda x: "COUNT({})".format(x)
    }

    @classmethod
    def constraint_maps(cls):
        return {
            CONSTRAINTS.AND: lambda m, w, f: '({})'.format(' AND '.join(m(o, f) for o in w.operands)),
            CONSTRAINTS.OR: lambda m, w, f: '({})'.format(' OR '.join(m(o, f) for o in w.operands)),
            CONSTRAINTS.EQUALITY: lambda m, w, f: "{} = {}".format(f[w.field], cls.qv(w.value)),
            CONSTRAINTS.INEQUALITY_GT: lambda m, w, f: "{} > {}".format(f[w.field], cls.qv(w.value)),
            CONSTRAINTS.INEQUALITY_GTE: lambda m, w, f: "{} >= {}".format(f[w.field], cls.qv(w.value)),
            CONSTRAINTS.INEQUALITY_LT: lambda m, w, f: "{} < {}".format(f[w.field], cls.qv(w.value)),
            CONSTRAINTS.INEQUALITY_LTE: lambda m, w, f: "{} <= {}".format(f[w.field], cls.qv(w.value)),
        }

    @classmethod
    def qc(cls, col):
        "This method quotes columns appropriately."
        return '{quote}{col}{quote}'.format(quote=cls.QUOTE_COL, col=col)

    @classmethod
    def qv(cls, value):
        "This method quotes values appropriately."
        if isinstance(value, str):
            return '{quote}{value}{quote}'.format(quote=cls.QUOTE_STR, value=value)
        elif isinstance(value, numbers.Number):
            return str(value)
        raise ValueError("SQL backend does not support quoting objects of type: `{}`".format(type(value)))


class PrestoDialect(SQLDialect):

    POSITIONAL_GROUPBY = True


class HiveDialect(SQLDialect):

    QUOTE_COL = '`'
    POSITIONAL_GROUPBY = False


DIALECTS = {
    'presto': PrestoDialect,
    'hive': HiveDialect,
}


class SQLMeasureProvider(MeasureProvider):
    # TODO: Handle unit-aggregation

    def __init__(self, *args, sql=None, db_client=None, dialect='presto', **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self._base_sql = sql
        self.db_client = db_client
        self.dialect = DIALECTS[dialect]

        self.add_measure('count', shared=True, distribution=None)

    def _sql(*args, **opts):
        return self._base_sql

    def _dimension_map(self, dimensions, joins):
        field_map = {}
        for dimension in dimensions:
            if not dimension.external:
                field_map[dimension.via_name] = '"{}_query"."{}"'.format(self.name, dimension.expr)

        for join in joins:
            for dimension in join.dimensions:
                if dimension not in join.right_on:
                    field_map[dimension.as_via(join.unit_type)] = '"{}"."{}"'.format(join.name, dimension.via_name)

        return field_map

    def _get_measures_sql(self, measures, joins, stats, covariates):
        aggs = []
        if covariates:
            raise NotImplementedError('covariates is not yet implemented in SQL provider')

        for measure in measures:
            if measure == 'count':
                if stats:
                    aggs.append('SUM(1) AS "count|sum"')
                    aggs.append('SUM(1) AS "count|count"')
                else:
                    aggs.append('SUM(1) AS "count|raw"')
            elif not measure.external and not measure.private:
                for field_suffix, col_map in self._get_distribution_fields(measure.distribution if stats else DISTRIBUTIONS.RAW).items():
                    aggs.append(
                        '{col_op} AS "{field_name}{field_suffix}"'.format(
                            col_op=col_map('"{}_query".{}'.format(self.name, measure.expr)),
                            field_name=measure.via_name,
                            field_suffix=field_suffix
                        )
                    )

        for join in joins:
            for measure in join.measures:
                if not measure.private:
                    suffixes = list(self._get_distribution_fields(measure.distribution))
                    aggs.extend([
                        'SUM("{n}"."{m}{s}") AS "{o}{s}"'.format(n=join.name, m=measure.via_name, o=measures[measure].via_name, s=suffix)
                        for suffix in suffixes
                    ])

        return aggs

    def _get_dimensions_sql(self, dimensions, joins):
        dims = []
        for dimension in dimensions:
            if not dimension.external and not dimension.private:
                dims.append('"{n}_query"."{m}" AS "{o}"'.format(n=self.name, m=dimension.expr, o=dimension.via_name))

        for join in joins:
            for dimension in join.dimensions:
                if not dimension.private and dimension not in join.right_on:
                    dims.append('"{n}"."{m}" AS "{o}"'.format(
                        n=join.name, m=dimension.via_name, o=dimension.as_via(join.unit_type))
                    )

        return dims

    def _get_groupby_sql(self, dimensions, joins):

        dims = []

        count = 1
        for dimension in dimensions:
            if not dimension.external and not dimension.private:
                if self.dialect.POSITIONAL_GROUPBY:
                    dims.append(count)
                    count += 1
                else:
                    dims.append('"{n}_query"."{m}"'.format(n=self.name, m=dimension.expr))

        for j in joins:
            for dimension in j.dimensions:
                if not dimension.private and dimension not in j.right_on:
                    if self.dialect.POSITIONAL_GROUPBY:
                        dims.append(count)
                        count += 1
                    else:
                        dims.append('"{n}"."{m}"'.format(n=j.name, m=dimension.via_name))

        return dims

    def _get_where_sql(self, where, field_map):
        if where is None:
            return None
        return self._constraint_map(where.kind)(self._get_where_sql, where, field_map)

    def _get_ir(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        field_map = self._dimension_map(segment_by, joins)
        sql = TEMPLATE.render(
            _sql=self._sql(unit_type=unit_type, measures=measures, segment_by=segment_by, where=where, joins=joins, stats=stats, covariates=covariates, **opts),
            provider=self,
            dimensions=self._get_dimensions_sql(segment_by, joins),
            measures=self._get_measures_sql(measures, joins, stats, covariates),
            groupby=self._get_groupby_sql(segment_by, joins),
            joins=joins,
            constraints=self._get_where_sql(where, field_map),
            positional_groupby=self.dialect.POSITIONAL_GROUPBY
        )
        if self.dialect.QUOTE_COL != '"':
            sql = sql.replace('"', self.dialect.QUOTE_COL)
        return sql

    def get_sql(self, *args, **kwargs):
        return self.get_ir(*args, **kwargs)

    def _evaluate(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        return self.db_client.query(self.get_sql(
            unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            joins=joins,
            **opts
        ))

    @property
    def _agg_methods(self):
        return self.dialect.AGG_METHODS

    @property
    def _constraint_maps(self):
        return self.dialect.constraint_maps()

    def _is_compatible_with(self, provider):
        return isinstance(provider, self.__class__)


class SQLTableMeasureProvider(SQLMeasureProvider):

    def _sql(self, measures, segment_by, where, joins):
        if len(self.identifiers) + len(self.dimensions) + len(self.measures) == 0:
            raise RuntimeError("No columns identified in table.")
        return TEMPLATE_TABLE.render(
            table=self.name,
            identifiers=self.identifiers,
            measures=[m for m in self.measures if m != 'count'],
            dimensions=self.dimensions
        )
