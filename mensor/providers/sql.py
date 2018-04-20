import jinja2

from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS

# TODO: Consider using sqlalchemy to generate SQL

TEMPLATE = jinja2.Template("""
WITH
    "base_query" AS (
        {{base_sql|indent(width=8)}}
    )
    {%- for join in joins %}
    , "{{join.name}}" AS (
        {{join.object|indent(width=8)}}
    )
    {%- endfor %}
SELECT
    {%- for dimension in dimensions %}
    {% if loop.index0 > 0 %}, {% endif %}{{ dimension }}
    {%- endfor %}
    {%- for measure in measures %}
    {% if dimensions or loop.index0 > 0 %}, {% endif %}{{ measure }}
    {%- endfor %}
FROM base_query
{%- if joins|length > 0 %}
{%- for join in joins %}
JOIN "{{join.name}}" ON
{%- for field in join.left_on -%}
{% if loop.index0 > 0 %} AND{% endif %} "base_query"."{{provider.resolve(field, kind='dimension').expr}}" = "{{join.name}}"."{{join.right_on[loop.index0]}}"
{%- endfor -%}
{%- endfor %}
{%- endif %}
{%- if groupby|length > 0 %}
GROUP BY
{%- for gb in groupby %}
{%- if positional_groupby %}
{%- if loop.index > 1 %},{% endif %} {{ gb }}
{%- else %}
    {% if loop.index > 1 %},{% endif %} {{gb}}
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

    MEASURE_AGG_METHODS = {
        AGG_METHODS.SUM: lambda x: "SUM({})".format(x),
        AGG_METHODS.MEAN: lambda x: "AVG({})".format(x),
        AGG_METHODS.SQUARE_SUM: lambda x: "SUM(POW({}, 2)".format(x),
        AGG_METHODS.COUNT: lambda x: "COUNT({})".format(x)
    }


class PrestoDialect(SQLDialect):

    POSITIONAL_GROUPBY = True


DIALECTS = {
    'presto': PrestoDialect
}


class SQLMeasureProvider(MeasureProvider):
    # TODO: Handle unit-aggregation

    def __init__(self, *args, sql=None, db_client=None, dialect='presto', **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self._sql = sql
        self.db_client = db_client
        self.dialect = DIALECTS[dialect]

        self.add_measure('count', distribution=None)

    @property
    def sql(self):
        return self._sql

    def _get_measures_sql(self, measures, joins):
        aggs = []

        for measure in measures:
            if measure == 'count':
                aggs.append('SUM(1) AS "count|sum"')
                aggs.append('SUM(1) AS "count|count"')
            elif not measure.external and not measure.private:
                for field_suffix, col_map in self._get_distribution_fields(measure.distribution).items():
                    aggs.append(
                        '{col_op} AS "{field_name}{field_suffix}"'.format(
                            col_op=col_map('"base_query".{}'.format(measure.expr)),
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
                dims.append('"base_query"."{m}" AS "{o}"'.format(m=dimension.expr, o=dimension.via_name))

        for j in joins:
            for dimension in j.dimensions:
                if not dimension.private and dimension not in j.right_on:
                    dims.append('"{n}"."{m}" AS "{o}"'.format(n=j.name, m=dimension.via_name, o=dimensions[dimension].via_name))

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
                    dims.append('"base_query"."{m}"'.format(m=dimension.expr))

        for j in joins:
            for dimension in j.dimensions:
                if not dimension.private and dimension not in j.right_on:
                    if self.dialect.POSITIONAL_GROUPBY:
                        dims.append(count)
                        count += 1
                    else:
                        dims.append('"{n}"."{m}"'.format(n=j.name, m=dimension.via_name))

        return dims

    def _get_ir(self, unit_type, measures=None, segment_by=None, where=None, joins=None, via=None, **opts):
        sql = TEMPLATE.render(
            base_sql=self.sql,
            provider=self,
            dimensions=self._get_dimensions_sql(segment_by, joins),
            measures=self._get_measures_sql(measures, joins),
            groupby=self._get_groupby_sql(segment_by, joins),
            joins=joins,
            filter=' AND '.join(where) if where else '',
            positional_groupby=self.dialect.POSITIONAL_GROUPBY
        )
        return sql

    def get_sql(self, *args, **kwargs):
        sql = self.get_ir(*args, **kwargs)
        if self.dialect.QUOTE_COL != '"':
            sql = sql.replace('"', self.dialect.QUOTE_COL)
        return sql

    def _evaluate(self, unit_type, measures=None, segment_by=None, where=None, joins=None, **opts):
        return self.db_client.query(self.get_sql(
            unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            joins=joins,
            **opts
        ))

    @property
    def _measure_agg_methods(self):
        return self.dialect.MEASURE_AGG_METHODS


class SQLTableMeasureProvider(SQLMeasureProvider):

    def __init__(self, *args, **kwargs):
        SQLMeasureProvider.__init__(self, *args, **kwargs)

    @property
    def sql(self):
        if len(self.identifiers) + len(self.dimensions) + len(self.measures) == 0:
            raise RuntimeError("No columns identified in table.")
        return TEMPLATE_TABLE.render(
            table=self.name,
            identifiers=self.identifiers,
            measures=[m for m in self.measures if m != 'count'],
            dimensions=self.dimensions
        )
