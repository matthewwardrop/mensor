import jinja2

from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS

TEMPLATE = jinja2.Template("""
WITH
    base_query AS (
        {{base_sql|indent(width=8)}}
    )
    {%- for join in joins %}
    , {{quote}}{{join.name}}{{quote}} AS (
        {{join.object|indent(width=8)}}
    )
    {%- endfor %}
SELECT
    {%- for dimension in dimensions %}
    {% if loop.index0 > 0 %}, {% endif %}{{ dimension[0] }} AS {{ dimension[1] }}
    {%- endfor %}
    {%- for measure in measures %}
    {% if dimensions or loop.index0 > 0 %}, {% endif %}{{ measure }}
    {%- endfor %}
FROM base_query
{%- if joins|length > 0 %}
{%- for join in joins %}
JOIN {{quote}}{{join.name}}{{quote}} ON base_query.{{quote}}{{provider.resolve(join.left_on, kind='dimension').expr}}{{quote}} = {{quote}}{{join.name}}{{quote}}.{{quote}}{{join.right_on}}{{quote}}
{%- endfor %}
{%- endif %}
{%- if dimensions|length > 0 %}
GROUP BY
{%- for dimension in dimensions %}
{%- if loop.index0 > 0 %}, {% endif %}{{ dimension[0] }}
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


class SQLMeasureProvider(MeasureProvider):
    # TODO: Handle unit-aggregation

    def __init__(self, *args, sql=None, db_client=None, quote='"', **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self._sql = sql
        self.db_client = db_client
        self.quote = quote

        self.add_measure('count', distribution=None)

    @property
    def sql(self):
        return self._sql

    def _get_measures_sql(self, measures, joins):
        aggs = []
        quote = self.quote

        for measure in measures:
            if measure == 'count':
                aggs.append('SUM(1) AS {quote}count|count{quote}'.format(**locals()))
                continue
            if not measure.external and measure != "count":
                if measure.measure_agg == 'normal':
                    aggs.extend(
                        x.format(**locals())
                        for x in [
                            'SUM(base_query.{quote}{measure.expr}{quote}) AS {quote}{measure.via_name}|norm|sum{quote}',
                            'POWER(SUM(base_query.{quote}{measure.expr}{quote}), 2) AS {quote}{measure.via_name}|norm|sos{quote}',
                            'COUNT(base_query.{quote}{measure.expr}{quote}) AS {quote}{measure.via_name}|norm|count{quote}'
                        ]
                    )
                elif measure.measure_agg == 'count':
                    aggs.append(
                        'COUNT({measure.expr}) AS {quote}{measure.via_name}|count{quote}'.
                        format(**locals())
                    )
                else:
                    raise RuntimeError("Invalid target type: {}".format(measure.measure_agg))

        for join in joins:
            for measure in join.measures:
                if not measure.private:
                    if measure.measure_agg == 'normal':
                        suffixes = ['|norm|sum', '|norm|sos', '|norm|count']
                    elif measure.measure_agg == 'count':
                        suffixes = ['|count']
                    else:
                        raise RuntimeError("Invalid target type: {}".format(measure.measure_agg))
                    aggs.extend([
                        'SUM({quote}{join.name}{quote}.{quote}{measure.via_name}{suffix}{quote}) AS {quote}{measures[measure].via_name}{suffix}{quote}'.format(**locals())
                        for suffix in suffixes
                    ])

        return aggs

    def _get_dimensions_sql(self, dimensions, joins):

        dims = []
        quote = self.quote
        for dimension in dimensions:
            if not dimension.external:
                dims.append((
                    'base_query.{quote}{dimension.expr}{quote}'.format(**locals()),
                    '{quote}{dimension.via_name}{quote}'.format(**locals())
                ))

        for j in joins:
            for dimension in j.dimensions:
                if not dimension.private and dimension != j.right_on:
                    dims.append((
                        '{quote}{j.name}{quote}.{quote}{dimension.via_name}{quote}'.format(**locals()),
                        '{quote}{dimensions[dimension].via_name}{quote}'.format(**locals())
                    ))

        return dims

    def _get_ir(self, unit_type, measures=None, segment_by=None, where=None, joins=None, via=None, **opts):
        sql = TEMPLATE.render(
            base_sql=self.sql,
            provider=self,
            dimensions=self._get_dimensions_sql(segment_by, joins),
            measures=self._get_measures_sql(measures, joins),
            joins=joins,
            filter=' AND '.join(where) if where else ''
        )
        return sql

    def get_sql(self, *args, **kwargs):
        return self.get_ir(*args, **kwargs)

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
        return {
            AGG_METHODS.SUM: lambda x: "SUM({})".format(x),
            AGG_METHODS.MEAN: lambda x: "AVG({})".format(x),
            AGG_METHODS.SQUARE_SUM: lambda x: "SUM(POW({}, 2)".format(x),
            AGG_METHODS.COUNT: lambda x: "COUNT({})".format(x)
        }


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
