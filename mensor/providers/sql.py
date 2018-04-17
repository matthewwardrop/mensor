import jinja2

from mensor.measures.provider import MeasureProvider

TEMPLATE_MEASURES = jinja2.Template("""
WITH
    base_query AS (
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
JOIN "{{join.name}}" ON base_query."{{provider.resolve(join.left_on, kind='dimension').expr}}" = "{{join.name}}"."{{join.right_on}}"
{%- endfor %}
{%- endif %}
{%- if dimensions|length > 0 %}
GROUP BY
{%- for dimension in dimensions %}
{%- if loop.index > 1 %},{% endif %} {{ loop.index }}
{%- endfor %}
{%- endif %}
""".strip())


class SQLMeasureProvider(MeasureProvider):

    def __init__(self, *args, sql=None, db_client=None, **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self.sql = sql
        self.db_client = db_client

        self.add_measure('count', measure_agg='count')

    def _get_measures_sql(self, measures, join):
        aggs = []

        for measure in measures:
            if not measure.external:
                if measure.measure_agg == 'normal':
                    agg = ['SUM(base_query."{m}") AS "{o}|norm|sum"',
                           'POWER(SUM(base_query."{m}"), 2) AS "{o}|norm|sos"',
                           'COUNT(base_query."{m}") AS "{o}|norm|count"']
                    aggs.extend(x.format(m=measure.expr, o=measure.via_name) for x in agg)
                elif measure.measure_agg == 'count':
                    aggs.append('COUNT({m}) AS "{o}|count"'.format(m=measure.expr, o=measure.via_name))
                else:
                    raise RuntimeError("Invalid target type: {}".format(measure.measure_agg))

        for j in join:
            for measure in j.measures:
                if not measure.private:
                    if measure.measure_agg == 'normal':
                        suffixes = ['|norm|sum', '|norm|sos', '|norm|count']
                    elif measure.measure_agg == 'count':
                        suffixes = ['|count']
                    else:
                        raise RuntimeError("Invalid target type: {}".format(measure.measure_agg))
                    aggs.extend([
                        'SUM("{n}"."{m}{s}") AS "{o}{s}"'.format(n=j.name, m=measure.via_name, o=measures[measure].via_name, s=suffix)
                        for suffix in suffixes
                    ])

        return aggs

    def _get_dimensions_sql(self, dimensions, join):

        dims = []

        for dimension in dimensions:
            if not dimension.external:
                dims.append('base_query."{m}" AS "{o}"'.format(m=dimension.expr, o=dimension.via_name))

        for j in join:
            for dimension in j.dimensions:
                if not dimension.private and dimension != j.right_on:
                    dims.append('"{n}"."{m}" AS "{o}"'.format(n=j.name, m=dimension.via_name, o=dimensions[dimension].via_name))

        return dims

    def _get_ir(self, unit_type, measures=None, segment_by=None, where=None, join=None, via=None, **opts):
        sql = TEMPLATE_MEASURES.render(
            base_sql=self.sql,
            provider=self,
            dimensions=self._get_dimensions_sql(segment_by, join),
            measures=self._get_measures_sql(measures, join),
            joins=join,
            filter=' AND '.join(where) if where else ''
        )
        return sql

    def get_sql(self, *args, **kwargs):
        return self.get_ir(*args, **kwargs)

    def _evaluate(self, unit_type, measures=None, segment_by=None, where=None, join=None, **opts):
        return self.db_client.query(self.get_sql(
            unit_type,
            measures=measures,
            segment_by=segment_by,
            where=where,
            join=join,
            **opts
        ))
