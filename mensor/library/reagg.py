from mensor.backends.sql import SQLMetricImplementation
from mensor.constraints import Constraint
from mensor.metrics import Metric


class RenormReaggMetric(Metric):

    REGISTRY_KEYS = ['renorm_reagg']

    def _init(self, measure, renormalise_over, window_dimensions):
        self.opts.add_option('measure', 'The measure to be extracted.', required=False, default=measure)
        self.opts.add_option('renormalise_over', '...', required=False, default=renormalise_over)
        self.opts.add_option('window_dimensions', '...', required=False, default=window_dimensions)

        self.implementations['sql'] = (
            SQLMetricImplementation(
                """
                SELECT
                    *
                    , ROUND(SUM("{{ name }}|normal|sum")
                      {%- if segment_by|length - renormalise_over|length > 0 %} OVER (PARTITION BY {% for d in segment_by if d not in marginalise and d not in renormalise_over %}{% if loop.index > 1 %}, {% endif %}{{ d.via_name }}{% endfor %})) {% endif -%}
                      AS "{{ name }}|normal|count"
                FROM (
                    SELECT
                        {%- with ns=namespace(_cnt=0) %}
                        {%- for dimension in segment_by if dimension not in marginalise %}
                        {% if loop.index0 > 0 %}, {% endif %}{{dimension.name}}
                        {%- set ns._cnt = ns._cnt + 1 %}
                        {%- endfor %}
                        {% if ns._cnt > 0 %}, {% endif %}SUM({{ measure }}) AS "{{ name }}|normal|sum"
                        , SUM(POW({{ measure }}, 2)) AS "{{ name }}|normal|sos"
                        {%- endwith %}
                    FROM (
                        SELECT
                            *
                            , 1.0 * "{{ measure }}|raw" / (SUM("{{ measure }}|raw")
                                OVER (PARTITION BY {% for d in segment_by if d in window_dimensions or (d not in renormalise_over and d not in marginalise) %}{% if loop.index > 1 %}, {% endif %}{{ d.via_name }}{% endfor %})) AS {{ measure }}
                        FROM (
                            {{ provision | indent(width=12) }}
                        )
                    )
                    GROUP BY
                        {%- for dimension in segment_by if dimension not in marginalise and not dimension.private %}
                        {% if loop.index0 > 0 %}, {% endif %}{{dimension.via_name}}
                        {%- endfor %}
                )
                """,
                post_stats=False
            )
        )

    def _required_measures(self, **opts):
        return [opts['measure']]

    def _required_segmentation(self, **opts):
        return []

    def _required_constraints(self, **opts):
        return []

    def _marginal_dimensions(self, **opts):
        return opts['renormalise_over'] + opts['window_dimensions']
