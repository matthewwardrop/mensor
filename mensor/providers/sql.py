import numbers
import textwrap

import jinja2

from mensor.measures.context import CONSTRAINTS
from mensor.measures.provider import MeasureProvider
from mensor.measures.types import AGG_METHODS

# TODO: Consider using sqlalchemy to generate SQL
# TODO: Consider creating an option to produce SQL using WITH clauses
#       subqueries are safer, but perhaps less readable


# Dialects
class SQLDialect(object):

    QUOTE_COL = '"'
    QUOTE_STR = "'"

    POSITIONAL_GROUPBY = True

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
            {% if loop.index0 > 0 %}AND {% endif %}{{ field_map[field] }} = {{ join.name | col }}.{{ join.right_on[loop.index0] | col }}
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
    """).strip()

    TEMPLATE_STATS = textwrap.dedent("""
        SELECT

        FROM (
            {{ base_sql }}
        )
    """).strip()

    TEMPLATE_TABLE = textwrap.dedent("""
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
            CONSTRAINTS.EQUALITY: lambda w, f, m: "{} = {}".format(f[w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_GT: lambda w, f, m: "{} > {}".format(f[w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_GTE: lambda w, f, m: "{} >= {}".format(f[w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_LT: lambda w, f, m: "{} < {}".format(f[w.field], ve(w.value)),
            CONSTRAINTS.INEQUALITY_LTE: lambda w, f, m: "{} <= {}".format(f[w.field], ve(w.value)),
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
        raise ValueError("SQL dialect `{}` does not support quoting objects of type: `{}`".format(cls, type(value)))

    # TODO?
    # @classmethod
    # def value_decode(cls, value):
    #     return value


class PrestoDialect(SQLDialect):

    POSITIONAL_GROUPBY = True


class HiveDialect(SQLDialect):

    QUOTE_COL = '`'
    POSITIONAL_GROUPBY = False

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
    # TODO: Handle unit-aggregation

    def __init__(self, *args, sql=None, db_client=None, dialect='presto', **kwargs):
        assert db_client is not None, "Must specify an (Omniduct-compatible) database client."

        MeasureProvider.__init__(self, *args, **kwargs)
        self._base_sql = sql
        self.db_client = db_client
        self.dialect = DIALECTS[dialect]

        self.add_measure('count', shared=True, distribution=None)

        self._template_environment = jinja2.Environment(loader=jinja2.FunctionLoader(lambda x: x))
        self._template_environment.filters.update({
            'col': self._col,
            'val': self._val
        })

    def _sql(self, unit_type, measures, segment_by, where, joins, stats, covariates, **opts):
        return self._base_sql

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
            positional_groupby=self.dialect.POSITIONAL_GROUPBY,
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
        field_map = {}

        self_table_name = self._table_name(unit_type)

        for measure in measures:
            if not measure.external:
                field_map[measure.via_name] = '{}.{}'.format(self._col(self_table_name), self._col(measure.expr))

        for dimension in dimensions:
            if not dimension.external:
                field_map[dimension.via_name] = '{}.{}'.format(self._col(self_table_name), self._col(dimension.expr))

        for join in joins:
            for measure in join.measures:
                field_map[measure.as_via(join.join_prefix).via_name] = '{}.{}'.format(self._col(join.name), self._col(measure.fieldname(role='measure')))
            for dimension in join.dimensions:
                field_map[dimension.as_via(join.join_prefix).via_name] = '{}.{}'.format(self._col(join.name), self._col(dimension.fieldname(role='dimension')))

        return field_map

    def _get_dimensions_sql(self, field_map, dimensions):
        dims = []
        for dimension in dimensions:
            if not dimension.private:
                dims.append(
                    '{} AS {}'.format(
                        field_map[dimension.via_name],
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
                                col_op=col_map('1' if measure == 'count' else field_map[measure.via_name]),
                                f=self._col(fieldname),
                            )
                        )

        return aggs

    def _get_groupby_sql(self, field_map, dimensions):
        groupby = []

        count = 1
        for dimension in dimensions:
            if not dimension.private:
                if self.dialect.POSITIONAL_GROUPBY:
                    groupby.append(count)
                    count += 1
                else:
                    groupby.append(field_map[dimension.via_name])

        return groupby

    def _get_where_sql(self, field_map, where):
        if where is None:
            return None
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
            identifiers=self.identifiers,
            measures=[m for m in self.measures if m != 'count'],
            dimensions=self.dimensions
        )
