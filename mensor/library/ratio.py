import functools

from mensor.metrics import Metric
from mensor.backends.sql import SimpleSQLMetricImplementation
from mensor.measures.types import DISTRIBUTIONS


def get_sum_and_variance(strategy, measure):
    measure = strategy.measures[measure]
    col = strategy.provider._col

    if measure.distribution == DISTRIBUTIONS.NORMAL:
        measure_sum = col('{}|normal|sum'.format(measure.via_name))
        measure_variance = '(1.0 * {sos} - 1.0 * POW({sum}, 2) / {count})'.format(
            sum=col('{}|normal|sum'.format(measure.via_name)),
            sos=col('{}|normal|sos'.format(measure.via_name)),
            count=col('{}|normal|count'.format(measure.via_name))
        )
    elif measure.distribution == DISTRIBUTIONS.BINOMIAL:
        measure_sum = col('{}|binomial|sum'.format(measure.via_name))
        measure_variance = '(1.0 * {sum} * (1.0 - {sum} / {count}))'.format(
            sum=col('{}|normal|sum'.format(measure.via_name)),
            count=col('{}|normal|count'.format(measure.via_name))
        )
    elif measure.distribution == DISTRIBUTIONS.NONE:
        measure_sum = col('{}|sum'.format(measure.via_name))
        measure_variance = '0.0'
    else:
        raise RuntimeError("SQL-backed RatioMetrics cannot yet interpret {} distributions.".format(measure.distribution))

    return measure_sum, measure_variance


def get_sql_ratio_lines(name, strategy, numerator, denominator, **opts):
    col = strategy.provider._col

    num_sum, num_var = get_sum_and_variance(strategy, numerator)
    den_sum, den_var = get_sum_and_variance(strategy, denominator)

    # TODO: Include covariance

    ratio_mean = "1.0 * {num_sum} / {den_sum}".format(num_sum=num_sum, den_sum=den_sum)
    ratio_variance = "1.0 * POW({num_sum}, 2) / POW({den_sum}, 2) * ( {num_var} / POW({num_sum}, 2) + {den_var} / POW({den_sum}, 2) )".format(
        num_sum=num_sum,
        num_var=num_var,
        den_sum=den_sum,
        den_var=den_var,
    )

    return [
        "{} AS {}".format(ratio_mean, col("{}|normal|mean".format(name))),
        "{} AS {}".format(ratio_variance, col("{}|normal|variance".format(name))),
    ]


def get_sql_measure_lines(name, strategy, measure, mean, **opts):
    col = strategy.provider._col

    measure_sum, measure_var = get_sum_and_variance(strategy, measure)

    if mean:
        count, _ = get_sum_and_variance(strategy, 'count')

        return [
            "1.0 * {measure_sum} / {count} AS {col}".format(
                measure_sum=measure_sum,
                count=count,
                col=col("{}|normal|mean".format(name))
            ),
            "1.0 * {measure_var} / {count} AS {col}".format(
                measure_var=measure_var,
                count=count,
                col=col("{}|normal|variance".format(name))
            )
        ]
    else:
        return [
            "{measure_sum} AS {col}".format(
                measure_sum=measure_sum,
                col=col("{}|normal|mean")
            ),
            "{measure_var} AS {col}".format(
                measure_var=measure_var,
                col=col("{}|normal|variance")
            )
        ]


class MeasureMetric(Metric):

    REGISTRY_KEYS = ['measure']

    def _init(self, measure, mean=False):
        self.opts.add_option('measure', 'The measure to be extracted.', required=False, default=measure)
        self.opts.add_option('mean', 'Whether to take the mean of the measure over unit type.', required=False, default=mean)

        self.add_implementation(SimpleSQLMetricImplementation(metrics_callback=get_sql_measure_lines))

    @property
    def required_measures(self):
        return [self.opts['measure']] + (['count'] if self.opts['mean'] else [])


class RatioMetric(Metric):

    REGISTRY_KEYS = ['ratio']

    def _init(self, numerator, denominator):
        self.opts.add_option('numerator', 'The numerator of the ratio.', required=False, default=numerator)
        self.opts.add_option('denominator', 'The denominator of the ratio.', required=False, default=denominator)

        self.add_implementation(SimpleSQLMetricImplementation(metrics_callback=get_sql_ratio_lines))

    @property
    def required_measures(self):
        return [self.opts['numerator'], self.opts['denominator']]
