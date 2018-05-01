from abc import abstractmethod, abstractproperty


class Metric(object):

    def __init__(self, name):
        self.name = name

    def evaluate(self, mdf):
        return self._evaluate(mdf.set_index(mdf.dimensions))

    @abstractmethod
    def _evaluate(self, mdf):
        raise NotImplementedError

    @abstractproperty
    def requires(self):
        raise NotImplementedError

    @property
    def evaluations(self):
        measures = {}
        for measure in self.requires:
            unit_type, measure = measure.split('/', 1)
            if unit_type not in measures:
                measures[unit_type] = set()
            measures[unit_type].add(measure)
        return measures


class RatioMetric(Metric):
    # TODO: account for covariance

    def __init__(self, name, numerator, denominator):
        Metric.__init__(self, name)
        self.numerator = numerator
        self.denominator = denominator

    def _evaluate(self, mdf):
        return mdf[self.numerator] / mdf[self.denominator]

    @property
    def requires(self):
        return [self.numerator.split('|')[0], self.denominator.split('|')[0]]
