# Output types

import numpy as np
import pandas as pd
from uncertainties.unumpy import uarray

from ..registries import global_stats_registry

__all__ = ['EvaluatedMeasures']


class EvaluatedMeasures(object):

    @classmethod
    def for_measures(cls, evaluations, stats_registry=None):
        if isinstance(evaluations, EvaluatedMeasures):
            return evaluations
        elif isinstance(evaluations, pd.DataFrame):
            return cls(evaluations, stats_registry=stats_registry)
        raise RuntimeError("Invalid measures type: {}".format(evaluations.__class__.__name__))

    def __init__(self, evaluations, stats_registry=None):
        self._evaluations = evaluations
        self._stats_registry = stats_registry or global_stats_registry

    @property
    def raw(self):
        return self._evaluations

    def to_frame(self, *measures, keep_raw_fields=False):
        measures = list(measures)

        if not measures:
            measures = self.measures

        if keep_raw_fields:
            measures += self.measure_fields

        df = self.raw
        for measure in measures:
            if measure not in df.columns and measure in self.measures:
                df = df.assign(**{measure: self._get_measure(measure)})

        if len(self.dimensions):
            return df.set_index(self.dimensions).sort_index()[measures]
        return df[measures].T[0]

    @property
    def columns(self):
        return self.raw.columns

    @property
    def measure_fields(self):
        return [col for col in self._evaluations.columns if '|' in col]

    @property
    def measures(self):
        return list(set(field.split('|')[0] for field in self.measure_fields))

    @property
    def dimensions(self):
        return [col for col in self._evaluations.columns if '|' not in col]

    def _get_measure_distribution(self, name):
        for field in self.measure_fields:
            if field.startswith(name):
                if len(field.split('|')) == 3:
                    return field.split('|')[1].lower()
                return None

    def _get_measure_distribution_fields(self, name):
        distribution = self._get_measure_distribution(name)
        return self.raw[[
            '{}|{}'.format(name, field) if distribution is None else '{}|{}|{}'.format(name, distribution, field)
            for field in self._stats_registry.distributions.get_stats(distribution)
        ]]

    def _get_measure(self, name):

        # Check if measure exists
        if name not in self.measures:
            raise KeyError(name)

        distribution = self._get_measure_distribution(name)
        distribution_fields = self._get_measure_distribution_fields(name).values.transpose()

        stats = self._stats_registry.distributions.get_scipy_repr(distribution)

        if isinstance(stats, tuple):
            model = stats[0]
            if model:
                params = {
                    param: f(*distribution_fields) for param, f in stats[1].items()
                }
                return pd.Series(uarray(model.mean(**params), model.std(**params)), name=name, index=self.raw.index)
        elif stats:
            return pd.Series(stats(*distribution_fields), name=name, index=self.raw.index)

        return distribution_fields[0]  # If no stats, return raw sum field

    # Allow getting measures by distribution stats
    def __getitem__(self, name):
        if isinstance(name, list):
            return self.to_frame(*name)
        else:
            return self.to_frame(name)[name]

    def segmentby(self, segment_by=None):
        segment_by = segment_by or []
        if len(segment_by):
            return EvaluatedMeasures(
                self._evaluations
                .groupby(segment_by)
                [self.measure_fields]
                .sum()
            )
        return EvaluatedMeasures(self._evaluations[self.measure_fields].sum())

    def query(self, *args, **kwargs):
        # TODO: Replace with `where`, and use Pandas constraint -> pandas expr filters.
        return EvaluatedMeasures(self.raw.query(*args, **kwargs))

    def __repr__(self):
        return self.to_frame().__repr__()

    def _repr_html_(self):
        df = self.to_frame()
        if hasattr(df, '_repr_html_'):
            return df._repr_html_()
        raise NotImplementedError

    def add_prefix(self, prefix):
        return EvaluatedMeasures(self._evaluations.add_prefix(prefix))


def quantilesofscores(self, as_weights=False, *, pre_sorted=False, sort_fields=None):
    idx = self.index.copy()
    s = self
    if not pre_sorted:
        s = s.sort_values()
    if as_weights:
        return (s.cumsum() / s.sum()).reindex(idx)
    return (pd.Series(np.ones(len(s)).cumsum(), index=s.index) / len(s)).reindex(idx)


pd.Series.quantilesofscores = quantilesofscores
