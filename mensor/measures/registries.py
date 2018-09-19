from collections import OrderedDict

import numpy as np
import scipy.stats


# Statistics Registry base classe

class Registry:

    def __init__(self, fallback=None):
        self._fallback = fallback
        self.__store = {}

    def _store(self, *keys, value=None):
        store = self.__store
        for key in keys[:-1]:
            if key not in store:
                store[key] = {}
            store = store[key]
        store[keys[-1]] = value

    def _fetch(self, *keys):
        try:
            store = self.__store
            for key in keys[:-1]:
                store = store[key]
            return store[keys[-1]]
        except KeyError:
            if self._fallback:
                return self._fallback._fetch(*keys)
            raise

    def _keys(self, *keys):
        try:
            store = self.__store
            for key in keys:
                store = store[key]
            out = list(store)
        except KeyError:
            out = []
        if self._fallback:
            out += self._fallback._keys(*keys)
        return out

    def register(self):
        raise NotImplementedError


class AggregationRegistry(Registry):

    def register(self, name, backend, agg):
        return self._store(name, backend, value=agg)

    def get(self, name, backend):
        return self._fetch(name, backend)

    def list(self):
        return self._keys()

    def list_backends(self, name):
        return self._keys(name)

    def for_backend(self, name):
        return {
            agg: self._fetch(agg, backend)
            for agg in self._keys()
            for backend in self._keys(agg)
            if backend == name
        }

    def for_provider(self, provider):
        if not provider.REGISTRY_KEYS:
            raise RuntimeError("Provider {} is not an instance of any registered backend.".format(provider))
        return self.for_backend(provider.REGISTRY_KEYS[0])


class TransformRegistry(Registry):

    def register(self, name, backend, transform):
        return self._store(name, backend, value=transform)

    def get(self, name, backend):
        return self._fetch(name, backend)

    def list(self):
        return self._keys()

    def list_backends(self, name):
        return self._keys(name)

    def for_backend(self, name):
        return {
            agg: self._fetch(agg, backend)
            for agg in self._keys()
            for backend in self._keys(agg)
            if backend == name
        }

    def for_provider(self, provider):
        if not provider.REGISTRY_KEYS:
            raise RuntimeError("Provider {} is not an instance of any registered backend.".format(provider))
        return self.for_backend(provider.REGISTRY_KEYS[0])


class DistributionRegistry(Registry):

    def register(self, name, stats, scipy_class, scipy_params):
        return self._store(name, value={
            'stats': stats,
            'scipy_class': scipy_class,
            'scipy_params': scipy_params
        })

    def get(self, name):
        return self._fetch(name)

    def get_stats(self, name):
        return self.get(name)['stats']

    def get_scipy_repr(self, name):
        dist = self.get(name)
        return (dist['scipy_class'], dist['scipy_params'])

    def list(self):
        return self._keys()


class StatsRegistry:

    def __init__(self, aggregations=None, transforms=None, distributions=None, fallback=None):
        self.aggregations = aggregations or AggregationRegistry(fallback=fallback.aggregations if fallback else None)
        self.transforms = transforms or TransformRegistry(fallback=fallback.transforms if fallback else None)
        self.distributions = distributions or DistributionRegistry(fallback=fallback.distributions if fallback else None)

    def distribution_for_provider(self, distribution, provider):
        backend = provider.REGISTRY_KEYS[0]
        fields = self.distributions.get_stats(distribution)
        return OrderedDict([
            (name, self.aggregations.get(agg, backend))
            for name, agg in fields.items()
        ])


# Create and populate global stats registry

global_stats_registry = StatsRegistry()
register_distn = global_stats_registry.distributions.register

# Raw distribution (distribution thrown away or manually recorded)
register_distn(
    name=None,
    stats=OrderedDict([
        ('sum', 'sum'),
        ('count', 'count')
    ]),
    scipy_class=None,
    scipy_params=None
)

# Normal
register_distn(
    name='normal',
    stats=OrderedDict([
        ('sum', 'sum'),
        ('sos', 'sos'),
        ('count', 'count')
    ]),
    scipy_class=scipy.stats.distributions.norm,
    scipy_params={
        'loc': lambda sum, sos, count: sum,
        'scale': lambda sum, sos, count: np.sqrt(sos - sum**2 / (count - 1))
    }
)

# Binomial
register_distn(
    name='binomial',
    stats=OrderedDict([
        ('sum', 'sum'),
        ('count', 'count')
    ]),
    scipy_class=scipy.stats.distributions.binom,
    scipy_params={
        'n': lambda sum, count: count,
        'p': lambda sum, count: sum / count
    }
)
