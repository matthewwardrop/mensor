from mensor.utils import SequenceMap

from .features import Feature
from .resolved import ResolvedFeature


class ResolvedFeatureCandidates(ResolvedFeature):

    __slots__ = ()

    def __init__(self, features, **props):
        super().__init__(features, **props)

    def __getattr__(self, attr):
        raise AttributeError(attr)

    @property
    def name(self):
        if self.feature:
            return self.feature[0].name
        return None

    @property
    def desc(self):
        if self.feature:
            self.feature[0].desc
        return None

    @property
    def features(self):
        return self.feature

    def append(self, feature):
        if isinstance(feature, ResolvedFeature):
            feature = feature.feature
        assert isinstance(feature, Feature)
        if self.name is not None:
            assert feature.name == self.name
        self.features.append(feature)

    def extend(self, features):
        for feature in features:
            self.append(feature)

    def for_provider(self, provider):
        for feature in self.features:
            if feature.provider == provider:
                return feature.resolve(**self.props)
        raise ValueError(f"No feature found for provider '{provider}'.")

    @property
    def providers(self):
        return SequenceMap(f.provider for f in self.feature)

    def __repr__(self):
        return f"ResolvedFeatureCandidates<{self.name}>"
