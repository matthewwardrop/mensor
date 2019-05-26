import pytest

from mensor.measures.structures.features import Dimension, Feature, Identifier, Measure
from mensor.measures.structures.resolved import ResolvedFeature


class TestFeature(object):

    @pytest.fixture
    def f1(self):
        return Feature('my_feature', expr='my_feature_col', default=True, desc="My feature.", shared=False, provider=None)

    @pytest.fixture
    def f2(self):
        return Feature('my_feature')

    @pytest.fixture
    def f3(self):
        return Feature('my_feature2')

    def test_attrs(self, f1):
        assert f1.name == 'my_feature'

    def test_invalid_name(self):
        with pytest.raises(ValueError):
            Feature('9asd')

    def test_feature_equality(self, f1, f2, f3):
        assert f1 == f2
        assert f1 != f3
        assert f1 == 'my_feature'
        assert f1 != 'my_feature2'
        assert hash(f1) == hash('my_feature')

    def test_feature_repr(self, f1):
        assert str(f1) == 'Feature<my_feature>'

    def test_feature_resolution(self, f1):
        assert isinstance(f1.resolve(), ResolvedFeature)


class TestIdentifier:

    @pytest.fixture
    def i(self):
        return Identifier('my_identifier', role='unique')

    @pytest.fixture
    def i2(self):
        return Identifier('my_identifier2', role='relation')

    def test_naming(self, i):
        assert i.name == 'my_identifier'

        with pytest.raises(ValueError):
            i.name = '9asd'

        with pytest.raises(ValueError):
            i.name = '!not_a_relation'

    def test_unique(self, i):
        assert i.is_primary == False
        assert i.is_unique == True
        assert i.is_relation == False

    def test_relation(self, i2):
        assert i2.name == 'my_identifier2'
        i2.name = '!relation'
        assert i2.name == '!relation'

        assert i2.is_primary == True
        assert i2.is_unique == True
        assert i2.is_relation == True

    def test_role(self, i, i2):
        assert i.role == 'unique'
        assert i2.role == 'relation'

    def test_matching(self):
        user = Identifier('user')
        guest = Identifier('user:guest')

        assert user.matches(user)
        assert user.matches(guest)
        assert not guest.matches(user)

        assert user.matches(user, reverse=True)
        assert not user.matches(guest, reverse=True)
        assert guest.matches(user, reverse=True)

    def test_repr(self, i, i2):
        assert str(i) == 'Identifier<*my_identifier>'
        assert str(i2) == 'Identifier<^my_identifier2(r)>'


class TestDimension:

    @pytest.fixture
    def d(self):
        return Dimension('my_dimension', partition=True, requires_constraint=True, shared=True)

    def test_dimension(self, d):
        assert d.partition == True
        assert d.requires_constraint == True
        assert d.shared == True

    def test_partition_sharing(self):
        with pytest.raises(ValueError):
            Dimension('my_dimension', partition=True)

    def test_repr(self, d):
        assert str(d) == 'Dimension<my_dimension>'


class TestMeasure:

    @pytest.fixture
    def m(self):
        return Measure('my_measure')

    def test_measure(self, m):
        assert m.distribution == 'normal'

    def test_repr(self, m):
        assert str(m) == 'Measure<my_measure>'
