import pytest

from mensor.measures import MutableMeasureProvider


@pytest.fixture
def mp():
    mp = MutableMeasureProvider()
    mp.add_identifier("account", role="primary")
    mp.add_dimension("asd")
    return mp


def test_hierarchical_unit_types(mp):
    assert mp.identifier_for_unit("account:guest") == "account"

    # Test resolution
    assert mp.resolve("account:guest", "asd") == "asd"
    assert mp.resolve("account:guest", "account:guest").name == "account"

    with pytest.raises(Exception):
        mp.resolve("account:guest", "account")
