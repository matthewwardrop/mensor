import os

import numpy as np
import pandas as pd
import pytest

from mensor.backends.pandas import PandasMeasureProvider
from mensor.measures import MetaMeasureProvider


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
N = 1000

with open(os.path.join(DATA_DIR, "geography_names.txt")) as f:
    GEOGRAPHY_NAMES = [name[:-1] for name in f.readlines()]

with open(os.path.join(DATA_DIR, "person_names.txt")) as f:
    PEOPLE_NAMES = [name[:-1] for name in f.readlines()]


@pytest.fixture
def df_geographies():
    return (
        pd.DataFrame(
            {
                "id_geography": range(len(GEOGRAPHY_NAMES)),
                "name": GEOGRAPHY_NAMES,
                "population": np.random.randint(1e6, size=len(GEOGRAPHY_NAMES)),
                "ds": "2018-01-01",
            }
        )
        .reset_index()
        .rename(columns={"index": "id"})
    )


@pytest.fixture
def df_people():
    return (
        pd.DataFrame(
            {
                "id_person": range(N),
                "name": np.random.choice(PEOPLE_NAMES, N),
                "age": np.random.randint(100, size=N),
                "id_geography": np.random.randint(len(GEOGRAPHY_NAMES), size=1000),
                "ds": "2018-01-01",
            }
        )
        .reset_index()
        .rename(columns={"index": "id"})
    )


@pytest.fixture
def df_transactions():
    return (
        pd.DataFrame(
            {
                "id_seller": np.random.randint(1000, size=2 * N),
                "id_buyer": np.random.randint(1000, size=2 * N),
                "value": np.random.randint(1000, size=2 * N),
                "ds": "2018-01-01",
            }
        )
        .reset_index()
        .rename(columns={"index": "id"})
    )


@pytest.fixture
def geographies(df_geographies):
    return (
        PandasMeasureProvider(name="geographies", data=df_geographies)
        .add_identifier("geography", expr="id_geography", role="foreign")
        .add_dimension("name")
        .add_measure("population")
        .add_partition("ds")
    )


@pytest.fixture
def people(df_people):
    return (
        PandasMeasureProvider(name="people", data=df_people)
        .add_identifier("person", expr="id", role="primary")
        .add_identifier("geography", expr="id_country", role="foreign")
        .add_dimension("name")
        .add_measure("age")
    )


@pytest.fixture
def transactions(df_transactions):
    return (
        PandasMeasureProvider(name="transactions", data=df_transactions)
        .add_identifier("transaction", expr="id", role="primary")
        .add_identifier("person:buyer", expr="id_buyer", role="foreign")
        .add_identifier("person:seller", expr="id_seller", role="foreign")
        .add_measure("value")
    )


@pytest.fixture
def metaprovider(geographies, people, transactions):
    return (
        MetaMeasureProvider()
        .register(geographies)
        .register(people)
        .register(transactions)
    )
