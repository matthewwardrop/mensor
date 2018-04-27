import pandas as pd
import os
import numpy as np

data_dir = os.path.dirname(__file__)
with open(os.path.join(data_dir, 'geography_names.txt')) as f:
    geography_names = [name[:-1] for name in f.readlines()]
with open(os.path.join(data_dir, 'names.txt')) as f:
    names = [name[:-1] for name in f.readlines()]

N = 1000

# Generate Geographies
geographies = pd.DataFrame({
    'id_geography': range(len(geography_names)),
    'name': geography_names,
    'population': np.random.randint(1e6, size=len(geography_names)),
    'ds': '2018-01-01'
}).reset_index().rename(columns={'index': 'id'})

geographies.to_csv(os.path.join(data_dir, 'geographies.csv'), index=False)

# Generate People
people = pd.DataFrame({
    'id_person': range(N),
    'name': np.random.choice(names, N),
    'age': np.random.randint(100, size=N),
    'id_geography': np.random.randint(len(geography_names), size=1000),
    'ds': '2018-01-01'
}).reset_index().rename(columns={'index': 'id'})

people.to_csv(os.path.join(data_dir, 'people.csv'), index=False)

# Generate Transactions
transactions = pd.DataFrame({
    'id_seller': np.random.randint(1000, size=2 * N),
    'id_buyer': np.random.randint(1000, size=2 * N),
    'value': np.random.randint(1000, size=2 * N),
    'ds': '2018-01-01'
}).reset_index().rename(columns={'index': 'id'})

transactions.to_csv(os.path.join(data_dir, 'transactions.csv'), index=False)
