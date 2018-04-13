import pandas as pd
import os
import numpy as np

data_dir = os.path.dirname(__file__)
with open(os.path.join(data_dir, 'names.csv')) as f:
    names = [name[:-1] for name in f.readlines()]

N = 1000

# Generate People
people = pd.DataFrame({
    'id_person': range(N),
    'name': np.random.choice(names, N),
    'age': np.random.randint(100, size=N),
    'id_country': np.random.randint(1000, size=1000)
}).reset_index().rename(columns={'index': 'id'})

people.to_csv(os.path.join(data_dir, 'people.csv'), index=False)

# Generate Transactions
transactions = pd.DataFrame({
    'id_seller': np.random.randint(1000, size=2 * N),
    'id_buyer': np.random.randint(1000, size=2 * N),
    'value': np.random.randint(1000, size=2 * N),
}).reset_index().rename(columns={'index': 'id'})

transactions.to_csv(os.path.join(data_dir, 'transactions.csv'), index=False)
