"""
subir_mysql.py

Loads the consolidated monthly CSV files into the inyeccion_medidores
table in MySQL. Uses append mode so existing records are never overwritten.

Usage: Update DB credentials and file list before running.
"""

import pandas as pd
from sqlalchemy import create_engine

USER = 'your_username'
PASSWORD = 'your_password'
HOST = 'your_host'
DATABASE = 'your_database'

engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}')

archivos = [
    'inyeccion_2025_01.csv',
    'inyeccion_2025_02.csv',
    'inyeccion_2025_03.csv',
    'inyeccion_2025_04.csv',
    'inyeccion_2025_05.csv',
    'inyeccion_2025_06.csv',
    'inyeccion_2025_07.csv',
    'inyeccion_2025_08.csv',
    'inyeccion_2025_09.csv',
    'inyeccion_2025_10.csv',
]

total = 0
for archivo in archivos:
    df = pd.read_csv(archivo)
    df.to_sql('inyeccion_medidores', engine, if_exists='append', index=False, chunksize=5000)
    total += len(df)
    print(f"{archivo}: {len(df)} records loaded")

print(f"\nTotal loaded: {total} records")
