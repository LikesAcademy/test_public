import pandas as pd
import requests
import os

file = 'history/parquets/1INCH_USDT.parquet'
df = pd.read_parquet(file)
df = df[['time', 'open', 'high', 'low', 'close']]

os.makedirs('sizes', exist_ok=True)

sizes = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]

for size in sizes:
    df.iloc[:size].to_csv(f'sizes/{size}.csv', index=False)