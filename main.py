import pandas as pd
import os
import shutil

parquet_folder = 'history/parquets'
csv_folder = 'history/csv'

if os.path.exists(csv_folder):
    shutil.rmtree(csv_folder)
os.makedirs(csv_folder)

for file in os.listdir(parquet_folder):
    if file.endswith('.parquet'):
        df = pd.read_parquet(os.path.join(parquet_folder, file))
        df = df[['time', 'open', 'high', 'low', 'close']]
        csv_path = os.path.join(csv_folder, file.replace('.parquet', '.csv'))
        df.to_csv(csv_path, index=False)
        print(f'Converted {file} to {csv_path}')
