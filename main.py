from decimal import Decimal
import requests
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
        df = df[['time', 'open', 'close', 'high', 'low']]
        df = df.tail(30240)
        csv_path = os.path.join(csv_folder, file.replace('.parquet', '.csv'))
        df.to_csv(csv_path, index=False)
        print(f'Converted {file} to {csv_path}')

def ticker(symbols_list):
    url = 'https://contract.mexc.com/api/v1/contract/ticker'
    response = requests.get(url).json()
    df = pd.DataFrame(response['data'])
    df = df[df['symbol'].isin(symbols_list)]
    df['pip'] = df.apply(lambda row: float(Decimal(str(row['ask1'])) - Decimal(str(row['bid1']))), axis=1)
    df['pip_size'] = df['pip'].apply(lambda x: format(x, '.10f').rstrip('0').rstrip('.')).astype(str)
    df['pip_size'] = df['pip_size'].apply(lambda x: ''.join(['1' if c not in ['0', '.'] else c for c in x]))
    df['pip_round'] = df['pip_size'].apply(lambda x: len(x) - 2)
    df['pip'] = df['pip_size'].astype(float)
    df['pip_price_%'] = round(df['pip'] / df['ask1'] * 100, 5)
    df = df.sort_values(by='pip_price_%', ascending=True).reset_index(drop=True)
    df = df[['symbol', 'pip_size', 'pip_round', 'pip_price_%'] + [col for col in df.columns if col not in ['symbol', 'pip', 'pip_size', 'pip_round', 'pip_price_%']]]
    return df

contract_info_url = 'https://contract.mexc.com/api/v1/contract/detail'
response = requests.get(contract_info_url)
df = pd.DataFrame(response.json()['data'])
df = df[df['isZeroFeeSymbol'] == True]
df = df.sort_values(by='symbol')
df.to_parquet('contract_info.parquet', index=False)
df.to_csv('contract_info.csv', index=False)
symbols = df['symbol'].tolist()
ticker_df = ticker(symbols)
ticker_df.to_parquet('ticker.parquet', index=False)
ticker_df.to_csv('ticker.csv', index=False)