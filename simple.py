import pandas as pd
import requests

symbol = 'DOGE_USDT'
url = f'https://contract.mexc.com/api/v1/contract/kline/{symbol}'

response = requests.get(url)
data = response.json()
df = pd.DataFrame(data['data'])
df.to_csv(f'{symbol}.csv', index=False)
df.to_parquet(f'{symbol}.parquet', index=False)
print(df.head())