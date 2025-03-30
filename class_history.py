import sys; sys.path.append('.')
import pandas as pd
import requests
import aiohttp
import asyncio
import time
import os

class History:
    def __init__(self):
        self.folder = 'history'
        self.parquet_folder = 'history/parquets'
        os.makedirs(self.parquet_folder, exist_ok=True)
        self.first_timestamp = None
        self.all_timestamps = None
        self.exchange_info = None
        self.relevant_info = None
        self.symbols = None
        self.get_relevant_info()

    def get_exchange_info(self):
        response = requests.get('https://contract.mexc.com/api/v1/contract/detail')
        exchange_info = pd.DataFrame(response.json()['data'])
        exchange_info.to_parquet(f'{self.folder}/exchange_info.parquet')
        return exchange_info


    def get_relevant_info(self):
        self.first_timestamp = int(time.time()) - 86400 - (int(time.time()) % 86400)
        self.all_timestamps = [self.first_timestamp - i * 86400 for i in range(30)]
        self.exchange_info = self.get_exchange_info()
        self.relevant_info = self.exchange_info[self.exchange_info['isZeroFeeRate'] == True]
        self.symbols = self.relevant_info['symbol'].tolist()
        self.relevant_info = self.relevant_info[['symbol', 'createTime', 'openingTime']]
        self.relevant_info.to_parquet(f'{self.folder}/relevant_info.parquet')

    def get_kline_data(self, symbol):
        # https://mexcdevelop.github.io/apidocs/contract_v1_en/#k-line-data
        # Rate limit:20 times/2 seconds
        # https://contract.mexc.com/api/v1/contract/kline/DOGE_USDT?interval=Min1&start=1739657040&end=1739777100
        # the first_timestamp is 
        batch_size = 15
        url = 'https://contract.mexc.com/api/v1/contract/kline/{}?interval=Min1&start={}&end={}'
        all_urls = [url.format(symbol, timestamp, timestamp + 86400 - 60) for timestamp in self.all_timestamps]
        symbol_data_frames = []
        while all_urls:
            batch_urls = all_urls[:batch_size]
            all_urls = all_urls[batch_size:]
            if not batch_urls:
                break
            data_frames = []
            responses = asyncio.run(self.get_batch_data(batch_urls))
            good_responses = []
            for response in responses:
                data, u = response
                if data['status'] == 200 and ('time' in data['data']) and len(data['data']['time']) == 1440:
                    good_responses.append(response)
            for response in good_responses:
                data, u = response
                df = pd.DataFrame(data['data'])
                data_frames.append(df)
            if data_frames:
                data_frames = pd.concat(data_frames)
                symbol_data_frames.append(data_frames)
            else:
                break
            time.sleep(-time.time() % 2)
        if symbol_data_frames:
            symbol_data_frames = pd.concat(symbol_data_frames)
            symbol_data_frames = symbol_data_frames.sort_values(by='time').reset_index(drop=True)
            time_unique_values = symbol_data_frames['time'].nunique()
            df_length = len(symbol_data_frames)
            result = 'success' if time_unique_values == df_length else 'fail !!!!!!!!!!!!!!!!!!!!!!!!!'
            print(f'{symbol} | Unique Time {time_unique_values} | DF Length {df_length} | Days {int(df_length / 1440)} | {result}')
            symbol_data_frames.to_parquet(f'{self.parquet_folder}/{symbol}.parquet')
        return symbol_data_frames

    def get_batch_data(self, urls):
        async def fetch(url, session):
            async with session.get(url) as response:
                d = await response.json()
                return {"status": response.status, "data": d.get('data', {})}, url
        async def fetch_all(urls):
            async with aiohttp.ClientSession() as session:
                return await asyncio.gather(*[fetch(url, session) for url in urls])
        return fetch_all(urls)
    
    def get_all_kline_data(self):
        start_time = time.time()
        for symbol in self.symbols:
            self.get_kline_data(symbol)
        time_taken = round(time.time() - start_time, 2)
        time_taken = time.strftime('%H:%M:%S', time.gmtime(time_taken))
        print(f'Time taken: {time_taken}')

if __name__ == '__main__':
    history = History()
    history.get_all_kline_data()

'''
conda activate 313 & cd desktop/mexc & python history.py
'''