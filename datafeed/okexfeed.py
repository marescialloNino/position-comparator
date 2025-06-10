import os
import ccxt
import ccxt.pro
import pandas as pd
import numpy as np
from datetime import datetime
try:
    from datetime import UTC
except:
    from datetime import timezone
    UTC = timezone.utc

import os
from dotenv import load_dotenv

load_dotenv()

import asyncio
from datafeed.motherfeeder import MotherFeeder


class OkexMarket(MotherFeeder):
    def __init__(self, account=0, request_timeout=60000, *args, **kwargs):
        self.account = account  # needed by next step
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        if self.account == 0:
            self.__API_KEY = ''#os.getenv('OKEXFUT_TRADE_API_KEY')
            self.__API_SEC = ''#os.getenv('OKEXFUT_TRADE_API_SECRET')
            self.__PASS__ = ''
        elif self.account == 1:
            self.__API_KEY = os.environ['OKEXFUT_PAIRSPREAD_API_KEY']
            self.__API_SEC = os.environ['OKEXFUT_PAIRSPREAD_API_SECRET']
            self.__PASS__ = os.environ['OKEXFUT_PAIRSPREAD_API_PASSWORD']
        elif self.account == 2:
            self.__API_KEY = os.environ['OKEXFUT_PAIRSPREAD2_API_KEY']
            self.__API_SEC = os.environ['OKEXFUT_PAIRSPREAD2_API_SECRET']
            self.__PASS__ = os.environ['OKEXFUT_PAIRSPREAD2_API_PASSWORD']
        elif self.account == 3:
            self.__API_KEY = os.environ['OKEXFUT_PAIRSPREAD3_API_KEY']
            self.__API_SEC = os.environ['OKEXFUT_PAIRSPREAD3_API_SECRET']
            self.__PASS__ = os.environ['OKEXFUT_PAIRSPREAD3_API_PASSWORD']
        elif self.account == 'edo1':
            self.__API_KEY = os.environ['OKEXFUT_EDO1_API_KEY']
            self.__API_SEC = os.environ['OKEXFUT_EDO1_API_SECRET']
            self.__PASS__ = os.environ['OKEXFUT_EDO1_API_PASSWORD']
        elif self.account == 'edo3':
            self.__API_KEY = os.environ['OKEXFUT_EDO3_API_KEY']
            self.__API_SEC = os.environ['OKEXFUT_EDO3_API_SECRET']
            self.__PASS__ = os.environ['OKEXFUT_EDO3_API_PASSWORD']
        else:
            self.__API_KEY = ''
            self.__API_SEC = ''
            self.__PASS__ = ''

        self.__DEFAULT_TIMEOUT = 30
        self.__LIMIT = 100  # https://www.okx.com/docs-v5/en/#rest-api-market-data-get-candlesticks

    def build_end_point_async(self):
        return ccxt.pro.okx({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        return ccxt.okx({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def _arrange_data(self, ohlcv_dataframe):
        ohlcv_dataframe['date'] = ohlcv_dataframe[0]
        ohlcv_dataframe['open'] = ohlcv_dataframe[1]
        ohlcv_dataframe['high'] = ohlcv_dataframe[2]
        ohlcv_dataframe['low'] = ohlcv_dataframe[3]
        ohlcv_dataframe['close'] = ohlcv_dataframe[4]
        ohlcv_dataframe['volume'] = ohlcv_dataframe[5]
        ohlcv_dataframe = ohlcv_dataframe.set_index('date')
        # Change ms timestamp to date in UTC
        ohlcv_dataframe = ohlcv_dataframe.set_index(
            pd.to_datetime(ohlcv_dataframe.index, unit='ms').tz_localize('UTC'))
        ohlcv_dataframe.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)

        return ohlcv_dataframe

    def _get_limit(self):
        return self.__LIMIT

    # def get_cash(self, currency):
    #     balance = self._exchange.fetch_balance(params={'ccy': currency})
    #     if currency in balance:
    #         return balance[currency]
    #     else:
    #         return 0
    #
    # async def get_cash_async(self, currency):
    #     balance = await self._exchange_async.fetch_balance(params={'ccy': currency})
    #     if balance is not None and currency in balance:
    #         if 'free' in balance[currency] and 'total' in balance[currency]:
    #             return balance[currency]['free'], balance[currency]['total']
    #     return 0, 0

    async def set_leverage_async(self, symbol, leverage):
        rez = await self._exchange_async.set_leverage(leverage, symbol, {'marginMode': 'cross'})
        return

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['pos']) * float(line['contractSize'])
            symbol = line['info']['instId']

            if qty != 0:
                price = float(line['info']['markPx'])
                entry_price = line['entryPrice']
                # dt = line['datetime']  # last tx on coin, can be funding
                dt = int(line['info']['cTime'])  # entry date
                amount = qty * price
                # book[symbol] = qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
                book[symbol] = qty, amount, entry_price, datetime.fromtimestamp(dt * 1e-3, tz=UTC)

        return book


async def main():
    ticker = 'APT-USDT'
    timeframe = '30m'
    end_time = None  # int(datetime.timestamp(datetime(2022, 12, 1)))
    start_time = int(datetime.timestamp(datetime(2022, 9, 15)))
    market = OkexMarket(account=1)
    data, done = market.read_bars(symbol=ticker, timeframe=timeframe, start_time = start_time, end_time=end_time)
    print(done)
    await market._exchange_async.set_leverage(5, 'BTC-USDT-SWAP')
    balance = await market._exchange_async.fetch_balance()
    await market._exchange_async.close()
    if balance is not None and 'free' in balance:
        print(balance['free'])

async def main_pos():
    ticker = 'ARB-USDT-SWAP'
    market = OkexMarket(account=1)
    result = await market.get_positions_async([ticker])
    pass


if __name__ == '__main__':
    asyncio.run(main_pos())


