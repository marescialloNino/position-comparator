import os
import ccxt
import ccxt.pro
from datetime import datetime

import pandas as pd
from datafeed.motherfeeder import MotherFeeder


class HyperliquidMarket(MotherFeeder):
    def __init__(self, account='0', request_timeout=30000, *args, **kwargs):
        self.account = account  # needed by next step
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        self.__PASS__ = ''
        if self.account == '0':
            self.__API_KEY = ''#os.environ['BITGET_TRADE_API_KEY']
            self.__API_SEC = ''#os.environ['BITGET_TRADE_API_SECRET']
            self.__PASS__ = ''

        self.__DEFAULT_TIMEOUT = 30
        self.__LIMIT = 100

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point_async(self):
        return ccxt.pro.hyperliquid({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        return ccxt.hyperliquid({
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

    async def set_leverage_async(self, symbol, leverage):
        rez = await self._exchange_async.set_leverage(leverage, symbol, {'marginMode': 'cross'})
        return

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['size']) * float(line['contractSize'])
            price = float(line['info']['markPrice'])
            symbol = line['info']['symbol']

            if qty != 0:
                direction = -1 if line['side'] == 'short' else 1
                qty = qty * direction
                entry_price = line['entryPrice']
                dt = line['datetime']
                amount = qty * price
                book[symbol] = qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
        return book
