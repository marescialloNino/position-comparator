import os
import ccxt
import ccxt.pro
from datetime import datetime
try:
    from datetime import UTC
except:
    from datetime import timezone
    UTC = timezone.utc

import pandas as pd
from datafeed.motherfeeder import MotherFeeder


class BybitMarket(MotherFeeder):
    def __init__(self, account='0', request_timeout=20000, *args, **kwargs):
        self.account = account  # needed by next step
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        self.__PASS__ = ''
        if self.account == '0':
            self.__API_KEY = os.environ['BYBIT_TRADE_API_KEY']
            self.__API_SEC = os.environ['BYBIT_TRADE_API_SECRET']
        elif self.account == '1':
            self.__API_KEY = os.environ['BYBIT_PAIRSPREAD1_API_KEY']
            self.__API_SEC = os.environ['BYBIT_PAIRSPREAD1_API_SECRET']
        elif self.account == '2':
            self.__API_KEY = os.environ['BYBIT_PAIRSPREAD2_API_KEY']
            self.__API_SEC = os.environ['BYBIT_PAIRSPREAD2_API_SECRET']
        elif self.account == '3':
            self.__API_KEY = os.environ['BYBIT_PAIRSPREAD3_API_KEY']
            self.__API_SEC = os.environ['BYBIT_PAIRSPREAD3_API_SECRET']

        self.__DEFAULT_TIMEOUT = 30
        self.__LIMIT = 100

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point_async(self):
        return ccxt.pro.bybit({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        return ccxt.bybit({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
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
            symbol = line['info']['symbol']

            if qty != 0:
                price = float(line['info']['markPrice'])
                direction = -1 if line['side'] == 'short' else 1
                qty = qty * direction
                entry_price = line['entryPrice']
                dt = line['timestamp']  # attention : le timestamp de l'entrée en book est remplacé par le dernier ts de funding
                amount = qty * price
                book[symbol] = qty, amount, entry_price, datetime.fromtimestamp(dt * 1e-3, tz=UTC)
        return book

    async def get_cash_async(self, currency):
        exchange_rates = {'USDT': 1, 'BTC': 90000 * 0.95, 'ETH': 3500 * 0.95}
        currencies = [currency] if isinstance(currency, str) else currency
        balance = await self._exchange_async.fetch_balance({'type': 'unified'})
        free_all = 0
        total_all = 0
        coin_list = balance['info']['result']['list']
        bal_dict = {item['coin'][0]['coin']: {
            'free': float(item['coin'][0]['availableToWithdraw']),
            'total': float(item['coin'][0]['usdValue'])
        } for item in coin_list}

        for currency in currencies:
            if currency in bal_dict:
                cross = exchange_rates.get(currency, 0)

                if 'free' in bal_dict[currency] and 'total' in bal_dict[currency]:
                    free_all = free_all + cross * bal_dict[currency]['free']
                    total_all = total_all + cross * bal_dict[currency]['total']
        return free_all, total_all
