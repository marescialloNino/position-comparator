import os
import ccxt
import ccxt.pro
import pandas as pd
from datetime import datetime
try:
    from datetime import UTC
except:
    from datetime import timezone
    UTC = timezone.utc
import asyncio
from datafeed.motherfeeder import MotherFeeder

class BinanceMarket(MotherFeeder):
    def __init__(self, request_timeout=30000, account='0', *args, **kwargs):
        self.account = account  # needed by next step
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)
        self.collateral = ['USDT', 'BNB', 'USDC']


    def _init_API(self):
        if self.account == '0':
            self.__API_KEY = os.environ['BINANCE_READ_API_KEY']
            self.__API_SEC = os.environ['BINANCE_READ_API_SECRET']
            self.__PASS__ = ''
        elif self.account == '1':
            self.__API_KEY = os.environ['BINANCE_PAIRSPREAD1_API_KEY']
            self.__API_SEC = os.environ['BINANCE_PAIRSPREAD1_API_SECRET']
            self.__PASS__ = ''
        elif self.account == '2':
            self.__API_KEY = os.environ['BINANCE_PAIRSPREAD2_API_KEY']
            self.__API_SEC = os.environ['BINANCE_PAIRSPREAD2_API_SECRET']
            self.__PASS__ = ''
        else:
            self.__API_KEY = os.environ['BINANCE_TRADE_API_KEY']
            self.__API_SEC = os.environ['BINANCE_TRADE_API_SECRET']
            self.__PASS__ = ''

        self.__LIMIT = 960

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point_async(self):
        return ccxt.pro.binance({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout,
        })

    def build_end_point(self):
        return ccxt.binance({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout,
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

    async def get_positions_async(self, tickers=None):
        positions = await self._exchange_async.fetch_balance({'type': 'margin'})

        return self._dict_from_pos(positions, tickers)

    async def set_leverage_async(self, symbol, leverage):
        return

    def _dict_from_pos(self, positions, tickers=None):
        book = {}

        for key, info in positions.items():
            if info is not None and 'debt' in info and 'free' in info:
                if (tickers is None or key in tickers) and key not in self.collateral:
                    short = info['debt']
                    long = info['free']

                    if short != 0 or long != 0:
                        book[key] = long - short, None, None, None
        return book

    def get_cash(self, currency):
        balance = self._exchange.fetch_balance({'type': 'margin'})
        if currency in balance:
            return balance[currency]
        else:
            return 0

    async def get_cash_async(self, currency):
        exchange_rates = {'USDT': 1, 'BTC': 90000 * 0.95, 'BNB': 600 * 0.95, 'USDC': 1}
        currencies = [currency] if isinstance(currency, str) else currency
        balance = await self._exchange_async.fetch_balance({'type': 'margin'})
        free_all = 0
        total_all = 0

        for currency in currencies:
            if currency in balance and 'free' in balance[currency] and 'total' in balance[currency]:
                cross = exchange_rates.get(currency, 0)
                free_all = free_all + cross * balance[currency]['free']
                total_all = total_all + cross * (balance[currency]['total'] - balance[currency]['debt'])
        return free_all, total_all

class BinanceFutureMarket(MotherFeeder):
    __DEFAULT_TIMEOUT = 30
    __LIMIT = 960
    __MARKETS = ['ETH/BTC', 'ETH/USDT']

    def __init__(self, request_timeout=30000, account='0', *args, **kwargs):
        self.account = account
        self.request_timeout = request_timeout
        super().__init__(*args, **kwargs)

    def _init_API(self):
        if self.account == '0':
            self.__API_KEY = os.environ['BINANCE_READ_API_KEY']
            self.__API_SEC = os.environ['BINANCE_READ_API_SECRET']
        elif self.account == 'mel_cm1':
            self.__API_KEY = os.environ['MELANION_CM1_API_KEY']
            self.__API_SEC = os.environ['MELANION_CM1_API_SECRET']
        elif self.account == 'mel_cm2':
            self.__API_KEY = os.environ['MELANION_CM2_API_KEY']
            self.__API_SEC = os.environ['MELANION_CM2_API_SECRET']
        elif self.account == 'mel_cm3':
            self.__API_KEY = os.environ['MELANION_CM3_API_KEY']
            self.__API_SEC = os.environ['MELANION_CM3_API_SECRET']
        elif self.account == 'nickel_cm1':
            self.__API_KEY = os.environ['BINANCE_NICKEL_CM1_API_KEY']
            self.__API_SEC = os.environ['BINANCE_NICKEL_CM1_API_SECRET']
        else:
            self.__API_KEY = os.environ['BINANCE_READ_API_KEY']
            self.__API_SEC = os.environ['BINANCE_READ_API_SECRET']
        self.__DEFAULT_TIMEOUT = 30
        self.__LIMIT = 960

    def _get_options(self):
        options_portfolio_margin = {}
        options_futures_account = {
            'defaultType': 'swap',
            'defaultSubType': 'linear'
        }
        return options_portfolio_margin if self.account == 'nickel_cm1' else options_futures_account

    def build_end_point(self):

        return ccxt.binance({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout,
            'options': self._get_options(),
        })

    def build_end_point_async(self):
        return ccxt.pro.binance({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'enableRateLimit': True,
            'timeout': self.request_timeout,
            'options': self._get_options(),
        })

    def _arrange_data(self, ohlcv_dataframe):
        ohlcv_dataframe['date'] = ohlcv_dataframe[0]
        ohlcv_dataframe['open'] = ohlcv_dataframe[1]
        ohlcv_dataframe['high'] = ohlcv_dataframe[2]
        ohlcv_dataframe['low'] = ohlcv_dataframe[3]
        ohlcv_dataframe['close'] = ohlcv_dataframe[4]
        ohlcv_dataframe['volume'] = ohlcv_dataframe[5]
        ohlcv_dataframe = ohlcv_dataframe.set_index('date')
        # Change binance ms timestamp to date in UTC
        ohlcv_dataframe = ohlcv_dataframe.set_index(
            pd.to_datetime(ohlcv_dataframe.index, unit='ms').tz_localize('UTC'))
        ohlcv_dataframe.drop([0, 1, 2, 3, 4, 5], axis=1, inplace=True)

        return ohlcv_dataframe

    def _get_limit(self):
        return self.__LIMIT

    def _dict_from_pos(self, positions):
        book = {}

        for line in positions:
            qty = float(line['info']['positionAmt'])
            price = float(line['info']['markPrice'])
            symbol = line['info']['symbol']

            if qty != 0:
                entry_price = line['entryPrice']
                dt = line['timestamp']
                amount = qty * price
                book[symbol] = [qty, amount, entry_price, datetime.fromtimestamp(dt * 1e-3, tz=UTC)]
        return book

    async def set_leverage_async(self, symbol, leverage):
        rez = await self._exchange_async.set_leverage(leverage, symbol, {'marginMode': 'cross'})
        return

    def get_cash(self, currency):
        exchange_rates = {'USDT': 1, 'BTC': 90000 * 0.95, 'BNB': 600 * 0.95, 'USDC': 1}
        currencies = [currency] if isinstance(currency, str) else currency
        balance_m = self._exchange.fetch_balance({'type': 'margin'})  # portfolio margin account collat
        balance_f = self._exchange.fetch_balance({'type': 'future'})  # portfolio margin account future
        free_all = 0
        total_all = 0

        for currency in currencies:
            cross = exchange_rates.get(currency, 0)
            if currency in balance_m and 'free' in balance_m[currency] and 'total' in balance_m[currency]:
                free_all = free_all + cross * balance_m[currency]['free']
                total_all = total_all + cross * balance_m[currency]['total']
            if currency in balance_f and 'total' in balance_f[currency]:
                cross = exchange_rates.get(currency, 0)
                total_all = total_all + cross * balance_f[currency]['total']

        return free_all, total_all

    async def get_cash_async(self, currency):
        exchange_rates = {'USDT': 1, 'BTC': 90000 * 0.95, 'BNB': 600 * 0.95, 'USDC': 1}
        currencies = [currency] if isinstance(currency, str) else currency
        balance_m = await self._exchange_async.fetch_balance({'type': 'margin'})  # portfolio margin account collat
        balance_f = await self._exchange_async.fetch_balance({'type': 'future'})  # portfolio margin account future
        free_all = 0
        total_all = 0

        for currency in currencies:
            cross = exchange_rates.get(currency, 0)
            if currency in balance_m and 'free' in balance_m[currency] and 'total' in balance_m[currency]:
                free_all = free_all + cross * balance_m[currency]['free']
                total_all = total_all + cross * balance_m[currency]['total']
            if currency in balance_f and 'total' in balance_f[currency]:
                cross = exchange_rates.get(currency, 0)
                total_all = total_all + cross * balance_f[currency]['total']

        return free_all, total_all

async def main0():
    ticker = 'BCHUSDT'
    timeframe = '30m'
    end_time = None  # int(datetime.timestamp(datetime(2022, 12, 1)))
    start_time = int(datetime.timestamp(datetime(2022, 9, 15)))
    market = BinanceFutureMarket(account=1)
    data, done = market.read_bars(symbol=ticker, timeframe=timeframe, start_time=start_time, end_time=end_time)
    print(done)

    balance = await market.get_cash_async(['USDT'])
    await market._exchange_async.close()
    print(balance)

async def main():
    market = BinanceMarket(account=2)

    balance = await market.get_cash_async(['USDT'])
    positions = await market.get_positions_async()
    await market._exchange_async.close()
    print(balance)

if __name__ == '__main__':
    asyncio.run(main())


