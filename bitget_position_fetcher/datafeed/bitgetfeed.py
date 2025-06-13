
import os
import ccxt
import ccxt.pro
from datetime import datetime
import pandas as pd
from .motherfeeder import MotherFeeder
import logging

# Configure logging
logger = logging.getLogger(__name__)

class BitgetMarket(MotherFeeder):
    def __init__(self, account=0, request_timeout=30000, api_key=None, api_secret=None, api_password=None, *args, **kwargs):
        self.account = account
        self.request_timeout = request_timeout
        # Initialize credentials
        self.__API_KEY = api_key or ''
        self.__API_SEC = api_secret or ''
        self.__PASS__ = api_password or ''
        super().__init__(*args, **kwargs)
        # Load credentials from environment if not provided
        if not all([self.__API_KEY, self.__API_SEC, self.__PASS__]):
            self._init_API()
        logger.debug(f"Initialized BitgetMarket for account {account} with API key: {'set' if self.__API_KEY else 'not set'}")

    def _init_API(self):
        """Load API credentials from environment variables based on account."""
        env_vars = {
            0: ('BITGET_TRADE_API_KEY', 'BITGET_TRADE_API_SECRET', 'BITGET_API_PASSWORD'),
            1: ('BITGET_PAIRSPREAD1_API_KEY', 'BITGET_PAIRSPREAD1_API_SECRET', 'BITGET_API_PASSWORD'),
            2: ('BITGET_PAIRSPREAD2_API_KEY', 'BITGET_PAIRSPREAD2_API_SECRET', 'BITGET_API_PASSWORD'),
            'H1': ('BITGET_HEDGE1_API_KEY', 'BITGET_HEDGE1_API_SECRET', 'BITGET_API_PASSWORD'),
            '2': ('BITGET_2_API_KEY', 'BITGET_2_API_SECRET', 'BITGET_API_PASSWORD')
        }
        if self.account not in env_vars:
            error_msg = f"Invalid account {self.account}. Supported accounts: {list(env_vars.keys())}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        key_var, secret_var, pass_var = env_vars[self.account]
        try:
            self.__API_KEY = os.environ[key_var]
            self.__API_SEC = os.environ[secret_var]
            self.__PASS__ = os.environ[pass_var]
            logger.info(f"Loaded API credentials from environment for account {self.account}")
        except KeyError as e:
            error_msg = f"Missing environment variable {e.args[0]} for account {self.account}"
            logger.error(error_msg)
            raise KeyError(error_msg)

    def _get_limit(self):
        return self.__LIMIT

    def build_end_point_async(self):
        if not self.__API_KEY:
            error_msg = f"API key not set for account {self.account}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return ccxt.pro.bitget({
            'apiKey': self.__API_KEY,
            'secret': self.__API_SEC,
            'password': self.__PASS__,
            'enableRateLimit': True,
            'timeout': self.request_timeout
        })

    def build_end_point(self):
        if not self.__API_KEY:
            error_msg = f"API key not set for account {self.account}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return ccxt.bitget({
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
            qty = float(line['contracts']) * float(line['contractSize'])
            price = float(line['info']['markPrice'])
            symbol = line['info']['symbol']
            if qty != 0:
                direction = -1 if line['side'] == 'short' else 1
                qty = qty * direction
                entry_price = float(line['entryPrice'])
                dt = line['datetime']
                amount = qty * price
                book[symbol] = qty, amount, entry_price, datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
        return book