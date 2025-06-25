"""
This is the mother class for all REST feeders
"""
import time
import datetime
import pandas as pd
from abc import abstractmethod, ABC
from datafeed.downsample import create_bars
import datafeed.df_constants as constants
from ccxt.base.errors import BadSymbol, OperationRejected


class MotherFeeder(ABC):
    """
    An abstract base class representing a market data feeder
    """

    def __init__(self, *args, **kwargs):
        self._init_API()
        self._exchange = self.build_end_point()
        self._exchange_async = self.build_end_point_async()
        self.markets = self._exchange.load_markets()

    @abstractmethod
    def _init_API(self):
        raise NotImplementedError("Should implement _init_API()!")

    @abstractmethod
    def build_end_point(self):
        raise NotImplementedError("Should implement build_end_point()!")

    @abstractmethod
    def build_end_point_async(self):
        raise NotImplementedError("Should implement build_end_point()!")

    @abstractmethod
    def _get_limit(self):
        raise NotImplementedError("Should implement _arrange_data()!")

    @abstractmethod
    def _arrange_data(self, df):
        raise NotImplementedError("Should implement _arrange_data()!")

    def get_markets(self):
        return self._exchange.fetch_markets()

    async def read_bars_async(self, symbol, timeframe, start_time, end_time=None, read_timeframe=None):
        if timeframe not in constants.TIMEFRAMES:
            timeframe = '1h'

        if read_timeframe is None:
            read_timeframe = timeframe
        elif read_timeframe not in constants.TIMEFRAMES:
            raise ValueError("Bad reading timeframe!")

        if constants.TIMEFRAMES[read_timeframe] * 200 < constants.TIMEFRAMES[timeframe]:
            read_timeframe = list(constants.TIMEFRAMES)[5]
        step_sec = constants.TIMEFRAMES[read_timeframe] * 60
        step_bar = constants.TIMEFRAMES[timeframe] * 60

        # time step in seconds
        bar_complete = False
        buffer = pd.DataFrame()
        if type(start_time) is float or type(start_time) is int:
            init_time = start_time + 1
        else:
            start_time = start_time.timestamp()
            init_time = start_time + 1

        while init_time < (time.time() if end_time is None else end_time):
            retry = 5
            while retry > 0:
                try:
                    ohlcv = await self._exchange_async.fetch_ohlcv(symbol, read_timeframe, since=int(init_time * 1000),
                                                   limit=self._get_limit())
                    retry = -1
                except BadSymbol:
                    return None
                except Exception as e:
                    print(str(e))
                    time.sleep(0.1)
                    retry = retry - 1
            if retry == -1:
                data = pd.DataFrame(ohlcv)
                if len(data) > 0:
                    data = self._arrange_data(data)
                    data = data.loc[~data.index.duplicated(keep='last')]

                    if not buffer.empty:
                        buffer = pd.concat([buffer, data], axis=0)
                    else:
                        buffer = data.copy()
                    init_time = data.index[-1].timestamp() + step_sec

                    buffer = buffer.loc[~buffer.index.duplicated(keep='last')]
            time.sleep(self._exchange_async.rateLimit / 1000)  # time.sleep wants seconds

        if len(buffer) > 0:
            bars = create_bars(buffer, units=constants.TIMEFRAMES[timeframe], vwap=False)  # TODO virer colonne date
            last_time = bars.index[-1].timestamp()

            if last_time - start_time == step_bar:
                bar_complete = True
        else:
            bars = None
            bar_complete = False

        return bars, bar_complete

    def read_bars(self, symbol, timeframe, start_time, end_time=None, read_timeframe=None):
        """
        Reads 1min bars since start_time and aggregate to timeframe
        :param symbol: unified symbol for asset
        :type symbol: str or conversion.symbol
        :param timeframe: timestep for bar data
        :type timeframe: str
        :param start_time:
        :type start_time: timestamp in s
        :param end_time:
        :type end_time: timestamp in s
        :return: index list of newly arrived data, flag when bar is complete
        :rtype: list, bool
        """

        if timeframe not in constants.TIMEFRAMES:
            timeframe = '1h'

        if read_timeframe is None:
            read_timeframe = timeframe
        elif read_timeframe not in constants.TIMEFRAMES:
            raise ValueError("Bad reading timeframe!")

        if constants.TIMEFRAMES[read_timeframe] * 200 < constants.TIMEFRAMES[timeframe]:
            read_timeframe = list(constants.TIMEFRAMES)[5]
        step_sec = constants.TIMEFRAMES[read_timeframe] * 60
        step_bar = constants.TIMEFRAMES[timeframe] * 60

        # time step in seconds
        bar_complete = False
        buffer = pd.DataFrame()
        if type(start_time) is float or type(start_time) is int:
            init_time = start_time + 1
        else:
            start_time = start_time.timestamp()
            init_time = start_time + 1

        while init_time < (time.time() if end_time is None else end_time):
            retry = 5
            while retry > 0:
                try:
                    ohlcv = self._exchange.fetch_ohlcv(symbol, read_timeframe, since=int(init_time * 1000),
                                                   limit=self._get_limit())
                    retry = -1
                except Exception as e:
                    print(f'Exception for symbol {symbol}: {str(e)} - retry {retry}')
                    time.sleep(2 * self._exchange.rateLimit / 1000)
                    retry = retry - 1
            if retry == -1:
                data = pd.DataFrame(ohlcv)
                if len(data) > 0:
                    data = self._arrange_data(data)
                    data = data.loc[~data.index.duplicated(keep='last')]

                    if not buffer.empty:
                        buffer = pd.concat([buffer, data], axis=0)
                    else:
                        buffer = data.copy()
                    init_time = data.index[-1].timestamp() + step_sec

                    buffer = buffer.loc[~buffer.index.duplicated(keep='last')]
                else:
                    init_time = init_time + step_sec * self._get_limit()
            time.sleep(2 * self._exchange.rateLimit / 1000)  # time.sleep wants seconds

        if len(buffer) > 0:
            bars = create_bars(buffer, units=constants.TIMEFRAMES[timeframe], vwap=False)  # TODO virer colonne date
            last_time = bars.index[-1].timestamp()

            if last_time - start_time == step_bar:
                bar_complete = True
        else:
            bars = None
            bar_complete = False

        return bars, bar_complete

    def read_funding(self, symbol, timeframe, start_time, end_time=None):
        """
        Reads funding since last data point and aggregate to timeframe
        :param symbol:
        :type symbol: str
        :param timeframe: timestep for data
        :type symbol:
        """

        if timeframe not in constants.TIMEFRAMES:
            timeframe = '1h'

        if type(start_time) is float or type(start_time) is int:
            init_time = start_time + 1
        else:
            init_time = datetime.timestamp(start_time) + 1
        read_timeframe = list(constants.TIMEFRAMES)[0]
        funding = self._exchange.fetch_funding(symbol, read_timeframe, since=int(init_time * 1000),
                                               limit=self._get_limit())
        return funding

    def market_info(self, symbol):
        try:
            return self._exchange.market(symbol)
        except (BadSymbol, AttributeError):
            return None

    def get_rounded(self, quantity, symbol):
        return self._exchange.amountToPrecision(symbol, quantity)

    def get_min_order(self, symbol):
        info = self._exchange.market(symbol)

        return info['limits']['amount']['min']

    def get_cash(self, currency):
        balance = self._exchange.fetch_balance({'type': 'future'})
        if currency in balance:
            return balance[currency]
        else:
            return 0

    async def get_cash_async(self, currency):
        exchange_rates = {'USDT': 1, 'USDC': 1, 'BTC': 90000 * 0.95, 'ETH': 3500 * 0.95, 'BNB': 600 * 0.95}
        currencies = [currency] if isinstance(currency, str) else currency
        balance = await self._exchange_async.fetch_balance({'type': 'future'})
        free_all = 0
        total_all = 0

        for currency in currencies:
            if currency in balance:
                cross = exchange_rates.get(currency, 0)

                if 'free' in balance[currency] and 'total' in balance[currency]:
                    free_all = free_all + cross * balance[currency]['free']
                    total_all = total_all + cross * balance[currency]['total']
        return free_all, total_all

    @abstractmethod
    async def set_leverage_async(self, symbol, leverage):
        raise NotImplementedError("Should implement get_cash()!")

    async def get_positions_async(self, tickers=None):
        '''

        :param tickers:
        :return: List[float, float, float, timestamp]: qty, amount, entry_price, entry_ts
        '''
        if tickers is None or len(tickers) == 0:
            try:
                positions = await self._exchange_async.fetch_positions()
            except OperationRejected as e:
                message = f'API problem: {e.args}'

                raise ConnectionError(message)
        else:
            symbols = [self._exchange.market(ticker)['symbol'] for ticker in tickers]
            # positions = await self._exchange_async.fetch_positions(symbols=symbols)  # tous les exchange n'acceptent pas
            positions = []
            for symbol in symbols:
                pos = await self._exchange_async.fetch_positions(symbols=[symbol])
                if pos is not None:
                    positions.extend(pos)

        return self._dict_from_pos(positions)

    @abstractmethod
    def _dict_from_pos(self, positions):
        raise NotImplementedError("Should implement _dict_from_pos()!")
