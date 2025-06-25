import sys
import asyncio
import datetime
import time
import logging
import math
from collections import deque

import numpy as np
from asyncio import run, create_task
import traceback
import pandas as pd

from datafeed.okexfeed import OkexMarket
from datafeed.bitgetfeed import BitgetMarket
from datafeed.binancefeed import BinanceFutureMarket, BinanceMarket
from datafeed.utils_online import today_utc

class RateLimiter:
    def __init__(self, min_interval):
        """
        :param min_interval: Minimum interval in seconds between calls to the rate-limited function.
        """
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last_call = 0

    async def __aenter__(self):
        async with self._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - self._last_call)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_call = time.monotonic()

    async def __aexit__(self, exc_type, exc, tb):
        pass


class MarketWatch:
    def __init__(self, exchange, tf, with_lob=False, event_queue=None):
        self.exchange = exchange
        self.timeframe = tf
        self.params = {
            'returnRateLimits': True,
                'streamLimits': {
                    'future': 100,
                }
        } # not working
        self.rate_limiting = RateLimiter(1)
        # https://www.bitget.com/api-doc/common/websocket-intro,
        # https://www.okx.com/docs-v5/en/#websocket-api-introduction
        # https://academy.binance.com/en/articles/what-are-binance-websocket-limits 300 cx per 5 minutes
        self.tf_ms = self.exchange.parse_timeframe(tf) * 1000
        self.bars = {}
        self.bars_df = pd.DataFrame()
        self.last = {'last_modified': pd.Timestamp.min.tz_localize('UTC')}
        self._ticker_tasks = dict()
        self._lob_tasks = dict()
        self._running = dict()
        self.limit = 1000
        self.lob_limit = {'Bybit':1, 'Binance':10} if with_lob else {}
        self.logger = logging.getLogger('market_watch')
        self.errors = deque([], maxlen=10)
        self.event_queue = event_queue

    def get_bidasklast(self, symbol):
        data = self.last.get(symbol, {})
        bid = data.get('bid', None)
        ask = data.get('ask', None)
        last = data.get('last', None)
        return bid, ask, last

    def get_age_last(self):
        return (today_utc() - self.last['last_modified']).total_seconds()

    def get_age_data(self, symbol):
        data = self.last.get(symbol, {})
        now_ts = today_utc().timestamp()
        result = {'last': now_ts - data.get('timestamp', 0) / 1000,
                  'lob': now_ts - data.get('lob_ts', 0) / 1000}
        return result

    async def _end_ticker_task(self, task):
        symbol = self._ticker_tasks.get(task, None)
        msg = task.result()
        self.errors.append(msg)
        self.logger.warning(f'listener said {msg}')

        if symbol is not None:
            self._running[symbol] = False
            await self.exchange.un_watch_ticker(symbol)

    @staticmethod
    async def _rerun_forever(coro, *args, **kwargs):
        while True:
            try:
                await coro(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:
                msg = traceback.format_exc()
                return msg

    def _build_histo_from_ticker(self, symbol, ticker):
        keep = ['last', 'bid', 'ask', 'timestamp']
        new_bar = False
        if ticker is not None:
            current = self.last.get(symbol, {})
            for key, value in ticker.items():
                if key in keep and value is not None:
                    current.update({key: value})
            self.last.update({symbol: current})
            if 'timestamp' in ticker:
                ts = ticker['timestamp']
                last_date = pd.Timestamp(ts_input=ts * 1e6, tz='UTC')
            else:
                last_date = pd.Timestamp(ts_input=self.exchange.milliseconds() * 1e6, tz='UTC')
                ts = last_date.timestamp()
            self.last['last_modified'] = last_date
            opening_time = int(math.floor(ts / self.tf_ms)) * self.tf_ms
            closing_time = int(math.ceil(ts / self.tf_ms)) * self.tf_ms
            bar_date = pd.Timestamp(ts_input=opening_time * 1e6, tz='UTC')

            if bar_date not in self.bars_df.index:
                bar_closingdate = pd.Timestamp(ts_input=closing_time * 1e6, tz='UTC')
                self.bars_df.loc[bar_date, 'closing_time'] = bar_closingdate
                new_bar = True
            if symbol not in self.bars_df.columns:
                new_data = pd.Series(index=self.bars_df.index, dtype=float, name=symbol)
                self.bars_df = pd.concat([self.bars_df, new_data], axis=1)
            last_quote = current.get('last', np.nan)
            self.bars_df.loc[bar_date, symbol] = last_quote
        return new_bar

    async def watch_ticker_loop(self, symbol, options={}):
        method = 'watchTicker'
        self.bars[symbol] = []
        self._running[symbol] = True
        if method not in self.exchange.has or not self.exchange.has[method]:
            return
        while True:
            try:
                ticker = await self.exchange.watch_ticker(symbol)
                new_bar = self._build_histo_from_ticker(symbol, ticker)
                if new_bar and self.event_queue is not None:
                    if len(self.bars_df) > 1:
                        # event = MarketEvent(time=self.last['last_modified'], index=self.bars_df.index[-2],
                        #                     size=1, period=self.timeframe)  # in case the client needs an event when bar is full
                        # await self.event_queue.put('Beep')
                        pass
            except ConnectionResetError as e:
                self.logger.error(f'Connection reset error for {symbol} on {self.exchange.name}: {e}')
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f'Unexpected error in watch_ticker_loop for {symbol} on {self.exchange.name}: {e}')
                await asyncio.sleep(5)

    async def watch_ohlcv(self, exchange, symbol):
        method = 'watchTrades'
        if method not in exchange.has or not exchange.has[method]:
            return
        since = exchange.milliseconds() - 30 * 1000  # last hour
        collected_trades = []
        self.bars[symbol] = []

        while self._running.get(symbol, False):
            try:
                trades = await exchange.watch_trades(symbol)
                collected_trades.extend(trades)
                generated_bars = exchange.build_ohlcvc(collected_trades, self.timeframe, since, self.limit)
                for bar in generated_bars:
                    bar_timestamp = bar[0]
                    collected_bars_length = len(self.bars[symbol])
                    last_collected_bar_timestamp = self.bars[symbol][collected_bars_length - 1][0] if \
                        collected_bars_length > 0 else 0
                    if bar_timestamp == last_collected_bar_timestamp:
                        self.bars[symbol][collected_bars_length - 1] = bar
                    elif bar_timestamp > last_collected_bar_timestamp:
                        self.bars[symbol].append(bar)
                        collected_trades = exchange.filter_by_since_limit(collected_trades, bar_timestamp)
            except Exception as exception:
                self.logger.error(f'Received exception: {exception}')
                if exception:
                    self.logger.error(f'Exception type: {type(exception)}')
                    self.logger.error(f'Exception args: {exception.args}')
                    self.logger.error('Traceback:', exc_info=True)

    async def _keep_alive(self):
        rate_limiter = RateLimiter(60)
        while True:
            lagging = []
            for task, symbol in self._ticker_tasks.items():
                age = self.get_age_data(symbol)
                if age['last'] > 120:
                    lagging.append((task, symbol))
            with rate_limiter:
                for restart in lagging:
                    task, symbol = restart
                    task.cancel()
                    try:
                        await task
                        self._ticker_tasks.pop(task, None)
                    except asyncio.CancelledError:
                        if not task.cancelled():
                            self.logger.warning('ticker task not cancelled')
                    self.logger.info(f'Restarting watch_ticker_loop for {symbol}')
                    await self._start_one_ticker(symbol)

    async def run_watcher(self, symbols):
        self.logger.info(f'Creating tickers on {self.exchange.name}')
        for symbol in symbols:
            self.logger.info(f'Created {symbol}')
            await self._start_one_ticker(symbol)

        if self.exchange.name in self.lob_limit:
            for symbol in symbols:
                limit = self.lob_limit[self.exchange.name]
                task = create_task(MarketWatch._rerun_forever(watch_lob_loop, self.exchange, symbol, limit))
                self._lob_tasks[task] = symbol
                timing = 0.2 + np.random.uniform(0.1, 0.2)
                await asyncio.sleep(timing)
            self.logger.info(f'Created lob watch')

        self.keep_alive_task = create_task(self._keep_alive())
        self.logger.info(f'Created keep_alive_task')

        def update_lob(symbol, data):
            if data is not None:
                current = self.last.get(symbol, {})
                ts = self.exchange.milliseconds()
                current['lob_ts'] = int(ts)
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                if len(bids) > 0:
                    current['bid'] = bids[0][0]
                    if len(asks) > 0:
                        current['ask'] = asks[0][0]
                        current['mid'] = 0.5 * (current['ask'] + current['bid'])
                self.last.update({symbol: current})
            else:
                self.logger.warning('lob empty')

        async def watch_lob_loop(exchange, symbol, limit):
            method = 'watchOrderBook'
            if method not in exchange.has or not exchange.has[method]:
                return
            while True:
                data = await exchange.watchOrderBook(symbol, limit=limit)  # 10 bin, 1 bybit
                update_lob(symbol, data)

    async def _start_one_ticker(self, symbol):
        async with self.rate_limiting:
            task = create_task(MarketWatch._rerun_forever(self.watch_ticker_loop, symbol))
        task.add_done_callback(self._end_ticker_task)
        self._ticker_tasks[task] = symbol
        self.logger.info(f'Created ticker task for {symbol}')

    async def restart_watcher(self, symbols):
        for task, symbol in self._ticker_tasks.items():
            task.cancel()
            try:
                await task
                self._ticker_tasks.pop(task, None)
            except asyncio.CancelledError:
                if not task.cancelled():
                    self.logger.warning(f'watcher task for {symbol} not cancelled')
        self.logger.info(f'Restarting watcher for {len(symbols)} symbols')

        for symbol in symbols:
            await self._start_one_ticker(symbol)
        await asyncio.sleep(60)

    async def stop_watcher(self):
        not_cancelled = {}
        for task in self._ticker_tasks:
            task.cancel()
            try:
                await task
                self._ticker_tasks.pop(task, None)
            except asyncio.CancelledError:
                if not task.cancelled():
                    self.logger.warning('watcher task not cancelled')
                    not_cancelled[task] = self._ticker_tasks[task]
        self.logger.info('tick cancelled')
        self._ticker_tasks = not_cancelled
        if len(not_cancelled):
            self.logger.info('remaining ticking')

        not_cancelled = {}
        for task in self._lob_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                if not task.cancelled():
                    self.logger.warning('lob task not cancelled')
                    not_cancelled[task] = self._lob_tasks[task]
        self.logger.info('lob cancelled')
        self._lob_tasks = not_cancelled
        self.keep_alive_task.cancel()

        for url, client in self.exchange.clients.items():
            if 'ws' in url:
                await client.close()
        self.logger.info('ws closed')


async def main():
    market = 'bitget'

    if market == 'bitget':
        symbols = ['BTCUSDT', 'LTCUSDT']
        # 'TOMIUSDT', 'MINAUSDT', 'AAVEUSDT', 'EOSUSDT',
        # 'MKRUSDT', 'NEOUSDT',
        # 'CHZUSDT', 'KSMUSDT', 'BATUSDT', 'CRVUSDT',
        # 'COMPUSDT', 'QTUMUSDT', '1INCHUSDT', 'YFIUSDT',
        # 'SNXUSDT', 'ONTUSDT', 'STORJUSDT', 'SUSHIUSDT',
        # 'BANDUSDT', 'APTUSDT', 'ARBUSDT', 'AXSUSDT',
        # 'ENSUSDT']
        endpoint = BitgetMarket(account='0')
    elif market == 'okx':
        symbols = ['BTC/USDT:USDT',
                   'ETH/USDT:USDT', 'POL/USDT:USDT',
                   'LTC/USDT:USDT', 'LINK/USDT:USDT', 'XLM/USDT:USDT',
                   'ATOM/USDT:USDT', 'ETC/USDT:USDT', 'ALGO/USDT:USDT', 'EGLD/USDT:USDT',
                   'MANA/USDT:USDT', 'XTZ/USDT:USDT', 'FIL/USDT:USDT', 'SAND/USDT:USDT',
                   'AAVE/USDT:USDT', 'EOS/USDT:USDT', 'MKR/USDT:USDT', 'NEO/USDT:USDT',
                   'CHZ/USDT:USDT', 'KSM/USDT:USDT', 'BAT/USDT:USDT', 'CRV/USDT:USDT',
                   'COMP/USDT:USDT', 'QTUM/USDT:USDT', '1INCH/USDT:USDT', 'YFI/USDT:USDT',
                   'SNX/USDT:USDT', 'ONT/USDT:USDT', 'STORJ/USDT:USDT', 'SUSHI/USDT:USDT',
                   'BAND/USDT:USDT', 'APT/USDT:USDT', 'ARB/USDT:USDT', 'AXS/USDT:USDT',
                   'BLUR/USDT:USDT', 'BNB/USDT:USDT', 'DYDX/USDT:USDT', 'ENS/USDT:USDT',
                   'IMX/USDT:USDT', 'LRC/USDT:USDT', 'MASK/USDT:USDT',
                   'OP/USDT:USDT', 'STX/USDT:USDT', 'ZIL/USDT:USDT',
                   'APE/USDT:USDT', 'BSV/USDT:USDT', 'ICP/USDT:USDT', 'GMX/USDT:USDT',
                   'HBAR/USDT:USDT', 'LDO/USDT:USDT', 'MAGIC/USDT:USDT', 'SUI/USDT:USDT',
                   'WOO/USDT:USDT', 'TON/USDT:USDT', 'MINA/USDT:USDT', 'WAXP/USDT:USDT']
        endpoint = OkexMarket(account=0, request_timeout=120000)
    elif market == 'bin':
        symbols = ['BTCUSDT', 'LTCUSDT', 'VETUSDT', 'ENSUSDT']
        endpoint = BinanceMarket(account=2)
    else:
        symbols = ['LTCUSDT', 'ENSUSDT']  #, 'ETCUSDT', 'SOLUSDT']
        endpoint = BinanceFutureMarket(account='mel_cm1')
    exchange = endpoint._exchange_async

    await exchange.load_markets()

    event_queue = asyncio.Queue()
    runner = MarketWatch(exchange, '10s', with_lob=True, event_queue=event_queue)

    async def printer_loop(watch):
        start = datetime.datetime.timestamp(datetime.datetime.today())
        end = start
        counter = 0
        while end - start < 3000:
            try:
                event = event_queue.get_nowait()
                if watch.last is not None:
                    if event is not None:
                        counter = counter + 1
                        print(watch.bars_df)
                        print(watch.get_age_data('BTC/USDT:USDT'))
                if counter > 10:
                    counter = 0
                    print('restarting')
                    await watch.restart_watcher(symbols=symbols)
            except asyncio.QueueEmpty:
                await asyncio.sleep(2)
                end = datetime.datetime.timestamp(datetime.datetime.today())
            except asyncio.CancelledError:
                break
            except Exception:
                raise

    create_task(runner.run_watcher(symbols))
    await printer_loop(runner)


    await runner.stop_watcher()
    await exchange.close()


if __name__ == '__main__':
    l = logging.getLogger('market_watch')
    h = logging.StreamHandler(sys.stdout)
    l.addHandler(h)
    l.setLevel(logging.DEBUG)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    run(main())
