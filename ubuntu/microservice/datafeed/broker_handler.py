import logging
import asyncio
import numpy as np
import datetime as dt
from ccxt import ExchangeError, NetworkError, DDoSProtection

from datafeed.utils_online import *

from datafeed.motherfeeder import MotherFeeder
import datafeed.okexfeed as okf
import datafeed.binancefeed as bnf
import datafeed.bybitfeed as bbf
import datafeed.bitgetfeed as bgf
import datafeed.dummyfeed as dyf
import datafeed.hyperliquidfeed as hlf


class BrokerHandler:
    def __init__(self, market_watch, strategy_param, end_point_trade, logger_name, allocator=None):
        """

        :param market_watch: name of exchange for data format names of coins
        :type market_watch:  str
        :param market_trade: name of exchange for trade format names of coins
        :type market_trade:  str
        :param end_point_trade: market endpoint for orders
        :type end_point_trade:
        :param leverage:
        :type leverage: float
        """
        self.market_watch = market_watch
        self._destination = strategy_param.get('send_orders', 'dummy')
        self.is_traded = (self._destination == 'web') or (self._destination == 'nickel')
        self._exec_monitoring = strategy_param.get('monitor_exec', '')
        self._leverage_settable = strategy_param.get('set_leverage', False)
        self._pos_matching = strategy_param.get('pos_matching', False)
        self._use_aum = strategy_param.get('use_aum', False)

        market_trade_name = strategy_param['exchange_trade']
        self._end_point_trade = end_point_trade
        self.market_trade = market_trade_name
        self._logger = logging.getLogger(logger_name)
        self.current_positions = None

    @staticmethod
    def build_end_point(market, account=0) -> MotherFeeder:
        if 'bin' in market and 'fut' not in market:
            end_point = bnf.BinanceMarket(account=account, request_timeout=30000)
        elif 'bin' in market and 'fut' in market:
            end_point = bnf.BinanceFutureMarket(account=account, request_timeout=30000)
        elif market == 'okex' or market == 'okexfut' or market == 'okx' or market == 'okxfut':
            end_point = okf.OkexMarket(account=account, request_timeout=60000)
        elif market == 'bybit':
            end_point = bbf.BybitMarket(account=account, request_timeout=60000)
        elif market == 'bitget':
            end_point = bgf.BitgetMarket(account=account, request_timeout=60000)
        else:
            end_point = dyf.DummyMarket(account)
        return end_point

    @staticmethod
    def symbol_to_ticker(symbol, market_trade):
        """
        Transform a symbol name to ticker format
        :param market_trade: destination market
        :type market_trade: str
        :param symbol: symbol of coin
        :type symbol: str
        :return: symbol
        :rtype: str
        """
        coin, _ = extract_coin_with_factor(symbol)
        new_symbol, _ = build_symbol(coin, market_trade, factor=True, to_quote=False, perp=True)

        return new_symbol


    def symbol_to_market(self, symbol, with_factor, universal=False):
        """
        Transform a symbol name in data exchange format into trade format
        :param with_factor:
        :param symbol: symbol of coin
        :type symbol: str
        :return: symbol
        :rtype: str
        """

        coin, _ = extract_coin_with_factor(symbol)
        new_symbol, _ = build_symbol(coin, self.market_trade, factor=with_factor,
                                     to_quote=(self.market_watch in ['binance']), universal=universal)
        return new_symbol

    def symbol_to_market_with_factor(self, symbol, universal=False):
        """
        Transform a symbol name in data exchange format into trade format and returns the price factor
        :param symbol: symbol of coin
        :type symbol: str
        :param universal: true if universal ccxt name, false if exchange specific name
        :type universal: bool
        :return: symbol
        :rtype: str
        """

        coin, factor1 = extract_coin_with_factor(symbol)
        symbol, factor2 = build_symbol(coin, self.market_trade, universal=universal)

        return symbol, factor1 * factor2

    def symbol_from_market(self, ticker, with_factor):
        """
        Transform a ticker name in trade exchange format into data exchange symbol
        :param with_factor:
        :param ticker: symbol of coin
        :type ticker: str
        :return: symbol
        :rtype: str
        """
        coin, _ = extract_coin_with_factor(ticker)
        new_symbol, _ = build_symbol(coin, self.market_watch, factor=with_factor)
        return new_symbol

    def symbol_from_market_with_factor(self, ticker):
        """
        Transform a ticker name in trade exchange format into data exchange symbol
        :param ticker: symbol of coin
        :type ticker: str
        :return: symbol
        :rtype: str
        """
        coin, factor1 = extract_coin_with_factor(ticker)
        new_symbol, factor2 = build_symbol(coin, self.market_watch)
        return new_symbol, factor1 * factor2

    async def get_aum_async(self, data_handler):
        aum = None
        free = None
        balance = None
        equity = 0
        collateral = ['USDT', 'BTC', 'BNB', 'USDC']

        try:
            balance = await self._end_point_trade.get_cash_async(collateral)
        except ExchangeError as e:
            self._logger.error(f'{e} in get_aum_async')
        except NetworkError as e:
            self._logger.error(f'{e} in get_aum_async')

        if balance is not None:
            free, aum = balance

        try:
            positions = await self.get_remote_positions()  # [entry_date, pos_amount, quantity]
            self.current_positions = positions
            for coin, pos in positions.items():
                amount = pos[1]
                if amount is None:
                    last_quote = await data_handler.get_last(coin)
                    amount = pos[2] * last_quote[0]
                if coin not in collateral and amount != 0:
                    equity = equity + amount  # TODO: utiliser AccountTracker avec un dictionnaire coin: ratio
            self.current_positions['equity'] = equity
        except ExchangeError as e:
            self._logger.error(f'{e} in get_remote_positions')
        except NetworkError as e:
            self._logger.error(f'{e} in get_remote_positions')

        return Aum(free, aum, equity)

    def get_aum(self):
        aum = None
        free = None
        balance = None
        equity = aum
        try:
            balance = self._end_point_trade.get_cash('USDT')
        except ExchangeError as e:
            self._logger.error(f'{e} in get_aum')
        except NetworkError as e:
            self._logger.error(f'{e} in get_aum')

        if balance is not None:
            free, aum, equity = balance

        return Aum(free, aum, equity)

    async def set_leverage(self, perimeter, leverage):
        async def leverage_job(symbol):
            try:
                await asyncio.sleep(np.random.uniform(0.1, 20))
                await self._end_point_trade.set_leverage_async(symbol, leverage)
            except (ExchangeError,DDoSProtection) as e:
                self._logger.error(f'{e} for {symbol}')

        if self.is_leverage_settable():
            self._logger.info(f'Setting leverage to {leverage} for {len(perimeter)} coins')
            tasks = {}
            for coin in perimeter:
                symbol, _ = self.symbol_to_market_with_factor(coin, universal=True)
                task = asyncio.create_task(leverage_job(symbol))
                tasks[task] = coin
            await asyncio.gather(*tasks)

            for task in tasks:
                if task.exception() is not None:
                    self._logger.error(f'Exception for {tasks[task]}: {task.get_stack()}')
        else:
            self._logger.info('Skipping leverage setting')

        return

    def is_execmonitored(self):
        if self._destination == 'dummy':
            return False
        else:
            return True

    def is_leverage_settable(self):
        if self._destination == 'dummy':
            return False
        else:
            return self._leverage_settable

    def is_aum_readable(self):
        if self._destination == 'dummy':
            return False
        # elif self._destination == 'edo':
            # return self._exec_monitoring != 'edotest'
        else:
            return self._use_aum

    def has_pair_matching(self):
        if self._destination == 'dummy':
            return False
        else:
            return self._pos_matching

    def exec_monitoring_type(self):
        return self._exec_monitoring

    async def get_remote_positions(self, symbols=None):
        '''
        Fetch account positions and build a dictionary coin:[entry_date, pos_amount, quantity]
        :param symbols: str
        :return: List[datetime,
        '''
        tickers = None if symbols is None else [self.symbol_to_market(symbol, with_factor=True, universal=True) for symbol in symbols]
        book = await self._end_point_trade.get_positions_async(tickers)
        pos = {}

        for ticker in book:
            coin, factor = self.symbol_from_market_with_factor(ticker)  # TODO check factor
            entry_date = book[ticker][3].timestamp() if book[ticker][3] is not None else None
            pos_amount = book[ticker][1] * factor if book[ticker][1] is not None else None
            quantity = book[ticker][0] * factor
            pos[coin] = [entry_date, pos_amount, quantity]
        return pos

    async def get_remote_coin_positions(self, amount_threshold, persisted, initial_quotes):
        """
        Retrieve account positions and perform a matching with persisted positions
        return a dictionary with coin:[position, quantity, entry_data, entry_exec] and lists:
        coin_to_liquidate, orphan_coin_to_liquidate, dust
        """

        book = await self._end_point_trade.get_positions_async()  # qty, (amount, entry_price, entry_ts)
        coin_book = {}
        coin_processed = []
        dust = []  # too small
        coin_to_liquidate = []  # persisted but exiting from perimeter
        orphan_coin_to_liquidate = []  # not persisted and not in perimeter

        # first check if persisted coins are present
        for coin in persisted:
            coin_info = persisted[coin]
            ticker = self.symbol_to_market(coin, with_factor=True, universal=False)

            if ticker in book:
                coin_processed.append(coin)
                pos_qty = book[ticker][0]

                if coin not in initial_quotes.index:
                    coin_to_liquidate.append([coin, pos_qty])
                    self._logger.warning(f'Found {coin} in current position and persisted book but not in allowed perimeter')

                if 'entry_data' in coin_info:
                    entry_ts = coin_info['entry_data'][1]
                    entry_price = coin_info['entry_data'][0]
                else:
                    entry_ts = None
                    entry_price = initial_quotes[coin]

                entry_exec_price = coin_info.get('entry_exec', 0)

                if entry_exec_price > 0:
                    entry_exec = entry_exec_price
                else:
                    entry_exec = entry_price

                pos_amount = abs(pos_qty) * entry_price

                if abs(pos_amount) > amount_threshold:
                    book_entry = book[ticker][3]

                    if entry_ts is not None:  # priority to persisted entry ts
                        book_entry = entry_ts
                    elif book_entry is not None:
                        book_entry = int(book_entry.timestamp() * 1e9)
                    else:
                        book_entry = int(datetime.today().timestamp() * 1e9)

                    coin_book[coin] = {'position': int(1) if pos_qty > 0 else int(-1),
                                       'quantity': abs(pos_qty),
                                       'entry_data': [entry_price, book_entry],
                                       'entry_exec': entry_exec
                                       }
                else:
                    self._logger.warning(f'Found {coin} in current position and persisted book but too small')
                    coin_to_liquidate.append([coin, pos_qty])

        for ticker in book:
            coin = self.symbol_from_market(ticker, with_factor=True)

            if coin in coin_processed:
                continue
            if coin not in initial_quotes.index:
                self._logger.warning(f'Found {coin} in current position, not persisted and not in allowed perimeter')
                if 'BNB' not in coin:  # ugly hack
                    orphan_coin_to_liquidate.append([coin, book[ticker][0]])
                continue
            pos_amount = book[ticker][0] * initial_quotes[coin]

            if abs(pos_amount) > amount_threshold:
                self._logger.warning(f'Found {coin} in current position and not in persisted book')
                entry_date = book[ticker][3]
                if entry_date is None:
                    entry_date = dt.datetime.today()
                entry_price = book[ticker][2]
                if entry_price is None:
                    entry_price = initial_quotes[coin]
                coin_book[coin] = {'position': int(1) if book[ticker][0] > 0 else int(-1),
                                   'quantity': abs(book[ticker][0]),
                                   'entry_data': [entry_price, int(entry_date.timestamp() * 1e9)],
                                   'entry_exec': entry_price
                                   }
            else:
                self._logger.info(f'Found {coin} in current position and too small')
                dust.append([coin, book[ticker][0]])

        return coin_book, coin_to_liquidate, orphan_coin_to_liquidate, dust

    async def get_remote_pair_positions(self, amount_threshold, entrytime_threshold, clusters, persisted):
        """
        Perform pair-matching on amount in position.
        If pos amount < threshold, it is dust
        A pair is a match if each amount > threshold and sign(pos1.pos2) < 0.
        The order of the pair is checked against cluster list and reversed if needed
        Persistance is based on semireduced symbol name for pair and exact ticker name for single coins
        Quantity/price is based on exact ticker

        :param persisted:
        :param amount_threshold:
        :type amount_threshold: float
        :param entrytime_threshold:
        :type entrytime_threshold: float
        :param clusters:
        :type clusters: list[str]
        :return: pair state, unmatched coins
        :rtype: Dict, List
        """
        book = await self._end_point_trade.get_positions_async()
        self._logger.info(f'Starting matching with {len(book)} positions in account')
        coin_processed = []
        dust = []  # too small
        pair_to_exit = []  # persisted but exiting from perimeter
        orphan_coin_to_liquidate = []  # not persisted and not in perimeter
        pair_book = {}

        # Start with persisted pair
        for pair in persisted:
            symbol1, symbol2 = parse_pair(pair)
            ticker1, f1 = self.symbol_to_market_with_factor(symbol1, universal=True)
            ticker2, f2 = self.symbol_to_market_with_factor(symbol2, universal=True)
            pair_info = persisted[pair]
            if 'position' not in pair_info or pair_info['position'] == 0:
                self._logger.info(f'Skipping {pair} with DB pos = 0')
                continue

            if 'entry_data' in pair_info:
                entry_ts = pair_info['entry_data'][2]
            else:
                entry_ts = None
            if 'entry_exec' in pair_info:
                entry_exec = pair_info['entry_exec']
            else:
                entry_exec = None

            if ticker1 in book and ticker2 in book:
                book_entry = book[ticker1][3]
                self._logger.info(f'Found persisted {pair} in account')

                if entry_ts is not None:  # persisted entry ts has priority
                    book_entry = entry_ts
                else:
                    book_entry = int(book_entry.timestamp() * 1e9)
                if abs(book[ticker1][1]) > amount_threshold and abs(
                        book[ticker2][1]) > amount_threshold:  # pos OK
                    if pair in clusters:
                        if np.sign(book[ticker1][1]) * np.sign(book[ticker2][1]) > 0:  # pos OK
                            self._logger.info(f'Pos for {pair} in account is not LS')
                        else:
                            if entry_exec is None: # pas de prix d'exec d'entrée
                                entry_exec = [book[ticker1][2], book[ticker2][2]]
                            # vérifier la somme des 2 et réajuster
                            pair_book[pair] = {'position': int(1) if book[ticker1][0] > 0 else int(-1),
                                               'quantity': [book[ticker1][0], book[ticker2][0]],
                                               'entry_data': [book[ticker1][2], book[ticker2][2], book_entry],
                                               'entry_exec': entry_exec,
                                               }
                            coin_processed.extend([symbol1, symbol2])
                            self._logger.info(f'Positions for {pair} in account seem OK, it\'s validated')
                    else:
                        self._logger.info(f'Pos for {pair} in account OK but pair is not selected...we set it for exit')
                        pair_to_exit.append(pair)
                        coin_processed.extend([symbol1, symbol2])
                else:
                    self._logger.info(f'Pos for {pair} in account too small..we set it to dust')
            else:
                missing = []
                if ticker1 not in book:
                    missing.append(ticker1)
                if ticker2 not in book:
                    missing.append(ticker2)
                self._logger.info(f'Missing leg for {pair} in account: {",".join(missing)}')
        for ticker1 in book:
            coin1 = self.symbol_from_market(ticker1, with_factor=True)
            if coin1 in coin_processed:
                continue

            entry_date = book[ticker1][3]
            pos_amount = book[ticker1][1]  # we check amount

            if abs(pos_amount) < amount_threshold:
                self._logger.info(f'Pos for {coin1} in account too small')
                coin_processed.append(coin1)
                dust.append([coin1, book[ticker1][0]])
                continue
            for ticker2 in book:
                coin2 = self.symbol_from_market(ticker2, with_factor=True)
                if coin2 == coin1 or coin2 in coin_processed:
                    continue
                peer_date = book[ticker2][3]
                peer_amount = book[ticker2][1]

                if abs(peer_amount) > amount_threshold:
                    pair = make_pair(coin1, coin2)
                    self._logger.info(f'Positions for {pair} in account are good')

                    # We stop checking for entry time in matching
                    # if entry_date is not None and peer_date is not None:
                    #     if np.abs(entry_date.timestamp() - peer_date.timestamp()) > entrytime_threshold:
                    #         self._logger.info(f'Entry dates for {pair} do not match')
                    #         continue
                    # else:
                    #     entry_date = dt.datetime.today()
                    #  looks like a long-short

                    # is one of the 2 pairs in clusters ?
                    if pair not in clusters:
                        ticker1, ticker2 = ticker2, ticker1
                        coin1, coin2 = coin2, coin1
                        pair = make_pair(coin1, coin2)

                    if pair in clusters:
                        if entry_date is None:
                            if peer_date is not None:
                                entry_date = peer_date
                            else:
                                entry_date = dt.datetime.today()
                        self._logger.info(f'Found and matched {pair} not persisted in account')

                        pair_book[pair] = {'position': int(1) if book[ticker1][0] > 0 else int(-1),
                                           'quantity': [book[ticker1][0], book[ticker2][0]],
                                           'entry_data': [book[ticker1][2], book[ticker2][2], int(entry_date.timestamp() * 1e9)],
                                           'entry_exec': [book[ticker1][2], book[ticker2][2]]
                                           }
                        coin_processed.extend([coin1, coin2])
                    else:
                        self._logger.info(f'Found {pair} but it is not eligible')

        for ticker in book:
            coin = self.symbol_from_market(ticker, with_factor=True)
            if coin not in coin_processed:
                self._logger.info(f'Found unmatched {coin}')
                orphan_coin_to_liquidate.append([coin, book[ticker][0]])

        return pair_book, pair_to_exit, orphan_coin_to_liquidate, dust

    async def close_exchange_async(self):
        self._logger.info('Closing exchange connection')
        await self._end_point_trade._exchange_async.close()

    async def get_max_short_amount(self, coins):
        if not 'spot' in self.market_trade:
            return {}
        for coin in coins:
            symbol, factor = self.symbol_to_market_with_factor(coin)
            qty = self._end_point_trade



    def get_contract_qty_from_coin(self, coin, quantity):
        info = self._end_point_trade._exchange.market(coin)
        factor = 1
        if 'contractSize' in info and info['contractSize'] is not None:
            factor = info['contractSize']
        return quantity / factor

    def get_min_notional(self, coin):
        info = self._end_point_trade._exchange.market(coin)
        min_notional = 5

        if 'limits' in info and info['limits'] is not None:
            if 'cost' in info['limits']:
                min_notional = info['limits']['cost']['min']
        return min_notional

