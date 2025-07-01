import os
import sys

import aiohttp
import pandas as pd
import websockets
import asyncio
import uuid
import logging
import json
import dotenv
from datetime import datetime, timezone
import time
import numpy as np
import traceback
from string import ascii_letters, digits
from random import choice

# import http_nt_pclient
# from http_nt_pclient.rest import ApiException

from reporting.bot_reporting import TGMessenger
from datafeed.broker_handler import BrokerHandler
from core_ms.bot_event import EventType, CoinOrderEvent
from datafeed.utils_online import parse_pair, today_utc

ONE_BP = 1e-4

class RateLimiter:
    def __init__(self, min_interval):
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

def order_reverse(s1, s2, spread_shift, action, target_size1, target_size2, p1, p2):
    if p2 < p1:
        s1, s2 = s2, s1
        p1, p2 = p2, p1
        target_size1, target_size2 = target_size2, target_size1
        action = - action
    spread = (1 - p1 / p2) / ONE_BP

    return s1, s2, action, target_size1, target_size2, spread, p1, p2


async def post_any(session, upi, json_req):
    async with session.post(upi, json=json_req) as resp:
        response = await resp.read()
        return {'status_code': resp.status, 'text': response}


async def get_any(session, upi):
    async with session.get(upi) as resp:
        response = await resp.read()
        return {'status_code': resp.status, 'text': response}

def build_broker(destination, account_trade, broker_handler, market_name, chat_channel, use_dummy=False):
    if use_dummy or destination == 'dummy':
        return DummyBroker(market=market_name, account=account_trade)
    elif destination == 'edo':
        return EdoSpreaderBroker(broker_handler=broker_handler, market=market_name, chat_channel=chat_channel,
                                account=account_trade)
    elif destination == 'web':
        bro = WebSpreaderBroker(broker_handler=broker_handler, market=market_name, chat_channel=chat_channel,
                                account=account_trade)
        return bro
    elif destination == 'nickel':
        bro = NickelBroker(broker_handler=broker_handler, market=market_name, chat_channel=chat_channel,
                          account=account_trade)
        return bro
    else:
        return None

class WebSpreaderListener:
    AMAZON_WS_UPI = 'ws://54.249.138.8:8080/wsapi/strat/update'

    def __init__(self, logger):
        self.listener_task = None
        self.subscriptions = set()
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000,month=1,day=1, tz='UTC')}
        self._logger = logger

    def message_age(self) -> float:
        """
        Age in seconds
        :return: message age
        """
        return (today_utc() - self.results['last_modified']).total_seconds()

    async def _listen(self):
        greeting = 'Allo?'
        self._logger.info('starting listener')
        async with websockets.connect(self.AMAZON_WS_UPI, max_size=4194304) as websocket:
            await websocket.send(greeting)
            while True:
                message = await websocket.recv()
                if message:
                    messages = json.loads(message)
                    events = messages.get('stratEvents', [])
                    self.results['last_modified'] = today_utc()

                    for event in events:
                        strat_id = event.get('stratId', '')
                        if strat_id in self.subscriptions:
                            if 'results' in event:
                                result = event.get('results', '')
                            else:
                                result = 'missing'
                            self.results[strat_id] = result

    def _start_listener(self):
        async def rerun_forever(coro, *args, **kwargs):
            while True:
                try:
                    await coro(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg = traceback.format_exc()
                    self.results['errors'].append(msg)
                    self._logger.warning(f'listener said {msg}')
                    await asyncio.sleep(60)

        def done_listener(task):
            self.listener_task = None

        self.results['errors'].clear()
        self.listener_task = asyncio.create_task(rerun_forever(self._listen))
        self.listener_task.add_done_callback(done_listener)

    def subscribe(self, strat_id):
        self.subscriptions.add(strat_id)
        if self.listener_task is None:
            self._start_listener()

    async def stop_listener(self, strat_id=None):
        if strat_id is not None:
            self.subscriptions.remove(strat_id)
            self.results.pop(strat_id, None)
            if len(self.subscriptions) > 0:
                return
        self.subscriptions.clear()
        if self.listener_task is not None:
            self._logger.info('cancelling listener')
            self.listener_task.cancel()

    def _get_strat_result(self, strat_id):
        if strat_id in self.results:
            return self.results[strat_id]
        else:
            self._logger.warning(f'retrieving non existing strat from listener with state {self.results["errors"]}')
            return {}

    def result_from_watcher_spread(self, strat_id):
        """
        Spread execution for 2 legs
        """
        result = self._get_strat_result(strat_id=strat_id)
        try:
            leg1_exec_qty = float(result.get('leg1ExecQty', 0))  # toujours signé
            leg2_exec_qty = float(result.get('leg2ExecQty', 0))
            leg1_exec_prc = float(result.get('leg1AvgExecPrc', 0))
            leg2_exec_prc = float(result.get('leg2AvgExecPrc', 0))
            spread = float(result.get('premium', 0))
        except ValueError:
            self._logger.info(f'WebSpreader listener got invalid spread result {result}')
            return None
        return (leg1_exec_qty, leg2_exec_qty), spread, (leg1_exec_prc, leg2_exec_prc)

    def result_from_watcher_twin(self, strat_id):
        """
        Double execution for 2 legs
        """
        result1 = self._get_strat_result(strat_id=strat_id+'L1')
        result2 = self._get_strat_result(strat_id=strat_id+'L2')
        spread = 0.0
        try:
            leg1_exec_qty = float(result1.get('execQty', 0))
            leg2_exec_qty = float(result2.get('execQty', 0))
            leg1_exec_prc = float(result1.get('avgPrc', 0))
            leg2_exec_prc = float(result2.get('avgPrc', 0))
        except ValueError:
            self._logger.info(f'WebSpreader listener got invalid twin result {result1}, {result2}')
            return None
        return (leg1_exec_qty, leg2_exec_qty), spread, (leg1_exec_prc, leg2_exec_prc)

    def result_from_watcher_single(self, strat_id):
        result = self._get_strat_result(strat_id=strat_id)

        try:
            exec_qty = float(result.get('execQty', 0))
            exec_prc = float(result.get('avgPrc', 0))
        except ValueError:
            self._logger.info(f'Spreader listener sent invalid single result {result}')
            return None
        return exec_qty, exec_prc

class WebSpreaderBroker:
    AMAZON_URL2 = 'http://54.249.138.8:8080/api'
    AMAZON_UPI_CREATE = AMAZON_URL2 + '/strat/spreader'
    AMAZON_UPI_STOP = AMAZON_URL2 + '/strat/stop'
    AMAZON_UPI_SPREADER_ALL_STOP = AMAZON_URL2 + '/strat/stopAllMarketSpreader'
    AMAZON_UPI_SPREADER_DELETE_ALL = AMAZON_URL2 + '/strat/deleteAllMarketSpreader'
    AMAZON_UPI_SINGLE = AMAZON_URL2 + '/manualOrder/createOrUpdate'
    AMAZON_UPI_SINGLE_STOP = AMAZON_URL2 + '/manualOrder/stop'
    AMAZON_UPI_SINGLE_ALL_STOP = AMAZON_UPI_STOP + '/ManualOrder'
    AMAZON_UPI_SINGLE_DELETE_ALL = AMAZON_URL2 + '/manualOrder/deleteAll'
    # api/strat/stop/ManualOrder /api/
    # /api/manualOrder/deleteAll
    # /api/strat/deleteAllMarketSpreader

    ACCOUNT_DICT = {
        'okx':
            {
                'edo1': 'edo1',
                'edo3': 'edo3'
            },
        'bybit_fut':
            {
                '1': 'pairspreading1',
                '2': 'pairspreading2',
                '3': 'pairspreading3'
            },
        'bin_spot':
            {
                '1': 'pairspreading_api1',
                '2': 'pairspreading_api2'
            },
        'bitget_fut':
            {
                '1': 'Pairspreading1',
                '2': 'Pairspreading2',
                'h1': 'hedge1',
            },
        'bin_fut':
            {
                'mel_cm1': 'mel_cm1',
                'mel_cm2': 'mel_cm2',
                'mel_cm3': 'mel_cm3',
                'nickel_cm1': 'nickel_cm1'
            }
    }

    def __init__(self, market, broker_handler, account=None, chat_channel=''):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler
        self.long_short_split = True   # if True, split order in 2

        if 'ok' in market:
            self.exchange = 'okx'
        elif 'bin' in market and 'fut' in market:
            self.exchange = 'bin_fut'
        elif 'bin' in market:
            self.exchange = 'bin_spot'
        elif 'bybit' in market:
            self.exchange = 'bybit_fut'
        elif 'bitget' in market:
            self.exchange = 'bitget_fut'
        else:
            raise ValueError(f'Web broker does not support {market} market')

        if 'nickel' in account:
            raise ValueError('Bad broker for nickel account')
        if account not in WebSpreaderBroker.ACCOUNT_DICT[self.exchange]:
            raise ValueError('Bad account')
        self.account_name = WebSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.pair_config = {'legOneExchange': self.exchange,
                            'legTwoExchange': self.exchange,
                            'legOneAccount': self.account_name,
                            'legTwoAccount': self.account_name,
                            'legOneOffset': 'Open', 'legOneUseMargin': False,
                            'legTwoOffset': 'Open', 'legTwoUseMargin': False,
                            'qtyMethod': 'IsoUsdt',
                            'start': 'Automatic'
                            }
        self.use_spread = False
        self.persistence = {}  # dict with uid of order, type, parameters, status
        self.logger = logging.getLogger(f'broker_web_{market}_{account}')
        self.listener = WebSpreaderListener(self.logger)

    @property
    def is_traded(self):
        return True

    async def close(self):
        try:
            if self.listener is not None:
                await self.listener.stop_listener()
        except Exception as e:
            self.logger.error(f'Exception when closing pool manager: {e}')
        return

    async def delete_all(self):
        async with aiohttp.ClientSession() as session:
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SPREADER_DELETE_ALL, json_req={})
            await post_any(session, WebSpreaderBroker.AMAZON_UPI_SINGLE_DELETE_ALL, json_req={})

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, coin=event.ticker, action=event.action, price=event.price,
                                                target_quantity=event.quantity, comment=event.comment,
                                                translate_qty_incontracts=True, use_algo=True, nature=event.nature)

    @staticmethod
    def get_max_order_size(target_quantity, amount_threshold=1000, price=None):
        if price is None or np.isnan(price):
            max_order_size = target_quantity * 0.9
            amount = None
        else:
            amount = target_quantity * price

            if amount > amount_threshold:
                max_amount = np.random.uniform(low=amount_threshold * 0.8, high=amount_threshold * 1.6)
                max_order_size = max_amount / price
                if max_order_size > target_quantity * 0.9:
                    max_order_size = target_quantity * 0.9
            else:
                max_order_size = amount_threshold / price
        if max_order_size > target_quantity * 0.9:
            max_order_size = target_quantity * 0.9

        return max_order_size, amount

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """

        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        p1, p2 = prices

        if 'LS' in comment and self.long_short_split:
            nature1 = 'exit' if nature[0] == 'x' else 'entry'
            nature2 = 'exit' if nature[1] == 'x' else 'entry'
            tasks = []
            self.logger.info('splitting order into 2 manual orders')
            tasks.append(asyncio.create_task(self.send_simple_order(id + 'L1', coin=s1, action=action, price=p1,
                                                                    target_quantity=target_size1,
                                                                    comment='leg1_' + nature1 + '_' + comment,
                                                                    translate_qty_incontracts=True, use_algo=True,
                                                                    nature=nature[0])))
            tasks.append(asyncio.create_task(self.send_simple_order(id + 'L2', coin=s2, action=-action, price=p2,
                                                                    target_quantity=target_size2,
                                                                    comment='leg2_' + nature2 + '_' + comment,
                                                                    translate_qty_incontracts=True, use_algo=True,
                                                                    nature=nature[1])))

            all_responses = await asyncio.gather(*tasks)
            try:
                response = all_responses[0] & all_responses[1]
            except:
                response = False
            return response

        with_tg_message = self._tg_channel != ''

        spread = spread_shift

        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'

        symbol1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1, universal=False)
        symbol2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2, universal=False)
        self.logger.info(f'Preparing order for {s1},{s2} with symbols {symbol1},{symbol2}')

        p1 /= factor1
        p2 /= factor2
        target_size1 *= factor1

        target_size1 = round(target_size1, 6)
        spread = round(spread, 8)
        max_order_size1, _ = WebSpreaderBroker.get_max_order_size(target_size1, price=p1)
        max_order_size1 = round(max_order_size1, 6)

        config = {
            'legOneSymbol': symbol1, 'legTwoSymbol': symbol2,
            'targetSize': target_size1,  # coin leg1
            'maxOrderSize': max_order_size1,  # pour découpage,
            'spreadDirection': direction
        }
        config.update(self.pair_config)

        if self.use_spread:
            config.update({'targetSpread': spread})
            request = {'clientOrderId': id, 'orderMsg': 0, 'strategyType': 'SpreaderLimit',
                       'spreaderLimitConfiguration': config}
        elif self.exchange == 'bin_spot':
            if nature[0] == 'x':
                side_effect1 = 'AUTO_REPAY'
            else:
                side_effect1 = 'MARGIN_BUY'
            if nature[1] == 'x':
                side_effect2 = 'AUTO_REPAY'
            else:
                side_effect2 = 'MARGIN_BUY'

            config.update({'targetSpread': spread,
                           'targetPrice1': p1,
                           'targetPrice2': p2,
                           'legOneSideEffect': side_effect1,
                           'legTwoSideEffect': side_effect2,
                           'legOneUseMargin': True,
                           'legTwoUseMargin': True,
                           'clientRef': comment})
            request = {'clientOrderId': id, 'orderMsg': 0, 'spreaderConfiguration': config}
        else:
            reduce1 = nature[0] == 'x'
            reduce2 = nature[1] == 'x'
            config.update({'targetSpread': spread,
                           'targetPrice1': p1,
                           'targetPrice2': p2,
                           'legOneReduceOnly': reduce1,
                           'legTwoReduceOnly': reduce2,
                           'clientRef': comment})
            request = {'clientOrderId': id, 'orderMsg': 0, 'spreaderConfiguration': config}

        message = ''
        response_broker = None
        self.logger.info(f'sending order to {WebSpreaderBroker.AMAZON_UPI_CREATE}: {request}')

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    tg_rez = await TGMessenger.send_message_async(
                        session=session, comment=f'{self.exchange}/{self.account_name}:{comment}',s1=symbol1, s2=symbol2,
                        direction=direction, spread=spread, p1=p1, p2=p2, chat_channel=self._tg_channel)
                else:
                    tg_rez = {'ok': True}
                response_broker = await post_any(session, WebSpreaderBroker.AMAZON_UPI_CREATE, json_req=request)

        except aiohttp.ClientConnectorError as e:
            message = f'web broker exception: {e}'
        except Exception as e:
            message = f'web broker exception: {e}'

        if not tg_rez.get('ok', True):
            self.logger.warning(f'TG problem')
        if response_broker is None or 'status_code' not in response_broker:
            message = f'broker null or ill-formated response' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        elif response_broker['status_code'] != 200:
            message = f'broker response: {response_broker["status_code"]}, {response_broker["text"]}' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=False, nature=''):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """

        with_tg_message = self._tg_channel != ''
        direction = 'BUY' if action > 0 else 'SELL'
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(coin, universal=False)
        ticker, _ = self.broker_handler.symbol_to_market_with_factor(coin, universal=True)

        self.logger.info(f'Preparing order for {symbol} with ticker {ticker}')

        if price is not None:
            price /= factor
        target_quantity *= factor

        if self.exchange != 'bin_spot' and translate_qty_incontracts:
            qty = self.broker_handler.get_contract_qty_from_coin(ticker, target_quantity)
            self.logger.info(f'Converted {target_quantity} {ticker} to {qty} contracts')
        else:
            qty = target_quantity

        max_order_size, amount = WebSpreaderBroker.get_max_order_size(qty, price=price)

        # max_order_size = 37

        min_notional = self.broker_handler.get_min_notional(symbol)
        self.logger.info(f'Received minimum notional of {min_notional} for {symbol}')

        target_quantity = round(qty, 6)
        max_order_size = round(max_order_size, 6)

        config = {'symbol': symbol,
                  'exchange': self.exchange,
                  'account': self.account_name,
                  'offset': 'Open',
                  'useMargin': True,
                  'targetSize': target_quantity,
                  'maxOrderSize': max_order_size,  # pour découpage,
                  'maxAliveOrderTime': 0,
                  'direction': direction,
                  'childOrderDelay': 3000,
                  'start': 'Automatic',
                  'marginMode': 'crossed',
                  'clientRef': comment}
        if nature == 'x':
            if self.exchange != 'bin_fut' or (min_notional is not None and amount is not None and amount < min_notional):
                config.update(
                {
                    'reduceOnly': True
                })
        else:
            config.update(
                {
                    'reduceOnly': False
                })
        if self.exchange == 'bin_spot':
            if nature == 'n' and action < 0:
                side_effect = 'MARGIN_BUY'
                config['sideEffectType'] = side_effect
            elif nature == 'x' and action > 0:
                side_effect = 'AUTO_REPAY'
                config['sideEffectType'] = side_effect

        childOrderDelay = int(1000 * np.random.uniform(0.2, 1))
        if use_algo:
            maxAliveOrderTime = int(1000 * np.random.uniform(5, 10))
            config.update(
                {
                    'type': 'LIMIT_WITH_LEEWAY',
                    'maxStratTime': 30 * 60 * 1000,  # 30 minutes
                    'timeInForce': 'GTC',
                    'maxAliveOrderTime': maxAliveOrderTime,  # 15 secondes
                    'childOrderDelay': childOrderDelay,
                    # 'maxRetryAsLimitOrder': 10,
                })
        elif price is not None and amount > 1000:
            config.update(
                {
                    'price': price,
                    'type': 'LIMIT',
                    'maxAliveOrderTime': 13 * 1000,  # 15 secondes
                    'timeInForce': 'GTC'
                })
        else:
            config.update({
                          'childOrderDelay': childOrderDelay,
                          'type': 'MARKET',  # no price for market order
                          })

        request = {'clientOrderId': order_id, 'orderMsg': 0, 'manualOrderConfiguration': config}

        self.logger.info(f'sending order to {WebSpreaderBroker.AMAZON_UPI_SINGLE}: {request}')
        response_broker = None
        message = ''

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    tg_rez = await TGMessenger.send_message_async(
                        session,
                        f'{self.exchange}/{self.account_name}:{comment}',
                        coin, '',
                        direction,0, price, '', chat_channel=self._tg_channel)
                else:
                    tg_rez = {'ok': True}

                response_broker = await post_any(session, WebSpreaderBroker.AMAZON_UPI_SINGLE, json_req=request)
        except (aiohttp.ClientConnectorError, TimeoutError) as e:
            message = f'web broker exception: {e}'
        except Exception as e:
            message = f'web broker exception: {e}'

        if not tg_rez.get('ok', True):
            self.logger.warning(f'TG prem')
        if response_broker is None or 'status_code' not in response_broker:
            message = f'broker null or ill-formated response' + message
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        elif response_broker['status_code'] != 200:
            message = f'broker response: {response_broker["status_code"]}{response_broker["text"]}{message}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    async def stop_order(self, order_id):
        code = 0

        try:
            upi = WebSpreaderBroker.AMAZON_UPI_STOP + f'/{order_id}'
            async with aiohttp.ClientSession() as session:
                response_broker = await get_any(session, upi)
            code = response_broker.get('status_code', 400)
            message = response_broker['text']
        except aiohttp.ClientConnectorError as e:
            message = f'Web_broker ClientConnectorError in stop_order: {e}'
        except Exception as e:
            message = f'web_broker exception in stop_order: {e}'

        if code != 200:
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    async def stop_simple_order(self, order_id):
        code = 0

        try:
            upi = WebSpreaderBroker.AMAZON_UPI_SINGLE_STOP + f'/{order_id}'
            async with aiohttp.ClientSession() as session:
                response_broker = await get_any(session, upi)
            code = response_broker.get('status_code', 400)
            message = response_broker['text']
        except aiohttp.ClientConnectorError as e:
            message = f'Web_broker ClientConnectorError in stop_simple_order: {e}'
        except Exception as e:
            message = f'web_broker exception in stop_simple_order: {e}'

        if code != 200:
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
            return False
        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id

""" class NickerListener:
    def __init__(self, logger, twap_api):
        self.listener_task = None
        self.subscriptions = set()
        self.results = {'errors': [], 'last_modified': pd.Timestamp(year=2000,month=1,day=1, tz='UTC')}
        self.twap_api_instance = twap_api
        self._logger = logger

    def message_age(self) -> float:

        return (today_utc() - self.results['last_modified']).total_seconds()

    async def _retrieve_twap(self):
        strat_to_retrieve = self.subscriptions.copy()  # copy to avoid issues with concurrent modification
        for strat_id in strat_to_retrieve:
            try:
                api_response = await self.twap_api_instance.twap_strategy('EXT-TOKYO-CM', strat_id)
                self.results['last_modified'] = today_utc()
                parameters = api_response.parameters

                if hasattr(parameters, 'statistics'):
                    side = -1 if parameters.side == 'SELL' else 1
                    statistics = parameters.statistics
                    average_execution_price = statistics.average_execution_price
                    progress = statistics.progress
                    traded = statistics.traded
                    result = {'execQty': traded * side,
                              'avgPrc': average_execution_price,
                              'progress': progress}
                else:
                    result = {}
                self.results[strat_id] = result
                await asyncio.sleep(4)  # to avoid rate limiting
            except ApiException as e:
                message = f'Nickel broker error while retrieving strategies states: {e} '
                self._logger.error(message)
            except Exception as e:
                message = f'Error while _retrieve_twap: {e} '
                self._logger.error(message)

    def _start_listener(self):
        self._logger.info('starting Nickel listener')
        async def run_forever():
            while True:
                try:
                    await self._retrieve_twap()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg = traceback.format_exc()
                    self.results['errors'].append(msg)
                    self._logger.warning(f'Nickel listener said {msg}')
                await asyncio.sleep(10)

        def done_listener(task):
            self.listener_task = None
            self.results.update({'last_modified': today_utc()})

        self.results['errors'].clear()
        self.listener_task = asyncio.create_task(run_forever())
        self.listener_task.add_done_callback(done_listener)

    def subscribe(self, strat_id):
        self.subscriptions.add(strat_id)
        if self.listener_task is None:
            self._start_listener()

    async def stop_listener(self, strat_id=None):
        if strat_id is not None:
            self.subscriptions.remove(strat_id)
            self.results.pop(strat_id, None)
            if len(self.subscriptions) > 0:
                return
        self.subscriptions.clear()
        if self.listener_task is not None:
            self._logger.info('cancelling listener')
            self.listener_task.cancel()

    def _get_strat_result(self, strat_id):
        if strat_id in self.results:
            return self.results.get(strat_id, {})
        else:
            self._logger.warning(f'retrieving non existing strat {strat_id} from nickel listener with errors {self.results["errors"]}')
            return {}

    def result_from_watcher_spread(self, strat_id):

        result = self._get_strat_result(strat_id=strat_id)
        try:
            leg1_exec_qty = float(result.get('leg1ExecQty', 0))  # toujours signé
            leg2_exec_qty = float(result.get('leg2ExecQty', 0))
            leg1_exec_prc = float(result.get('leg1AvgExecPrc', 0))
            leg2_exec_prc = float(result.get('leg2AvgExecPrc', 0))
            spread = float(result.get('premium', 0))
        except ValueError:
            self._logger.info(f'WebSpreader listener got invalid spread result {result}')
            return None
        return (leg1_exec_qty, leg2_exec_qty), spread, (leg1_exec_prc, leg2_exec_prc)

    def result_from_watcher_twin(self, strat_id):

        result1 = self._get_strat_result(strat_id=strat_id+'L1')
        result2 = self._get_strat_result(strat_id=strat_id+'L2')
        spread = 0.0
        try:
            leg1_exec_qty = float(result1.get('execQty', 0))
            leg2_exec_qty = float(result2.get('execQty', 0))
            leg1_exec_prc = float(result1.get('avgPrc', 0))
            leg2_exec_prc = float(result2.get('avgPrc', 0))
        except ValueError:
            self._logger.info(f'WebSpreader listener got invalid twin result {result1}, {result2}')
            return None
        return (leg1_exec_qty, leg2_exec_qty), spread, (leg1_exec_prc, leg2_exec_prc)

    def result_from_watcher_single(self, strat_id):
        result = self._get_strat_result(strat_id=strat_id)

        try:
            exec_qty = float(result.get('execQty', 0))
            exec_prc = float(result.get('avgPrc', 0))
        except ValueError:
            self._logger.info(f'Spreader listener sent invalid single result {result}')
            return None
        return exec_qty, exec_prc

class NickelBroker:
    AGGRESSION_CAP = 0.1
    AGGRESSION_FLOOR = 0.2
    MAX_EXECUTION_TIME = 12  # minutes
    STEP_TIME = 40 # seconds
    STEP_AMOUNT = 2000 # USDT

    @staticmethod
    def get_max_order_size(target_quantity, amount_threshold=STEP_AMOUNT, price=None):
        if price is None or np.isnan(price):
            max_order_size = target_quantity * 0.9
        else:
            amount = target_quantity * price

            if amount > amount_threshold:
                max_amount = np.random.uniform(low=amount_threshold * 0.8, high=amount_threshold * 1.6)
                max_order_size = max_amount / price
                if max_order_size > target_quantity * 0.9:
                    max_order_size = target_quantity * 0.9
            else:
                max_order_size = amount_threshold / price
        if max_order_size > target_quantity * 0.9:
            max_order_size = target_quantity * 0.9

        return max_order_size

    def __init__(self, broker_handler, market='binancefut', account='1', chat_channel=''):
        self.broker_handler = broker_handler
        self.logger = logging.getLogger(f'broker_nickel_{market}_{account}')
        self.username = os.environ['NICKEL_TRADER_CM1_KEY']
        self.password = os.environ['NICKEL_TRADER_CM1_SECRET']
        self.host = os.environ['NICKEL_TRADER_URL']
        self.site = 'EXT-TOKYO-CM'
        self.account_id = 'CM'
        self.exchange = 'BINANCE_FUTURES_USDT'
        self.rate_limiter = RateLimiter(3)  # 3 requests per second

        try:
            self.twap_api_instance = http_nt_pclient.TWAPApi()
            self.api_instance = http_nt_pclient.StrategyApi()
        except Exception as e:
            self.logger.error(f'Error initializing TWAPApi: {e}')
            raise
        self.twap_api_instance.api_client.configuration.username = self.username
        self.twap_api_instance.api_client.configuration.password = self.password
        self.twap_api_instance.api_client.configuration.host = self.host
        self.api_instance.api_client.configuration.username = self.username
        self.api_instance.api_client.configuration.password = self.password
        self.api_instance.api_client.configuration.host = self.host
        self.base_params = dict(
            account=self.account_id,
            guaranteed_execution='YES',
            aggression_cap=NickelBroker.AGGRESSION_CAP,
            aggression_floor=NickelBroker.AGGRESSION_FLOOR,
            start=True
        )
        self.id = None
        self.logger.info('Nickel broker initialized')
        self.listener = NickerListener(self.logger, self.twap_api_instance)

    @property
    def is_traded(self):
        return True

    async def stop_all(self, delete_all=False):
        try:
            # Returns list of user strategies
            api_response = await self.api_instance.strategies(self.site)
        except ApiException as e:
            message = f'Nickel broker error {e}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()
            return False

        self.logger.info('Stopping Nickel listener')
        await self.listener.stop_listener()

        ids = {twap.execution_id: twap.state for twap in api_response.twaps}
        print(ids)

        running_ids = [id for id, state in ids.items() if state == 'RUNNING']
        stopped_ids = [id for id, state in ids.items() if state != 'RUNNING']

        if running_ids:
            body = http_nt_pclient.MultipleStrategiesActionRequest(action='STOP', execution_ids=running_ids)
            self.logger.info(f'Stopping Nickel TWaps {running_ids}')
            try:
                api_response = await self.api_instance.change_state_multi(body, self.site)
            except ApiException as e:
                self.logger.warning(f'Exception when calling StrategyApi->change_state_multi: {e}')

        if stopped_ids and delete_all:
            body = http_nt_pclient.ExecutionIds(execution_ids=stopped_ids)
            self.logger.info(f'Deleting Nickel TWaps {stopped_ids}')
            try:
                api_response = await self.api_instance.delete_multi(body, self.site)
            except ApiException as e:
                self.logger.warning(f'Exception when calling StrategyApi->delete_multi: {e}')

            print(api_response)

        self.logger.info(f'Cleaning up connection pool for Nickel broker')
        await self.close()

        return True

    async def close(self):
        try:
            if self.listener is not None:
                await self.listener.stop_listener()
            await self.api_instance.api_client.rest_client.pool_manager.close()
            await self.twap_api_instance.api_client.rest_client.pool_manager.close()
        except Exception as e:
            self.logger.error(f'Exception when closing pool manager: {e}')
        return

    async def delete_all(self):
        try:
            # Returns list of user strategies
            api_response = await self.api_instance.strategies(self.site)
        except ApiException as e:
            message = f'Nickel broker error {e}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()
            return False
        ids = {twap.execution_id: twap.state for twap in api_response.twaps}
        stopped_ids = [id for id, state in ids.items() if state != 'RUNNING']

        self.logger.info(f'Found {len(ids)} twap and {len(stopped_ids)} stopped ones to delete')

        if stopped_ids:
            body = http_nt_pclient.ExecutionIds(execution_ids=stopped_ids)
            self.logger.info(f'Deleting Nickel TWaps {stopped_ids}')
            try:
                api_response = await self.api_instance.delete_multi(body, self.site)
            except ApiException as e:
                self.logger.warning(f'Exception when calling StrategyApi->delete_multi: {e}')



    async def send_order(self, id, event, spread):
        self.logger.info('Nickel broker received order')
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, coin=event.ticker, action=event.action, price=event.price,
                                                target_quantity=event.quantity, comment=event.comment,
                                                nature=event.nature)
        else:
            return None

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        self.logger.info(f'Preparing {nature} order for {action} with ticker {s1}_{s2} and comment {comment}')

        p1, p2 = prices
        symbol1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1, universal=False)
        symbol2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2, universal=False)
        ticker1 = f'{symbol1}@{self.exchange}'
        ticker2 = f'{symbol2}@{self.exchange}'
        p1 /= factor1
        p2 /= factor2
        params1 = self.base_params.copy()
        params2 = self.base_params.copy()
        id1 = id+'L1'
        id2 = id+'L2'
        max_order_size1 = NickelBroker.get_max_order_size(target_size1, price=p1)
        max_order_size2 = NickelBroker.get_max_order_size(target_size2, price=p2)
        params1['instrument'] = ticker1
        params1['side'] = 'BUY' if action > 0 else 'SELL'
        params1['target'] = float(round(target_size1, 8))
        params1['step_size'] = float(round(max_order_size1, 8))
        params1['step_time'] = NickelBroker.STEP_TIME

        params2['instrument'] = ticker2
        params2['side'] = 'BUY' if action < 0 else 'SELL'
        params2['target'] = float(round(target_size2, 8))
        params2['step_size'] = float(round(max_order_size2, 8))
        params2['step_time'] = NickelBroker.STEP_TIME

        multi_params = http_nt_pclient.MultipleTwapParameters(twaps=[
            http_nt_pclient.TwapParametersWithId(
                execution_id=id1,
                parameters=http_nt_pclient.TwapParameters(**params1)),
            http_nt_pclient.TwapParametersWithId(
                execution_id=id2,
                parameters=http_nt_pclient.TwapParameters(**params2))
        ])

        self.logger.info(f'sending double twap to NickelBroker: {id1}:{params1}, {id2}:{params2}')
        try:
            async with self.rate_limiter:
                api_response = await self.twap_api_instance.create_multi(multi_params, self.site)
        except ApiException as e:
            message = f'Nickel broker error {e}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()
            return False
        if not api_response:
            message = f'Nickel broker returned false'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()

            return False 

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                               nature='', translate_qty_incontracts=False, use_algo=False):
        self.logger.info(f'Preparing {nature} order for {action} with ticker {coin} and comment {comment}')
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(coin, universal=False)
        ticker = f'{symbol}@{self.exchange}'
        self.logger.info(f'Received factor of {factor} for {coin} with symbol {symbol} and ticker {ticker}')

        if price is not None:
            price /= factor
        target_quantity *= factor
        max_order_size = NickelBroker.get_max_order_size(target_quantity, price=price)
        parameters = self.base_params.copy()
        parameters['instrument'] = ticker
        parameters['side'] = 'BUY' if action > 0 else 'SELL'
        parameters['target'] = float(round(target_quantity, 8))
        parameters['step_size'] = float(round(max_order_size, 8))
        parameters['step_time'] = NickelBroker.STEP_TIME

        params = http_nt_pclient.TwapParameters(**parameters)

        self.logger.info(f'sending twap to NickelBroker: {parameters}')
        try:
            async with self.rate_limiter:
                api_response = await self.twap_api_instance.create(params, self.site, order_id)
        except ApiException as e:
            message = f'Nickel broker error {e}'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()

            return False
        if not api_response:
            message = f'Nickel broker returned false'
            self.logger.warning(message)
            async with aiohttp.ClientSession() as session:
                await TGMessenger.send_async(session, message, chat_channel='AwsMonitor')
                await session.close()

            return False

        return True

    async def stop_order(self, order_id):
        id1 = order_id + 'L1'
        id2 = order_id + 'L2'
        api_response = False

        try:
            body = http_nt_pclient.MultipleStrategiesActionRequest(action='STOP', execution_ids=[id1, id2])
            async with self.rate_limiter:
                api_response = await self.api_instance.change_state_multi(body, self.site)
        except ApiException as e:
            self.logger.error(f'Nickel broker sent error {e} while stopping pair order {order_id}')
        except Exception as e:
            self.logger.error(f'Error stopping order {order_id}: {e}')
        return api_response

    async def stop_simple_order(self, order_id):
        api_response = False

        try:
            body = http_nt_pclient.RequestedAction(action='STOP')
            async with self.rate_limiter:
                api_response = await self.api_instance.change_state(body, self.site, order_id)
        except ApiException as e:
            self.logger.error(f'Nickel broker sent error {e} while stopping order {order_id}')
        except Exception as e:
            self.logger.error(f'Error stopping simple order {order_id}: {e}')
        return api_response

    @property
    def get_id(self):
        self.id =  ''.join([choice(ascii_letters + digits) for i in range(6)])
        return self.id"""

class EdoSpreaderBroker:
    ACCOUNT_DICT = {
        'okx':
            {
                'edo1': 'edo1',
                'edo3': 'edo3'
            }
    }

    def __init__(self, market, broker_handler, account='1', chat_channel=None):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler

        if 'ok' in market:
            self.exchange = 'okx'
        else:
            raise ValueError('Bad market for Edo broker')

        if (self.exchange not in EdoSpreaderBroker.ACCOUNT_DICT or
                account not in EdoSpreaderBroker.ACCOUNT_DICT[self.exchange]):
            raise ValueError('Bad exchange/account for Edo broker')
        self.account_name = EdoSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.logger = logging.getLogger(f'broker_edo_{market}_{account}')
        self.listener = None

    @property
    def is_traded(self):
        return True

    async def stop_all(self, delete_all=False):
        return

    @staticmethod
    async def delete_all():
        return

    async def close(self):
        return

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, coin=event.ticker, action=event.action,
                                                price=event.price, target_quantity=event.quantity,
                                                comment=event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """
        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        with_tg_message = self._tg_channel != ''

        p1, p2 = prices
        spread = spread_shift
        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        spread = round(spread, 8)
        target_size1 = round(target_size1, 6)
        target_size2 = round(target_size2, 6)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'
        s1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1, universal=True)
        s2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2, universal=True)
        p1 /= factor1
        p2 /= factor2
        target_size1 *= factor1
        target_size2 *= factor2

        config = {
            'legOneSymbol': s1, 'legTwoSymbol': s2,
            'targetSizeOne': target_size1,
            'targetSizeTwo': target_size2,
            'spreadDirection': direction,
            'comment': comment
        }
        tasks = []
        self.logger.info(f'Received spread order: {config}')

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    await TGMessenger.send_message_async(session, f'{self.exchange}/{self.account_name}:{comment}',
                                                         s1, s2, direction,
                                                         spread, p1, p2, chat_channel=self._tg_channel)
        except aiohttp.ClientConnectorError as e:
            print(f'web_broker: {e}')
        except Exception as e:
            print(f'web_broker: {e}')

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=True):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """
        direction = 'BUY_LEG' if action > 0 else 'SELL_LEG'
        symbol, factor = self.broker_handler.symbol_to_market_with_factor(coin)
        price /= factor
        target_quantity *= factor
        config = {
            'legSymbol': symbol,
            'targetSize': target_quantity,
            'direction': direction,
            'comment': comment
        }

        self.logger.info(f'Received simple order: {config}')

        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id

    async def stop_order(self, order_id):
        return True

    async def stop_simple_order(self, order_id):
        return True


class MelanionSpreaderBroker:
    ACCOUNT_DICT = {
        'bin_spot':
            {
                'dummy': 'dummy',
            }
    }

    def __init__(self, market, broker_handler, account=1, chat_channel=None):
        self._tg_channel = chat_channel
        self.id = None
        self.broker_handler = broker_handler
        working_dir = 'output_melanion/'
        output = 'order.log'

        if 'bin' in market:
            self.exchange = 'bin_fut'
        else:
            raise ValueError('Bad market for Melanion broker')

        if (self.exchange not in MelanionSpreaderBroker.ACCOUNT_DICT or
                account not in MelanionSpreaderBroker.ACCOUNT_DICT[self.exchange]):
            raise ValueError('Bad exchange/account for Melanion broker')
        self.account_name = MelanionSpreaderBroker.ACCOUNT_DICT[self.exchange][account]
        self.logger = logging.getLogger('Melanion')
        handler = logging.FileHandler(filename=os.path.join(working_dir, output))
        handler.setFormatter(logging.Formatter('{asctime}:{levelname}:{name}:{message}', style='{'))
        self.logger.addHandler(handler)
        self.listener = None

    def get_contract_qty_from_coin(self, coin, qty):
        return self.broker_handler.get_contract_qty_from_coin(coin, qty)

    async def send_order(self, id, event, spread):
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.nature, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, event.coin_name, spread, event.action, event.quantity,
                                                event.nature, event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, nature,
                              comment):
        """

        :param id: int, id of order
        :param s1: str, ticker1
        :param s2: str, ticker2
        :param spread_shift: float, leeway for order acceptance in bp
        :param action: int, direction for ticker1
        :param target_size1: float
        :param target_size2: float
        :param prices: Tuple(float, float)
        :param comment: str
        :return: bool
        """

        with_tg_message = self._tg_channel != ''

        p1, p2 = prices
        spread = spread_shift
        max_order_size1 = target_size1 * 0.4  # TODO later
        # spread_s = np.format_float_positional(spread, 6, unique=False, fractional=False)
        # target_size_s = np.format_float_positional(target_size1, 6, unique=False, fractional=False)
        # max_order_size_s = np.format_float_positional(max_order_size1, 6, unique=False, fractional=False)

        spread = round(spread, 8)
        target_size1 = round(target_size1, 6)
        max_order_size1 = round(max_order_size1, 6)

        direction = 'BUY_LEG1' if action > 0 else 'SELL_LEG1'
        s1, factor1 = self.broker_handler.symbol_to_market_with_factor(s1)
        s2, factor2 = self.broker_handler.symbol_to_market_with_factor(s2)
        p1 /= factor1
        p2 /= factor2

        try:
            async with aiohttp.ClientSession() as session:
                if with_tg_message:
                    await TGMessenger.send_message_async(session, f'{self.exchange}/{self.account_name}:{comment}',
                                                         s1, s2, direction,
                                                         spread, p1, p2, chat_channel=self._tg_channel)
        except aiohttp.ClientConnectorError as e:
            self.logger.warning(f'web_broker: {e}')
        except Exception as e:
            self.logger.warning(f'web_broker: {e}')

        self.logger.info(f'{direction},{s1},{s2},{p1},{p2},{comment}')

        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=True):
        """, coin, direction, quantity

        :param order_id: int, id of order
        :param coin: str, ticker
        :param action: int, direction for ticker
        :param price: float, price for LIM order
        :param target_quantity: float
        :return: bool
        """

        return True

    @property
    def get_id(self):
        self.id = str(uuid.uuid4())
        return self.id

    async def stop_order(self, order_id):
        return True


class DummyBroker:
    def __init__(self, market, account=1):
        self.logger = logging.getLogger(f'broker_dummy_{market}_{account}')
        self.listener = None

    @property
    def is_traded(self):
        return False

    async def stop_all(self, delete_all=False):
        return

    @staticmethod
    async def delete_all():
        return

    async def close(self):
        return

    async def send_order(self, id, event, spread):
        self.logger.info(event, {'order': event})
        if event.type == EventType.PAIRORDER:
            s1, s2 = parse_pair(event.ticker)
            return await self.send_pair_order(id, s1, s2, spread, event.action, event.quantity[0],
                                              event.quantity[1], event.price, event.comment)
        elif event.type == EventType.ORDER:
            return await self.send_simple_order(id, event.ticker, spread, event.action, event.quantity,
                                                event.comment)

    async def send_pair_order(self, id, s1, s2, spread_shift, action, target_size1, target_size2, prices, comment):
        return True

    async def send_simple_order(self, order_id, coin, action, price, target_quantity, comment,
                                translate_qty_incontracts=False, use_algo=False):
        return True

    async def stop_order(self, order_id):
        return True

    async def stop_simple_order(self, order_id):
        return True

    @property
    def get_id(self):
        return 'xyz'



async def pair_bin():
    params = {
        'exchange_trade': 'binancefut',
        'account_trade': 'mel_cm3'
    }
    bh = BrokerHandler(market_watch='binancefut', strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market='bin_fut', broker_handler=bh, account='mel_cm3', chat_channel='')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    s1 = 'XRPUSDT'
    s2 = 'ADAUSDT'
    price1 = 0.5809
    price2 = 0.5815
    target_size1 = 35
    target_size2 = target_size1 * price1 / price2
    action = -1

    await wb.send_pair_order(id, s1, s2, 10.0, action=action, target_size1=target_size1,
                             target_size2=target_size2, prices=(price1, price2), nature='xx', comment='test')
    index = 0
    wsl.subscribe(id)
    while index < 100:
        await asyncio.sleep(1)
        result = wsl.get_strat_result(id)
        print(result)
        if 'STOPPED' in result.get('state', '') or 'reached' in result.get('info', '') and 'TERMINATED' in result.get(
                'state', ''):
            index = 1e5
        index = index + 1
    await wsl.stop_listener(id)
    await wb.stop_order(id)


async def single():
    # TODO : modifier params
    bh = BrokerHandler('bybit', 'bybit', None, 'bybit')
    wb = WebSpreaderBroker('bybit', bh, '1', 'CM')
    wsl = WebSpreaderListener(logging.getLogger())
    id = wb.get_id
    print(id)
    await wb.send_simple_order(id, 'STORJUSDT', 1, 0.639, 824.3, 'test', False)
    wsl.subscribe(id)
    index = 0
    while index < 600:
        await asyncio.sleep(1)
        print(wsl.get_strat_result(id))
        index = index + 1
    await wsl.stop_listener(id)
    await wb.stop_order(id)


async def single_bin():
    exchange_trade = 'binancefut'#'binancefut' 'bybit'
    account_trade = 'mel_cm3'#'mel_cm3' 1
    params = {
        'exchange_trade': exchange_trade, #'binancefut',
        'account_trade': account_trade #'mel_cm3'
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    target_amnt = 1000
    coin = 'VETUSDT' #'XRPUSDT'
    action = 1
    nature = 'x'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)
    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        print(state, ':', exq)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def do_single_binspot():
    exchange_trade = 'binance'#'binancefut' 'bybit'
    account_trade = '2'
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    target_amnt = 10
    coin = 'BNBUSDT' #'XRPUSDT'
    action = 1
    nature = 'n'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)
    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        prc = float(result.get('avgPrc', 0))
        print(state, ':', exq, prc)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def do_single_bitget():
    exchange_trade = 'bitget'#'binancefut' 'bybit'
    account_trade = '1'
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }
    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = WebSpreaderBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    wsl = WebSpreaderListener(logger=logging.getLogger())
    id = wb.get_id
    print(id)
    #target_amnt = 10
    coin = 'ARCUSDT' #'XRPUSDT'
    action = 1
    nature = 'x'
    ticker = await ep._exchange_async.fetch_ticker(coin)
    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    target_quantity = 567 # target_amnt / price
    await wb.send_simple_order(id, coin=coin, action=action, price=price, target_quantity=target_quantity,
                               comment='test', nature=nature,
                               translate_qty_incontracts=False, use_algo=True)


    wsl.subscribe(id)
    index = 0
    max_time = 3000 / 5
    while index < max_time:
        await asyncio.sleep(5)
        result = wsl.get_strat_result(id)
        state = result.get('state', '')
        exq = float(result.get('execQty', 0))
        prc = float(result.get('avgPrc', 0))
        print(state, ':', exq, prc)
        if np.abs(exq - action * target_quantity) < 1e-3:
            index = max_time
        print(100 * np.abs(exq) / target_quantity, '%')
        if 'topped' in state:
            index = max_time
        index = index + 1
    await wsl.stop_listener()
    await wb.stop_simple_order(id)
    await ep._exchange_async.close()

async def clean_up_nickel():
    logger = logging.getLogger('default')
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    if not logger.handlers:  # Évite d'ajouter plusieurs fois le même gestionnaire
        logger.addHandler(console_handler)

    exchange_trade = 'binancefut'
    account_trade = 'CM'
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }

    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = NickelBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    await wb.delete_all()

    await ep._exchange_async.close()

async def do_single_nickel():
    # one twap
    logger = logging.getLogger('default')
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    if not logger.handlers:  # Évite d'ajouter plusieurs fois le même gestionnaire
        logger.addHandler(console_handler)

    exchange_trade = 'binancefut'
    account_trade = 'CM'
    params = {
        'exchange_trade': exchange_trade,
        'account_trade': account_trade
    }

    ep = BrokerHandler.build_end_point(exchange_trade, account_trade)
    bh = BrokerHandler(market_watch=exchange_trade, end_point_trade=ep, strategy_param=params, logger_name='default')
    wb = NickelBroker(market=exchange_trade, broker_handler=bh, account=account_trade, chat_channel='CM')
    # await wb.test_api()
    id = wb.get_id
    print(id)
    #target_amnt = 10
    coin = 'PAXGUSDT' #'XRPUSDT'
    action = 1
    nature = 'n'

    target_quantity = 0.01
    ticker = await ep._exchange_async.fetch_ticker(coin)

    if 'last' in ticker:
        price = ticker['last']
    else:
        raise ValueError()

    order = CoinOrderEvent(timestamp=int(1000 * datetime.timestamp(datetime.today())),
                                      ticker=coin,
                                      action=action,
                                      quantity=target_quantity, price=price,
                                      nature=nature,
                                      comment='single_entry',
                                      strat_name='test')
    await wb.send_order(id, order, 0)

    wb.listener.subscribe(id)
    index = 0
    max_time = 20

    while index < max_time:
        await asyncio.sleep(1)
        index += 1
        result = wb.listener.get_strat_result(id)
        print(result)

    await wb.stop_all()

    await ep._exchange_async.close()
    index = 0
    max_time = 3000 / 5
    # while index < max_time:
    #     await asyncio.sleep(5)
    #     state = result.get('state', '')
    #     exq = float(result.get('execQty', 0))
    #     prc = float(result.get('avgPrc', 0))
    #     print(state, ':', exq, prc)
    #     if np.abs(exq - action * target_quantity) < 1e-3:
    #         index = max_time
    #     print(100 * np.abs(exq) / target_quantity, '%')
    #     if 'topped' in state:
    #         index = max_time
    #     index = index + 1
    # await wb.stop_simple_order(id)
    # await ep._exchange_async.close()

async def listener():
    wsl = WebSpreaderListener(logger=logging.getLogger())
    wsl.subscribe('r2d2')
    await asyncio.sleep(10000)

async def main():
    await clean_up_nickel()

if __name__ == '__main__':
    dotenv.load_dotenv()
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # loop = asyncio.SelectorEventLoop()
        # asyncio.set_event_loop(loop)

    asyncio.run(main())
'''

SB
    IP:8080/wsapi/strat/update/ab15f001-d541-4b5c-b413-f5fb6e77cb1d
    /api/strat/start/{strat UID}
    /api/strat/stop/{strat UIID}
    /api/strat/delete/{strat UID}

export interface ManualOrderConfiguration {
    name: string,
    strategyName: string,
    symbol: string,
    exchange: string,
    account: string,
    offset: Offset,
    useMargin: boolean,
    targetSize: number,
    price: number,
    maxOrderSize: number,
    maxAliveOrderTime: number,
    direction:  Side,
    type: OrderType,
    childOrderDelay: number,
    start: Start
    }
export interface StrategyOrderRequest {
  clientOrderId: string,
  orderMsg: OrderMsg,
  spreaderConfiguration?: SpreaderConfiguration
  manualOrderConfiguration?: ManualOrderConfiguration
}
{'avgPrc': '0', 'state': 'LiveOrder', 'execQty': '0', 'info': ''}
{'avgPrc': '0.5914', 'state': 'RetriedLiveOrder', 'execQty': '17', 'info': ''}
{'avgPrc': '0.591267', 'state': 'RetriedLiveOrder', 'execQty': '51', 'info': ''}
{'avgPrc': '0.592283', 'state': 'Stopped', 'execQty': '-102', 'info': 'targetSize reached'}

{}
{'leg2AvgExecPrc': '0', 'pricingTimestamp': '1712160079707', 'premium': '-6.9036', 'leg2ExecQty': '0', 'leg1ExecQty': '0', 'delayed': 'false', 'state': 'BOTH_LEG_SENT', 'leg1AvgExecPrc': '0', 'info': ''}

{'leg2AvgExecPrc': '0', 'pricingTimestamp': '1712159785707', 'premium': '-37.7173', 
'leg2ExecQty': '0', 'leg1ExecQty': '0', 
'delayed': 'false', 'state': 'WAITING_SIGNAL', 'leg1AvgExecPrc': '0', 'info': ''}
{'leg2AvgExecPrc': '0.5811', 'pricingTimestamp': '1712159641707', 'premium': '-6.8923', 
'leg2ExecQty': '-258', 'leg1ExecQty': '259', 'delayed': 'false', 'state': 'TERMINATED', 
'leg1AvgExecPrc': '0.5801', 'info': 'targetSize reached'}


Process finished with exit code 0
    
'''
