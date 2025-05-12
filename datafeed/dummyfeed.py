import numpy as np
import pandas as pd
from datetime import datetime
import time
from .motherfeeder import MotherFeeder


class DummyEndPoint(object):
    """
    This is a fake data source for testing
    """
    rateLimit = 1000

    def load_markets(self):
        return None

    def fetch_markets(self):
        return [
            {'id': 'ETHBTC', 'symbol': 'ETH/BTC', 'base': 'ETH', 'quote': 'BTC', 'baseId': 'ETH', 'quoteId': 'BTC',
                 'info': {'symbol': 'ETHBTC', 'status': 'TRADING', 'baseAsset': 'ETH', 'baseAssetPrecision': 8,
                          'quoteAsset': 'BTC', 'quotePrecision': 8, 'quoteAssetPrecision': 8,
                          'baseCommissionPrecision': 8, 'quoteCommissionPrecision': 8,
                          'orderTypes': ['LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT'],
                          'icebergAllowed': True, 'ocoAllowed': True, 'quoteOrderQtyMarketAllowed': True,
                          'isSpotTradingAllowed': True, 'isMarginTradingAllowed': True,
                          'filters': [{'filterType': 'PRICE_FILTER', 'minPrice': '0.00000100',
                                       'maxPrice': '100000.00000000', 'tickSize': '0.00000100'},
                                      {'filterType': 'PERCENT_PRICE', 'multiplierUp': '5', 'multiplierDown': '0.2', 'avgPriceMins': 5},
                                      {'filterType': 'LOT_SIZE', 'minQty': '0.00100000', 'maxQty': '100000.00000000', 'stepSize': '0.00100000'},
                                      {'filterType': 'MIN_NOTIONAL', 'minNotional': '0.00010000', 'applyToMarket': True, 'avgPriceMins': 5},
                                      {'filterType': 'ICEBERG_PARTS', 'limit': 10},
                                      {'filterType': 'MARKET_LOT_SIZE', 'minQty': '0.00000000', 'maxQty': '11603.98885764', 'stepSize': '0.00000000'},
                                      {'filterType': 'MAX_NUM_ALGO_ORDERS', 'maxNumAlgoOrders': 5},
                                      {'filterType': 'MAX_NUM_ORDERS', 'maxNumOrders': 200}],
                          'permissions': ['SPOT', 'MARGIN']},
             'active': True, 'precision': {'base': 8, 'quote': 8, 'amount': 3, 'price': 6},
             'limits': {'amount': {'min': 0.001, 'max': 100000.0},
                        'price': {'min': 1e-06, 'max': 100000.0},
                        'cost': {'min': 0.0001, 'max': None}}},
            {'id': 'ETHUSDT', 'symbol': 'ETHUSDT', 'base': 'LTC', 'quote': 'BTC', 'baseId': 'LTC', 'quoteId': 'BTC', 'info': {'symbol': 'LTCBTC', 'status': 'TRADING', 'baseAsset': 'LTC', 'baseAssetPrecision': 8, 'quoteAsset': 'BTC', 'quotePrecision': 8, 'quoteAssetPrecision': 8, 'baseCommissionPrecision': 8, 'quoteCommissionPrecision': 8, 'orderTypes': ['LIMIT', 'LIMIT_MAKER', 'MARKET', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT'], 'icebergAllowed': True, 'ocoAllowed': True, 'quoteOrderQtyMarketAllowed': True, 'isSpotTradingAllowed': True, 'isMarginTradingAllowed': True, 'filters': [{'filterType': 'PRICE_FILTER', 'minPrice': '0.00000100', 'maxPrice': '100000.00000000', 'tickSize': '0.00000100'}, {'filterType': 'PERCENT_PRICE', 'multiplierUp': '5', 'multiplierDown': '0.2', 'avgPriceMins': 5}, {'filterType': 'LOT_SIZE', 'minQty': '0.01000000', 'maxQty': '100000.00000000', 'stepSize': '0.01000000'}, {'filterType': 'MIN_NOTIONAL', 'minNotional': '0.00010000', 'applyToMarket': True, 'avgPriceMins': 5}, {'filterType': 'ICEBERG_PARTS', 'limit': 10}, {'filterType': 'MARKET_LOT_SIZE', 'minQty': '0.00000000', 'maxQty': '32245.65149306', 'stepSize': '0.00000000'}, {'filterType': 'MAX_NUM_ORDERS', 'maxNumOrders': 200}, {'filterType': 'MAX_NUM_ALGO_ORDERS', 'maxNumAlgoOrders': 5}], 'permissions': ['SPOT', 'MARGIN']}, 'active': True, 'precision': {'base': 8, 'quote': 8, 'amount': 2, 'price': 6}, 'limits': {'amount': {'min': 0.01, 'max': 100000.0}, 'price': {'min': 1e-06, 'max': 100000.0}, 'cost': {'min': 0.0001, 'max': None}}},
        ]

    def fetch_ohlcv(self, symbol, read_timeframe, since, limit):
        step = 60 * 60
        end_time = int(time.time())
        index = 0
        open = 1000

        time_schedule = pd.date_range(start=datetime.fromtimestamp(since//1000), end=datetime.fromtimestamp(end_time), freq='min')
        # time step in seconds
        event_index = []

        def get_random_row(date, open):
            o = open + np.random.normal()
            c = open + np.random.normal()
            h = open + np.random.normal()
            l = open + np.random.normal()
            hi = max([o,c,h,l])
            lo = min([o,c,h,l])
            return [date, o, hi, lo, c, 10000 + open + np.random.normal(scale=500)]

        ohlcv = [get_random_row(date, open) for date in time_schedule]

        data = pd.DataFrame(ohlcv, columns=['date', 'open', 'high', 'low','close', 'volume'],
                            index=time_schedule).drop('date', axis=1)

        return data

    async def fetch_positions(self):
        return {}
    async def close(self):
        return

class DummyMarket(MotherFeeder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def build_end_point(self):
        return DummyEndPoint()

    def build_end_point_async(self):
        return DummyEndPoint()

    def _get_limit(self):
        return 1e10

    def _arrange_data(self, df):
        return df

    def _init_API(self):
        return
    def get_rounded(self, quantity, symbol):
        return np.round(quantity, 5)

    def get_cash(self, currency):
        return 100000, 100000

    async def get_cash_async(self, currency):
        return 100000, 100000

    async def set_leverage_async(self, symbol, leverage):
        return

    def _dict_from_pos(self, positions):
        return {}