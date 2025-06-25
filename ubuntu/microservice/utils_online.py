import numpy as np
import pandas as pd
import json
import io
from datetime import datetime, timedelta, date
import calendar

BASKET_LABEL_SEPARATOR = '__'


def print_to_string(*args, **kwargs):
    with io.StringIO() as output:
        print(*args, file=output, **kwargs)
        contents = output.getvalue()
        output.close()
    return contents


def make_column(ticker, feature):
    return ticker + '_' + feature


def parse_column(name):
    return name.split('_')


def dt2ts(dt):
    ts = calendar.timegm(dt.timetuple())
    return ts


def weekly_name(filename, ext):
    week = f'{datetime.today().isocalendar()[1]:02d}'
    year = str(datetime.today().year)

    return filename + year + week + ext


def hour_rounder(t, nearest=False):
    # Rounds to previous hour
    # or nearest by adding a timedelta hour if minute >= 30

    if nearest:
        td = timedelta(hours=t.minute//30)
        return t.replace(second=0, microsecond=0, minute=0, hour=t.hour) + td
    else:
        return t.replace(second=0, microsecond=0, minute=0, hour=t.hour)


def second_rounder(t):
    # Rounds to previous second
    return t.replace(second=t.second, microsecond=0, minute=t.minute)


def today_utc() -> pd.Timestamp:
    tz_info = datetime.now().astimezone().tzinfo
    now = pd.Timestamp(datetime.today()).tz_localize(tz_info).tz_convert('UTC')

    return now


def utc_ize(local_timestamp: float) -> pd.Timestamp:
    """
    :param local_timestamp: timestamp to convert to UTC
    :return:
    """
    tz_info = datetime.now().astimezone().tzinfo
    time_date = pd.Timestamp(datetime.fromtimestamp(local_timestamp, tz=tz_info)).tz_convert('UTC')

    return time_date


def parse_pair(pair_name):
    if type(pair_name) == tuple:
        return pair_name
    s1 = pair_name[:pair_name.find(BASKET_LABEL_SEPARATOR)]
    s2 = pair_name[pair_name.find(BASKET_LABEL_SEPARATOR)+len(BASKET_LABEL_SEPARATOR):]
    return s1, s2


def make_pair(s1, s2, ordered=False):
    pair = [s1, s2]
    if ordered:
        pair.sort()
    return BASKET_LABEL_SEPARATOR.join(pair)


def reverse_pair(pair_name):
    s1, s2 = parse_pair(pair_name)
    pair = [s2, s1]
    return BASKET_LABEL_SEPARATOR.join(pair)


def make_basket(coin_list):
    coin_list.sort()  # TODO: leader first?
    return BASKET_LABEL_SEPARATOR.join(coin_list)


def coins_from_pairs(pairs):
    coins = set()
    for pair_name in pairs:
        s1, s2 = parse_pair(pair_name)
        coins.add(s1)
        coins.add(s2)
    return coins


def coins_from_peers(peers):
    perimeter = set()
    for coin, peer in peers.items():
        perimeter.add(coin)
        perimeter.update(peer)
    return perimeter


def get_pair_labels(returns):
    labels = []
    dim = returns.shape[1]

    for row_number in range(dim):
        for column_number in range(row_number+1, dim):
            labels.append(make_basket([returns.columns[row_number], returns.columns[column_number]]))

    return labels


def extract_coin(symbol):
    symbol = symbol.replace('_perp', '')  # file
    symbol = symbol.replace('/USDT:USDT', '') # universal
    symbol = symbol.replace('/USDC:USDC', '') # hyperliquid
    symbol = symbol.replace('SWAP', '')  # okx
    # symbol = symbol.replace('UMCBL', '')  # bitget
    symbol = symbol.replace('USDTM', '')  # kucoin
    symbol = symbol.replace('USDT', '')  # bybit, binance, okx
    symbol = symbol.replace('-', '')  # okx
    symbol = symbol.replace('_', '')  # bitget
    return symbol


def extract_coin_with_factor(symbol):
    symbol = extract_coin(symbol)
    factor = 1
    if '10000' in symbol:
        symbol = symbol.replace('10000', '')
        factor = 10000
    elif '1000' in symbol:
        symbol = symbol.replace('1000', '')
        factor = 1000
    return symbol, factor


def build_symbol(coin, market, perp=False, factor=True, to_quote=False, universal=False):
    """

    :param coin:
    :type coin: str
    :param market:
    :type market: str
    :return:
    :rtype:
    """
    if universal and 'spot' not in market:
        symbol = coin + '/USDT:USDT'
    elif 'bin' in market and not to_quote:
        symbol = coin + 'USDT'
    elif market == 'okex' or market == 'okx':
        symbol = coin + '-USDT'
    elif market == 'okexfut' or market == 'okxfut':
        symbol = coin + ('/USDT:USDT' if perp else '-USDT-SWAP')  # bug ccxt dans la clé
    elif market == 'bybit':
        symbol = coin + 'USDT'
    elif market == 'kucoin':
        symbol = coin + 'USDTM'
    elif market == 'bitget':
        symbol = coin + 'USDT'
    elif market == 'hyperliquid':
        symbol = coin + '/USDC:USDC'
    elif market == 'file':
        if perp:
            symbol = coin + '_perp'
        else:
            symbol = coin
    else:
        symbol = coin

    if not factor:
        return symbol, 1

    factor = 1
    if market == 'binancefut' or market == 'bybit' or market == 'bitget':
        if 'SHIB' in symbol:
            if market == 'bybit':
                symbol = symbol.replace('SHIB', 'SHIB1000')
            if market == 'binancefut':
                symbol = symbol.replace('SHIB', '1000SHIB')
            factor = 1e-3

        else:
            sym_list = {
                'binancefut': ['PEPE', 'SATS', 'LUNC', 'XEC', 'BONK', 'FLOKI'],
                'bybit': ['PEPE', 'SATS', 'LUNC', 'XEC', 'BONK', 'FLOKI'],
                'bitget': ['XEC', 'BONK', 'SATS', 'RATS', 'CAT']
            }

            for name in sym_list.get(market, []):
                if name in symbol:
                    symbol = symbol.replace(name, '1000' + name)
                    factor = 1e-3
                    break
    return symbol, factor


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            date_format = '%Y/%m/%d %H:%M'
            return obj.strftime(date_format)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)
