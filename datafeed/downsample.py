import pandas as pd
import numpy as np
import numba as nb
from numba import float32


#@nb.jit((float32[:], float32))
def find_index(step, data):
    threshold = 0
    g_count = 0

    for index in range(data.shape[0]):
        if data[index, 8] > threshold:
            threshold = threshold + step
            g_count += 1
        data[index, 7] = g_count
    return data


def __extract_data(data):
    # Extract data
    date_time = data[['date', 'Group']].groupby('Group')['date'].last()
    open = data[['open', 'Group']].astype(float).groupby('Group').first()
    high = data[['high', 'Group']].astype(float).groupby('Group').max()
    low = data[['low', 'Group']].astype(float).groupby('Group').min()
    close = data[['close', 'Group']].astype(float).groupby('Group').last()
    volume = data[['volume', 'Group']].astype(float).groupby('Group').sum()
    vwap = pd.DataFrame(data[['Transaction', 'Group']].astype(float).groupby('Group').sum().values / volume.values)

    # Create DataFrame
    bars = pd.concat([date_time, open, high, low, close, volume], axis=1)
    bars.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    bars['vwap'] = vwap.values

    return bars


def __time_bars(data, units, vwap=False):
    # Set the time frame
    data['date'] = data.index
    duration = str(units) + 'min'

    # Extract data
    ohlc = data.resample(duration).agg({'open': 'first',
                                 'high': 'max',
                                 'low': 'min',
                                 'close': 'last', 'volume':'sum'})
    volume = ohlc['volume']
    date_time = pd.DataFrame(ohlc.index, index=ohlc.index)
    # Create DataFrame

    if vwap:
        vwap_col = data.resample(duration, label='right')['Transaction'].sum().values / volume
        data = pd.concat([ohlc, vwap_col], axis=1)
        data.columns = ['open', 'high', 'low', 'close', 'volume', 'vwap']
    else:
        data = pd.concat([ohlc], axis=1)
        data.columns = ['open', 'high', 'low', 'close', 'volume']

    return data


def __dollar_bars(data, units):
    # Dollar metric
    data['CumDollars'] = data['Transaction'].cumsum()
    col_names = data.columns

    # Set the relevant group for each row
    data = find_index(units, np.array(data))
    data = pd.DataFrame(data, columns=col_names)
    data = __extract_data(data)

    return data


def __volume_bars(data, units):
    # Volume metric
    data['CumVol'] = data['volume'].cumsum()
    col_names = data.columns

    # Set the relevant group for each row
    data = find_index(units, np.array(data))
    data = pd.DataFrame(data, columns=col_names)
    data = __extract_data(data)

    return data


def create_bars(data, units=1000, type='time', vwap=False):
    """
    Creates the desired bars. 4 different types:
    1. Time Bars
    2. Tick Bars
    3. Volume Bars
    4. Dollar Bars

    See book for more info:
    Marcos Prado (2018), Advances in Financial Machine Learning, pg 25

    :param data: Pandas DataFrame of ohlcv with datetime index
    :param units: Number of units in a bar.
                  Time Bars: Number of minutes per bar
                  Tick Bars: Number of ticks per bar
                  Volume Bars: Number of shares traded per bar
                  Dollar Bars: Transaction size traded per bar

    :param type: String of the bar type, ('tick', 'volume', 'dollar', 'time')
    :return: Pandas DataFrame of relevant bar data
    """
    data['Transaction'] = data['close'] * data['volume']

    # Create an empty column
    data['Group'] = np.nan

    if type == 'tick':
        raise ValueError('Not implemented')
    elif type == 'volume':
        bars = __volume_bars(data, units)
    elif type == 'dollar':
        bars = __dollar_bars(data, units)
    elif type == 'time':
        bars = __time_bars(data, units)
    else:
        raise ValueError('Type must be: tick, volume, dollar, or time')

    return bars
